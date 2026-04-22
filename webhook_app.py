"""
Chargebee webhook → Salesforce expansion Opportunity (minimal service).

Designed to run on a host with a public URL (Railway, Render, Fly.io, Google Cloud Run, etc.),
not on your laptop. Use the repo Dockerfile; set environment variables in the host’s dashboard
(same names as .env.example — you do not need a .env file on the server).

Chargebee: Settings → Configure Chargebee → Webhooks
  - URL: https://<your-host>/chargebee
  - Events: subscription_changed (optionally subscription_created to learn baselines)
  - HTTP Basic Auth: match WEBHOOK_BASIC_USER / WEBHOOK_BASIC_PASS

State file WEBHOOK_STATE_PATH defaults to ./webhook_state.json inside the container; use a volume
if you need quantities to survive redeploys (otherwise the next event re-baselines without an opp).

Run `python seed_webhook_state.py` (with Chargebee API env vars) or POST `/admin/seed-state` with
header `X-Seed-Secret` (= `WEBHOOK_SEED_SECRET`) to merge current Chargebee quantities into state.

If a subscription line has no stored quantity yet, we treat the prior quantity as 0 so the
first seats added (e.g. 0→1) create an Opportunity. Run seed_webhook_state (or POST /admin/seed-state)
so existing high quantities in Chargebee are in state before go-live—otherwise the first webhook
after deploy could count the full current qty as "new" seats.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from simple_salesforce import Salesforce

from chargebee_client import (
    _addon_exact_ids_from_env,
    _crm_addon_line_matches,
    self_service_line_state_key,
)
from seed_webhook_state import merge_chargebee_into_state_file

load_dotenv(Path(__file__).resolve().parent / ".env")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("webhook_app")

app = Flask(__name__)

_state_lock = threading.Lock()
_STATE_PATH = Path(
    os.getenv("WEBHOOK_STATE_PATH", str(Path(__file__).resolve().parent / "webhook_state.json"))
)


def _load_state() -> dict[str, Any]:
    if not _STATE_PATH.exists():
        return {"processed_event_ids": [], "line_quantities": {}}
    try:
        return json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {"processed_event_ids": [], "line_quantities": {}}


def _save_state(state: dict[str, Any]) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=0), encoding="utf-8")
    tmp.replace(_STATE_PATH)


def _trim_processed_ids(ids: list[str], max_keep: int = 2000) -> list[str]:
    if len(ids) > max_keep:
        return ids[-max_keep:]
    return ids


def _soql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _valid_sf_field_api_name(name: str) -> bool:
    return bool(name) and bool(re.match(r"^[A-Za-z][A-Za-z0-9_]*$", name))


def _sf_date_payload(d: date) -> str:
    """Date custom fields usually accept YYYY-MM-DD; DateTime fields need a full instant."""
    if (os.getenv("SF_OPP_DATE_FIELDS_USE_DATETIME", "") or "").strip() in ("1", "true", "True"):
        return f"{d.isoformat()}T00:00:00.000Z"
    return d.isoformat()


def _log_salesforce_failure(operation: str, exc: BaseException, payload: Any = None) -> None:
    raw = getattr(exc, "content", None)
    if isinstance(raw, bytes):
        try:
            detail = raw.decode("utf-8")
        except Exception:
            detail = repr(raw)
    else:
        detail = str(raw) if raw else ""
    status = getattr(exc, "status", None)
    keys = list(payload.keys()) if isinstance(payload, dict) else payload
    log.error("%s failed status=%s keys=%s detail=%s", operation, status, keys, detail[:8000])


def _invalid_field_names_from_salesforce(exc: BaseException) -> list[str]:
    """Parse INVALID_FIELD_FOR_INSERT_UPDATE field list from API error body."""
    raw = getattr(exc, "content", None)
    if not isinstance(raw, bytes):
        return []
    try:
        errs = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return []
    if not isinstance(errs, list):
        return []
    out: list[str] = []
    for err in errs:
        if err.get("errorCode") != "INVALID_FIELD_FOR_INSERT_UPDATE":
            continue
        out.extend(err.get("fields") or [])
    return out


def _event_sync_date(payload: dict[str, Any]) -> date:
    ts = payload.get("occurred_at")
    if ts is not None:
        try:
            return datetime.utcfromtimestamp(int(ts)).date()
        except (TypeError, ValueError, OSError):
            pass
    return datetime.utcnow().date()


def _parse_sf_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, str) and len(val) >= 10:
        try:
            return date.fromisoformat(val[:10])
        except ValueError:
            return None
    return None


def _months_between_start_and_end(start: date, end: date) -> int:
    """Whole calendar months from start month through end month (non-negative)."""
    if end < start:
        return 0
    return max(0, (end.year - start.year) * 12 + (end.month - start.month))


def _query_open_renewal_contract_end(sf: Salesforce, account_id: str) -> date | None:
    """Contract end date from the newest open Renewal Opportunity on this Account."""
    end_field = (os.getenv("SF_RENEWAL_CONTRACT_END_DATE_FIELD") or "").strip()
    if not end_field or not _valid_sf_field_api_name(end_field):
        return None
    rt_dev = (os.getenv("SF_RENEWAL_RECORD_TYPE_DEVELOPER_NAME") or "Renewal").strip()
    if not rt_dev or not _valid_sf_field_api_name(rt_dev):
        return None
    safe_acc = _soql_escape(account_id)
    q = (
        f"SELECT {end_field} FROM Opportunity WHERE AccountId = '{safe_acc}' "
        f"AND RecordType.DeveloperName = '{_soql_escape(rt_dev)}' AND IsClosed = false "
        "ORDER BY CloseDate DESC LIMIT 1"
    )
    try:
        res = sf.query(q)
    except Exception:
        log.exception("Renewal Opportunity SOQL failed")
        return None
    recs = res.get("records") or []
    if not recs:
        log.warning("No open Renewal Opportunity for Account %s (RecordType %s)", account_id, rt_dev)
        return None
    return _parse_sf_date(recs[0].get(end_field))


def _get_salesforce() -> Salesforce:
    username = (os.getenv("SF_USERNAME") or "").strip()
    password = (os.getenv("SF_PASSWORD") or "").strip()
    token = (os.getenv("SF_SECURITY_TOKEN") or "").strip()
    domain = (os.getenv("SF_DOMAIN") or "login").strip()
    if not username or not password:
        raise ValueError("Set SF_USERNAME and SF_PASSWORD in .env (and SF_SECURITY_TOKEN if required).")
    # simple_salesforce requires security_token as its own arg (use "" if login IP is trusted / no token).
    return Salesforce(
        username=username,
        password=password,
        security_token=token,
        domain=domain,
    )


def _resolve_account_id(sf: Salesforce, customer: dict[str, Any], customer_id: str) -> str | None:
    cb_field = (os.getenv("CHARGEBEE_CUSTOMER_SF_ACCOUNT_FIELD") or "").strip()
    if cb_field:
        raw = customer.get(cb_field)
        if raw and str(raw).strip():
            return str(raw).strip()

    acc_field = (os.getenv("SF_ACCOUNT_CHARGEBEE_CUSTOMER_FIELD") or "").strip()
    if not acc_field:
        log.warning(
            "No Account mapping: set CHARGEBEE_CUSTOMER_SF_ACCOUNT_FIELD (Chargebee CF API name) "
            "or SF_ACCOUNT_CHARGEBEE_CUSTOMER_FIELD (Account custom field matching customer id)."
        )
        return None

    safe = _soql_escape(customer_id)
    q = f"SELECT Id FROM Account WHERE {acc_field} = '{safe}' LIMIT 1"
    res = sf.query(q)
    recs = res.get("records") or []
    if not recs:
        log.warning("No Salesforce Account for Chargebee customer_id=%s", customer_id)
        return None
    return str(recs[0]["Id"])


def _unit_price_major(si: dict[str, Any], currency_code: str) -> float | None:
    dec = si.get("unit_price_in_decimal")
    if dec is not None and str(dec).strip() != "":
        try:
            return float(str(dec).strip())
        except ValueError:
            pass
    raw = si.get("unit_price")
    if raw is None:
        return None
    try:
        cents = int(raw)
    except (TypeError, ValueError):
        return None
    cur = (currency_code or "USD").upper()
    zero_dec = {
        "BIF", "BYR", "CLF", "CLP", "CVE", "DJF", "GNF", "ISK", "JPY", "KMF", "KRW",
        "MGA", "PYG", "RWF", "UGX", "VND", "VUV", "XAF", "XOF", "XPF",
    }
    if cur in zero_dec:
        return float(cents)
    return cents / 100.0


def _create_expansion_opportunity(
    sf: Salesforce,
    *,
    company: str,
    account_id: str,
    customer_id: str,
    subscription_id: str,
    event_id: str,
    total_seat_delta: int,
    description: str,
    amount: float | None,
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
) -> str:
    stage = (os.getenv("SF_OPPORTUNITY_STAGE") or "Prospecting").strip()
    use_event_close = (os.getenv("SF_OPPORTUNITY_CLOSE_DATE_USE_EVENT", "1") or "1").strip() not in (
        "0",
        "false",
        "False",
    )
    if use_event_close:
        close_d = sync_date
    else:
        close_days = int((os.getenv("SF_OPPORTUNITY_CLOSE_DAYS") or "30").strip() or "30")
        close_d = date.today() + timedelta(days=close_days)
    name = (os.getenv("SF_OPPORTUNITY_NAME_TEMPLATE") or "Expansion: {company} (+{delta} CRM self-serve seats)").format(
        company=company or customer_id,
        delta=total_seat_delta,
    )
    body: dict[str, Any] = {
        "Name": name[:120],
        "AccountId": account_id,
        "StageName": stage,
        "CloseDate": close_d.isoformat(),
        "Description": description,
    }
    rt = (os.getenv("SF_OPPORTUNITY_RECORD_TYPE_ID") or "").strip()
    if rt:
        body["RecordTypeId"] = rt
    if amount is not None:
        body["Amount"] = round(amount, 2)

    start_f = (os.getenv("SF_OPP_CONTRACT_START_DATE_FIELD") or "").strip()
    if start_f and _valid_sf_field_api_name(start_f):
        body[start_f] = _sf_date_payload(sync_date)
    end_f = (os.getenv("SF_OPP_CONTRACT_END_DATE_FIELD") or "").strip()
    if end_f and _valid_sf_field_api_name(end_f) and contract_end is not None:
        body[end_f] = _sf_date_payload(contract_end)
    term_f = (os.getenv("SF_OPP_TERM_MONTHS_FIELD") or "").strip()
    if term_f and _valid_sf_field_api_name(term_f) and term_months is not None:
        body[term_f] = float(term_months)
    # MRR is set after OpportunityLineItem rows (many orgs use formulas or validation tied to products).
    if (os.getenv("SF_OPP_MRR_SET_ON_CREATE", "") or "").strip() in ("1", "true", "True"):
        mrr_f = (os.getenv("SF_OPP_MRR_FIELD") or "").strip()
        if mrr_f and _valid_sf_field_api_name(mrr_f) and amount is not None:
            body[mrr_f] = round(float(amount), 2)

    last_exc: BaseException | None = None
    for _attempt in range(8):
        try:
            result = sf.Opportunity.create(body)
            oid = result.get("id")
            if not oid:
                raise RuntimeError(f"Salesforce Opportunity.create unexpected response: {result}")
            return str(oid)
        except Exception as exc:
            last_exc = exc
            if getattr(exc, "status", None) != 400:
                _log_salesforce_failure("Opportunity.create", exc, body)
                raise
            drop = _invalid_field_names_from_salesforce(exc)
            if not drop:
                _log_salesforce_failure("Opportunity.create", exc, body)
                raise
            removed = False
            for f in drop:
                if f in body:
                    body.pop(f)
                    removed = True
            if not removed:
                _log_salesforce_failure("Opportunity.create", exc, body)
                raise
            log.warning(
                "Retrying Opportunity.create without non-writable fields (grant FLS or map a writable field): %s",
                drop,
            )
    if last_exc:
        _log_salesforce_failure("Opportunity.create", last_exc, body)
        raise last_exc
    raise RuntimeError("Opportunity.create exhausted retries")


def _update_opportunity_mrr_after_products(
    sf: Salesforce, opp_id: str, amount: float | None
) -> None:
    if (os.getenv("SF_OPP_MRR_SET_ON_CREATE", "") or "").strip() in ("1", "true", "True"):
        return
    mrr_f = (os.getenv("SF_OPP_MRR_FIELD") or "").strip()
    if not mrr_f or not _valid_sf_field_api_name(mrr_f) or amount is None:
        return
    patch = {mrr_f: round(float(amount), 2)}
    try:
        sf.Opportunity.update(opp_id, patch)
        log.info("Set %s on Opportunity %s", mrr_f, opp_id)
    except Exception as exc:
        _log_salesforce_failure("Opportunity.update (MRR)", exc, patch)
        log.warning(
            "Could not set %s (formula/rollup, FLS, or validation). Opportunity %s still created.",
            mrr_f,
            opp_id,
        )


def _resolve_pricebook_entry(sf: Salesforce) -> tuple[str, str] | None:
    """Return (PricebookEntryId, Pricebook2Id) for OpportunityLineItem rows."""
    pbe_env = (os.getenv("SF_PRICEBOOK_ENTRY_ID") or "").strip()
    if pbe_env:
        res = sf.query(
            "SELECT Id, Pricebook2Id FROM PricebookEntry WHERE Id = "
            f"'{_soql_escape(pbe_env)}' LIMIT 1"
        )
        recs = res.get("records") or []
        if recs:
            return str(recs[0]["Id"]), str(recs[0]["Pricebook2Id"])
        log.warning("SF_PRICEBOOK_ENTRY_ID not found")
        return None

    prod2 = (os.getenv("SF_PRODUCT2_ID") or "").strip()
    if prod2:
        q = (
            "SELECT Id, Pricebook2Id FROM PricebookEntry WHERE Product2Id = "
            f"'{_soql_escape(prod2)}' AND Pricebook2.IsStandard = true AND IsActive = true LIMIT 1"
        )
    else:
        name = (os.getenv("SF_PRODUCT_NAME") or "Additional CRM Seats").strip()
        q = (
            "SELECT Id, Pricebook2Id FROM PricebookEntry WHERE Product2.Name = "
            f"'{_soql_escape(name)}' AND Pricebook2.IsStandard = true AND IsActive = true LIMIT 1"
        )
    res = sf.query(q)
    recs = res.get("records") or []
    if not recs:
        log.warning(
            "No standard PricebookEntry for expansion product — set SF_PRODUCT_NAME (default "
            "'Additional CRM Seats'), SF_PRODUCT2_ID, or SF_PRICEBOOK_ENTRY_ID"
        )
        return None
    return str(recs[0]["Id"]), str(recs[0]["Pricebook2Id"])


def _attach_opportunity_line_items(
    sf: Salesforce,
    opp_id: str,
    pending: list[tuple[str, int, int, str, float | None]],
    *,
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
) -> None:
    """One OpportunityLineItem per Chargebee line in pending (delta qty, UnitPrice from Chargebee)."""
    resolved = _resolve_pricebook_entry(sf)
    if not resolved:
        return
    pbe_id, pb2_id = resolved
    try:
        sf.Opportunity.update(opp_id, {"Pricebook2Id": pb2_id})
    except Exception:
        log.exception("Could not set Opportunity Pricebook2Id")
        return

    oli_start_f = (os.getenv("SF_OLI_START_DATE_FIELD") or "").strip()
    oli_end_f = (os.getenv("SF_OLI_END_DATE_FIELD") or "").strip()
    oli_term_f = (os.getenv("SF_OLI_TERM_MONTHS_FIELD") or "").strip()

    for _key, _new_qty, delta, ip_id, up in pending:
        if delta <= 0:
            continue
        body: dict[str, Any] = {
            "OpportunityId": opp_id,
            "PricebookEntryId": pbe_id,
            "Quantity": delta,
        }
        if up is not None:
            body["UnitPrice"] = round(up, 2)
        if oli_start_f and _valid_sf_field_api_name(oli_start_f):
            body[oli_start_f] = _sf_date_payload(sync_date)
        if oli_end_f and _valid_sf_field_api_name(oli_end_f) and contract_end is not None:
            body[oli_end_f] = _sf_date_payload(contract_end)
        if oli_term_f and _valid_sf_field_api_name(oli_term_f) and term_months is not None:
            body[oli_term_f] = float(term_months)
        try:
            sf.OpportunityLineItem.create(body)
            log.info("Added OpportunityLineItem opp=%s qty=%s unit_price=%s item_price_id=%s", opp_id, delta, up, ip_id)
        except Exception:
            log.exception("OpportunityLineItem create failed item_price_id=%s", ip_id)


def _check_basic_auth() -> bool:
    user = (os.getenv("WEBHOOK_BASIC_USER") or "").strip()
    password = (os.getenv("WEBHOOK_BASIC_PASS") or "").strip()
    if not user or not password:
        log.error("Set WEBHOOK_BASIC_USER and WEBHOOK_BASIC_PASS for webhook authentication.")
        return False
    auth = request.authorization
    return bool(auth and auth.username == user and auth.password == password)


def _handle_subscription_event(payload: dict[str, Any]) -> None:
    event_id = str(payload.get("id") or "")
    event_type = str(payload.get("event_type") or "")
    if not event_id:
        log.warning("Event missing id; skipping")
        return

    if event_type not in frozenset({"subscription_changed", "subscription_created"}):
        log.info("Ignoring event_type=%s", event_type)
        return

    sync_date = _event_sync_date(payload)

    content = payload.get("content") or {}
    sub = content.get("subscription") or {}
    customer = content.get("customer") or {}
    subscription_id = str(sub.get("id") or "")
    if not subscription_id:
        log.warning("subscription id missing")
        return

    customer_id = str(customer.get("id") or customer.get("customer_id") or "")
    company = str(customer.get("company") or "").strip()
    currency = str(sub.get("currency_code") or "USD")

    exact = _addon_exact_ids_from_env()
    items = sub.get("subscription_items") or sub.get("subscription_items_list") or []
    if not isinstance(items, list):
        items = []

    with _state_lock:
        state = _load_state()
        processed: list[str] = list(state.get("processed_event_ids") or [])
        if event_id in processed:
            log.info("Duplicate event %s; skipping", event_id)
            return

        lines: dict[str, int] = dict(state.get("line_quantities") or {})
        pending: list[tuple[str, int, int, str, float | None]] = []
        # pending: (line_key, new_qty, delta, item_price_id, unit_price_major)

        for si in items:
            if not isinstance(si, dict):
                continue
            ip_id = si.get("item_price_id")
            itype = si.get("item_type")
            if not _crm_addon_line_matches(
                str(ip_id) if ip_id else None,
                str(itype) if itype else None,
                exact,
            ):
                continue
            qty_raw = si.get("quantity")
            try:
                new_qty = int(qty_raw) if qty_raw is not None else 0
            except (TypeError, ValueError):
                new_qty = 0
            key = self_service_line_state_key(subscription_id, str(ip_id))
            stored = lines.get(key)
            old_qty = 0 if stored is None else stored
            if new_qty < old_qty:
                lines[key] = new_qty
                continue
            if new_qty == old_qty:
                continue

            delta = new_qty - old_qty
            up = _unit_price_major(si, currency)
            pending.append((key, new_qty, delta, str(ip_id), up))

        mark_processed = True
        if pending:
            sf = _get_salesforce()
            account_id = _resolve_account_id(sf, customer, customer_id)
            if account_id:
                total_delta = sum(p[2] for p in pending)
                amount: float | None = None
                parts: list[str] = [
                    f"Chargebee self-serve CRM seat increases (total +{total_delta} seats).",
                    f"customer_id={customer_id}",
                    f"subscription_id={subscription_id}",
                    f"chargebee_event_id={event_id}",
                    "",
                ]
                amt_sum = 0.0
                any_price = False
                for key, _new_qty, delta, ip_id, up in pending:
                    parts.append(f"item_price_id={ip_id}  +{delta} seats")
                    if up is not None:
                        any_price = True
                        amt_sum += up * delta
                if any_price:
                    amount = amt_sum
                desc = "\n".join(parts)
                contract_end = _query_open_renewal_contract_end(sf, account_id)
                term_months: int | None = None
                if contract_end is not None:
                    term_months = _months_between_start_and_end(sync_date, contract_end)
                oid = _create_expansion_opportunity(
                    sf,
                    company=company,
                    account_id=account_id,
                    customer_id=customer_id,
                    subscription_id=subscription_id,
                    event_id=event_id,
                    total_seat_delta=total_delta,
                    description=desc,
                    amount=amount,
                    sync_date=sync_date,
                    contract_end=contract_end,
                    term_months=term_months,
                )
                log.info("Created Opportunity %s total_delta=%s", oid, total_delta)
                _attach_opportunity_line_items(
                    sf,
                    oid,
                    pending,
                    sync_date=sync_date,
                    contract_end=contract_end,
                    term_months=term_months,
                )
                _update_opportunity_mrr_after_products(sf, oid, amount)
                for key, new_qty, _, _, _ in pending:
                    lines[key] = new_qty
            else:
                log.warning("Skipping Opportunity: no Salesforce Account for customer_id=%s", customer_id)
                mark_processed = False

        if mark_processed:
            processed.append(event_id)
        state["processed_event_ids"] = _trim_processed_ids(processed)
        state["line_quantities"] = lines
        _save_state(state)


@app.post("/chargebee")
def chargebee_webhook():
    if not _check_basic_auth():
        return "", 401
    if not request.is_json:
        return {"error": "expected application/json"}, 400
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return {"error": "invalid json"}, 400
    inner = payload.get("event")
    if isinstance(inner, dict):
        payload = inner
    try:
        _handle_subscription_event(payload)
    except Exception:
        log.exception("Handler failed for event %s", payload.get("id"))
        return {"error": "internal"}, 500
    return "", 200


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/admin/seed-state")
def admin_seed_state():
    """Merge Chargebee line quantities into webhook state (same as seed_webhook_state.py)."""
    secret = (os.getenv("WEBHOOK_SEED_SECRET") or "").strip()
    if not secret:
        return "", 404
    if (request.headers.get("X-Seed-Secret") or "").strip() != secret:
        return jsonify({"error": "unauthorized"}), 401
    try:
        with _state_lock:
            n = merge_chargebee_into_state_file(_STATE_PATH)
    except ValueError as e:
        log.warning("Seed failed: %s", e)
        return jsonify({"ok": False, "error": str(e)}), 500
    except Exception:
        log.exception("admin seed failed")
        return jsonify({"ok": False, "error": "internal"}), 500
    return jsonify({"ok": True, "seeded_lines": n}), 200


def main() -> None:
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG") == "1")


if __name__ == "__main__":
    main()
