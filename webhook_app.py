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

First time we see a subscription line we only record quantity (no Opportunity).
When quantity increases, we create one Opportunity per webhook (seat increases aggregated).
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, request
from simple_salesforce import Salesforce

from chargebee_client import _addon_exact_ids_from_env, _crm_addon_line_matches

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


def _line_key(sub_id: str, item_price_id: str) -> str:
    return f"{sub_id}|{item_price_id}"


def _soql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


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
) -> str:
    stage = (os.getenv("SF_OPPORTUNITY_STAGE") or "Prospecting").strip()
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

    result = sf.Opportunity.create(body)
    oid = result.get("id")
    if not oid:
        raise RuntimeError(f"Salesforce Opportunity.create unexpected response: {result}")
    return str(oid)


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
            key = _line_key(subscription_id, str(ip_id))
            old_qty = lines.get(key)
            if old_qty is None:
                lines[key] = new_qty
                log.info("Baseline quantity key=%s qty=%s", key, new_qty)
                continue
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
                )
                log.info("Created Opportunity %s total_delta=%s", oid, total_delta)
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


def main() -> None:
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=os.getenv("FLASK_DEBUG") == "1")


if __name__ == "__main__":
    main()
