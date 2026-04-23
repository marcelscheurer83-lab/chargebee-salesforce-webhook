"""
Chargebee webhook → Salesforce expansion Opportunity (minimal service).

Designed to run on a host with a public URL (Railway, Render, Fly.io, Google Cloud Run, etc.),
not on your laptop. Salesforce field API names and options can live in one JSON file
(`salesforce_field_map.json` or `SF_CONFIG_JSON`) so you do not need dozens of separate host vars;
secrets (passwords, API keys) should stay in the host env. See `.env.example`.

Chargebee: Settings → Configure Chargebee → Webhooks
  - URL: https://<your-host>/chargebee
  - Events: subscription_changed (optionally subscription_created to learn baselines)
  - HTTP Basic Auth: match WEBHOOK_BASIC_USER / WEBHOOK_BASIC_PASS

State file WEBHOOK_STATE_PATH defaults to ./webhook_state.json inside the container; use a volume
if you need quantities to survive redeploys (otherwise the next event re-baselines without an opp).

By default, after each successfully processed webhook we merge all active self-serve line quantities
from the Chargebee API into state (same data as seed_webhook_state / POST /admin/seed-state), so
baselines stay aligned without a manual seed. Disable with WEBHOOK_MERGE_CHARGEBEE_STATE_ON_WEBHOOK=0
if you prefer fewer API calls. Manual seed is still useful before go-live if state is empty: the
first increase is computed before this merge, so a cold start can still treat the full current qty
as delta unless you seeded or use a persistent volume with prior quantities.

Products: creates a Quote on the expansion Opportunity, adds QuoteLineItems (seat delta qty), then
enables Quote→Opportunity sync (IsSyncing + SyncedQuoteId) so products roll up on the Opp. Optional
SF_QUOTE_* revenue fields and SF_QUOTELINEITEM_STATIC_FIELDS_JSON are in .env.example. After each
successful webhook, line quantities are synced from the payload, then optionally merged from Chargebee.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from simple_salesforce import Salesforce

from chargebee_client import (
    _addon_exact_ids_from_env,
    _crm_addon_line_matches,
    self_service_line_state_key,
)
from seed_webhook_state import (
    merge_chargebee_into_state_file,
    merge_chargebee_line_quantities_into_dict,
)
from sf_config import init_sf_config, sf_cfg

load_dotenv(Path(__file__).resolve().parent / ".env")
init_sf_config()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("webhook_app")

app = Flask(__name__)

_state_lock = threading.Lock()
_STATE_PATH = Path(
    os.getenv("WEBHOOK_STATE_PATH", str(Path(__file__).resolve().parent / "webhook_state.json"))
)

log.info("Expansion pricing: Quote + QuoteLineItems (standard); quote lines use seat delta quantity")


def _merge_chargebee_state_after_webhook_enabled() -> bool:
    raw = (os.getenv("WEBHOOK_MERGE_CHARGEBEE_STATE_ON_WEBHOOK") or "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _sync_self_service_quantities_from_subscription(
    items: list[Any],
    lines: dict[str, int],
    subscription_id: str,
    exact: frozenset[str] | None,
) -> None:
    """Set stored qty for every self-serve line in this subscription payload (Chargebee = source of truth)."""
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
        lines[key] = new_qty


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
    if (sf_cfg("SF_OPP_DATE_FIELDS_USE_DATETIME") or "").strip().lower() in ("1", "true", "yes"):
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


def _salesforce_error_entries(exc: BaseException) -> list[dict[str, Any]]:
    """Normalize simple_salesforce exception body (bytes, str JSON, or pre-parsed list)."""
    raw = getattr(exc, "content", None)
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, bytes):
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return []
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
        if isinstance(parsed, dict):
            return [parsed]
        return []
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
        if isinstance(parsed, dict):
            return [parsed]
    return []


def _invalid_field_names_from_salesforce(exc: BaseException) -> list[str]:
    """Parse INVALID_FIELD_FOR_INSERT_UPDATE field list from API error body."""
    out: list[str] = []
    for err in _salesforce_error_entries(exc):
        if err.get("errorCode") != "INVALID_FIELD_FOR_INSERT_UPDATE":
            continue
        out.extend(err.get("fields") or [])
    return out


_NO_SUCH_COLUMN_RE = re.compile(r"No such column '([^']+)'", re.IGNORECASE)


def _fields_to_drop_from_salesforce_400(exc: BaseException, body: dict[str, Any]) -> list[str]:
    """Field API names to remove and retry: FLS errors, missing columns, bad picklist values, etc."""
    _PICKLIST_AND_FIELD_ERRORS = frozenset(
        {
            "INVALID_FIELD_FOR_INSERT_UPDATE",
            "INVALID_OR_NULL_FOR_RESTRICTED_PICKLIST",
            "DUPLICATE_VALUE",  # rarely has fields; harmless if empty
        }
    )
    found: list[str] = []
    for err in _salesforce_error_entries(exc):
        code = err.get("errorCode") or ""
        if code in _PICKLIST_AND_FIELD_ERRORS:
            found.extend(str(f) for f in (err.get("fields") or []) if f)
        elif code == "INVALID_FIELD":
            msg = str(err.get("message") or "")
            m = _NO_SUCH_COLUMN_RE.search(msg)
            if m:
                found.append(m.group(1))
    out: list[str] = []
    seen: set[str] = set()
    for f in found:
        if f in body and f not in seen:
            seen.add(f)
            out.append(f)
    return out


def _event_timezone() -> ZoneInfo:
    """Default America/New_York (Miami / Eastern). Override with SF_EVENT_TIMEZONE."""
    name = (sf_cfg("SF_EVENT_TIMEZONE", "America/New_York") or "America/New_York").strip()
    try:
        return ZoneInfo(name)
    except Exception:
        log.warning("Invalid SF_EVENT_TIMEZONE %r; using America/New_York", name)
        return ZoneInfo("America/New_York")


def _event_sync_date(payload: dict[str, Any]) -> date:
    """Event calendar date in SF_EVENT_TIMEZONE (default Eastern / Miami), not UTC."""
    tz = _event_timezone()
    ts = payload.get("occurred_at")
    if ts is not None:
        try:
            dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
            return dt_utc.astimezone(tz).date()
        except (TypeError, ValueError, OSError):
            pass
    return datetime.now(tz).date()


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


def _opportunity_mrr_field_updates(amount: float | None) -> dict[str, float]:
    """Delta-based MRR (Chargebee unit price × seat delta) for mapped Opportunity fields."""
    if amount is None:
        return {}
    amt = round(float(amount), 2)
    patch: dict[str, float] = {}
    mrr_f = sf_cfg("SF_OPP_MRR_FIELD")
    if mrr_f and _valid_sf_field_api_name(mrr_f):
        patch[mrr_f] = amt
    exp_f = sf_cfg("SF_OPP_EXPANSION_MRR_FIELD")
    if exp_f and _valid_sf_field_api_name(exp_f):
        patch[exp_f] = amt
    return patch


def _query_expansion_contract_end_from_renewal(sf: Salesforce, account_id: str) -> date | None:
    """
    Value for the expansion Opportunity's contract end date field.

    Default: one day before Contract Start on the newest open Renewal Opportunity on this Account.
    If SF_RENEWAL_CONTRACT_START_DATE_FIELD is unset but SF_RENEWAL_CONTRACT_END_DATE_FIELD is set,
    falls back to that renewal end date (legacy).
    """
    start_field = sf_cfg("SF_RENEWAL_CONTRACT_START_DATE_FIELD")
    end_field = sf_cfg("SF_RENEWAL_CONTRACT_END_DATE_FIELD")
    use_start = bool(start_field and _valid_sf_field_api_name(start_field))
    use_end = bool(not use_start and end_field and _valid_sf_field_api_name(end_field))
    if not use_start and not use_end:
        return None
    select_field = start_field if use_start else end_field
    rt_dev = sf_cfg("SF_RENEWAL_RECORD_TYPE_DEVELOPER_NAME", "Renewal")
    if not rt_dev or not _valid_sf_field_api_name(rt_dev):
        return None
    safe_acc = _soql_escape(account_id)
    q = (
        f"SELECT {select_field} FROM Opportunity WHERE AccountId = '{safe_acc}' "
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
    raw = _parse_sf_date(recs[0].get(select_field))
    if raw is None:
        log.warning(
            "Open Renewal for Account %s missing or invalid %s",
            account_id,
            select_field,
        )
        return None
    if use_start:
        return raw - timedelta(days=1)
    return raw


def _get_salesforce() -> Salesforce:
    username = sf_cfg("SF_USERNAME")
    password = sf_cfg("SF_PASSWORD")
    token = sf_cfg("SF_SECURITY_TOKEN")
    domain = sf_cfg("SF_DOMAIN", "login")
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
    cb_field = sf_cfg("CHARGEBEE_CUSTOMER_SF_ACCOUNT_FIELD")
    if cb_field:
        raw = customer.get(cb_field)
        if raw and str(raw).strip():
            return str(raw).strip()

    acc_field = sf_cfg("SF_ACCOUNT_CHARGEBEE_CUSTOMER_FIELD")
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
    stage = sf_cfg("SF_OPPORTUNITY_STAGE", "Prospecting")
    use_event_close = (sf_cfg("SF_OPPORTUNITY_CLOSE_DATE_USE_EVENT", "1") or "1").strip().lower() not in (
        "0",
        "false",
    )
    if use_event_close:
        close_d = sync_date
    else:
        close_days = int((sf_cfg("SF_OPPORTUNITY_CLOSE_DAYS", "30") or "30").strip() or "30")
        close_d = datetime.now(_event_timezone()).date() + timedelta(days=close_days)
    name = (sf_cfg("SF_OPPORTUNITY_NAME_TEMPLATE", "Expansion: {company} (+{delta} CRM self-serve seats)")).format(
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
    rt = sf_cfg("SF_OPPORTUNITY_RECORD_TYPE_ID")
    if rt:
        body["RecordTypeId"] = rt
    if amount is not None:
        body["Amount"] = round(amount, 2)

    start_f = sf_cfg("SF_OPP_CONTRACT_START_DATE_FIELD")
    if start_f and _valid_sf_field_api_name(start_f):
        body[start_f] = _sf_date_payload(sync_date)
    end_f = sf_cfg("SF_OPP_CONTRACT_END_DATE_FIELD")
    if end_f and _valid_sf_field_api_name(end_f) and contract_end is not None:
        body[end_f] = _sf_date_payload(contract_end)
    term_f = sf_cfg("SF_OPP_TERM_MONTHS_FIELD")
    if term_f and _valid_sf_field_api_name(term_f) and term_months is not None:
        body[term_f] = float(term_months)
    # MRR is set after QuoteLineItem rows (many orgs use formulas tied to products).
    if (sf_cfg("SF_OPP_MRR_SET_ON_CREATE") or "").strip().lower() in ("1", "true", "yes"):
        body.update(_opportunity_mrr_field_updates(amount))

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
            status = getattr(exc, "status", None)
            if status is None:
                _log_salesforce_failure("Opportunity.create", exc, body)
                raise
            try:
                code = int(status)
            except (TypeError, ValueError):
                _log_salesforce_failure("Opportunity.create", exc, body)
                raise
            if code != 400:
                _log_salesforce_failure("Opportunity.create", exc, body)
                raise
            drop = _fields_to_drop_from_salesforce_400(exc, body)
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
    if (sf_cfg("SF_OPP_MRR_SET_ON_CREATE") or "").strip().lower() in ("1", "true", "yes"):
        return
    patch = _opportunity_mrr_field_updates(amount)
    if not patch:
        return
    last_exc: BaseException | None = None
    for _attempt in range(8):
        try:
            sf.Opportunity.update(opp_id, patch)
            log.info("Set MRR field(s) on Opportunity %s: %s", opp_id, sorted(patch.keys()))
            return
        except Exception as exc:
            last_exc = exc
            status = getattr(exc, "status", None)
            if status is None:
                break
            try:
                code = int(status)
            except (TypeError, ValueError):
                break
            if code != 400:
                break
            drop = _fields_to_drop_from_salesforce_400(exc, patch)
            if not drop:
                break
            removed = False
            for f in drop:
                if f in patch:
                    patch.pop(f)
                    removed = True
            if not removed or not patch:
                break
            log.warning(
                "Retrying Opportunity.update (MRR) without bad fields: %s",
                drop,
            )
    if last_exc:
        if patch:
            _log_salesforce_failure("Opportunity.update (MRR)", last_exc, patch)
            log.warning(
                "Could not set MRR field(s) (formula/rollup, FLS, or validation). Opportunity %s still created.",
                opp_id,
            )
        else:
            log.info(
                "Skipped MRR update on Opportunity %s (all mapped fields were invalid or non-writable).",
                opp_id,
            )


def _resolve_pricebook_entry(sf: Salesforce) -> tuple[str, str] | None:
    """Return (PricebookEntryId, Pricebook2Id) for quote / product lines."""
    pbe_env = sf_cfg("SF_PRICEBOOK_ENTRY_ID")
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

    prod2 = sf_cfg("SF_PRODUCT2_ID")
    if prod2:
        q = (
            "SELECT Id, Pricebook2Id FROM PricebookEntry WHERE Product2Id = "
            f"'{_soql_escape(prod2)}' AND Pricebook2.IsStandard = true AND IsActive = true LIMIT 1"
        )
    else:
        name = (sf_cfg("SF_PRODUCT_NAME", "Additional CRM Seats") or "Additional CRM Seats").strip()
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


def _ensure_opportunity_pricebook(sf: Salesforce, opp_id: str, pb2_id: str) -> bool:
    try:
        sf.Opportunity.update(opp_id, {"Pricebook2Id": pb2_id})
        return True
    except Exception:
        log.exception("Could not set Opportunity Pricebook2Id")
        return False


def _product_line_custom_fields(
    *,
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
) -> dict[str, Any]:
    """Optional custom fields for QuoteLineItem (SF_OLI_* env names kept for backward compatibility)."""
    out: dict[str, Any] = {}
    start_f = sf_cfg("SF_OLI_START_DATE_FIELD")
    end_f = sf_cfg("SF_OLI_END_DATE_FIELD")
    term_f = sf_cfg("SF_OLI_TERM_MONTHS_FIELD")
    if start_f and _valid_sf_field_api_name(start_f):
        out[start_f] = _sf_date_payload(sync_date)
    if end_f and _valid_sf_field_api_name(end_f) and contract_end is not None:
        out[end_f] = _sf_date_payload(contract_end)
    if term_f and _valid_sf_field_api_name(term_f) and term_months is not None:
        out[term_f] = float(term_months)
    return out


def _sf_quote_autorenew_payload() -> Any | None:
    """
    Auto Renew is usually a restricted picklist (API values Yes / No).
    SF_CONFIG_JSON booleans become the strings \"true\"/\"false\" in sf_cfg — those are not valid
    picklist tokens unless the org defines them; we map them to Yes/No unless checkbox mode is on.

    Set SF_QUOTE_AUTORENEW_AS_BOOLEAN=1 only if the field is a real checkbox (not a picklist).
    Do not set SF_QUOTE_AUTORENEW_AS_BOOLEAN to JSON true unless you mean checkbox mode.
    """
    raw = sf_cfg("SF_QUOTE_AUTORENEW_VALUE")
    if not raw:
        return None
    as_checkbox = (sf_cfg("SF_QUOTE_AUTORENEW_AS_BOOLEAN") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    low = raw.lower()
    if as_checkbox:
        if low in ("1", "true", "yes"):
            return True
        if low in ("0", "false", "no"):
            return False
        return raw.strip()

    if low in ("1", "true", "yes"):
        return "Yes"
    if low in ("0", "false", "no"):
        return "No"
    return raw.strip()


def _merge_quote_header_custom_fields(
    body: dict[str, Any],
    *,
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
    amount: float | None,
) -> None:
    """Map org-specific Quote fields (contract dates, term, autorenew, MRR/ARR from seat-delta total)."""
    start_f = sf_cfg("SF_QUOTE_CONTRACT_START_DATE_FIELD")
    if start_f and _valid_sf_field_api_name(start_f):
        body[start_f] = _sf_date_payload(sync_date)
    end_f = sf_cfg("SF_QUOTE_CONTRACT_END_DATE_FIELD")
    if end_f and _valid_sf_field_api_name(end_f) and contract_end is not None:
        body[end_f] = _sf_date_payload(contract_end)
    term_f = sf_cfg("SF_QUOTE_TERM_MONTHS_FIELD")
    if term_f and _valid_sf_field_api_name(term_f) and term_months is not None:
        body[term_f] = float(term_months)
    mirror = (sf_cfg("SF_QUOTE_HEADER_MIRROR_LINE_DATE_FIELDS") or "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if mirror:
        if not start_f:
            oli_s = sf_cfg("SF_OLI_START_DATE_FIELD")
            if oli_s and _valid_sf_field_api_name(oli_s):
                body[oli_s] = _sf_date_payload(sync_date)
        if not end_f and contract_end is not None:
            oli_e = sf_cfg("SF_OLI_END_DATE_FIELD")
            if oli_e and _valid_sf_field_api_name(oli_e):
                body[oli_e] = _sf_date_payload(contract_end)
        if not term_f and term_months is not None:
            oli_t = sf_cfg("SF_OLI_TERM_MONTHS_FIELD")
            if oli_t and _valid_sf_field_api_name(oli_t):
                body[oli_t] = float(term_months)
    ar_f = sf_cfg("SF_QUOTE_AUTORENEW_FIELD")
    if ar_f and _valid_sf_field_api_name(ar_f):
        ar_v = _sf_quote_autorenew_payload()
        if ar_v is not None:
            body[ar_f] = ar_v
    _merge_quote_financial_header_fields(body, amount)


def _log_quote_financial_fls_hint(exc: BaseException) -> None:
    """If Salesforce returns FLS/read-only errors, tell admins which Quote fields to open up."""
    for err in _salesforce_error_entries(exc):
        code = err.get("errorCode") or ""
        msg = str(err.get("message") or "").lower()
        if code != "INVALID_FIELD_FOR_INSERT_UPDATE" and "security settings" not in msg:
            continue
        names = [
            sf_cfg("SF_QUOTE_MRR_FIELD"),
            sf_cfg("SF_QUOTE_ARR_FIELD"),
            sf_cfg("SF_QUOTE_ONETIME_FEES_FIELD"),
        ]
        names = [n for n in names if n and _valid_sf_field_api_name(n)]
        if names:
            log.warning(
                "Grant the Salesforce integration user Edit (field-level security) on Quote for: %s. "
                "Setup → Permission Sets (or Profile) → Object Settings → Quote → Field Permissions.",
                ", ".join(names),
            )
        return


def _merge_quote_financial_header_fields(body: dict[str, Any], amount: float | None) -> None:
    """
    Quote header: MRR/ARR from delta seat revenue (Chargebee unit price × delta qty).
    Optional SF_QUOTE_ONETIME_FEES_FIELD set to 0 when the org stores one-time total there
    (often a manual or rollup field; formula/read-only fields are dropped on retry).
    """
    if amount is not None:
        mrr_f = sf_cfg("SF_QUOTE_MRR_FIELD")
        if mrr_f and _valid_sf_field_api_name(mrr_f):
            body[mrr_f] = round(float(amount), 2)
        arr_f = sf_cfg("SF_QUOTE_ARR_FIELD")
        if arr_f and _valid_sf_field_api_name(arr_f):
            body[arr_f] = round(float(amount) * 12.0, 2)
    otf = sf_cfg("SF_QUOTE_ONETIME_FEES_FIELD")
    if otf and _valid_sf_field_api_name(otf):
        body[otf] = 0.0


def _quotelineitem_extra_fields_from_config() -> dict[str, Any]:
    """
    Optional JSON object merged into each QuoteLineItem create (e.g. charge type / recurring picklist).
    SF_QUOTELINEITEM_STATIC_FIELDS_JSON='{"Some_Field__c":"Recurring"}' — keys must be valid API names.
    """
    raw = (sf_cfg("SF_QUOTELINEITEM_STATIC_FIELDS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("SF_QUOTELINEITEM_STATIC_FIELDS_JSON is not valid JSON; ignored")
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in data.items():
        if isinstance(k, str) and _valid_sf_field_api_name(k):
            out[k] = v
    return out


def _patch_quote_header_after_create(
    sf: Salesforce,
    quote_id: str,
    *,
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
    amount: float | None,
) -> None:
    """
    Quote.create (especially minimal fallback) may omit custom header fields.
    Patch contract dates / term / MRR onto the Quote with the same retry logic as create.
    """
    patch: dict[str, Any] = {}
    _merge_quote_header_custom_fields(
        patch,
        sync_date=sync_date,
        contract_end=contract_end,
        term_months=term_months,
        amount=amount,
    )
    if not patch:
        return
    last_exc: BaseException | None = None
    for _attempt in range(8):
        try:
            sf.Quote.update(quote_id, patch)
            log.info("Patched Quote header fields on %s: %s", quote_id, sorted(patch.keys()))
            return
        except Exception as exc:
            last_exc = exc
            status = getattr(exc, "status", None)
            if status is None:
                break
            try:
                code = int(status)
            except (TypeError, ValueError):
                break
            if code != 400:
                break
            drop = _fields_to_drop_from_salesforce_400(exc, patch)
            if not drop:
                break
            removed = False
            for f in drop:
                if f in patch:
                    patch.pop(f)
                    removed = True
            if not removed or not patch:
                break
            log.warning("Retrying Quote.update (header) without bad fields: %s", drop)
    if last_exc:
        log.warning(
            "Quote header patch incomplete for %s (wrong API names, FLS, or formula fields). Last error: %s",
            quote_id,
            last_exc,
        )


def _patch_quote_financials_after_line_items(
    sf: Salesforce,
    quote_id: str,
    *,
    amount: float | None,
) -> None:
    """
    Re-apply MRR / ARR / one-time fees on the Quote after QuoteLineItems exist so manual fields
    stick and rollups that depend on lines have run. Skipped if no financial keys are configured.
    """
    patch: dict[str, Any] = {}
    _merge_quote_financial_header_fields(patch, amount)
    if not patch:
        return
    last_exc: BaseException | None = None
    for _attempt in range(8):
        try:
            sf.Quote.update(quote_id, patch)
            log.info("Patched Quote financial fields on %s: %s", quote_id, sorted(patch.keys()))
            return
        except Exception as exc:
            last_exc = exc
            status = getattr(exc, "status", None)
            if status is None:
                break
            try:
                code = int(status)
            except (TypeError, ValueError):
                break
            if code != 400:
                break
            drop = _fields_to_drop_from_salesforce_400(exc, patch)
            if not drop:
                break
            removed = False
            for f in drop:
                if f in patch:
                    patch.pop(f)
                    removed = True
            if not removed or not patch:
                break
            log.warning("Retrying Quote.update (financials) without bad fields: %s", drop)
    if last_exc:
        _log_quote_financial_fls_hint(last_exc)
        log.warning(
            "Quote financial patch incomplete for %s (FLS, formula/rollup read-only, or wrong API names). Last error: %s",
            quote_id,
            last_exc,
        )


def _try_set_quote_syncing(sf: Salesforce, opp_id: str, quote_id: str) -> None:
    """
    Standard Salesforce Quote: turn on sync so QuoteLineItems mirror to OpportunityLineItem on this Opp.
    Then set Opportunity.SyncedQuoteId to this quote when allowed (UI + rollups treat it as the synced quote).

    Custom checkbox/picklist instead of IsSyncing: SF_QUOTE_SYNCING_FIELD + SF_QUOTE_SYNCING_VALUE.
    CPQ (SBQQ__Quote__c) uses a different model — this targets standard Quote + Opportunity only.

    Disable Opportunity.SyncedQuoteId patch with SF_QUOTE_SET_OPPORTUNITY_SYNCED_QUOTE_ID=0 if your org rejects it.
    """
    if (sf_cfg("SF_QUOTE_SYNC_TO_OPPORTUNITY", "1") or "").strip().lower() in ("0", "false", "no"):
        return
    alt = sf_cfg("SF_QUOTE_SYNCING_FIELD")
    if alt and _valid_sf_field_api_name(alt):
        vraw = sf_cfg("SF_QUOTE_SYNCING_VALUE", "true")
        low = vraw.lower()
        if low in ("1", "true", "yes"):
            val: Any = True
        elif low in ("0", "false", "no"):
            val = False
        else:
            val = vraw
        q_patch: dict[str, Any] = {alt: val}
    else:
        q_patch = {"IsSyncing": True}

    quote_sync_ok = False
    last_q: BaseException | None = None
    for _attempt in range(8):
        try:
            sf.Quote.update(quote_id, q_patch)
            log.info(
                "Quote → Opportunity sync enabled on Quote %s (keys=%s)",
                quote_id,
                list(q_patch.keys()),
            )
            quote_sync_ok = True
            break
        except Exception as exc:
            last_q = exc
            status = getattr(exc, "status", None)
            if status is None:
                break
            try:
                code = int(status)
            except (TypeError, ValueError):
                break
            if code != 400:
                break
            drop = _fields_to_drop_from_salesforce_400(exc, q_patch)
            if not drop:
                break
            removed = False
            for f in drop:
                if f in q_patch:
                    q_patch.pop(f)
                    removed = True
            if not removed or not q_patch:
                break
            log.warning("Retrying Quote.update (sync flag) without bad fields: %s", drop)

    if not quote_sync_ok:
        if last_q:
            log.warning("Could not set quote sync flag on %s: %s", quote_id, last_q)
        return

    if (sf_cfg("SF_QUOTE_SET_OPPORTUNITY_SYNCED_QUOTE_ID", "1") or "").strip().lower() in (
        "0",
        "false",
        "no",
    ):
        return

    o_patch: dict[str, Any] = {"SyncedQuoteId": quote_id}
    last_o: BaseException | None = None
    for _attempt in range(8):
        try:
            sf.Opportunity.update(opp_id, o_patch)
            log.info("Opportunity %s SyncedQuoteId set to Quote %s", opp_id, quote_id)
            return
        except Exception as exc:
            last_o = exc
            status = getattr(exc, "status", None)
            if status is None:
                break
            try:
                code = int(status)
            except (TypeError, ValueError):
                break
            if code != 400:
                break
            drop = _fields_to_drop_from_salesforce_400(exc, o_patch)
            if not drop:
                break
            removed = False
            for f in drop:
                if f in o_patch:
                    o_patch.pop(f)
                    removed = True
            if not removed or not o_patch:
                break
            log.warning("Retrying Opportunity.update (SyncedQuoteId) without bad fields: %s", drop)

    if last_o:
        log.warning(
            "Could not set Opportunity.SyncedQuoteId (Quote.IsSyncing may still drive line sync). opp=%s err=%s",
            opp_id,
            last_o,
        )


def _quote_expiration_date_iso() -> str:
    exp_raw = (sf_cfg("SF_QUOTE_EXPIRATION_DAYS", "30") or "30").strip() or "30"
    try:
        exp_days = max(1, int(exp_raw))
    except ValueError:
        exp_days = 30
    return (datetime.now(_event_timezone()).date() + timedelta(days=exp_days)).isoformat()


def _create_expansion_quote_minimal(
    sf: Salesforce,
    opp_id: str,
    *,
    name: str,
    pb2_id: str,
) -> str | None:
    """Standard Quote fields only (no custom header map). Use when full create keeps failing."""
    body: dict[str, Any] = {
        "OpportunityId": opp_id,
        "Name": name[:255],
        "Pricebook2Id": pb2_id,
        "ExpirationDate": _quote_expiration_date_iso(),
    }
    st = sf_cfg("SF_QUOTE_STATUS", "Draft")
    if st:
        body["Status"] = st
    qc = sf_cfg("SF_QUOTE_CONTACT_ID")
    if qc:
        body["ContactId"] = qc
    attempts: list[dict[str, Any]] = [body]
    if "Status" in body:
        lean = {k: v for k, v in body.items() if k != "Status"}
        attempts.append(lean)
    for variant in attempts:
        try:
            result = sf.Quote.create(variant)
            qid = result.get("id")
            if not qid:
                raise RuntimeError(f"Salesforce Quote.create unexpected response: {result}")
            log.warning(
                "Created Quote %s with minimal fields only (custom quote field map skipped). "
                "Add formulas/flow or fix SF_CONFIG_JSON for Contract/MRR/ARR on Quote.",
                qid,
            )
            return str(qid)
        except Exception as exc:
            last_exc = exc
            if variant is attempts[-1]:
                _log_salesforce_failure("Quote.create (minimal fallback)", exc, variant)
            else:
                log.warning("Quote minimal create failed with Status; retrying without Status: %s", exc)
    return None


def _create_expansion_quote(
    sf: Salesforce,
    opp_id: str,
    *,
    company: str,
    total_seat_delta: int,
    pb2_id: str,
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
    amount: float | None,
) -> str | None:
    tmpl = sf_cfg("SF_QUOTE_NAME_TEMPLATE", "Expansion quote (+{delta} CRM self-serve seats)")
    try:
        name = tmpl.format(delta=total_seat_delta, company=company or "")[:255]
    except (KeyError, ValueError, IndexError):
        name = tmpl[:255]
    body: dict[str, Any] = {
        "OpportunityId": opp_id,
        "Name": name,
        "Pricebook2Id": pb2_id,
        "Status": sf_cfg("SF_QUOTE_STATUS", "Draft"),
    }
    qc = sf_cfg("SF_QUOTE_CONTACT_ID")
    if qc:
        body["ContactId"] = qc
    body["ExpirationDate"] = _quote_expiration_date_iso()
    _merge_quote_header_custom_fields(
        body,
        sync_date=sync_date,
        contract_end=contract_end,
        term_months=term_months,
        amount=amount,
    )

    last_exc: BaseException | None = None
    for _attempt in range(8):
        try:
            result = sf.Quote.create(body)
            qid = result.get("id")
            if not qid:
                raise RuntimeError(f"Salesforce Quote.create unexpected response: {result}")
            log.info("Created Quote %s on Opportunity %s", qid, opp_id)
            return str(qid)
        except Exception as exc:
            last_exc = exc
            status = getattr(exc, "status", None)
            if status is None:
                _log_salesforce_failure("Quote.create", exc, body)
                return _create_expansion_quote_minimal(sf, opp_id, name=name, pb2_id=pb2_id)
            try:
                code = int(status)
            except (TypeError, ValueError):
                _log_salesforce_failure("Quote.create", exc, body)
                return _create_expansion_quote_minimal(sf, opp_id, name=name, pb2_id=pb2_id)
            if code != 400:
                _log_salesforce_failure("Quote.create", exc, body)
                return _create_expansion_quote_minimal(sf, opp_id, name=name, pb2_id=pb2_id)
            drop = _fields_to_drop_from_salesforce_400(exc, body)
            if not drop:
                _log_salesforce_failure("Quote.create", exc, body)
                break
            removed = False
            for f in drop:
                if f in body:
                    body.pop(f)
                    removed = True
            if not removed:
                _log_salesforce_failure("Quote.create", exc, body)
                break
            log.warning(
                "Retrying Quote.create without bad/missing fields: %s",
                drop,
            )
    if last_exc:
        _log_salesforce_failure("Quote.create (full field set exhausted)", last_exc, body)
    return _create_expansion_quote_minimal(sf, opp_id, name=name, pb2_id=pb2_id)


def _add_quote_line_items(
    sf: Salesforce,
    quote_id: str,
    pbe_id: str,
    pending: list[tuple[str, int, int, str, float | None]],
    *,
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
) -> None:
    """One QuoteLineItem per Chargebee line; Quantity = seat delta for this event."""
    extras = _product_line_custom_fields(
        sync_date=sync_date, contract_end=contract_end, term_months=term_months
    )
    qli_static = _quotelineitem_extra_fields_from_config()
    for _key, _new_qty, delta, ip_id, up in pending:
        if delta <= 0:
            continue
        qli_qty = float(delta)
        body: dict[str, Any] = {
            "QuoteId": quote_id,
            "PricebookEntryId": pbe_id,
            "Quantity": qli_qty,
            **qli_static,
            **extras,
        }
        if up is not None:
            body["UnitPrice"] = round(up, 2)
            line_mrr = round(float(up) * float(delta), 2)
            lm = sf_cfg("SF_QTI_MRR_FIELD")
            if lm and _valid_sf_field_api_name(lm):
                body[lm] = line_mrr
            la = sf_cfg("SF_QTI_ARR_FIELD")
            if la and _valid_sf_field_api_name(la):
                body[la] = round(line_mrr * 12.0, 2)
        li_ok = False
        last_li: BaseException | None = None
        for _attempt in range(8):
            try:
                sf.QuoteLineItem.create(body)
                log.info(
                    "Added QuoteLineItem quote=%s qty=%s (delta) unit_price=%s item_price_id=%s",
                    quote_id,
                    qli_qty,
                    up,
                    ip_id,
                )
                li_ok = True
                break
            except Exception as exc:
                last_li = exc
                status = getattr(exc, "status", None)
                if status is None:
                    break
                try:
                    code = int(status)
                except (TypeError, ValueError):
                    break
                if code != 400:
                    break
                drop = _fields_to_drop_from_salesforce_400(exc, body)
                if not drop:
                    break
                removed = False
                for f in drop:
                    if f in body:
                        body.pop(f)
                        removed = True
                if not removed:
                    break
                log.warning(
                    "Retrying QuoteLineItem without bad/missing fields %s item_price_id=%s",
                    drop,
                    ip_id,
                )
        if not li_ok and last_li:
            log.exception("QuoteLineItem create failed item_price_id=%s", ip_id)


def _attach_expansion_quote_and_lines(
    sf: Salesforce,
    opp_id: str,
    *,
    company: str,
    total_seat_delta: int,
    pending: list[tuple[str, int, int, str, float | None]],
    sync_date: date,
    contract_end: date | None,
    term_months: int | None,
    amount: float | None,
) -> None:
    """Create Quote on Opportunity and add QuoteLineItems (seat deltas)."""
    resolved = _resolve_pricebook_entry(sf)
    if not resolved:
        log.error(
            "Cannot create Quote: no PricebookEntry resolved (check SF_PRODUCT_NAME / "
            "SF_PRODUCT2_ID / SF_PRICEBOOK_ENTRY_ID in SF_CONFIG_JSON). opp=%s",
            opp_id,
        )
        return
    pbe_id, pb2_id = resolved
    if not _ensure_opportunity_pricebook(sf, opp_id, pb2_id):
        log.error("Cannot create Quote: failed to set Opportunity Pricebook2Id=%s opp=%s", pb2_id, opp_id)
        return
    qid = _create_expansion_quote(
        sf,
        opp_id,
        company=company,
        total_seat_delta=total_seat_delta,
        pb2_id=pb2_id,
        sync_date=sync_date,
        contract_end=contract_end,
        term_months=term_months,
        amount=amount,
    )
    if not qid:
        log.error(
            "Quote was not created after full + minimal attempts. opp=%s — search Salesforce "
            "for standard Quote (not only CPQ SBQQ quotes) or check Quote object permissions.",
            opp_id,
        )
        return
    _patch_quote_header_after_create(
        sf,
        qid,
        sync_date=sync_date,
        contract_end=contract_end,
        term_months=term_months,
        amount=amount,
    )
    _add_quote_line_items(
        sf,
        qid,
        pbe_id,
        pending,
        sync_date=sync_date,
        contract_end=contract_end,
        term_months=term_months,
    )
    _patch_quote_financials_after_line_items(sf, qid, amount=amount)
    _try_set_quote_syncing(sf, opp_id, qid)


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
            if new_qty <= old_qty:
                continue

            delta = new_qty - old_qty
            up = _unit_price_major(si, currency)
            log.info(
                "Self-serve line item_price_id=%s stored_qty=%s chargebee_qty=%s delta_qty=%s",
                ip_id,
                old_qty,
                new_qty,
                delta,
            )
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
                contract_end = _query_expansion_contract_end_from_renewal(sf, account_id)
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
                _attach_expansion_quote_and_lines(
                    sf,
                    oid,
                    company=company or customer_id,
                    total_seat_delta=total_delta,
                    pending=pending,
                    sync_date=sync_date,
                    contract_end=contract_end,
                    term_months=term_months,
                    amount=amount,
                )
                _update_opportunity_mrr_after_products(sf, oid, amount)
            else:
                log.warning("Skipping Opportunity: no Salesforce Account for customer_id=%s", customer_id)
                mark_processed = False

        if mark_processed:
            _sync_self_service_quantities_from_subscription(
                items, lines, subscription_id, exact
            )
            if _merge_chargebee_state_after_webhook_enabled():
                try:
                    n_cb = merge_chargebee_line_quantities_into_dict(lines)
                    log.info(
                        "Merged %s self-serve subscription lines from Chargebee API into state",
                        n_cb,
                    )
                except ValueError as e:
                    log.warning("Chargebee state merge skipped (check CHARGEBEE_* env): %s", e)
                except Exception:
                    log.exception(
                        "Chargebee state merge failed; continuing with payload-synced quantities only"
                    )
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
