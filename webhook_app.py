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

State: default is JSON file WEBHOOK_STATE_PATH (use a volume if the container is ephemeral).
Alternatively set WEBHOOK_STATE_BACKEND=postgres with DATABASE_URL (or WEBHOOK_STATE_DATABASE_URL)
so baselines and processed_event_ids live in PostgreSQL (table webhook_app_state).

Delta logic: for each self-serve line key ``subscription_id|item_price_id``, ``stored_qty`` is the
last known quantity in ``line_quantities``. The webhook payload supplies ``chargebee_qty``; a
positive expansion is queued only when ``chargebee_qty > stored_qty`` (delta = new - old). Decreases
update the baseline but do not create an Opportunity. Missing baselines can use Chargebee events
(``WEBHOOK_INFER_BASELINE_FROM_EVENTS``) to infer prior qty.

After a successful run, the handler saves payload-synced baselines, then optionally merges all active
subscription lines from the Chargebee API in a background thread by default
(``WEBHOOK_MERGE_CHARGEBEE_ASYNC``) so the HTTP response is not blocked by large Subscription.list walks.

subscription_created (new subscription) refreshes baseline only by default — it does not create an
expansion for the initial quantities (set WEBHOOK_ALLOW_EXPANSION_ON_SUBSCRIPTION_CREATED=1 to change).

Products: creates a Quote on the expansion Opportunity, adds QuoteLineItems (seat delta qty), then
enables Quote→Opportunity sync (IsSyncing + SyncedQuoteId). Expansion can set Amended_Contract__c from
the latest Closed Won NB/Renewal Opportunity’s ContractId (SF_OPP_AMENDED_CONTRACT_*). Optionally
SF_OPP_POPULATE_CONTRACT_LOOKUP + SOQL sets Opportunity ContractId. After each successful webhook,
line quantities are synced from the payload, then optionally merged from Chargebee. Prorated Term (Months)
(SF_USE_PRORATED_TERM_MONTHS) and ServiceDate on lines help match co-termed expansion subscription quotes.
QLI Term (Months) defaults to matching QuoteLineItem End_Date__c formula math (Start_Date__c +
Term_Months__c: ADDMONTHS - 1 day for whole months; fractional branch uses days in start month).
Set SF_TERM_MONTHS_MATCH_OLI_FORMULA=0 only if you rely on proration/calendar term instead.
QuoteLineItem API writes are start/term only when standard/custom line end fields are not writable.

Expansion contract end is still read from the latest Closed Won New Business/Renewal for term math and
Quote header mapping (``SF_QUOTE_CONTRACT_END_DATE_FIELD``). Optional Chatter post (SF_CHATTER_EXPANSION_*).
"""

from __future__ import annotations

import calendar
import json
import logging
import math
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from simple_salesforce import Salesforce

from chargebee_client import (
    ADDON_ITEM_PRICE_ID_PREFIXES,
    _addon_exact_ids_from_env,
    _crm_addon_line_matches,
    infer_prior_self_serve_qty_from_subscription_changed_events,
    self_service_line_state_key,
    subscription_items_from_webhook_subscription,
    subscription_line_dicts_from_chargebee_retrieve,
)
from seed_webhook_state import (
    merge_chargebee_into_state_dict,
    merge_chargebee_line_quantities_into_dict,
)
from webhook_state_store import build_webhook_state_store
from sf_config import init_sf_config, sf_cfg

load_dotenv(Path(__file__).resolve().parent / ".env")
init_sf_config()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("webhook_app")

app = Flask(__name__)

_state_lock = threading.Lock()
_DEFAULT_STATE_FILE = Path(__file__).resolve().parent / "webhook_state.json"
_state_store, _WEBHOOK_STATE_BACKEND = build_webhook_state_store(_DEFAULT_STATE_FILE)
_STATE_PATH = Path(os.getenv("WEBHOOK_STATE_PATH", str(_DEFAULT_STATE_FILE)))

log.info("Expansion pricing: Quote + QuoteLineItems (standard); quote lines use seat delta quantity")
log.info(
    "Webhook state backend=%s path=%s (path used when backend=file)",
    _WEBHOOK_STATE_BACKEND,
    _STATE_PATH,
)


def _merge_chargebee_state_after_webhook_enabled() -> bool:
    raw = (os.getenv("WEBHOOK_MERGE_CHARGEBEE_STATE_ON_WEBHOOK") or "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _merge_chargebee_full_baseline_async_default() -> bool:
    raw = (os.getenv("WEBHOOK_MERGE_CHARGEBEE_ASYNC") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _schedule_chargebee_full_baseline_merge() -> None:
    """Merge Chargebee API line qtys into state; slow. Runs in a daemon thread by default."""

    def _run() -> None:
        try:
            with _state_lock:
                st = _load_state()
                ln: dict[str, int] = dict(st.get("line_quantities") or {})
                try:
                    n = merge_chargebee_line_quantities_into_dict(ln)
                except ValueError as e:
                    log.warning("Chargebee baseline merge skipped (check CHARGEBEE_* env): %s", e)
                    return
                except Exception:
                    log.exception("Chargebee baseline merge failed")
                    return
                st["line_quantities"] = ln
                _save_state(st)
            log.info(
                "Merged %s self-serve subscription lines from Chargebee API into state baseline",
                n,
            )
        except Exception:
            log.exception("Chargebee baseline merge state write failed")

    if _merge_chargebee_full_baseline_async_default():
        threading.Thread(target=_run, daemon=True).start()
    else:
        _run()


def _allow_expansion_on_subscription_created() -> bool:
    raw = (os.getenv("WEBHOOK_ALLOW_EXPANSION_ON_SUBSCRIPTION_CREATED") or "").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _live_subscription_for_delta_enabled() -> bool:
    """Use Subscription.retrieve for line quantities (avoids stale/partial webhook JSON). Opt out with 0."""
    raw = (os.getenv("WEBHOOK_USE_LIVE_SUBSCRIPTION_FOR_DELTA") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _sync_self_service_quantities_from_subscription(
    items: list[Any],
    lines: dict[str, int],
    subscription_id: str,
    exact: frozenset[str] | None,
) -> None:
    """
    Set stored qty for every self-serve line on this subscription (Chargebee = source of truth).
    Removes baseline keys for CRM add-on item_price_ids that no longer appear on the subscription
    (seats removed / add-on deleted) so the next increase is not compared to a stale high watermark.
    """
    present_ip: set[str] = set()
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
        ip_s = str(ip_id).strip()
        if ip_s:
            present_ip.add(ip_s)
        qty_raw = si.get("quantity")
        try:
            new_qty = int(qty_raw) if qty_raw is not None else 0
        except (TypeError, ValueError):
            new_qty = 0
        key = self_service_line_state_key(subscription_id, str(ip_id))
        lines[key] = new_qty

    prefix = f"{subscription_id}|"
    stale: list[str] = []
    for k in list(lines.keys()):
        if not k.startswith(prefix):
            continue
        ip_only = k[len(prefix) :]
        if not _crm_addon_line_matches(ip_only, None, exact):
            continue
        if ip_only not in present_ip:
            stale.append(k)
    for k in stale:
        del lines[k]
    if stale:
        log.info(
            "Pruned webhook baseline (subscription %s) — Chargebee no longer has CRM line(s): %s",
            subscription_id,
            stale,
        )


def _load_state() -> dict[str, Any]:
    return _state_store.load()


def _save_state(state: dict[str, Any]) -> None:
    _state_store.save(state)


def _trim_processed_ids(ids: list[str], max_keep: int = 2000) -> list[str]:
    if len(ids) > max_keep:
        return ids[-max_keep:]
    return ids


def _soql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


_SF_ID_SUFFIX_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"


def _canonical_salesforce_id(raw: str) -> str:
    """
    Normalize a 15- or 18-character Salesforce Id for strict API/SOQL use.

    The last three characters of an 18-char Id are a case checksum over the first 15.
    Copy/paste or OCR often corrupts them (e.g. ...IAC vs ...IIAR), which yields
    INVALID_QUERY_FILTER_OPERATOR / invalid ID field.
    """
    s = (raw or "").strip()
    if len(s) < 15:
        return s
    base = s[:15]
    suf = ""
    for i in range(0, 15, 5):
        flags = 0
        for j in range(5):
            if base[i + j].isupper():
                flags |= 1 << j
        suf += _SF_ID_SUFFIX_ALPHABET[flags]
    full = base + suf
    if s != full:
        log.info("Normalized Salesforce Id (checksum fix): %s -> %s", s, full)
    return full


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


def _add_months_salesforce(start: date, months: int) -> date:
    """Calendar add of whole months (Salesforce ADDMONTHS-style: clamp day to end of target month)."""
    if months == 0:
        return start
    m0 = start.month - 1 + months
    y = start.year + m0 // 12
    mo = m0 % 12 + 1
    last = calendar.monthrange(y, mo)[1]
    day = min(start.day, last)
    return date(y, mo, day)


def _days_in_month_of_date(d: date) -> int:
    """Days in d's month (28–31); matches MONTH length CASE + leap February in typical SF formulas."""
    return calendar.monthrange(d.year, d.month)[1]


def _round_half_up0(x: float) -> int:
    """ROUND(x, 0) half-away-from-zero for positive fractional-day math."""
    if x >= 0:
        return int(math.floor(float(x) + 0.5))
    return int(math.ceil(float(x) - 0.5))


def _sf_oli_formula_end_date(start: date, term: float) -> date:
    """
    Mirror common QuoteLineItem End_Date__c formula driven by Start_Date__c + Term_Months__c:
    - term == 0 → start
    - whole months → ADDMONTHS(start, n) - 1 day
    - else → ADDMONTHS(start, floor(term)) + ROUND(frac * days_in_month(start), 0) days
    """
    if term <= 0 or abs(term) < 1e-12:
        return start
    w = math.floor(term + 1e-15)
    frac = term - w
    whole = abs(frac) < 1e-9
    if whole:
        if w <= 0:
            return start
        return _add_months_salesforce(start, w) - timedelta(days=1)
    dim = _days_in_month_of_date(start)
    extra = _round_half_up0(frac * float(dim))
    return _add_months_salesforce(start, w) + timedelta(days=extra)


def _inverse_term_for_sf_oli_formula(start: date, end: date) -> float | None:
    """Term (months) such that _sf_oli_formula_end_date(start, term) == end, if any within a bounded search."""
    if end < start:
        return None
    if end == start:
        return 0.0
    for n in range(1, 1201):
        if _add_months_salesforce(start, n) - timedelta(days=1) == end:
            return float(n)
    dim = _days_in_month_of_date(start)
    if dim <= 0:
        return None
    for w in range(0, 1201):
        base = _add_months_salesforce(start, w)
        delta = (end - base).days
        if delta < 0:
            continue
        if delta == 0:
            if w == 0:
                continue
            term = w + (0.49 / float(dim))
            if _sf_oli_formula_end_date(start, term) == end:
                return term
            continue
        frac = delta / float(dim)
        if frac <= 0 or frac >= 1:
            continue
        term = w + frac
        if abs(term - round(term)) < 1e-7:
            continue
        if _sf_oli_formula_end_date(start, term) == end:
            return term
    lo, hi = 0.0, 1200.0
    fe_lo = _sf_oli_formula_end_date(start, lo)
    fe_hi = _sf_oli_formula_end_date(start, hi)
    if fe_lo > end or fe_hi < end:
        return None
    best: float | None = None
    for _ in range(100):
        mid = (lo + hi) / 2.0
        c = _sf_oli_formula_end_date(start, mid)
        if c == end:
            best = mid
            break
        if c < end:
            lo = mid
        else:
            hi = mid
    if best is not None:
        return best
    for cand in (lo, hi, (lo + hi) / 2.0):
        if _sf_oli_formula_end_date(start, cand) == end:
            return cand
    return None


def _prorated_term_decimals() -> int:
    try:
        nd = int((sf_cfg("SF_PRORATED_TERM_DECIMALS") or "6").strip() or "6")
    except ValueError:
        nd = 6
    return max(0, min(12, nd))


def _prorated_term_months(start: date, end: date) -> float:
    """
    Fractional months from calendar day span (matches common CPQ co-term math: days / (365.25/12)).
    E.g. 364 days ≈ 11.97 months — aligns manual expansion quotes where Term (Months) is decimal.

    Prefer ``SF_TERM_MONTHS_MATCH_OLI_FORMULA=1`` when line End_Date__c is a formula of Start + Term
    (ADDMONTHS rules); use proration when that flag is off or as fallback after an inverse solve fails.
    Low rounding (e.g. 2 decimals) can land one day short of ``end``. Use ``SF_PRORATED_TERM_DECIMALS``
    (default 6, max 12). Ensure the Term field allows enough decimal scale.
    """
    if end <= start:
        return 0.0
    days = (end - start).days
    months = days / (365.25 / 12.0)
    nd = _prorated_term_decimals()
    return round(months, nd)


def _expansion_term_months(sync_date: date, contract_end: date | None) -> float | None:
    """Quote + line term: QLI End_Date__c formula inverse (default), else proration or whole calendar months."""
    if contract_end is None:
        return None
    nd = _prorated_term_decimals()
    if _oli_formula_term_matching_enabled():
        inv = _inverse_term_for_sf_oli_formula(sync_date, contract_end)
        if inv is not None:
            out = round(inv, nd)
            log.info(
                "Expansion term_months=%s (QLI formula inverse for contract_end=%s, sync=%s, decimals=%s)",
                out,
                contract_end.isoformat(),
                sync_date.isoformat(),
                nd,
            )
            return out
        log.warning(
            "SF_TERM_MONTHS_MATCH_OLI_FORMULA: no Term_Months__c matches ADDMONTHS-style line end "
            "(sync=%s contract_end=%s); falling back to prorated/calendar term",
            sync_date.isoformat(),
            contract_end.isoformat(),
        )
    if _truthy_cfg("SF_USE_PRORATED_TERM_MONTHS"):
        out = _prorated_term_months(sync_date, contract_end)
        log.info(
            "Expansion term_months=%s (prorated days/(365.25/12); sync=%s contract_end=%s)",
            out,
            sync_date.isoformat(),
            contract_end.isoformat(),
        )
        return out
    cal = _months_between_start_and_end(sync_date, contract_end)
    out = float(cal) if cal > 0 else None
    if out is not None:
        log.info(
            "Expansion term_months=%s (calendar month span; sync=%s contract_end=%s)",
            out,
            sync_date.isoformat(),
            contract_end.isoformat(),
        )
    return out


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


def _closed_won_nb_renewal_record_type_developer_names() -> list[str] | None:
    """RecordType DeveloperNames for New Business + Renewal (same scope as amended Contract lookup)."""
    rts = (sf_cfg("SF_AMENDED_CONTRACT_SOURCE_RECORD_TYPE_DEVELOPER_NAMES") or "").strip()
    if rts:
        names = [n.strip() for n in rts.split(",") if n.strip()]
    else:
        names = []
        nb = (sf_cfg("SF_NEW_BUSINESS_RECORD_TYPE_DEVELOPER_NAME") or "").strip()
        ren = (sf_cfg("SF_RENEWAL_RECORD_TYPE_DEVELOPER_NAME") or "Renewal").strip()
        if nb and _valid_sf_field_api_name(nb):
            names.append(nb)
        if ren and _valid_sf_field_api_name(ren):
            names.append(ren)
    if not names:
        return None
    for n in names:
        if not _valid_sf_field_api_name(n):
            return None
    return names


def _query_expansion_contract_end_from_won_opportunities(sf: Salesforce, account_id: str) -> date | None:
    """
    Expansion Opportunity contract end: use SF_OPP_CONTRACT_END_DATE_FIELD (or Contract.EndDate) from the
    latest Closed Won New Business / Renewal on the Account — same record-type set as amended Contract.
    """
    end_f = sf_cfg("SF_OPP_CONTRACT_END_DATE_FIELD")
    if not end_f or not _valid_sf_field_api_name(end_f):
        return None
    names = _closed_won_nb_renewal_record_type_developer_names()
    if not names:
        log.warning(
            "Contract end from Won opp: configure SF_AMENDED_CONTRACT_SOURCE_RECORD_TYPE_DEVELOPER_NAMES "
            "or SF_NEW_BUSINESS_RECORD_TYPE_DEVELOPER_NAME + SF_RENEWAL_RECORD_TYPE_DEVELOPER_NAME"
        )
        return None
    safe_acc = _soql_escape(account_id)
    in_list = ",".join(f"'{_soql_escape(n)}'" for n in names)
    q = (
        f"SELECT Id, CloseDate, {end_f}, Contract.EndDate FROM Opportunity WHERE AccountId = '{safe_acc}' "
        f"AND IsClosed = true AND IsWon = true "
        f"AND RecordType.DeveloperName IN ({in_list}) "
        f"ORDER BY CloseDate DESC LIMIT 1"
    )
    try:
        res = sf.query(q)
    except Exception:
        log.exception("Contract end (Won NB/Renewal) SOQL failed")
        return None
    recs = res.get("records") or []
    if not recs:
        log.warning(
            "No Closed Won %s Opportunity for contract end date (Account %s)",
            names,
            account_id,
        )
        return None
    row = recs[0]
    opp_won_id = str(row.get("Id") or "")
    from_field = _parse_sf_date(row.get(end_f))
    con = row.get("Contract")
    from_contract = _parse_sf_date(con.get("EndDate")) if isinstance(con, dict) else None
    if (
        from_field is not None
        and from_contract is not None
        and from_field != from_contract
    ):
        log.warning(
            "Won Opp %s: %s=%s differs from Contract.EndDate=%s; using %s for expansion (aligns with NB/Renewal opp).",
            opp_won_id,
            end_f,
            from_field.isoformat(),
            from_contract.isoformat(),
            end_f,
        )
    raw = from_field
    source = end_f
    if raw is None:
        raw = from_contract
        source = "Contract.EndDate"
    if raw is None:
        log.warning(
            "Latest Won %s Opportunity %s for Account %s has no %s or Contract.EndDate",
            names,
            opp_won_id or "?",
            account_id,
            end_f,
        )
        return None
    log.info(
        "Expansion contract end %s from Won NB/Renewal Opp %s (CloseDate=%s, source=%s; Account %s)",
        raw.isoformat(),
        opp_won_id,
        row.get("CloseDate"),
        source,
        account_id,
    )
    return raw


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


def _truthy_cfg(key: str) -> bool:
    return (sf_cfg(key) or "").strip().lower() in ("1", "true", "yes", "on")


def _oli_formula_term_matching_enabled() -> bool:
    """
    Solve Term_Months__c so QLI End_Date__c (ADDMONTHS + fractional-day formula) matches contract end.
    Default on when unset — set SF_TERM_MONTHS_MATCH_OLI_FORMULA=0 to use only proration/calendar math.
    """
    raw = (sf_cfg("SF_TERM_MONTHS_MATCH_OLI_FORMULA") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _infer_baseline_from_chargebee_events_enabled() -> bool:
    raw = (os.getenv("WEBHOOK_INFER_BASELINE_FROM_EVENTS") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _events_baseline_scan_limit() -> int:
    raw = (os.getenv("WEBHOOK_EVENTS_BASELINE_SCAN_LIMIT") or "100").strip()
    try:
        return max(10, min(int(raw), 500))
    except ValueError:
        return 100


def _finalize_ql_dates_sleep_sec() -> float:
    raw = (os.getenv("WEBHOOK_FINALIZE_QLI_DATES_SLEEP_SEC") or "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, min(float(raw), 30.0))
    except ValueError:
        return 0.0


def _resolve_sf_contract_id_for_account(
    sf: Salesforce,
    account_id: str,
    sync_date: date,
    subscription: dict[str, Any] | None,
) -> str | None:
    """
    Salesforce Contract Id for SF_OPP_POPULATE_CONTRACT_LOOKUP + SF_OPP_CONTRACT_LOOKUP_FIELD (optional).

    Resolution order:
      1) Chargebee subscription custom field CHARGEBEE_SUBSCRIPTION_SF_CONTRACT_FIELD (18-char SF Id), if set.
      2) SOQL on standard Contract: AccountId + optional Status filter + optional date overlap with sync_date.

    Configure SF_CONTRACT_STATUS_FILTER (comma-separated, e.g. Activated) and SF_CONTRACT_DATE_OVERLAP_EVENT.
    """
    cb_field = (sf_cfg("CHARGEBEE_SUBSCRIPTION_SF_CONTRACT_FIELD") or "").strip()
    if cb_field and subscription:
        raw = subscription.get(cb_field)
        if raw is not None and str(raw).strip():
            cid = str(raw).strip()
            log.info("Contract Id from Chargebee field %s=%s", cb_field, cid)
            return cid

    obj = (sf_cfg("SF_CONTRACT_OBJECT_API_NAME") or "Contract").strip()
    if obj not in ("Contract", "ServiceContract"):
        log.warning("SF_CONTRACT_OBJECT_API_NAME must be Contract or ServiceContract; got %r", obj)
        obj = "Contract"

    safe_acc = _soql_escape(account_id)
    conditions: list[str] = [f"AccountId = '{safe_acc}'"]

    status_raw = (sf_cfg("SF_CONTRACT_STATUS_FILTER") or "").strip()
    if status_raw:
        parts = [p.strip() for p in status_raw.split(",") if p.strip()]
        if len(parts) == 1:
            conditions.append(f"Status = '{_soql_escape(parts[0])}'")
        elif len(parts) > 1:
            in_list = ",".join(f"'{_soql_escape(p)}'" for p in parts)
            conditions.append(f"Status IN ({in_list})")

    if (sf_cfg("SF_CONTRACT_DATE_OVERLAP_EVENT", "1") or "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    ):
        d = sync_date.isoformat()
        conditions.append(f"StartDate <= {d}")
        conditions.append(f"(EndDate = NULL OR EndDate >= {d})")

    order = (sf_cfg("SF_CONTRACT_SOQL_ORDER_BY") or "StartDate DESC").strip()
    allowed_order = frozenset(
        {
            "StartDate DESC",
            "StartDate ASC",
            "EndDate DESC",
            "EndDate ASC",
            "CreatedDate DESC",
        }
    )
    if order not in allowed_order:
        order = "StartDate DESC"

    q = f"SELECT Id FROM {obj} WHERE {' AND '.join(conditions)} ORDER BY {order} LIMIT 1"
    try:
        res = sf.query(q)
    except Exception:
        log.exception("Contract lookup SOQL failed: %s", q[:500])
        return None
    recs = res.get("records") or []
    if not recs:
        log.warning(
            "No %s matched Account %s (Status filter=%r overlap=%s).",
            obj,
            account_id,
            status_raw or None,
            (sf_cfg("SF_CONTRACT_DATE_OVERLAP_EVENT", "1") or "1").strip().lower()
            not in ("0", "false", "no", "off"),
        )
        return None
    cid = str(recs[0]["Id"])
    log.info("Resolved %s %s for expansion Opportunity (Account %s)", obj, cid, account_id)
    return cid


def _query_amended_contract_from_won_opportunities(sf: Salesforce, account_id: str) -> str | None:
    """
    Contract Id to store on SF_OPP_AMENDED_CONTRACT_FIELD: copy Opportunity.ContractId from the latest
    Closed Won New Business or Renewal on this Account (RecordType.DeveloperName IN configured list).
    """
    acf = sf_cfg("SF_OPP_AMENDED_CONTRACT_FIELD")
    if not acf or not _valid_sf_field_api_name(acf):
        return None

    names = _closed_won_nb_renewal_record_type_developer_names()
    if not names:
        log.warning(
            "Amended_Contract: set SF_AMENDED_CONTRACT_SOURCE_RECORD_TYPE_DEVELOPER_NAMES "
            "(comma-separated RecordType DeveloperNames) or SF_NEW_BUSINESS_RECORD_TYPE_DEVELOPER_NAME "
            "+ SF_RENEWAL_RECORD_TYPE_DEVELOPER_NAME"
        )
        return None

    safe_acc = _soql_escape(account_id)
    in_list = ",".join(f"'{_soql_escape(n)}'" for n in names)
    q = (
        f"SELECT ContractId FROM Opportunity WHERE AccountId = '{safe_acc}' "
        f"AND IsClosed = true AND IsWon = true AND ContractId != null "
        f"AND RecordType.DeveloperName IN ({in_list}) "
        f"ORDER BY CloseDate DESC LIMIT 1"
    )
    try:
        res = sf.query(q)
    except Exception:
        log.exception("Amended Contract SOQL failed")
        return None
    recs = res.get("records") or []
    if not recs:
        log.warning(
            "No Closed Won %s Opportunity with ContractId for Account %s",
            names,
            account_id,
        )
        return None
    cid = str(recs[0].get("ContractId") or "").strip()
    if not cid:
        return None
    log.info(
        "Amended_Contract: using ContractId %s from latest Closed Won NB/Renewal (Account %s)",
        cid,
        account_id,
    )
    return cid


def _post_expansion_opportunity_chatter(sf: Salesforce, opportunity_id: str) -> None:
    """Optional Chatter post on the new expansion Opportunity (mentions + message via Chatter REST)."""
    if (sf_cfg("SF_CHATTER_EXPANSION_POST") or "1").strip().lower() in ("0", "false", "no", "off"):
        return
    uid_raw = (sf_cfg("SF_CHATTER_EXPANSION_NOTIFY_USER_ID") or "").strip()
    msg = (sf_cfg("SF_CHATTER_EXPANSION_MESSAGE") or "New self-service expansion opportunity created.").strip()
    if not msg:
        return
    segments: list[dict[str, Any]] = []
    mention_ids: list[str] = []
    seen: set[str] = set()
    for part in uid_raw.split(","):
        p = part.strip()
        if not p:
            continue
        uid = _canonical_salesforce_id(p)
        if len(uid) >= 15 and uid not in seen:
            seen.add(uid)
            mention_ids.append(uid)
    for i, uid in enumerate(mention_ids):
        if i:
            segments.append({"type": "Text", "text": " "})
        segments.append({"type": "Mention", "id": uid})
    if mention_ids:
        segments.append({"type": "Text", "text": ": "})
    segments.append({"type": "Text", "text": msg})
    payload: dict[str, Any] = {
        "body": {"messageSegments": segments},
        "feedElementType": "FeedItem",
        "subjectId": opportunity_id,
    }
    try:
        sf.restful("chatter/feed-elements", method="POST", json=payload)
        log.info("Posted Chatter on Opportunity %s", opportunity_id)
    except Exception:
        log.exception("Chatter feed post failed for Opportunity %s", opportunity_id)


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
    term_months: float | None,
    contract_id: str | None = None,
    amended_contract_id: str | None = None,
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
    term_f = sf_cfg("SF_OPP_TERM_MONTHS_FIELD")
    if term_f and _valid_sf_field_api_name(term_f) and term_months is not None:
        body[term_f] = float(term_months)
    # MRR is set after QuoteLineItem rows (many orgs use formulas tied to products).
    if (sf_cfg("SF_OPP_MRR_SET_ON_CREATE") or "").strip().lower() in ("1", "true", "yes"):
        body.update(_opportunity_mrr_field_updates(amount))

    if contract_id and _truthy_cfg("SF_OPP_POPULATE_CONTRACT_LOOKUP"):
        clf = sf_cfg("SF_OPP_CONTRACT_LOOKUP_FIELD")
        if clf and _valid_sf_field_api_name(clf):
            body[clf] = contract_id

    acf = sf_cfg("SF_OPP_AMENDED_CONTRACT_FIELD")
    if amended_contract_id and acf and _valid_sf_field_api_name(acf):
        body[acf] = amended_contract_id

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
    """
    Return (PricebookEntryId, Pricebook2Id) for quote / product lines.

    Order: SF_PRICEBOOK_ENTRY_ID; else SF_PRICEBOOK2_ID + product; else Standard Price Book + product;
    else any active PricebookEntry for that product (many orgs only add SKUs to a custom catalog).
    """
    pbe_env = (sf_cfg("SF_PRICEBOOK_ENTRY_ID") or "").strip()
    if pbe_env:
        pbe_canon = _canonical_salesforce_id(pbe_env)
        try:
            res = sf.query(
                "SELECT Id, Pricebook2Id FROM PricebookEntry WHERE Id = "
                f"'{_soql_escape(pbe_canon)}' LIMIT 1"
            )
        except Exception as exc:
            log.warning(
                "SF_PRICEBOOK_ENTRY_ID lookup failed (%s); trying product-based resolution.",
                exc,
            )
        else:
            recs = res.get("records") or []
            if recs:
                return str(recs[0]["Id"]), str(recs[0]["Pricebook2Id"])
            log.warning("SF_PRICEBOOK_ENTRY_ID not found")

    prod2_raw = sf_cfg("SF_PRODUCT2_ID")
    prod2 = _canonical_salesforce_id(prod2_raw) if prod2_raw else ""
    if prod2:
        pcond = f"Product2Id = '{_soql_escape(prod2)}'"
    else:
        name = (sf_cfg("SF_PRODUCT_NAME", "Additional CRM Seats") or "Additional CRM Seats").strip()
        pcond = f"Product2.Name = '{_soql_escape(name)}'"

    pb2_cfg_raw = (sf_cfg("SF_PRICEBOOK2_ID") or "").strip()
    pb2_cfg = _canonical_salesforce_id(pb2_cfg_raw) if pb2_cfg_raw else ""
    attempts: list[tuple[str, str]] = []
    if pb2_cfg:
        attempts.append(
            (
                f"{pcond} AND Pricebook2Id = '{_soql_escape(pb2_cfg)}' AND IsActive = true",
                f"SF_PRICEBOOK2_ID={pb2_cfg}",
            )
        )
    attempts.append(
        (f"{pcond} AND Pricebook2.IsStandard = true AND IsActive = true", "standard price book")
    )
    attempts.append((f"{pcond} AND IsActive = true", "first active price book (non-standard ok)"))

    for where_extra, label in attempts:
        order = (
            " ORDER BY CreatedDate DESC"
            if label == "first active price book (non-standard ok)"
            else ""
        )
        q = f"SELECT Id, Pricebook2Id FROM PricebookEntry WHERE {where_extra}{order} LIMIT 1"
        try:
            res = sf.query(q)
        except Exception:
            log.exception("PricebookEntry lookup failed (%s)", label)
            continue
        recs = res.get("records") or []
        if recs:
            pbe_id = str(recs[0]["Id"])
            pb_id = str(recs[0]["Pricebook2Id"])
            if label != "standard price book":
                log.info("Resolved PricebookEntry via %s — use SF_PRICEBOOK_ENTRY_ID or SF_PRICEBOOK2_ID to pin this.", label)
            return pbe_id, pb_id

    log.warning(
        "No active PricebookEntry for expansion product — set SF_PRICEBOOK_ENTRY_ID (01u…), or "
        "SF_PRODUCT2_ID / SF_PRODUCT_NAME with SF_PRICEBOOK2_ID if the SKU is not on the Standard Price Book."
    )
    return None


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
    term_months: float | None,
) -> dict[str, Any]:
    """QuoteLineItem custom start/term (SF_OLI_*). End-date fields are omitted when the org denies API write."""
    out: dict[str, Any] = {}
    start_f = sf_cfg("SF_OLI_START_DATE_FIELD")
    term_f = sf_cfg("SF_OLI_TERM_MONTHS_FIELD")
    if start_f and _valid_sf_field_api_name(start_f):
        out[start_f] = _sf_date_payload(sync_date)
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
    term_months: float | None,
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


def _quote_onetime_fees_api_name() -> str:
    """
    Writable Quote currency only. If One_Time_Fees__c is a roll-up summary, API writes fail — use
    SF_QUOTE_ONETIME_FEES_FIELD_DISABLE=1 and set QuoteLineItem fields (e.g. Recurring) so lines are
    excluded from the rollup filter. Defaults to One_Time_Fees__c when not disabled.
    """
    if (sf_cfg("SF_QUOTE_ONETIME_FEES_FIELD_DISABLE") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return ""
    raw = (sf_cfg("SF_QUOTE_ONETIME_FEES_FIELD") or "One_Time_Fees__c").strip()
    if raw and _valid_sf_field_api_name(raw):
        return raw
    return ""


def _quote_onetime_zero_final_sleep_sec() -> float:
    """Optional pause before last One_Time_Fees update so async SF logic can finish (0–15s)."""
    raw = (os.getenv("WEBHOOK_QUOTE_ONETIME_ZERO_SLEEP_SEC") or "0").strip()
    try:
        s = float(raw)
    except ValueError:
        return 0.0
    return max(0.0, min(s, 15.0))


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
            _quote_onetime_fees_api_name(),
        ]
        names = [n for n in names if n and _valid_sf_field_api_name(n)]
        if names:
            log.warning(
                "Grant the Salesforce integration user Edit (field-level security) on Quote for: %s. "
                "Setup → Permission Sets (or Profile) → Object Settings → Quote → Field Permissions.",
                ", ".join(names),
            )
        return


def _quote_patch_mrr_arr_on_quote_enabled() -> bool:
    """Quote Total_MRR__c / Total_ARR__c API writes. Off when FLS denies or fields are formula/rollup-only."""
    raw = (sf_cfg("SF_QUOTE_PATCH_MRR_ARR_ON_QUOTE") or "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _merge_quote_financial_header_fields(body: dict[str, Any], amount: float | None) -> None:
    """
    Quote header: optional MRR/ARR from delta seat revenue; optional one-time=0 on writable currency.
    Set SF_QUOTE_PATCH_MRR_ARR_ON_QUOTE=0 when the integration user cannot edit header MRR/ARR (skips retries).
    """
    if amount is not None and _quote_patch_mrr_arr_on_quote_enabled():
        mrr_f = sf_cfg("SF_QUOTE_MRR_FIELD")
        if mrr_f and _valid_sf_field_api_name(mrr_f):
            body[mrr_f] = round(float(amount), 2)
        arr_f = sf_cfg("SF_QUOTE_ARR_FIELD")
        if arr_f and _valid_sf_field_api_name(arr_f):
            body[arr_f] = round(float(amount) * 12.0, 2)
    otf = _quote_onetime_fees_api_name()
    if otf:
        body[otf] = 0.0


def _quotelineitem_extra_fields_from_config() -> dict[str, Any]:
    """
    Optional JSON object merged into each QuoteLineItem create (e.g. subscription / price-basis picklists,
    checkboxes such as isARR__c, or fields your “Add Products” wizard sets). Keys must be valid API names;
    values must match picklist API values or booleans for checkboxes.
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
    term_months: float | None,
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

    One-time defaults to **One_Time_Fees__c** unless SF_QUOTE_ONETIME_FEES_FIELD overrides or
    SF_QUOTE_ONETIME_FEES_FIELD_DISABLE=1. Use the writable source field, not a formula display field.
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


def _patch_quote_onetime_fees_zero_final(sf: Salesforce, quote_id: str) -> None:
    """Last-step Quote.update with only the one-time field = 0 (wins over late rollups / flows in many orgs)."""
    otf = _quote_onetime_fees_api_name()
    if not otf:
        return
    delay = _quote_onetime_zero_final_sleep_sec()
    if delay > 0:
        log.info("Sleep %.2fs before final Quote one-time=0 patch (WEBHOOK_QUOTE_ONETIME_ZERO_SLEEP_SEC)", delay)
        time.sleep(delay)
    patch: dict[str, Any] = {otf: 0.0}
    last_exc: BaseException | None = None
    for _attempt in range(8):
        try:
            sf.Quote.update(quote_id, patch)
            log.info("Final Quote patch: %s=0 on %s", otf, quote_id)
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
            log.warning("Retrying final Quote one-time patch without bad fields: %s", drop)
    if last_exc:
        _log_quote_financial_fls_hint(last_exc)
        log.warning(
            "Final Quote one-time=0 patch failed for %s (%s). If the field is formula/rollup, API cannot clear it.",
            quote_id,
            last_exc,
        )


def _sf_update_with_400_retries(
    sf: Salesforce,
    *,
    update_callable: Any,
    record_id: str,
    patch: dict[str, Any],
    retry_label: str,
) -> tuple[bool, BaseException | None]:
    """Return (ok, last_error). Retries on HTTP 400 by dropping fields Salesforce names in the error."""
    last_exc: BaseException | None = None
    for _attempt in range(8):
        try:
            update_callable(record_id, patch)
            return True, None
        except Exception as exc:
            last_exc = exc
            status = getattr(exc, "status", None)
            if status is None:
                log.warning("%s: non-HTTP error for id=%s: %s", retry_label, record_id, exc)
                return False, last_exc
            try:
                code = int(status)
            except (TypeError, ValueError):
                log.warning("%s: unexpected status=%r for id=%s: %s", retry_label, status, record_id, exc)
                return False, last_exc
            if code != 400:
                log.warning("%s: HTTP %s for id=%s: %s", retry_label, code, record_id, exc)
                return False, last_exc
            drop = _fields_to_drop_from_salesforce_400(exc, patch)
            if not drop:
                return False, last_exc
            removed = False
            for f in drop:
                if f in patch:
                    patch.pop(f)
                    removed = True
            if not removed or not patch:
                return False, last_exc
            log.warning("%s: retry without bad fields %s", retry_label, drop)
    return False, last_exc


def _try_set_quote_syncing(sf: Salesforce, opp_id: str, quote_id: str) -> None:
    """
    Standard Salesforce Quote: link the Opportunity to this Quote (SyncedQuoteId), then set the Quote
    sync flag (IsSyncing or SF_QUOTE_SYNCING_FIELD) so line items mirror to OpportunityLineItem.

    Order matches common API expectations: Opportunity first, then Quote. If the layout shows a custom
    \"Syncing\" checkbox instead of standard IsSyncing, set SF_QUOTE_SYNCING_FIELD to that API name.

    CPQ (SBQQ__Quote__c) uses a different model — this targets standard Quote + Opportunity only.

    Disable Opportunity.SyncedQuoteId patch with SF_QUOTE_SET_OPPORTUNITY_SYNCED_QUOTE_ID=0 if your org rejects it.
    """
    sync_raw = (sf_cfg("SF_QUOTE_SYNC_TO_OPPORTUNITY", "1") or "").strip().lower()
    if sync_raw in ("0", "false", "no", "off"):
        log.info("Quote→Opp sync skipped (SF_QUOTE_SYNC_TO_OPPORTUNITY is off)")
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
        sync_field_desc = alt
    else:
        q_patch = {"IsSyncing": True}
        sync_field_desc = "IsSyncing"

    log.info(
        "Quote→Opp sync: opp=%s quote=%s (will set SyncedQuoteId then %s)",
        opp_id,
        quote_id,
        sync_field_desc,
    )

    if (sf_cfg("SF_QUOTE_SET_OPPORTUNITY_SYNCED_QUOTE_ID", "1") or "").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    ):
        o_patch: dict[str, Any] = {"SyncedQuoteId": quote_id}
        ok_o, err_o = _sf_update_with_400_retries(
            sf,
            update_callable=sf.Opportunity.update,
            record_id=opp_id,
            patch=o_patch,
            retry_label="Opportunity.update (SyncedQuoteId)",
        )
        if ok_o:
            log.info("Opportunity %s SyncedQuoteId set to Quote %s", opp_id, quote_id)
        elif err_o:
            log.warning(
                "Opportunity.SyncedQuoteId not set (continuing with Quote sync flag). opp=%s err=%s",
                opp_id,
                err_o,
            )

    ok_q, err_q = _sf_update_with_400_retries(
        sf,
        update_callable=sf.Quote.update,
        record_id=quote_id,
        patch=q_patch,
        retry_label="Quote.update (sync)",
    )
    if ok_q:
        log.info("Quote → Opportunity sync enabled on %s (keys=%s)", quote_id, list(q_patch.keys()))
    elif err_q:
        log.warning(
            "Could not set quote sync flag on %s (%s). Check FLS on Quote.%s and validation rules.",
            quote_id,
            err_q,
            sync_field_desc,
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
    term_months: float | None,
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


def _patch_quotelineitem_start_and_term(
    sf: Salesforce,
    qli_id: str,
    *,
    sync_date: date,
    term_months: float | None,
) -> None:
    """After QuoteLineItem.create, align start/term only (end-date API writes skipped for restricted orgs)."""
    patch: dict[str, Any] = {}
    term_f = sf_cfg("SF_OLI_TERM_MONTHS_FIELD")
    if term_f and _valid_sf_field_api_name(term_f) and term_months is not None:
        patch[term_f] = float(term_months)
    start_f = sf_cfg("SF_OLI_START_DATE_FIELD")
    if start_f and _valid_sf_field_api_name(start_f):
        patch[start_f] = _sf_date_payload(sync_date)
    if not patch:
        return
    planned = frozenset(patch.keys())
    ok, err = _sf_update_with_400_retries(
        sf,
        update_callable=sf.QuoteLineItem.update,
        record_id=qli_id,
        patch=patch,
        retry_label="QuoteLineItem.update (start/term)",
    )
    if ok:
        accepted = frozenset(patch.keys())
        dropped = planned - accepted
        log.info("Patched QuoteLineItem %s (fields accepted): %s", qli_id, sorted(accepted))
        if dropped:
            log.warning(
                "QuoteLineItem %s: Salesforce rejected fields %s on update.",
                qli_id,
                sorted(dropped),
            )
    elif err:
        log.warning(
            "QuoteLineItem update incomplete for %s (grant FLS on %s): %s",
            qli_id,
            ", ".join(sorted(patch.keys())),
            err,
        )


def _log_quotelineitem_end_dates_after_finalize(sf: Salesforce, quote_id: str) -> None:
    """Debug SOQL for line end after sync; many orgs have no standard QuoteLineItem.EndDate (only custom/formula)."""
    qid = _canonical_salesforce_id(quote_id)
    end_f = sf_cfg("SF_OLI_END_DATE_FIELD")
    if not end_f or not _valid_sf_field_api_name(end_f):
        return
    fields = f"Id, {end_f}"
    try:
        res = sf.query(
            f"SELECT {fields} FROM QuoteLineItem WHERE QuoteId = '{_soql_escape(qid)}' LIMIT 80"
        )
    except Exception as exc:
        log.warning(
            "QuoteLineItem post-finalize date readback skipped quote=%s (%s)",
            qid,
            exc,
        )
        return
    parts: list[str] = []
    for r in res.get("records") or []:
        rid = r.get("Id")
        parts.append(f"{rid}:{end_f}={r.get(end_f)}")
    if parts:
        log.info("QuoteLineItem %s after sync+finalize on quote %s: %s", end_f, qid, "; ".join(parts))


def _finalize_expansion_quotelineitem_contract_dates(
    sf: Salesforce,
    quote_id: str,
    *,
    sync_date: date,
    contract_end: date | None,
    term_months: float | None,
) -> None:
    qid = _canonical_salesforce_id(quote_id)
    try:
        res = sf.query(
            f"SELECT Id FROM QuoteLineItem WHERE QuoteId = '{_soql_escape(qid)}'"
        )
    except Exception:
        log.exception("Finalize QLI: list QuoteLineItem Ids failed quote=%s", qid)
        return
    ids = [str(r["Id"]) for r in (res.get("records") or []) if r.get("Id")]
    if not ids:
        return
    delay = _finalize_ql_dates_sleep_sec()
    if delay > 0:
        log.info(
            "Sleep %.2fs before final QuoteLineItem date patch (WEBHOOK_FINALIZE_QLI_DATES_SLEEP_SEC)",
            delay,
        )
        time.sleep(delay)
    for qli_id in ids:
        _patch_quotelineitem_start_and_term(
            sf,
            qli_id,
            sync_date=sync_date,
            term_months=term_months,
        )
    _log_quotelineitem_end_dates_after_finalize(sf, qid)


def _skip_expansion_when_baseline_missing() -> bool:
    return (os.getenv("WEBHOOK_SKIP_EXPANSION_IF_BASELINE_MISSING") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _quote_line_item_description(delta: int, item_price_id: str) -> str | None:
    """Optional Line Item Description from SF_QTI_DESCRIPTION_TEMPLATE (mirrors manual quotes)."""
    tmpl = (sf_cfg("SF_QTI_DESCRIPTION_TEMPLATE") or "").strip()
    if not tmpl:
        return None
    product_name = (sf_cfg("SF_PRODUCT_NAME", "Additional CRM Seats") or "Additional CRM Seats").strip()
    try:
        text = tmpl.format(delta=delta, item_price_id=item_price_id, product_name=product_name)
    except (KeyError, ValueError, IndexError):
        log.warning("SF_QTI_DESCRIPTION_TEMPLATE is invalid for format(); check placeholders")
        return None
    return text.strip()[:255]


def _add_quote_line_items(
    sf: Salesforce,
    quote_id: str,
    pbe_id: str,
    pending: list[tuple[str, int, int, str, float | None]],
    *,
    sync_date: date,
    contract_end: date | None,
    term_months: float | None,
) -> None:
    """One QuoteLineItem per Chargebee line; Quantity = seat delta for this event."""
    extras = _product_line_custom_fields(sync_date=sync_date, term_months=term_months)
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
        desc = _quote_line_item_description(delta, ip_id)
        if desc:
            body["Description"] = desc
        if _truthy_cfg("SF_QTI_SET_STANDARD_SERVICE_DATE"):
            body["ServiceDate"] = sync_date.isoformat()
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
                result = sf.QuoteLineItem.create(body)
                qli_id = str(result.get("id") or "") if isinstance(result, dict) else ""
                log.info(
                    "Added QuoteLineItem quote=%s qty=%s (delta) unit_price=%s item_price_id=%s",
                    quote_id,
                    qli_qty,
                    up,
                    ip_id,
                )
                if qli_id:
                    _patch_quotelineitem_start_and_term(
                        sf,
                        qli_id,
                        sync_date=sync_date,
                        term_months=term_months,
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


def _log_pricebook_entry_product_for_quote(sf: Salesforce, pbe_id: str) -> None:
    """
    Log Product2 behind the PricebookEntry. Quote MRR/ARR vs one-time rollups usually key off
    Product Family or custom Product / QLI fields; API-created lines skip the 'Add Products' wizard
    that sets those defaults unless you map them (SF_QUOTELINEITEM_STATIC_FIELDS_JSON).
    """
    pid = _canonical_salesforce_id(pbe_id)
    try:
        res = sf.query(
            "SELECT Product2Id, Product2.Name, Product2.Family FROM PricebookEntry WHERE Id = "
            f"'{_soql_escape(pid)}' LIMIT 1"
        )
    except Exception:
        log.exception("PricebookEntry product context query failed pbe=%s", pid)
        return
    recs = res.get("records") or []
    if not recs:
        log.warning("PricebookEntry %s not found for product context log", pid)
        return
    r0 = recs[0]
    p2 = r0.get("Product2") or {}
    log.info(
        "Quote will use PricebookEntry %s → Product2 %s Name=%r Family=%r "
        "(if rollups put line total in One-Time but MRR is $0, fix Product2 classification or set "
        "SF_QUOTELINEITEM_STATIC_FIELDS_JSON to match a manual recurring line)",
        pid,
        r0.get("Product2Id"),
        p2.get("Name"),
        p2.get("Family"),
    )


def _attach_expansion_quote_and_lines(
    sf: Salesforce,
    opp_id: str,
    *,
    company: str,
    total_seat_delta: int,
    pending: list[tuple[str, int, int, str, float | None]],
    sync_date: date,
    contract_end: date | None,
    term_months: float | None,
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
    _log_pricebook_entry_product_for_quote(sf, pbe_id)
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
    _finalize_expansion_quotelineitem_contract_dates(
        sf,
        qid,
        sync_date=sync_date,
        contract_end=contract_end,
        term_months=term_months,
    )
    # Sync / line rollups can repopulate manual one-time buckets after the first patch; re-apply MRR/ARR/OTF=0.
    _patch_quote_financials_after_line_items(sf, qid, amount=amount)
    _patch_quote_onetime_fees_zero_final(sf, qid)


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

    log.info(
        "Chargebee webhook received event_id=%s event_type=%s subscription_id=%s customer_id=%s",
        event_id,
        event_type,
        subscription_id,
        customer_id or "?",
    )

    exact = _addon_exact_ids_from_env()
    items_webhook = subscription_items_from_webhook_subscription(sub if isinstance(sub, dict) else {})
    items: list[Any] = list(items_webhook)
    if _live_subscription_for_delta_enabled():
        live = subscription_line_dicts_from_chargebee_retrieve(subscription_id)
        if live is not None and len(live) > 0:
            items = live
            log.info(
                "Delta qtys from Chargebee API retrieve subscription_id=%s (webhook item rows=%s api rows=%s)",
                subscription_id,
                len(items_webhook),
                len(live),
            )
        elif live is None:
            log.warning(
                "Chargebee Subscription.retrieve failed for %s; using webhook payload for line qtys",
                subscription_id,
            )

    with _state_lock:
        state = _load_state()
        processed: list[str] = list(state.get("processed_event_ids") or [])
        if event_id in processed:
            log.info("Duplicate event %s; skipping", event_id)
            return

        lines: dict[str, int] = dict(state.get("line_quantities") or {})
        defer_full_chargebee_merge = False

        if event_type == "subscription_created" and not _allow_expansion_on_subscription_created():
            _sync_self_service_quantities_from_subscription(items, lines, subscription_id, exact)
            processed.append(event_id)
            log.info(
                "subscription_created %s: baseline updated; expansion skipped "
                "(WEBHOOK_ALLOW_EXPANSION_ON_SUBSCRIPTION_CREATED is off)",
                subscription_id,
            )
            defer_full_chargebee_merge = _merge_chargebee_state_after_webhook_enabled()
        else:
            pending: list[tuple[str, int, int, str, float | None]] = []
            # pending: (line_key, new_qty, delta, item_price_id, unit_price_major)
            missing_baseline_warned = False
            first_tracked_line: tuple[str, str, int | None, int, int] | None = None

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
                inferred: int | None = None
                if stored is None and _infer_baseline_from_chargebee_events_enabled():
                    try:
                        inferred = infer_prior_self_serve_qty_from_subscription_changed_events(
                            subscription_id,
                            str(ip_id),
                            new_qty,
                            event_id,
                            max_events=_events_baseline_scan_limit(),
                        )
                    except Exception:
                        log.exception(
                            "Chargebee events baseline infer failed key=%s; continuing without infer",
                            key,
                        )
                        inferred = None
                if stored is not None:
                    old_qty = stored
                elif inferred is not None:
                    old_qty = inferred
                    log.info(
                        "Inferred prior qty from Chargebee events for %s: %s (current %s)",
                        key,
                        inferred,
                        new_qty,
                    )
                else:
                    old_qty = 0
                if first_tracked_line is None:
                    first_tracked_line = (key, str(ip_id), stored, old_qty, new_qty)
                if stored is None and inferred is None and new_qty > 0 and not missing_baseline_warned:
                    missing_baseline_warned = True
                    log.warning(
                        "No stored baseline for self-serve line %s; treating prior qty as 0 for this delta. "
                        "If you just redeployed, seed WEBHOOK_STATE_PATH (POST /admin/seed-state or "
                        "seed_webhook_state.py) so the next increase is only the real delta. "
                        "Or set WEBHOOK_SKIP_EXPANSION_IF_BASELINE_MISSING=1 to block expansion until seeded.",
                        key,
                    )
                if stored is None and inferred is None and _skip_expansion_when_baseline_missing():
                    if new_qty > old_qty:
                        log.error(
                            "Skipping expansion for line %s (Chargebee qty=%s): "
                            "WEBHOOK_SKIP_EXPANSION_IF_BASELINE_MISSING is on and state has no baseline.",
                            key,
                            new_qty,
                        )
                    continue
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
            if not pending:
                log.info(
                    "No expansion Opportunity: no positive seat delta for tracked self-serve lines "
                    "(event_id=%s subscription_id=%s). Usually state already matches Chargebee qty "
                    "(subscription_created baseline, POST /admin/seed-state, or merge after a prior webhook). "
                    "New subs: set WEBHOOK_ALLOW_EXPANSION_ON_SUBSCRIPTION_CREATED=1 if the first seat qty "
                    "should create an expansion.",
                    event_id,
                    subscription_id,
                )
                if first_tracked_line:
                    fk, fip, fst, fold, fnew = first_tracked_line
                    log.info(
                        "No-expansion detail (first CRM line in payload): key=%s item_price_id=%s "
                        "stored_in_state=%s effective_prior_qty=%s chargebee_qty=%s "
                        "(delta needs chargebee_qty > effective_prior; subscription_created may have set state first).",
                        fk,
                        fip,
                        fst,
                        fold,
                        fnew,
                    )
                else:
                    brief: list[str] = []
                    for si in items:
                        if not isinstance(si, dict):
                            continue
                        brief.append(
                            "type=%r ip=%r id=%r qty=%r"
                            % (
                                si.get("item_type"),
                                si.get("item_price_id"),
                                si.get("id"),
                                si.get("quantity"),
                            )
                        )
                    log.warning(
                        "No CRM add-on lines matched this webhook (subscription_id=%s). "
                        "Parsed %s line object(s): %s | CHARGEBEE_ADDON_IDS exact=%s | prefix=%s",
                        subscription_id,
                        len(items),
                        "; ".join(brief[:15]) if brief else "(none — check content.subscription line keys)",
                        sorted(exact) if exact else "(unset)",
                        ADDON_ITEM_PRICE_ID_PREFIXES,
                    )
            if pending:
                sf = _get_salesforce()
                account_id = _resolve_account_id(sf, customer, customer_id)
                if account_id:
                    contract_id = None
                    if _truthy_cfg("SF_OPP_POPULATE_CONTRACT_LOOKUP"):
                        contract_id = _resolve_sf_contract_id_for_account(sf, account_id, sync_date, sub)
                    amended_contract_id = _query_amended_contract_from_won_opportunities(sf, account_id)

                    skip_expansion = False
                    if _truthy_cfg("SF_OPP_CONTRACT_LOOKUP_REQUIRED") and not contract_id:
                        log.error(
                            "Skipping expansion Opportunity: SF_OPP_CONTRACT_LOOKUP_REQUIRED but no Contract "
                            "resolved for Account %s (enable SF_OPP_POPULATE_CONTRACT_LOOKUP and SOQL filters).",
                            account_id,
                        )
                        skip_expansion = True
                    if _truthy_cfg("SF_OPP_AMENDED_CONTRACT_REQUIRED") and not amended_contract_id:
                        log.error(
                            "Skipping expansion Opportunity: SF_OPP_AMENDED_CONTRACT_REQUIRED but no Closed Won "
                            "New Business/Renewal with ContractId for Account %s (check record type DeveloperNames).",
                            account_id,
                        )
                        skip_expansion = True
                    if skip_expansion:
                        mark_processed = False
                    else:
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
                        contract_end = _query_expansion_contract_end_from_won_opportunities(sf, account_id)
                        if contract_end is None:
                            contract_end = _query_expansion_contract_end_from_renewal(sf, account_id)
                        term_months = _expansion_term_months(sync_date, contract_end)
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
                            contract_id=contract_id,
                            amended_contract_id=amended_contract_id,
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
                        _post_expansion_opportunity_chatter(sf, oid)
                else:
                    log.warning("Skipping Opportunity: no Salesforce Account for customer_id=%s", customer_id)
                    mark_processed = False

            if mark_processed:
                _sync_self_service_quantities_from_subscription(items, lines, subscription_id, exact)
                processed.append(event_id)
            defer_full_chargebee_merge = bool(
                mark_processed and _merge_chargebee_state_after_webhook_enabled()
            )

        state["processed_event_ids"] = _trim_processed_ids(processed)
        state["line_quantities"] = lines
        _save_state(state)

    if defer_full_chargebee_merge:
        _schedule_chargebee_full_baseline_merge()


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
            st = _load_state()
            n = merge_chargebee_into_state_dict(st)
            _save_state(st)
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
