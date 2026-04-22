"""Fetch add-on and related data from Chargebee API."""

import os
from datetime import datetime
from pathlib import Path
from typing import Any

from chargebee import Chargebee
from dotenv import load_dotenv

# Load .env from the project folder (same directory as this file)
_load_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(_load_env_path)

# Match subscription lines by item_price_id prefix (any billing period / slug Chargebee uses).
# Run python list_crm_item_price_ids.py to see actual ids on your site.
ADDON_ITEM_PRICE_ID_PREFIXES = (
    "Additional-CRM-User-Self-Service-USD-",
    "Additional-CRM-User-Self-Serve-USD-",
)

# Optional: set CHARGEBEE_ADDON_IDS in .env to a comma-separated list of exact item_price_ids
# to use exact matching instead of prefixes (e.g. if you must exclude a SKU).


def _crm_addon_line_matches(
    item_price_id: str | None,
    item_type: str | None,
    exact_ids: frozenset[str] | None,
) -> bool:
    if not item_price_id:
        return False
    if (item_type or "").lower() == "plan":
        return False
    ip = item_price_id.strip()
    if exact_ids is not None:
        return ip in exact_ids
    return any(ip.startswith(p) for p in ADDON_ITEM_PRICE_ID_PREFIXES)


def _addon_exact_ids_from_env() -> frozenset[str] | None:
    raw = (os.getenv("CHARGEBEE_ADDON_IDS") or "").strip()
    if not raw:
        return None
    return frozenset(x.strip() for x in raw.split(",") if x.strip())

# Chargebee stores integer amounts in smallest currency unit except these (whole major units).
_ZERO_DECIMAL_CURRENCIES = frozenset(
    {
        "BIF",
        "BYR",
        "CLF",
        "CLP",
        "CVE",
        "DJF",
        "GNF",
        "ISK",
        "JPY",
        "KMF",
        "KRW",
        "MGA",
        "PYG",
        "RWF",
        "UGX",
        "VND",
        "VUV",
        "XAF",
        "XOF",
        "XPF",
    }
)


def _format_subscription_item_unit_price(
    si: Any, currency_code: str | None
) -> str:
    """Match Chargebee UI: major currency unit (e.g. 200.00 for USD $200), not raw cents."""
    if si is None:
        return ""
    dec = getattr(si, "unit_price_in_decimal", None)
    if dec is not None and str(dec).strip() != "":
        return str(dec).strip()
    raw = getattr(si, "unit_price", None)
    if raw is None:
        return ""
    cur = (currency_code or "USD").upper()
    if cur in _ZERO_DECIMAL_CURRENCIES:
        return str(int(raw))
    # USD, EUR, etc.: API value is in cents / minor units
    major = int(raw) / 100.0
    if major == int(major):
        return str(int(major))
    return f"{major:.2f}"


def get_client() -> Chargebee:
    """Build Chargebee client from environment."""
    site = (os.getenv("CHARGEBEE_SITE") or "").strip()
    api_key = (os.getenv("CHARGEBEE_API_KEY") or "").strip()
    if not site or not api_key:
        raise ValueError(
            "Chargebee credentials not set. Edit the .env file in the project folder\n"
            "and set CHARGEBEE_SITE and CHARGEBEE_API_KEY.\n"
            "Get the API key from Chargebee: Settings → API Keys."
        )
    if site == "your-site" or api_key == "your_api_key":
        raise ValueError(
            "Replace the placeholder values in .env with your real Chargebee credentials:\n"
            "CHARGEBEE_SITE=your-chargebee-site-name\n"
            "CHARGEBEE_API_KEY=your_actual_api_key\n"
            "Get these from Chargebee: Settings → API Keys."
        )
    # SDK appends .chargebee.com — use only the site name (e.g. dazos, not dazos.chargebee.com)
    if site.lower().endswith(".chargebee.com"):
        site = site[: -len(".chargebee.com")].strip()
    return Chargebee(api_key=api_key, site=site)


def self_service_line_state_key(subscription_id: str, item_price_id: str) -> str:
    """Stable key for webhook state: one entry per subscription line (matches webhook_app)."""
    return f"{subscription_id}|{item_price_id}"


def fetch_self_service_line_quantities(
    client: Chargebee | None = None,
    item_price_ids: list[str] | None = None,
) -> dict[str, int]:
    """
    Current quantities for all CRM self-service add-on lines on active subscriptions.
    Keys match webhook state (subscription_id|item_price_id). Use to seed baselines before
    relying on webhooks (run seed_webhook_state.py on deploy or on a schedule).
    """
    if client is None:
        client = get_client()
    exact: frozenset[str] | None
    if item_price_ids is not None and len(item_price_ids) > 0:
        exact = frozenset(item_price_ids)
    else:
        exact = _addon_exact_ids_from_env()

    out: dict[str, int] = {}
    next_offset = None
    while True:
        params: dict = {"status": {"IS": "active"}}
        if next_offset is not None:
            params["offset"] = next_offset
        response = client.Subscription.list(params)
        for list_item in response.list:
            sub = list_item.subscription
            sid = getattr(sub, "id", None)
            if not sid:
                continue
            items = getattr(sub, "subscription_items", None) or []
            for si in items:
                if si is None:
                    continue
                ip_id = getattr(si, "item_price_id", None)
                itype = getattr(si, "item_type", None)
                if not _crm_addon_line_matches(ip_id, itype, exact):
                    continue
                qty = getattr(si, "quantity", None)
                try:
                    q = int(qty) if qty is not None else 0
                except (TypeError, ValueError):
                    q = 0
                out[self_service_line_state_key(str(sid), str(ip_id))] = q
        next_offset = getattr(response, "next_offset", None)
        if not next_offset:
            break
    return out


def fetch_customers_with_addons(
    client: Chargebee | None = None,
    item_price_ids: list[str] | None = None,
) -> list[list]:
    """
    Find CRM Self-Service USD add-on lines (item_price_id prefix match, or exact ids from env
    CHARGEBEE_ADDON_IDS / item_price_ids arg). Returns one row per line: company, customer_id,
    add_on_item, quantity, unit_price, date_added. Only active subscriptions.
    """
    if client is None:
        client = get_client()
    exact: frozenset[str] | None
    if item_price_ids is not None and len(item_price_ids) > 0:
        exact = frozenset(item_price_ids)
    else:
        exact = _addon_exact_ids_from_env()

    rows: list[list] = []
    headers = ["company", "customer_id", "add_on_item", "quantity", "unit_price", "date_added"]
    rows.append(headers)

    next_offset = None
    while True:
        params = {
            "status": {"IS": "active"},
        }
        if next_offset is not None:
            params["offset"] = next_offset
        response = client.Subscription.list(params)
        for list_item in response.list:
            sub = list_item.subscription
            cust = getattr(list_item, "customer", None)
            cid = getattr(sub, "customer_id", None)
            if not cid:
                continue
            company = getattr(cust, "company", "") or "" if cust else ""
            # Use subscription created_at as "date added" (Chargebee has no per-line date in list)
            ts = getattr(sub, "created_at", None)
            date_added = ""
            if ts is not None:
                try:
                    date_added = datetime.utcfromtimestamp(int(ts)).strftime("%Y-%m-%d")
                except (TypeError, ValueError, OSError):
                    date_added = str(ts)
            currency = getattr(sub, "currency_code", None) or "USD"
            items = getattr(sub, "subscription_items", None) or []
            for si in items:
                if si is None:
                    continue
                ip_id = getattr(si, "item_price_id", None)
                itype = getattr(si, "item_type", None)
                if not _crm_addon_line_matches(ip_id, itype, exact):
                    continue
                qty = getattr(si, "quantity", None)
                quantity = str(qty) if qty is not None else ""
                unit_price = _format_subscription_item_unit_price(si, currency)
                rows.append([company, cid, ip_id or "", quantity, unit_price, date_added])
        next_offset = getattr(response, "next_offset", None)
        if not next_offset:
            break

    return rows


def _item_price_to_row(ip: Any) -> list:
    """Convert one ItemPrice to a flat row for the sheet."""
    return [
        getattr(ip, "id", "") or "",
        getattr(ip, "item_id", "") or "",
        getattr(ip, "name", "") or "",
        getattr(ip, "pricing_model", "") or "",
        getattr(ip, "price", "") or "",
        getattr(ip, "currency_code", "") or "",
        getattr(ip, "period_unit", "") or "",
        getattr(ip, "period", "") or "",
        getattr(ip, "external_name", "") or "",
        getattr(ip, "status", "") or "",
        getattr(ip, "item_type", "") or "",
    ]


def fetch_item_prices(client: Chargebee | None = None) -> list[list]:
    """
    List all item prices from Chargebee (includes add-ons).
    Returns a list of rows; first row is headers.
    """
    if client is None:
        client = get_client()

    headers = [
        "id",
        "item_id",
        "name",
        "pricing_model",
        "price",
        "currency_code",
        "period_unit",
        "period",
        "external_name",
        "status",
        "item_type",
    ]
    rows = [headers]

    next_offset = None
    while True:
        params = {}
        if next_offset is not None:
            params["offset"] = next_offset

        response = client.ItemPrice.list(params)
        for list_item in response.list:
            ip = list_item.item_price
            rows.append(_item_price_to_row(ip))

        next_offset = getattr(response, "next_offset", None)
        if not next_offset:
            break

    return rows
