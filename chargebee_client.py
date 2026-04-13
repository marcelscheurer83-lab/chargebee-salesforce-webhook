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

# Add-on item price IDs to find customers for (CRM add-ons from your sheet)
ADDON_ITEM_PRICE_IDS = [
    "Additional-CRM-User-Self-Service-USD-Monthly",
    "Additional-CRM-User-Self-Serve-USD-Monthly",
]


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


def fetch_customers_with_addons(
    client: Chargebee | None = None,
    item_price_ids: list[str] | None = None,
) -> list[list]:
    """
    Find all subscription line items that are one of the given add-on item_price_ids.
    Returns one row per add-on line with: company, customer_id, add_on_item, quantity,
    unit_price, date_added. unit_price uses unit_price_in_decimal when set, else unit_price
    (smallest currency unit, e.g. cents for USD).
    Only includes subscriptions with status "active" (excludes in_trial, non_renewing, cancelled).
    Chargebee's list filter "item_price_id" applies to the plan only, so we list subscriptions
    and check subscription_items client-side.
    """
    if client is None:
        client = get_client()
    ids = item_price_ids or ADDON_ITEM_PRICE_IDS
    ids_set = set(ids)

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
            items = getattr(sub, "subscription_items", None) or []
            for si in items:
                ip_id = getattr(si, "item_price_id", None)
                if ip_id not in ids_set:
                    continue
                qty = getattr(si, "quantity", None)
                quantity = str(qty) if qty is not None else ""
                dec = getattr(si, "unit_price_in_decimal", None) or ""
                raw = getattr(si, "unit_price", None)
                if dec:
                    unit_price = str(dec)
                elif raw is not None:
                    unit_price = str(raw)
                else:
                    unit_price = ""
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
