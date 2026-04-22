"""
Print Chargebee item price IDs that look like the Additional CRM Self-Service add-ons.
Run: python list_crm_item_price_ids.py

Use the "id" column for reference; sync uses prefix matching by default (or set CHARGEBEE_ADDON_IDS for exact ids).
subscription line items exactly (internal display names in the UI are not the API id).
"""

from chargebee_client import get_client


def main() -> None:
    client = get_client()
    seen: set[str] = set()
    next_offset = None
    while True:
        params = {}
        if next_offset is not None:
            params["offset"] = next_offset
        response = client.ItemPrice.list(params)
        for list_item in response.list:
            ip = list_item.item_price
            raw_id = getattr(ip, "id", None) or ""
            iid = raw_id.lower()
            name = getattr(ip, "name", None) or ""
            if not iid:
                continue
            if "additional-crm" not in iid:
                continue
            if "self-service" not in iid and "self-serve" not in iid:
                continue
            if raw_id not in seen:
                seen.add(raw_id)
                print(f"{raw_id}\t{name}")
        next_offset = getattr(response, "next_offset", None)
        if not next_offset:
            break
    if not seen:
        print("No matches. Try broadening needles in list_crm_item_price_ids.py or check Item catalog.")


if __name__ == "__main__":
    main()
