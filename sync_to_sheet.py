"""
Chargebee Self-Service Add-Ons — sync Chargebee data to Google Sheets.

Run: python sync_to_sheet.py
"""

from chargebee_client import fetch_customers_with_addons, get_client
from sheets_client import write_to_sheet


def main() -> None:
    print("Fetching customers with CRM add-on subscriptions from Chargebee...")
    client = get_client()
    rows = fetch_customers_with_addons(client)
    print(f"Fetched {len(rows) - 1} customers (+ header).")

    if len(rows) <= 1:
        print("No rows found. Check Chargebee data or CHARGEBEE_ADDON_IDS in chargebee_client.py / .env.")
        return

    print("Writing to Google Sheet...")
    write_to_sheet(
        rows,
        tab_name="Customers with CRM Add-Ons",
        append_timestamp=True,
    )
    print("Done.")


if __name__ == "__main__":
    main()
