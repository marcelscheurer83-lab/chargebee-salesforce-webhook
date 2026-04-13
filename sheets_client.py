"""Write data to Google Sheets."""

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# Load .env from the project folder
_load_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(_load_env_path)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


def get_sheet_client():
    """Build gspread client using service account credentials."""
    path = os.getenv("GOOGLE_CREDENTIALS_PATH")
    if not path or not os.path.isfile(path):
        raise ValueError(
            "Set GOOGLE_CREDENTIALS_PATH in .env to your service account JSON path"
        )
    creds = Credentials.from_service_account_file(path, scopes=SCOPES)
    return gspread.authorize(creds)


def write_to_sheet(
    rows: list[list[Any]],
    sheet_id: str | None = None,
    tab_name: str | None = None,
    append_timestamp: bool = False,
) -> None:
    """
    Replace sheet content with the given rows.
    Uses GOOGLE_SHEET_ID and optionally GOOGLE_SHEET_TAB from env if not passed.
    If append_timestamp is True, appends _<date>_<time> to tab_name (e.g. MyTab_2025-02-25_14-30-00).
    Only updates cell values; does not change column widths or other formatting.
    """
    sheet_id = sheet_id or os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id or sheet_id.strip() == "your_google_sheet_id":
        raise ValueError(
            "Set GOOGLE_SHEET_ID in .env to your Google Sheet ID.\n"
            "Open your sheet → copy the ID from the URL:\n"
            "  https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit\n"
            "Then share the sheet with your service account email (Editor)."
        )

    base_tab_name = tab_name or os.getenv("GOOGLE_SHEET_TAB", "Chargebee Add-Ons")
    if append_timestamp:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        tab_name = f"{base_tab_name}_{ts}"
    else:
        tab_name = base_tab_name

    client = get_sheet_client()
    workbook = client.open_by_key(sheet_id)

    # When appending timestamp: find existing tab whose title starts with base name, or create new
    if append_timestamp:
        worksheet = None
        for ws in workbook.worksheets():
            if ws.title.startswith(base_tab_name):
                worksheet = ws
                break
        if worksheet is None:
            worksheet = workbook.add_worksheet(
                title=tab_name, rows=max(len(rows) + 100, 500), cols=20
            )
        else:
            worksheet.update_title(tab_name)
    else:
        try:
            worksheet = workbook.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            worksheet = workbook.add_worksheet(
                title=tab_name, rows=max(len(rows) + 100, 500), cols=20
            )

    # Clear only values in the data range (preserves formatting), then write data
    if rows:
        num_cols = len(rows[0]) if rows else 0
        end_col = chr(ord("A") + num_cols - 1) if num_cols <= 26 else "E"
        data_range = f"A1:{end_col}{max(len(rows), 1)}"
        worksheet.batch_clear([data_range])
        worksheet.update(rows, "A1", value_input_option="USER_ENTERED")
    else:
        worksheet.batch_clear(["A1:Z1000"])

    print(f"Wrote {len(rows)} rows to sheet '{tab_name}' (id: {sheet_id})")
