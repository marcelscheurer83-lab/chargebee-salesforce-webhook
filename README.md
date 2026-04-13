# Chargebee Self-Service Add-Ons

Pulls data from the [Chargebee API](https://apidocs.chargebee.com/docs/api) into a Google Sheet. Use this to keep a spreadsheet of your add-ons (item prices) for self-service or reporting.

## Setup

### 1. Python

- Python **3.11+**
- Create a virtual environment (recommended):

  ```bash
  cd "Chargebee Self-Service Add-Ons"
  python -m venv .venv
  .venv\Scripts\activate
  pip install -r requirements.txt
  ```

### 2. Chargebee

- In Chargebee: **Settings → API Keys**.
- Create or copy an API key and note your **site name** (e.g. `mycompany` for `mycompany.chargebee.com`).

### 3. Google Sheet

- **Create a Google Sheet** and note its ID from the URL:  
  `https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit`
- Use a **Service Account** so the script can run without a browser:
  1. [Google Cloud Console](https://console.cloud.google.com/) → your project → **APIs & Services** → **Credentials**.
  2. **Create credentials** → **Service account**.
  3. Create a key (JSON) and download it.
  4. Share the Google Sheet with the service account email (e.g. `xxx@yyy.iam.gserviceaccount.com`) as **Editor**.

### 4. Environment

- Copy `.env.example` to `.env` and fill in:

  ```env
  CHARGEBEE_SITE=your-site
  CHARGEBEE_API_KEY=your_api_key
  GOOGLE_CREDENTIALS_PATH=C:\path\to\your-service-account.json
  GOOGLE_SHEET_ID=your_google_sheet_id
  ```

  Optional: `GOOGLE_SHEET_TAB=Chargebee Add-Ons` (default tab name).

## Run

```bash
python sync_to_sheet.py
```

This will:

1. Fetch all **item prices** (including add-ons) from Chargebee.
2. Clear the configured sheet tab and write the data (headers + one row per item price).

## What gets synced

- **Item prices** from Chargebee: id, item_id, name, pricing_model, price, currency_code, period_unit, period, external_name, status, item_type.

You can extend `chargebee_client.py` to also pull subscriptions, customers, or other entities and add more sheets/tabs if needed.

## Security

- Keep `.env` and the service account JSON **out of version control** (they are in `.gitignore`).
- Restrict the Chargebee API key to the minimum scopes needed (e.g. read-only if you only sync data).
