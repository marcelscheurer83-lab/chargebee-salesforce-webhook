# Running the sync at the end of every day (without your machine)

The sync can run automatically in the cloud so it updates every day even when your PC is off. Two options:

---

## Option A: Google Apps Script (recommended — no extra accounts)

The script runs **inside your Google Sheet** on a schedule. No GitHub, no server.

### Setup

1. **Open your spreadsheet** (the one you already use for the sync).

2. **Extensions → Apps Script.** Delete any existing code in `Code.gs`.

3. **Copy the contents** of `apps-script/Code.gs` from this project into the Apps Script editor and save.

4. **Set script properties** (store your secrets here, not in the sheet):
   - **File → Project properties → Script properties**
   - Add these properties (one per row):

   | Property          | Value                                      |
   |-------------------|--------------------------------------------|
   | `CHARGEBEE_SITE`  | Your site name, e.g. `dazos` (no `.chargebee.com`) |
   | `CHARGEBEE_API_KEY` | Your Chargebee API key                   |
   | `ADDON_IDS`       | (Optional) Comma-separated **exact** item price IDs only. **Leave empty** to match all CRM Self‑Service USD add-ons by prefix (recommended). |

5. **Add a daily trigger**
   - Click the **clock icon** (Triggers) in the left sidebar.
   - **Add Trigger**
   - Function: **`runDailySync`**
   - Event: **Time-driven → Day timer**
   - Time: **11:55 PM to 12:00 AM** (or your preferred “end of day”).
   - Save.

6. **First run**
   - In the editor: **Run → runManualSync** (or **runDailySync**).
   - When prompted, **Authorize** the script to access your spreadsheet and external APIs (Chargebee).
   - You should see an alert when the sync finishes.

After that, the trigger will run the sync at the chosen time every day. The script writes to the **same spreadsheet** and renames the tab to `Customers with CRM Add-Ons_YYYY-MM-DD_HH-MM-SS`.

---

## Option B: GitHub Actions (keeps using the Python script)

The same Python sync runs on **GitHub’s servers** on a schedule.

### Setup

1. **Push this repo to GitHub** (create a repo and push, or use an existing one).

2. **Add repository secrets**
   - Repo → **Settings → Secrets and variables → Actions**
   - **New repository secret** for each:

   | Secret name              | Value |
   |--------------------------|--------|
   | `CHARGEBEE_SITE`         | e.g. `dazos` |
   | `CHARGEBEE_API_KEY`      | Your Chargebee API key |
   | `GOOGLE_SHEET_ID`        | Spreadsheet ID from the sheet URL |
   | `GOOGLE_CREDENTIALS_JSON`| **Entire contents** of your service account JSON file (paste the whole JSON as the secret value) |

3. **Schedule**
   - The workflow is in `.github/workflows/daily-sync.yml`.
   - It runs at **23:55 UTC** every day. To change the time, edit the `cron` line in that file (e.g. `55 23 * * *` = 23:55 UTC).

4. **Manual run**
   - **Actions** tab → **Daily Chargebee sync** → **Run workflow**.

The workflow uses your secrets to build `.env` and the credentials file, then runs `python sync_to_sheet.py`. Your spreadsheet must be shared with the **service account email** (e.g. `...@....iam.gserviceaccount.com`) as in your current setup.

---

## Summary

| | Apps Script | GitHub Actions |
|---|-------------|----------------|
| Runs in | Your Google Sheet | GitHub’s cloud |
| Needs | Google account only | GitHub account + repo |
| Code | JavaScript in Sheet | Same Python as local |
| Secrets | Script properties | Repository secrets |
| Best for | Simplest “set and forget” | When you want to keep one Python codebase |

Use **Option A** if you want the least setup and are fine with the script living in the sheet. Use **Option B** if you prefer to keep everything in Python and already use (or want to use) GitHub.
