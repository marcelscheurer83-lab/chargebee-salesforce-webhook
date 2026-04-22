"""
Merge current Chargebee self-serve line quantities into webhook_state.json.

Run after deploy or on a schedule so the webhook already knows baselines and the first
real increase creates an Opportunity (not a silent baseline).

Requires CHARGEBEE_SITE + CHARGEBEE_API_KEY. Same WEBHOOK_STATE_PATH as webhook_app.

Examples:
  python seed_webhook_state.py
  railway run python seed_webhook_state.py
  python seed_webhook_state.py --remote

HTTP (when WEBHOOK_SEED_SECRET is set on the host):
  curl -X POST -H "X-Seed-Secret: YOUR_SECRET" https://<host>/admin/seed-state

Remote (--remote): set WEBHOOK_PUBLIC_URL (e.g. https://your-app.up.railway.app) and WEBHOOK_SEED_SECRET
in .env to match the deployed service; no Chargebee vars required for that call.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv

from chargebee_client import fetch_self_service_line_quantities, get_client

_DEFAULT_STATE = Path(__file__).resolve().parent / "webhook_state.json"


def merge_chargebee_into_state_file(state_path: Path) -> int:
    """
    Pull active subscription self-serve quantities from Chargebee and merge into state_path.
    Preserves processed_event_ids. Returns count of subscription lines from the API.
    """
    client = get_client()
    from_api = fetch_self_service_line_quantities(client)

    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            state = {}
    else:
        state = {}

    state.setdefault("processed_event_ids", [])
    merged = dict(state.get("line_quantities") or {})
    merged.update(from_api)
    state["line_quantities"] = merged

    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(state_path)
    return len(from_api)


def remote_seed_via_admin_endpoint(base_url: str, secret: str, timeout_s: int = 120) -> dict:
    """POST /admin/seed-state on the deployed webhook (Railway, etc.)."""
    url = base_url.rstrip("/") + "/admin/seed-state"
    req = Request(url, method="POST", headers={"X-Seed-Secret": secret})
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode()
            return json.loads(raw) if raw.strip() else {}
    except HTTPError as e:
        detail = e.read().decode(errors="replace") if e.fp else ""
        raise SystemExit(f"HTTP {e.code} {e.reason}: {detail or url}") from e
    except URLError as e:
        raise SystemExit(f"Request failed: {e.reason}") from e


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed webhook line_quantities from Chargebee or remote admin.")
    parser.add_argument(
        "--remote",
        action="store_true",
        help="Call deployed POST /admin/seed-state (WEBHOOK_PUBLIC_URL + WEBHOOK_SEED_SECRET)",
    )
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent / ".env")

    if args.remote:
        base = (os.getenv("WEBHOOK_PUBLIC_URL") or "").strip()
        secret = (os.getenv("WEBHOOK_SEED_SECRET") or "").strip()
        if not base or not secret:
            raise SystemExit(
                "For --remote, set WEBHOOK_PUBLIC_URL and WEBHOOK_SEED_SECRET in .env "
                "(secret must match the value on the host)."
            )
        out = remote_seed_via_admin_endpoint(base, secret)
        print(json.dumps(out, indent=2))
        return

    path = Path(os.getenv("WEBHOOK_STATE_PATH", str(_DEFAULT_STATE)))
    n = merge_chargebee_into_state_file(path)
    if n == 0:
        print("No matching self-service lines on active subscriptions (check prefixes / CHARGEBEE_ADDON_IDS).")
    print(f"Seeded {n} subscription lines into {path}")


if __name__ == "__main__":
    main()
