"""
Merge current Chargebee self-serve line quantities into webhook_state.json.

Run after deploy or on a schedule so the webhook already knows baselines and the first
real increase creates an Opportunity (not a silent baseline).

Requires CHARGEBEE_SITE + CHARGEBEE_API_KEY. Same WEBHOOK_STATE_PATH as webhook_app.

Examples:
  python seed_webhook_state.py
  railway run python seed_webhook_state.py

HTTP (when WEBHOOK_SEED_SECRET is set on the host):
  curl -X POST -H "X-Seed-Secret: YOUR_SECRET" https://<host>/admin/seed-state
"""

from __future__ import annotations

import json
import os
from pathlib import Path

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


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    path = Path(os.getenv("WEBHOOK_STATE_PATH", str(_DEFAULT_STATE)))
    n = merge_chargebee_into_state_file(path)
    if n == 0:
        print("No matching self-service lines on active subscriptions (check prefixes / CHARGEBEE_ADDON_IDS).")
    print(f"Seeded {n} subscription lines into {path}")


if __name__ == "__main__":
    main()
