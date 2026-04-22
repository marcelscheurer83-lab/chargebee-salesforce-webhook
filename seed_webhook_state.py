"""
Merge current Chargebee self-serve line quantities into webhook_state.json.

Run after deploy or on a schedule so the webhook already knows baselines and the first
real increase creates an Opportunity (not a silent baseline).

Requires CHARGEBEE_SITE + CHARGEBEE_API_KEY. Same WEBHOOK_STATE_PATH as webhook_app.

Examples:
  python seed_webhook_state.py
  railway run python seed_webhook_state.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv

from chargebee_client import fetch_self_service_line_quantities, get_client

load_dotenv(Path(__file__).resolve().parent / ".env")

_DEFAULT_STATE = Path(__file__).resolve().parent / "webhook_state.json"


def main() -> None:
    path = Path(os.getenv("WEBHOOK_STATE_PATH", str(_DEFAULT_STATE)))
    client = get_client()
    from_api = fetch_self_service_line_quantities(client)
    if not from_api:
        print("No matching self-service lines on active subscriptions (check prefixes / CHARGEBEE_ADDON_IDS).")

    if path.exists():
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            state = {}
    else:
        state = {}

    state.setdefault("processed_event_ids", [])
    merged = dict(state.get("line_quantities") or {})
    merged.update(from_api)
    state["line_quantities"] = merged

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(path)
    print(f"Seeded {len(from_api)} subscription lines into {path}")


if __name__ == "__main__":
    main()
