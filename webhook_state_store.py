"""
Persistence for webhook baselines (processed_event_ids + line_quantities).

- file (default): WEBHOOK_STATE_PATH JSON file
- postgres: single-row JSON document in table webhook_app_state (Railway DATABASE_URL, etc.)

Set WEBHOOK_STATE_BACKEND=postgres and DATABASE_URL or WEBHOOK_STATE_DATABASE_URL.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Protocol


def default_state_dict() -> dict[str, Any]:
    return {"processed_event_ids": [], "line_quantities": {}}


def normalize_state(raw: dict[str, Any]) -> dict[str, Any]:
    out = dict(raw)
    pe = out.get("processed_event_ids")
    out["processed_event_ids"] = list(pe) if isinstance(pe, list) else []
    lq = out.get("line_quantities")
    if not isinstance(lq, dict):
        out["line_quantities"] = {}
    else:
        fixed: dict[str, int] = {}
        for k, v in lq.items():
            try:
                fixed[str(k)] = int(v)
            except (TypeError, ValueError):
                fixed[str(k)] = 0
        out["line_quantities"] = fixed
    return out


class WebhookStateStore(Protocol):
    def load(self) -> dict[str, Any]: ...
    def save(self, state: dict[str, Any]) -> None: ...


class FileWebhookStateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return default_state_dict()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return default_state_dict()
        if not isinstance(raw, dict):
            return default_state_dict()
        return normalize_state(raw)

    def save(self, state: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=0), encoding="utf-8")
        tmp.replace(self.path)


class PostgresWebhookStateStore:
    """One row (id=1) holding the full state object as JSONB."""

    _TABLE = "webhook_app_state"

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def _connect(self):  # noqa: ANN202
        import psycopg

        return psycopg.connect(self.dsn)

    def _ensure_table(self, conn) -> None:  # noqa: ANN001
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._TABLE} (
                    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
                    state JSONB NOT NULL DEFAULT '{{}}'::jsonb
                )
                """
            )
        conn.commit()

    def load(self) -> dict[str, Any]:
        with self._connect() as conn:
            self._ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(f"SELECT state FROM {self._TABLE} WHERE id = 1")
                row = cur.fetchone()
        if not row or row[0] is None:
            return default_state_dict()
        raw = row[0]
        if not isinstance(raw, dict):
            return default_state_dict()
        return normalize_state(raw)

    def save(self, state: dict[str, Any]) -> None:
        import psycopg
        from psycopg.types.json import Json

        with self._connect() as conn:
            self._ensure_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {self._TABLE} (id, state) VALUES (1, %s)
                    ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state
                    """,
                    (Json(state),),
                )
            conn.commit()


def build_webhook_state_store(
    default_file: Path,
) -> tuple[WebhookStateStore, str]:
    """
    Returns (store, label) where label is ``file`` or ``postgres``.
    """
    backend = (os.getenv("WEBHOOK_STATE_BACKEND") or "file").strip().lower()
    if backend in ("postgres", "postgresql", "sql"):
        dsn = (os.getenv("WEBHOOK_STATE_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
        if not dsn:
            raise RuntimeError(
                "WEBHOOK_STATE_BACKEND=postgres requires WEBHOOK_STATE_DATABASE_URL or DATABASE_URL"
            )
        return PostgresWebhookStateStore(dsn), "postgres"
    path = Path(os.getenv("WEBHOOK_STATE_PATH", str(default_file)))
    return FileWebhookStateStore(path), "file"
