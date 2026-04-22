"""
Salesforce field / integration settings from one JSON file or SF_CONFIG_JSON blob.

Resolution order for each key:
  1) process environment (Railway, docker --env, etc.) — wins
  2) merged JSON: salesforce_field_map.json in app dir, then SF_CONFIG_PATH file, then SF_CONFIG_JSON

Keep secrets (SF_PASSWORD, CHARGEBEE_API_KEY, …) in the host env; the map file is for API names and non-secret options.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

log = logging.getLogger("sf_config")

_MERGED: dict[str, str] = {}


def _load_json_into(merged: dict[str, str], path: Path) -> None:
    if not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("Could not read SF config file %s: %s", path, e)
        return
    if not isinstance(data, dict):
        return
    for k, v in data.items():
        if not isinstance(k, str) or not k.strip():
            continue
        if v is None:
            continue
        key = k.strip()
        if isinstance(v, bool):
            merged[key] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            merged[key] = str(v)
        else:
            s = str(v).strip()
            if s:
                merged[key] = s


def init_sf_config() -> None:
    global _MERGED
    merged: dict[str, str] = {}
    _load_json_into(merged, Path(__file__).resolve().parent / "salesforce_field_map.json")
    path_override = (os.getenv("SF_CONFIG_PATH") or "").strip()
    if path_override:
        _load_json_into(merged, Path(path_override))
    raw = (os.getenv("SF_CONFIG_JSON") or "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                for k, v in data.items():
                    if not isinstance(k, str) or not k.strip():
                        continue
                    if v is None:
                        continue
                    key = k.strip()
                    if isinstance(v, bool):
                        merged[key] = "true" if v else "false"
                    elif isinstance(v, (int, float)):
                        merged[key] = str(v)
                    else:
                        s = str(v).strip()
                        if s:
                            merged[key] = s
        except json.JSONDecodeError:
            log.warning("SF_CONFIG_JSON is not valid JSON; skipped")
    _MERGED = merged
    if _MERGED:
        log.info(
            "Loaded %s Salesforce config keys from file / SF_CONFIG_JSON (env vars override per key)",
            len(_MERGED),
        )


def sf_cfg(key: str, default: str = "") -> str:
    """Config value: environment first, then merged JSON map, then default."""
    e = (os.getenv(key) or "").strip()
    if e:
        return e
    return (_MERGED.get(key) or default).strip()
