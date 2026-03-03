from .config import CONFIG_FILES, REGISTRY_NAMES
"""Index JSON config files into json_config_entries table.

Flattens nested JSON to dot-path entries, infers category dimension,
and flags unknown category keys against known registry names.
"""

import hashlib
import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TextIO

from .persistence import persist_discrepancy

logger = logging.getLogger(__name__)

# JSON config files to index — configured via codegrapher.yaml or env vars
KNOWN_CONFIG_FILES = list(CONFIG_FILES)

# Files where we index metadata only (key count, not values)
METADATA_ONLY_FILES: list = []


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha1_file(filepath: Path) -> str:
    h = hashlib.sha1()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _value_type(val) -> str:
    """Classify JSON value type."""
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "bool"
    if isinstance(val, (int, float)):
        return "num"
    if isinstance(val, str):
        return "str"
    if isinstance(val, list):
        return "arr"
    if isinstance(val, dict):
        return "obj"
    return "unknown"


def _to_num(val) -> Optional[float]:
    """Extract numeric value, or None."""
    if isinstance(val, bool):
        return float(val)
    if isinstance(val, (int, float)):
        return float(val)
    return None


def _flatten_json(
    data, prefix: str = "", category: Optional[str] = None,
) -> list[dict]:
    """Recursively flatten JSON to (key_path, value_repr, value_num, value_type) entries."""
    entries = []
    if isinstance(data, dict):
        for k, v in data.items():
            key_path = f"{prefix}.{k}" if prefix else k
            vt = _value_type(v)

            if isinstance(v, dict):
                entries.extend(_flatten_json(v, key_path, category))
            elif isinstance(v, list):
                entries.append({
                    "key_path": key_path,
                    "value_repr": json.dumps(v),
                    "value_num": None,
                    "value_type": "arr",
                    "category": category,
                })
            else:
                entries.append({
                    "key_path": key_path,
                    "value_repr": str(v) if v is not None else None,
                    "value_num": _to_num(v),
                    "value_type": vt,
                    "category": category,
                })
    return entries


def _infer_category_dimension(data: dict, known_categories: set) -> bool:
    """Check if top-level keys are known category names (>50% match)."""
    if not isinstance(data, dict) or len(data) == 0:
        return False
    matches = sum(1 for k in data if k.upper().replace(" ", "_") in known_categories)
    return matches > len(data) * 0.5


def _load_known_categories(db: sqlite3.Connection) -> set:
    """Load known category names from the registries table.

    Checks all REGISTRY_NAMES configured in codegrapher.yaml.
    Returns an empty set if no registries are configured.
    """
    categories: set = set()
    if not REGISTRY_NAMES:
        return categories

    for name in REGISTRY_NAMES:
        rows = db.execute(
            "SELECT value_json, value_keys_json FROM registries WHERE name = ?",
            (name,),
        ).fetchall()
        for val_json, keys_json in rows:
            for src in (val_json, keys_json):
                if src:
                    try:
                        data = json.loads(src)
                        if isinstance(data, list):
                            categories.update(str(x).upper().replace(" ", "_") for x in data)
                        elif isinstance(data, dict):
                            categories.update(k.upper().replace(" ", "_") for k in data)
                    except (json.JSONDecodeError, TypeError):
                        pass

    return categories


def index_json_file(
    filepath: Path,
    relpath: str,
    db: sqlite3.Connection,
    known_categories: set,
    jsonl_file: Optional[TextIO] = None,
) -> int:
    """Index a single JSON config file. Returns entry count."""
    now = _now_iso()
    file_hash = _sha1_file(filepath)

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to parse %s: %s", relpath, e)
        persist_discrepancy(
            db, "JSON_PARSE_ERROR", "warn", relpath, None,
            relpath, {"error": str(e)}, jsonl_file=jsonl_file,
        )
        return 0

    if not isinstance(data, dict):
        logger.warning("Skipping %s: top-level is not object", relpath)
        return 0

    is_category_keyed = _infer_category_dimension(data, known_categories)
    count = 0

    for top_key, top_val in data.items():
        category = None
        if is_category_keyed:
            normalized = top_key.upper().replace(" ", "_")
            if normalized in known_categories:
                category = normalized
            else:
                persist_discrepancy(
                    db, "JSON_UNKNOWN_CATEGORY_KEY", "warn", relpath, None,
                    top_key, {
                        "file": relpath,
                        "key": top_key,
                        "hint": "Key looks like a category but not in known registries",
                    }, jsonl_file=jsonl_file,
                )
                category = top_key  # Store as-is

        if isinstance(top_val, dict) and is_category_keyed:
            entries = _flatten_json(top_val, top_key, category)
        else:
            entries = [{
                "key_path": top_key,
                "value_repr": json.dumps(top_val) if isinstance(top_val, (dict, list)) else str(top_val),
                "value_num": _to_num(top_val),
                "value_type": _value_type(top_val),
                "category": category,
            }]

        for entry in entries:
            db.execute(
                "INSERT INTO json_config_entries "
                "(source_file, source_hash, category, key_path, value_repr, "
                " value_num, value_type, indexed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (relpath, file_hash, entry["category"], entry["key_path"],
                 entry["value_repr"], entry["value_num"], entry["value_type"], now),
            )
            count += 1

            if jsonl_file:
                jsonl_file.write(json.dumps({
                    "type": "json_config_entry", "source_file": relpath,
                    "category": entry["category"], "key_path": entry["key_path"],
                    "value_type": entry["value_type"], "indexed_at": now,
                }, default=str) + "\n")

    logger.info("  JSON indexed: %s (%d entries, category_keyed=%s)",
                relpath, count, is_category_keyed)
    return count


def index_all_json_configs(
    root: Path,
    db: sqlite3.Connection,
    jsonl_file: Optional[TextIO] = None,
) -> int:
    """Entry point: index all known JSON config files. Returns total entry count."""
    db.execute("DELETE FROM json_config_entries")

    known_categories = _load_known_categories(db)
    total = 0

    for relpath in KNOWN_CONFIG_FILES:
        filepath = root / relpath
        if filepath.exists():
            total += index_json_file(filepath, relpath, db, known_categories, jsonl_file)
        else:
            logger.debug("JSON config not found: %s", relpath)

    # Metadata-only files: just store key count
    now = _now_iso()
    for relpath in METADATA_ONLY_FILES:
        filepath = root / relpath
        if filepath.exists():
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    file_hash = _sha1_file(filepath)
                    db.execute(
                        "INSERT INTO json_config_entries "
                        "(source_file, source_hash, category, key_path, value_repr, "
                        " value_num, value_type, indexed_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (relpath, file_hash, None, "_metadata.key_count",
                         str(len(data)), float(len(data)), "num", now),
                    )
                    total += 1
                    logger.info("  JSON metadata: %s (%d keys)", relpath, len(data))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to read metadata file %s: %s", relpath, e)

    return total
