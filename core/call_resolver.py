"""Post-indexing pass to resolve calls.callee_fqn.

Resolution priority (descending confidence):
  same_module     0.98  callee_name_raw matches a symbol in the same module
  method_self     0.95  self.method() resolved to enclosing class method
  import_alias    0.97  callee_name_raw matches alias/imported name via from-import
  qualified       0.90  X.foo where X is an import alias, foo is in that module
  global_unique   0.80  bare name matches exactly one symbol across all modules
  heuristic       0.30  bare name matches >1 symbol — candidates stored, no fqn set

Rules:
  - callee_fqn written back to calls only when confidence >= 0.95.
  - Dunder calls (__xxx__) are silently skipped.
  - Heuristic hits stored in call_resolution only (with candidates_json).
"""

from __future__ import annotations

import builtins as _builtins_mod
import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TextIO

logger = logging.getLogger(__name__)

CONFIDENCE_THRESHOLD = 0.95

# ---------------------------------------------------------------------------
# Noise classification sets
# ---------------------------------------------------------------------------

# Python built-in names — calls to these are skipped, not "unresolved"
_PYTHON_BUILTINS: frozenset[str] = frozenset(dir(_builtins_mod))

# Top-level package roots that are definitely external to the project.
# Calls that resolve to FQNs starting with these are classified as
# "external_resolved" rather than "internal_resolved".
# Calls to unresolved names whose import prefix matches these are "external".
_EXTERNAL_PACKAGE_ROOTS: frozenset[str] = frozenset({
    # Data science / numeric
    "pandas", "numpy", "scipy", "matplotlib", "seaborn", "plotly",
    "sklearn", "torch", "tensorflow", "cudf", "cupy", "numba", "statsmodels",
    # Finance / trading
    "ib_insync", "yfinance", "ta", "talib",
    # Qt / GUI
    "PySide6", "PyQt5", "PyQt6",
    # DB / web / network
    "sqlalchemy", "requests", "aiohttp", "httpx", "flask", "fastapi",
    "websocket", "websockets",
    # Serialisation / config
    "yaml", "toml", "dotenv",
    # Stdlib modules (present as imports but no .py source in the repo)
    "os", "sys", "re", "json", "logging", "datetime", "pathlib",
    "collections", "itertools", "functools", "typing", "abc", "math",
    "random", "time", "threading", "multiprocessing", "concurrent",
    "asyncio", "enum", "dataclasses", "warnings", "traceback", "io",
    "csv", "sqlite3", "hashlib", "struct", "copy", "gc", "inspect",
    "importlib", "contextlib", "operator", "string", "textwrap",
    "subprocess", "shutil", "tempfile", "glob", "fnmatch", "stat",
    "pickle", "shelve", "zipfile", "tarfile", "gzip", "base64",
    "urllib", "http", "email", "html", "xml", "socket", "ssl",
    "argparse", "configparser", "pprint", "difflib", "uuid",
    "calendar", "locale", "gettext", "codecs", "unicodedata",
    "queue", "heapq", "bisect", "array", "weakref", "types",
    "dis", "ast", "token", "tokenize", "linecache", "platform",
    "signal", "atexit", "ctypes", "mmap", "psutil",
})

# Internal repo root prefixes — loaded from config (overridable via codegrapher.yaml)
from .config import INTERNAL_ROOTS as _INTERNAL_ROOTS

# Common local variable names that conventionally hold external library objects.
# e.g. df.resample() — 'df' is almost always a pandas DataFrame.
# Allows classification of unresolved dotted calls as external noise
# without requiring full type inference.
_EXTERNAL_VAR_NAMES: frozenset[str] = frozenset({
    # pandas / numpy
    "df", "dfs", "data", "bar_df", "bars_df", "prices", "series",
    "ohlcv", "hist", "result_df", "merged", "pivot", "grouped",
    "arr", "vals", "matrix", "mask", "idx",
    # matplotlib / plotting
    "ax", "axes", "fig", "figure",
    # sqlite3 / DB
    "conn", "cursor", "cur", "db", "session",
    # HTTP / requests
    "response", "resp", "res",
    # asyncio
    "loop", "fut", "coro",
    # ib_insync objects
    "contract", "ticker", "ib_order", "bars",
    # Qt / PySide6 widgets
    "widget", "layout", "btn", "label", "dialog",
    # logging — logger.info/debug/warning/error are never internal calls
    "logger", "log",
    # Generic iterators commonly wrapping external objects
    "row", "col", "elem", "node",
})

# Method name suffixes that are never project-internal calls.
# Applies to any dotted call (X.method) where the resolver found no match.
# e.g. config.get(), lines.append(), self.logger.info()
# Risk: if a project class defines a method with one of these names, it would
# be misclassified. That risk is low — internal methods typically use descriptive names.
_EXTERNAL_METHOD_SUFFIXES: frozenset[str] = frozenset({
    # logging.Logger
    "info", "debug", "warning", "warn", "error", "critical", "exception",
    # dict protocol
    "get", "setdefault", "pop", "popitem", "update",
    "keys", "values", "items", "clear",
    # list protocol
    "append", "extend", "insert", "remove", "sort", "reverse", "index", "count",
    # string protocol
    "format", "format_map", "join", "split", "rsplit", "splitlines",
    "strip", "lstrip", "rstrip", "replace", "startswith", "endswith",
    "encode", "decode", "upper", "lower", "title", "capitalize",
    "zfill", "ljust", "rjust", "center",
    # file-like
    "read", "readline", "readlines", "write", "writelines", "close", "flush",
    "seek", "tell", "truncate",
    # DB cursors (sqlite3, sqlalchemy)
    "fetchall", "fetchone", "fetchmany", "commit", "rollback", "rollback",
    # set protocol
    "add", "discard", "union", "intersection", "difference", "issubset",
    # general stdlib
    "copy", "deepcopy", "acquire", "release",
})


def _classify_fqn(fqn: str) -> str:
    """Return 'internal' or 'external' for a resolved FQN."""
    for root in _INTERNAL_ROOTS:
        if fqn.startswith(root):
            return "internal"
    top = fqn.split(".")[0]
    if top in _EXTERNAL_PACKAGE_ROOTS:
        return "external"
    # Short bare names that resolved via global_unique but are not in internal roots
    # are likely external or test scaffolding — classify conservatively as external
    return "external" if "." not in fqn else "internal"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_dunder(name: str) -> bool:
    bare = name.split(".")[-1]
    return bare.startswith("__") and bare.endswith("__")


# ---------------------------------------------------------------------------
# Index builders (called once, reused per-call)
# ---------------------------------------------------------------------------

def _build_from_import_map(db: sqlite3.Connection) -> dict:
    """Return {module_id: {local_name: resolved_fqn}} for from-imports."""
    result: dict[int, dict] = {}
    rows = db.execute(
        "SELECT module_id, imported, alias, from_module FROM imports "
        "WHERE is_from = 1 AND from_module IS NOT NULL"
    ).fetchall()
    for mod_id, imported, alias, from_module in rows:
        local = alias if alias else imported
        result.setdefault(mod_id, {})[local] = f"{from_module}.{imported}"
    return result


def _build_module_alias_map(db: sqlite3.Connection) -> dict:
    """Return {module_id: {alias: dotted_module_path}} for plain import statements."""
    result: dict[int, dict] = {}
    rows = db.execute(
        "SELECT module_id, imported, alias FROM imports WHERE is_from = 0"
    ).fetchall()
    for mod_id, imported, alias in rows:
        local = alias if alias else imported.split(".")[-1]
        result.setdefault(mod_id, {})[local] = imported
    return result


def _build_same_module_symbols(db: sqlite3.Connection) -> dict:
    """Return {module_id: {name: fqn}} for function/class/method symbols."""
    result: dict[int, dict] = {}
    rows = db.execute(
        "SELECT module_id, name, fqn FROM symbols "
        "WHERE kind IN ('function', 'class', 'method')"
    ).fetchall()
    for mod_id, name, fqn in rows:
        # Keep first per name per module (function preferred over method)
        result.setdefault(mod_id, {}).setdefault(name, fqn)
    return result


def _build_global_name_index(db: sqlite3.Connection) -> dict:
    """Return {bare_name: [(fqn, kind), ...]} across all modules."""
    idx: dict[str, list] = {}
    rows = db.execute(
        "SELECT name, fqn, kind FROM symbols "
        "WHERE kind IN ('function', 'class', 'method')"
    ).fetchall()
    for name, fqn, kind in rows:
        idx.setdefault(name, []).append((fqn, kind))
    return idx


def _build_class_method_map(db: sqlite3.Connection) -> dict:
    """Return {module_id: {class_fqn: {method_name: method_fqn}}}."""
    result: dict[int, dict] = {}
    rows = db.execute(
        "SELECT s.module_id, s.name, s.fqn, p.fqn "
        "FROM symbols s JOIN symbols p ON s.parent_symbol_id = p.id "
        "WHERE s.kind = 'method' AND p.kind = 'class'"
    ).fetchall()
    for mod_id, name, fqn, class_fqn in rows:
        result.setdefault(mod_id, {}).setdefault(class_fqn, {})[name] = fqn
    return result


def _build_caller_to_class(db: sqlite3.Connection) -> dict:
    """Return {symbol_id: class_fqn} mapping methods to their parent class."""
    result = {}
    rows = db.execute(
        "SELECT s.id, p.fqn FROM symbols s "
        "JOIN symbols p ON s.parent_symbol_id = p.id "
        "WHERE s.kind = 'method' AND p.kind = 'class'"
    ).fetchall()
    for sym_id, class_fqn in rows:
        result[sym_id] = class_fqn
    return result


# Patterns for scanning __init__ bodies to infer attribute types
_INIT_ASSIGN_PAT = re.compile(r"self\.(\w+)\s*=\s*([A-Z]\w+)\s*\(")
_INIT_ANNOT_PAT  = re.compile(r"self\.(\w+)\s*:\s*([A-Z]\w+)")


def _build_init_attr_map(db: sqlite3.Connection, root: Path) -> dict[str, dict[str, str]]:
    """Return {class_fqn: {attr_name: bare_type_name}} inferred from __init__ bodies.

    Scans __init__ methods for two patterns:
      self.attr = ClassName(...)    → HIGH confidence
      self.attr: ClassName          → HIGH confidence (explicit annotation)

    Only uppercase-starting names are treated as types (filters out plain values).
    Only scans src/ files for project-internal classes.
    """
    result: dict[str, dict[str, str]] = {}
    rows = db.execute(
        "SELECT s.lineno, s.end_lineno, p.fqn, m.path "
        "FROM symbols s "
        "JOIN symbols p ON s.parent_symbol_id = p.id "
        "JOIN modules m ON s.module_id = m.id "
        "WHERE s.name = '__init__' AND s.kind = 'method' "
        "  AND p.kind = 'class' AND m.path LIKE 'src/%'"
    ).fetchall()

    for lineno, end_lineno, class_fqn, rel_path in rows:
        fpath = root / rel_path
        if not fpath.exists():
            continue
        try:
            lines = fpath.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        # Extract the __init__ body lines (lineno is 1-based)
        body_start = lineno          # line of 'def __init__'
        body_end   = end_lineno or body_start + 60
        body = "\n".join(lines[body_start : min(body_end, len(lines))])

        class_attrs = result.setdefault(class_fqn, {})
        for m in _INIT_ASSIGN_PAT.finditer(body):
            class_attrs.setdefault(m.group(1), m.group(2))
        for m in _INIT_ANNOT_PAT.finditer(body):
            class_attrs.setdefault(m.group(1), m.group(2))

    return result


def _build_global_type_methods(db: sqlite3.Connection) -> dict[str, dict[str, str]]:
    """Return {bare_class_name: {method_name: method_fqn}} for self.attr.method() lookup.

    Used to resolve attr-chain calls: once we know self.foo is a RiskManager,
    look up 'RiskManager' here to find method FQNs.
    """
    result: dict[str, dict[str, str]] = {}
    rows = db.execute(
        "SELECT p.name, s.name, s.fqn "
        "FROM symbols s JOIN symbols p ON s.parent_symbol_id = p.id "
        "WHERE s.kind = 'method' AND p.kind = 'class'"
    ).fetchall()
    for class_name, method_name, method_fqn in rows:
        result.setdefault(class_name, {})[method_name] = method_fqn
    return result


# ---------------------------------------------------------------------------
# Single-call resolution
# ---------------------------------------------------------------------------

def _resolve_one(
    callee_raw: str,
    module_id: int,
    caller_sym_id: Optional[int],
    from_imports: dict,
    mod_aliases: dict,
    same_mod_syms: dict,
    global_idx: dict,
    class_methods: dict,
    caller_classes: dict,
    attr_type_map: dict | None = None,
    global_type_methods: dict | None = None,
) -> tuple[Optional[str], str, float, Optional[list]]:
    """Resolve one call. Returns (fqn, method, confidence, candidates)."""

    mod_from = from_imports.get(module_id, {})
    mod_alias = mod_aliases.get(module_id, {})
    mod_syms = same_mod_syms.get(module_id, {})
    mod_classes = class_methods.get(module_id, {})

    # --- self.method() ---
    if callee_raw.startswith("self."):
        method_name = callee_raw[5:]
        # --- self.attr.method() — two-dot attr chain ---
        if "." in method_name and method_name.count(".") == 1:
            attr_name, chain_method = method_name.split(".", 1)
            if (attr_type_map and global_type_methods
                    and caller_sym_id and caller_sym_id in caller_classes):
                class_fqn = caller_classes[caller_sym_id]
                attr_type = attr_type_map.get(class_fqn, {}).get(attr_name)
                if attr_type:
                    type_methods = global_type_methods.get(attr_type, {})
                    if chain_method in type_methods:
                        return type_methods[chain_method], "attr_chain", 0.80, None
            return None, "unresolved", 0.0, None
        # More than two dots — too ambiguous
        if "." in method_name:
            return None, "unresolved", 0.0, None
        # Determine enclosing class
        if caller_sym_id and caller_sym_id in caller_classes:
            class_fqn = caller_classes[caller_sym_id]
            meths = mod_classes.get(class_fqn, {})
            if method_name in meths:
                return meths[method_name], "method_self", 0.95, None
        # Fallback: try all classes in module
        candidates = []
        for cls_fqn, meths in mod_classes.items():
            if method_name in meths:
                candidates.append(meths[method_name])
        if len(candidates) == 1:
            return candidates[0], "method_self", 0.85, None
        if candidates:
            return candidates[0], "heuristic", 0.30, candidates[:10]
        return None, "unresolved", 0.0, None

    # --- super().method() — skip ---
    if callee_raw.startswith("super()."):
        return None, "unresolved", 0.0, None

    # --- Bare name or dotted ---
    has_dot = "." in callee_raw
    bare = callee_raw.split(".")[-1] if has_dot else callee_raw

    # 1. Same-module match (bare names only)
    if not has_dot and bare in mod_syms:
        return mod_syms[bare], "same_module", 0.98, None

    # 2. Direct from-import alias
    if not has_dot and bare in mod_from:
        return mod_from[bare], "import_alias", 0.97, None

    # 3. Qualified: X.foo where X is imported module
    if has_dot:
        parts = callee_raw.split(".", 1)
        prefix, suffix = parts[0], parts[1]
        # Check plain import alias
        if prefix in mod_alias:
            resolved = f"{mod_alias[prefix]}.{suffix}"
            return resolved, "qualified", 0.90, None
        # Check from-import (class/object with attribute)
        if prefix in mod_from:
            resolved = f"{mod_from[prefix]}.{suffix}"
            return resolved, "qualified", 0.85, None

    # 4. Global unique
    if not has_dot and bare in global_idx:
        matches = global_idx[bare]
        if len(matches) == 1:
            fqn, kind = matches[0]
            return fqn, "global_unique", 0.80, None
        if 1 < len(matches) <= 10:
            fqns = [m[0] for m in matches]
            return None, "heuristic", 0.30, fqns

    return None, "unresolved", 0.0, None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def resolve_all_calls(
    db: sqlite3.Connection,
    jsonl_file: Optional[TextIO] = None,
    root: Optional[Path] = None,
) -> dict:
    """Resolve all calls, write call_resolution, update high-confidence callee_fqn.

    Returns stats dict with counts.
    """
    now = _now_iso()

    # Clear previous resolutions
    db.execute("DELETE FROM call_resolution")
    db.execute("UPDATE calls SET callee_fqn = NULL")

    # Build indexes
    from_imports = _build_from_import_map(db)
    mod_aliases = _build_module_alias_map(db)
    same_mod_syms = _build_same_module_symbols(db)
    global_idx = _build_global_name_index(db)
    class_methods = _build_class_method_map(db)
    caller_classes = _build_caller_to_class(db)
    # Attr-chain indexes (self.attr.method() resolution)
    attr_type_map = _build_init_attr_map(db, root) if root else {}
    global_type_methods = _build_global_type_methods(db)

    # Fetch all calls
    calls = db.execute(
        "SELECT id, module_id, caller_symbol_id, callee_name_raw FROM calls"
    ).fetchall()

    stats = {
        "total": len(calls),
        "resolved": 0,
        "high_conf": 0,
        "skipped_dunder": 0,
        "by_method": {},
        # Noise-classified breakdown
        "internal_resolved": 0,   # resolved → project-internal FQN
        "external_resolved": 0,   # resolved → external library FQN (noise)
        "builtin_skipped": 0,     # unresolved bare names that are Python builtins
        "truly_unresolved": 0,    # unresolved, not builtin — actionable gap
    }

    resolution_batch = []
    update_batch = []
    BATCH = 5000

    for call_id, module_id, caller_sym_id, callee_raw in calls:
        if not callee_raw:
            continue
        if _is_dunder(callee_raw):
            stats["skipped_dunder"] += 1
            continue

        fqn, method, conf, candidates = _resolve_one(
            callee_raw, module_id, caller_sym_id,
            from_imports, mod_aliases, same_mod_syms,
            global_idx, class_methods, caller_classes,
            attr_type_map, global_type_methods,
        )

        if fqn or candidates:
            resolved_fqn = fqn or (candidates[0] if candidates else "")
            resolution_batch.append((
                call_id, resolved_fqn, method, conf,
                json.dumps(candidates) if candidates else None, now,
            ))
            if fqn:
                stats["resolved"] += 1
                stats["by_method"][method] = stats["by_method"].get(method, 0) + 1
                if conf >= CONFIDENCE_THRESHOLD:
                    update_batch.append((fqn, call_id))
                    stats["high_conf"] += 1
                # Noise classification
                if _classify_fqn(fqn) == "internal":
                    stats["internal_resolved"] += 1
                else:
                    stats["external_resolved"] += 1
        else:
            # Unresolved — classify noise vs actionable gap
            has_dot = "." in callee_raw
            bare = callee_raw.split(".")[-1] if has_dot else callee_raw
            if not has_dot and bare in _PYTHON_BUILTINS:
                # Python built-in function (len, abs, isinstance, etc.)
                stats["builtin_skipped"] += 1
            elif has_dot and bare in _EXTERNAL_METHOD_SUFFIXES:
                # Method suffix that is never a project-internal call
                # (dict.get, list.append, logger.info, etc.)
                stats["external_resolved"] += 1
            else:
                prefix = callee_raw.split(".")[0] if has_dot else None
                if prefix and (prefix in _EXTERNAL_PACKAGE_ROOTS
                               or prefix in _EXTERNAL_VAR_NAMES):
                    # External import alias or well-known variable convention
                    stats["external_resolved"] += 1
                else:
                    stats["truly_unresolved"] += 1

        if len(resolution_batch) >= BATCH:
            _flush(db, resolution_batch, update_batch)
            resolution_batch.clear()
            update_batch.clear()

    if resolution_batch:
        _flush(db, resolution_batch, update_batch)

    truly_unresolvable = stats["skipped_dunder"] + stats["builtin_skipped"] + stats["external_resolved"]
    logger.info(
        "Call resolution: %d/%d resolved (%d high-conf) | "
        "internal=%d external=%d builtin=%d dunder=%d | "
        "truly_unresolved=%d (actionable gaps)",
        stats["resolved"], stats["total"], stats["high_conf"],
        stats["internal_resolved"], stats["external_resolved"],
        stats["builtin_skipped"], stats["skipped_dunder"],
        stats["truly_unresolved"],
    )

    if jsonl_file:
        jsonl_file.write(json.dumps({
            "type": "call_resolution_summary", "stats": stats, "indexed_at": now,
        }, default=str) + "\n")

    return stats


def _flush(db: sqlite3.Connection, res_batch: list, upd_batch: list) -> None:
    db.executemany(
        "INSERT OR REPLACE INTO call_resolution "
        "(call_id, resolved_callee_fqn, resolution_method, confidence, "
        " candidates_json, indexed_at) VALUES (?, ?, ?, ?, ?, ?)",
        res_batch,
    )
    if upd_batch:
        db.executemany(
            "UPDATE calls SET callee_fqn = ? WHERE id = ?",
            upd_batch,
        )
