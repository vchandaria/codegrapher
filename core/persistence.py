"""Persistence layer: writes ModuleRecord data to SQLite + JSONL.

Handles incremental logic — checks sha1 to skip unchanged modules.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, TextIO

from .ast_walker import (
    ModuleRecord, SymbolRecord, ImportRecord, CallRecord,
    RegistryRecord, ConstantRecord, EnvVarRecord, FieldReadRecord,
)
from .schema import clear_module_data


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def check_needs_reindex(db: sqlite3.Connection, relpath: str, sha1: str) -> tuple[bool, Optional[int]]:
    """Check if a module needs re-indexing. Returns (needs_reindex, existing_module_id)."""
    row = db.execute(
        "SELECT id, sha1 FROM modules WHERE path = ?", (relpath,)
    ).fetchone()
    if row is None:
        return True, None
    existing_id, existing_sha1 = row
    if existing_sha1 == sha1:
        return False, existing_id
    return True, existing_id


def persist_module(
    db: sqlite3.Connection,
    rec: ModuleRecord,
    jsonl_file: Optional[TextIO] = None,
    incremental: bool = False,
) -> Optional[int]:
    """Write a ModuleRecord to SQLite and optionally JSONL.

    Returns the module_id, or None if skipped (incremental + unchanged).
    """
    now = _now_iso()

    if incremental:
        needs, existing_id = check_needs_reindex(db, rec.path, rec.sha1)
        if not needs:
            return None
        if existing_id is not None:
            clear_module_data(db, existing_id)
            db.execute("DELETE FROM modules WHERE id = ?", (existing_id,))

    # Insert module row
    cur = db.execute(
        "INSERT INTO modules (path, relpath_no_ext, sha1, mtime, size, loc, indexed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (rec.path, rec.relpath_no_ext, rec.sha1, rec.mtime, rec.size, rec.loc, now),
    )
    mod_id = cur.lastrowid

    # Emit JSONL module record
    if jsonl_file:
        _write_jsonl(jsonl_file, {
            "id": mod_id, "type": "module", "path": rec.path,
            "loc": rec.loc, "sha1": rec.sha1, "indexed_at": now,
            "parse_error": rec.parse_error,
            "all_exports": rec.all_exports,
        })

    if rec.parse_error:
        return mod_id

    # ── Symbols ──
    # Two passes: first insert all symbols, then resolve parent_symbol_id
    fqn_to_id: dict[str, int] = {}
    symbol_parents: list[tuple[int, str]] = []  # (symbol_id, parent_fqn)
    inheritance_records: list[tuple] = []  # (child_symbol_id, parent_name, order)

    for sym in rec.symbols:
        # Handle FQN collisions: append #L<lineno> if fqn already exists
        fqn = sym.fqn
        if fqn in fqn_to_id:
            fqn = f"{fqn}#L{sym.lineno}"
        # Also check DB for cross-module collisions
        existing = db.execute("SELECT id FROM symbols WHERE fqn = ?", (fqn,)).fetchone()
        if existing:
            fqn = f"{sym.fqn}#L{sym.lineno}"

        try:
            cur = db.execute(
                "INSERT INTO symbols "
                "(module_id, name, kind, fqn, scope_depth, lineno, end_lineno, "
                " signature, decorators_json, doc_head, is_exported, is_async, is_generator) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (mod_id, sym.name, sym.kind, fqn, sym.scope_depth,
                 sym.lineno, sym.end_lineno, sym.signature,
                 json.dumps(sym.decorators) if sym.decorators else None,
                 sym.doc_head, int(sym.is_exported), int(sym.is_async),
                 int(sym.is_generator)),
            )
        except sqlite3.IntegrityError:
            # Last resort: append module path hash
            fqn = f"{sym.fqn}#{rec.relpath_no_ext}#L{sym.lineno}"
            cur = db.execute(
                "INSERT INTO symbols "
                "(module_id, name, kind, fqn, scope_depth, lineno, end_lineno, "
                " signature, decorators_json, doc_head, is_exported, is_async, is_generator) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (mod_id, sym.name, sym.kind, fqn, sym.scope_depth,
                 sym.lineno, sym.end_lineno, sym.signature,
                 json.dumps(sym.decorators) if sym.decorators else None,
                 sym.doc_head, int(sym.is_exported), int(sym.is_async),
                 int(sym.is_generator)),
            )
        sym_id = cur.lastrowid
        fqn_to_id[fqn] = sym_id
        if sym.parent_fqn:
            symbol_parents.append((sym_id, sym.parent_fqn))

        if jsonl_file:
            _write_jsonl(jsonl_file, {
                "id": sym_id, "type": "symbol", "module_path": rec.path,
                "name": sym.name, "kind": sym.kind, "fqn": sym.fqn,
                "parent_fqn": sym.parent_fqn, "scope_depth": sym.scope_depth,
                "lineno": sym.lineno, "end_lineno": sym.end_lineno,
                "signature": sym.signature, "is_exported": sym.is_exported,
                "is_async": sym.is_async, "is_generator": sym.is_generator,
                "doc_head": sym.doc_head, "indexed_at": now,
            })

    # Resolve parent_symbol_id
    for sym_id, parent_fqn in symbol_parents:
        parent_id = fqn_to_id.get(parent_fqn)
        if parent_id:
            db.execute(
                "UPDATE symbols SET parent_symbol_id = ? WHERE id = ?",
                (parent_id, sym_id),
            )

    # ── Inheritance (extract from class symbols' bases) ──
    # We need to re-walk the symbols to find classes and their bases
    # The AST walker stores bases info indirectly — we re-parse from the tree
    # For now, we use the class symbols and their recorded bases
    # (inheritance is populated by a second pass)

    # ── Imports ──
    for imp in rec.imports:
        db.execute(
            "INSERT INTO imports (module_id, imported, alias, is_from, from_module, lineno) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (mod_id, imp.imported, imp.alias, int(imp.is_from),
             imp.from_module, imp.lineno),
        )
        if jsonl_file:
            _write_jsonl(jsonl_file, {
                "type": "import", "module_path": rec.path,
                "imported": imp.imported, "alias": imp.alias,
                "is_from": imp.is_from, "from_module": imp.from_module,
                "lineno": imp.lineno, "indexed_at": now,
            })

    # ── Calls ──
    for call in rec.calls:
        caller_sym_id = fqn_to_id.get(call.caller_fqn)
        db.execute(
            "INSERT INTO calls "
            "(module_id, caller_symbol_id, caller_fqn, caller_module_path, "
            " callee_name_raw, lineno, positional_arg_count, kw_names_json, "
            " has_varargs, has_varkw) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (mod_id, caller_sym_id, call.caller_fqn, rec.path,
             call.callee_name_raw, call.lineno, call.positional_arg_count,
             json.dumps(call.kw_names) if call.kw_names else None,
             int(call.has_varargs), int(call.has_varkw)),
        )

    # ── Registries ──
    for reg in rec.registries:
        db.execute(
            "INSERT INTO registries "
            "(module_id, name, kind, lineno, value_mode, value_json, "
            " value_keys_json, value_hash) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (mod_id, reg.name, reg.kind, reg.lineno, reg.value_mode,
             reg.value_json, reg.value_keys_json, reg.value_hash),
        )
        if jsonl_file:
            _write_jsonl(jsonl_file, {
                "type": "registry", "module_path": rec.path,
                "name": reg.name, "kind": reg.kind, "lineno": reg.lineno,
                "value_mode": reg.value_mode, "value_hash": reg.value_hash,
                "value_keys_json": reg.value_keys_json,
                "indexed_at": now,
            })

    # ── Constants ──
    for const in rec.constants:
        db.execute(
            "INSERT INTO constants (module_id, name, lineno, value_repr, value_num, scale_hint) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (mod_id, const.name, const.lineno, const.value_repr,
             const.value_num, const.scale_hint),
        )
        if jsonl_file:
            _write_jsonl(jsonl_file, {
                "type": "constant", "module_path": rec.path,
                "name": const.name, "lineno": const.lineno,
                "value_repr": const.value_repr, "value_num": const.value_num,
                "scale_hint": const.scale_hint, "indexed_at": now,
            })

    # ── Env vars ──
    for ev in rec.env_vars:
        db.execute(
            "INSERT INTO env_vars "
            "(module_id, var_name, lineno, default_value, access_pattern, context_snippet) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (mod_id, ev.var_name, ev.lineno, ev.default_value,
             ev.access_pattern, ev.context_snippet),
        )
        if jsonl_file:
            _write_jsonl(jsonl_file, {
                "type": "env_var", "module_path": rec.path,
                "var_name": ev.var_name, "lineno": ev.lineno,
                "default_value": ev.default_value,
                "access_pattern": ev.access_pattern,
                "indexed_at": now,
            })

    # ── Field reads ──
    for fr in rec.field_reads:
        # Determine consumer FQN from scope info
        consumer_fqn = None
        # field_reads don't store caller_fqn directly, use module path
        db.execute(
            "INSERT INTO field_reads "
            "(module_id, source_dict, field_name, parent_field, default_value, "
            " consumer_file, consumer_fqn, lineno, access_pattern, confidence) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (mod_id, fr.source_dict, fr.field_name, fr.parent_field,
             fr.default_value, rec.path, consumer_fqn, fr.lineno,
             fr.access_pattern, fr.confidence),
        )
        if jsonl_file:
            _write_jsonl(jsonl_file, {
                "type": "field_read", "module_path": rec.path,
                "source_dict": fr.source_dict, "field_name": fr.field_name,
                "parent_field": fr.parent_field, "default_value": fr.default_value,
                "lineno": fr.lineno, "access_pattern": fr.access_pattern,
                "confidence": fr.confidence, "indexed_at": now,
            })

    return mod_id


def persist_discrepancy(
    db: sqlite3.Connection,
    disc_type: str,
    severity: str,
    module_path: Optional[str],
    lineno: Optional[int],
    subject: str,
    details: dict,
    module_id: Optional[int] = None,
    symbol_id: Optional[int] = None,
    jsonl_file: Optional[TextIO] = None,
) -> int:
    """Write a discrepancy to SQLite and optionally JSONL."""
    now = _now_iso()
    cur = db.execute(
        "INSERT INTO discrepancies "
        "(type, severity, module_id, module_path, lineno, subject, "
        " symbol_id, details_json, indexed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (disc_type, severity, module_id, module_path, lineno, subject,
         symbol_id, json.dumps(details, default=str), now),
    )
    disc_id = cur.lastrowid
    if jsonl_file:
        _write_jsonl(jsonl_file, {
            "id": disc_id, "type": "discrepancy", "disc_type": disc_type,
            "severity": severity, "module_path": module_path,
            "lineno": lineno, "subject": subject,
            "details": details, "indexed_at": now,
        })
    return disc_id


def _write_jsonl(f: TextIO, record: dict) -> None:
    """Write one JSON record as a single line."""
    f.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
