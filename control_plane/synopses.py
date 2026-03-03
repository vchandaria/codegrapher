"""Symbol synopsis and mini-trace generation.

Generates cached 1-3 sentence summaries of symbols and 1-line
caller->self->callees trace chains for compact context packs.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def generate_synopsis(db: sqlite3.Connection, symbol_id: int) -> str:
    """Generate a 1-3 sentence synopsis from symbol metadata.

    Uses: kind, name, module path, signature, doc_head, caller/callee counts.
    """
    row = db.execute(
        """SELECT s.name, s.kind, s.fqn, s.signature, s.doc_head,
                  m.path as module_path
           FROM symbols s
           JOIN modules m ON m.id = s.module_id
           WHERE s.id = ?""",
        (symbol_id,)
    ).fetchone()
    if not row:
        return ""

    name, kind, fqn, signature, doc_head, module_path = row

    # Count callers and callees from graph_edges
    caller_count = db.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE callee_symbol_id = ? AND edge_kind = 'call'",
        (symbol_id,)
    ).fetchone()[0]
    callee_count = db.execute(
        "SELECT COUNT(*) FROM graph_edges WHERE caller_symbol_id = ? AND edge_kind = 'call'",
        (symbol_id,)
    ).fetchone()[0]

    # Count discrepancies mentioning this symbol's FQN
    disc_count = db.execute(
        "SELECT COUNT(*) FROM discrepancies WHERE subject = ? OR subject LIKE ?",
        (fqn, f"%{name}%")
    ).fetchone()[0]

    # Build synopsis
    parts = []

    # Line 1: What it is
    kind_label = kind.capitalize()
    module_short = module_path.replace("\\", "/").rsplit("/", 1)[-1].replace(".py", "")
    sig_part = f" {signature}" if signature and len(signature) < 80 else ""
    parts.append(f"{kind_label} `{name}`{sig_part} in {module_short}.")

    # Line 2: What it does (from docstring or inferred)
    if doc_head and len(doc_head) > 5:
        # Truncate long doc_heads
        dh = doc_head[:120].rstrip(".")
        parts.append(f"{dh}.")

    # Line 3: Connectivity + issues
    connectivity_parts = []
    if caller_count > 0:
        connectivity_parts.append(f"called by {caller_count}")
    if callee_count > 0:
        connectivity_parts.append(f"calls {callee_count}")
    if disc_count > 0:
        connectivity_parts.append(f"{disc_count} discrepancies")

    if connectivity_parts:
        parts.append(f"Graph: {', '.join(connectivity_parts)}.")

    return " ".join(parts)


def generate_mini_trace(db: sqlite3.Connection, symbol_id: int, max_callers: int = 2, max_callees: int = 3) -> str:
    """Generate a 1-line caller->self->callees trace chain.

    Example: risk_manager.assess -> unified_position_sizer.calculate -> engine.execute_trade
    """
    # Get symbol name
    row = db.execute("SELECT fqn FROM symbols WHERE id = ?", (symbol_id,)).fetchone()
    if not row:
        return ""
    self_fqn = row[0]
    self_short = _short_fqn(self_fqn)

    # Top callers by confidence
    callers = db.execute(
        """SELECT s.fqn FROM graph_edges ge
           JOIN symbols s ON s.id = ge.caller_symbol_id
           WHERE ge.callee_symbol_id = ? AND ge.edge_kind = 'call'
           ORDER BY ge.confidence DESC LIMIT ?""",
        (symbol_id, max_callers)
    ).fetchall()

    # Top callees by confidence
    callees = db.execute(
        """SELECT s.fqn FROM graph_edges ge
           JOIN symbols s ON s.id = ge.callee_symbol_id
           WHERE ge.caller_symbol_id = ? AND ge.edge_kind = 'call'
           ORDER BY ge.confidence DESC LIMIT ?""",
        (symbol_id, max_callees)
    ).fetchall()

    parts = []
    if callers:
        caller_names = [_short_fqn(r[0]) for r in callers]
        parts.append(" | ".join(caller_names))
    parts.append(self_short)
    if callees:
        callee_names = [_short_fqn(r[0]) for r in callees]
        parts.append(" | ".join(callee_names))

    return " -> ".join(parts)


def _short_fqn(fqn: str) -> str:
    """Shorten FQN to last 2 segments: src.utils.foo.bar -> foo.bar"""
    parts = fqn.split(".")
    if len(parts) <= 2:
        return fqn
    return ".".join(parts[-2:])


def build_all_synopses(db: sqlite3.Connection, force: bool = False) -> int:
    """Generate synopses for all functions/methods/classes.

    Skips already-generated unless force=True.
    Returns count generated.
    """
    now = datetime.now(timezone.utc).isoformat()

    if force:
        db.execute("DELETE FROM synopses")

    # Get symbols that need synopses
    if force:
        where_clause = "WHERE s.kind IN ('function', 'method', 'class')"
    else:
        where_clause = """WHERE s.kind IN ('function', 'method', 'class')
                          AND s.id NOT IN (SELECT symbol_id FROM synopses)"""

    symbols = db.execute(
        f"SELECT s.id FROM symbols s {where_clause}"
    ).fetchall()

    count = 0
    batch = []
    for (symbol_id,) in symbols:
        synopsis = generate_synopsis(db, symbol_id)
        mini_trace = generate_mini_trace(db, symbol_id)

        if synopsis:
            batch.append((symbol_id, synopsis, mini_trace, now))
            count += 1

        if len(batch) >= 500:
            _flush_batch(db, batch, force)
            batch = []

    if batch:
        _flush_batch(db, batch, force)

    logger.info("Synopses generated: %d symbols", count)
    return count


def _flush_batch(db: sqlite3.Connection, batch: list, force: bool) -> None:
    """Insert or replace synopsis batch."""
    db.executemany(
        "INSERT OR REPLACE INTO synopses (symbol_id, synopsis, mini_trace, generated_at) "
        "VALUES (?, ?, ?, ?)",
        batch,
    )
