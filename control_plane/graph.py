"""Graph edge materialization and subgraph extraction.

Materializes call graph edges from calls + call_resolution into the
graph_edges table (integer symbol IDs, not string FQNs). Provides BFS
subgraph extraction for context pack assembly.
"""

from __future__ import annotations

import logging
import sqlite3
from collections import deque
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


def build_graph_edges(db: sqlite3.Connection, min_confidence: float = 0.80) -> int:
    """Materialize graph_edges from resolved calls + inheritance.

    Only includes edges where:
    - call_resolution has confidence >= min_confidence
    - resolved_callee_fqn maps to an actual symbol_id in symbols table

    Also adds inheritance edges (child class -> parent class).

    Returns total edge count.
    """
    now = datetime.now(timezone.utc).isoformat()

    # Clear existing edges (full rebuild each time)
    db.execute("DELETE FROM graph_edges")

    # ── Call edges ──
    # Join: calls -> call_resolution -> symbols (callee)
    # We need caller_symbol_id from calls table and callee_symbol_id from symbols
    call_edge_sql = """
        INSERT INTO graph_edges (caller_symbol_id, callee_symbol_id, edge_kind, confidence, lineno, indexed_at)
        SELECT DISTINCT
            c.caller_symbol_id,
            s_callee.id,
            'call',
            cr.confidence,
            c.lineno,
            ?
        FROM call_resolution cr
        JOIN calls c ON c.id = cr.call_id
        JOIN symbols s_callee ON s_callee.fqn = cr.resolved_callee_fqn
        WHERE cr.confidence >= ?
          AND c.caller_symbol_id IS NOT NULL
    """
    cursor = db.execute(call_edge_sql, (now, min_confidence))
    call_count = cursor.rowcount

    # ── Inheritance edges ──
    # child_symbol_id -> parent symbol (if resolved)
    inh_edge_sql = """
        INSERT INTO graph_edges (caller_symbol_id, callee_symbol_id, edge_kind, confidence, lineno, indexed_at)
        SELECT DISTINCT
            inh.child_symbol_id,
            s_parent.id,
            'inheritance',
            0.95,
            s_child.lineno,
            ?
        FROM inheritance inh
        JOIN symbols s_parent ON s_parent.fqn = inh.parent_fqn
        JOIN symbols s_child ON s_child.id = inh.child_symbol_id
        WHERE inh.parent_fqn IS NOT NULL
    """
    cursor = db.execute(inh_edge_sql, (now,))
    inh_count = cursor.rowcount

    total = call_count + inh_count
    logger.info("Graph edges built: %d call + %d inheritance = %d total", call_count, inh_count, total)

    # Enrichment: import edges + formation-semantic edges
    from .graph_enricher import enrich_graph
    total += enrich_graph(db, now)

    return total


def get_subgraph_bfs(
    db: sqlite3.Connection,
    seed_fqns: list[str],
    depth: int = 1,
    edge_kinds: Optional[list[str]] = None,
    direction: str = "both",
) -> dict:
    """BFS from seed symbols up to given depth.

    Args:
        seed_fqns: Starting symbol FQNs
        depth: Max BFS hops (1 = immediate neighbors)
        edge_kinds: Filter to specific edge types (None = all)
        direction: "forward" (callees), "reverse" (callers), "both"

    Returns:
        {
            nodes: [{id, fqn, kind, module_path, signature, synopsis}],
            edges: [{from_id, to_id, kind, confidence}],
            seed_ids: [int]
        }
    """
    # Resolve seed FQNs to symbol IDs
    seed_ids = []
    for fqn in seed_fqns:
        row = db.execute("SELECT id FROM symbols WHERE fqn = ?", (fqn,)).fetchone()
        if row:
            seed_ids.append(row[0])

    if not seed_ids:
        return {"nodes": [], "edges": [], "seed_ids": []}

    # BFS
    visited: set[int] = set(seed_ids)
    frontier: deque[tuple[int, int]] = deque((sid, 0) for sid in seed_ids)
    collected_edges: list[tuple] = []

    # Build edge kind filter
    kind_clause = ""
    kind_params: list = []
    if edge_kinds:
        placeholders = ",".join("?" for _ in edge_kinds)
        kind_clause = f" AND ge.edge_kind IN ({placeholders})"
        kind_params = list(edge_kinds)

    while frontier:
        symbol_id, current_depth = frontier.popleft()
        if current_depth >= depth:
            continue

        neighbors: list[tuple] = []

        if direction in ("forward", "both"):
            rows = db.execute(
                f"SELECT ge.callee_symbol_id, ge.edge_kind, ge.confidence "
                f"FROM graph_edges ge WHERE ge.caller_symbol_id = ?{kind_clause}",
                [symbol_id] + kind_params
            ).fetchall()
            for callee_id, kind, conf in rows:
                collected_edges.append((symbol_id, callee_id, kind, conf))
                neighbors.append((callee_id, current_depth + 1))

        if direction in ("reverse", "both"):
            rows = db.execute(
                f"SELECT ge.caller_symbol_id, ge.edge_kind, ge.confidence "
                f"FROM graph_edges ge WHERE ge.callee_symbol_id = ?{kind_clause}",
                [symbol_id] + kind_params
            ).fetchall()
            for caller_id, kind, conf in rows:
                collected_edges.append((caller_id, symbol_id, kind, conf))
                neighbors.append((caller_id, current_depth + 1))

        for neighbor_id, next_depth in neighbors:
            if neighbor_id not in visited:
                visited.add(neighbor_id)
                frontier.append((neighbor_id, next_depth))

    # Fetch node details
    nodes = []
    if visited:
        placeholders = ",".join("?" for _ in visited)
        rows = db.execute(
            f"""SELECT s.id, s.fqn, s.kind, m.path, s.signature,
                       COALESCE(syn.synopsis, s.doc_head) as synopsis
                FROM symbols s
                JOIN modules m ON m.id = s.module_id
                LEFT JOIN synopses syn ON syn.symbol_id = s.id
                WHERE s.id IN ({placeholders})""",
            list(visited)
        ).fetchall()
        for row in rows:
            nodes.append({
                "id": row[0], "fqn": row[1], "kind": row[2],
                "module_path": row[3], "signature": row[4], "synopsis": row[5],
            })

    # Dedupe edges
    seen_edges: set[tuple] = set()
    edges = []
    for from_id, to_id, kind, conf in collected_edges:
        key = (from_id, to_id, kind)
        if key not in seen_edges:
            seen_edges.add(key)
            edges.append({"from_id": from_id, "to_id": to_id, "kind": kind, "confidence": conf})

    return {"nodes": nodes, "edges": edges, "seed_ids": seed_ids}


def get_callers(db: sqlite3.Connection, symbol_id: int, limit: int = 50) -> list[dict]:
    """Get immediate callers of a symbol."""
    rows = db.execute(
        """SELECT s.id, s.fqn, s.kind, m.path, ge.confidence
           FROM graph_edges ge
           JOIN symbols s ON s.id = ge.caller_symbol_id
           JOIN modules m ON m.id = s.module_id
           WHERE ge.callee_symbol_id = ? AND ge.edge_kind = 'call'
           ORDER BY ge.confidence DESC
           LIMIT ?""",
        (symbol_id, limit)
    ).fetchall()
    return [{"id": r[0], "fqn": r[1], "kind": r[2], "module_path": r[3], "confidence": r[4]} for r in rows]


def get_callees(db: sqlite3.Connection, symbol_id: int, limit: int = 50) -> list[dict]:
    """Get immediate callees of a symbol."""
    rows = db.execute(
        """SELECT s.id, s.fqn, s.kind, m.path, ge.confidence
           FROM graph_edges ge
           JOIN symbols s ON s.id = ge.callee_symbol_id
           JOIN modules m ON m.id = s.module_id
           WHERE ge.caller_symbol_id = ? AND ge.edge_kind = 'call'
           ORDER BY ge.confidence DESC
           LIMIT ?""",
        (symbol_id, limit)
    ).fetchall()
    return [{"id": r[0], "fqn": r[1], "kind": r[2], "module_path": r[3], "confidence": r[4]} for r in rows]
