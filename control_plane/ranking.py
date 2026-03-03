"""Intent-aware symbol ranking.

Ranks symbols by weighted relevance factors that vary by intent class.
Supports seed-based BFS scoping and filter-based pre-selection.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict

from .graph import get_subgraph_bfs

logger = logging.getLogger(__name__)

# Intent-class weight profiles
INTENT_WEIGHTS: dict[str, dict[str, float]] = {
    "bugfix": {
        "proof_failures": 3.0,
        "discrepancy_count": 2.0,
        "callers": 1.0,
        "complexity": 0.5,
        "callees": 0.3,
    },
    "parity_cleanup": {
        "formula_parity": 3.0,
        "dead_code": 2.0,
        "discrepancy_count": 1.5,
        "callers": 0.5,
    },
    "governance": {
        "discrepancy_count": 3.0,
        "callers": 2.0,
        "complexity": 1.0,
        "callees": 0.5,
    },
    "data_pipeline": {
        "callers": 2.0,
        "callees": 2.0,
        "discrepancy_count": 1.5,
        "complexity": 1.0,
    },
}


def rank_symbols(
    db: sqlite3.Connection,
    seeds: list[str] | None = None,
    filter: dict | None = None,
    intent_class: str = "bugfix",
    k: int = 50,
) -> list[dict]:
    """Score and rank symbols by intent-weighted relevance.

    Args:
        seeds: Optional seed FQNs — BFS from these to scope candidates
        filter: Optional filter dict {kind, module_path, formation}
        intent_class: Weight profile to use
        k: Max results to return

    Returns:
        [{fqn, score, kind, module_path, reasons: {factor: contribution}}]
    """
    weights = INTENT_WEIGHTS.get(intent_class, INTENT_WEIGHTS["bugfix"])

    # Step 1: Determine candidate symbol set
    if seeds:
        subgraph = get_subgraph_bfs(db, seeds, depth=2)
        candidate_ids = {n["id"] for n in subgraph["nodes"]}
        if not candidate_ids:
            return []
    else:
        candidate_ids = None  # All symbols

    # Step 2: Build scoring factors
    # Factor: discrepancy_count per module_path
    disc_by_module: dict[str, int] = defaultdict(int)
    disc_by_fqn: dict[str, int] = defaultdict(int)
    for row in db.execute("SELECT module_path, subject, COUNT(*) FROM discrepancies GROUP BY module_path, subject"):
        if row[0]:
            disc_by_module[row[0]] += row[2]
        if row[1]:
            disc_by_fqn[row[1]] += row[2]

    # Factor: proof_failures per module
    proof_by_module: dict[str, int] = defaultdict(int)
    proof_rows = db.execute(
        """SELECT details_json FROM proof_results
           WHERE run_id = (SELECT MAX(run_id) FROM proof_results) AND status = 'fail'"""
    ).fetchall()
    for (details_json,) in proof_rows:
        try:
            details = json.loads(details_json) if details_json else {}
            for v in details.get("violations", []):
                if isinstance(v, dict) and "file" in v:
                    proof_by_module[v["file"]] += 1
        except (json.JSONDecodeError, TypeError):
            pass

    # Factor: formula_parity (business rules with divergence)
    parity_fqns: set[str] = set()
    for row in db.execute(
        "SELECT subject FROM discrepancies WHERE type LIKE 'FORMULA_PARITY%' AND severity IN ('error', 'warn')"
    ):
        parity_fqns.add(row[0])

    # Factor: caller count (in-degree from graph_edges)
    caller_counts: dict[int, int] = {}
    for row in db.execute(
        "SELECT callee_symbol_id, COUNT(*) FROM graph_edges WHERE edge_kind = 'call' GROUP BY callee_symbol_id"
    ):
        caller_counts[row[0]] = row[1]

    # Factor: callee count (out-degree)
    callee_counts: dict[int, int] = {}
    for row in db.execute(
        "SELECT caller_symbol_id, COUNT(*) FROM graph_edges WHERE edge_kind = 'call' GROUP BY caller_symbol_id"
    ):
        callee_counts[row[0]] = row[1]

    # Step 3: Score each candidate symbol
    query = """
        SELECT s.id, s.fqn, s.kind, m.path, s.complexity
        FROM symbols s
        JOIN modules m ON m.id = s.module_id
        WHERE s.kind IN ('function', 'method', 'class')
    """
    params: list = []

    if candidate_ids:
        placeholders = ",".join("?" for _ in candidate_ids)
        query += f" AND s.id IN ({placeholders})"
        params.extend(candidate_ids)

    if filter:
        if "kind" in filter:
            query += " AND s.kind = ?"
            params.append(filter["kind"])
        if "module_path" in filter:
            query += " AND m.path LIKE ?"
            params.append(f"%{filter['module_path']}%")

    scored: list[dict] = []
    for row in db.execute(query, params).fetchall():
        sym_id, fqn, kind, module_path, complexity = row

        reasons: dict[str, float] = {}
        total = 0.0

        # discrepancy_count
        w = weights.get("discrepancy_count", 0)
        if w > 0:
            disc_score = disc_by_module.get(module_path, 0) + disc_by_fqn.get(fqn, 0)
            contribution = w * min(disc_score / 5.0, 3.0)  # Normalize: 5 discs = 1x weight, cap at 3x
            if contribution > 0:
                reasons["discrepancy_count"] = round(contribution, 2)
                total += contribution

        # proof_failures
        w = weights.get("proof_failures", 0)
        if w > 0:
            proof_score = proof_by_module.get(module_path, 0)
            contribution = w * min(proof_score, 3.0)
            if contribution > 0:
                reasons["proof_failures"] = round(contribution, 2)
                total += contribution

        # formula_parity
        w = weights.get("formula_parity", 0)
        if w > 0:
            # Check if this symbol's name matches any parity-flagged rule
            name_parts = fqn.split(".")
            for pf in parity_fqns:
                if any(part in pf for part in name_parts[-2:]):
                    reasons["formula_parity"] = round(w * 2.0, 2)
                    total += w * 2.0
                    break

        # callers (in-degree = blast radius)
        w = weights.get("callers", 0)
        if w > 0:
            nc = caller_counts.get(sym_id, 0)
            contribution = w * min(nc / 10.0, 2.0)  # Normalize: 10 callers = 1x weight
            if contribution > 0:
                reasons["callers"] = round(contribution, 2)
                total += contribution

        # callees (out-degree = complexity proxy)
        w = weights.get("callees", 0)
        if w > 0:
            nc = callee_counts.get(sym_id, 0)
            contribution = w * min(nc / 10.0, 2.0)
            if contribution > 0:
                reasons["callees"] = round(contribution, 2)
                total += contribution

        # complexity
        w = weights.get("complexity", 0)
        if w > 0 and complexity and complexity > 0:
            contribution = w * min(complexity / 20.0, 2.0)  # Normalize: 20 = 1x weight
            if contribution > 0:
                reasons["complexity"] = round(contribution, 2)
                total += contribution

        # dead_code
        w = weights.get("dead_code", 0)
        if w > 0:
            dead_count = db.execute(
                "SELECT COUNT(*) FROM discrepancies WHERE type = 'DEAD_CODE_FUNCTION' AND subject = ?",
                (fqn,)
            ).fetchone()[0]
            if dead_count > 0:
                reasons["dead_code"] = round(w * 1.5, 2)
                total += w * 1.5

        if total > 0:
            scored.append({
                "fqn": fqn,
                "score": round(total, 2),
                "kind": kind,
                "module_path": module_path,
                "reasons": reasons,
            })

    # Sort by score descending, take top k
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Cross-encoder reranking (if enabled and seeds provide a query)
    try:
        from .retrieval.config import RERANKER_ENABLED
        if RERANKER_ENABLED and scored and seeds:
            from .retrieval.reranker import rerank_symbols
            query = " ".join(seeds) if isinstance(seeds, list) else str(seeds)
            scored = rerank_symbols(query, scored, top_k=k)
    except ImportError:
        pass  # retrieval module not available

    # Temporal decay on discrepancy contributions (if enabled)
    try:
        from .retrieval.config import TEMPORAL_DECAY_ENABLED
        if TEMPORAL_DECAY_ENABLED and scored:
            from .retrieval.temporal import temporal_decay_score
            for item in scored:
                # Decay discrepancy_count contribution by module freshness
                if "discrepancy_count" in item.get("reasons", {}):
                    ts_row = db.execute(
                        "SELECT MAX(indexed_at) FROM discrepancies WHERE module_path = ?",
                        (item["module_path"],)
                    ).fetchone()
                    if ts_row and ts_row[0]:
                        decay = temporal_decay_score(ts_row[0])
                        old_contrib = item["reasons"]["discrepancy_count"]
                        item["reasons"]["discrepancy_count"] = round(old_contrib * decay, 2)
                        item["score"] = round(sum(item["reasons"].values()), 2)
            # Re-sort after decay adjustment
            scored.sort(key=lambda x: x["score"], reverse=True)
    except ImportError:
        pass

    return scored[:k]


def dedupe_and_cluster(ranked: list[dict], max_clusters: int = 5) -> list[dict]:
    """Group ranked symbols by module, dedupe by FQN prefix, return top per cluster.

    Returns list of cluster dicts:
    [{module: str, symbols: [{fqn, score, reasons}], total_score: float}]
    """
    clusters: dict[str, list[dict]] = defaultdict(list)
    for item in ranked:
        module = item["module_path"]
        clusters[module].append(item)

    # Sort clusters by total score
    cluster_list = []
    for module, symbols in clusters.items():
        total = sum(s["score"] for s in symbols)
        # Dedupe: keep highest-scored per FQN prefix (last 2 parts)
        seen_prefixes: set[str] = set()
        deduped = []
        for s in sorted(symbols, key=lambda x: x["score"], reverse=True):
            prefix = ".".join(s["fqn"].split(".")[-2:])
            if prefix not in seen_prefixes:
                seen_prefixes.add(prefix)
                deduped.append(s)
        cluster_list.append({"module": module, "symbols": deduped, "total_score": round(total, 2)})

    cluster_list.sort(key=lambda x: x["total_score"], reverse=True)
    return cluster_list[:max_clusters]
