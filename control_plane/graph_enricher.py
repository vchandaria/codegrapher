"""Graph edge enrichment — import edges and formation-semantic edges.

Called from build_graph_edges() after call + inheritance edges are materialized.
Adds:
  - import edges (edge_kind='import') from the imports table
  - formation-semantic edges (edge_kind='formation_t1'/'formation_t2'/'formation_esm')
"""

from __future__ import annotations

import json
import logging
import sqlite3

logger = logging.getLogger(__name__)

from ..core.config import ENABLE_FORMATION_EDGES

# Standard library modules to exclude from import edges
_STDLIB = frozenset({
    "os", "sys", "typing", "pathlib", "logging", "collections", "json",
    "datetime", "math", "re", "abc", "functools", "itertools", "sqlite3",
    "threading", "dataclasses", "enum", "copy", "time", "traceback",
    "warnings", "inspect", "types", "io", "struct", "hashlib", "uuid",
    "random", "subprocess", "shutil", "textwrap", "contextlib",
    "concurrent", "concurrent.futures", "multiprocessing", "signal",
    "statistics", "decimal", "fractions", "operator", "string",
    "argparse", "configparser", "csv", "pickle", "shelve", "tempfile",
    "glob", "fnmatch", "socket", "http", "urllib", "asyncio",
    "queue", "heapq", "bisect", "array", "weakref", "gc",
    "unittest", "doctest", "pdb", "profile", "timeit",
})

# Third-party prefixes to exclude
_THIRD_PARTY_PREFIXES = (
    "PySide", "numpy", "pandas", "scipy", "sklearn", "matplotlib",
    "torch", "cudf", "cupy", "ib_insync", "ibapi", "requests",
    "aiohttp", "websocket", "psutil", "tqdm", "plotly", "dash",
    "flask", "fastapi", "uvicorn", "pydantic", "sqlalchemy",
    "redis", "celery", "boto", "google", "anthropic", "openai",
)


def _build_import_edges(db: sqlite3.Connection, now: str) -> int:
    """Add module-level import edges to graph_edges.

    For each from-import where the source module resolves to an indexed module,
    creates one edge linking the first top-level symbol in the importing module
    to the first top-level symbol in the imported module.
    """
    # Build stdlib + third-party exclusion
    # We filter in Python because SQLite can't do prefix matching against a list efficiently
    rows = db.execute("""
        SELECT DISTINCT
            imp.module_id AS src_module_id,
            imp.from_module,
            imp.lineno,
            m_tgt.id AS tgt_module_id
        FROM imports imp
        JOIN modules m_tgt ON (
            m_tgt.relpath_no_ext = REPLACE(imp.from_module, '.', '/')
        )
        WHERE imp.is_from = 1
          AND imp.from_module IS NOT NULL
          AND m_tgt.id != imp.module_id
    """).fetchall()

    count = 0
    for src_mod_id, from_module, lineno, tgt_mod_id in rows:
        # Skip stdlib
        top_pkg = from_module.split(".")[0]
        if top_pkg in _STDLIB:
            continue
        # Skip third-party
        if any(from_module.startswith(p) for p in _THIRD_PARTY_PREFIXES):
            continue

        # Find first top-level symbol in each module
        src_sym = db.execute(
            "SELECT id FROM symbols WHERE module_id = ? AND scope_depth = 0 "
            "AND kind IN ('function', 'class') ORDER BY lineno LIMIT 1",
            (src_mod_id,)
        ).fetchone()
        tgt_sym = db.execute(
            "SELECT id FROM symbols WHERE module_id = ? AND scope_depth = 0 "
            "AND kind IN ('function', 'class') ORDER BY lineno LIMIT 1",
            (tgt_mod_id,)
        ).fetchone()

        if src_sym and tgt_sym:
            db.execute(
                "INSERT OR IGNORE INTO graph_edges "
                "(caller_symbol_id, callee_symbol_id, edge_kind, confidence, lineno, indexed_at) "
                "VALUES (?, ?, 'import', 1.0, ?, ?)",
                (src_sym[0], tgt_sym[0], lineno, now)
            )
            count += 1

    return count


def _build_formation_semantic_edges(db: sqlite3.Connection, now: str) -> int:
    """Add formation pipeline edges: METADATA -> T1 -> T2 -> ESM."""
    # Get ALL_FORMATIONS from registries
    reg_row = db.execute(
        "SELECT value_json FROM registries WHERE name = 'ALL_FORMATIONS' LIMIT 1"
    ).fetchone()
    if not reg_row or not reg_row[0]:
        logger.warning("ALL_FORMATIONS not found in registries — skipping formation edges")
        return 0

    try:
        formations = json.loads(reg_row[0])
    except (json.JSONDecodeError, TypeError):
        logger.warning("ALL_FORMATIONS JSON decode failed — skipping formation edges")
        return 0

    # Find ESM entry point
    esm_row = db.execute(
        "SELECT id FROM symbols WHERE name = 'create_exit_state_machine_for_trade' "
        "AND kind = 'function' LIMIT 1"
    ).fetchone()
    esm_id = esm_row[0] if esm_row else None

    # Find FORMATION_METADATA symbol as the source node
    meta_row = db.execute(
        "SELECT s.id FROM symbols s JOIN modules m ON m.id = s.module_id "
        "WHERE m.relpath_no_ext = 'src/utils/formation_params' "
        "AND s.name = 'FORMATION_METADATA' LIMIT 1"
    ).fetchone()
    meta_id = meta_row[0] if meta_row else None

    count = 0
    for formation in formations:
        snake = formation.lower()

        # T1 scorer entry function: _gpu_score_{snake}
        t1_row = db.execute(
            "SELECT s.id FROM symbols s JOIN modules m ON m.id = s.module_id "
            "WHERE m.relpath_no_ext = ? AND s.name = ?",
            (f"src/scorers/tier1_{snake}", f"_gpu_score_{snake}")
        ).fetchone()

        # T2 scorer entry function: _score_tier2_{snake}
        t2_row = db.execute(
            "SELECT s.id FROM symbols s JOIN modules m ON m.id = s.module_id "
            "WHERE m.relpath_no_ext = ? AND s.name = ?",
            (f"src/scorers/tier2_{snake}", f"_score_tier2_{snake}")
        ).fetchone()

        t1_id = t1_row[0] if t1_row else None
        t2_id = t2_row[0] if t2_row else None

        edges = []
        if meta_id and t1_id:
            edges.append((meta_id, t1_id, "formation_t1"))
        if t1_id and t2_id:
            edges.append((t1_id, t2_id, "formation_t2"))
        if t2_id and esm_id:
            edges.append((t2_id, esm_id, "formation_esm"))

        for caller_id, callee_id, kind in edges:
            db.execute(
                "INSERT OR IGNORE INTO graph_edges "
                "(caller_symbol_id, callee_symbol_id, edge_kind, confidence, lineno, indexed_at) "
                "VALUES (?, ?, ?, 1.0, NULL, ?)",
                (caller_id, callee_id, kind, now)
            )
            count += 1

    logger.info("Formation semantic edges: %d for %d formations", count, len(formations))
    return count


def enrich_graph(db: sqlite3.Connection, now: str) -> int:
    """Run all graph enrichments. Called from build_graph_edges()."""
    import_count = _build_import_edges(db, now)
    formation_count = _build_formation_semantic_edges(db, now) if ENABLE_FORMATION_EDGES else 0
    total = import_count + formation_count
    logger.info("Graph enrichment: %d import + %d formation = %d total",
                import_count, formation_count, total)
    return total
