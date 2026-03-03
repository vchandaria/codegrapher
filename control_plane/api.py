"""Control Plane API — stable functions for querying the code index.

This is the primary tool surface for PTC scripts and the CLI.
Default mode is read_only; enforce mode requires explicit opt-in.

Guardrails:
- sql() blocks all mutation keywords (INSERT/UPDATE/DELETE/DROP/ALTER/CREATE)
- Max 30 SQL calls per session, max 500 rows per query
- Write primitives only available in enforce mode
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .graph import get_subgraph_bfs, get_callers, get_callees
from .ranking import rank_symbols, dedupe_and_cluster
from .packs import build_pack, estimate_tokens
from .synopses import build_all_synopses

logger = logging.getLogger(__name__)

from ..core.config import INTENT_PATH, DB_PATH as _DB_PATH
from pathlib import Path as _Path
INDEX_DIR = _Path(_DB_PATH).parent

# SQL mutation pattern
_MUTATION_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|REPLACE|ATTACH|DETACH|VACUUM|REINDEX)\b",
    re.IGNORECASE,
)


class ControlPlane:
    """Stable API for the code index.

    Usage:
        cp = ControlPlane(db_path)
        rows = cp.sql("SELECT fqn FROM symbols WHERE kind = 'function' LIMIT 10")
        proof = cp.get_proof()
        pack = cp.build_pack("bugfix", seeds=["src.config.my_function"])
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        mode: str = "read_only",
        max_sql: int = 30,
        max_rows: int = 500,
    ):
        if db_path is None:
            db_path = INDEX_DIR / "code_index.db"
        self._db_path = Path(db_path)
        self._db = sqlite3.connect(str(self._db_path))
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.row_factory = sqlite3.Row
        self._mode = mode
        self._sql_count = 0
        self._total_rows = 0
        self._max_sql = max_sql
        self._max_rows = max_rows

    def close(self):
        """Close the database connection."""
        self._db.close()

    @property
    def stats(self) -> dict:
        """Current session stats."""
        return {
            "mode": self._mode,
            "sql_calls": self._sql_count,
            "total_rows": self._total_rows,
            "max_sql": self._max_sql,
            "max_rows": self._max_rows,
        }

    # ── Read-Only Primitives ──────────────────────────

    def sql(self, query: str, params: dict | None = None, limit: int | None = None) -> list[dict]:
        """Execute a read-only SQL query.

        Blocks all mutation keywords. Enforces row and call limits.
        Returns list of dicts (column_name: value).
        """
        # Validate read-only
        if _MUTATION_PATTERN.search(query):
            raise PermissionError(f"Mutation queries blocked in {self._mode} mode. Use write primitives instead.")

        # Enforce call limit
        self._sql_count += 1
        if self._sql_count > self._max_sql:
            raise RuntimeError(f"SQL call limit exceeded ({self._max_sql}). Create a new ControlPlane instance.")

        # Enforce row limit
        effective_limit = min(limit or self._max_rows, self._max_rows)
        # Inject LIMIT if not already present
        query_upper = query.upper().strip()
        if "LIMIT" not in query_upper:
            query = f"{query.rstrip(';')} LIMIT {effective_limit}"

        try:
            if params:
                cursor = self._db.execute(query, params)
            else:
                cursor = self._db.execute(query)
            rows = cursor.fetchall()
            self._total_rows += len(rows)
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            raise RuntimeError(f"SQL error: {e}") from e

    def get_proof(self, run_id: str | None = "latest") -> dict:
        """Get proof report.

        Returns {run_id, rules: [{name, kind, status, details}], summary: {pass, fail, skip, total}}
        """
        if run_id == "latest":
            row = self._db.execute("SELECT MAX(run_id) FROM proof_results").fetchone()
            run_id = row[0] if row and row[0] else None

        if not run_id:
            return {"run_id": None, "rules": [], "summary": {"total": 0, "pass": 0, "fail": 0, "skip": 0}}

        rows = self._db.execute(
            "SELECT rule_name, rule_kind, status, details_json FROM proof_results WHERE run_id = ?",
            (run_id,)
        ).fetchall()

        rules = []
        for r in rows:
            details = {}
            if r["details_json"]:
                try:
                    details = json.loads(r["details_json"])
                except (json.JSONDecodeError, TypeError):
                    pass
            rules.append({
                "name": r["rule_name"],
                "kind": r["rule_kind"],
                "status": r["status"],
                "details": details,
            })

        summary = {
            "total": len(rules),
            "pass": sum(1 for r in rules if r["status"] == "pass"),
            "fail": sum(1 for r in rules if r["status"] == "fail"),
            "skip": sum(1 for r in rules if r["status"] == "skip"),
        }

        return {"run_id": run_id, "rules": rules, "summary": summary}

    def get_discrepancies(self, filter: dict | None = None, limit: int = 200, decay: bool = False) -> list[dict]:
        """Query discrepancies with optional filter.

        Filter keys: type, severity, formation, module_path
        Set decay=True to apply temporal+importance scoring.
        """
        query = "SELECT type, severity, module_path, lineno, subject, details_json FROM discrepancies WHERE 1=1"
        params: list = []

        if filter:
            if "type" in filter:
                query += " AND type = ?"
                params.append(filter["type"])
            if "severity" in filter:
                query += " AND severity = ?"
                params.append(filter["severity"])
            if "module_path" in filter:
                query += " AND module_path LIKE ?"
                params.append(f"%{filter['module_path']}%")
            if "formation" in filter:
                query += " AND subject = ?"
                params.append(filter["formation"])

        effective_limit = min(limit, self._max_rows)
        query += f" ORDER BY CASE severity WHEN 'error' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END LIMIT {effective_limit}"

        rows = self._db.execute(query, params).fetchall()
        results = []
        for r in rows:
            item = dict(r)
            if item.get("details_json"):
                try:
                    item["details"] = json.loads(item["details_json"])
                except (json.JSONDecodeError, TypeError):
                    item["details"] = {}
                del item["details_json"]
            results.append(item)

        # Apply temporal decay + importance scoring
        if decay:
            try:
                from .retrieval.temporal import apply_decay_to_discrepancies
                results = apply_decay_to_discrepancies(results)
            except ImportError:
                pass

        return results

    def get_lineage(self, run_id: str | None = "latest") -> dict:
        """Get index build lineage: table stats and file counts."""
        tables = {}
        for table in ("modules", "symbols", "imports", "calls", "registries", "constants",
                       "env_vars", "discrepancies", "call_resolution", "research_params",
                       "json_config_entries", "business_rules", "pipeline_steps",
                       "graph_edges", "proof_results", "synopses", "agent_runs"):
            try:
                cnt = self._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                tables[table] = cnt
            except sqlite3.Error:
                tables[table] = 0

        return {
            "tables": tables,
            "total_files": tables.get("modules", 0),
            "total_symbols": tables.get("symbols", 0),
            "total_calls": tables.get("calls", 0),
            "total_discrepancies": tables.get("discrepancies", 0),
        }

    def get_artifact_catalog(self) -> dict:
        """Get artifact catalog: modules with symbol and call counts."""
        rows = self._db.execute(
            """SELECT m.path,
                      (SELECT COUNT(*) FROM symbols WHERE module_id = m.id) as sym_count,
                      (SELECT COUNT(*) FROM imports WHERE module_id = m.id) as imp_count,
                      (SELECT COUNT(*) FROM calls WHERE module_id = m.id) as call_count,
                      m.loc, m.size
               FROM modules m
               ORDER BY call_count DESC LIMIT 100"""
        ).fetchall()

        files = [dict(r) for r in rows]
        return {"files": files, "total_modules": len(files)}

    def get_symbol(self, fqn: str | None = None, symbol_id: int | None = None) -> dict:
        """Get symbol details with synopsis, parents, callers, callees."""
        if fqn:
            row = self._db.execute(
                """SELECT s.id, s.fqn, s.kind, s.signature, s.doc_head, s.complexity,
                          m.path, s.lineno, s.end_lineno,
                          syn.synopsis, syn.mini_trace
                   FROM symbols s
                   JOIN modules m ON m.id = s.module_id
                   LEFT JOIN synopses syn ON syn.symbol_id = s.id
                   WHERE s.fqn = ?""",
                (fqn,)
            ).fetchone()
        elif symbol_id:
            row = self._db.execute(
                """SELECT s.id, s.fqn, s.kind, s.signature, s.doc_head, s.complexity,
                          m.path, s.lineno, s.end_lineno,
                          syn.synopsis, syn.mini_trace
                   FROM symbols s
                   JOIN modules m ON m.id = s.module_id
                   LEFT JOIN synopses syn ON syn.symbol_id = s.id
                   WHERE s.id = ?""",
                (symbol_id,)
            ).fetchone()
        else:
            return {}

        if not row:
            return {}

        sym_id = row[0]
        callers = get_callers(self._db, sym_id, limit=10)
        callees = get_callees(self._db, sym_id, limit=10)

        # Get parent class if method
        parents = []
        if row[2] == "method":
            parent_rows = self._db.execute(
                "SELECT s2.fqn FROM symbols s2 WHERE s2.id = "
                "(SELECT parent_symbol_id FROM symbols WHERE id = ?)",
                (sym_id,)
            ).fetchall()
            parents = [r[0] for r in parent_rows]

        return {
            "id": sym_id,
            "fqn": row[1],
            "kind": row[2],
            "signature": row[3],
            "doc_head": row[4],
            "complexity": row[5],
            "module_path": row[6],
            "lineno": row[7],
            "end_lineno": row[8],
            "synopsis": row[9],
            "mini_trace": row[10],
            "parents": parents,
            "callers": callers,
            "callees": callees,
        }

    def get_subgraph(
        self,
        seeds: list[str],
        depth: int = 1,
        intent_class: str = "bugfix",
    ) -> dict:
        """BFS from seed symbols. Returns nodes, edges, and relevant proof failures."""
        subgraph = get_subgraph_bfs(self._db, seeds, depth=depth)

        # Enrich with proof failures for nodes in subgraph
        node_modules = {n.get("module_path") for n in subgraph.get("nodes", [])}
        proof_failures = []
        if node_modules:
            proof_report = self.get_proof()
            for rule in proof_report.get("rules", []):
                if rule["status"] == "fail":
                    violations = rule.get("details", {}).get("violations", [])
                    for v in violations:
                        if isinstance(v, dict) and v.get("file") in node_modules:
                            proof_failures.append({"rule": rule["name"], "violation": v})

        subgraph["proof_failures"] = proof_failures
        return subgraph

    def rank(
        self,
        seeds: list[str] | None = None,
        filter: dict | None = None,
        intent_class: str = "bugfix",
        k: int = 50,
    ) -> list[dict]:
        """Rank symbols by intent-weighted relevance."""
        return rank_symbols(self._db, seeds=seeds, filter=filter, intent_class=intent_class, k=k)

    # ── Write Primitives (enforce mode only) ──────────

    def _require_enforce(self):
        """Raise if not in enforce mode."""
        if self._mode != "enforce":
            raise PermissionError(
                "Write operation requires enforce mode. "
                "Initialize ControlPlane with mode='enforce'."
            )

    def run_recipe(self, recipe_id: str, scope: dict | None = None, mode: str = "dry_run") -> dict:
        """Execute a recipe. dry_run previews; enforce applies changes."""
        if mode == "enforce":
            self._require_enforce()
        return _run_recipe(self._db, recipe_id, scope, mode)

    def reindex(self, scope: dict | None = None, incremental: bool = True) -> dict:
        """Trigger re-index. Returns summary of what was reindexed."""
        self._require_enforce()

        import subprocess
        cmd = ["python", "-m", "codegrapher.core.indexer"]
        if incremental:
            cmd.append("--incremental")
        else:
            cmd.append("--clean")

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=str(ROOT))
            return {
                "status": "completed" if result.returncode == 0 else "error",
                "returncode": result.returncode,
                "stdout_tail": result.stdout[-500:] if result.stdout else "",
                "stderr_tail": result.stderr[-500:] if result.stderr else "",
            }
        except subprocess.TimeoutExpired:
            return {"status": "timeout"}

    def run_proof(self, run_id: str | None = None) -> dict:
        """Run proof engine. Returns proof report."""
        return run_proof_engine(self._db, INTENT_PATH)

    # ── Pack Building ─────────────────────────────────

    def semantic_search(
        self,
        query: str,
        top_k: int = 20,
        intent_class: str | None = None,
    ) -> list[dict]:
        """Semantic search over code symbols using V4 retrieval pipeline.

        Combines FTS5 + vector search, RRF fusion, graph boost,
        cross-encoder reranking, and agentic evaluation.

        Args:
            query: Natural language or symbol query
            top_k: Max results
            intent_class: Optional intent hint

        Returns:
            List of symbol dicts with score and metadata.
        """
        from .retrieval.search import semantic_search as _semantic_search
        from dataclasses import asdict
        results = _semantic_search(self._db, query, top_k, intent_class)
        return [asdict(r) for r in results]

    def build_embeddings(self, force: bool = False) -> int:
        """Build/rebuild vector embeddings for semantic search.

        Returns number of symbols embedded.
        """
        from .retrieval.embeddings import build_embeddings as _build_embeddings
        return _build_embeddings(self._db, force=force)

    def build_context_pack(
        self,
        template: str = "bugfix",
        seeds: list[str] | None = None,
        intent_class: str | None = None,
        max_tokens: int = 2000,
    ) -> dict:
        """Build a context pack from a template."""
        return build_pack(self._db, template, seeds=seeds, intent_class=intent_class, max_tokens=max_tokens)
