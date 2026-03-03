#!/usr/bin/env python
"""codegrapher — Code Graph Query Tool.

11 subcommands for call-chain tracing, impact analysis, dead code detection,
and more. All queries are instant (pre-built index).

Usage:
    python -m codegrapher.core.query_engine callers my_function
    python -m codegrapher.core.query_engine chain func_a func_b
    
    python -m codegrapher.core.query_engine impact my_function --depth 4
    python -m codegrapher.core.query_engine dead --module src
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import deque
from pathlib import Path

DEFAULT_DB = Path.cwd() / ".codegrapher" / "code_index.db"


# ── Resolver ────────────────────────────────────────────────

class Resolver:
    """Fuzzy FQN resolution: exact -> suffix -> FTS5 fallback."""

    @staticmethod
    def resolve(db: sqlite3.Connection, name: str, kind_filter: str | None = None) -> list[tuple[int, str, str, str]]:
        """Resolve a name to (id, fqn, kind, module_path) tuples."""
        kind_clause = f" AND s.kind = '{kind_filter}'" if kind_filter else ""

        # 1. Exact FQN match
        rows = db.execute(
            f"SELECT s.id, s.fqn, s.kind, m.path FROM symbols s "
            f"JOIN modules m ON m.id = s.module_id "
            f"WHERE s.fqn = ?{kind_clause}", (name,)
        ).fetchall()
        if rows:
            return rows

        # 2. Exact name match (short name)
        rows = db.execute(
            f"SELECT s.id, s.fqn, s.kind, m.path FROM symbols s "
            f"JOIN modules m ON m.id = s.module_id "
            f"WHERE s.name = ?{kind_clause} ORDER BY s.fqn", (name,)
        ).fetchall()
        if rows:
            return rows

        # 3. Suffix match: fqn ends with .name
        rows = db.execute(
            f"SELECT s.id, s.fqn, s.kind, m.path FROM symbols s "
            f"JOIN modules m ON m.id = s.module_id "
            f"WHERE s.fqn LIKE ?{kind_clause} ORDER BY s.fqn",
            (f"%.{name}",)
        ).fetchall()
        if rows:
            return rows

        # 4. FTS5 fallback
        try:
            safe_name = name.replace('"', '""')
            rows = db.execute(
                f"SELECT s.id, s.fqn, s.kind, m.path FROM symbols_fts ft "
                f"JOIN symbols s ON s.id = ft.rowid "
                f"JOIN modules m ON m.id = s.module_id "
                f"WHERE symbols_fts MATCH '\"{safe_name}\"'{kind_clause} "
                f"ORDER BY rank LIMIT 10"
            ).fetchall()
            return rows
        except sqlite3.OperationalError:
            return []

    @staticmethod
    def resolve_one(db: sqlite3.Connection, name: str, kind_filter: str | None = None) -> tuple[int, str] | None:
        """Resolve to a single (id, fqn). Returns None if not found, prints ambiguity."""
        matches = Resolver.resolve(db, name, kind_filter)
        if not matches:
            print(f"  No symbol found matching '{name}'", file=sys.stderr)
            return None
        if len(matches) == 1:
            return matches[0][0], matches[0][1]
        # Multiple matches — prefer exact name match, then shortest FQN
        exact = [m for m in matches if m[1].endswith(f".{name}") or m[1] == name]
        if len(exact) == 1:
            return exact[0][0], exact[0][1]
        # Show disambiguation
        print(f"  Multiple matches for '{name}' — using first:", file=sys.stderr)
        for i, (sid, fqn, kind, path) in enumerate(matches[:5]):
            marker = " *" if i == 0 else ""
            print(f"    [{i}] {fqn} ({kind}) in {path}{marker}", file=sys.stderr)
        return matches[0][0], matches[0][1]


# ── QueryEngine ─────────────────────────────────────────────

class QueryEngine:

    @staticmethod
    def find_callers(db: sqlite3.Connection, name: str, limit: int = 30) -> dict:
        resolved = Resolver.resolve_one(db, name)
        if not resolved:
            return {"target": name, "callers": [], "count": 0}
        sym_id, fqn = resolved

        rows = db.execute("""
            SELECT s.fqn, s.kind, m.path, s.lineno, ge.confidence,
                   COALESCE(syn.synopsis, s.doc_head) AS synopsis
            FROM graph_edges ge
            JOIN symbols s ON s.id = ge.caller_symbol_id
            JOIN modules m ON m.id = s.module_id
            LEFT JOIN synopses syn ON syn.symbol_id = s.id
            WHERE ge.callee_symbol_id = ? AND ge.edge_kind = 'call'
            ORDER BY ge.confidence DESC, m.path
            LIMIT ?
        """, (sym_id, limit)).fetchall()

        callers = [{"fqn": r[0], "kind": r[1], "path": r[2], "line": r[3],
                     "confidence": r[4], "synopsis": r[5]} for r in rows]
        return {"target": fqn, "callers": callers, "count": len(callers)}

    @staticmethod
    def find_callees(db: sqlite3.Connection, name: str, limit: int = 30) -> dict:
        resolved = Resolver.resolve_one(db, name)
        if not resolved:
            return {"source": name, "callees": [], "count": 0}
        sym_id, fqn = resolved

        rows = db.execute("""
            SELECT s.fqn, s.kind, m.path, s.lineno, ge.confidence,
                   COALESCE(syn.synopsis, s.doc_head) AS synopsis
            FROM graph_edges ge
            JOIN symbols s ON s.id = ge.callee_symbol_id
            JOIN modules m ON m.id = s.module_id
            LEFT JOIN synopses syn ON syn.symbol_id = s.id
            WHERE ge.caller_symbol_id = ? AND ge.edge_kind = 'call'
            ORDER BY ge.confidence DESC
            LIMIT ?
        """, (sym_id, limit)).fetchall()

        callees = [{"fqn": r[0], "kind": r[1], "path": r[2], "line": r[3],
                     "confidence": r[4], "synopsis": r[5]} for r in rows]
        return {"source": fqn, "callees": callees, "count": len(callees)}

    @staticmethod
    def call_chain(db: sqlite3.Connection, start_name: str, end_name: str, max_depth: int = 6) -> dict:
        start = Resolver.resolve_one(db, start_name)
        end = Resolver.resolve_one(db, end_name)
        if not start or not end:
            return {"start": start_name, "end": end_name, "paths": [], "found": False}
        start_id, start_fqn = start
        end_id, end_fqn = end

        # Python BFS — recursive CTE with cycle guard can be fragile on large graphs
        # BFS is cleaner and gives shortest path guarantee
        queue: deque[tuple[int, list[int]]] = deque([(start_id, [start_id])])
        visited: set[int] = {start_id}
        paths: list[list[str]] = []

        # Preload adjacency for efficiency
        adj: dict[int, list[int]] = {}
        for row in db.execute(
            "SELECT caller_symbol_id, callee_symbol_id FROM graph_edges WHERE edge_kind = 'call'"
        ).fetchall():
            adj.setdefault(row[0], []).append(row[1])

        while queue and len(paths) < 5:
            current, path = queue.popleft()
            if len(path) - 1 > max_depth:
                break
            if current == end_id and len(path) > 1:
                # Resolve IDs to FQNs
                fqns = []
                for sid in path:
                    row = db.execute("SELECT fqn FROM symbols WHERE id = ?", (sid,)).fetchone()
                    fqns.append(row[0] if row else f"?{sid}")
                paths.append(fqns)
                continue
            for neighbor in adj.get(current, []):
                if neighbor not in visited or neighbor == end_id:
                    if neighbor != end_id:
                        visited.add(neighbor)
                    queue.append((neighbor, path + [neighbor]))

        return {"start": start_fqn, "end": end_fqn, "paths": paths,
                "found": len(paths) > 0, "count": len(paths)}

    @staticmethod
    def find_dead_code(db: sqlite3.Connection, module_filter: str | None = None, limit: int = 50) -> dict:
        mod_clause = ""
        params: list = []
        if module_filter:
            mod_clause = " AND m.path LIKE ?"
            params.append(f"%{module_filter}%")
        params.append(limit)

        rows = db.execute(f"""
            SELECT s.fqn, s.kind, m.path, s.lineno, s.signature,
                   (SELECT COUNT(*) FROM graph_edges ge
                    WHERE ge.callee_symbol_id = s.id AND ge.edge_kind = 'call') AS caller_count
            FROM symbols s
            JOIN modules m ON m.id = s.module_id
            WHERE s.kind IN ('function', 'method')
              AND caller_count = 0
              AND s.name NOT IN ('__init__', '__main__', 'main', 'setup', 'run',
                                  '__new__', '__del__', '__enter__', '__exit__',
                                  '__repr__', '__str__', '__hash__', '__eq__')
              AND s.name NOT LIKE 'test_%'
              AND s.name NOT LIKE '_test_%'
              {mod_clause}
            ORDER BY m.path, s.lineno
            LIMIT ?
        """, params).fetchall()

        dead = [{"fqn": r[0], "kind": r[1], "path": r[2], "line": r[3],
                 "signature": r[4]} for r in rows]
        return {"dead_functions": dead, "count": len(dead), "module_filter": module_filter}

    @staticmethod
    def class_hierarchy(db: sqlite3.Connection, name: str) -> dict:
        resolved = Resolver.resolve_one(db, name, kind_filter="class")
        if not resolved:
            return {"class": name, "parents": [], "children": [], "methods": []}
        sym_id, fqn = resolved

        # Parents
        parents = db.execute("""
            SELECT inh.parent_fqn,
                   (SELECT m.path FROM symbols sp JOIN modules m ON m.id = sp.module_id
                    WHERE sp.fqn = inh.parent_fqn LIMIT 1) AS parent_path
            FROM inheritance inh
            WHERE inh.child_symbol_id = ?
            ORDER BY inh.order_index
        """, (sym_id,)).fetchall()

        # Children (subclasses)
        children = db.execute("""
            SELECT s.fqn, m.path
            FROM inheritance inh
            JOIN symbols s ON s.id = inh.child_symbol_id
            JOIN modules m ON m.id = s.module_id
            WHERE inh.parent_fqn = ?
        """, (fqn,)).fetchall()

        # Methods
        methods = db.execute("""
            SELECT s.name, s.fqn, s.signature, s.lineno,
                   (SELECT COUNT(*) FROM graph_edges ge
                    WHERE ge.callee_symbol_id = s.id AND ge.edge_kind = 'call') AS caller_count
            FROM symbols s
            WHERE s.parent_symbol_id = ? AND s.kind = 'method'
            ORDER BY s.lineno
        """, (sym_id,)).fetchall()

        return {
            "class": fqn,
            "parents": [{"fqn": r[0], "path": r[1]} for r in parents],
            "children": [{"fqn": r[0], "path": r[1]} for r in children],
            "methods": [{"name": r[0], "fqn": r[1], "signature": r[2],
                         "line": r[3], "caller_count": r[4]} for r in methods],
        }

    @staticmethod
    def impact_analysis(db: sqlite3.Connection, name: str, max_depth: int = 6) -> dict:
        resolved = Resolver.resolve_one(db, name)
        if not resolved:
            return {"target": name, "impacted": [], "count": 0}
        sym_id, fqn = resolved

        # Reverse BFS via Python (more reliable than recursive CTE for cycles)
        visited: dict[int, int] = {sym_id: 0}  # id -> min_depth
        frontier: deque[tuple[int, int]] = deque([(sym_id, 0)])

        # Preload reverse adjacency
        rev_adj: dict[int, list[int]] = {}
        for row in db.execute(
            "SELECT caller_symbol_id, callee_symbol_id FROM graph_edges WHERE edge_kind = 'call'"
        ).fetchall():
            rev_adj.setdefault(row[1], []).append(row[0])

        while frontier:
            current, depth = frontier.popleft()
            if depth >= max_depth:
                continue
            for caller_id in rev_adj.get(current, []):
                if caller_id not in visited:
                    visited[caller_id] = depth + 1
                    frontier.append((caller_id, depth + 1))

        # Resolve to symbol details (skip the seed itself)
        impacted = []
        if len(visited) > 1:
            ids = [sid for sid in visited if sid != sym_id]
            if ids:
                placeholders = ",".join("?" for _ in ids)
                rows = db.execute(f"""
                    SELECT s.id, s.fqn, s.kind, m.path, s.lineno
                    FROM symbols s JOIN modules m ON m.id = s.module_id
                    WHERE s.id IN ({placeholders})
                    ORDER BY s.fqn
                """, ids).fetchall()
                for r in rows:
                    impacted.append({"fqn": r[1], "kind": r[2], "path": r[3],
                                     "line": r[4], "depth": visited[r[0]]})
                impacted.sort(key=lambda x: (x["depth"], x["path"]))

        # Unique modules affected
        modules = sorted({i["path"] for i in impacted})

        return {"target": fqn, "impacted": impacted, "count": len(impacted),
                "modules_affected": modules, "module_count": len(modules),
                "max_depth": max_depth}

    @staticmethod
    def module_deps(db: sqlite3.Connection, module_name: str) -> dict:
        # Resolve module
        mod_row = db.execute(
            "SELECT id, path FROM modules WHERE path LIKE ? OR relpath_no_ext LIKE ? LIMIT 1",
            (f"%{module_name}%", f"%{module_name}%")
        ).fetchone()
        if not mod_row:
            return {"module": module_name, "imports": [], "imported_by": []}
        mod_id, mod_path = mod_row

        # What does this module import? (exclude stdlib)
        imports = db.execute("""
            SELECT imp.from_module, imp.imported, imp.lineno, imp.alias
            FROM imports imp
            WHERE imp.module_id = ? AND imp.is_from = 1 AND imp.from_module IS NOT NULL
            ORDER BY imp.from_module
        """, (mod_id,)).fetchall()

        # What modules import from this one?
        relpath = db.execute(
            "SELECT relpath_no_ext FROM modules WHERE id = ?", (mod_id,)
        ).fetchone()
        dotted = relpath[0].replace("/", ".").replace("\\", ".") if relpath else ""

        imported_by = db.execute("""
            SELECT DISTINCT m.path
            FROM imports imp
            JOIN modules m ON m.id = imp.module_id
            WHERE (imp.from_module = ? OR imp.from_module LIKE ?)
              AND m.id != ?
            ORDER BY m.path
        """, (dotted, f"{dotted}.%", mod_id)).fetchall()

        return {
            "module": mod_path,
            "imports": [{"from": r[0], "name": r[1], "line": r[2], "alias": r[3]} for r in imports],
            "imported_by": [r[0] for r in imported_by],
            "import_count": len(imports),
            "imported_by_count": len(imported_by),
        }

    @staticmethod
    def search(db: sqlite3.Connection, query: str, limit: int = 20) -> dict:
        safe_query = query.replace('"', '""')
        try:
            rows = db.execute(f"""
                SELECT s.fqn, s.kind, m.path, s.lineno,
                       COALESCE(syn.synopsis, s.doc_head) AS synopsis,
                       (SELECT COUNT(*) FROM graph_edges ge
                        WHERE ge.callee_symbol_id = s.id AND ge.edge_kind = 'call') AS caller_count
                FROM symbols_fts ft
                JOIN symbols s ON s.id = ft.rowid
                JOIN modules m ON m.id = s.module_id
                LEFT JOIN synopses syn ON syn.symbol_id = s.id
                WHERE symbols_fts MATCH '"{safe_query}"'
                ORDER BY rank
                LIMIT ?
            """, (limit,)).fetchall()
        except sqlite3.OperationalError:
            # FTS5 match failed — fall back to LIKE
            rows = db.execute("""
                SELECT s.fqn, s.kind, m.path, s.lineno,
                       COALESCE(syn.synopsis, s.doc_head) AS synopsis,
                       (SELECT COUNT(*) FROM graph_edges ge
                        WHERE ge.callee_symbol_id = s.id AND ge.edge_kind = 'call') AS caller_count
                FROM symbols s
                JOIN modules m ON m.id = s.module_id
                LEFT JOIN synopses syn ON syn.symbol_id = s.id
                WHERE s.fqn LIKE ? OR s.name LIKE ? OR s.doc_head LIKE ?
                ORDER BY s.fqn
                LIMIT ?
            """, (f"%{query}%", f"%{query}%", f"%{query}%", limit)).fetchall()

        results = [{"fqn": r[0], "kind": r[1], "path": r[2], "line": r[3],
                     "synopsis": r[4], "caller_count": r[5]} for r in rows]
        return {"query": query, "results": results, "count": len(results)}

    @staticmethod
    def discrepancies(db: sqlite3.Connection, type_filter: str | None = None,
                      sev_filter: str | None = None, limit: int = 50) -> dict:
        query = "SELECT type, severity, module_path, lineno, subject, details_json FROM discrepancies WHERE 1=1"
        params: list = []
        if type_filter:
            query += " AND type = ?"
            params.append(type_filter)
        if sev_filter:
            query += " AND severity = ?"
            params.append(sev_filter)
        query += " ORDER BY CASE severity WHEN 'error' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END LIMIT ?"
        params.append(limit)

        rows = db.execute(query, params).fetchall()
        issues = [{"type": r[0], "severity": r[1], "path": r[2], "line": r[3],
                   "subject": r[4], "details": r[5]} for r in rows]

        # Summary counts
        counts = db.execute(
            "SELECT severity, COUNT(*) FROM discrepancies GROUP BY severity"
        ).fetchall()
        summary = {r[0]: r[1] for r in counts}

        return {"issues": issues, "count": len(issues), "summary": summary,
                "type_filter": type_filter, "severity_filter": sev_filter}

    @staticmethod
    def field_readers(db: sqlite3.Connection, field_name: str,
                      source_dict: str | None = None, limit: int = 50) -> dict:
        """Who reads a specific dict field (e.g., stop_pct from FORMATION_METADATA)?"""
        query = """
            SELECT source_dict, field_name, parent_field, consumer_file,
                   consumer_fqn, lineno, access_pattern, confidence, default_value
            FROM field_reads
            WHERE field_name LIKE ?
        """
        params: list = [f"%{field_name}%"]
        if source_dict:
            query += " AND source_dict = ?"
            params.append(source_dict)
        query += " ORDER BY source_dict, consumer_file, lineno LIMIT ?"
        params.append(limit)

        rows = db.execute(query, params).fetchall()
        readers = [{"source_dict": r[0], "field": r[1], "parent_field": r[2],
                     "file": r[3], "function": r[4], "line": r[5],
                     "access": r[6], "confidence": r[7], "default": r[8]} for r in rows]

        # Summary: unique files and access patterns
        files = sorted({r["file"] for r in readers})
        patterns = {}
        for r in readers:
            patterns[r["access"]] = patterns.get(r["access"], 0) + 1

        return {"field": field_name, "source_dict": source_dict, "readers": readers,
                "count": len(readers), "files": files, "file_count": len(files),
                "access_patterns": patterns}

    @staticmethod
    def consumes(db: sqlite3.Connection, name: str, limit: int = 50) -> dict:
        """What dict fields does a function/module consume?"""
        # Resolve name to file paths via symbols table, then query field_reads by file
        file_matches = db.execute(
            "SELECT DISTINCT m.path FROM symbols s JOIN modules m ON m.id = s.module_id "
            "WHERE s.name = ? OR s.fqn LIKE ?",
            (name, f"%{name}%")
        ).fetchall()
        file_paths = [r[0] for r in file_matches]

        # Also try direct file path match
        if not file_paths:
            file_paths = [name]

        # Query field_reads for all matching files
        placeholders = ",".join("?" for _ in file_paths)
        rows = db.execute(f"""
            SELECT source_dict, field_name, parent_field, consumer_file,
                   consumer_fqn, lineno, access_pattern, confidence, default_value
            FROM field_reads
            WHERE consumer_file IN ({placeholders})
               OR consumer_fqn LIKE ?
            ORDER BY source_dict, field_name, lineno
            LIMIT ?
        """, file_paths + [f"%{name}%", limit]).fetchall()

        fields = [{"source_dict": r[0], "field": r[1], "parent_field": r[2],
                    "file": r[3], "function": r[4], "line": r[5],
                    "access": r[6], "confidence": r[7], "default": r[8]} for r in rows]

        # Group by source_dict
        by_dict: dict[str, list] = {}
        for f in fields:
            by_dict.setdefault(f["source_dict"], []).append(f["field"])
        summary = {k: sorted(set(v)) for k, v in by_dict.items()}

        return {"consumer": name, "fields": fields, "count": len(fields),
                "by_source_dict": summary}

    @staticmethod
    def dict_writes(
        db: sqlite3.Connection,
        module_filter: str | None = None,
        field_filter: str | None = None,
        target_dict: str | None = None,
        limit: int = 50,
    ) -> dict:
        """What fields are written into tracked pipeline dicts?

        Complements 'field' (reads) with the write side.
        Cross-references with field_reads to surface ghost fields (written
        but never read) and missing fields (read but never written).
        """
        conditions = []
        params: list = []

        if module_filter:
            conditions.append("writer_file LIKE ?")
            params.append(f"%{module_filter}%")
        if field_filter:
            conditions.append("field_name = ?")
            params.append(field_filter)
        if target_dict:
            conditions.append("target_dict = ?")
            params.append(target_dict)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        rows = db.execute(f"""
            SELECT target_dict, field_name, writer_file, writer_fqn, lineno,
                   value_repr, confidence
            FROM pipeline_dict_writes
            {where}
            ORDER BY target_dict, field_name, writer_file, lineno
            LIMIT ?
        """, params + [limit]).fetchall()

        writes = [
            {
                "dict":       r[0],
                "field":      r[1],
                "file":       r[2],
                "function":   r[3],
                "line":       r[4],
                "value":      r[5],
                "confidence": r[6],
            }
            for r in rows
        ]

        # Group by target_dict for summary
        by_dict: dict[str, list] = {}
        for w in writes:
            by_dict.setdefault(w["dict"], []).append(w["field"])
        summary = {k: sorted(set(v)) for k, v in by_dict.items()}

        # Ghost fields: written but no field_reads entry for same field
        ghost: list[str] = []
        if writes:
            try:
                written_fields = set(w["field"] for w in writes)
                read_fields = set(
                    r[0] for r in db.execute(
                        "SELECT DISTINCT field_name FROM field_reads"
                    ).fetchall()
                )
                ghost = sorted(written_fields - read_fields)
            except Exception:
                pass

        return {
            "writes": writes,
            "count": len(writes),
            "by_dict": summary,
            "ghost_fields": ghost,
            "filter": {
                "module": module_filter,
                "field":  field_filter,
                "dict":   target_dict,
            },
        }


# ── Formatter ───────────────────────────────────────────────

class Formatter:

    def render(self, data: dict, command: str, fmt: str) -> None:
        if fmt == "json":
            print(json.dumps(data, indent=2, default=str))
            return
        method = getattr(self, f"_text_{command}", None)
        if method:
            method(data)
        else:
            # Fallback: pretty-print key stats
            print(json.dumps(data, indent=2, default=str))

    def _text_callers(self, data: dict) -> None:
        print(f"\nCallers of {data['target']} ({data['count']} found)")
        print("-" * 60)
        for c in data["callers"]:
            conf = f"[{c['confidence']:.2f}]" if c.get("confidence") else ""
            print(f"  {conf} {c['fqn']}")
            print(f"         {c['path']}:{c.get('line', '?')}")
            if c.get("synopsis"):
                print(f"         {c['synopsis'][:80]}")

    def _text_callees(self, data: dict) -> None:
        print(f"\nCallees of {data['source']} ({data['count']} found)")
        print("-" * 60)
        for c in data["callees"]:
            conf = f"[{c['confidence']:.2f}]" if c.get("confidence") else ""
            print(f"  {conf} {c['fqn']}")
            print(f"         {c['path']}:{c.get('line', '?')}")

    def _text_chain(self, data: dict) -> None:
        print(f"\nCall chain: {data['start']} -> {data['end']}")
        print("-" * 60)
        if not data["found"]:
            print("  No path found (try increasing --depth)")
            return
        for i, path in enumerate(data["paths"]):
            print(f"\n  Path {i+1} (length {len(path)-1}):")
            for j, fqn in enumerate(path):
                prefix = "    -> " if j > 0 else "    "
                print(f"{prefix}{fqn}")

    def _text_dead(self, data: dict) -> None:
        mod = f" in {data['module_filter']}" if data.get("module_filter") else ""
        print(f"\nDead functions{mod} ({data['count']} found)")
        print("-" * 60)
        for d in data["dead_functions"]:
            sig = d.get("signature") or ""
            print(f"  {d['fqn']}")
            print(f"    {d['path']}:{d.get('line', '?')}  {sig[:60]}")

    def _text_class(self, data: dict) -> None:
        print(f"\nClass: {data['class']}")
        print("-" * 60)
        if data["parents"]:
            print("  Parents:")
            for p in data["parents"]:
                print(f"    {p['fqn']}  ({p.get('path', '?')})")
        if data["children"]:
            print("  Children:")
            for c in data["children"]:
                print(f"    {c['fqn']}  ({c.get('path', '?')})")
        if data["methods"]:
            print(f"  Methods ({len(data['methods'])}):")
            for m in data["methods"]:
                callers = f"  [{m['caller_count']} callers]" if m.get("caller_count") else ""
                sig = m.get("signature") or ""
                print(f"    {m['name']}{sig[:40]}{callers}")

    def _text_impact(self, data: dict) -> None:
        print(f"\nImpact analysis for {data['target']}")
        print(f"  {data['count']} functions affected across {data['module_count']} modules (depth {data['max_depth']})")
        print("-" * 60)
        if data.get("modules_affected"):
            print("  Affected modules:")
            for m in data["modules_affected"]:
                print(f"    {m}")
        print()
        current_depth = -1
        for item in data["impacted"]:
            if item["depth"] != current_depth:
                current_depth = item["depth"]
                print(f"  Depth {current_depth}:")
            print(f"    {item['fqn']}  ({item['path']}:{item.get('line', '?')})")

    def _text_deps(self, data: dict) -> None:
        print(f"\nModule: {data['module']}")
        print("-" * 60)
        print(f"  Imports ({data['import_count']}):")
        for imp in data["imports"]:
            alias = f" as {imp['alias']}" if imp.get("alias") else ""
            print(f"    from {imp['from']} import {imp['name']}{alias}  (L{imp.get('line', '?')})")
        print(f"\n  Imported by ({data['imported_by_count']}):")
        for path in data["imported_by"]:
            print(f"    {path}")

    def _text_search(self, data: dict) -> None:
        print(f"\nSearch: \"{data['query']}\" ({data['count']} results)")
        print("-" * 60)
        for r in data["results"]:
            callers = f"  [{r['caller_count']} callers]" if r.get("caller_count") else ""
            print(f"  {r['kind']:8s} {r['fqn']}{callers}")
            print(f"           {r['path']}:{r.get('line', '?')}")
            if r.get("synopsis"):
                print(f"           {r['synopsis'][:80]}")

    def _text_field(self, data: dict) -> None:
        src = f" from {data['source_dict']}" if data.get("source_dict") else ""
        print(f"\nField readers: '{data['field']}'{src} ({data['count']} reads across {data['file_count']} files)")
        if data.get("access_patterns"):
            parts = [f"{p}: {c}" for p, c in sorted(data["access_patterns"].items())]
            print(f"  Access patterns: {', '.join(parts)}")
        print("-" * 60)
        for r in data["readers"]:
            nested = f" (nested in {r['parent_field']})" if r.get("parent_field") else ""
            default = f"  default={r['default']}" if r.get("default") else ""
            fn = f"  fn={r['function']}" if r.get("function") else ""
            print(f"  [{r['confidence']:6s}] {r['source_dict']}.{r['field']}{nested}")
            print(f"          {r['file']}:{r.get('line', '?')}  [{r['access']}]{default}{fn}")

    def _text_consumes(self, data: dict) -> None:
        print(f"\nFields consumed by '{data['consumer']}' ({data['count']} reads)")
        print("-" * 60)
        for src_dict, fields in data.get("by_source_dict", {}).items():
            print(f"\n  {src_dict} ({len(fields)} fields):")
            for f in fields:
                print(f"    {f}")
        if data.get("fields"):
            print(f"\n  Detail:")
            for r in data["fields"]:
                nested = f" (in {r['parent_field']})" if r.get("parent_field") else ""
                print(f"    {r['source_dict']}.{r['field']}{nested}  {r['file']}:{r.get('line', '?')}  [{r['access']}]")

    def _text_dict_writes(self, data: dict) -> None:
        f = data.get("filter", {})
        filter_parts = [f"{k}={v}" for k, v in f.items() if v]
        filter_str = f" ({', '.join(filter_parts)})" if filter_parts else ""
        print(f"\nPipeline dict writes{filter_str} ({data['count']} records)")
        print("-" * 60)
        for d, fields in data.get("by_dict", {}).items():
            print(f"\n  {d} ({len(fields)} distinct fields):")
            for field in fields:
                print(f"    {field}")
        if data.get("ghost_fields"):
            print(f"\n  Ghost fields (written but never read by field_reads):")
            for g in data["ghost_fields"]:
                print(f"    {g}")
        if data.get("writes"):
            print(f"\n  Detail:")
            for w in data["writes"][:20]:
                val = f" = {w['value']}" if w.get("value") else ""
                print(f"    {w['dict']}.{w['field']}{val}  {w['file']}:{w['line']}  [{w['confidence']}]")

    def _text_issues(self, data: dict) -> None:
        summary = data.get("summary", {})
        filters = []
        if data.get("type_filter"):
            filters.append(f"type={data['type_filter']}")
        if data.get("severity_filter"):
            filters.append(f"severity={data['severity_filter']}")
        filter_str = f" ({', '.join(filters)})" if filters else ""

        print(f"\nDiscrepancies{filter_str} ({data['count']} shown)")
        if summary:
            parts = [f"{sev}: {cnt}" for sev, cnt in sorted(summary.items())]
            print(f"  Total: {', '.join(parts)}")
        print("-" * 60)
        for d in data["issues"]:
            print(f"  [{d['severity']:5s}] {d['type']}")
            print(f"         {d.get('path', '?')}:{d.get('line', '?')}  subject={d.get('subject', '?')}")


# ── CLI ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="code_graph_query",
        description="codegrapher — Query the pre-built code index",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to code_index.db")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    parser.add_argument("--limit", type=int, default=30, help="Max results (default: 30)")

    sub = parser.add_subparsers(dest="cmd", required=True)

    # callers
    p = sub.add_parser("callers", help="Who calls this function?")
    p.add_argument("name", help="Function name or FQN")

    # callees
    p = sub.add_parser("callees", help="What does this function call?")
    p.add_argument("name", help="Function name or FQN")

    # chain
    p = sub.add_parser("chain", help="Shortest call path between two functions")
    p.add_argument("start", help="Start function")
    p.add_argument("end", help="End function")
    p.add_argument("--depth", type=int, default=6, help="Max BFS depth (default: 6)")

    # dead
    p = sub.add_parser("dead", help="Dead functions (zero callers)")
    p.add_argument("--module", help="Filter by module path substring")

    # class
    p = sub.add_parser("class", help="Class hierarchy — parents, children, methods")
    p.add_argument("name", help="Class name or FQN")

    # impact
    p = sub.add_parser("impact", help="Blast radius — all transitive callers")
    p.add_argument("name", help="Function name or FQN")
    p.add_argument("--depth", type=int, default=6, help="Max BFS depth (default: 6)")

    # deps
    p = sub.add_parser("deps", help="Import graph for a module")
    p.add_argument("module", help="Module path or name")

    # search
    p = sub.add_parser("search", help="FTS5 search across all symbols")
    p.add_argument("query", help="Search query")

    # field
    p = sub.add_parser("field", help="Who reads a dict field? (e.g., stop_pct)")
    p.add_argument("name", help="Field name (e.g., stop_pct, trailing_activation)")
    p.add_argument("--source", help="Filter by source dict (e.g., FORMATION_METADATA)")

    # consumes
    p = sub.add_parser("consumes", help="What dict fields does a function/module consume?")
    p.add_argument("name", help="Function name, FQN, or file path")

    # issues
    p = sub.add_parser("issues", help="Discrepancies report")
    p.add_argument("--type", dest="type_filter", help="Filter by type")
    p.add_argument("--sev", dest="sev_filter", help="Filter by severity (error|warn|info)")

    # dict-writes
    p = sub.add_parser("dict-writes", help="Pipeline dict write inventory (write side of field_reads)")
    p.add_argument("--module", help="Filter by module path substring (e.g., src/flow)")
    p.add_argument("--field", help="Filter by field name (e.g., flow_multiplier)")
    p.add_argument("--dict", dest="target_dict", help="Filter by target dict (e.g., trade_setup)")

    args = parser.parse_args()

    # Open DB read-only
    db_path = args.db
    if not db_path.exists():
        print(f"Database not found: {db_path}", file=sys.stderr)
        print("Run: python -m codegrapher.core.indexer --clean", file=sys.stderr)
        sys.exit(1)

    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.OperationalError:
        # Fallback for older SQLite without URI support
        db = sqlite3.connect(str(db_path))

    engine = QueryEngine()
    fmt = Formatter()

    # Dispatch
    if args.cmd == "callers":
        result = engine.find_callers(db, args.name, limit=args.limit)
    elif args.cmd == "callees":
        result = engine.find_callees(db, args.name, limit=args.limit)
    elif args.cmd == "chain":
        result = engine.call_chain(db, args.start, args.end, max_depth=args.depth)
    elif args.cmd == "dead":
        result = engine.find_dead_code(db, module_filter=args.module, limit=args.limit)
    elif args.cmd == "class":
        result = engine.class_hierarchy(db, args.name)
    elif args.cmd == "impact":
        result = engine.impact_analysis(db, args.name, max_depth=args.depth)
    elif args.cmd == "deps":
        result = engine.module_deps(db, args.module)
    elif args.cmd == "search":
        result = engine.search(db, args.query, limit=args.limit)
    elif args.cmd == "field":
        result = engine.field_readers(db, args.name, source_dict=args.source, limit=args.limit)
    elif args.cmd == "consumes":
        result = engine.consumes(db, args.name, limit=args.limit)
    elif args.cmd == "issues":
        result = engine.discrepancies(db, type_filter=args.type_filter,
                                       sev_filter=args.sev_filter, limit=args.limit)
    elif args.cmd == "dict-writes":
        result = engine.dict_writes(db, module_filter=args.module,
                                    field_filter=args.field, target_dict=args.target_dict,
                                    limit=args.limit)
        # Map command to formatter method name (hyphen → underscore)
        args.cmd = "dict_writes"
    else:
        parser.print_help()
        sys.exit(1)

    fmt.render(result, args.cmd, "json" if args.json else "text")
    db.close()


if __name__ == "__main__":
    main()
