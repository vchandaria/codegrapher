from .config import SKELETON_TITLE, SKELETON_EXCLUDE_PATHS
"""Repo skeleton builder — Aider-style signatures-only map.

Hard budget: 50K tokens total (~200KB text).
Strategy:
  - Top 50 modules by connectivity: full signatures
  - Remaining modules: 1-line summary
"""

import logging
import sqlite3
from io import StringIO
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_TOKENS_ESTIMATE = 50_000  # ~4 chars/token = 200KB
CHARS_BUDGET = MAX_TOKENS_ESTIMATE * 4


def build_skeleton(db: sqlite3.Connection, output_path: Path) -> None:
    """Build repo_skeleton.txt from the index database."""
    buf = StringIO()
    buf.write(f"# {SKELETON_TITLE}\n")
    buf.write("# Signatures only — no bodies. Query code_index.db for details.\n\n")

    # Compute module connectivity (imports_from + imported_by)
    modules = db.execute("""
        SELECT m.id, m.path, m.loc,
            (SELECT COUNT(*) FROM imports i WHERE i.module_id = m.id) as out_degree,
            (SELECT COUNT(*) FROM imports i2
             WHERE i2.imported LIKE '%' || REPLACE(m.relpath_no_ext, '/', '.') || '%') as in_degree
        FROM modules m
        WHERE 1=1
        ORDER BY (out_degree + in_degree) DESC
    """).fetchall()

    # Split into top-N and rest
    top_n = min(50, len(modules))
    top_modules = modules[:top_n]
    rest_modules = modules[top_n:]

    # ── Top modules: full signatures ──
    buf.write(f"## Core Modules ({top_n} by connectivity)\n\n")
    for mod_id, path, loc, out_deg, in_deg in top_modules:
        buf.write(f"### {path} ({loc}L, {in_deg}←/{out_deg}→)\n")

        # Classes + methods
        classes = db.execute("""
            SELECT s.name, s.lineno, s.end_lineno, s.doc_head
            FROM symbols s WHERE s.module_id = ? AND s.kind = 'class' AND s.scope_depth = 0
            ORDER BY s.lineno
        """, (mod_id,)).fetchall()

        for cls_name, cls_line, cls_end, cls_doc in classes:
            doc_hint = f"  # {cls_doc[:60]}" if cls_doc else ""
            buf.write(f"  class {cls_name}:{doc_hint}\n")
            methods = db.execute("""
                SELECT s.name, s.signature, s.is_async
                FROM symbols s
                WHERE s.module_id = ? AND s.kind = 'method'
                  AND s.parent_symbol_id = (
                      SELECT id FROM symbols WHERE module_id = ? AND name = ? AND kind = 'class' LIMIT 1
                  )
                ORDER BY s.lineno
            """, (mod_id, mod_id, cls_name)).fetchall()
            for mname, msig, masync in methods:
                prefix = "async " if masync else ""
                buf.write(f"    {prefix}def {mname}{msig or '(...)'}\n")

        # Top-level functions
        funcs = db.execute("""
            SELECT s.name, s.signature, s.is_async, s.doc_head
            FROM symbols s
            WHERE s.module_id = ? AND s.kind = 'function' AND s.scope_depth = 0
            ORDER BY s.lineno
        """, (mod_id,)).fetchall()

        for fname, fsig, fasync, fdoc in funcs:
            prefix = "async " if fasync else ""
            doc_hint = f"  # {fdoc[:60]}" if fdoc else ""
            buf.write(f"  {prefix}def {fname}{fsig or '(...)'}{doc_hint}\n")

        # Top-level registries/constants
        regs = db.execute("""
            SELECT r.name, r.kind, r.value_mode
            FROM registries r WHERE r.module_id = ?
            ORDER BY r.lineno
        """, (mod_id,)).fetchall()
        for rname, rkind, rmode in regs:
            buf.write(f"  {rname}: {rkind} [{rmode}]\n")

        buf.write("\n")

        # Budget check
        if buf.tell() > CHARS_BUDGET * 0.8:
            buf.write("\n# ... truncated (budget limit) ...\n")
            break

    # ── Rest: 1-line summary ──
    if rest_modules:
        buf.write(f"\n## Other Modules ({len(rest_modules)} files)\n\n")
        for mod_id, path, loc, out_deg, in_deg in rest_modules:
            sym_count = db.execute(
                "SELECT COUNT(*) FROM symbols WHERE module_id = ?", (mod_id,)
            ).fetchone()[0]
            buf.write(f"  {path} ({loc}L, {sym_count} symbols)\n")
            if buf.tell() > CHARS_BUDGET * 0.95:
                buf.write("  ... truncated ...\n")
                break

    content = buf.getvalue()
    output_path.write_text(content, encoding="utf-8")
    token_estimate = len(content) // 4
    logger.info(f"Wrote skeleton: {len(content)} chars (~{token_estimate} tokens) to {output_path}")
