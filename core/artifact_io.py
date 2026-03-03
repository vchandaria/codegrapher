"""Artifact I/O Extractor — catalogs file read/write callsites across the codebase.

Detects parquet, JSON, CSV, and SQLite I/O patterns and records them
in the artifact_io table for lineage tracking and orphan detection.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# I/O detection patterns: (regex, io_direction, io_format)
IO_PATTERNS: list[tuple[str, str, str]] = [
    # Parquet
    (r"pd\.read_parquet\s*\(", "read", "parquet"),
    (r"\.read_parquet\s*\(", "read", "parquet"),
    (r"read_parquet\s*\(", "read", "parquet"),
    (r"\.to_parquet\s*\(", "write", "parquet"),
    (r"to_parquet\s*\(", "write", "parquet"),
    # JSON
    (r"json\.load\s*\(", "read", "json"),
    (r"json\.dump\s*\(", "write", "json"),
    # CSV
    (r"pd\.read_csv\s*\(", "read", "csv"),
    (r"\.read_csv\s*\(", "read", "csv"),
    (r"csv\.reader\s*\(", "read", "csv"),
    (r"\.to_csv\s*\(", "write", "csv"),
    (r"csv\.writer\s*\(", "write", "csv"),
    # SQLite
    (r"sqlite3\.connect\s*\(", "read", "sqlite"),
]

# Compiled patterns for performance
_COMPILED_PATTERNS = [(re.compile(p), d, f) for p, d, f in IO_PATTERNS]

# Path extraction: first string argument in the matched line
_PATH_RE = re.compile(r"""['"]([\w./\\:~\-{}%$]+(?:\.(?:parquet|json|csv|db|sqlite|jsonl))?)['"]\s*""")


def extract_artifact_io(root: Path, db: sqlite3.Connection) -> int:
    """Scan all indexed modules for file I/O callsites.

    Returns count of records written to artifact_io table.
    """
    now = datetime.now(timezone.utc).isoformat()
    db.execute("DELETE FROM artifact_io")
    count = 0

    # Get all indexed modules
    modules = db.execute("SELECT id, path FROM modules").fetchall()

    for module_id, rel_path in modules:
        full_path = root / rel_path
        if not full_path.exists():
            continue

        try:
            source = full_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        lines = source.split("\n")

        for line_idx, line in enumerate(lines):
            stripped = line.strip()
            # Skip comments
            if stripped.startswith("#"):
                continue

            for pattern, io_dir, io_fmt in _COMPILED_PATTERNS:
                if pattern.search(line):
                    lineno = line_idx + 1

                    # Extract path expression
                    path_expr = _extract_path_expr(line)

                    # Find enclosing function FQN
                    symbol_fqn = _find_enclosing_symbol(db, module_id, lineno)

                    db.execute(
                        """INSERT INTO artifact_io
                           (module_id, module_path, lineno, io_direction,
                            io_format, path_expr, symbol_fqn, indexed_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (module_id, rel_path, lineno, io_dir,
                         io_fmt, path_expr, symbol_fqn, now),
                    )
                    count += 1
                    break  # One match per line is enough

    db.commit()
    logger.info("Artifact I/O extracted: %d callsites across %d modules", count, len(modules))
    return count


def check_artifact_orphans(db: sqlite3.Connection) -> int:
    """Detect artifacts written but never read (or vice versa).

    Emits ARTIFACT_ORPHAN discrepancies.
    Returns count of discrepancies.
    """
    now = datetime.now(timezone.utc).isoformat()
    count = 0

    # Get all path_exprs grouped by direction
    writes = db.execute(
        "SELECT DISTINCT path_expr, module_path, lineno FROM artifact_io "
        "WHERE io_direction = 'write' AND path_expr IS NOT NULL"
    ).fetchall()

    reads = db.execute(
        "SELECT DISTINCT path_expr FROM artifact_io "
        "WHERE io_direction = 'read' AND path_expr IS NOT NULL"
    ).fetchall()

    read_paths = {r[0] for r in reads}

    for path_expr, module_path, lineno in writes:
        # Check if this write path is ever read
        if path_expr not in read_paths:
            # Also check partial matches (the write path might be a variable expression)
            has_reader = any(
                path_expr in rp or rp in path_expr
                for rp in read_paths
            )
            if not has_reader:
                db.execute(
                    """INSERT INTO discrepancies
                       (type, severity, module_path, lineno, subject,
                        details_json, indexed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    ("ARTIFACT_ORPHAN", "info", module_path, lineno,
                     path_expr,
                     json.dumps({"reason": "write-only artifact", "path": path_expr}),
                     now),
                )
                count += 1

    db.commit()
    logger.info("Artifact orphan check: %d orphans detected", count)
    return count


def _extract_path_expr(line: str) -> str | None:
    """Extract the file path expression from an I/O line."""
    match = _PATH_RE.search(line)
    if match:
        return match.group(1)

    # Try f-string or variable patterns
    fstr = re.search(r'f["\']([^"\']+)["\']', line)
    if fstr:
        return fstr.group(1)

    return None


def _find_enclosing_symbol(
    db: sqlite3.Connection, module_id: int, lineno: int
) -> str | None:
    """Find the FQN of the enclosing function/method for a given line."""
    row = db.execute(
        """SELECT fqn FROM symbols
           WHERE module_id = ? AND kind IN ('function', 'method')
             AND lineno <= ? AND (end_lineno IS NULL OR end_lineno >= ?)
           ORDER BY lineno DESC LIMIT 1""",
        (module_id, lineno, lineno),
    ).fetchone()
    return row[0] if row else None
