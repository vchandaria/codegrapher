"""Optional external tool integration: radon (complexity), vulture (dead code).

These are run only if installed. PyCG is deferred to Phase 2.
"""

import json
import logging
import sqlite3
import subprocess
from pathlib import Path
from typing import Optional, TextIO

from .persistence import persist_discrepancy

logger = logging.getLogger(__name__)


def run_radon_complexity(
    db: sqlite3.Connection,
    root: Path,
    dirs: list[str],
) -> bool:
    """Run radon cc and update symbols.complexity. Returns True if radon was available."""
    try:
        args = ["python", "-m", "radon", "cc", "-s", "-j"] + [
            str(root / d) for d in dirs
        ]
        result = subprocess.run(
            args, capture_output=True, text=True, timeout=600, cwd=str(root)
        )
        if result.returncode != 0:
            logger.warning(f"radon failed: {result.stderr[:200]}")
            return False
    except FileNotFoundError:
        logger.info("radon not installed — skipping complexity analysis")
        return False
    except subprocess.TimeoutExpired:
        logger.warning("radon timed out after 600s")
        return False

    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError:
        logger.warning("radon produced invalid JSON")
        return False

    update_count = 0
    for filepath, items in data.items():
        # Normalize path to relative
        try:
            rel = str(Path(filepath).relative_to(root)).replace("\\", "/")
        except ValueError:
            continue

        for item in items:
            if not isinstance(item, dict):
                continue
            name = item.get("name", "")
            complexity = item.get("complexity", 0)
            lineno = item.get("lineno", 0)

            # Update the symbol's complexity field
            db.execute("""
                UPDATE symbols SET complexity = ?
                WHERE module_id IN (SELECT id FROM modules WHERE path = ?)
                  AND name = ? AND lineno = ?
            """, (complexity, rel, name, lineno))
            update_count += 1

    db.commit()
    logger.info(f"radon: updated complexity for {update_count} symbols")
    return True


def run_vulture_deadcode(
    db: sqlite3.Connection,
    root: Path,
    dirs: list[str],
    jsonl_file: Optional[TextIO] = None,
) -> bool:
    """Run vulture and emit DEAD_CODE discrepancies. Returns True if vulture was available."""
    try:
        args = ["python", "-m", "vulture", "--min-confidence=80", "--sort-by-size"] + [
            str(root / d) for d in dirs
        ]
        result = subprocess.run(
            args, capture_output=True, text=True, timeout=600, cwd=str(root)
        )
    except FileNotFoundError:
        logger.info("vulture not installed — skipping dead code analysis")
        return False
    except subprocess.TimeoutExpired:
        logger.warning("vulture timed out after 600s")
        return False

    # vulture outputs lines like:
    # src/utils/utilities.py:42: unused function 'some_func' (confidence: 90%)
    count = 0
    for line in result.stdout.splitlines():
        parts = line.split(":")
        if len(parts) < 3:
            continue
        try:
            filepath = parts[0].strip()
            lineno = int(parts[1].strip())
            rest = ":".join(parts[2:]).strip()
        except (ValueError, IndexError):
            continue

        # Parse confidence
        conf_match = None
        if "(confidence:" in rest:
            try:
                conf_str = rest.split("(confidence:")[1].split("%")[0].strip()
                conf_match = int(conf_str)
            except (ValueError, IndexError):
                pass

        # Normalize path
        try:
            rel = str(Path(filepath).relative_to(root)).replace("\\", "/")
        except ValueError:
            rel = filepath.replace("\\", "/")

        persist_discrepancy(
            db, "DEAD_CODE", "info",
            rel, lineno,
            rest.split("(")[0].strip(),
            {
                "vulture_message": rest,
                "confidence": conf_match,
            },
            jsonl_file=jsonl_file,
        )
        count += 1

    logger.info(f"vulture: found {count} dead code items")
    return True


def save_complexity_report(db: sqlite3.Connection, output_path: Path) -> None:
    """Write top complex functions to complexity.json."""
    rows = db.execute("""
        SELECT s.fqn, s.name, s.complexity, s.lineno, s.end_lineno, m.path
        FROM symbols s
        JOIN modules m ON m.id = s.module_id
        WHERE s.complexity IS NOT NULL AND s.complexity > 5
        ORDER BY s.complexity DESC
        LIMIT 100
    """).fetchall()

    records = []
    for fqn, name, complexity, lineno, end_lineno, path in rows:
        rank = (
            "A" if complexity <= 5 else
            "B" if complexity <= 10 else
            "C" if complexity <= 15 else
            "D" if complexity <= 20 else
            "E" if complexity <= 25 else "F"
        )
        records.append({
            "fqn": fqn,
            "name": name,
            "complexity": complexity,
            "rank": rank,
            "lineno": lineno,
            "end_lineno": end_lineno,
            "path": path,
        })

    output_path.write_text(
        json.dumps(records, indent=2), encoding="utf-8"
    )
    logger.info(f"Wrote {len(records)} complex functions to {output_path}")
