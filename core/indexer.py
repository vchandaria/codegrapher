#!/usr/bin/env python3
"""codegrapher — Generic Code Indexer.

Builds a queryable SQLite database of any Python project.
Phase 1: Structural (symbols, imports, calls, registries, constants)
Phase 2: Semantic (call resolution, JSON configs, external tools)
Phase 3: Control plane (graph edges, synopses, skeleton)

Usage:
    python -m codegrapher.core.indexer                         # index current directory
    python -m codegrapher.core.indexer --project-root /path/to/project
    python -m codegrapher.core.indexer --db-path .codegrapher/index.db
    python -m codegrapher.core.indexer --clean                 # delete DB + rebuild
    python -m codegrapher.core.indexer --incremental           # skip unchanged files
    python -m codegrapher.core.indexer --no-external           # skip radon/vulture
    python -m codegrapher.core.indexer --phase3-only           # graph + synopses only

Output (relative to --project-root):
    .codegrapher/code_index.db       — master SQLite database
    .codegrapher/code_index.jsonl    — all records as JSONL
    .codegrapher/discrepancies.jsonl — detected issues
    .codegrapher/complexity.json     — top complex functions (if radon installed)
    .codegrapher/repo_skeleton.txt   — Aider-style signatures map
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

logger = logging.getLogger("codegrapher.indexer")

# Directories always excluded from indexing
_SKIP_DIRS = frozenset({
    "venv", "venv_311", ".venv", "__pycache__", "build", "dist", ".git",
    "node_modules", ".mypy_cache", ".pytest_cache", ".tox", ".eggs",
    "build_temp", ".codegrapher",
})


def discover_files(root: Path, extra_skip: list[str] | None = None) -> list[Path]:
    """Find all Python files under root, skipping excluded directories."""
    skip = _SKIP_DIRS | frozenset(extra_skip or [])
    files: list[Path] = []
    for py_file in root.rglob("*.py"):
        parts = py_file.relative_to(root).parts
        if any(p in skip for p in parts):
            continue
        files.append(py_file)
    return sorted(set(files))


def run_index(
    project_root: Path,
    db_path: Path,
    incremental: bool = False,
    no_external: bool = False,
    skip_phase1: bool = False,
    skip_phase2: bool = False,
    verbose: bool = False,
) -> dict:
    """Run the full indexing pipeline. Returns stats dict."""
    from .schema import create_database, rebuild_fts
    from .ast_walker import analyze_file
    from .persistence import persist_module
    from .call_resolver import resolve_all_calls
    from .external_tools import run_radon_complexity, run_vulture_deadcode, save_complexity_report
    from .skeleton_builder import build_skeleton
    from .json_config_indexer import index_all_json_configs
    from .artifact_io import extract_artifact_io, check_artifact_orphans

    output_dir = db_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    jsonl_path = output_dir / "code_index.jsonl"
    disc_path = output_dir / "discrepancies.jsonl"

    stats: dict = {}
    t_start = time.time()

    db = create_database(db_path)

    # ══════════════════════════════════════════════════════════
    # PHASE 1: Structural Index
    # ══════════════════════════════════════════════════════════
    if not skip_phase1:
        t0 = time.time()
        files = discover_files(project_root)
        logger.info("Stage 1 DISCOVER: found %d Python files in %.1fs", len(files), time.time() - t0)

        t1 = time.time()
        jsonl_file = open(jsonl_path, "w", encoding="utf-8")
        indexed = skipped = errors = 0

        for i, filepath in enumerate(files):
            rec = analyze_file(filepath, project_root)
            result = persist_module(db, rec, jsonl_file, incremental=incremental)
            if result is None:
                skipped += 1
            elif rec.parse_error:
                errors += 1
                logger.debug("Parse error %s: %s", rec.path, rec.parse_error)
            else:
                indexed += 1

            if (i + 1) % 100 == 0:
                db.commit()
                logger.info("  ... %d/%d files processed", i + 1, len(files))

        db.commit()
        jsonl_file.close()
        rebuild_fts(db)

        stats["phase1_indexed"] = indexed
        stats["phase1_skipped"] = skipped
        stats["phase1_errors"] = errors
        logger.info(
            "Stage 2 PARSE: %d indexed, %d skipped, %d errors in %.1fs",
            indexed, skipped, errors, time.time() - t1,
        )

    # ══════════════════════════════════════════════════════════
    # PHASE 2: Semantic Index
    # ══════════════════════════════════════════════════════════
    if not skip_phase2:
        # Call resolution
        t25 = time.time()
        resolve_stats = resolve_all_calls(db, root=project_root)
        db.commit()
        logger.info(
            "Stage 2.5 CALL_RESOLVE: %d/%d resolved in %.1fs",
            resolve_stats.get("resolved", 0), resolve_stats.get("total", 0),
            time.time() - t25,
        )
        stats["call_resolve"] = resolve_stats

        # External tools (radon, vulture)
        if not skip_phase1 and not no_external:
            t3 = time.time()
            scan_dirs = [str(d) for d in project_root.iterdir() if d.is_dir() and d.name not in _SKIP_DIRS]
            radon_ok = run_radon_complexity(db, project_root, scan_dirs)
            if radon_ok:
                save_complexity_report(db, output_dir / "complexity.json")
            vulture_ok = run_vulture_deadcode(db, project_root, scan_dirs)
            logger.info(
                "Stage 3 EXTERNAL: radon=%s vulture=%s in %.1fs",
                "ok" if radon_ok else "skip", "ok" if vulture_ok else "skip",
                time.time() - t3,
            )
            stats["radon"] = radon_ok
            stats["vulture"] = vulture_ok

        # JSON config indexing (uses CONFIG_FILES from config.py)
        t45 = time.time()
        json_count = index_all_json_configs(project_root, db)
        db.commit()
        logger.info("Stage 4.5 JSON_CONFIGS: %d entries in %.1fs", json_count, time.time() - t45)
        stats["json_configs"] = json_count

        # Artifact I/O mapping
        t_aio = time.time()
        try:
            aio_count = extract_artifact_io(project_root, db)
            db.commit()
            logger.info("Stage 4.9 ARTIFACT_IO: %d records in %.1fs", aio_count, time.time() - t_aio)
            stats["artifact_io"] = aio_count
        except Exception as exc:
            logger.warning("Artifact I/O extraction skipped: %s", exc)

    # ══════════════════════════════════════════════════════════
    # PHASE 3: Control Plane
    # ══════════════════════════════════════════════════════════
    try:
        from ..control_plane.graph import build_graph_edges
        from ..control_plane.synopses import build_all_synopses

        t_graph = time.time()
        edge_count = build_graph_edges(db)
        db.commit()
        logger.info("Stage 5 GRAPH: %d edges in %.1fs", edge_count, time.time() - t_graph)
        stats["graph_edges"] = edge_count

        t_syn = time.time()
        syn_count = build_all_synopses(db)
        db.commit()
        logger.info("Stage 6 SYNOPSES: %d in %.1fs", syn_count, time.time() - t_syn)
        stats["synopses"] = syn_count

        skeleton_path = db_path.parent / "repo_skeleton.txt"
        build_skeleton(db, skeleton_path)
        logger.info("Stage 7 SKELETON: written to %s", skeleton_path)

    except Exception as exc:
        logger.warning("Phase 3 partially failed: %s", exc)

    db.close()
    stats["total_seconds"] = round(time.time() - t_start, 1)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="codegrapher — Index any Python project for AI-assisted development",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--project-root", type=Path, default=Path.cwd(),
        help="Root directory of the project to index (default: cwd)",
    )
    parser.add_argument(
        "--db-path", type=Path, default=None,
        help="Path to output SQLite DB (default: <project-root>/.codegrapher/code_index.db)",
    )
    parser.add_argument("--clean", action="store_true", help="Delete existing DB and rebuild")
    parser.add_argument("--incremental", action="store_true", help="Skip unchanged files")
    parser.add_argument("--no-external", action="store_true", help="Skip radon/vulture")
    parser.add_argument("--phase3-only", action="store_true", help="Run Phase 3 (graph/synopses) only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    project_root = args.project_root.resolve()
    db_path = args.db_path or (project_root / ".codegrapher" / "code_index.db")

    if args.clean and db_path.exists():
        db_path.unlink()
        logger.info("Deleted existing database at %s", db_path)

    stats = run_index(
        project_root=project_root,
        db_path=db_path,
        incremental=args.incremental,
        no_external=args.no_external,
        skip_phase1=args.phase3_only,
        skip_phase2=args.phase3_only,
        verbose=args.verbose,
    )

    print(f"\nIndexing complete in {stats['total_seconds']}s")
    print(f"  Phase 1: {stats.get('phase1_indexed', 'skipped')} modules indexed")
    print(f"  Calls resolved: {stats.get('call_resolve', {}).get('resolved', 'skipped')}")
    print(f"  Graph edges: {stats.get('graph_edges', 'skipped')}")
    print(f"  Database: {db_path}")


if __name__ == "__main__":
    main()
