# /codegrapher-index

Build or rebuild the code intelligence index for the current project.

## What it does

Runs the codegrapher indexer on the current working directory, producing:
- `.codegrapher/code_index.db` — SQLite database (symbols, calls, imports, graph)
- `.codegrapher/repo_skeleton.txt` — Aider-style signatures map
- `.codegrapher/complexity.json` — top complex functions (if radon installed)

## Usage

When the user runs `/codegrapher-index`, execute:

```bash
python -m codegrapher.core.indexer --project-root .
```

Common flags:
- `--clean` — delete existing DB and rebuild from scratch
- `--incremental` — skip unchanged files (faster for large projects)
- `--no-external` — skip radon/vulture (faster, skips complexity + dead code)
- `--verbose` — show detailed progress

## Expected output

```
Indexing complete in 12.3s
  Phase 1: 247 modules indexed
  Calls resolved: 8,432
  Graph edges: 21,891
  Database: .codegrapher/code_index.db
```

## When to run

- First time setting up codegrapher on a project
- After significant code changes (new files, major refactors)
- When queries return unexpected "not found" results

## Notes

- Index time: ~5s per 100 Python files
- Requires Python 3.11+
- Optional: `pip install radon vulture` for complexity + dead code detection
