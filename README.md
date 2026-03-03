# codegrapher

> Code intelligence for any Python project — AST indexing, call graphs, dead code detection, blast radius analysis, and skill freshness validation.

## Demo: FastAPI (1118 files indexed in 14.5s)

```
$ python -m codegrapher.core.indexer --project-root fastapi/ --clean

Stage 1 DISCOVER: found 1118 Python files in 0.1s
Stage 2 PARSE:    1118 indexed, 0 skipped, 0 errors in 9.1s
Stage 2.5 CALL_RESOLVE: 3702/15278 resolved in 0.1s
Stage 5 GRAPH:    1382 edges in 0.0s
Stage 6 SYNOPSES: 5241 symbols in 0.4s

Indexing complete in 14.5s
```

**Blast radius — "if I change get_dependant, what breaks?"**
```
$ codegrapher-query impact get_dependant --depth 4

Impact analysis for fastapi.dependencies.utils.get_dependant
  6 functions affected across 2 modules (depth 4)
------------------------------------------------------------
  Depth 1:
    fastapi.dependencies.utils.get_parameterless_sub_dependant  (dependencies/utils.py:121)
    fastapi.dependencies.utils.solve_dependencies               (dependencies/utils.py:595)
    fastapi.routing.APIRoute.__init__                           (routing.py:808)
    fastapi.routing.APIWebSocketRoute.__init__                  (routing.py:766)
  Depth 2:
    fastapi.routing.get_request_handler.app                     (routing.py:378)
    fastapi.routing.get_websocket_app.app                       (routing.py:733)
```

**Dead code — public API methods with zero internal callers**
```
$ codegrapher-query dead --module fastapi

Dead functions in fastapi (30 found)
  fastapi.applications.FastAPI.build_middleware_stack  (applications.py:1021)
  fastapi.applications.FastAPI.openapi                 (applications.py:1069)
  fastapi.applications.FastAPI.__call__                (applications.py:1157)
  fastapi._compat.v2.ModelField.validate               (_compat/v2.py:160)
  fastapi._compat.v2.ModelField.serialize              (_compat/v2.py:177)
  ... 25 more
```

**Call graph — who calls solve_dependencies?**
```
$ codegrapher-query callers solve_dependencies

Callers of fastapi.dependencies.utils.solve_dependencies (3 found)
  [0.98] fastapi.dependencies.utils.solve_dependencies  (utils.py:595)  ← recursive
  [0.80] fastapi.routing.get_request_handler.app         (routing.py:378)
  [0.80] fastapi.routing.get_websocket_app.app           (routing.py:733)
```

## What it does

Install codegrapher on any Python project and instantly get:

- **Call graph queries** — who calls this function? what does it call?
- **Blast radius analysis** — if I change this, what breaks?
- **Dead code detection** — which functions have zero callers?
- **Import dependency graph** — what does this module depend on?
- **Field/dict tracking** — who reads `stop_pct`? who writes to `trade_setup`?
- **Discrepancy detection** — signature mismatches, unresolved calls, drift
- **Skill freshness validation** — auto-detect stale facts in Claude Code skill files

## Installation

```bash
pip install git+https://github.com/vchandaria/codegrapher.git

# Optional: richer analysis (complexity + dead code via external tools)
pip install "git+https://github.com/vchandaria/codegrapher.git[all]"
```

## Quick Start

```bash
# 1. Index your project (~14s for 1000 files)
codegrapher-index --clean

# 2. Query the index
codegrapher-query callers my_function
codegrapher-query impact validate_order --depth 4
codegrapher-query dead --module src/legacy
codegrapher-query issues --sev error
```

## Slash Commands (Claude Code)

| Command | Description |
|---------|-------------|
| `/codegrapher-index` | Build or rebuild the code index |
| `/codegrapher-query <subcommand>` | Query the code graph |
| `/codegrapher-validate` | Check skills for stale facts |

## Query Reference

| Subcommand | Description | Example |
|-----------|-------------|---------|
| `callers <name>` | Who calls this function? | `callers process_order` |
| `callees <name>` | What does this function call? | `callees main` |
| `chain <start> <end>` | Shortest call path | `chain parse_config run_server` |
| `dead [--module PATH]` | Dead functions | `dead --module src/utils` |
| `class <name>` | Class hierarchy | `class OrderProcessor` |
| `impact <name> [--depth N]` | Blast radius | `impact load_config --depth 5` |
| `deps <module>` | Import dependencies | `deps src/config` |
| `search <query>` | FTS5 search | `search retry exponential` |
| `field <name>` | Who reads this field? | `field stop_pct` |
| `consumes <name>` | What fields does this consume? | `consumes execute_trade` |
| `issues [--sev]` | Discrepancies report | `issues --sev error` |
| `dict-writes` | Pipeline write inventory | `dict-writes --field price` |

All commands accept `--json` for machine-readable output and `--limit N` for result count.

## Configuration

Create `codegrapher.yaml` in your project root to customize behaviour:

```yaml
# codegrapher.yaml
db_path: .codegrapher/code_index.db

# Roots treated as "internal" (not external packages)
internal_roots:
  - src.
  - scripts.
  - tools.
  - apps.

# JSON config files to index (relative to project root)
config_files:
  - config/settings.json
  - data/schema.json

# Named dicts inside those JSON files to use as registries
registry_names:
  - MY_CONFIG_DICT

# Custom title for repo_skeleton.txt
skeleton_title: "MyProject Codebase Skeleton"
```

All settings can also be set via environment variables (prefix `CODEGRAPHER_`).

## Output Files

All output goes to `.codegrapher/` in the project root:

| File | Description |
|------|-------------|
| `code_index.db` | Master SQLite database |
| `code_index.jsonl` | All records as JSONL |
| `discrepancies.jsonl` | Detected issues only |
| `complexity.json` | Top complex functions (requires radon) |
| `repo_skeleton.txt` | Aider-style signatures map (~50K tokens) |

## Requirements

- Python 3.11+
- No required dependencies (standard library only for core)
- Optional: `pip install radon vulture` for complexity + dead code
- Optional: `pip install pyyaml` for `codegrapher.yaml` config file support

## License

MIT
