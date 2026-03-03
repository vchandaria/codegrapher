# codegrapher

> Code intelligence for any Python project — AST indexing, call graphs, dead code detection, blast radius analysis, and skill freshness validation.

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
# Clone the plugin
git clone https://github.com/yourusername/codegrapher ~/.claude/plugins/codegrapher

# Install dependencies (optional: radon + vulture for richer analysis)
pip install radon vulture
```

## Quick Start

```bash
# 1. Index your project (run from parent dir of codegrapher/, ~5s per 100 files)
python -m codegrapher.core.indexer --clean

# 2. Query the index
python -m codegrapher.core.query_engine callers my_function
python -m codegrapher.core.query_engine impact validate_order --depth 4
python -m codegrapher.core.query_engine dead --module src/legacy
python -m codegrapher.core.query_engine issues --sev error
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
