# /codegrapher-query

Query the code graph. All queries are instant (pre-built index).

## Subcommands

| Subcommand | Description | Example |
|-----------|-------------|---------|
| `callers <name>` | Who calls this function? | `callers process_order` |
| `callees <name>` | What does this function call? | `callees main` |
| `chain <start> <end>` | Shortest call path between two functions | `chain parse_config execute_trade` |
| `dead [--module PATH]` | Dead functions (zero callers) | `dead --module src/utils` |
| `class <name>` | Class hierarchy, parents, children, methods | `class OrderProcessor` |
| `impact <name> [--depth N]` | Blast radius — all transitive callers | `impact validate_schema --depth 4` |
| `deps <module>` | Import graph for a module | `deps src/config` |
| `search <query>` | FTS5 search across all symbols | `search retry exponential` |
| `field <name> [--source DICT]` | Who reads a dict field? | `field stop_pct` |
| `consumes <name>` | What fields does a function consume? | `consumes execute_order` |
| `issues [--sev error\|warn]` | Discrepancies report | `issues --sev error` |
| `dict-writes [--module] [--field]` | Pipeline dict write inventory | `dict-writes --field price` |

## Usage

When the user runs `/codegrapher-query <subcommand> [args]`, execute:

```bash
python -m codegrapher.core.query_engine <subcommand> [args]
```

Add `--json` for machine-readable output, `--limit N` to control result count.

## Examples

```bash
# Find all callers of a function
python -m codegrapher.core.query_engine callers validate_order

# Blast radius analysis
python -m codegrapher.core.query_engine impact load_config --depth 5

# Dead code in a module
python -m codegrapher.core.query_engine dead --module src/legacy

# All issues at error severity
python -m codegrapher.core.query_engine issues --sev error

# Shortest path between two functions
python -m codegrapher.core.query_engine chain parse_args run_server
```

## Notes

- Requires index to exist: run `/codegrapher-index` first
- All queries are read-only (no DB mutations)
- Default DB path: `.codegrapher/code_index.db` in current directory
- Override with `--db /path/to/code_index.db`
