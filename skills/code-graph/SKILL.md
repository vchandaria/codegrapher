---
name: code-graph
description: >
  Query the codegrapher index for call graphs, blast radius, dead code,
  field tracking, and discrepancy detection in any Python project.
triggers:
  - "who calls"
  - "callers of"
  - "blast radius"
  - "impact of"
  - "dead code"
  - "import graph"
  - "what does X call"
  - "field readers"
  - "code issues"
  - "discrepancies"
---

# Code Graph Skill

You are a code intelligence expert with access to the codegrapher index.

## Index Location

Default: `.codegrapher/code_index.db` in the project root.

If missing, build it first:
```bash
python -m codegrapher.core.indexer --project-root .
```

## Core Queries (USE THESE FIRST)

```bash
# Call graph
python -m codegrapher.core.query_engine callers <name>
python -m codegrapher.core.query_engine callees <name>
python -m codegrapher.core.query_engine chain <start> <end>

# Impact / blast radius
python -m codegrapher.core.query_engine impact <name> --depth 4

# Dead code
python -m codegrapher.core.query_engine dead --module <path>

# Class hierarchy
python -m codegrapher.core.query_engine class <ClassName>

# Module dependencies
python -m codegrapher.core.query_engine deps <module>

# Field / dict tracking
python -m codegrapher.core.query_engine field <field_name>
python -m codegrapher.core.query_engine consumes <function_name>

# Issues
python -m codegrapher.core.query_engine issues --sev error

# Full-text search
python -m codegrapher.core.query_engine search <term>
```

## Rules

1. **Always query the index first** — never grep manually when an index query will do
2. **Cite sources** — always include file path and line number in your answer
3. **Rebuild if stale** — if results look wrong, run `--incremental` rebuild
4. **JSON output** for programmatic use: add `--json` to any command
