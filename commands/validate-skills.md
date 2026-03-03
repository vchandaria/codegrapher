# /codegrapher-validate

Check all Claude Code skills for stale facts (version numbers, file paths, LOC counts, formation counts, etc.).

## What it does

Scans all skill `.md` files in `.claude/skills/`, `~/.claude/skills/`, and `~/.ai/skills/` for hard-coded facts that may have drifted from the actual codebase state.

Detects drift in:
- Version numbers (e.g., "V182.0" in skill text vs actual code)
- File LOC counts (e.g., "engine.py: 485 lines")
- Symbol counts (e.g., "61 active formations")
- File paths that no longer exist
- Function names that were renamed or deleted

## Usage

When the user runs `/codegrapher-validate`, execute:

```bash
python -m codegrapher.core.skill_validator
```

With auto-fix:
```bash
python -m codegrapher.core.skill_validator --fix
```

Check a specific skill:
```bash
python -m codegrapher.core.skill_validator --skill rasmus-core-specialist
```

## Expected output

```
Scanning 8 skill files...

STALE FACTS FOUND:
  rasmus-core-specialist/SKILL.md
    Line 12: "61 active formations" → actual: 65
    Line 18: "engine.py: 485L" → actual: 512L

  ai-system-evolution/SKILL.md
    Line 8: "V181.0" → actual: V182.0

Run with --fix to auto-correct simple drift.
```

## Notes

- Non-destructive by default (read-only scan)
- `--fix` only patches simple numeric drift (versions, LOC, counts)
- Complex rewrites are flagged but require manual review
