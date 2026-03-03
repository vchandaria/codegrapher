#!/usr/bin/env python
"""Skill Lifecycle Validator — detects and auto-fixes stale facts in skill .md files.

Usage:
    python tools/skill_validator.py              # check all skills
    python tools/skill_validator.py --fix        # auto-fix simple drift
    python tools/skill_validator.py --skill NAME # check one skill
    python tools/skill_validator.py --json       # JSON output
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Project root detection
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent  # e.g. tools/ -> project root
HOME = Path.home()

SKILL_DIRS = [
    PROJECT_ROOT / ".claude" / "skills",
    HOME / ".claude" / "skills",
    HOME / ".ai" / "skills",
]

# ---------------------------------------------------------------------------
# Source resolvers
# ---------------------------------------------------------------------------

def resolve_json_count(rel_path: str) -> int:
    """json_count:path -> len(json.load(file)) for top-level list/dict."""
    p = PROJECT_ROOT / rel_path
    if not p.exists():
        raise FileNotFoundError(f"json_count: {p}")
    with open(p, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        return len(data)
    raise ValueError(f"json_count: unexpected type {type(data).__name__}")


def resolve_glob_count(pattern_spec: str) -> int:
    """glob_count:pattern1+pattern2 -> total file count across patterns."""
    total = 0
    for pat in pattern_spec.split("+"):
        pat = pat.strip()
        total += len(list(PROJECT_ROOT.glob(pat)))
    return total


def resolve_loc(rel_path: str) -> int:
    """loc:path -> line count of a file."""
    p = PROJECT_ROOT / rel_path
    if not p.exists():
        raise FileNotFoundError(f"loc: {p}")
    return sum(1 for _ in open(p, encoding="utf-8"))


def resolve_status_version(rel_path: str) -> str:
    """status_version:path -> first V\\d+.\\d+ match from file."""
    p = PROJECT_ROOT / rel_path
    if not p.exists():
        raise FileNotFoundError(f"status_version: {p}")
    text = p.read_text(encoding="utf-8")
    m = re.search(r"V(\d+\.\d+)", text)
    if not m:
        raise ValueError(f"status_version: no version found in {p}")
    return m.group(1)


def resolve_file_exists(rel_path: str) -> bool:
    """file_exists:path -> boolean."""
    return (PROJECT_ROOT / rel_path).exists()


RESOLVERS = {
    "json_count": resolve_json_count,
    "glob_count": resolve_glob_count,
    "loc": resolve_loc,
    "status_version": resolve_status_version,
    "file_exists": resolve_file_exists,
}


def resolve_source(source: str):
    """Dispatch 'resolver_type:arg' to the right resolver."""
    kind, _, arg = source.partition(":")
    if kind not in RESOLVERS:
        raise ValueError(f"Unknown resolver: {kind}")
    return RESOLVERS[kind](arg)


# ---------------------------------------------------------------------------
# Skill discovery & YAML parsing
# ---------------------------------------------------------------------------

def find_all_skills() -> list[Path]:
    """Scan skill directories for .md files with YAML frontmatter."""
    found = []
    for d in SKILL_DIRS:
        if not d.exists():
            continue
        for md in sorted(d.rglob("*.md")):
            text = md.read_text(encoding="utf-8", errors="replace")
            if text.startswith("---"):
                found.append(md)
    return found


def parse_skill_frontmatter(path: Path) -> dict:
    """Extract YAML-like frontmatter between --- delimiters.

    Handles the verifiable_facts list-of-dicts structure manually
    to avoid requiring PyYAML.
    """
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---"):
        return {}

    end = text.find("---", 3)
    if end == -1:
        return {}

    block = text[3:end].strip()
    result = {}
    current_fact = None
    facts = []
    in_facts = False

    for line in block.split("\n"):
        stripped = line.strip()

        # Detect verifiable_facts list start
        if stripped == "verifiable_facts:":
            in_facts = True
            continue

        if in_facts:
            # New list item
            if stripped.startswith("- key:"):
                if current_fact:
                    facts.append(current_fact)
                current_fact = {"key": stripped.split(":", 1)[1].strip()}
                continue
            # Continuation of current fact
            if current_fact and ":" in stripped:
                k, v = stripped.split(":", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k in ("pattern", "source"):
                    current_fact[k] = v
                    continue
            # If we hit a non-indented, non-list line, facts section is over
            if not line.startswith(" ") and not line.startswith("\t") and stripped:
                in_facts = False
                if current_fact:
                    facts.append(current_fact)
                    current_fact = None
        # Regular key: value
        if not in_facts and ":" in stripped and not stripped.startswith("-"):
            k, v = stripped.split(":", 1)
            result[k.strip()] = v.strip().strip("'\"")

    if current_fact:
        facts.append(current_fact)
    if facts:
        result["verifiable_facts"] = facts

    return result


# ---------------------------------------------------------------------------
# Fact checking
# ---------------------------------------------------------------------------

LOC_TOLERANCE = 0.10  # 10% tolerance for ~ prefixed LOC claims


def check_fact(fact: dict, skill_text: str) -> dict:
    """Check a single verifiable fact against ground truth.

    Returns: {key, claimed, actual, match, severity, auto_fixable, pattern}
    """
    key = fact["key"]
    pattern = fact["pattern"]
    source = fact["source"]

    result = {
        "key": key,
        "claimed": None,
        "actual": None,
        "match": False,
        "severity": "info",
        "auto_fixable": False,
        "pattern": pattern,
    }

    # Extract claimed value from skill text
    m = re.search(pattern, skill_text)
    if not m:
        result["severity"] = "warn"
        result["claimed"] = "(pattern not found)"
        return result

    claimed_str = m.group(1)
    result["claimed"] = claimed_str

    # Resolve actual value
    try:
        actual = resolve_source(source)
    except Exception as e:
        result["actual"] = f"ERROR: {e}"
        result["severity"] = "error"
        return result

    result["actual"] = actual

    # Compare
    resolver_kind = source.split(":")[0]

    if resolver_kind == "file_exists":
        result["match"] = actual is True
        result["severity"] = "error" if not actual else "info"
        return result

    if resolver_kind == "status_version":
        result["match"] = str(claimed_str) == str(actual)
        result["auto_fixable"] = True
        result["severity"] = "drift" if not result["match"] else "info"
        return result

    if resolver_kind == "loc":
        claimed_num = int(claimed_str)
        actual_num = int(actual)
        # With ~ prefix tolerance
        has_tilde = "~" in pattern or "~" in skill_text[max(0, m.start() - 5):m.start()]
        if has_tilde:
            result["match"] = abs(actual_num - claimed_num) / max(claimed_num, 1) <= LOC_TOLERANCE
        else:
            result["match"] = claimed_num == actual_num
        result["auto_fixable"] = True
        result["severity"] = "drift" if not result["match"] else "info"
        return result

    # Numeric comparisons (json_count, glob_count)
    try:
        claimed_num = int(claimed_str)
        result["match"] = claimed_num == int(actual)
        result["auto_fixable"] = True
        result["severity"] = "drift" if not result["match"] else "info"
    except (ValueError, TypeError):
        result["match"] = str(claimed_str) == str(actual)
        result["severity"] = "drift" if not result["match"] else "info"

    return result


# ---------------------------------------------------------------------------
# Skill validation & auto-fix
# ---------------------------------------------------------------------------

def validate_skill(path: Path) -> dict:
    """Validate all verifiable_facts in a skill file."""
    fm = parse_skill_frontmatter(path)
    text = path.read_text(encoding="utf-8")
    facts = fm.get("verifiable_facts", [])
    last_verified = fm.get("last_verified", "never")

    results = []
    for fact in facts:
        if "pattern" in fact and "source" in fact:
            results.append(check_fact(fact, text))

    drift_count = sum(1 for r in results if not r["match"])
    auto_fixable = sum(1 for r in results if not r["match"] and r["auto_fixable"])

    return {
        "skill": fm.get("name", path.stem),
        "path": str(path),
        "last_verified": last_verified,
        "facts": results,
        "drift_count": drift_count,
        "auto_fixable_count": auto_fixable,
    }


def auto_fix_skill(path: Path, results: dict) -> int:
    """Apply auto-fixes for drifted facts. Returns count of fixes applied."""
    text = path.read_text(encoding="utf-8")
    fix_count = 0

    for fact in results["facts"]:
        if fact["match"] or not fact["auto_fixable"]:
            continue
        if fact["actual"] is None or str(fact["actual"]).startswith("ERROR"):
            continue

        pattern = fact["pattern"]
        actual = fact["actual"]

        # Replace the captured group value in all matches
        def replacer(m):
            full = m.group(0)
            old_val = m.group(1)
            return full.replace(old_val, str(actual), 1)

        new_text = re.sub(pattern, replacer, text)
        if new_text != text:
            text = new_text
            fix_count += 1

    # Update last_verified date
    today = datetime.now().strftime("%Y-%m-%d")
    if "last_verified:" in text:
        text = re.sub(
            r'last_verified:\s*"?\d{4}-\d{2}-\d{2}"?',
            f'last_verified: "{today}"',
            text,
        )
    elif text.startswith("---"):
        # Insert last_verified after opening ---
        text = text.replace("---\n", f"---\nlast_verified: \"{today}\"\n", 1)

    path.write_text(text, encoding="utf-8")
    return fix_count


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display_results(all_results: list[dict], as_json: bool = False):
    """Print validation results."""
    if as_json:
        # Convert Path objects for JSON serialization
        print(json.dumps(all_results, indent=2, default=str))
        return

    total_skills = len(all_results)
    total_drift = sum(r["drift_count"] for r in all_results)
    total_fixable = sum(r["auto_fixable_count"] for r in all_results)

    for r in all_results:
        rel = r["path"]
        # Shorten path for display
        if HOME.as_posix() in rel:
            rel = rel.replace(HOME.as_posix(), "~")
        elif str(HOME) in rel:
            rel = rel.replace(str(HOME), "~")

        print(f"\n{r['skill']} ({rel})")
        print(f"  Last verified: {r['last_verified']}")

        if not r["facts"]:
            print("  (no verifiable facts)")
            continue

        for f in r["facts"]:
            if f["match"]:
                print(f"  [OK]    {f['key']}: {f['claimed']}")
            else:
                tag = "auto-fixable" if f["auto_fixable"] else "manual"
                print(f"  [DRIFT] {f['key']}: claimed {f['claimed']}, actual {f['actual']} ({tag})")

    print(f"\nSummary: {total_skills} skills, {total_drift} drifted ({total_fixable} auto-fixable).", end="")
    if total_fixable > 0:
        print(" Run --fix to apply.")
    else:
        print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Validate skill freshness")
    parser.add_argument("--fix", action="store_true", help="Auto-fix simple drift")
    parser.add_argument("--skill", type=str, help="Check only this skill (name match)")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    skills = find_all_skills()
    if not skills:
        print("No skills found.")
        sys.exit(1)

    # Filter by name if requested
    if args.skill:
        needle = args.skill.lower()
        skills = [s for s in skills if needle in s.stem.lower() or needle in s.parent.name.lower()]
        if not skills:
            print(f"No skill matching '{args.skill}' found.")
            sys.exit(1)

    all_results = []
    for path in skills:
        result = validate_skill(path)
        all_results.append(result)

        if args.fix and result["auto_fixable_count"] > 0:
            fixes = auto_fix_skill(path, result)
            if fixes and not args.json:
                print(f"  Fixed {fixes} fact(s) in {path.name}")

    # If we fixed, re-validate to show clean state
    if args.fix:
        all_results = [validate_skill(p) for p in skills]

    display_results(all_results, as_json=args.json)

    # Exit code: 1 if any drift remains
    total_drift = sum(r["drift_count"] for r in all_results)
    sys.exit(1 if total_drift > 0 else 0)


if __name__ == "__main__":
    main()
