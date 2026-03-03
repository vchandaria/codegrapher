"""Context pack assembly from templates.

Builds compact, token-efficient context packs by combining:
- Proof failures (always first)
- Ranked symbols with synopses
- Mini-traces for call chains
- Discrepancies filtered by relevance
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path

from .ranking import rank_symbols, dedupe_and_cluster

logger = logging.getLogger(__name__)

# Default output dir for pack templates
_INDEX_DIR = Path(__file__).resolve().parent.parent.parent / "docs" / "codebase_index"


def build_pack(
    db: sqlite3.Connection,
    template_name: str,
    seeds: list[str] | None = None,
    intent_class: str | None = None,
    max_tokens: int = 2000,
    query_library: dict | None = None,
) -> dict:
    """Assemble a context pack using a template.

    Args:
        template_name: Name from pack_templates.json
        seeds: Optional seed FQNs for scoping
        intent_class: Override template's default intent class
        max_tokens: Maximum token budget
        query_library: Pre-loaded query library (or loads from disk)

    Returns:
        {
            template: str,
            sections: [{title, content, token_count}],
            total_tokens: int,
            symbols_included: [fqns],
            proof_failures_included: [rule_names]
        }
    """
    template = _load_template(template_name)
    if not template:
        return {"template": template_name, "sections": [], "total_tokens": 0,
                "symbols_included": [], "proof_failures_included": [],
                "error": f"Template '{template_name}' not found"}

    if query_library is None:
        query_library = _load_query_library()

    effective_intent = intent_class or template_name
    template_max = template.get("max_tokens", max_tokens)
    budget = min(max_tokens, template_max)

    sections: list[dict] = []
    total_tokens = 0
    symbols_included: list[str] = []
    proof_failures: list[str] = []

    # Always include proof failures first
    pf_section = _build_proof_failures_section(db)
    if pf_section["content"]:
        pf_tokens = estimate_tokens(pf_section["content"])
        sections.append({"title": "Proof Failures", "content": pf_section["content"], "token_count": pf_tokens})
        total_tokens += pf_tokens
        proof_failures = pf_section.get("rule_names", [])

    # Process template sections
    for section_spec in template.get("sections", []):
        if total_tokens >= budget:
            break

        title = section_spec.get("title", "")
        remaining = budget - total_tokens

        if section_spec.get("query"):
            # Query-based section
            content = _run_query_section(db, section_spec, query_library, seeds, remaining)
        elif "depth" in section_spec:
            # Subgraph section
            content = _build_subgraph_section(db, seeds, section_spec.get("depth", 1),
                                              section_spec.get("max_nodes", 20))
        elif title == "Semantic Search":
            # Semantic search section — uses V4 retrieval pipeline
            query = section_spec.get("query_text") or (
                " ".join(seeds) if seeds else effective_intent
            )
            content, syms = _build_semantic_section(db, query,
                                                     section_spec.get("max_items", 10),
                                                     effective_intent, remaining)
            symbols_included.extend(syms)
        elif title == "Symbol Synopses":
            # Synopsis section
            content, syms = _build_synopsis_section(db, seeds, effective_intent,
                                                     section_spec.get("max_items", 10), remaining)
            symbols_included.extend(syms)
        elif title == "Mini Traces":
            content = _build_mini_trace_section(db, seeds, effective_intent,
                                                 section_spec.get("max_items", 5))
        else:
            content = ""

        if content:
            tok = estimate_tokens(content)
            if total_tokens + tok <= budget:
                sections.append({"title": title, "content": content, "token_count": tok})
                total_tokens += tok

    return {
        "template": template_name,
        "sections": sections,
        "total_tokens": total_tokens,
        "symbols_included": symbols_included,
        "proof_failures_included": proof_failures,
    }


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~3.5 chars per token for code/mixed content."""
    return max(1, int(len(text) / 3.5))


def _load_template(name: str) -> dict | None:
    """Load a pack template from pack_templates.json."""
    path = _INDEX_DIR / "pack_templates.json"
    if not path.exists():
        return None
    try:
        templates = json.loads(path.read_text(encoding="utf-8"))
        return templates.get(name)
    except (json.JSONDecodeError, OSError):
        return None


def _load_query_library() -> dict:
    """Load query library from query_library.json."""
    path = _INDEX_DIR / "query_library.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _build_proof_failures_section(db: sqlite3.Connection) -> dict:
    """Build proof failures section content."""
    rows = db.execute(
        """SELECT rule_name, details_json FROM proof_results
           WHERE run_id = (SELECT MAX(run_id) FROM proof_results) AND status = 'fail'"""
    ).fetchall()

    if not rows:
        return {"content": "", "rule_names": []}

    lines = ["## Proof Failures\n"]
    rule_names = []
    for rule_name, details_json in rows:
        rule_names.append(rule_name)
        lines.append(f"- **{rule_name}**: FAIL")
        if details_json:
            try:
                details = json.loads(details_json)
                count = details.get("count", 0)
                if count:
                    lines[-1] += f" ({count} violations)"
                violations = details.get("violations", [])
                for v in violations[:3]:  # Show at most 3
                    if isinstance(v, dict):
                        lines.append(f"  - {_format_violation(v)}")
            except (json.JSONDecodeError, TypeError):
                pass

    return {"content": "\n".join(lines), "rule_names": rule_names}


def _format_violation(v: dict) -> str:
    """Format a single violation dict into a readable line."""
    parts = []
    for key in ("formation", "constant", "rule", "subject", "file"):
        if key in v:
            parts.append(f"{key}={v[key]}")
    if "line" in v:
        parts.append(f"L{v['line']}")
    return ", ".join(parts) if parts else str(v)


def _run_query_section(
    db: sqlite3.Connection,
    section_spec: dict,
    query_library: dict,
    seeds: list[str] | None,
    max_tokens: int,
) -> str:
    """Execute a named query and format results."""
    query_name = section_spec["query"]
    query_def = query_library.get(query_name)
    if not query_def:
        return ""

    sql = query_def.get("sql", "")
    if not sql:
        return ""

    max_items = section_spec.get("max_items", 20)

    # Substitute parameters
    params: dict = {}
    if seeds and ":formation" in sql:
        params["formation"] = seeds[0] if seeds else ""
    if seeds and ":symbol_id" in sql:
        # Resolve first seed to symbol_id
        row = db.execute("SELECT id FROM symbols WHERE fqn = ?", (seeds[0],)).fetchone()
        params["symbol_id"] = row[0] if row else 0
    if ":rule_name" in sql:
        params["rule_name"] = seeds[0] if seeds else ""
    if ":limit" in sql:
        params["limit"] = max_items
    if ":canonical_prefix" in sql:
        params["canonical_prefix"] = seeds[0] if seeds else ""
    if ":canonical_path" in sql:
        params["canonical_path"] = seeds[1] if seeds and len(seeds) > 1 else ""

    try:
        # Use named params
        rows = db.execute(sql, params).fetchall()
    except sqlite3.Error:
        # Try positional fallback
        try:
            rows = db.execute(sql.replace(":limit", str(max_items))).fetchall()
        except sqlite3.Error as e:
            return f"<!-- Query error: {e} -->"

    if not rows:
        return ""

    # Format as compact table
    lines = []
    col_names = [desc[0] for desc in db.execute(sql, params).description] if rows else []
    for row in rows[:max_items]:
        parts = []
        for i, val in enumerate(row):
            col = col_names[i] if i < len(col_names) else f"col{i}"
            if val is not None:
                val_str = str(val)
                if len(val_str) > 80:
                    val_str = val_str[:77] + "..."
                parts.append(f"{col}={val_str}")
        lines.append("  " + ", ".join(parts))

    return "\n".join(lines)


def _build_subgraph_section(
    db: sqlite3.Connection,
    seeds: list[str] | None,
    depth: int,
    max_nodes: int,
) -> str:
    """Build a subgraph section showing call relationships."""
    if not seeds:
        return ""

    from .graph import get_subgraph_bfs
    subgraph = get_subgraph_bfs(db, seeds, depth=depth)

    nodes = subgraph.get("nodes", [])[:max_nodes]
    edges = subgraph.get("edges", [])

    if not nodes:
        return ""

    # Build adjacency text
    node_map = {n["id"]: n for n in nodes}
    lines = [f"Subgraph: {len(nodes)} nodes, {len(edges)} edges\n"]

    for node in nodes:
        synopsis = node.get("synopsis", "")
        if synopsis:
            synopsis = f" -- {synopsis[:60]}"
        lines.append(f"  [{node['kind'][0].upper()}] {node['fqn']}{synopsis}")

    # Show edges compactly
    if edges:
        lines.append("")
        for e in edges[:30]:
            from_node = node_map.get(e["from_id"], {})
            to_node = node_map.get(e["to_id"], {})
            if from_node and to_node:
                from_short = from_node["fqn"].split(".")[-1]
                to_short = to_node["fqn"].split(".")[-1]
                lines.append(f"  {from_short} -> {to_short} [{e['kind']}]")

    return "\n".join(lines)


def _build_synopsis_section(
    db: sqlite3.Connection,
    seeds: list[str] | None,
    intent_class: str,
    max_items: int,
    max_tokens: int,
) -> tuple[str, list[str]]:
    """Build synopsis section for top-ranked symbols."""
    ranked = rank_symbols(db, seeds=seeds, intent_class=intent_class, k=max_items)

    lines = []
    fqns = []
    for item in ranked:
        fqn = item["fqn"]
        fqns.append(fqn)

        # Get synopsis from DB
        row = db.execute(
            "SELECT syn.synopsis FROM synopses syn "
            "JOIN symbols s ON s.id = syn.symbol_id WHERE s.fqn = ?",
            (fqn,)
        ).fetchone()

        synopsis = row[0] if row else f"{item['kind']} {fqn.split('.')[-1]}"
        score_str = f"[score={item['score']:.1f}]"
        lines.append(f"- {fqn} {score_str}: {synopsis}")

        # Check token budget
        if estimate_tokens("\n".join(lines)) > max_tokens * 0.4:
            break

    return "\n".join(lines), fqns


def _build_semantic_section(
    db: sqlite3.Connection,
    query: str,
    max_items: int,
    intent_class: str,
    max_tokens: int,
) -> tuple[str, list[str]]:
    """Build semantic search section using V4 retrieval pipeline."""
    try:
        from .retrieval.search import semantic_search
    except ImportError:
        return "", []

    results = semantic_search(db, query, top_k=max_items, intent_class=intent_class)

    lines = []
    fqns = []
    for r in results:
        fqns.append(r.fqn)
        synopsis = r.synopsis or f"{r.kind} {r.fqn.split('.')[-1]}"
        lines.append(f"- {r.fqn} [score={r.score:.3f}]: {synopsis}")
        if estimate_tokens("\n".join(lines)) > max_tokens * 0.4:
            break

    return "\n".join(lines), fqns


def _build_mini_trace_section(
    db: sqlite3.Connection,
    seeds: list[str] | None,
    intent_class: str,
    max_items: int,
) -> str:
    """Build mini-trace section for top-ranked symbols."""
    ranked = rank_symbols(db, seeds=seeds, intent_class=intent_class, k=max_items)

    lines = []
    for item in ranked[:max_items]:
        row = db.execute(
            "SELECT syn.mini_trace FROM synopses syn "
            "JOIN symbols s ON s.id = syn.symbol_id WHERE s.fqn = ?",
            (item["fqn"],)
        ).fetchone()
        if row and row[0]:
            lines.append(f"  {row[0]}")

    return "\n".join(lines) if lines else ""
