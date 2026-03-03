"""Central configuration for codegrapher.

Defaults work for any Python project out of the box.
Override via a `codegrapher.yaml` file in the project root, or env vars.

Advanced users: set TRACKED_SOURCE_DICTS, METADATA_RETURNING_FUNCS, etc.
to enable full dict/field tracking behaviour.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import FrozenSet, List, Optional

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_PATH: str = os.environ.get(
    "CODEGRAPHER_DB",
    ".codegrapher/code_index.db",
)

# ---------------------------------------------------------------------------
# Call resolution — roots treated as "internal" (not external packages)
# Extend this list in codegrapher.yaml if your project uses non-standard roots.
# ---------------------------------------------------------------------------
INTERNAL_ROOTS: FrozenSet[str] = frozenset(
    os.environ.get(
        "CODEGRAPHER_INTERNAL_ROOTS",
        "src.,scripts.,tools.,apps.",
    ).split(",")
)

# ---------------------------------------------------------------------------
# Dict / field tracking (empty by default — populate via codegrapher.yaml)
# TRACKED_SOURCE_DICTS  : top-level dicts whose keys are tracked as "fields"
# METADATA_RETURNING_FUNCS : functions that return a tracked dict entry
# METADATA_PARAM_NAMES  : parameter names that carry tracked dict entries
# ---------------------------------------------------------------------------
TRACKED_SOURCE_DICTS: FrozenSet[str] = frozenset(
    filter(None, os.environ.get("CODEGRAPHER_TRACKED_DICTS", "").split(","))
)
METADATA_RETURNING_FUNCS: FrozenSet[str] = frozenset(
    filter(None, os.environ.get("CODEGRAPHER_META_FUNCS", "").split(","))
)
METADATA_PARAM_NAMES: FrozenSet[str] = frozenset(
    filter(None, os.environ.get("CODEGRAPHER_META_PARAMS", "").split(","))
)

# ---------------------------------------------------------------------------
# Skeleton builder
# ---------------------------------------------------------------------------
SKELETON_TITLE: str = os.environ.get(
    "CODEGRAPHER_SKELETON_TITLE",
    "Codebase Skeleton (auto-generated)",
)
# Glob patterns (relative to project root) excluded from the skeleton view.
SKELETON_EXCLUDE_PATHS: List[str] = list(
    filter(None, os.environ.get("CODEGRAPHER_SKELETON_EXCLUDE", "").split(","))
)

# ---------------------------------------------------------------------------
# JSON config indexer
# Relative paths (from project root) of JSON files to index.
# ---------------------------------------------------------------------------
CONFIG_FILES: List[str] = list(
    filter(None, os.environ.get("CODEGRAPHER_CONFIG_FILES", "").split(","))
)
# Named top-level dicts inside the indexed JSON files to treat as registries.
REGISTRY_NAMES: List[str] = list(
    filter(None, os.environ.get("CODEGRAPHER_REGISTRY_NAMES", "").split(","))
)

# ---------------------------------------------------------------------------
# Control plane
# ---------------------------------------------------------------------------
# Optional path to a project intent YAML (intent.yaml-style).
INTENT_PATH: Optional[Path] = (
    Path(os.environ["CODEGRAPHER_INTENT_PATH"])
    if "CODEGRAPHER_INTENT_PATH" in os.environ
    else None
)

# Set to True to enable project-specific formation semantic edges in graph_enricher.
ENABLE_FORMATION_EDGES: bool = (
    os.environ.get("CODEGRAPHER_FORMATION_EDGES", "false").lower() == "true"
)

# ---------------------------------------------------------------------------
# Load overrides from codegrapher.yaml if present
# ---------------------------------------------------------------------------
def _load_yaml_overrides() -> None:  # noqa: C901
    """Merge settings from codegrapher.yaml in the current working directory."""
    yaml_path = Path.cwd() / "codegrapher.yaml"
    if not yaml_path.exists():
        return
    try:
        import yaml  # type: ignore
    except ImportError:
        return

    global DB_PATH, INTERNAL_ROOTS, TRACKED_SOURCE_DICTS, METADATA_RETURNING_FUNCS
    global METADATA_PARAM_NAMES, SKELETON_TITLE, SKELETON_EXCLUDE_PATHS
    global CONFIG_FILES, REGISTRY_NAMES, INTENT_PATH, ENABLE_FORMATION_EDGES

    data: dict = yaml.safe_load(yaml_path.read_text()) or {}
    if "db_path" in data:
        DB_PATH = data["db_path"]
    if "internal_roots" in data:
        INTERNAL_ROOTS = frozenset(data["internal_roots"])
    if "tracked_source_dicts" in data:
        TRACKED_SOURCE_DICTS = frozenset(data["tracked_source_dicts"])
    if "metadata_returning_funcs" in data:
        METADATA_RETURNING_FUNCS = frozenset(data["metadata_returning_funcs"])
    if "metadata_param_names" in data:
        METADATA_PARAM_NAMES = frozenset(data["metadata_param_names"])
    if "skeleton_title" in data:
        SKELETON_TITLE = data["skeleton_title"]
    if "skeleton_exclude_paths" in data:
        SKELETON_EXCLUDE_PATHS = list(data["skeleton_exclude_paths"])
    if "config_files" in data:
        CONFIG_FILES = list(data["config_files"])
    if "registry_names" in data:
        REGISTRY_NAMES = list(data["registry_names"])
    if "intent_path" in data:
        INTENT_PATH = Path(data["intent_path"])
    if "enable_formation_edges" in data:
        ENABLE_FORMATION_EDGES = bool(data["enable_formation_edges"])


_load_yaml_overrides()
