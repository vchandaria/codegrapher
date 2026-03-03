"""SQLite schema for codegrapher Code Index.

Tables (Phase 1):
  modules       — one row per .py file
  symbols       — functions, classes, methods, variables
  inheritance   — class hierarchy (child → parent)
  imports       — import edges (module → imported module)
  calls         — call edges (caller_fqn → callee)
  registries    — top-level dicts/lists/sets from indexed JSON/Python configs
  constants     — top-level UPPER_CASE numeric/string assignments
  env_vars      — os.environ / os.getenv usage
  discrepancies — detected drift / inconsistencies / dead code

Tables (Phase 2):
  research_params     — extracted params from SESSION_RESEARCH_*.md
  json_config_entries — flattened key-value from JSON config files
  business_rules      — extracted formula instances with code_path
  pipeline_steps      — Step 0-7 boundaries with dead code markers

Tables (Phase 3):
  graph_edges    — materialized call graph with resolved symbol IDs
  proof_results  — per-invariant/contract pass/fail from proof runs
  agent_runs     — PTC script execution log
  synopses       — cached symbol summaries + mini-traces

Tables (Phase 4):
  object_spine             — dict construction sites with extracted keys
  artifact_io              — file I/O callsites catalog (parquet/json/csv/sqlite)
  bug_zoo                  — deterministic regression checks for known bug patterns
  pipeline_step_enrichment — per-step entry/exit/early-exit enrichment
"""

import sqlite3
from pathlib import Path

DDL = """
-- ── modules ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS modules (
    id            INTEGER PRIMARY KEY,
    path          TEXT UNIQUE NOT NULL,
    relpath_no_ext TEXT NOT NULL,
    sha1          TEXT,
    mtime         REAL,
    size          INTEGER,
    loc           INTEGER,
    indexed_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_modules_relpath ON modules(relpath_no_ext);

-- ── symbols ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS symbols (
    id               INTEGER PRIMARY KEY,
    module_id        INTEGER NOT NULL REFERENCES modules(id),
    name             TEXT NOT NULL,
    kind             TEXT NOT NULL,  -- function|class|method|variable
    fqn              TEXT NOT NULL,
    parent_symbol_id INTEGER REFERENCES symbols(id),
    scope_depth      INTEGER NOT NULL DEFAULT 0,
    lineno           INTEGER,
    end_lineno       INTEGER,
    signature        TEXT,
    decorators_json  TEXT,
    doc_head         TEXT,
    is_exported      INTEGER NOT NULL DEFAULT 0,
    complexity       INTEGER,
    is_async         INTEGER NOT NULL DEFAULT 0,
    is_generator     INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_symbols_fqn ON symbols(fqn);
CREATE INDEX IF NOT EXISTS idx_symbols_module ON symbols(module_id);
CREATE INDEX IF NOT EXISTS idx_symbols_parent ON symbols(parent_symbol_id);
CREATE INDEX IF NOT EXISTS idx_symbols_kind ON symbols(kind);

-- ── inheritance ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inheritance (
    id               INTEGER PRIMARY KEY,
    child_symbol_id  INTEGER NOT NULL REFERENCES symbols(id),
    parent_name      TEXT NOT NULL,
    parent_fqn       TEXT,
    order_index      INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_inh_child ON inheritance(child_symbol_id);
CREATE INDEX IF NOT EXISTS idx_inh_parent_fqn ON inheritance(parent_fqn);

-- ── imports ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS imports (
    id           INTEGER PRIMARY KEY,
    module_id    INTEGER NOT NULL REFERENCES modules(id),
    imported     TEXT NOT NULL,
    alias        TEXT,
    is_from      INTEGER NOT NULL DEFAULT 0,
    from_module  TEXT,
    lineno       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_imports_module ON imports(module_id);
CREATE INDEX IF NOT EXISTS idx_imports_imported ON imports(imported);

-- ── calls ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS calls (
    id                   INTEGER PRIMARY KEY,
    module_id            INTEGER NOT NULL REFERENCES modules(id),
    caller_symbol_id     INTEGER REFERENCES symbols(id),
    caller_fqn           TEXT,
    caller_module_path   TEXT NOT NULL,
    callee_name_raw      TEXT NOT NULL,
    callee_fqn           TEXT,
    lineno               INTEGER,
    positional_arg_count INTEGER,
    kw_names_json        TEXT,
    has_varargs          INTEGER NOT NULL DEFAULT 0,
    has_varkw            INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_calls_module ON calls(module_id);
CREATE INDEX IF NOT EXISTS idx_calls_caller ON calls(caller_fqn);
CREATE INDEX IF NOT EXISTS idx_calls_callee ON calls(callee_fqn);
CREATE INDEX IF NOT EXISTS idx_calls_callee_raw ON calls(callee_name_raw);

-- ── registries ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS registries (
    id              INTEGER PRIMARY KEY,
    module_id       INTEGER NOT NULL REFERENCES modules(id),
    name            TEXT NOT NULL,
    kind            TEXT,          -- Dict|List|Set|Tuple|Call
    lineno          INTEGER,
    value_mode      TEXT NOT NULL,  -- full|keys_only|too_large|dynamic
    value_json      TEXT,
    value_keys_json TEXT,
    value_hash      TEXT
);
CREATE INDEX IF NOT EXISTS idx_reg_name ON registries(name);
CREATE INDEX IF NOT EXISTS idx_reg_module ON registries(module_id);

-- ── constants ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS constants (
    id          INTEGER PRIMARY KEY,
    module_id   INTEGER NOT NULL REFERENCES modules(id),
    name        TEXT NOT NULL,
    lineno      INTEGER,
    value_repr  TEXT,
    value_num   REAL,
    scale_hint  TEXT  -- 0_1|0_100|pct|unknown
);
CREATE INDEX IF NOT EXISTS idx_const_name ON constants(name);

-- ── env_vars ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS env_vars (
    id              INTEGER PRIMARY KEY,
    module_id       INTEGER NOT NULL REFERENCES modules(id),
    var_name        TEXT NOT NULL,
    lineno          INTEGER,
    default_value   TEXT,
    access_pattern  TEXT,  -- os.getenv|os.environ.get|os.environ[]|dotenv
    context_snippet TEXT
);
CREATE INDEX IF NOT EXISTS idx_env_name ON env_vars(var_name);

-- ── discrepancies ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS discrepancies (
    id           INTEGER PRIMARY KEY,
    type         TEXT NOT NULL,
    severity     TEXT NOT NULL,  -- error|warn|info
    module_id    INTEGER REFERENCES modules(id),
    module_path  TEXT,
    lineno       INTEGER,
    subject      TEXT,
    symbol_id    INTEGER REFERENCES symbols(id),
    details_json TEXT,
    indexed_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_disc_type ON discrepancies(type);
CREATE INDEX IF NOT EXISTS idx_disc_severity ON discrepancies(severity);
CREATE INDEX IF NOT EXISTS idx_disc_module ON discrepancies(module_path);

-- ── research_params (Phase 2) ───────────────────────────
CREATE TABLE IF NOT EXISTS research_params (
    id              INTEGER PRIMARY KEY,
    category        TEXT NOT NULL,
    source_file     TEXT NOT NULL,
    param_name      TEXT NOT NULL,
    param_value     TEXT,
    param_num       REAL,
    normalized_unit TEXT,            -- pct|bp|days|days_range|ratio|unknown
    section         TEXT,
    extraction_kind TEXT NOT NULL,   -- python_dict|table_cell|inline|code_block|regime_rate
    confidence      REAL NOT NULL DEFAULT 0.5,  -- 0.0-1.0
    raw_text        TEXT,            -- short snippet capped at 200 chars
    lineno          INTEGER,
    indexed_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rp_category ON research_params(category);
CREATE INDEX IF NOT EXISTS idx_rp_param ON research_params(param_name);
CREATE INDEX IF NOT EXISTS idx_rp_confidence ON research_params(confidence);

-- ── json_config_entries (Phase 2) ───────────────────────
CREATE TABLE IF NOT EXISTS json_config_entries (
    id            INTEGER PRIMARY KEY,
    source_file   TEXT NOT NULL,
    source_hash   TEXT,              -- sha1 of the JSON file at index time
    category        TEXT,
    key_path      TEXT NOT NULL,     -- dot-separated: BULL_FLAG.stop_pct
    value_repr    TEXT,
    value_num     REAL,
    value_type    TEXT,              -- num|str|bool|null|obj|arr
    indexed_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jce_category ON json_config_entries(category);
CREATE INDEX IF NOT EXISTS idx_jce_key ON json_config_entries(key_path);
CREATE INDEX IF NOT EXISTS idx_jce_source ON json_config_entries(source_file);

-- ── call_resolution (Phase 2) ───────────────────────────
-- Separate resolution table — never overwrites calls without traceability.
-- calls.callee_fqn is only set when confidence >= 0.95.
CREATE TABLE IF NOT EXISTS call_resolution (
    id                  INTEGER PRIMARY KEY,
    call_id             INTEGER NOT NULL REFERENCES calls(id),
    resolved_callee_fqn TEXT NOT NULL,
    resolution_method   TEXT NOT NULL,  -- same_module|import_alias|qualified|global_unique|method_self|heuristic
    confidence          REAL NOT NULL,
    candidates_json     TEXT,           -- JSON list when ambiguous (capped at 10)
    indexed_at          TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cr_call ON call_resolution(call_id);
CREATE INDEX IF NOT EXISTS idx_cr_fqn ON call_resolution(resolved_callee_fqn);
CREATE INDEX IF NOT EXISTS idx_cr_method ON call_resolution(resolution_method);
CREATE INDEX IF NOT EXISTS idx_cr_conf ON call_resolution(confidence);

-- ── business_rules (Phase 2) ────────────────────────────
CREATE TABLE IF NOT EXISTS business_rules (
    id             INTEGER PRIMARY KEY,
    module_id      INTEGER REFERENCES modules(id),
    module_path    TEXT NOT NULL,
    source_file    TEXT NOT NULL,
    rule_name      TEXT NOT NULL,
    rule_variant   TEXT,
    lineno         INTEGER,
    end_lineno     INTEGER,
    formula_text   TEXT,            -- raw extracted snippet
    rule_hash      TEXT,            -- sha1 of normalized formula
    variables_json TEXT,            -- {var_name: role}
    code_path      TEXT,            -- gpu_fast_path|serial_path|shared
    stage_hint     TEXT,            -- Step N if inside a detected step region
    confidence     REAL NOT NULL DEFAULT 0.5,
    indexed_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_br_rule ON business_rules(rule_name);
CREATE INDEX IF NOT EXISTS idx_br_hash ON business_rules(rule_hash);
CREATE INDEX IF NOT EXISTS idx_br_path ON business_rules(code_path);
CREATE INDEX IF NOT EXISTS idx_br_module ON business_rules(module_path);

-- ── pipeline_steps (Phase 2) ────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_steps (
    id            INTEGER PRIMARY KEY,
    module_id     INTEGER REFERENCES modules(id),
    module_path   TEXT NOT NULL,
    source_file   TEXT NOT NULL,
    step_number   INTEGER NOT NULL,
    step_label    TEXT,
    lineno        INTEGER NOT NULL,
    end_lineno    INTEGER,
    code_path     TEXT,
    -- deadness levels: dead_by_intent|dead_by_reachability|alive|unknown
    deadness      TEXT NOT NULL DEFAULT 'unknown',
    is_dead_code  INTEGER NOT NULL DEFAULT 0,   -- 1 if deadness != alive
    confidence    REAL NOT NULL DEFAULT 0.5,
    indexed_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ps_step ON pipeline_steps(step_number);
CREATE INDEX IF NOT EXISTS idx_ps_dead ON pipeline_steps(deadness);

-- ── graph_edges (Phase 3) ──────────────────────────────
-- Materialized call graph edges with resolved symbol IDs.
-- Built from calls + call_resolution where confidence >= 0.80.
CREATE TABLE IF NOT EXISTS graph_edges (
    id               INTEGER PRIMARY KEY,
    caller_symbol_id INTEGER NOT NULL REFERENCES symbols(id),
    callee_symbol_id INTEGER NOT NULL REFERENCES symbols(id),
    edge_kind        TEXT NOT NULL,  -- call|import|inheritance
    confidence       REAL NOT NULL,
    lineno           INTEGER,
    indexed_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ge_caller ON graph_edges(caller_symbol_id);
CREATE INDEX IF NOT EXISTS idx_ge_callee ON graph_edges(callee_symbol_id);
CREATE INDEX IF NOT EXISTS idx_ge_kind ON graph_edges(edge_kind);

-- ── proof_results (Phase 3) ────────────────────────────
-- Per-invariant/contract pass/fail from each proof run.
CREATE TABLE IF NOT EXISTS proof_results (
    id            INTEGER PRIMARY KEY,
    run_id        TEXT NOT NULL,         -- ISO timestamp
    rule_name     TEXT NOT NULL,         -- invariant/contract name from intent YAML
    rule_kind     TEXT NOT NULL,         -- invariant|data_contract
    status        TEXT NOT NULL,         -- pass|fail|skip
    details_json  TEXT,                  -- evidence dict
    checked_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_proof_run ON proof_results(run_id);
CREATE INDEX IF NOT EXISTS idx_proof_rule ON proof_results(rule_name);
CREATE INDEX IF NOT EXISTS idx_proof_status ON proof_results(status);

-- ── agent_runs (Phase 3) ───────────────────────────────
-- Logs every PTC script execution for feedback/audit.
CREATE TABLE IF NOT EXISTS agent_runs (
    id             INTEGER PRIMARY KEY,
    run_id         TEXT NOT NULL UNIQUE,  -- UUID
    script_text    TEXT,
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    status         TEXT NOT NULL DEFAULT 'running',  -- running|completed|error|timeout
    sql_calls      INTEGER DEFAULT 0,
    total_rows     INTEGER DEFAULT 0,
    error_msg      TEXT,
    pack_tokens    INTEGER,
    details_json   TEXT
);
CREATE INDEX IF NOT EXISTS idx_ar_status ON agent_runs(status);

-- ── synopses (Phase 3) ────────────────────────────────
-- Cached 1-3 sentence summaries of key symbols.
CREATE TABLE IF NOT EXISTS synopses (
    id          INTEGER PRIMARY KEY,
    symbol_id   INTEGER UNIQUE NOT NULL REFERENCES symbols(id),
    synopsis    TEXT NOT NULL,
    mini_trace  TEXT,
    generated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_syn_symbol ON synopses(symbol_id);

-- ── object_spine (Phase 4) ────────────────────────────────
-- Dict construction sites with extracted keys for data contract validation.
CREATE TABLE IF NOT EXISTS object_spine (
    id             INTEGER PRIMARY KEY,
    module_id      INTEGER REFERENCES modules(id),
    module_path    TEXT NOT NULL,
    dict_name      TEXT NOT NULL,
    lineno         INTEGER NOT NULL,
    end_lineno     INTEGER,
    keys_json      TEXT NOT NULL,
    key_count      INTEGER NOT NULL,
    construction   TEXT NOT NULL,
    code_path      TEXT,
    contract_name  TEXT,
    indexed_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_os_name ON object_spine(dict_name);
CREATE INDEX IF NOT EXISTS idx_os_module ON object_spine(module_path);
CREATE INDEX IF NOT EXISTS idx_os_contract ON object_spine(contract_name);

-- ── artifact_io (Phase 4) ────────────────────────────────
-- File I/O callsites catalog (parquet, json, csv, sqlite).
CREATE TABLE IF NOT EXISTS artifact_io (
    id             INTEGER PRIMARY KEY,
    module_id      INTEGER REFERENCES modules(id),
    module_path    TEXT NOT NULL,
    lineno         INTEGER NOT NULL,
    io_direction   TEXT NOT NULL,
    io_format      TEXT NOT NULL,
    path_expr      TEXT,
    symbol_fqn     TEXT,
    indexed_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_aio_format ON artifact_io(io_format);
CREATE INDEX IF NOT EXISTS idx_aio_dir ON artifact_io(io_direction);
CREATE INDEX IF NOT EXISTS idx_aio_module ON artifact_io(module_path);

-- ── bug_zoo (Phase 4) ────────────────────────────────────
-- Deterministic regression checks for known historical bug patterns.
CREATE TABLE IF NOT EXISTS bug_zoo (
    id             INTEGER PRIMARY KEY,
    pattern_name   TEXT NOT NULL,
    category       TEXT NOT NULL,
    module_path    TEXT NOT NULL,
    lineno         INTEGER,
    status         TEXT NOT NULL,
    evidence_json  TEXT,
    severity       TEXT NOT NULL,
    indexed_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bz_pattern ON bug_zoo(pattern_name);
CREATE INDEX IF NOT EXISTS idx_bz_status ON bug_zoo(status);

-- ── pipeline_step_enrichment (Phase 4) ───────────────────
-- Per-step entry/exit/early-exit enrichment data.
CREATE TABLE IF NOT EXISTS pipeline_step_enrichment (
    id             INTEGER PRIMARY KEY,
    step_id        INTEGER NOT NULL REFERENCES pipeline_steps(id),
    entry_fqn      TEXT,
    exit_outputs   TEXT,
    early_exits    TEXT,
    called_fqns    TEXT,
    confidence     REAL NOT NULL DEFAULT 0.5,
    indexed_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pse_step ON pipeline_step_enrichment(step_id);

-- ── field_reads (Phase 5) ────────────────────────────────
-- Dict field consumption tracking: who reads what field from which source dict.
-- Captures FORMATION_METADATA, SCANNER_PROFILES, etc. field reads including nested dicts.
CREATE TABLE IF NOT EXISTS field_reads (
    id              INTEGER PRIMARY KEY,
    module_id       INTEGER NOT NULL REFERENCES modules(id),
    source_dict     TEXT NOT NULL,       -- 'FORMATION_METADATA', 'SCANNER_PROFILES', etc.
    field_name      TEXT NOT NULL,       -- 'stop_pct' or 'runner_extension.enabled' (dotted)
    parent_field    TEXT,                -- 'runner_extension' for nested reads, NULL for top-level
    default_value   TEXT,                -- from .get(field, default) if present
    consumer_file   TEXT NOT NULL,       -- relative path
    consumer_fqn    TEXT,                -- fully qualified function name
    lineno          INTEGER NOT NULL,
    access_pattern  TEXT NOT NULL,       -- 'subscript', 'get', 'get_nested'
    confidence      TEXT NOT NULL DEFAULT 'MEDIUM'  -- 'HIGH', 'MEDIUM', 'LOW'
);
CREATE INDEX IF NOT EXISTS idx_fr_source ON field_reads(source_dict);
CREATE INDEX IF NOT EXISTS idx_fr_field ON field_reads(field_name);
CREATE INDEX IF NOT EXISTS idx_fr_parent ON field_reads(parent_field);
CREATE INDEX IF NOT EXISTS idx_fr_module ON field_reads(module_id);
CREATE INDEX IF NOT EXISTS idx_fr_consumer ON field_reads(consumer_file);
CREATE INDEX IF NOT EXISTS idx_fr_confidence ON field_reads(confidence);

-- ── Pipeline dict writes ─────────────────────────────────
-- Tracks writes into shared dicts (task_dict, trade_setup, simulation_params)
-- so we can cross-reference what was written vs. what is consumed (field_reads).
CREATE TABLE IF NOT EXISTS pipeline_dict_writes (
    id              INTEGER PRIMARY KEY,
    module_id       INTEGER NOT NULL REFERENCES modules(id),
    target_dict     TEXT NOT NULL,   -- 'task_dict', 'trade_setup', 'simulation_params', etc.
    field_name      TEXT NOT NULL,   -- 'flow_multiplier', 'stop_pct', etc.
    writer_file     TEXT NOT NULL,   -- relative path
    writer_fqn      TEXT,            -- fully qualified function writing the field
    lineno          INTEGER NOT NULL,
    value_repr      TEXT,            -- right-hand side repr (if simple literal/constant)
    confidence      TEXT NOT NULL DEFAULT 'MEDIUM'  -- 'HIGH', 'MEDIUM', 'LOW'
);
CREATE INDEX IF NOT EXISTS idx_pdw_target ON pipeline_dict_writes(target_dict);
CREATE INDEX IF NOT EXISTS idx_pdw_field  ON pipeline_dict_writes(field_name);
CREATE INDEX IF NOT EXISTS idx_pdw_module ON pipeline_dict_writes(module_id);
CREATE INDEX IF NOT EXISTS idx_pdw_writer ON pipeline_dict_writes(writer_file);

-- ── FTS for symbol search ────────────────────────────────
CREATE VIRTUAL TABLE IF NOT EXISTS symbols_fts
    USING fts5(name, fqn, doc_head, content=symbols, content_rowid=id);
"""

REBUILD_FTS = """
INSERT INTO symbols_fts(symbols_fts) VALUES('rebuild');
"""


def create_database(db_path: Path) -> sqlite3.Connection:
    """Create (or open) the index database and ensure schema exists."""
    db = sqlite3.connect(str(db_path))
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript(DDL)
    return db


def clear_module_data(db: sqlite3.Connection, module_id: int) -> None:
    """Remove all data for a module (for incremental re-index)."""
    for table in ("symbols", "imports", "calls", "registries",
                  "constants", "env_vars", "discrepancies",
                  "business_rules", "pipeline_steps",
                  "object_spine", "artifact_io", "bug_zoo",
                  "field_reads", "pipeline_dict_writes"):
        db.execute(f"DELETE FROM {table} WHERE module_id = ?", (module_id,))
    db.execute("DELETE FROM inheritance WHERE child_symbol_id IN "
               "(SELECT id FROM symbols WHERE module_id = ?)", (module_id,))
    # pipeline_step_enrichment uses step_id, not module_id
    db.execute("DELETE FROM pipeline_step_enrichment WHERE step_id IN "
               "(SELECT id FROM pipeline_steps WHERE module_id = ?)", (module_id,))


def rebuild_fts(db: sqlite3.Connection) -> None:
    """Rebuild the FTS index after bulk inserts."""
    db.executescript(REBUILD_FTS)
