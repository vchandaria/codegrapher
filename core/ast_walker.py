"""AST visitor that extracts symbols, imports, calls, registries, constants, env vars.

Walks a single Python file and returns a ModuleRecord dataclass with all extracted data.
Does NOT persist — that's persistence.py's job.
"""

import ast
import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .config import (
    TRACKED_SOURCE_DICTS as _CFG_TRACKED_DICTS,
    METADATA_RETURNING_FUNCS as _CFG_META_FUNCS,
    METADATA_PARAM_NAMES as _CFG_META_PARAMS,
)


# ── Data containers ──────────────────────────────────────

@dataclass
class SymbolRecord:
    name: str
    kind: str  # function|class|method|variable
    fqn: str
    parent_fqn: Optional[str]
    scope_depth: int
    lineno: int
    end_lineno: Optional[int]
    signature: Optional[str]
    decorators: list
    doc_head: Optional[str]
    is_exported: bool
    is_async: bool
    is_generator: bool


@dataclass
class ImportRecord:
    imported: str
    alias: Optional[str]
    is_from: bool
    from_module: Optional[str]
    lineno: int


@dataclass
class CallRecord:
    caller_fqn: Optional[str]
    callee_name_raw: str
    lineno: int
    positional_arg_count: int
    kw_names: list
    has_varargs: bool
    has_varkw: bool


@dataclass
class RegistryRecord:
    name: str
    kind: str
    lineno: int
    value_mode: str  # full|keys_only|too_large|dynamic
    value_json: Optional[str]
    value_keys_json: Optional[str]
    value_hash: Optional[str]


@dataclass
class ConstantRecord:
    name: str
    lineno: int
    value_repr: str
    value_num: Optional[float]
    scale_hint: Optional[str]


@dataclass
class EnvVarRecord:
    var_name: str
    lineno: int
    default_value: Optional[str]
    access_pattern: str
    context_snippet: str


@dataclass
class FieldReadRecord:
    source_dict: str         # 'FORMATION_METADATA', 'SCANNER_PROFILES', etc.
    field_name: str          # 'stop_pct' or 'runner_extension.enabled' (dotted for nested)
    parent_field: Optional[str]  # 'runner_extension' for nested reads, None for top-level
    default_value: Optional[str]  # from .get(field, default) if present
    lineno: int
    access_pattern: str      # 'subscript', 'get', 'get_nested'
    confidence: str          # 'HIGH', 'MEDIUM', 'LOW'


@dataclass
class ModuleRecord:
    path: str
    relpath_no_ext: str
    loc: int
    sha1: str
    mtime: float
    size: int
    parse_error: Optional[str] = None
    all_exports: Optional[list] = None
    symbols: list = field(default_factory=list)
    imports: list = field(default_factory=list)
    calls: list = field(default_factory=list)
    registries: list = field(default_factory=list)
    constants: list = field(default_factory=list)
    env_vars: list = field(default_factory=list)
    field_reads: list = field(default_factory=list)


# ── Value hashing ────────────────────────────────────────

MAX_VALUE_JSON_SIZE = 10_000  # 10KB cap

def _stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _try_literal_eval(node: ast.expr):
    """Try to evaluate an AST node as a literal. Returns (value, success)."""
    try:
        val = ast.literal_eval(node)
        return val, True
    except (ValueError, TypeError, RecursionError):
        return None, False


def _extract_registry_value(node: ast.expr):
    """Extract value info for a registry (dict/list/set/tuple) node."""
    val, ok = _try_literal_eval(node)
    if ok:
        serialized = json.dumps(val, sort_keys=True, default=str)
        vhash = _stable_hash(serialized)
        if len(serialized) <= MAX_VALUE_JSON_SIZE:
            keys_json = None
            if isinstance(val, dict):
                keys_json = json.dumps(list(val.keys()), default=str)
            return "full", serialized, keys_json, vhash
        else:
            keys_json = None
            if isinstance(val, dict):
                keys_json = json.dumps(list(val.keys()), default=str)
            return "keys_only" if keys_json else "too_large", None, keys_json, vhash
    # Non-literal: store AST dump hash
    try:
        dump = ast.dump(node)
        vhash = _stable_hash(dump)
        # Try to extract dict keys even from non-literal dicts
        keys_json = None
        if isinstance(node, ast.Dict):
            keys = []
            for k in node.keys:
                if k is not None:
                    try:
                        keys.append(ast.literal_eval(k))
                    except (ValueError, TypeError):
                        try:
                            keys.append(ast.unparse(k))
                        except Exception:
                            keys.append("__DYNAMIC__")
            keys_json = json.dumps(keys, default=str) if keys else None
        return "dynamic", None, keys_json, vhash
    except Exception:
        return "dynamic", None, None, None


# ── Scale hint inference ─────────────────────────────────

_SCALE_0_1_PATTERNS = re.compile(
    r'(ratio|factor|weight|alpha|beta|decay|probability|prob|pct_raw)', re.I
)
_SCALE_0_100_PATTERNS = re.compile(
    r'(score|confidence|threshold|conviction|min_score|max_score)', re.I
)
_SCALE_PCT_PATTERNS = re.compile(
    r'(pct|percent|rate|risk_per_trade)', re.I
)


def _infer_scale_hint(name: str, value: float) -> str:
    if _SCALE_PCT_PATTERNS.search(name):
        return "pct"
    if _SCALE_0_100_PATTERNS.search(name):
        return "0_100" if value > 1.0 else "0_1"
    if _SCALE_0_1_PATTERNS.search(name):
        return "0_1"
    if 0.0 <= value <= 1.0:
        return "0_1"
    if 1.0 < value <= 100.0:
        return "0_100"
    return "unknown"


# ── Generator detection ──────────────────────────────────

def _has_yield(node: ast.AST) -> bool:
    """Check if a function body contains yield/yield from."""
    for child in ast.walk(node):
        if isinstance(child, (ast.Yield, ast.YieldFrom)):
            return True
    return False


# ── Main AST Visitor ─────────────────────────────────────

class CodeVisitor(ast.NodeVisitor):
    """Extracts all structural data from a single Python module."""

    # Source dicts, returning funcs, and param names loaded from config.
    # Set via codegrapher.yaml or env vars; empty by default.
    _TRACKED_SOURCE_DICTS = _CFG_TRACKED_DICTS
    _METADATA_RETURNING_FUNCS = _CFG_META_FUNCS
    _METADATA_PARAM_NAMES = _CFG_META_PARAMS

    def __init__(self, module_ns: str, source_lines: list[str]):
        self.module_ns = module_ns
        self.source_lines = source_lines
        self.symbols: list[SymbolRecord] = []
        self.imports: list[ImportRecord] = []
        self.calls: list[CallRecord] = []
        self.registries: list[RegistryRecord] = []
        self.constants: list[ConstantRecord] = []
        self.env_vars: list[EnvVarRecord] = []
        self.field_reads: list[FieldReadRecord] = []
        self.all_exports: Optional[list] = None

        # Scope tracking
        self._scope_stack: list[str] = []  # [ClassA, method_b, ...]
        self._depth = 0

        # Field tracking: aliases that map to known source dicts
        # Maps variable name -> source dict name
        # e.g., "fp" -> "FORMATION_METADATA", "runner_ext" -> "FORMATION_METADATA.runner_extension"
        self._dict_aliases: dict[str, str] = {}
        # Track current function's parameter names that are aliases
        self._param_aliases: dict[str, str] = {}

    @property
    def _current_fqn_prefix(self) -> str:
        if self._scope_stack:
            return self.module_ns + "." + ".".join(self._scope_stack)
        return self.module_ns

    def _make_fqn(self, name: str, is_nested_func: bool = False) -> str:
        prefix = self._current_fqn_prefix
        if is_nested_func and self._depth > 1:
            return f"{prefix}::{name}"
        return f"{prefix}.{name}"

    def _get_context_snippet(self, lineno: int) -> str:
        if 0 < lineno <= len(self.source_lines):
            return self.source_lines[lineno - 1].strip()[:120]
        return ""

    # ── Imports ──

    def visit_Import(self, node: ast.Import):
        for alias in node.names:
            self.imports.append(ImportRecord(
                imported=alias.name,
                alias=alias.asname,
                is_from=False,
                from_module=None,
                lineno=node.lineno,
            ))
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        from_mod = node.module or ""
        for alias in node.names:
            target = f"{from_mod}.{alias.name}" if alias.name != "*" else f"{from_mod}.*"
            self.imports.append(ImportRecord(
                imported=target,
                alias=alias.asname,
                is_from=True,
                from_module=from_mod,
                lineno=node.lineno,
            ))
        self.generic_visit(node)

    # ── Classes ──

    def visit_ClassDef(self, node: ast.ClassDef):
        fqn = self._make_fqn(node.name)
        parent_fqn = self._current_fqn_prefix if self._scope_stack else None
        bases = []
        for b in node.bases:
            try:
                bases.append(ast.unparse(b))
            except Exception:
                pass
        doc = ast.get_docstring(node)
        self.symbols.append(SymbolRecord(
            name=node.name,
            kind="class",
            fqn=fqn,
            parent_fqn=parent_fqn,
            scope_depth=self._depth,
            lineno=node.lineno,
            end_lineno=node.end_lineno,
            signature=None,
            decorators=[ast.unparse(d) for d in node.decorator_list],
            doc_head=doc[:200] if doc else None,
            is_exported=False,  # computed later
            is_async=False,
            is_generator=False,
        ))
        # Store inheritance
        for idx, base_name in enumerate(bases):
            self.symbols[-1]  # reference for later linking
            # We store inheritance as a pseudo-record; persistence layer handles it
        # Push scope
        self._scope_stack.append(node.name)
        self._depth += 1
        self.generic_visit(node)
        self._depth -= 1
        self._scope_stack.pop()

    # ── Functions / Methods ──

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self._handle_func(node, is_async=False)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        self._handle_func(node, is_async=True)

    def _handle_func(self, node, is_async: bool):
        # Determine if method or function
        in_class = self._scope_stack and any(
            s[0].isupper() for s in self._scope_stack[-1:]
        )
        kind = "method" if in_class else "function"
        is_nested = self._depth > (1 if in_class else 0)
        fqn = self._make_fqn(node.name, is_nested_func=is_nested)
        parent_fqn = self._current_fqn_prefix if self._scope_stack else None

        # Build signature string
        try:
            args = []
            for a in node.args.args:
                ann = ""
                if a.annotation:
                    try:
                        ann = f": {ast.unparse(a.annotation)}"
                    except Exception:
                        pass
                args.append(f"{a.arg}{ann}")
            sig = f"({', '.join(args)})"
            if node.returns:
                try:
                    sig += f" -> {ast.unparse(node.returns)}"
                except Exception:
                    pass
        except Exception:
            sig = None

        doc = ast.get_docstring(node)
        self.symbols.append(SymbolRecord(
            name=node.name,
            kind=kind,
            fqn=fqn,
            parent_fqn=parent_fqn,
            scope_depth=self._depth,
            lineno=node.lineno,
            end_lineno=node.end_lineno,
            signature=sig,
            decorators=[ast.unparse(d) for d in node.decorator_list],
            doc_head=doc[:200] if doc else None,
            is_exported=False,
            is_async=is_async,
            is_generator=_has_yield(node),
        ))

        # Push scope and visit children
        # Track parameter-name aliases for field reads
        saved_param_aliases = dict(self._param_aliases)
        saved_dict_aliases = dict(self._dict_aliases)
        for a in node.args.args:
            if a.arg in self._METADATA_PARAM_NAMES:
                self._param_aliases[a.arg] = "FORMATION_METADATA"
                self._dict_aliases[a.arg] = "FORMATION_METADATA"

        self._scope_stack.append(node.name)
        self._depth += 1
        self.generic_visit(node)
        self._depth -= 1
        self._scope_stack.pop()

        # Restore aliases (scope exit)
        self._param_aliases = saved_param_aliases
        self._dict_aliases = saved_dict_aliases

    # ── Calls ──

    def visit_Call(self, node: ast.Call):
        try:
            callee_raw = ast.unparse(node.func)
        except Exception:
            callee_raw = "???"

        # Detect env var access
        self._check_env_var(node, callee_raw)

        # Detect .get("field") on tracked dict aliases
        self._check_dict_get(node, callee_raw)

        kw_names = [kw.arg for kw in node.keywords if kw.arg is not None]
        has_varargs = any(
            isinstance(a, ast.Starred) for a in node.args
        )
        has_varkw = any(kw.arg is None for kw in node.keywords)

        caller_fqn = self._current_fqn_prefix if self._scope_stack else self.module_ns
        self.calls.append(CallRecord(
            caller_fqn=caller_fqn,
            callee_name_raw=callee_raw,
            lineno=node.lineno,
            positional_arg_count=len(node.args),
            kw_names=kw_names,
            has_varargs=has_varargs,
            has_varkw=has_varkw,
        ))
        self.generic_visit(node)

    # ── Env var detection ──

    def _check_env_var(self, node: ast.Call, callee_raw: str):
        patterns = {
            "os.environ.get": "os.environ.get",
            "os.getenv": "os.getenv",
            "os.environ.setdefault": "os.environ.setdefault",
        }
        for pat, access in patterns.items():
            if callee_raw.endswith(pat) or callee_raw == pat:
                if node.args:
                    try:
                        var_name = ast.literal_eval(node.args[0])
                    except (ValueError, TypeError):
                        var_name = ast.unparse(node.args[0])
                    default_val = None
                    if len(node.args) > 1:
                        try:
                            default_val = repr(ast.literal_eval(node.args[1]))
                        except (ValueError, TypeError):
                            default_val = ast.unparse(node.args[1])
                    # Check keyword default
                    if default_val is None:
                        for kw in node.keywords:
                            if kw.arg == "default":
                                try:
                                    default_val = repr(ast.literal_eval(kw.value))
                                except (ValueError, TypeError):
                                    default_val = ast.unparse(kw.value)
                    self.env_vars.append(EnvVarRecord(
                        var_name=str(var_name),
                        lineno=node.lineno,
                        default_value=default_val,
                        access_pattern=access,
                        context_snippet=self._get_context_snippet(node.lineno),
                    ))
                break

    # ── Alias tracking for dict field reads ──

    def _check_alias_assignment(self, node: ast.Assign):
        """Detect assignments that create aliases for tracked source dicts.

        Patterns:
          fp = get_params(key)  -> fp aliases a tracked source dict
          runner_ext = fp.get('runner_extension', {})  -> runner_ext aliases tracked_dict.runner_extension
          runner_ext = fp['runner_extension']  -> same
        """
        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            return
        var_name = node.targets[0].id

        # Pattern 1: var = metadata_returning_func(...)
        if isinstance(node.value, ast.Call):
            try:
                callee = ast.unparse(node.value.func)
            except Exception:
                callee = ""
            # Strip module prefix: pkg.module.func_name -> func_name
            callee_short = callee.rsplit(".", 1)[-1] if "." in callee else callee
            if callee_short in self._METADATA_RETURNING_FUNCS:
                self._dict_aliases[var_name] = self._TRACKED_SOURCE_DICTS[0] if self._TRACKED_SOURCE_DICTS else var_name
                return

            # Pattern 2: var = alias.get('field', {}) -> var aliases source_dict.field
            if isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "get":
                base = None
                if isinstance(node.value.func.value, ast.Name):
                    base = node.value.func.value.id
                if base and base in self._dict_aliases and node.value.args:
                    try:
                        field_key = ast.literal_eval(node.value.args[0])
                        if isinstance(field_key, str):
                            parent_source = self._dict_aliases[base]
                            self._dict_aliases[var_name] = f"{parent_source}.{field_key}"
                    except (ValueError, TypeError):
                        pass
                return

        # Pattern 3: var = alias['field'] -> var aliases source_dict.field
        if isinstance(node.value, ast.Subscript):
            if isinstance(node.value.value, ast.Name) and isinstance(node.value.slice, ast.Constant):
                base = node.value.value.id
                field_key = node.value.slice.value
                if isinstance(field_key, str) and base in self._dict_aliases:
                    parent_source = self._dict_aliases[base]
                    self._dict_aliases[var_name] = f"{parent_source}.{field_key}"

    # ── Top-level assignments (registries, constants, __all__) + alias tracking ──

    def visit_Assign(self, node: ast.Assign):
        # Track dict alias assignments at ANY depth
        self._check_alias_assignment(node)

        # Only capture registries/constants at module-level or class-level
        if self._depth > 1:
            self.generic_visit(node)
            return

        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            name = target.id

            # __all__ detection
            if name == "__all__":
                try:
                    val = ast.literal_eval(node.value)
                    if isinstance(val, (list, tuple)):
                        self.all_exports = list(val)
                except (ValueError, TypeError):
                    pass
                continue

            # UPPER_CASE names only
            if not name.isupper() and not name.startswith("_"):
                self.generic_visit(node)
                return

            if not name.isupper():
                continue

            # Registry detection (dicts, lists, sets, tuples)
            if isinstance(node.value, (ast.Dict, ast.List, ast.Set, ast.Tuple)):
                kind = type(node.value).__name__
                mode, vjson, vkeys, vhash = _extract_registry_value(node.value)
                self.registries.append(RegistryRecord(
                    name=name, kind=kind, lineno=node.lineno,
                    value_mode=mode, value_json=vjson,
                    value_keys_json=vkeys, value_hash=vhash,
                ))
            elif isinstance(node.value, ast.Call):
                # dict(...) / set(...) / frozenset(...)
                if isinstance(node.value.func, ast.Name):
                    call_name = node.value.func.id
                    if call_name in ("dict", "set", "frozenset", "OrderedDict"):
                        mode, vjson, vkeys, vhash = _extract_registry_value(node.value)
                        self.registries.append(RegistryRecord(
                            name=name, kind="Call:" + call_name, lineno=node.lineno,
                            value_mode=mode, value_json=vjson,
                            value_keys_json=vkeys, value_hash=vhash,
                        ))
            else:
                # Constant detection: UPPER_CASE numeric or string
                try:
                    val = ast.literal_eval(node.value)
                    if isinstance(val, (int, float)):
                        self.constants.append(ConstantRecord(
                            name=name, lineno=node.lineno,
                            value_repr=repr(val), value_num=float(val),
                            scale_hint=_infer_scale_hint(name, float(val)),
                        ))
                    elif isinstance(val, str):
                        self.constants.append(ConstantRecord(
                            name=name, lineno=node.lineno,
                            value_repr=repr(val)[:200], value_num=None,
                            scale_hint=None,
                        ))
                except (ValueError, TypeError):
                    pass

        self.generic_visit(node)

    # ── Dict .get() field read detection ──

    def _check_dict_get(self, node: ast.Call, callee_raw: str):
        """Detect .get("field_name", default) on tracked dict aliases.

        Also captures dynamic key accesses like .get(variable) as
        '<dynamic:variable_name>' for inventory tracking.
        """
        if not isinstance(node.func, ast.Attribute):
            return
        if node.func.attr != "get":
            return
        if not node.args:
            return

        # Get the field name from first arg
        is_dynamic = False
        dynamic_var = None
        try:
            field_name = ast.literal_eval(node.args[0])
            if not isinstance(field_name, str):
                return
        except (ValueError, TypeError):
            # Dynamic key — capture the variable name
            is_dynamic = True
            try:
                dynamic_var = ast.unparse(node.args[0])[:80]
            except Exception:
                dynamic_var = "<unknown>"
            field_name = f"<dynamic:{dynamic_var}>"

        # Get default value if present
        default_val = None
        if len(node.args) > 1:
            try:
                default_val = repr(ast.literal_eval(node.args[1]))
            except (ValueError, TypeError):
                try:
                    default_val = ast.unparse(node.args[1])[:100]
                except Exception:
                    pass

        # Determine if the base is a tracked alias
        base_name = None
        source_dict = None
        confidence = "LOW"

        if isinstance(node.func.value, ast.Name):
            base_name = node.func.value.id
        elif isinstance(node.func.value, ast.Attribute):
            try:
                base_name = ast.unparse(node.func.value)
            except Exception:
                return

        if base_name is None:
            return

        # Check if base is a known source dict or alias
        if base_name in self._TRACKED_SOURCE_DICTS:
            source_dict = base_name
            confidence = "HIGH"
        elif base_name in self._dict_aliases:
            source_dict = self._dict_aliases[base_name]
            confidence = "HIGH" if base_name in self._param_aliases else "MEDIUM"
        elif base_name in self._METADATA_PARAM_NAMES:
            source_dict = "FORMATION_METADATA"
            confidence = "MEDIUM"

        if source_dict is None:
            return

        # Determine if this is a nested read
        parent_field = None
        actual_field = field_name
        if "." in source_dict:
            # source_dict is like "FORMATION_METADATA.runner_extension"
            parts = source_dict.split(".", 1)
            source_dict = parts[0]
            parent_field = parts[1]
            actual_field = f"{parent_field}.{field_name}"

        # Dynamic reads get LOW confidence regardless of alias resolution quality
        if is_dynamic:
            confidence = "LOW"

        caller_fqn = self._current_fqn_prefix if self._scope_stack else self.module_ns
        self.field_reads.append(FieldReadRecord(
            source_dict=source_dict,
            field_name=actual_field,
            parent_field=parent_field,
            default_value=default_val,
            lineno=node.lineno,
            access_pattern="get_dynamic" if is_dynamic else "get",
            confidence=confidence,
        ))

    # ── Subscript access: os.environ["KEY"] + dict field reads ──

    def visit_Subscript(self, node: ast.Subscript):
        # Original: os.environ["KEY"] detection
        try:
            base = ast.unparse(node.value)
            if base == "os.environ" and isinstance(node.slice, ast.Constant):
                self.env_vars.append(EnvVarRecord(
                    var_name=str(node.slice.value),
                    lineno=node.lineno,
                    default_value=None,
                    access_pattern="os.environ[]",
                    context_snippet=self._get_context_snippet(node.lineno),
                ))
        except Exception:
            pass

        # New: dict field read detection via subscript
        self._check_subscript_field_read(node)
        self.generic_visit(node)

    def _check_subscript_field_read(self, node: ast.Subscript):
        """Detect DICT["field"] or alias["field"] patterns.

        Also captures dynamic key accesses like DICT[variable] as
        '<dynamic:variable_name>' for inventory tracking.
        """
        is_dynamic = False
        if isinstance(node.slice, ast.Constant):
            field_name = node.slice.value
            if not isinstance(field_name, str):
                return
        else:
            # Dynamic key — capture the variable expression
            is_dynamic = True
            try:
                dynamic_var = ast.unparse(node.slice)[:80]
            except Exception:
                dynamic_var = "<unknown>"
            field_name = f"<dynamic:{dynamic_var}>"

        # Determine the base variable
        base_name = None
        source_dict = None
        confidence = "LOW"
        parent_field = None

        if isinstance(node.value, ast.Name):
            base_name = node.value.id
        elif isinstance(node.value, ast.Subscript):
            # Nested subscript: DICT["X"]["field"] — the outer subscript is the parent
            # Check if inner is a tracked dict
            try:
                inner_base = ast.unparse(node.value.value) if isinstance(node.value.value, ast.Name) else None
            except Exception:
                inner_base = None
            if inner_base and inner_base in self._TRACKED_SOURCE_DICTS:
                # This is TRACKED_DICT["X"]["field"] or TRACKED_DICT["X"][variable]
                source_dict = inner_base
                confidence = "LOW" if is_dynamic else "HIGH"
                caller_fqn = self._current_fqn_prefix if self._scope_stack else self.module_ns
                self.field_reads.append(FieldReadRecord(
                    source_dict=source_dict,
                    field_name=field_name,
                    parent_field=None,
                    default_value=None,
                    lineno=node.lineno,
                    access_pattern="subscript_dynamic" if is_dynamic else "subscript",
                    confidence=confidence,
                ))
                return
            # Check if inner is a tracked alias with a field subscript
            # e.g., fp["runner_extension"]["enabled"]
            if isinstance(node.value.value, ast.Name):
                alias_base = node.value.value.id
                if alias_base in self._dict_aliases:
                    parent_source = self._dict_aliases[alias_base]
                    if isinstance(node.value.slice, ast.Constant) and isinstance(node.value.slice.value, str):
                        parent_key = node.value.slice.value
                        actual_source = parent_source.split(".")[0] if "." in parent_source else parent_source
                        # This is alias["parent_key"]["field"] or alias["parent_key"][variable]
                        caller_fqn = self._current_fqn_prefix if self._scope_stack else self.module_ns
                        self.field_reads.append(FieldReadRecord(
                            source_dict=actual_source,
                            field_name=f"{parent_key}.{field_name}",
                            parent_field=parent_key,
                            default_value=None,
                            lineno=node.lineno,
                            access_pattern="subscript_dynamic" if is_dynamic else "subscript",
                            confidence="LOW" if is_dynamic else "MEDIUM",
                        ))
                        return
            return

        if base_name is None:
            return

        # Check if base is a known source dict or alias
        if base_name in self._TRACKED_SOURCE_DICTS:
            source_dict = base_name
            confidence = "HIGH"
        elif base_name in self._dict_aliases:
            source_dict = self._dict_aliases[base_name]
            confidence = "HIGH" if base_name not in self._METADATA_PARAM_NAMES else "MEDIUM"
        elif base_name in self._METADATA_PARAM_NAMES:
            source_dict = "FORMATION_METADATA"
            confidence = "MEDIUM"

        if source_dict is None:
            return

        # Handle dotted source_dict (nested alias)
        if "." in source_dict:
            parts = source_dict.split(".", 1)
            source_dict = parts[0]
            parent_field = parts[1]
            field_name = f"{parent_field}.{field_name}"

        # Dynamic reads get LOW confidence regardless of alias resolution quality
        if is_dynamic:
            confidence = "LOW"

        caller_fqn = self._current_fqn_prefix if self._scope_stack else self.module_ns
        self.field_reads.append(FieldReadRecord(
            source_dict=source_dict,
            field_name=field_name,
            parent_field=parent_field,
            default_value=None,
            lineno=node.lineno,
            access_pattern="subscript_dynamic" if is_dynamic else "subscript",
            confidence=confidence,
        ))


# ── Public API ───────────────────────────────────────────

def analyze_file(filepath: Path, root: Path) -> ModuleRecord:
    """Parse a single Python file and return all extracted data."""
    rel = filepath.relative_to(root)
    relpath_no_ext = str(rel.with_suffix("")).replace("\\", "/")
    module_ns = relpath_no_ext.replace("/", ".")

    stat = filepath.stat()
    try:
        raw = filepath.read_bytes()
        sha1 = hashlib.sha1(raw).hexdigest()
        source = raw.decode("utf-8", errors="replace")
    except OSError as e:
        return ModuleRecord(
            path=str(rel).replace("\\", "/"),
            relpath_no_ext=relpath_no_ext,
            loc=0, sha1="", mtime=stat.st_mtime, size=0,
            parse_error=str(e),
        )

    lines = source.splitlines()
    loc = len(lines)

    try:
        tree = ast.parse(source, filename=str(filepath))
    except SyntaxError as e:
        return ModuleRecord(
            path=str(rel).replace("\\", "/"),
            relpath_no_ext=relpath_no_ext,
            loc=loc, sha1=sha1, mtime=stat.st_mtime, size=stat.st_size,
            parse_error=f"SyntaxError: {e.msg} (line {e.lineno})",
        )

    visitor = CodeVisitor(module_ns, lines)
    visitor.visit(tree)

    # Compute is_exported for symbols
    has_all = visitor.all_exports is not None
    for sym in visitor.symbols:
        if has_all:
            sym.is_exported = sym.name in visitor.all_exports
        else:
            sym.is_exported = (
                not sym.name.startswith("_") and sym.scope_depth == 0
            )

    return ModuleRecord(
        path=str(rel).replace("\\", "/"),
        relpath_no_ext=relpath_no_ext,
        loc=loc,
        sha1=sha1,
        mtime=stat.st_mtime,
        size=stat.st_size,
        all_exports=visitor.all_exports,
        symbols=visitor.symbols,
        imports=visitor.imports,
        calls=visitor.calls,
        registries=visitor.registries,
        constants=visitor.constants,
        env_vars=visitor.env_vars,
        field_reads=visitor.field_reads,
    )
