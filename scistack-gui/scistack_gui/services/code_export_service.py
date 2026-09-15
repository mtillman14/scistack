"""
Translate a GUI pipeline into a standalone script (to-do #6,
plan-pipeline-to-code-export.md).

Never exports data/records — the generated script RECOMPUTES them by
running the same wiring against the same database. Language is detected
per the closure's function nodes: all-Python -> .py, all-MATLAB -> native
.m, mixed -> unsupported (explicit error; see plan doc for why this is a
generator-scope limitation, not an execution-level one).

Python generation reuses execution_service.build_backend_pipeline's
compiled scidb.Pipeline directly (Pipeline._composed_steps/_topo_order —
the same methods Pipeline.plan() itself calls internally). MATLAB
generation cannot do that (registry.get_function, which
build_backend_pipeline needs for a live callable, only ever resolves
Python functions) — but the underlying target/input RESOLUTION
(derive_target_for_node, apply_pending_overrides, filter_hidden_targets,
build_run_inputs) turns out to already be language-agnostic (see plan
doc), so the MATLAB path reuses all of that and only adds its own driving
loop, its own type-level topological sort, and a MATLAB-syntax
serializer for the same resolved values.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

EXPORT_DIRNAME = "exports"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def export_pipeline_to_code(db, pipeline_id: str) -> dict:
    """Detect language across the closure and generate a standalone
    script. Raises ValueError for a mixed-language closure (unsupported —
    see module docstring)."""
    from scistack_gui import matlab_registry
    from scistack_gui.services.pipeline_service import get_pipeline_graph
    from scistack_gui.services.portability_service import _closure_pipeline_ids

    pipeline_ids = _closure_pipeline_ids(db, pipeline_id)

    fn_labels: set[str] = set()
    for pid in pipeline_ids:
        graph = get_pipeline_graph(db, pid)
        for node in graph["nodes"]:
            if node["type"] == "functionNode":
                fn_labels.add(node["data"]["label"])

    matlab_fns = {f for f in fn_labels if matlab_registry.is_matlab_function(f)}
    python_fns = fn_labels - matlab_fns

    if python_fns and matlab_fns:
        raise ValueError(
            "mixed-language pipelines aren't supported by code export yet "
            f"— Python function(s): {sorted(python_fns)}; "
            f"MATLAB function(s): {sorted(matlab_fns)}"
        )

    language = "matlab" if matlab_fns else "python"
    if language == "python":
        script, warnings = _generate_python_script(db, pipeline_id, pipeline_ids)
        ext = "py"
    else:
        script, warnings = _generate_matlab_script(db, pipeline_id, pipeline_ids)
        ext = "m"

    import re
    from pathlib import Path

    from scistack_gui import pipeline_store as ps

    names_by_id = {p["pipeline_id"]: p["name"] for p in ps.list_pipelines(db)}
    root_name = names_by_id.get(pipeline_id, pipeline_id)
    safe_name = re.sub(r"[^\w.-]+", "_", root_name).strip("_") or "pipeline"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    out_dir = Path(str(db.dataset_db_path)).resolve().parent / EXPORT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{safe_name}_{timestamp}.{ext}"
    out_path.write_text(script)

    logger.info(
        "[code_export] export_pipeline_to_code(%s): language=%s, %d warning(s) -> %s",
        pipeline_id, language, len(warnings), out_path,
    )
    return {
        "path": str(out_path),
        "language": language,
        "script": script,
        "warnings": warnings,
    }


# ---------------------------------------------------------------------------
# Shared: disconnected-wiring skip comments
# ---------------------------------------------------------------------------


def _skip_comments(db, pipeline_id: str, comment_prefix: str) -> tuple[list[str], list[str]]:
    """[comment_lines], [warning strings] for wirings that won't run
    because a required input is disconnected — reuses
    execution_service.disconnected_report_entries verbatim (language-
    agnostic: driven by hidden edges, not function language), per the
    warn-comment recommendation in gui-export-to-plain-python.md."""
    from scistack_gui.services.execution_service import disconnected_report_entries

    lines: list[str] = []
    warnings: list[str] = []
    for skipped in disconnected_report_entries(db, pipeline_id):
        reason = skipped.get("skip_reason", "disconnected wiring")
        lines.append(f"{comment_prefix} SKIPPED: '{skipped['step']}' — {reason}")
        warnings.append(f"{skipped['step']}: {reason}")
    return lines, warnings


# ---------------------------------------------------------------------------
# Python generator
# ---------------------------------------------------------------------------


def _each_of_cls():
    """``scifor.EachOf`` itself. Imported lazily for the same reason
    :func:`_column_selection_parts` is: this module is imported at server
    start, before a project's registry exists."""
    from scifor import EachOf

    return EachOf


def _column_selection_parts(value):
    """``(inner, columns, iterate)`` for a ``scifor.ColumnSelection``, or
    ``None`` for anything else.

    Matched against the SCIFOR base class, not scidb's subclass: a bare
    ``scifor.ColumnSelection`` is a legal for_each input too, and every other
    isinstance check in this stack is written the same way (see
    docs/claude/column-selection.md).
    """
    try:
        from scifor import ColumnSelection
    except ImportError:  # pragma: no cover - scifor is a hard dependency
        return None
    if not isinstance(value, ColumnSelection):
        return None
    return value.data, list(value.columns or []), bool(value.iterate)


def _py_literal(value) -> str:
    """A bare class (BaseVariable subclass) -> its name; everything else
    -> repr(). EachOf/PathInput's own __repr__ already produce valid,
    readable Python constructor syntax (recursively, via this same rule),
    so no per-type branching is needed here — see plan doc.

    ``ColumnSelection`` is the exception that does need a branch: its
    ``__repr__`` is the default ``<...ColumnSelection object at 0x...>``, so
    without this the exported script renders an object address where a
    subscript belongs — a file that looks fine until it is run.
    """
    parts = _column_selection_parts(value)
    if parts is not None:
        inner, columns, iterate = parts
        base = _py_literal(inner)
        if iterate:
            if columns:
                return f"{base}.for_columns({columns!r})"
            return f"{base}.for_columns()"
        if len(columns) == 1:
            return f"{base}[{columns[0]!r}]"
        return f"{base}[{columns!r}]"
    if isinstance(value, type):
        return value.__name__
    if type(value) is _each_of_cls():
        # A BARE EachOf only -- deliberately `type(...) is`, not isinstance.
        # `scidb.Parameter` IS an EachOf, and its own __repr__ spells
        # `Parameter(10, 20, description='')`, which is both the right
        # constructor and what the generated header imports. Catching
        # subclasses here rendered every exported Parameter as `EachOf(...)`,
        # losing the description and the type.
        #
        # The bare case needs the branch because EachOf.__repr__ renders each
        # alternative via ``__name__`` -- a DISPLAY name: ColumnSelection's
        # spells a multi-column iterate selection ``Trials["a", "b",
        # iterate]``, which is not Python. `build_run_inputs` produces exactly
        # this shape for a multi-type binding with a column selection.
        items = ", ".join(_py_literal(a) for a in value.alternatives)
        return f"EachOf({items})"
    return repr(value)


def _py_dict_literal(d: dict) -> str:
    if not d:
        return "{}"
    parts = [f"{k!r}: {_py_literal(v)}" for k, v in d.items()]
    return "{" + ", ".join(parts) + "}"


def _glue_comments(glue: "dict | None", comment_prefix: str) -> list[str]:
    """One comment line per glued parameter, above the step it belongs to.

    Glue appears **inline with the consuming step, never as a step of its
    own** — it has no saved output and is not a node in the compiled
    pipeline. The comment makes that visible in the exported script: the
    reader sees which reshaping happens on the way into this call, without
    a phantom ``for_each`` that produces nothing.
    """
    if not glue:
        return []
    out = []
    for param in sorted(glue):
        names = ", ".join(getattr(s, "name", str(s)) for s in glue[param])
        out.append(
            f"{comment_prefix} '{param}' is reshaped in memory by {names} "
            f"(glue: not saved, fused into this call)"
        )
    return out


def _py_glue_literal(glue: "dict | None") -> str:
    """``{'emg': [glue_drop_baseline]}`` — bare function names.

    The glue functions come into scope through the header's
    ``from <module> import *``, exactly as the pipeline functions do: a glue
    node is an ordinary named function in an ordinary file.

    Note what is NOT emitted: a bare ``df = glue_x(df)`` statement before the
    call. There is no ``df`` in scope at that point — the table only exists
    inside ``for_each``'s load step — so such a script would neither run nor
    reproduce the records it was exported from, which is the contract the
    export exists to keep. ``glue=`` is the faithful spelling of the same
    fusion.
    """
    if not glue:
        return ""
    parts = [
        f"{param!r}: [" + ", ".join(getattr(s, "name", str(s)) for s in chain) + "]"
        for param, chain in sorted(glue.items())
    ]
    return "{" + ", ".join(parts) + "}"


def _py_header(db) -> str:
    from scistack_gui import registry

    lines = [
        '"""',
        "Standalone export of a SciStack GUI pipeline.",
        "Generated by scistack-gui's pipeline-to-code export (to-do #6).",
        "Data/records are NOT included — running this script recomputes",
        "them against the same database (adjust the import/db path below",
        "if this file has been moved).",
        '"""',
        "",
        "from scidb import EachOf, Parameter, PathInput, configure_database, for_each",
        "",
    ]

    module_paths = []
    if registry._config is not None and registry._config.modules:
        module_paths = list(registry._config.modules)
    elif registry._module_path is not None:
        module_paths = [registry._module_path]

    if module_paths:
        lines.append("import sys")
        for p in module_paths:
            lines.append(f"sys.path.insert(0, {str(p.parent)!r})")
        for p in module_paths:
            lines.append(f"from {p.stem} import *  # noqa: F401,F403")
    else:
        lines.append("# TODO: import your own module (functions/variables) here")
    lines.append("")

    db_path = str(db.dataset_db_path)
    schema_keys = list(db.dataset_schema_keys)
    lines.append(f"configure_database({db_path!r}, {schema_keys!r})")
    return "\n".join(lines)


def _generate_python_script(db, pipeline_id: str, pipeline_ids: list) -> tuple[str, list[str]]:
    from scistack_gui.services.execution_service import (
        _discard_compiled,
        build_backend_pipeline,
    )

    built: dict = {}
    try:
        pipe = build_backend_pipeline(db, pipeline_id, built)
        pairs = pipe._composed_steps()
        order = pipe._topo_order(pairs)
        steps = [pairs[i][1] for i in order]  # StepSpec, topological order
    finally:
        _discard_compiled(built)

    lines = [_py_header(db), ""]
    for spec in steps:
        fn_name = getattr(spec.fn, "__name__", repr(spec.fn))
        outputs = [o for o in spec.outputs if isinstance(o, type)]
        output_names = [o.__name__ for o in outputs]
        inputs_src = _py_dict_literal(spec.inputs)
        outputs_src = "[" + ", ".join(output_names) + "]"
        iterables_src = _py_dict_literal(spec.metadata_iterables)
        glue_src = _py_glue_literal(spec.options.get("glue"))
        lines.append(f"# {fn_name} -> {', '.join(output_names)}")
        for comment in _glue_comments(spec.options.get("glue"), "#"):
            lines.append(comment)
        call = f"for_each({fn_name}, {inputs_src}, {outputs_src}"
        if glue_src:
            call += f", glue={glue_src}"
        if spec.metadata_iterables:
            call += f", **{iterables_src}"
        call += ")"
        lines.append(call)
        lines.append("")

    skip_lines, warnings = _skip_comments(db, pipeline_id, "#")
    lines.extend(skip_lines)

    return "\n".join(lines).rstrip() + "\n", warnings


# ---------------------------------------------------------------------------
# MATLAB generator
# ---------------------------------------------------------------------------


def _matlab_str(s) -> str:
    return '"' + str(s).replace('"', '""') + '"'


def _matlab_literal(value) -> str:
    """Same resolved values build_run_inputs already returns (classes,
    constants, EachOf, PathInput, ColumnSelection) — MATLAB syntax instead of
    Python's.

    MATLAB has no ``ColumnSelection`` wrapper: the columns go to the
    ``BaseVariable`` constructor and ``iterate`` comes from ``for_columns``
    (``+scidb/BaseVariable.m``), so the four spellings match
    ``api.matlab_command._format_variable_class`` exactly — two renderers, one
    syntax, and a test in each pins it.
    """
    parts = _column_selection_parts(value)
    if parts is not None:
        inner, columns, iterate = parts
        name = inner.__name__ if isinstance(inner, type) else str(inner)
        if iterate:
            cols = (
                "[" + ", ".join(_matlab_str(c) for c in columns) + "]"
                if columns
                else ""
            )
            return f"{name}().for_columns({cols})"
        if len(columns) == 1:
            return f"{name}({_matlab_str(columns[0])})"
        return f"{name}([" + ", ".join(_matlab_str(c) for c in columns) + "])"
    if isinstance(value, type):
        return f"{value.__name__}()"  # MATLAB convention: constructed instance
    alternatives = getattr(value, "alternatives", None)
    if alternatives is not None:  # scifor.EachOf
        items = ", ".join(_matlab_literal(a) for a in alternatives)
        return f"scifor.EachOf({items})"
    if type(value).__name__ == "PathInput":
        template = getattr(value, "path_template", "")
        root_folder = getattr(value, "root_folder", None)
        if root_folder is not None:
            return f"scifor.PathInput({_matlab_str(template)}, 'root_folder', {_matlab_str(root_folder)})"
        return f"scifor.PathInput({_matlab_str(template)})"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return _matlab_str(value)
    return repr(value)  # numbers


def _matlab_struct(inputs: dict) -> str:
    if not inputs:
        return "struct()"
    parts = [f"'{k}', {_matlab_literal(v)}" for k, v in inputs.items()]
    return "struct(" + ", ".join(parts) + ")"


def _matlab_glue_struct(glue: "dict | None") -> str:
    """``struct('emg', {{@glue_drop_baseline}})`` — handles, applied in order.

    The doubled braces are not a typo: ``struct('f', {c})`` with a cell value
    would build a struct ARRAY, one element per cell entry, so a two-node
    chain would silently become two structs. Wrapping once more makes the
    field hold the cell itself, which is what ``+scidb/for_each.m``'s glue
    option expects.
    """
    if not glue:
        return ""
    parts = []
    for param, chain in sorted(glue.items()):
        handles = ", ".join(f"@{getattr(s, 'name', str(s))}" for s in chain)
        parts.append(f"'{param}', {{{{{handles}}}}}")
    return "struct(" + ", ".join(parts) + ")"


def _matlab_iteration_args(schema_iterables: dict) -> str:
    parts = []
    for key, values in schema_iterables.items():
        arr = "[" + ", ".join(_matlab_str(v) for v in values) + "]"
        parts.append(f"'{key}', {arr}")
    return ", ".join(parts)


def _matlab_header(db) -> str:
    lines = [
        "% Standalone export of a SciStack GUI pipeline.",
        "% Generated by scistack-gui's pipeline-to-code export (to-do #6).",
        "% Data/records are NOT included — running this script recomputes",
        "% them against the same database (adjust the addpath/db path",
        "% below if this file has been moved).",
        "",
    ]

    try:
        import scimatlab

        from pathlib import Path

        scimatlab_matlab_dir = Path(scimatlab.__file__).parent / "matlab"
        lines.append(f"addpath({_matlab_str(str(scimatlab_matlab_dir))});")
    except ImportError:
        lines.append("% TODO: addpath('/path/to/scimatlab/matlab')")

    from scistack_gui import matlab_registry

    addpaths = []
    if matlab_registry._config is not None:
        addpaths = list(matlab_registry._config.matlab_addpath)
    if addpaths:
        for p in addpaths:
            lines.append(f"addpath({_matlab_str(str(p))});")
    else:
        lines.append("% TODO: addpath your own .m function/variable files")
    lines.append("")

    db_path = str(db.dataset_db_path)
    schema_keys_src = "[" + ", ".join(_matlab_str(k) for k in db.dataset_schema_keys) + "]"
    lines.append(f"scidb.configure_database({_matlab_str(db_path)}, {schema_keys_src});")
    return "\n".join(lines)


def _matlab_steps(db, pipeline_ids: list) -> list:
    """[(fn_label, target), ...] across the whole closure — mirrors
    build_backend_pipeline's own per-node loop body (derive_target_for_node
    + apply_pending_overrides + filter_hidden_targets + dedup), minus the
    for_each(...) call, since there's no compiled Pipeline to walk for
    MATLAB (registry.get_function only resolves Python callables)."""
    from scistack_gui import pipeline_store as ps
    from scistack_gui.domain.variant_resolver import (
        filter_hidden_targets,
        hidden_call_ids_for_fn,
    )
    from scistack_gui.services.execution_service import (
        _scope_function_node_ids,
        apply_pending_overrides,
        derive_target_for_node,
    )

    pending_consts = ps.get_pending_constants(db)
    hidden_ids = ps.get_hidden_node_ids(db)

    steps: list = []
    for pid in pipeline_ids:
        for node_id, fn_label in _scope_function_node_ids(db, pid):
            targets = apply_pending_overrides(
                derive_target_for_node(db, node_id), pending_consts
            )
            targets = filter_hidden_targets(
                targets, fn_label, hidden_call_ids_for_fn(hidden_ids, fn_label),
                pending_consts, distribute=False, as_table=None,
            )
            seen_keys: set = set()
            for target in targets:
                key = (tuple(sorted(target["constants"].items())), target["output_type"])
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                steps.append((fn_label, target))
    return steps


def _topo_sort_targets(steps: list) -> list:
    """Kahn's algorithm at the input/output-TYPE level — the MATLAB-side
    equivalent of scidb.Pipeline._topo_order, since there's no compiled
    Pipeline object to ask (see module docstring). Raises ValueError with
    the stuck steps' function names on a cycle."""
    def consumed_types(target: dict) -> set:
        types: set = set()
        for v in target["input_types"].values():
            if isinstance(v, list):
                types.update(v)
            else:
                types.add(v)
        return types

    producers: dict = {}
    for i, (_fn, target) in enumerate(steps):
        producers.setdefault(target["output_type"], []).append(i)

    deps = {i: set() for i in range(len(steps))}
    for i, (_fn, target) in enumerate(steps):
        for t in consumed_types(target):
            for p in producers.get(t, []):
                if p != i:
                    deps[i].add(p)

    order: list = []
    done: set = set()
    remaining = list(range(len(steps)))
    while remaining:
        ready = [i for i in remaining if deps[i] <= done]
        if not ready:
            stuck = [steps[i][0] for i in remaining]
            raise ValueError(f"dependency cycle detected among steps: {stuck}")
        order.extend(ready)
        done.update(ready)
        remaining = [i for i in remaining if i not in done]
    return order


def _generate_matlab_script(db, pipeline_id: str, pipeline_ids: list) -> tuple[str, list[str]]:
    from scistack_gui import registry
    from scistack_gui.services.execution_service import (
        build_run_glue,
        build_run_inputs,
    )

    steps = _matlab_steps(db, pipeline_ids)
    order = _topo_sort_targets(steps)

    schema_iterables = {
        key: db.distinct_schema_values(key) for key in db.dataset_schema_keys
    }
    iter_args = _matlab_iteration_args(schema_iterables)

    lines = [_matlab_header(db), ""]
    for i in order:
        fn_label, target = steps[i]
        inputs = build_run_inputs(target, fn_label, db)
        glue = build_run_glue(target, fn_label)
        output_cls = registry.get_variable_class(target["output_type"])
        inputs_src = _matlab_struct(inputs)
        outputs_src = "{" + _matlab_literal(output_cls) + "}"
        call = f"scidb.for_each(@{fn_label}, {inputs_src}, {outputs_src}"
        glue_src = _matlab_glue_struct(glue)
        if glue_src:
            call += f", 'glue', {glue_src}"
        if iter_args:
            call += f", {iter_args}"
        call += ");"
        lines.append(f"% {fn_label} -> {target['output_type']}")
        for comment in _glue_comments(glue, "%"):
            lines.append(comment)
        lines.append(call)
        lines.append("")

    skip_lines, warnings = _skip_comments(db, pipeline_id, "%")
    lines.extend(skip_lines)

    return "\n".join(lines).rstrip() + "\n", warnings
