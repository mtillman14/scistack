"""Pure renderers: Inspector dataclasses → terminal text.

No database access here — every function takes facade result objects and
returns a string. This keeps renderers unit-testable against hand-built
fixtures (golden-file friendly) and lets the CLI/GUI share the facade.

**Render style seam:** every presentation constant (tree glyphs, state-tag
wording, count formats, diagram colors) lives in `RenderStyle`; renderers
must not hardcode presentation literals. Tuning the output = editing
`DEFAULT_STYLE` (or passing another instance) — renderer logic and its
callers never change. Phase 3+ renderers must consume the same RenderStyle,
adding fields for any new presentation need.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

from .api import (
    DbOverview,
    ExclusionRecord,
    IntentReport,
    NodeStateSummary,
    PickCandidate,
    ProvenanceTree,
    RecordSummary,
    RunRecord,
    SchemaNode,
    SchemaTree,
    VariableDetail,
    VariableSummary,
)
from .graph import FunctionNode, PipelineGraph, VariantSummary, parse_path_input
from .mutate import MutationResult


@dataclass
class RenderStyle:
    """All presentation constants for the text/diagram renderers.

    Field defaults ARE the product's look — change them here (or pass a
    custom instance / preset) to retune every renderer at once.
    """

    # -- tree glyphs --------------------------------------------------------
    var_bullet: str = "● "  # top-level (root/isolated) variable
    branch_mid: str = "├─ "  # function under a variable, not last
    branch_last: str = "└─ "
    cont_mid: str = "│  "  # continuation prefixes under the above
    cont_last: str = "   "
    out_branch_mid: str = "├─▶ "  # output variable under a function
    out_branch_last: str = "└─▶ "
    out_cont_mid: str = "│   "
    out_cont_last: str = "    "
    back_ref_suffix: str = "  (↑ shown above)"  # repeated variable node
    back_ref_fn_fmt: str = "{name} (↑ shown above)"  # repeated function node
    input_marker: str = "◀"  # "param ◀ Type" lines (--values)
    variant_bullet: str = "· "  # per-variant lines (--variants)

    # -- suffix / label formats --------------------------------------------
    records_suffix_fmt: str = "    {n} records"  # pipeline tree nodes
    node_records_suffix_fmt: str = "  {n} records"  # schema tree nodes
    variants_suffix_fmt: str = "  {n} variants"
    variant_records_suffix_fmt: str = "   {n} records"  # --variants lines
    constants_set_fmt: str = "{k} = {{{vals}}}"  # low_hz = {20, 30}
    constants_single_fmt: str = "{k} = {v}"
    no_constants_label: str = "(no constants)"

    # -- state-tag wording ---------------------------------------------------
    tag_fmt: str = "[{tag}]"
    tag_unknown: str = "[state unknown]"
    stored_hash_note: str = ", last-run recipe"  # basis caveat on the tag
    missing_note_fmt: str = " — {missing}/{total} combos missing"

    # -- trace / state (Phase 3) ---------------------------------------------
    record_id_fmt: str = "record {rid}"
    id_abbrev_len: int = 8
    id_ellipsis: str = "…"
    label_sep: str = "    "  # between segments of a record/fn line
    saved_fmt: str = "saved {ts} by {user}"
    saved_no_user_fmt: str = "saved {ts}"
    fn_hash_fmt: str = "fn_hash {h}"
    run_note_fmt: str = "(run {n}×, last {ts})"
    raw_tag: str = "  (raw save)"
    # -- down to the run (`trace --runs`) ------------------------------------
    code_version_fmt: str = "code {v}"
    call_id_fmt: str = "call {cid}"
    invocation_fmt: str = "invocation {inv}"
    run_options_fmt: str = "run options: {opts}"
    run_line_fmt: str = "run {rid}  {ts}"
    run_user_fmt: str = "  by {user}"
    run_where_fmt: str = "  where {where}"
    run_variant_fmt: str = "  [{variant}]"  # per-run inv/hash when they differ
    reproduced_note_fmt: str = "  ({n} producing invocations)"
    no_runs_label: str = "(no tracked run)"
    const_join: str = "  "  # between k=v pairs on a constants line
    missing_line_fmt: str = "    missing  {combo}"
    missing_more_fmt: str = "    … +{n} more"
    missing_display_cap: int = 25

    # -- schema locations (the location picker's CLI half) -------------------
    # One mark per state. Amber and grey have no equivalent anywhere else in
    # this file because node state is binary; a location is not.
    loc_mark_green: str = "●"
    loc_mark_amber: str = "◐"
    loc_mark_red: str = "○"
    loc_mark_grey: str = "·"
    loc_counts_fmt: str = "  {green}/{total}"
    loc_version_fmt: str = "  [{version}]"
    loc_excluded_label: str = "  (excluded)"
    loc_header_fmt: str = "{variable}: {green}/{total} locations green"
    loc_variant_fmt: str = "  variant: {variant}"
    loc_basis_fmt: str = "  denominator: {basis}"
    loc_depth_more_fmt: str = "{prefix}… +{n} deeper"

    # -- terminal colors (empty = no color; see with_ansi_colors) -----------
    color_green: str = ""
    color_amber: str = ""
    color_red: str = ""
    color_unknown: str = ""
    color_reset: str = ""

    # -- diagram colors ------------------------------------------------------
    mermaid_green: str = "fill:#c9f2cf,stroke:#2e7d32"
    mermaid_red: str = "fill:#f8cdc9,stroke:#c62828"
    mermaid_unknown: str = "fill:#e8e8e8,stroke:#757575,stroke-dasharray: 4 3"
    dot_green: str = "#2e7d32"
    dot_red: str = "#c62828"
    dot_unknown: str = "#757575"


DEFAULT_STYLE = RenderStyle()

# Preset for terminals without Unicode box-drawing support; also the proof
# that the style seam works (test-covered).
ASCII_STYLE = RenderStyle(
    var_bullet="* ",
    branch_mid="+- ",
    branch_last="`- ",
    cont_mid="|  ",
    cont_last="   ",
    out_branch_mid="+-> ",
    out_branch_last="`-> ",
    out_cont_mid="|   ",
    out_cont_last="    ",
    back_ref_suffix="  (^ shown above)",
    back_ref_fn_fmt="{name} (^ shown above)",
    input_marker="<-",
    variant_bullet="- ",
    missing_note_fmt=" - {missing}/{total} combos missing",
    id_ellipsis="...",
    run_note_fmt="(run {n}x, last {ts})",
    missing_more_fmt="    ... +{n} more",
    loc_mark_green="+",
    loc_mark_amber="~",
    loc_mark_red="-",
    loc_mark_grey=".",
    loc_depth_more_fmt="{prefix}... +{n} deeper",
)


def with_ansi_colors(style: RenderStyle) -> RenderStyle:
    """A copy of ``style`` with ANSI state colors enabled (TTY output)."""
    return replace(
        style,
        color_green="\x1b[32m",
        color_amber="\x1b[33m",
        color_red="\x1b[31m",
        color_unknown="\x1b[90m",
        color_reset="\x1b[0m",
    )


def format_table(headers: list[str], rows: list[list]) -> str:
    """Plain ASCII table. None renders as empty; everything else via str()."""
    cells = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [len(h) for h in headers]
    for row in cells:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = [
        "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers)).rstrip(),
        "  ".join("-" * w for w in widths),
    ]
    for row in cells:
        lines.append("  ".join(c.ljust(widths[i]) for i, c in enumerate(row)).rstrip())
    return "\n".join(lines)


def _human_size(n_bytes: int) -> str:
    size = float(n_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024


def render_overview(o: DbOverview) -> str:
    lines = [
        f"database          {o.db_path}  ({_human_size(o.db_size_bytes)})",
        f"schema keys       {', '.join(o.schema_keys)}",
        f"schema locations  {o.n_schema_locations}",
        f"variables         {o.n_variables}",
        f"records           {o.n_records}"
        + (f"  ({o.n_excluded_records} excluded)" if o.n_excluded_records else ""),
        f"invocations       {o.n_invocations}",
        f"runs              {o.n_runs}",
        f"last save         {o.last_save or '-'}",
        f"last run          {o.last_run or '-'}",
    ]
    return "\n".join(lines)


def render_variables(variables: list[VariableSummary]) -> str:
    if not variables:
        return "(no variables registered)"
    return format_table(
        ["variable", "level", "records", "variants", "last saved", "dtype"],
        [
            [
                v.name,
                v.schema_level,
                v.record_count
                if not v.excluded_count
                else f"{v.record_count} (+{v.excluded_count} excl)",
                v.variant_count,
                v.last_saved,
                (v.dtype or "")[:40],
            ]
            for v in variables
        ],
    )


def render_variable_detail(v: VariableDetail) -> str:
    lines = [
        f"variable       {v.name}",
        f"schema level   {v.schema_level or '-'}",
        f"dtype          {v.dtype or '-'}",
        f"description    {v.description or '-'}",
        f"records        {v.record_count}"
        + (f"  ({v.excluded_count} excluded)" if v.excluded_count else ""),
        f"variants       {v.variant_count}",
        f"last saved     {v.last_saved or '-'}",
        f"data columns   {', '.join(v.data_columns) or '-'}",
    ]
    if v.records_by_level:
        lines.append("records by schema level:")
        for level, n in sorted(v.records_by_level.items()):
            lines.append(f"  {level:<12} {n}")
    return "\n".join(lines)


def render_schema_summary(tree: SchemaTree) -> str:
    """Per-level rollup: how many realized locations and records at each level."""
    locations: dict[str, int] = {}
    records: dict[str, int] = {}

    def walk(node: SchemaNode):
        if node.schema_id is not None and node.schema_level is not None:
            locations[node.schema_level] = locations.get(node.schema_level, 0) + 1
            records[node.schema_level] = (
                records.get(node.schema_level, 0) + node.record_count
            )
        for c in node.children:
            walk(c)

    for root in tree.roots:
        walk(root)

    lines = [f"schema keys: {', '.join(tree.schema_keys)}"]
    if locations:
        # Report in hierarchy order (levels are schema key names).
        ordered = [k for k in tree.schema_keys if k in locations]
        ordered += [k for k in locations if k not in tree.schema_keys]
        lines.append(
            format_table(
                ["level", "locations", "records"],
                [[lvl, locations[lvl], records[lvl]] for lvl in ordered],
            )
        )
    else:
        lines.append("(no schema locations realized yet)")
    return "\n".join(lines)


def render_schema_tree(tree: SchemaTree, style: RenderStyle | None = None) -> str:
    s = style or DEFAULT_STYLE
    lines = [f"schema keys: {', '.join(tree.schema_keys)}"]

    def walk(node: SchemaNode, prefix: str, is_last: bool):
        branch = s.branch_last if is_last else s.branch_mid
        count = (
            s.node_records_suffix_fmt.format(n=node.record_count)
            if node.record_count
            else ""
        )
        lines.append(f"{prefix}{branch}{node.key}={node.value}{count}")
        child_prefix = prefix + (s.cont_last if is_last else s.cont_mid)
        for i, c in enumerate(node.children):
            walk(c, child_prefix, i == len(node.children) - 1)

    if not tree.roots:
        lines.append("(no schema locations realized yet)")
    for i, root in enumerate(tree.roots):
        walk(root, "", i == len(tree.roots) - 1)
    return "\n".join(lines)


def _loc_mark(state: str, s: RenderStyle) -> str:
    """The coloured dot for one location state."""
    mark = {
        "green": s.loc_mark_green,
        "amber": s.loc_mark_amber,
        "red": s.loc_mark_red,
        "grey": s.loc_mark_grey,
    }.get(state, s.loc_mark_grey)
    color = {
        "green": s.color_green,
        "amber": s.color_amber,
        "red": s.color_red,
        "grey": s.color_unknown,
    }.get(state, s.color_unknown)
    return f"{color}{mark}{s.color_reset}" if color else mark


def render_location_tree(tree, style: RenderStyle | None = None, depth: int = 0) -> str:
    """The ``scidb locations`` view: a status dot and roll-up per node.

    Counts are shown on interior nodes only. At a leaf they would always read
    ``1/1`` or ``0/1``, which is what the dot already says — the same reason the
    GUI pane omits them at the deepest level.

    ``depth`` caps how far down the tree prints (0 = no cap). A trial-level
    study is thousands of lines otherwise, and the interesting number is almost
    always the roll-up at the level above.
    """
    s = style or DEFAULT_STYLE
    lines = [
        _loc_mark(tree.verdict, s)
        + " "
        + s.loc_header_fmt.format(
            variable=tree.variable, green=tree.green, total=tree.total
        )
    ]
    if tree.counts.get("amber") or tree.counts.get("red") or tree.counts.get("grey"):
        parts = [
            f"{n} {name}"
            for name, n in (
                ("amber", tree.counts.get("amber", 0)),
                ("red", tree.counts.get("red", 0)),
                ("excluded", tree.counts.get("grey", 0)),
            )
            if n
        ]
        lines.append("  " + ", ".join(parts))
    if tree.variant:
        lines.append(
            s.loc_variant_fmt.format(
                variant=", ".join(f"{k}={v}" for k, v in sorted(tree.variant.items()))
            )
        )
    lines.append(s.loc_basis_fmt.format(basis=tree.basis))
    # Notes are caveats about what the count is worth, so they belong beside it
    # rather than at the bottom where a long tree would bury them.
    lines.extend(f"  ! {note}" for note in tree.notes)
    lines.append("")

    def walk(node, prefix: str, is_last: bool, level: int):
        branch = s.branch_last if is_last else s.branch_mid
        label = f"{prefix}{branch}{_loc_mark(node.state, s)} {node.key}={node.value}"
        if node.children:
            label += s.loc_counts_fmt.format(
                green=node.counts.get("green", 0),
                total=sum(node.counts.get(k, 0) for k in ("green", "amber", "red")),
            )
        if node.state == "grey" and not node.children:
            label += s.loc_excluded_label
        if node.code_version:
            label += s.loc_version_fmt.format(version=node.code_version)
        lines.append(label)

        child_prefix = prefix + (s.cont_last if is_last else s.cont_mid)
        if depth and level >= depth:
            if node.children:
                lines.append(
                    s.loc_depth_more_fmt.format(
                        prefix=child_prefix, n=_descendants(node)
                    )
                )
            return
        for i, child in enumerate(node.children):
            walk(child, child_prefix, i == len(node.children) - 1, level + 1)

    if not tree.roots:
        lines.append("(no schema locations)")
    for i, root in enumerate(tree.roots):
        walk(root, "", i == len(tree.roots) - 1, 1)
    return "\n".join(lines)


def _descendants(node) -> int:
    return len(node.children) + sum(_descendants(c) for c in node.children)


def render_variants_table(variants: list[VariantSummary]) -> str:
    if not variants:
        return "(no pipeline variants)"
    # The run-options column appears only when it distinguishes something. In a
    # project that never flipped distribute/as_table every row would read
    # "distribute=false", which is a column of noise; where two rows differ ONLY
    # there, it is the column that explains why they are two rows.
    show_runs = len({v.run_options for v in variants if v.run_options}) > 1
    headers = ["output", "#", "function", "constants", "records", "call_id"]
    if show_runs:
        headers.insert(4, "run options")
    rows = []
    for v in variants:
        row = [
            v.output_type,
            v.output_num,
            v.function_name,
            ", ".join(f"{k}={val}" for k, val in sorted(v.constants.items())) or "-",
            v.record_count,
            v.call_id,
        ]
        if show_runs:
            row.insert(4, v.run_options or "-")
        rows.append(row)
    return format_table(headers, rows)


# ---------------------------------------------------------------------------
# Pipeline renderers
# ---------------------------------------------------------------------------


def _state_tag(state: str, counts: dict, basis: str, s: RenderStyle) -> str:
    """Shared tag wording for pipeline nodes and the state command."""
    if state == "unknown":
        return s.tag_unknown
    color = {"green": s.color_green, "red": s.color_red}.get(state, s.color_unknown)
    tag = f"{color}{state}{s.color_reset}" if color else state
    if state == "red" and counts.get("missing"):
        total = counts.get("up_to_date", 0) + counts["missing"]
        tag += s.missing_note_fmt.format(missing=counts["missing"], total=total)
    if basis == "stored_hash":
        tag += s.stored_hash_note
    return s.tag_fmt.format(tag=tag)


def _fn_state_tag(fn: FunctionNode, s: RenderStyle) -> str:
    return _state_tag(fn.state, fn.state_counts, fn.state_basis, s)


def _fn_label_lines(
    fn: FunctionNode, expand_variants: bool, include_values: bool, s: RenderStyle
) -> list[str]:
    head = f"{fn.function_name}  {_fn_state_tag(fn, s)}"
    if fn.variant_count > 1:
        head += s.variants_suffix_fmt.format(n=fn.variant_count)
    lines = [head]
    if expand_variants:
        seen = set()
        for v in fn.variants:
            const_str = (
                ", ".join(f"{k}={val}" for k, val in sorted(v.constants.items()))
                or s.no_constants_label
            )
            if const_str in seen:
                continue
            seen.add(const_str)
            n = sum(
                x.record_count
                for x in fn.variants
                if sorted(x.constants.items()) == sorted(v.constants.items())
            )
            lines.append(
                f"{s.variant_bullet}{const_str}"
                + s.variant_records_suffix_fmt.format(n=n)
            )
    elif fn.constants:
        lines.append(
            "   ".join(
                s.constants_set_fmt.format(k=k, vals=", ".join(vals))
                if len(vals) > 1
                else s.constants_single_fmt.format(k=k, v=vals[0])
                for k, vals in fn.constants.items()
            )
        )
    if include_values:
        for param, var_type in sorted(fn.input_params.items()):
            lines.append(f"{param} {s.input_marker} {var_type}")
        for param, template in sorted(fn.path_inputs.items()):
            lines.append(f"{param} {s.input_marker} PathInput({template!r})")
    return lines


def render_pipeline_tree(
    graph: PipelineGraph,
    expand_variants: bool = False,
    include_values: bool = False,
    style: RenderStyle | None = None,
) -> str:
    s = style or DEFAULT_STYLE
    if not graph.functions and not graph.variables:
        return "(empty pipeline)"

    fns = {f.id: f for f in graph.functions}
    variables = {v.id: v for v in graph.variables}
    consumers: dict[str, list[str]] = {}
    for e in graph.edges:
        if e.source in variables and e.target in fns:
            consumers.setdefault(e.source, [])
            if e.target not in consumers[e.source]:
                consumers[e.source].append(e.target)

    lines: list[str] = []
    visited_vars: set[str] = set()
    visited_fns: set[str] = set()

    def emit_var(var_id: str) -> None:
        # Top-level roots only; downstream variables are emitted by emit_fn.
        v = variables[var_id]
        count = s.records_suffix_fmt.format(n=v.record_count) if v.record_count else ""
        lines.append(f"{s.var_bullet}{v.name}{count}")
        visited_vars.add(var_id)
        child_fns = consumers.get(var_id, [])
        for i, fid in enumerate(child_fns):
            emit_fn(fid, "", i == len(child_fns) - 1)

    def emit_fn(fid: str, prefix: str, is_last: bool) -> None:
        fn = fns[fid]
        branch = s.branch_last if is_last else s.branch_mid
        cont = s.cont_last if is_last else s.cont_mid
        if fid in visited_fns:
            lines.append(
                prefix + branch + s.back_ref_fn_fmt.format(name=fn.function_name)
            )
            return
        visited_fns.add(fid)
        label_lines = _fn_label_lines(fn, expand_variants, include_values, s)
        lines.append(f"{prefix}{branch}{label_lines[0]}")
        for extra in label_lines[1:]:
            lines.append(f"{prefix}{cont}  {extra}")
        outs = fn.output_types
        for j, out in enumerate(outs):
            out_branch = s.out_branch_last if j == len(outs) - 1 else s.out_branch_mid
            out_cont = s.out_cont_last if j == len(outs) - 1 else s.out_cont_mid
            v = variables[f"var__{out}"]
            count = (
                s.records_suffix_fmt.format(n=v.record_count) if v.record_count else ""
            )
            lines.append(f"{prefix}{cont}{out_branch}{out}{count}")
            if v.id in visited_vars:
                if consumers.get(v.id):
                    lines[-1] += s.back_ref_suffix
                continue
            visited_vars.add(v.id)
            child_fns = consumers.get(v.id, [])
            for i, cfid in enumerate(child_fns):
                emit_fn(cfid, prefix + cont + out_cont, i == len(child_fns) - 1)

    # Roots: source variables (no producer, has consumers), then loader
    # functions with no variable inputs, then anything left unvisited.
    roots = [v.id for v in graph.variables if not v.produced_by and consumers.get(v.id)]
    for var_id in roots:
        emit_var(var_id)
    for fn in graph.functions:
        if not fn.input_params and fn.id not in visited_fns:
            emit_fn(fn.id, "", True)
    for fn in graph.functions:
        if fn.id not in visited_fns:
            emit_fn(fn.id, "", True)

    isolated = [
        v
        for v in graph.variables
        if v.id not in visited_vars and not v.produced_by and not v.consumed_by
    ]
    if isolated:
        lines.append("")
        lines.append("not in any pipeline step:")
        for v in isolated:
            count = (
                s.records_suffix_fmt.format(n=v.record_count) if v.record_count else ""
            )
            lines.append(f"{s.var_bullet}{v.name}{count}")
    return "\n".join(lines)


def _mermaid_id(node_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", node_id)


def render_pipeline_mermaid(
    graph: PipelineGraph, style: RenderStyle | None = None
) -> str:
    s = style or DEFAULT_STYLE
    lines = ["flowchart TD"]
    for v in graph.variables:
        label = f"{v.name}<br/>{v.record_count} records" if v.record_count else v.name
        lines.append(f'    {_mermaid_id(v.id)}(["{label}"])')
    state_classes: dict[str, list[str]] = {"green": [], "red": [], "unknown": []}
    for f in graph.functions:
        label = f.function_name
        if f.variant_count > 1:
            label += f"<br/>{f.variant_count} variants"
        lines.append(f'    {_mermaid_id(f.id)}[["{label}"]]')
        state_classes[f.state].append(_mermaid_id(f.id))
    for e in graph.edges:
        arrow = f"-->|{e.param}|" if e.param else "-->"
        lines.append(f"    {_mermaid_id(e.source)} {arrow} {_mermaid_id(e.target)}")
    lines.append(f"    classDef stgreen {s.mermaid_green}")
    lines.append(f"    classDef stred {s.mermaid_red}")
    lines.append(f"    classDef stunknown {s.mermaid_unknown}")
    for state, ids in state_classes.items():
        if ids:
            lines.append(f"    class {','.join(ids)} st{state}")
    return "\n".join(lines)


def render_pipeline_dot(graph: PipelineGraph, style: RenderStyle | None = None) -> str:
    s = style or DEFAULT_STYLE
    colors = {"green": s.dot_green, "red": s.dot_red, "unknown": s.dot_unknown}
    lines = [
        "digraph pipeline {",
        "    rankdir=TB;",
        '    node [fontname="Helvetica"];',
    ]
    for v in graph.variables:
        label = f"{v.name}\\n{v.record_count} records" if v.record_count else v.name
        lines.append(f'    "{v.id}" [shape=ellipse label="{label}"];')
    for f in graph.functions:
        label = f.function_name
        if f.variant_count > 1:
            label += f"\\n{f.variant_count} variants"
        lines.append(
            f'    "{f.id}" [shape=box label="{label}" color="{colors[f.state]}"];'
        )
    for e in graph.edges:
        attrs = f' [label="{e.param}"]' if e.param else ""
        lines.append(f'    "{e.source}" -> "{e.target}"{attrs};')
    lines.append("}")
    return "\n".join(lines)


def _abbrev(identifier: str, s: RenderStyle) -> str:
    if len(identifier) <= s.id_abbrev_len:
        return identifier
    return identifier[: s.id_abbrev_len] + s.id_ellipsis


def render_runs_table(runs: list[RunRecord]) -> str:
    if not runs:
        return "(no runs recorded)"
    has_run_meta = any(r.run_id is not None for r in runs)
    headers = ["timestamp", "user", "function"]
    if has_run_meta:
        headers.append("invocations")
        # Which surfaces the run read (gui / script / replay). "?" for a row
        # older than the column: unknown, never assumed.
        headers.append("origin")
    headers.append("where")
    rows = []
    for r in runs:
        row = [r.timestamp, r.user_id, r.function_name]
        if has_run_meta:
            row.append(r.n_invocations)
            row.append(r.origin or "?")
        row.append(r.where_clause or "-")
        rows.append(row)
    return format_table(headers, rows)


def _run_lines(n, prefix: str, s: RenderStyle) -> list[str]:
    """The identity + execution block under one trace node's function line.

    Two layers, and the split matters. The first names the *call*: which
    invocation, which call site, which run options — the three things that make
    two records at one schema location different records rather than a
    duplicate. The second lists the *executions* of that call, one line each,
    because a record reproduced by several runs has several answers to "which
    run produced this" and picking one would be the same silent choice this
    whole feature exists to undo.

    A run whose invocation or function hash differs from the node's headline
    one is tagged in place — that is a record reproduced by genuinely different
    code or flags, and it must not read as another run of the same thing.
    """
    lines: list[str] = []
    identity = []
    if n.invocation_id:
        identity.append(s.invocation_fmt.format(inv=_abbrev(n.invocation_id, s)))
    if n.call_id:
        identity.append(s.call_id_fmt.format(cid=_abbrev(n.call_id, s)))
    if n.run_options:
        identity.append(s.run_options_fmt.format(opts=n.run_options))
    if identity:
        line = prefix + s.label_sep.join(identity)
        if len(n.invocation_ids) > 1:
            line += s.reproduced_note_fmt.format(n=len(n.invocation_ids))
        lines.append(line)
    if n.function_name is None:
        return lines  # a raw save has no invocation and no run
    if not n.runs:
        lines.append(prefix + s.no_runs_label)
        return lines
    for run in n.runs:
        line = prefix + s.run_line_fmt.format(
            rid=_abbrev(run.run_id, s), ts=run.timestamp
        )
        if run.user_id:
            line += s.run_user_fmt.format(user=run.user_id)
        if run.invocation_id != n.invocation_id or run.function_hash != n.function_hash:
            line += s.run_variant_fmt.format(
                variant=f"{_abbrev(run.invocation_id, s)}"
                + (f" {run.run_options}" if run.run_options else "")
            )
        if run.where_clause:
            line += s.run_where_fmt.format(where=run.where_clause)
        lines.append(line)
    return lines


def render_trace(
    tree: ProvenanceTree, style: RenderStyle | None = None, show_runs: bool = False
) -> str:
    s = style or DEFAULT_STYLE
    nodes = {n.record_id: n for n in tree.nodes}
    lines: list[str] = []
    visited: set[str] = set()

    def record_label(n) -> str:
        head = n.variable
        schema_str = " ".join(f"{k}={v}" for k, v in n.schema.items())
        if schema_str:
            head += f"  {schema_str}"
        parts = [head, s.record_id_fmt.format(rid=_abbrev(n.record_id, s))]
        if n.saved:
            parts.append(
                s.saved_fmt.format(ts=n.saved, user=n.saved_by)
                if n.saved_by
                else s.saved_no_user_fmt.format(ts=n.saved)
            )
        label = s.label_sep.join(parts)
        if n.function_name is None:
            label += s.raw_tag
        return label

    def emit_producer(n, prefix: str) -> None:
        # The record's producing-function block, then its inputs recursively.
        if n.function_name is None:
            return
        fn_parts = [n.function_name]
        if n.function_hash:
            fn_parts.append(s.fn_hash_fmt.format(h=_abbrev(n.function_hash, s)))
        if show_runs and n.code_version:
            fn_parts.append(s.code_version_fmt.format(v=n.code_version))
        if n.run_count:
            fn_parts.append(s.run_note_fmt.format(n=n.run_count, ts=n.last_run or "?"))
        lines.append(f"{prefix}{s.branch_last}{s.label_sep.join(fn_parts)}")
        inner = prefix + s.cont_last
        if n.constants:
            lines.append(
                inner
                + "  "
                + s.const_join.join(f"{k}={v}" for k, v in sorted(n.constants.items()))
            )
        if show_runs:
            lines.extend(_run_lines(n, inner + "  ", s))
        for param, spec in sorted(n.path_inputs.items()):
            info = parse_path_input(spec)
            shown = info["template"] if info else spec
            lines.append(f"{inner}  {param} {s.input_marker} PathInput({shown!r})")
        for i, inp in enumerate(n.inputs):
            last = i == len(n.inputs) - 1
            branch = s.branch_last if last else s.branch_mid
            cont = s.cont_last if last else s.cont_mid
            child = nodes.get(inp.record_id)
            if child is None:  # beyond max_depth — id only
                lines.append(
                    f"{inner}{branch}{inp.param} {s.input_marker} {inp.variable}"
                    f"{s.label_sep}"
                    + s.record_id_fmt.format(rid=_abbrev(inp.record_id, s))
                )
                continue
            lines.append(
                f"{inner}{branch}{inp.param} {s.input_marker} {record_label(child)}"
            )
            if child.record_id in visited:
                lines[-1] += s.back_ref_suffix
                continue
            visited.add(child.record_id)
            emit_producer(child, inner + cont)

    if tree.selection:
        # The CANONICAL pin, not what the user typed: `Code:grSides=v2` and
        # `__code__.grSides=v2` are the same selection, and seeing which one it
        # resolved to is how a typo that silently selected nothing becomes
        # visible.
        lines.append(
            "variant: "
            + "  ".join(f"{k}={v}" for k, v in sorted(tree.selection.items()))
        )
    if len(tree.matched_record_ids) > 1:
        # The pin named several records — one per schema location, normally.
        # Tracing roots at one of them; saying so beats a tree that silently
        # describes a single subject when the user asked about a variant.
        lines.append(
            f"({len(tree.matched_record_ids)} records match; tracing "
            f"{_abbrev(tree.root_record_id, s)})"
        )
    root = nodes[tree.root_record_id]
    lines.append(record_label(root))
    visited.add(root.record_id)
    emit_producer(root, "")

    if tree.audit:
        lines.append("")
        lines.append("runs that produced this record:")
        lines.append(render_runs_table(tree.audit))
    return "\n".join(lines)


def render_node_states(
    states: list[NodeStateSummary],
    show_missing: bool = False,
    style: RenderStyle | None = None,
) -> str:
    s = style or DEFAULT_STYLE
    if not states:
        return "(no pipeline functions recorded)"
    has_configs = any(st.constants for st in states)
    headers = ["function", "state", "basis", "up-to-date", "missing"]
    if has_configs:
        headers.insert(1, "config")
    rows = []
    for st in states:
        row = [
            st.function_name,
            _state_tag(
                st.state,
                {"up_to_date": st.up_to_date, "missing": st.missing},
                # basis note is its own column here — keep tags short
                basis="",
                s=s,
            ),
            st.state_basis,
            st.up_to_date,
            st.missing,
        ]
        if has_configs:
            row.insert(
                1,
                s.const_join.join(
                    f"{k}={v}" for k, v in sorted((st.constants or {}).items())
                )
                or "-",
            )
        rows.append(row)
    lines = [format_table(headers, rows)]
    if show_missing:
        for st in states:
            if not st.missing_combos:
                continue
            lines.append(f"{st.function_name}:")
            for combo in st.missing_combos[: s.missing_display_cap]:
                combo_str = " ".join(f"{k}={v}" for k, v in combo.items())
                lines.append(s.missing_line_fmt.format(combo=combo_str))
            overflow = len(st.missing_combos) - s.missing_display_cap
            if overflow > 0:
                lines.append(s.missing_more_fmt.format(n=overflow))
    return "\n".join(lines)


def render_pick_table(candidates: list[PickCandidate], schema_keys: list[str]) -> str:
    if not candidates:
        return "(no matching records)"
    used_keys = [k for k in schema_keys if any(k in c.schema for c in candidates)]
    param_keys = sorted({k for c in candidates for k in c.branch_params})
    headers = [*used_keys, *param_keys, "function", "saved", "record_id"]
    rows = [
        [
            *[c.schema.get(k, "") for k in used_keys],
            *[c.branch_params.get(k, "-") for k in param_keys],
            c.function_name or "(raw)",
            c.saved,
            c.record_id,
        ]
        for c in candidates
    ]
    return format_table(headers, rows)


def render_exclusions(exclusions: list[ExclusionRecord], schema_keys: list[str]) -> str:
    if not exclusions:
        return "(no schema exclusions)"
    used_keys = [k for k in schema_keys if any(k in e.schema for e in exclusions)]
    return format_table(
        [*used_keys, "reason", "since", "by"],
        [
            [
                # Wildcard keys (omitted at exclude time) render as *.
                *[e.schema.get(k, "*") for k in used_keys],
                e.reason,
                e.changed_at,
                e.changed_by,
            ]
            for e in exclusions
        ],
    )


def render_mutation_result(result: MutationResult) -> str:
    target = " ".join(f"{k}={v}" for k, v in result.target.items()) or "-"
    lines = [f"{result.operation}: {target}", f"reason: {result.reason}"]
    if result.detail:
        lines.append(result.detail)
    return "\n".join(lines)


def render_records(records: list[RecordSummary], schema_keys: list[str]) -> str:
    if not records:
        return "(no matching records)"
    # Only show schema columns that at least one record uses.
    used_keys = [k for k in schema_keys if any(k in r.schema for r in records)]
    has_previews = any(r.value_preview is not None for r in records)
    headers = ["record_id", *used_keys, "saved", "user", "v", "excluded"]
    if has_previews:
        headers.append("value")
    rows = []
    for r in records:
        row = [
            r.record_id,
            *[r.schema.get(k, "") for k in used_keys],
            r.timestamp,
            r.user_id,
            r.schema_version,
            "yes" if r.excluded else "",
        ]
        if has_previews:
            row.append(r.value_preview)
        rows.append(row)
    return format_table(headers, rows)


def render_intent(report: IntentReport) -> str:
    """Intent, fact and the Decision for one function, as text.

    Three blocks, in the order a reader needs them: what the last run WAS
    (origin), what each field RESOLVES to and from where, and the statements
    this origin does not read. A field flagged ``lost`` — history recorded a
    selection, the winner carries none — is the shape of a dropped selection
    and is marked so it cannot be skimmed past.
    """
    from ..intent import describe_columns

    lines = [f"{report.function_name} — intent vs fact (origin={report.origin}, scope={report.scope})"]
    if report.last_run:
        lines.append(
            f"  last run: {report.last_run.get('timestamp') or '?'} "
            f"origin={report.last_run.get('origin') or '?'}"
        )
    else:
        lines.append("  last run: (none recorded)")

    if not report.fields:
        lines.append("  (no statements and no recorded fact)")
    else:
        rows = []
        for f in report.fields:
            if f.aspect == "columns":
                shown = describe_columns(f.value)
                recorded = describe_columns(f.recorded) if f.recorded else "-"
            else:
                shown = repr(f.value)
                recorded = repr(f.recorded) if f.recorded is not None else "-"
            rows.append(
                [
                    f"{f.aspect}" + (f".{f.key}" if f.key else ""),
                    shown,
                    f.surface + (f"@{f.scope}" if f.scope and f.surface != "history" else ""),
                    recorded,
                    "LOST" if f.lost else "",
                ]
            )
        lines.append(format_table(["field", "resolves to", "from", "history recorded", ""], rows))

    if report.unread:
        lines.append(f"  not read by a {report.origin} run ({len(report.unread)} statement(s)):")
        for s in report.unread:
            key = f".{s['key']}" if s.get("key") else ""
            lines.append(f"    {s['aspect']}{key} = {s['value']!r}  [{s['surface']}@{s['scope']}]")
    if not report.statements:
        lines.append("  (no statements in the intent store for this function)")
    return "\n".join(lines)
