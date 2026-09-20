"""
``plot_data(spec, table)`` — the long table a plot is drawn from, as data.

"Save data" in the Plot Studio writes this frame to CSV so that the statistics
run on exactly the rows the figure shows. It is NOT a re-derivation: it takes
the same plan ``resolve`` takes (variants folded, level groups, the 1-D cell
collapse, filters, the ITERATE fan-out) and, per figure, the same
``reduce._sample_frame`` the marks are built from. See
``docs/claude/plot-data-export.md``.

**The default is the sample.** With ``[subject, session, speed, trial,
cycle]``, session grouped, speed separating figures and subject / trial /
cycle collapsed, the chain averages cycle within trial, then trial within
subject; subject is the sample. The CSV is then ``subject, session, speed,
<measure>`` — the rows a bar summarises into mean ± error, a box draws as a
distribution and a scatter draws as points (every kind draws the sample:
schema-level parity, 2026-09-19).

**A depth keeps more.** ``depth="trial"`` cuts the chain before trial
(``roles.chain_cut``, the rule "Show sample" uses): only cycle is averaged,
every trial of every subject is a row. ``depth="cycle"`` — the deepest
collapsed key — averages nothing.

**Struct / table variables** (scalar fields melted into a ``ColName`` field
factor by the source) are written **one column per field** by default —
``subject, session, RTA, RMG, …`` — the layout a repeated-measures package
expects (``fields_as_columns=True``, the user's default, 2026-09-19). It
applies to the field factor ONLY (``FactorInfo.is_field``), and only when the
field survives the chosen depth. ``fields_as_columns=False`` keeps the long
form with a ``ColName`` column.

Scalar measures only (user decision, 2026-09-19). A 1-D measure drawn by a
scalar kind has been reduced to one value per record by the cell collapse, so
it IS scalar here and qualifies; a raw 1-D or 2-D measure is refused with the
reason (:func:`data_unavailable`).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log

from .roles import chain_cut, collapse_order, collapse_steps
from .shape import Shape
from .spec import LocationFilter, PlotSpec
from .table import LongTable

LAYER = "scistackplot"


@dataclass(frozen=True)
class DataDepth:
    """One choice in the "Save data" depth picker.

    ``key`` is the deepest schema key kept as its own column (None when
    nothing is collapsed — the rows as plotted); ``averaged`` the collapsed
    keys averaged away first, deepest first; ``columns`` the header the file
    will have, in order.
    """

    key: str | None
    label: str
    averaged: list[str]
    columns: list[str]
    #: The header with one column per FIELD (``fields_as_columns``), or None
    #: when this depth has no field factor to spread (none in the table, or
    #: collapsed away at this depth).
    wide_columns: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "averaged": list(self.averaged),
            "columns": list(self.columns),
            "wide_columns": list(self.wide_columns) if self.wide_columns is not None else None,
        }


@dataclass(frozen=True)
class DataExportOptions:
    """What "Save data" can write for a spec — the one statement of it, read
    by :func:`plot_data` (to validate a depth) and by the capability report
    (to draw the button and the picker)."""

    available: bool
    reason: str | None = None
    #: Default first, then progressively deeper.
    depths: list[DataDepth] = field(default_factory=list)
    #: The collapse chain, deepest first, and the sample key(s) — for the
    #: panel's wording and the log.
    chain: list[str] = field(default_factory=list)
    sample: list[str] = field(default_factory=list)
    pooled: bool = False
    #: The struct field factor (``ColName``) that "one column per field"
    #: spreads, when some depth keeps it; None otherwise (no checkbox).
    field_factor: str | None = None

    @property
    def default(self) -> DataDepth | None:
        return self.depths[0] if self.depths else None

    def depth(self, key: str | None) -> DataDepth:
        """The choice for ``key`` (None = the default), or ValueError naming
        the choices."""
        if key is None:
            if self.default is None:
                raise ValueError(self.reason or "Nothing to export.")
            return self.default
        for depth in self.depths:
            if depth.key == key:
                return depth
        raise ValueError(
            f"No export depth {key!r}; choose one of "
            f"{[d.key for d in self.depths]}."
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "reason": self.reason,
            "depths": [d.to_dict() for d in self.depths],
            "default": self.default.key if self.default else None,
            "chain": list(self.chain),
            "sample": list(self.sample),
            "pooled": self.pooled,
            "field_factor": self.field_factor,
        }


def data_unavailable(shape: Shape) -> str | None:
    """Why the drawn measure cannot be exported, or None when it can.

    ``shape`` is the DRAWN shape — after the cell collapse, so a 1-D measure
    drawn as a violin is scalar here."""
    if shape is Shape.SCALAR:
        return None
    if shape is Shape.SERIES_1D:
        return (
            "This plot draws a 1-D measure (a series per record). Saving its "
            "data is supported for scalar plots only — choose a scalar kind "
            "(bar, box, violin, scatter, strip), which reduces each record to "
            "one value first, and that value is what is saved."
        )
    if shape is Shape.MATRIX_2D:
        return "This plot draws a 2-D measure; saving its data is supported for scalar plots only."
    return f"This plot draws a {shape} measure; saving its data is supported for scalar plots only."


def data_export_options(
    spec: PlotSpec,
    roles: dict,
    table: LongTable,
    shape: Shape | None = None,
) -> DataExportOptions:
    """The depths "Save data" offers.

    ``table`` is the table the figure is drawn from — variants folded, level
    groups applied, the cell collapse done (``capabilities`` holds it as
    ``collapsed``, a plan as ``plan.table``) — and ``roles`` the completed
    roles. ``shape`` defaults to the drawn measure's shape in ``table``.

    Pure over those inputs, so the capability report can offer the picker
    without building a plan, and :func:`plot_data` validates a depth with
    the very same answer.
    """
    shape = shape or table.shape_of(spec.y_measure)
    reason = data_unavailable(shape)
    if reason is not None:
        return DataExportOptions(available=False, reason=reason)

    order = collapse_order(roles, table)
    steps = collapse_steps(spec, roles, table)
    present = list(table.frame.columns)
    field_name = _field_factor(table)
    field_levels = _field_levels(spec, table, field_name) if field_name else []

    def depth(key: str | None, averaged: list[str], label: str) -> DataDepth:
        columns = _columns(present, averaged, spec, table)
        return DataDepth(
            key=key,
            label=label,
            averaged=averaged,
            columns=columns,
            wide_columns=(
                _wide_header(columns, field_name, field_levels, spec)
                if field_name in columns
                else None
            ),
        )

    if not order:
        depths = [depth(None, [], "As plotted — nothing is collapsed, one row per mark")]
    elif spec.aggregate.pooled:
        # Pooled: every collapsed key is the sample at once, so the plotted
        # rows already hold every collapsed level unaveraged — there is no
        # deeper choice to offer.
        depths = [
            depth(
                order[-1],
                [],
                f"As plotted — pooled ({' x '.join(reversed(order))}), nothing averaged",
            )
        ]
    else:
        sample = order[-1]
        depths = [
            depth(
                sample,
                list(steps.pre),
                f"{sample} — the plotted sample"
                + (f" ({', '.join(steps.pre)} averaged)" if steps.pre else ""),
            )
        ]
        # Deeper cuts, shallowest first: trial, then cycle.
        for key in reversed(order[:-1]):
            averaged, _kept = chain_cut(order, key)
            label = (
                f"down to {key} (raw — nothing averaged)"
                if not averaged
                else f"down to {key} ({', '.join(averaged)} averaged)"
            )
            depths.append(depth(key, averaged, label))

    return DataExportOptions(
        available=True,
        depths=depths,
        chain=list(order),
        sample=list(steps.sample),
        pooled=bool(spec.aggregate.pooled),
        field_factor=(
            field_name if any(d.wide_columns is not None for d in depths) else None
        ),
    )


def plot_data(
    spec: PlotSpec,
    table: LongTable,
    *,
    depth: str | None = None,
    fields_as_columns: bool = True,
) -> pd.DataFrame:
    """The long table ``spec``'s figures are drawn from — every figure of the
    fan-out in one frame, the figure keys as columns.

    ``depth`` is a :class:`DataDepth` key from :func:`data_export_options`;
    None is the default (the plotted sample). Raises :class:`ValueError` (a
    ``RoleError`` for an invalid spec) with the reason when the spec's data
    cannot be exported.

    ``fields_as_columns`` spreads a struct variable's field factor
    (``ColName``) into one column per field — the default, as in the GUI's
    checkbox. It is a pure reshape of the long rows (:func:`_fields_to_columns`):
    no value is averaged, dropped or added, and a field a record lacks is an
    empty cell. Inert when there is no field factor at the chosen depth.

    Rows whose measure is missing are dropped, as the figure drops them
    (``reduce._panel_frame``), and the count is logged: a CSV row the plot
    never drew would break the parity this function exists for.
    """
    from .reduce import _collapse_levels, _plan, _sample_frame
    from .roles import CollapseSteps

    with Log.timer("plot_data", layer=LAYER, extra=str(spec.kind)) as timing:
        with timing.phase("plan"):
            plan = _plan(spec, table)
            options = data_export_options(plan.spec, plan.roles, plan.table, plan.shape)
            if not options.available:
                raise ValueError(options.reason)
            chosen = options.depth(depth)
            steps = collapse_steps(plan.spec, plan.roles, plan.table)

        pieces: list[pd.DataFrame] = []
        with timing.phase("collapse", extra=f"{len(plan.groups)} figure(s)"):
            for figure_key, group in plan.groups:
                if list(chosen.averaged) == list(steps.pre):
                    # The default: the very call the figure makes.
                    rows = _sample_frame(group, steps, plan.spec, plan.table, None)
                else:
                    rows = _sample_frame(
                        group,
                        CollapseSteps(pre=list(chosen.averaged), sample=[]),
                        plan.spec,
                        plan.table,
                        None,
                    )
                Log.debug(
                    "[plot-data] figure %s: %d row(s)",
                    ", ".join(f"{k}={v}" for k, v in figure_key.items()) or "single",
                    len(rows),
                    layer=LAYER,
                )
                pieces.append(rows)

        with timing.phase("assemble"):
            frame = pd.concat(pieces, ignore_index=True) if pieces else plan.frame.iloc[:0]
            columns = [name for name in chosen.columns if name in frame.columns]
            dropped_internal = [c for c in frame.columns if str(c).startswith("__")]
            if dropped_internal:
                Log.debug(
                    "[plot-data] internal column(s) not written: %s",
                    dropped_internal,
                    layer=LAYER,
                )
            frame = frame[columns]
            y = plan.spec.y_measure
            missing = frame[y].isna() | pd.to_numeric(frame[y], errors="coerce").isna()
            if missing.any():
                Log.info(
                    "[plot-data] %d row(s) with no %s dropped — the figure does "
                    "not draw them either",
                    int(missing.sum()),
                    y,
                    layer=LAYER,
                )
                frame = frame[~missing]
            frame = _ordered(frame, plan.table).reset_index(drop=True)
            spread = options.field_factor if fields_as_columns else None
            if spread and spread in frame.columns:
                long_rows = len(frame)
                frame = _fields_to_columns(
                    frame, spread, plan.spec, plan.table,
                    levels=_field_levels(plan.spec, plan.table, spread),
                )
                Log.info(
                    "[plot-data] one column per %s: %d long row(s) -> %d row(s), "
                    "field column(s) %s",
                    spread,
                    long_rows,
                    len(frame),
                    [c for c in frame.columns if c not in chosen.columns],
                    layer=LAYER,
                )

    Log.info(
        "[plot-data] %s: %d figure(s), chain %s, depth=%s%s -> %d row(s) x %d "
        "column(s) %s",
        plan.spec.y_measure,
        len(plan.groups),
        _describe_chain(options),
        chosen.key or "as plotted",
        ", one column per field" if fields_as_columns and chosen.wide_columns else "",
        len(frame),
        len(frame.columns),
        list(frame.columns),
        layer=LAYER,
    )
    return frame


def _columns(
    present: list[str], averaged: list[str], spec: PlotSpec, table: LongTable
) -> list[str]:
    """The header, in order: factors outermost-first (by hierarchy depth, then
    the table's own order; factors with no depth — a variant axis, a field, a
    derived bucket — after the schema), then the measure(s)."""
    depths = table.factor_depths
    factors = [
        name
        for name in table.factor_names
        if name in present and name not in averaged and not str(name).startswith("__")
    ]
    position = {name: index for index, name in enumerate(table.factor_names)}
    factors.sort(
        key=lambda name: (
            (0, depths[name]) if name in depths else (1, 0),
            position[name],
        )
    )
    measures = [
        name
        for name in dict.fromkeys([*spec.measures, spec.x_measure])
        if name and name in present
    ]
    return [*factors, *measures]


def _field_factor(table: LongTable) -> str | None:
    """The struct field factor (``ColName``), by its flag and never by name —
    a source that renamed it to dodge a schema key (``ColName_``) still
    has exactly one."""
    fields = [f.name for f in table.field_factors if f.name in table.frame.columns]
    return fields[0] if fields else None


def _field_levels(spec: PlotSpec, table: LongTable, name: str) -> list[Any]:
    """The field levels the file will have: the declared levels, narrowed by
    the filters on the FIELD column itself (a field picker is a filter) -- and
    by nothing else. A filter on another column that happens to empty the
    data must not also empty the header: the file still has one column per
    field, with no rows (integration suite, 2026-09-19)."""
    from dataclasses import replace

    from .reduce import _level_rank, apply_filters

    own = [f for f in spec.filters if f.column == name]
    frame = table.frame
    if own:
        frame = apply_filters(frame, replace(spec, filters=own, location_filter=LocationFilter()))
    present = frame[name].dropna().unique().tolist()
    return sorted(present, key=lambda level: _level_rank(table, name, level))


def _wide_header(
    columns: list[str], field_name: str, levels: list[Any], spec: PlotSpec
) -> list[str]:
    """``columns`` with the field factor and the measure(s) replaced by one
    column per field level."""
    measures = _measure_columns(columns, spec)
    index = [c for c in columns if c != field_name and c not in measures]
    return [*index, *_wide_names(levels, measures, index)]


def _measure_columns(columns: list[str], spec: PlotSpec) -> list[str]:
    wanted = [m for m in dict.fromkeys([*spec.measures, spec.x_measure]) if m]
    return [c for c in columns if c in wanted]


def _wide_names(levels: list[Any], measures: list[str], index: list[str]) -> list[str]:
    """One name per (measure, field level). The bare field name when there is
    one measure and no field is named like a factor column; otherwise
    ``<measure>.<field>`` for every column, so the header never holds two
    columns of the same name."""
    bare = [str(level) for level in levels]
    if len(measures) == 1 and not set(bare) & set(index):
        return bare
    return [f"{measure}.{level}" for measure in measures for level in bare]


def _fields_to_columns(
    frame: pd.DataFrame,
    field_name: str,
    spec: PlotSpec,
    table: LongTable,
    levels: list[Any] | None = None,
) -> pd.DataFrame:
    """Spread the field factor into one column per field — a pure reshape.

    One output row per combination of the other factor columns (a missing
    level is a level: ``dropna=False``, since coarse inputs leave whole key
    columns NULL), in the order the long rows already had. Each field's value
    lands in its own column; a record lacking a field is an empty cell.
    Two long rows for one (record, field) would mean the field was not the
    only thing distinguishing them, and is refused rather than silently
    keeping one.
    """
    from .reduce import _level_rank

    measures = _measure_columns(list(frame.columns), spec)
    index = [c for c in frame.columns if c != field_name and c not in measures]
    fields = frame[field_name]
    if levels is None:
        levels = sorted(
            fields.dropna().unique().tolist(),
            key=lambda level: _level_rank(table, field_name, level),
        )
    if fields.isna().any():
        Log.warn(
            "[plot-data] %d row(s) have no %s and cannot be placed in a field column — dropped",
            int(fields.isna().sum()),
            field_name,
            layer=LAYER,
        )
    if index:
        ids = frame.groupby(index, dropna=False, sort=False).ngroup().to_numpy()
    else:
        ids = np.zeros(len(frame), dtype=np.int64)
    pairs = pd.DataFrame({"id": ids, "field": fields.to_numpy()})
    clash = pairs.dropna().duplicated()
    if clash.any():
        # Name the first few clashing locations: "N records" alone cannot
        # say whether the frame really holds a location twice or an index
        # column is missing from `index`.
        examples = (
            pd.concat([frame[index].reset_index(drop=True), pairs], axis=1)[clash.reindex(pairs.index, fill_value=False)]
            .head(3)
            .to_dict("records")
        )
        raise ValueError(
            f"Cannot give each {field_name} its own column: {int(clash.sum())} "
            f"record(s) hold the same field twice (index columns {index}; e.g. "
            f"{examples}). Save with one row per field instead."
        )
    count = int(ids.max()) + 1 if len(ids) else 0
    _, first = np.unique(ids, return_index=True)
    out = frame.iloc[first][index].reset_index(drop=True)
    names = iter(_wide_names(levels, measures, index))
    for measure in measures:
        values = pd.to_numeric(frame[measure], errors="coerce").to_numpy(dtype=float)
        for level in levels:
            column = np.full(count, np.nan)
            mask = (fields == level).to_numpy()
            column[ids[mask]] = values[mask]
            out[next(names)] = column
    return out


def _ordered(frame: pd.DataFrame, table: LongTable) -> pd.DataFrame:
    """Rows in the factors' declared level order, column by column — the order
    the figure's axes use (``reduce._level_rank``), so ``"02"`` precedes
    ``"10"``. Missing levels last. Vectorised per column: one factorize and a
    sort of the distinct levels, never a Python key per row."""
    from .reduce import _level_rank

    factor_columns = [name for name in frame.columns if table.has_factor(name)]
    if frame.empty or not factor_columns:
        return frame
    ranks: dict[str, np.ndarray] = {}
    for name in factor_columns:
        codes, uniques = pd.factorize(frame[name], use_na_sentinel=True)
        ordered = sorted(range(len(uniques)), key=lambda i: _level_rank(table, name, uniques[i]))
        rank_of_code = np.empty(len(uniques) + 1, dtype=np.int64)
        for rank, code in enumerate(ordered):
            rank_of_code[code] = rank
        rank_of_code[len(uniques)] = len(uniques)  # NaN (code -1) sorts last
        ranks[name] = rank_of_code[np.where(codes < 0, len(uniques), codes)]
    order = np.lexsort([ranks[name] for name in reversed(factor_columns)])
    return frame.iloc[order]


def _describe_chain(options: DataExportOptions) -> str:
    if not options.chain:
        return "nothing collapsed"
    if options.pooled:
        return " x ".join(options.chain) + " (pooled sample)"
    return " -> ".join([*options.chain[:-1], f"{options.chain[-1]} (sample)"])
