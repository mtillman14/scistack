"""
Loading scidb variables into long-format frames.

The long format itself is nearly free — schema keys are already ordinary
columns once a variable is joined to ``_schema``, which is the same shape
``stat_`` functions receive via ``as_table``. What this module adds is the
part a flat CSV never needed: attaching branch params as columns, and knowing
which schema keys a given variable actually occupies.

Queries go through ``_fetchall``/``_fetchone``/``_fetchdf`` (never
``_execute(...).fetchall()`` — see docs/claude on DuckDB fetch locking); the
payload itself only ever through ``_fetchdf``, which is what keeps a sample from
becoming a Python float (see ``load_variable``) and batch the branch-params walk
rather than asking per record (the N+1 trap).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scistacklog import Log
from scistackplot import CODE_FACTOR_PREFIX, RUN_FACTOR_PREFIX
from scistackplot.framesize import format_extent, frame_extent

LAYER = "scistackplotdb"

#: Column name given to the producing function's version when a variable holds
#: records built by more than one version of its function's source. Named like
#: the stack's other synthetic factor (``source.FIELD_FACTOR`` = ``ColName``)
#: rather than after a database column, because that is what it is to a reader
#: of the figure: a condition, not a record attribute.
#: Owned by scistackplot (``CODE_FACTOR_PREFIX``), re-exported here under the
#: name this layer's callers already use. The rendering layer decides how a code
#: axis is presented and defaulted, so it owns the convention; sources conform
#: rather than each inventing their own prefix.
VERSION_FACTOR_PREFIX = CODE_FACTOR_PREFIX

#: Level for a record whose chain does not include this column's function at
#: all — a raw save, or a record that reached this schema location by a route
#: that never ran it. Without a level of its own those rows would hold NaN and
#: drop silently out of every facet.
#:
#: Note this replaced a single ``CodeVersion`` column carrying ``"(raw)"``. One
#: column per function is what makes a multi-layer chain expressible, and it is
#: also what lets the level mean the same thing everywhere: ``v2`` in
#: ``Code:bandpass_filter`` is the same code at every schema location, which a
#: single merged column could not promise once two functions were in play.
MISSING_VERSION_LEVEL = "(n/a)"

#: Per-row flag for "this record's code version is the newest at ITS OWN schema
#: location". Not a variant factor — a helper the default pin filters on.
#:
#: Pinning has to happen on this rather than on ``Code:fn == "v2"``, and the
#: difference is not cosmetic. Version ordinals are numbered per function so
#: their levels mean the same thing everywhere, which means pinning a level
#: drops every schema location that was never re-run under the newest code —
#: silently losing subjects from the figure. This flag is resolved per location,
#: so pinning it keeps each location's own newest record and loses nothing.
#:
#: It is also **one flag for the whole chain**, not one per code column. That is
#: what keeps the default a single checkbox however many layers were edited: an
#: N-function chain would otherwise need N pins to express "just show me the
#: current results".
LATEST_COLUMN = "CodeIsLatest"


@dataclass
class VariableFrame:
    """A variable's records as a long frame, plus what its columns mean."""

    name: str
    frame: pd.DataFrame
    #: Schema keys this variable actually occupies (non-null for its records).
    levels: list[str] = field(default_factory=list)
    #: Data column(s) of the variable's table, excluding record_id.
    data_columns: list[str] = field(default_factory=list)
    #: Variant columns attached from the provenance graph — branch params, plus
    #: the producing function's version when there is more than one.
    variant_columns: list[str] = field(default_factory=list)
    #: One entry per variant column saying where it came from:
    #: ``{"column", "kind": "code"|"param", "function", "param"}``.
    #:
    #: Column names encode this already (``Code:bandpass``, ``bandpass.low_hz``),
    #: but only as *scidb's* namespacing conventions. Anything that needed the
    #: producing function — the GUI mapping an axis to its pipeline node, most
    #: obviously — would otherwise re-implement those conventions by splitting
    #: strings, one layer away from where they are defined and first to break
    #: when they change. Carrying the structure costs nothing here, where both
    #: halves are still in hand.
    variant_axes: list[dict] = field(default_factory=list)
    #: Name of the per-row "this is my location's newest code version" flag, or
    #: None when the variable holds only one version. See :data:`LATEST_COLUMN`.
    latest_column: str | None = None

    @property
    def value_column(self) -> str:
        return self.data_columns[0] if self.data_columns else self.name


def schema_keys(db) -> list[str]:
    return list(db._duck.dataset_schema)


def registered_variables(db) -> list[str]:
    rows = db._duck._fetchall("SELECT variable_name FROM _variables ORDER BY variable_name")
    return [row[0] for row in rows]


def table_name_for(db, variable: str) -> str:
    """
    Resolve a variable's data table.

    ``_registered_types.table_name`` is deliberately NOT unique (see the note
    in ``DatabaseManager._ensure_meta_tables``), so every query below also
    filters on ``_record.type`` — reading a shared table without that filter
    would silently mix two variables' records into one plot.
    """
    row = db._duck._fetchone(
        "SELECT table_name FROM _registered_types WHERE type_name = ?", [variable]
    )
    return row[0] if row and row[0] else f"{variable}_data"


def data_columns_for(db, variable: str) -> list[str]:
    return list(data_column_types_for(db, variable))


def data_column_types_for(db, variable: str) -> dict[str, str]:
    """``{column: declared DuckDB type}``, in ordinal order.

    One query, the same one :func:`data_columns_for` has always made — it just
    keeps the type column it was already paying for. The type is used for one
    purpose only: cheaply REFUSING a column that cannot hold a group label
    (:func:`is_container_type`). It never accepts one; see that function.
    """
    table = table_name_for(db, variable)
    rows = db._duck._fetchall(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? AND column_name != 'record_id' "
        "ORDER BY ordinal_position",
        [table],
    )
    return {row[0]: str(row[1] or "") for row in rows}


#: DuckDB type names whose values are containers rather than single cells.
#: Matched as a whole leading word so ``DECIMAL(18,3)`` and ``TIMESTAMP WITH
#: TIME ZONE`` — which have punctuation but hold one value — are not caught.
_CONTAINER_TYPE_HEADS = ("STRUCT", "MAP", "UNION", "LIST", "ARRAY")

#: Types holding one value that is still not a label a human would group by.
_OPAQUE_TYPES = frozenset({"BLOB", "BIT", "JSON"})


def is_container_type(data_type: str) -> bool:
    """Whether a declared DuckDB type holds an array/struct rather than a value.

    ``DOUBLE[]`` (a 1-D signal per cell), ``DOUBLE[][]`` (a matrix),
    ``DOUBLE[3]`` (a fixed-size array), ``STRUCT(...)``, ``MAP(...)``, ``BLOB``.
    See ``docs/claude/duckdb-column-types.md``: the DuckDB column type IS the
    cell value type, so this is exactly "does one cell hold a signal".

    **Refusal only, never acceptance, and that asymmetry is the whole design.**
    :func:`sample_value` states plainly why shape classification reads a VALUE
    and not a declared type name: the duckdb client's own Python type for a cell
    is ground truth, with no dependency on how DuckDB spells list types across
    versions. That reasoning still stands and is not being reversed here — this
    is a PRE-FILTER in front of it, not a replacement.

    So an unrecognised spelling falls through to the sampling path and is
    classified exactly as before. A new DuckDB version renaming ``DOUBLE[]``
    costs this function its speed-up; it can never cost it a wrong answer, and
    a wrong answer here would mean a signal column offered as a grouping or a
    real label column silently missing from the list.

    Why it is worth having at all: classifying by value means
    :func:`sample_column_value` pulls a whole EMG trace out of DuckDB per
    muscle, only to discard it. Measured on the user's project (scidb.log
    2026-09-15), that was 4.2 s for ``FilteredDelsys`` and 5.4 s for ``RawEMG``
    — ~8 s added to every panel open, by two variables that offer no groupings
    at all.
    """
    text = str(data_type or "").strip().upper()
    if not text:
        return False
    if text in _OPAQUE_TYPES:
        return True
    # `DOUBLE[]`, `DOUBLE[][]`, `DOUBLE[3]` — anything with a subscript.
    if "[" in text:
        return True
    head = text.split("(", 1)[0].strip()
    return head in _CONTAINER_TYPE_HEADS


def sample_value(db, variable: str) -> Any:
    """
    One value from a variable's data column — enough to classify its shape.

    Deliberately a value rather than a declared SQL type name: the duckdb
    client's own Python type for a cell (float for a scalar column, list for a
    LIST column) is the ground truth, with no dependency on how DuckDB spells
    list/array types across versions. The GUI's pre-existing
    ``_numeric_plot_kind`` made the same call for the same reason.
    """
    columns = data_columns_for(db, variable)
    if not columns:
        return None
    table = table_name_for(db, variable)
    row = db._duck._fetchone(
        f'SELECT t."{columns[0]}" FROM "{table}" t '
        f"JOIN _record r ON t.record_id = r.record_id "
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE "
        f"AND t.\"{columns[0]}\" IS NOT NULL LIMIT 1",
        [variable],
    )
    return row[0] if row else None


def sample_column_value(db, variable: str, column: str) -> Any:
    """One value from ONE column of a variable — enough to classify it.

    The per-column twin of :func:`sample_value`, which always samples the first
    column. A wide table (a demographics sheet) holds a different kind of value
    in every column, so "what is this variable" cannot answer "may this column
    group a figure".
    """
    table = table_name_for(db, variable)
    row = db._duck._fetchone(
        f'SELECT t."{column}" FROM "{table}" t '
        f"JOIN _record r ON t.record_id = r.record_id "
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE "
        f'AND t."{column}" IS NOT NULL LIMIT 1',
        [variable],
    )
    return row[0] if row else None


def column_levels(db, variable: str, column: str, *, limit: int) -> list[str]:
    """Distinct values of one column, as text, at most ``limit`` of them.

    ``limit`` is asked for as *one more* than the caller's threshold by
    convention, so "too many to be a group" is answerable without counting the
    whole column — a column of free text on a large table would otherwise pay a
    full scan to be rejected.

    Text for the same reason the loader casts schema keys: a column that holds a
    NULL arrives as float64 in pandas and would spell its integer levels
    ``1.0``, which nothing downstream matches.
    """
    table = table_name_for(db, variable)
    rows = db._duck._fetchall(
        f'SELECT DISTINCT CAST(t."{column}" AS VARCHAR) AS level FROM "{table}" t '
        f"JOIN _record r ON t.record_id = r.record_id "
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE "
        f'AND t."{column}" IS NOT NULL '
        f"LIMIT {int(limit)}",
        [variable],
    )
    return [row[0] for row in rows]


def variable_levels(db, variable: str) -> list[str]:
    """
    Which schema keys a variable occupies, without loading any data.

    ``describe()`` needs this for every registered variable, and loading each
    one's full frame to find out would mean reading every 1-D array in the
    database just to open the panel. COUNT ignores NULLs, so one row of counts
    says exactly which keys are populated.
    """
    keys = schema_keys(db)
    if not keys:
        return []
    counts = ", ".join(f'COUNT(s."{key}")' for key in keys)
    row = db._duck._fetchone(
        f"SELECT {counts} FROM _record r "
        f"LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE",
        [variable],
    )
    if row is None:
        return []
    return [key for key, count in zip(keys, row, strict=True) if count]


def _normalize_cell(value: Any) -> Any:
    """One data cell as the shape downstream expects, converted PER CELL only.

    A DataFrame fetch already delivers a ``DOUBLE[]`` cell as an ndarray; this
    leaves it alone. What it handles:

    * a ``DOUBLE[][]`` cell, which DuckDB hands over as an object ndarray of
      row ndarrays — stacked into one 2-D float array when the rows are
      rectangular, so ``np.asarray(cell).shape`` is ``(rows, cols)`` as the
      heatmap path expects; ragged rows become a list of row arrays;
    * a Python ``list`` cell, from a DuckDB build that still boxes LIST columns
      on the pandas path — converted once, so the rest of the stack sees one
      contract. The boxing has already been paid by then; the "loaded" log line
      says so (``boxed``), which is the signal to look at the DuckDB version.

    A NULL cell, which the DataFrame fetch spells ``pd.NA``, becomes None — the
    spelling every downstream check (``value is None``) already knows. Scalars,
    None and NaN pass through.
    """
    if value is pd.NA:
        return None
    if isinstance(value, np.ndarray):
        if (
            value.dtype == object
            and value.size
            and isinstance(value.flat[0], (np.ndarray, list))
        ):
            try:
                return np.stack([_float_row(row) for row in value])
            except ValueError:
                # Ragged rows: a Python list OF row arrays (one object per row,
                # not per sample), which `shape.classify_value` still reads
                # as MATRIX_2D — an object ndarray of ndim 1 would read as a
                # 1-D series and the heatmap would silently become a line.
                return list(value)
        if isinstance(value, np.ma.MaskedArray):
            # A NULL ELEMENT inside a list — which is what scidb's single-record
            # INSERT binding stores a NaN as — comes back masked, and the first
            # `np.asarray` downstream silently drops the mask and exposes the
            # fill value: a NaN sample became a number, and every "NaN rows are
            # dropped" comparison in the parity suite failed (2026-09-13).
            # Filled here, once, with the NaN every consumer already handles.
            return _float_row(value)
        return value
    if isinstance(value, list):
        try:
            return np.asarray(value, dtype=float)
        except (TypeError, ValueError):
            return np.asarray(value, dtype=object)
    return value


def _float_row(row: Any) -> np.ndarray:
    """One 1-D float64 array from a row/cell, a masked element becoming NaN."""
    masked = np.ma.asarray(row, dtype="float64")
    return np.ma.filled(masked, np.nan)


def _object_column(cells: list, index) -> pd.Series:
    """A Series of exactly these cells, dtype object, no inference.

    Not ``Series.map`` and not ``pd.Series(cells)``: both run dtype inference
    over the values, and a column of same-length float arrays is precisely the
    input that inference reinterprets (a 2-D block, a scalar per cell, a string
    dtype for the keys). Filling a preallocated object array one cell at a time
    is the one construction every pandas version leaves alone.
    """
    out = np.empty(len(cells), dtype=object)
    for i, cell in enumerate(cells):
        out[i] = cell
    return pd.Series(out, index=index, dtype=object)


def _key_text(value: Any) -> "str | None":
    """A schema key as text, or None for a NULL — whether pandas spelled that
    NULL as None or as NaN."""
    if value is None:
        return None
    if isinstance(value, float) and value != value:
        return None
    return str(value)


def load_variable(
    db, variable: str, *, with_variants: bool = True, include_data: bool = True
) -> VariableFrame:
    """Load every non-excluded record of ``variable`` as a long frame.

    ``include_data=False`` selects the record ids, schema keys and variant
    columns but **no data columns** — everything needed to answer a question
    about a variable's identity and variants, and none of the payload. It exists
    because the payload is where all the cost is: on 2026-09-13 the data columns
    of one variable were 174 million samples / ~5.2 GB, and the surfaces that
    loaded them to read variant metadata simply never returned
    (.claude/plot-at-scale-plan.md §7). The returned frame reports
    ``data_columns=[]``, so it is NOT a plottable table and ``_build_table`` will
    (correctly) refuse it.
    """
    with Log.timer(
        "load_variable",
        layer=LAYER,
        extra=variable if include_data else f"{variable} (metadata only)",
    ) as timer:
        with timer.phase("column_metadata"):
            keys = schema_keys(db)
            columns = data_columns_for(db, variable)
        if not columns:
            Log.warn("variable %r has no data columns", variable, layer=LAYER)
            return VariableFrame(name=variable, frame=pd.DataFrame())
        if not include_data:
            columns = []

        table = table_name_for(db, variable)
        # Schema keys as VARCHAR in the query, not stringified after: a
        # DataFrame fetch types each column, and an integer key with one NULL
        # among its rows would arrive as float64 and stringify as "1.0".
        schema_select = "".join(
            f', CAST(s."{key}" AS VARCHAR) AS "{key}"' for key in keys
        )
        data_select = "".join(f', t."{column}"' for column in columns)
        query = (
            f"SELECT t.record_id{data_select}{schema_select} "
            f'FROM "{table}" t '
            f"JOIN _record r ON t.record_id = r.record_id "
            f"LEFT JOIN _schema s ON r.schema_id = s.schema_id "
            f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE"
        )
        # NOTE: no schema filter, no LIMIT, no column projection — this reads
        # EVERY non-excluded record of the variable with EVERY data column,
        # whatever the caller intends to plot (pushdown is a later stage of
        # .claude/plan-plot-minimal-load-examples.md).
        #
        # `_fetchdf`, never `_fetchall`, for the data columns. Measured on the
        # real database 2026-09-13 (plan §7): one DOUBLE[] column of 17.4 M
        # samples took 4.7 s through `fetchall` — a Python float per sample —
        # and 0.26 s through `.df()`, which hands each cell over as one numpy
        # buffer. That 18x was 86 of the 91 s `plot_describe` spent before it
        # timed out. The "loaded" line below says `ndarray` or `boxed` so a
        # regression to per-sample boxing is visible in the log, not inferred.
        with timer.phase("fetch"):
            frame = db._duck._fetchdf(query, [variable])

        with timer.phase("dataframe"):
            frame = frame[["record_id", *columns, *keys]].copy()
            for column in columns:
                if pd.api.types.is_numeric_dtype(frame[column]):
                    continue  # a scalar column: already one float64 buffer
                frame[column] = _object_column(
                    [_normalize_cell(v) for v in frame[column].to_numpy()],
                    frame.index,
                )
        with timer.phase("stringify_keys"):
            for key in keys:
                frame[key] = _object_column(
                    [_key_text(v) for v in frame[key].to_numpy()], frame.index
                )

        levels = [key for key in keys if frame[key].notna().any()]
        variant_columns: list[str] = []
        variant_axes: list[dict] = []
        latest_column: str | None = None
        if with_variants and len(frame):
            with timer.phase("attach_variants"):
                frame, variant_columns, latest_column, variant_axes = attach_variants(
                    db, frame
                )

        # Cells and SAMPLES alongside the record count: 419 records is the same
        # number whether each holds 200 samples or 250,000, and only the second
        # explains a plot that never returns. Measured over the data columns
        # only, one O(cells) pass (scistackplot.framesize).
        with timer.phase("measure_extent"):
            extent = frame_extent(frame, columns)
        Log.info(
            "loaded %s: %d record(s), %s, levels=%s, variants=%s",
            variable,
            len(frame),
            format_extent(extent),
            levels,
            variant_columns or "none",
            layer=LAYER,
        )
        if len(frame) > 1:
            with timer.phase("identical_content"):
                _log_identical_content(db, variable, frame, keys)
        return VariableFrame(
            name=variable,
            frame=frame,
            levels=levels,
            data_columns=columns,
            variant_columns=variant_columns,
            variant_axes=variant_axes,
            latest_column=latest_column,
        )


def _log_identical_content(db, variable: str, frame: pd.DataFrame, keys) -> None:
    """Warn when records of ``variable`` hold byte-identical payloads, naming
    the schema keys that vary across each duplicate group.

    This is the one thing a plot cannot show you. N records with the same data
    render as N traces drawn exactly on top of each other: the figure looks
    like a single line in whatever colour was drawn last, and filtering to any
    one value of the colour factor shows the same curve again. That is the
    signature of a step that ran with ``distribute=False`` when it should have
    distributed — every location of the key got the whole result instead of
    its own slice — so it belongs in the log at load time rather than being
    re-derived from a confusing figure.

    Never raises: duplicate content is legal, and a diagnostic must not be able
    to break a load.
    """
    try:
        from scidb.provenance_query import identical_content_groups

        groups = identical_content_groups(db._duck, variable)
        if not groups:
            return
        varying: set = set()
        indexed = frame.set_index("record_id")
        for _hash, record_ids in groups:
            present = [rid for rid in record_ids if rid in indexed.index]
            if len(present) < 2:
                continue
            for key in keys:
                values = {indexed.at[rid, key] for rid in present}
                if len(values) > 1:
                    varying.add(key)
        Log.warn(
            "%s: %d group(s) of records hold IDENTICAL data "
            "(%d record(s) total); they differ only by %s. Plots colored or "
            "faceted by those keys will draw overlapping identical traces. "
            "This is what a step that should have run with distribute=True "
            "looks like.",
            variable,
            len(groups),
            sum(len(rids) for _h, rids in groups),
            sorted(varying) or "no schema key",
            layer=LAYER,
        )
    except Exception as e:  # diagnostics must never break a load
        Log.debug(
            "identical-content check skipped for %s: %s", variable, e, layer=LAYER
        )


def attach_variants(
    db, frame: pd.DataFrame
) -> tuple[pd.DataFrame, list[str], str | None, list[dict]]:
    """
    Add one column per thing that distinguishes these records, from the
    provenance graph: each branch param, plus the producing function's version.

    Returns ``(frame, variant_columns, latest_column, variant_axes)`` — the
    third being the name of the :data:`LATEST_COLUMN` flag when versions are in
    play (or None), and the fourth the structured description of each column
    (see :attr:`VariableFrame.variant_axes`).

    This is the correctness-critical step. A variable produced at two filter
    cutoffs has **two records per schema combination**; without these columns
    those rows look like replicates of one another and get overplotted — a
    figure that is wrong in a way that looks like data. With them, the variant
    is an ordinary factor the user must assign (``roles.validate`` refuses to
    let a multi-level variant sit unassigned).

    Branch params alone were not enough. Two records produced by **different
    versions of the same function's source** carry identical branch params, so
    they arrived here indistinguishable and were overplotted as replicates —
    precisely the failure this function exists to prevent, reached by the one
    route it did not cover.

    Nor was the producing function's own version enough, for the same reason one
    hop further out: two records whose producer never changed are still
    different when something *upstream* of it did. ``code_chain`` closes that,
    contributing one ``Code:<fn>`` column per upstream function that genuinely
    holds more than one version. scidb omits single-version functions, so an
    unedited project gets no code columns at all and nothing changes for it.

    Code columns come **first**: they are the axis a reader most often wants
    pinned, and a stable leading position beats having them appear wherever the
    branch-param iteration order happened to put them. Run-option columns
    (``Run:<fn>``) follow them, for the same reason.

    Run options are the third discriminator, added 2026-09-14 for the same
    failure one axis over: ``distribute``/``as_table`` are part of
    ``invocation_id``, so a loader re-run with ``distribute=true`` wrote a
    second record beside every ``distribute=false`` one — same code, same
    constants — and the two were drawn on top of each other trial for trial.
    ``run_chain`` (scidb) names, per upstream function that has run more than
    one way, the options it ran under for this record; one ``Run:<fn>`` column
    each, and the latest flag is attached for these exactly as for code.

    **Only columns that actually distinguish something are attached.** A branch
    param holding the same value on every record is dropped: it cannot separate
    two records, so as a factor it asks the user to choose between one thing,
    and the ``variant`` tag makes it look like a swept parameter that needs a
    decision. Code axes were always filtered this way (scidb omits
    single-version functions); branch params were not, because
    ``branch_params_batch`` returns every upstream *constant* regardless of
    whether it varies. Absence counts as a value in that test — a key present on
    some records and missing on others does tell them apart.

    See ``docs/claude/variant-selection.md`` and
    ``docs/claude/function-version-variants.md``.
    """
    from scidb.provenance_query import variant_identity_batch

    record_ids = frame["record_id"].tolist()
    ident = variant_identity_batch(db._duck, record_ids)

    # --- code chain: one column per multi-version upstream function ---
    # Sorted by function name so the column order is a property of the data and
    # not of dict iteration — a saved PlotSpec must keep meaning the same thing.
    fn_names = sorted(
        {name for info in ident.values() for name in info.get("code_chain", {})}
    )
    code_keys: list[str] = []
    axes: list[dict] = []
    for fn_name in fn_names:
        column = f"{VERSION_FACTOR_PREFIX}{fn_name}"
        while column in frame.columns:  # never shadow a schema key
            column += "_"
        frame[column] = [
            (ident.get(rid) or {}).get("code_chain", {}).get(
                fn_name, MISSING_VERSION_LEVEL
            )
            for rid in record_ids
        ]
        code_keys.append(column)
        axes.append(
            {"column": column, "kind": "code", "function": fn_name, "param": None}
        )

    # --- run options: one column per upstream function that ran >1 way ---
    # scidb already omits functions with a single option set (`run_option_axes`),
    # but a function may have run both ways producing OTHER types; the
    # branch-param rule below (drop a column with one level over THESE records)
    # applies here too so a single-level column never demands a role.
    run_fn_names = sorted(
        {name for info in ident.values() for name in info.get("run_chain", {})}
    )
    run_keys: list[str] = []
    for fn_name in run_fn_names:
        column = f"{RUN_FACTOR_PREFIX}{fn_name}"
        while column in frame.columns:  # never shadow a schema key
            column += "_"
        values = [
            (ident.get(rid) or {}).get("run_chain", {}).get(
                fn_name, MISSING_VERSION_LEVEL
            )
            for rid in record_ids
        ]
        if len(set(values)) <= 1:
            Log.info(
                "%s ran under >1 run-option set overall but only %r over these "
                "%d record(s) — not an axis here",
                fn_name,
                values[0] if values else None,
                len(record_ids),
                layer=LAYER,
            )
            continue
        frame[column] = values
        run_keys.append(column)
        axes.append(
            {"column": column, "kind": "run", "function": fn_name, "param": None}
        )

    # --- branch params ---
    candidate_keys: list[str] = []
    for info in ident.values():
        for key in info["branch_params"]:
            if key not in candidate_keys:
                candidate_keys.append(key)

    param_keys: list[str] = []
    constants: dict[str, Any] = {}
    for key in candidate_keys:
        values = [
            _stringify(ident.get(rid, {}).get("branch_params", {}).get(key))
            for rid in record_ids
        ]
        # A constant is not an axis.
        #
        # A variant column exists for exactly one reason: to stop records that
        # DIFFER from being overplotted as replicates. A column holding the same
        # value on every record cannot do that, so offering it as a factor asks
        # the user to choose between one thing — and, being tagged `variant`, it
        # is indistinguishable from a genuinely swept parameter until you count
        # its levels.
        #
        # Code axes have had this guard from the start: scidb's
        # `code_version_ordinals` omits single-version functions, which is why an
        # unedited project gets no `Code:` columns at all. Branch params never
        # got the matching rule, because `branch_params_batch` returns every
        # upstream CONSTANT whether it varies or not — the name promises a branch,
        # the query does not check for one. Measured on a real project
        # (2026-09-11): `filterDelsys.config` and `filterDelsys.Fs` each held ONE
        # level across both records and still demanded a role.
        #
        # **Absence counts as a value.** The test is over the raw list, not over
        # the non-null values, because a key present on some records and missing
        # on others genuinely does tell them apart — dropping it there would
        # reintroduce the very overplotting this function exists to prevent. A
        # key missing everywhere is all-None, one distinct value, and goes.
        #
        # Known gap, pre-existing and deliberately not addressed here: a
        # partially-present key survives with None on the records that lack it,
        # and `FactorInfo.levels` drops nulls, so those rows carry a level-less
        # NaN. Code axes solve this with MISSING_VERSION_LEVEL; branch params
        # have no equivalent sentinel yet.
        if len(set(values)) <= 1:
            constants[key] = values[0] if values else None
            continue
        frame[key] = values
        param_keys.append(key)
        # Branch params are namespaced ``{producing_fn}.{param}`` by
        # `_build_upstream_closure`. A bare key (no dot) is possible in principle
        # and split defensively rather than assumed away.
        function, _, param = key.rpartition(".")
        axes.append(
            {
                "column": key,
                "kind": "param",
                "function": function or None,
                "param": param,
            }
        )

    keys = code_keys + run_keys + param_keys

    if constants:
        # Never silent. These values are real provenance — they are simply not a
        # CHOICE — and a user hunting for the filter cutoff they swept needs to
        # see that this layer looked at it and found one value, rather than that
        # it was never there.
        Log.info(
            "%d constant branch param(s) are not variant axes (one value over "
            "%d record(s)): %s",
            len(constants),
            len(record_ids),
            {key: _truncate(value) for key, value in sorted(constants.items())},
            layer=LAYER,
        )

    if keys:
        counts = {
            column: int(frame[column].astype(str).nunique(dropna=False))
            for column in keys
        }
        # A code axis reporting one level means scidb emitted a single-version
        # function, which `code_version_ordinals` promises not to do. Deliberately
        # NOT filtered here: duplicating that rule would hide the regression
        # instead of surfacing it.
        thin = sorted(
            column for column in code_keys if counts.get(column, 0) <= 1
        )
        if thin:
            Log.warn(
                "code axis/axes %s hold one version — scidb is expected to omit "
                "single-version functions, so this is a provenance bug, not a "
                "plotting one",
                thin,
                layer=LAYER,
            )
        Log.info(
            "variant axis levels over %d record(s): %s",
            len(record_ids),
            counts,
            layer=LAYER,
        )

    latest_column = None
    if code_keys or run_keys:
        # `is_latest` is one chain-wide, per-location flag over code AND run
        # options, so it answers a run-option axis exactly as it answers a code
        # axis — and the default pin needs it for either.
        latest_column = LATEST_COLUMN
        while latest_column in frame.columns:
            latest_column += "_"
        # Deliberately NOT appended to `keys`: it is a filter helper, not a
        # condition anyone plots by.
        frame[latest_column] = [
            bool((ident.get(rid) or {}).get("is_latest")) for rid in record_ids
        ]

        Log.info(
            "attached %d code column(s) %s and %d run-option column(s) %s over "
            "%d record(s) (%d row(s) current) — these would otherwise plot as "
            "replicates of each other",
            len(code_keys),
            code_keys,
            len(run_keys),
            run_keys,
            len(record_ids),
            int(frame[latest_column].sum()),
            layer=LAYER,
        )

    if not keys:
        return frame, [], None, []

    Log.debug("attached %d variant column(s): %s", len(keys), keys, layer=LAYER)
    return frame, keys, latest_column, axes


def _stringify(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value)
    return str(value)


def _truncate(value: Any, limit: int = 60) -> Any:
    """Shorten a value for a log line.

    A struct-valued branch param stringifies to its whole repr — measured at
    ~100 characters for one filter config, and unbounded in principle. Several
    of those on one line buries the key names the line exists to report.
    """
    if not isinstance(value, str) or len(value) <= limit:
        return value
    return f"{value[: limit - 1]}…"
