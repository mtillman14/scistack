"""
Loading scidb variables into long-format frames.

The long format itself is nearly free — schema keys are already ordinary
columns once a variable is joined to ``_schema``, which is the same shape
``stat_`` functions receive via ``as_table``. What this module adds is the
part a flat CSV never needed: attaching branch params as columns, and knowing
which schema keys a given variable actually occupies.

Queries go through ``_fetchall``/``_fetchone`` (never ``_execute(...).fetchall()``
— see docs/claude on DuckDB fetch locking) and batch the branch-params walk
rather than asking per record (the N+1 trap).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from scistacklog import Log
from scistackplot import CODE_FACTOR_PREFIX

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
    table = table_name_for(db, variable)
    rows = db._duck._fetchall(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? AND column_name != 'record_id' "
        "ORDER BY ordinal_position",
        [table],
    )
    return [row[0] for row in rows]


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


def load_variable(db, variable: str, *, with_variants: bool = True) -> VariableFrame:
    """Load every non-excluded record of ``variable`` as a long frame."""
    with Log.timer("load_variable", layer=LAYER, extra=variable):
        keys = schema_keys(db)
        columns = data_columns_for(db, variable)
        if not columns:
            Log.warn("variable %r has no data columns", variable, layer=LAYER)
            return VariableFrame(name=variable, frame=pd.DataFrame())

        table = table_name_for(db, variable)
        schema_select = "".join(f', s."{key}"' for key in keys)
        data_select = "".join(f', t."{column}"' for column in columns)
        query = (
            f"SELECT t.record_id{data_select}{schema_select} "
            f'FROM "{table}" t '
            f"JOIN _record r ON t.record_id = r.record_id "
            f"LEFT JOIN _schema s ON r.schema_id = s.schema_id "
            f"WHERE r.type = ? AND r.excluded IS DISTINCT FROM TRUE"
        )
        rows = db._duck._fetchall(query, [variable])

        frame = pd.DataFrame(
            rows, columns=["record_id", *columns, *keys]
        )
        for key in keys:
            frame[key] = frame[key].map(lambda v: None if v is None else str(v))

        levels = [key for key in keys if frame[key].notna().any()]
        variant_columns: list[str] = []
        variant_axes: list[dict] = []
        latest_column: str | None = None
        if with_variants and len(frame):
            frame, variant_columns, latest_column, variant_axes = attach_variants(
                db, frame
            )

        Log.info(
            "loaded %s: %d record(s), levels=%s, variants=%s",
            variable,
            len(frame),
            levels,
            variant_columns or "none",
            layer=LAYER,
        )
        return VariableFrame(
            name=variable,
            frame=frame,
            levels=levels,
            data_columns=columns,
            variant_columns=variant_columns,
            variant_axes=variant_axes,
            latest_column=latest_column,
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
    branch-param iteration order happened to put them.

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

    keys = code_keys + param_keys

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
    if code_keys:
        latest_column = LATEST_COLUMN
        while latest_column in frame.columns:
            latest_column += "_"
        # Deliberately NOT appended to `keys`: it is a filter helper, not a
        # condition anyone plots by.
        frame[latest_column] = [
            bool((ident.get(rid) or {}).get("is_latest")) for rid in record_ids
        ]

        Log.info(
            "attached %d code column(s) %s over %d record(s) (%d row(s) current) "
            "— these would otherwise plot as replicates of each other",
            len(code_keys),
            code_keys,
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
