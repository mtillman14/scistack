"""
``ScidbSource`` — the scidb implementation of scistackplot's ``DataSource``.

This is the entire compatibility mechanism: the GUI and every plotting
function above it talk to the protocol, so the same code path serves a lone CSV
(``CsvSource``) and a full scidb project. Nothing above this file knows about
DuckDB, records, or branch params.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pandas as pd
from scistacklog import Log
from scistackplot import (
    MISSING_LEVEL,
    FactorVariable,
    LongTable,
    Shape,
    classify_column,
    classify_value,
    is_plottable,
    natural_sort_key,
)
from scistackplot.dedup import SingleFlight
from scistackplot.framesize import format_extent, frame_extent
from scistackplot.sources import BaseSource
from scistackplot.variants import VARIABLE_COLUMN

from .hierarchy import join_frames, joinable, joined_levels
from .load import (
    LATEST_COLUMN,
    column_levels,
    data_column_types_for,
    data_columns_for,
    is_container_type,
    load_variable,
    registered_variables,
    sample_column_value,
    sample_value,
    schema_keys,
    variable_levels,
)

LAYER = "scistackplotdb"

#: Column name given to a dict/struct variable's field names when they are
#: melted into long format. Matches the stack's existing vocabulary for "the
#: name of a data column" — ``scidb.ColName()`` and ``PathOutput("{ColName}")``
#: (docs/claude/for-columns-iteration.md) — so a plot faceted by field and a
#: ``for_columns`` run name the same axis the same way.
FIELD_FACTOR = "ColName"

#: How many distinct values a column may hold and still be offered as a
#: grouping. Past this it is an identifier, not a group: a colour per level is
#: unreadable and a facet per level is hundreds of empty panels. Not a hard
#: limit — a spec naming such a column still works — only what
#: :meth:`ScidbSource.groupable_report` will put in front of a user.
MAX_GROUP_LEVELS = 50


def _as_levels(column):
    """A joined grouping column as factor levels: missing named, rest as text.

    Text because a factor level IS text everywhere else in the stack — schema
    keys are cast to VARCHAR on load for the same reason
    (``load.load_variable``).

    Deliberately these two calls and nothing cleverer: ``scistackplot.codegen``
    emits ``fillna(...).astype(str)`` into the exported endpoint, so anything
    extra here (normalising ``70.0`` to ``70``, say) would spell a level one way
    in the preview and another way in the export — the disagreement that
    synthetic-factors invariant 4 exists to prevent. The order matters:
    ``astype(str)`` first would turn every missing value into the string
    ``"nan"``.
    """
    return column.fillna(MISSING_LEVEL).astype(str)


class ScidbSource(BaseSource):
    """
    Plot data straight out of a scidb database.

    ``db`` is an open ``DatabaseManager``. The caller owns it — in the GUI that
    means the ONE manager the server already holds, never a second connection
    (a second DuckDB handle reintroduces the write-lock contention the MATLAB
    run-ownership work resolved).
    """

    def __init__(
        self, db, *, name: str | None = None, fast: bool = True
    ) -> None:
        self._db = db
        # `dataset_db_path`, not `db_path` — DatabaseManager has never had the
        # latter, so this silently fell through to "scidb" for every project.
        self.name = name or str(getattr(db, "dataset_db_path", None) or "scidb")
        self._frames: dict[str, Any] = {}
        # Data-LESS frames, for questions about a variable's provenance rather
        # than its values. A separate cache, deliberately: putting one of these
        # in `_frames` would hand a later plot a frame with no measure in it.
        self._variant_frames: dict[str, Any] = {}
        self._shapes: dict[str, Shape] = {}
        self._levels: dict[str, list[str]] = {}
        # Whether tables from this source carry the numpy reducer
        # (``scistackplot.NumpyReducer``, over the ndarray cells `load_variable`
        # delivers) or the pandas reference. On by default — the reference is
        # the path that spent 500 s on 174 M samples — and switchable so the
        # two can be run side by side on one database, which is how the parity
        # suite works and how a suspected disagreement gets bisected.
        #
        # A DuckDB-SQL reducer sat here for one day (2026-09-13) and lost every
        # measurement to numpy once the fetch stopped boxing samples
        # (.claude/plan-plot-minimal-load-examples.md §8). DuckDB selects rows;
        # numpy reduces them — so `resolve` no longer touches the database and
        # the GUI's connection hold ends when the frames are loaded.
        self._fast = fast
        self._reducer_instance = None

    def _reducer(self):
        """The reducer every table from this source carries (one per source)."""
        if not self._fast:
            return None  # -> reducer_for() supplies the pandas reference
        if self._reducer_instance is None:
            from scistackplot.reducer import NumpyReducer

            self._reducer_instance = NumpyReducer()
        return self._reducer_instance

    # ---- description -----------------------------------------------------

    def describe(self) -> dict:
        """
        Every plottable variable plus the schema keys, without loading data.

        Shapes come from one sampled value per variable (a single-row query),
        so opening the panel on a large database stays cheap; the levels come
        from ``_schema``, which is small by construction.
        """
        with Log.timer("describe", layer=LAYER):
            keys = schema_keys(self._db)
            measures = []
            for variable in registered_variables(self._db):
                shape = self._shape_of(variable)
                columns = data_columns_for(self._db, variable)
                measures.append(
                    {
                        "name": variable,
                        "display": variable,
                        "shape": str(shape),
                        "exploded": False,
                        "plottable": shape
                        in (Shape.SCALAR, Shape.SERIES_1D, Shape.MATRIX_2D),
                        "columns": columns,
                        "levels": self._levels_of(variable),
                    }
                )

            factors = []
            for key in keys:
                levels = self._schema_levels(key)  # one query per key, not two
                factors.append(
                    {
                        "name": key,
                        "display": key,
                        "levels": levels,
                        "level_count": len(levels),
                        "is_variant": False,
                    }
                )

            Log.info(
                "describe: %d variable(s), %d schema key(s)",
                len(measures),
                len(factors),
                layer=LAYER,
            )
            return {
                "name": self.name,
                "schema_keys": keys,
                "factors": factors,
                "measures": measures,
                "index_column": None,
            }

    def _shape_of(self, variable: str) -> Shape:
        if variable not in self._shapes:
            self._shapes[variable] = classify_value(sample_value(self._db, variable))
        return self._shapes[variable]

    def _levels_of(self, variable: str) -> list[str]:
        """Schema depth WITHOUT loading the variable — describe() asks for every
        registered variable, and loading each frame would read the whole
        database to open the panel."""
        if variable in self._frames:
            return self._frames[variable].levels
        if variable not in self._levels:
            self._levels[variable] = variable_levels(self._db, variable)
        return self._levels[variable]

    def _schema_levels(self, key: str) -> list[str]:
        rows = self._db._duck._fetchall(
            f'SELECT DISTINCT s."{key}" FROM _schema s WHERE s."{key}" IS NOT NULL'
        )
        values = [str(row[0]) for row in rows]
        return self._ordered(key, values)

    def _ordered(self, key: str, values: list[str]) -> list[str]:
        """
        Order a factor's levels.

        Declared key types decide, not pandas' default: a key declared
        ``"numeric"`` sorts numerically, and everything else goes through the
        natural-sort key so zero-padded string IDs ("01", "02", … "10") land in
        the order a reader expects instead of the lexicographic 1, 10, 2.
        """
        declared = getattr(self._db, "dataset_schema_key_types", None) or {}
        unique = list(dict.fromkeys(values))
        # "no value here" is not a level the sort has an opinion about — it goes
        # last wherever it appears, so a legend reads as the groups first and
        # the leftovers after them.
        has_missing = MISSING_LEVEL in unique
        if has_missing:
            unique = [v for v in unique if v != MISSING_LEVEL]

        def default(vals: list[str]) -> list[str]:
            if declared.get(key) == "numeric":
                def numeric_key(value: str):
                    try:
                        return (0, float(value))
                    except (TypeError, ValueError):
                        return (1, 0.0)

                return sorted(vals, key=numeric_key)
            return sorted(vals, key=natural_sort_key)

        # A DECLARED order (`[schema_keys]` in the project config) wins, and
        # everything it does not name is appended by the rule above — so a
        # session added after the file was written appears at the end instead
        # of vanishing. scidb owns the declaration; this only applies it.
        order = getattr(self._db, "dataset_schema_key_order", None) or {}
        if order.get(key):
            from scidb.schema_order import order_levels

            ordered = order_levels(key, unique, declared=order, fallback=default)
        else:
            ordered = default(unique)
        return [*ordered, MISSING_LEVEL] if has_missing else ordered

    # ---- metadata --------------------------------------------------------

    def variant_table(self, variable: str) -> LongTable:
        """A table carrying this variable's VARIANT structure and no payload.

        Same variant columns, levels, ``default_pin`` and latest-flag a full
        ``get_table`` would produce, built from a query that selects no data
        columns at all. ``measures`` is empty, so this is not plottable and must
        not be handed to ``resolve`` — it answers metadata questions only.

        Why it exists: :func:`scistackplot.default_selection` reads
        ``default_pin``, ``latest_column`` and the variant factors' levels, and
        nothing else — it never touches a measure column. Answering it through
        ``get_table`` meant loading 174 million samples / ~5.2 GB to read a
        handful of variant levels, which is why the schema-location picker timed
        out on a 419-location variable (.claude/plot-at-scale-plan.md §7).

        Cached and deduplicated like any other table, under its own key, so the
        picker opening twice costs one query.
        """
        key = ("__variants__", variable)
        memo = self._table_cache()
        label = f"variant_table({variable})"
        if key in memo:
            Log.info("%s: table cache HIT", label, layer=LAYER)
            return memo[key]

        def _build():
            if key in memo:
                Log.info("%s: table cache HIT (filled while waiting)", label, layer=LAYER)
                return memo[key]
            Log.info("%s: table cache MISS — building (no data columns)", label, layer=LAYER)
            # Shared with `variant_graph`: the picker asks both of these about
            # the same variable one call apart, and they want the same frame.
            variable_frame = self._variant_frame(variable)
            frame = variable_frame.frame
            latest = variable_frame.latest_column
            # Variant columns first, then schema keys — the same order and the
            # same `_ordered` level sorting `_build_table` uses, so a selection
            # derived here and one derived from the full table cannot disagree
            # about which level is "first".
            variant_columns = [
                c for c in variable_frame.variant_columns if c in frame.columns
            ]
            factors = list(variant_columns)
            factors.extend(key for key in variable_frame.levels if key in frame.columns)
            level_order = {
                name: self._ordered(
                    name, [str(v) for v in frame[name].dropna().unique()]
                )
                for name in factors
            }
            table = LongTable.from_frame(
                frame,
                factors=factors,
                measures=[],
                level_order=level_order,
                variant_factors=variant_columns,
                name=variable,
                default_pin={latest: True} if latest else None,
                latest_column=latest,
                factor_origins={
                    axis["column"]: axis for axis in variable_frame.variant_axes
                },
                schema_levels=variable_frame.levels,
            )
            memo[key] = table
            return table

        return self._table_single_flight().run(
            key,
            _build,
            on_wait=lambda: Log.info(
                "%s: build already in flight — waiting for it", label, layer=LAYER
            ),
        )

    # ---- data ------------------------------------------------------------

    def _variable_frame(self, variable: str):
        # Hit/miss at INFO: this cache is the difference between a request that
        # reads the whole variable and one that reads nothing, and a plot request
        # that returned in milliseconds is otherwise unattributable
        # (.claude/plot-at-scale-plan.md §1). The layer below (load_variable)
        # reports what a miss actually cost.
        if variable in self._frames:
            Log.info("variable frame cache HIT: %s", variable, layer=LAYER)
            return self._frames[variable]

        def _load():
            if variable in self._frames:
                Log.info(
                    "variable frame cache HIT: %s (filled while waiting)",
                    variable,
                    layer=LAYER,
                )
                return self._frames[variable]
            Log.info("variable frame cache MISS: %s — loading", variable, layer=LAYER)
            frame = load_variable(self._db, variable)
            self._frames[variable] = frame
            return frame

        # Deduped for the same reason get_table is, one layer down: two DIFFERENT
        # table keys over the same variable (a plot's measure and a factor join,
        # say) both land here, and each would otherwise read the whole variable.
        return self._frame_single_flight().run(
            variable,
            _load,
            on_wait=lambda: Log.info(
                "variable frame load already in flight: %s — waiting for it",
                variable,
                layer=LAYER,
            ),
        )

    def _frame_single_flight(self):
        """The in-flight map for :meth:`_variable_frame`, created on first use."""
        from scistackplot.sources.base import _lazy_attr

        return _lazy_attr(self, "_frame_inflight", SingleFlight)

    def _build_table(
        self,
        measures: list[str],
        *,
        x_measure: str | None = None,
        factor_variables: list[FactorVariable] | None = None,
    ) -> LongTable:
        """
        Build the long table for a plot's variables.

        Called by :meth:`~scistackplot.sources.base.BaseSource.get_table`, which
        memoizes the result — this method always does the full work.

        ``measures`` are the y variables. Several of them **stack**: one value
        column plus a :data:`~scistackplot.variants.VARIABLE_COLUMN` saying
        which variable each row came from, which is what lets a variant row
        claim its own variable's rows and no others.

        ``x_measure`` is the x axis of a relational plot and **joins** instead —
        one x value per row of y, broadcast down the hierarchy when it lives at
        a shallower level (see :mod:`.hierarchy`).

        ``factor_variables`` are variables — or single columns of them — joined
        in as **factors** rather than plotted: a subject-level ``Condition``
        holding stim/sham, or the ``InterventionGroup`` column of a wide
        demographics table, becomes a column every row carries and takes a role
        like any other factor.

        The two are genuinely different operations, which is why they are
        different parameters. Stacking two variables that a wide join would
        have paired gives twice the rows and no pairing; joining two variables
        that should have stacked silently drops every row without a partner.
        """
        if not measures:
            raise ValueError("get_table needs at least one measure (variable name).")
        if len(measures) > 1 and x_measure is not None:
            raise ValueError(
                f"An x measure pairs with ONE y measure; got y={measures} and "
                f"x={x_measure!r}. Stacked variables have no single value to "
                f"pair each x with."
            )

        factor_variables = list(factor_variables or [])
        known = registered_variables(self._db)
        requested = [
            *measures,
            *([x_measure] if x_measure else []),
            *(group.variable for group in factor_variables),
        ]
        unknown = [name for name in requested if name not in known]
        if unknown:
            # Same failure shape as the CSV source's unknown-column error, so
            # callers (and the GUI) handle one kind of "no such measure".
            raise KeyError(f"Unknown variable(s) {unknown}. Available: {known}")

        if len(measures) > 1:
            return self._stacked_table(measures, factor_variables)

        primary = self._variable_frame(measures[0])
        field_columns: list[str] = []

        if x_measure is None:
            if len(primary.data_columns) > 1:
                frame, field_factor = self._melt_fields(primary, measures[0])
                field_columns = [field_factor]
            else:
                frame = self._named_frame(primary, measures[0])
            levels = primary.levels
            variant_columns = list(primary.variant_columns)
            variant_axes = list(primary.variant_axes)
            measure_names = [measures[0]]
            # Open on the current code version.
            latest_column = primary.latest_column
            default_pin = {latest_column: True} if latest_column else None
        else:
            secondary = self._variable_frame(x_measure)
            pair = (measures[0], x_measure)
            multi = [
                name
                for name, frame in zip(pair, (primary, secondary), strict=True)
                if len(frame.data_columns) > 1
            ]
            if multi:
                raise ValueError(
                    f"{multi} store one column per dict/struct field, so there is "
                    f"no single value to pair with another measure. Plot one of "
                    f"them on its own (its fields become subplots), or save the "
                    f"field you want as its own variable."
                )
            # Rename BEFORE joining. Two variables' data columns routinely share
            # a name ("value" is the default), and renaming after the merge
            # cannot separate them — one rename key silently shadows the other
            # and the first measure's column vanishes.
            left = replace(primary, frame=self._named_frame(primary, measures[0]))
            right = replace(secondary, frame=self._named_frame(secondary, x_measure))
            frame = join_frames(
                left,
                right,
                left_value=measures[0],
                right_value=x_measure,
            )
            levels = joined_levels(primary, secondary)
            variant_columns = list(
                dict.fromkeys(primary.variant_columns + secondary.variant_columns)
            )
            variant_axes = list(
                {
                    axis["column"]: axis
                    for axis in primary.variant_axes + secondary.variant_axes
                }.values()
            )
            measure_names = [measures[0], x_measure]
            # `join_frames` now carries both sides' flags through the merge and
            # ANDs them, so the pin-latest default applies to two-measure plots
            # too. It used to drop the flag, which made a relational scatter the
            # one place the default silently stopped protecting the figure.
            latest_column = LATEST_COLUMN if LATEST_COLUMN in frame.columns else None
            default_pin = {latest_column: True} if latest_column else None

        frame, group_depths = self._attach_factor_variables(
            frame, levels, factor_variables
        )

        # Variant columns first, and code versions lead within them (see
        # `attach_variants`). The variants are what a reader has to make a
        # decision about — a schema key is just where the data sits — so they
        # get the top of the factor list rather than whatever position the
        # schema happened to leave them.
        factors = [c for c in variant_columns if c in frame.columns]
        factors.extend(key for key in levels if key in frame.columns)
        # Keys: the attached factor names, in the order they were joined.
        factors.extend(group_depths)
        factors.extend(c for c in field_columns if c in frame.columns)

        level_order = {
            name: self._ordered(name, [str(v) for v in frame[name].dropna().unique()])
            for name in factors
        }

        table = LongTable.from_frame(
            frame,
            factors=factors,
            measures=measure_names,
            level_order=level_order,
            variant_factors=variant_columns,
            field_factors=field_columns,
            name=measures[0],
            default_pin=default_pin,
            latest_column=latest_column,
            factor_origins={axis["column"]: axis for axis in variant_axes},
            # Where each joined grouping sits in the hierarchy, so a nested x
            # axis puts a subject-level grouping OUTSIDE the sessions inside it.
            factor_depths=group_depths,
            # The variable's own schema depth, outermost first — the nesting
            # that decides which keys a fan-out has to iterate together and in
            # which order (roles.iterate_ancestors / roles.fanout_keys).
            schema_levels=levels,
        )
        Log.debug(
            "get_table(%s): %d row(s), factors=%s",
            measures,
            len(frame),
            factors,
            layer=LAYER,
        )
        table.reducer = self._reducer()
        return table

    def _attach_factor_variables(
        self, frame, levels: list[str], factor_variables: list[FactorVariable]
    ):
        """Join grouping variables onto the frame as ordinary factor columns.

        A ``Condition`` recorded per subject is broadcast down to every one of
        that subject's rows — the same prefix-merge :mod:`.hierarchy` performs
        for an x measure, and the reason "stim vs sham" needs no new concept
        once the database already records it.

        An entry naming a ``column`` takes that one column of a wide table
        (``Demographics.InterventionGroup``) instead of the variable's single
        value. That is the only way a spreadsheet of demographics can group a
        figure: the variable as a whole holds Age, Sex and InterventionGroup at
        once and so has no value to group by, which is exactly what the error
        below says when the column is left out.

        A grouping variable's own **variant columns are deliberately dropped**.
        Which version of the code produced a group label is not what the figure
        is comparing, and carrying those columns in would put a variant factor
        on screen that ``roles.validate`` then demands a role for. If a
        variable's variants are the subject, it belongs in a variant row.

        Returns the frame and ``{factor name: depth}`` — how many schema keys
        pin one of the factor's values, which is what nests a grouping OUTSIDE
        the keys recorded beneath it on a shared x axis
        (``FactorInfo.depth``). The number is the grouping variable's own
        schema depth, known here and nowhere downstream.
        """
        attached: dict[str, int] = {}
        if not factor_variables:
            return frame, attached

        for group in factor_variables:
            name = group.variable
            variable = self._variable_frame(name)
            source_column = self._factor_source_column(variable, group)
            factor = group.factor_name
            if factor in frame.columns:
                # Never merge over a column that is already there. pandas would
                # suffix one of them (`InterventionGroup_x`) and every role,
                # filter and y-scope naming the factor would then point at a
                # column nobody meant — a figure that is wrong in a way no
                # error message accounts for.
                raise ValueError(
                    f"Grouping by {group.label!r} would add a column named "
                    f"{factor!r}, which this table already has. Rename the "
                    f"column in the source data, or group by something else."
                )
            if len(variable.levels) > len(levels) or levels[
                : len(variable.levels)
            ] != list(variable.levels):
                # Deeper than the data, or a different branch of the schema:
                # merging would multiply rows or match nothing, and either way
                # the figure would be quietly wrong about how many observations
                # it holds.
                raise ValueError(
                    f"{group.label!r} sits at {variable.levels}, which is not a "
                    f"prefix of {levels} — there is no unambiguous way to "
                    f"attach one of its values to each row. Group by a variable "
                    f"recorded at or above the level of the data."
                )
            on = list(variable.levels)
            right = self._pinned_variant(variable, group)
            right = right.rename(columns={source_column: factor})
            right = self._one_label_per_location(right, on, factor, group)
            frame = frame.merge(right, on=on, how="left")

            # A row the grouping variable says nothing about keeps its place and
            # says so. Dropping it would remove data from the figure to answer a
            # question about grouping, and silently — see MISSING_LEVEL.
            missing = int(frame[factor].isna().sum())
            if missing:
                Log.warn(
                    "%r has no value for %d of %d row(s) — those rows are "
                    "grouped as %r rather than dropped",
                    group.label,
                    missing,
                    len(frame),
                    MISSING_LEVEL,
                    layer=LAYER,
                )
            frame[factor] = _as_levels(frame[factor])

            # Depth is the grouping variable's own schema depth: a subject-level
            # sheet pins one value per subject, so it sits at the same level as
            # `subject` and OUTSIDE `session` when both share the x axis.
            attached[factor] = len(on)
            n_levels = frame[factor].nunique(dropna=True)
            Log.info(
                "attached %r as factor %r on %s (%d level(s))",
                group.label,
                factor,
                on,
                n_levels,
                layer=LAYER,
            )
            if n_levels < 2:
                # Kept, unlike an auto-derived single-level variant column
                # (synthetic-factors invariant 6): the user ticked this one, so
                # dropping it would read as a broken checkbox. Say why it does
                # nothing instead.
                Log.warn(
                    "%r holds one level (%s) across this table, so it separates "
                    "nothing",
                    group.label,
                    list(frame[factor].unique()),
                    layer=LAYER,
                )
        return frame, attached

    def _pinned_variant(self, variable_frame, group: FactorVariable):
        """The grouping variable's rows, narrowed to the pinned variant.

        A grouping variable has variants like any other: edit the loader that
        reads the demographics sheet and two records per subject coexist, each
        with its own labels. Before this they were merged together and
        ``drop_duplicates`` silently kept whichever came first — a figure
        stratified by a version of the spreadsheet nobody chose.

        The pin is applied with :func:`scistackplot.variants.variant_set_mask`,
        the same one definition the measure's own variant rows use, so "what a
        selection keeps" cannot mean two things one call apart.

        An **empty** selection means nothing was said and nothing is filtered —
        the same inert reading an unfilled variant row has
        (``plot-variant-rows.md`` § "An unfilled row is inert"). What used to be
        silent about that state is now :meth:`_one_label_per_location`.
        """
        from scistackplot.variants import variant_set_mask

        selection = group.selection
        frame = variable_frame.frame
        if not selection:
            return frame

        mask = variant_set_mask(
            frame, selection, latest_column=variable_frame.latest_column
        )
        kept = frame[mask]
        Log.info(
            "%r pinned to %s: %d of %d record(s)",
            group.label,
            selection,
            len(kept),
            len(frame),
            layer=LAYER,
        )
        if kept.empty and not frame.empty:
            # Never silently: every row of the figure is about to be labelled
            # `(missing)`, which looks exactly like a spreadsheet with no
            # matching subjects. The pin is applied blindly by design (see
            # `variants.default_selection`), so an empty result is a state the
            # user can reach and has to be told about.
            Log.warn(
                "%r: the pinned variant %s matched no records — every row will "
                "be grouped as %r. Pick a different variant for this grouping.",
                group.label,
                selection,
                MISSING_LEVEL,
                layer=LAYER,
            )
        return kept

    def _one_label_per_location(self, right, on: list[str], factor: str, group):
        """``[*on, factor]``, one row per schema location — saying so if it had
        to choose.

        ``drop_duplicates`` is not new; being loud about what it dropped is. A
        pin need not resolve to exactly one record (two variants can differ in
        something this column does not depend on, or the selection may name a
        subcube), so the ambiguity survives pinning and the user has no other
        way to find out which label won.
        """
        right = right[[*on, factor]]
        # Cheap pre-check: with no duplicate locations there is nothing to
        # choose between, which is the ordinary case and must cost nothing.
        if on and right.duplicated(subset=on).any():
            per_location = right.groupby(on, dropna=False)[factor].nunique()
            ambiguous = int((per_location > 1).sum())
            if ambiguous:
                Log.warn(
                    "%r has more than one %s for %d schema location(s) even "
                    "after the variant pin — taking the first. Narrow the "
                    "grouping's variant to choose deliberately.",
                    group.label,
                    factor,
                    ambiguous,
                    layer=LAYER,
                )
        return right.drop_duplicates(subset=on)

    def _factor_source_column(self, variable_frame, group: FactorVariable) -> str:
        """Which column of the grouping variable's frame holds the labels."""
        columns = list(variable_frame.data_columns)
        if group.column is None:
            if len(columns) > 1:
                raise ValueError(
                    f"{group.variable!r} stores one column per field "
                    f"({columns}), so it has no single value to group by. Name "
                    f"the column to group by, e.g. "
                    f"FactorVariable({group.variable!r}, {columns[0]!r})."
                )
            if not columns:
                raise ValueError(
                    f"{group.variable!r} has no data column to group by."
                )
            return columns[0]
        if group.column not in columns:
            raise ValueError(
                f"{group.variable!r} has no column {group.column!r}. "
                f"It has: {columns}."
            )
        return group.column

    def _stacked_table(
        self,
        measures: list[str],
        factor_variables: list[FactorVariable] | None = None,
    ) -> LongTable:
        """Several variables as ONE measure plus a ``Variable`` column.

        The long form a multi-series figure needs: Raw and Filtered become rows
        of the same value column, told apart by a column the variant rows then
        consume into the ``Variant`` factor.

        The value column is named after the **primary** measure, so everything
        downstream — ``spec.y_measure``, the renderers, the generated
        ``y=`` argument — keeps working unchanged, and the measure's *label*
        carries every variable's name so the axis does not claim to be one of
        them. The alternative, a neutral column name, would have made
        ``spec.measures`` stop naming a real variable.
        """
        frames = [self._variable_frame(name) for name in measures]

        # Dict/struct variables stack too, provided they carry the SAME fields:
        # RawEMG and FilteredEMG, both keyed by muscle, are the archetypal
        # "plot these two together" case. Each is melted into one value column
        # plus a shared ``ColName`` factor first, so what stacks is the melted
        # long form — after which nothing downstream can tell the difference
        # between this and two scalar variables.
        multi = [f for f in frames if len(f.data_columns) > 1]
        if multi and len(multi) != len(frames):
            single = [f.name for f in frames if len(f.data_columns) == 1]
            raise ValueError(
                f"{[f.name for f in multi]} store one column per dict/struct "
                f"field, but {single} store a single value — there is no "
                f"correspondence between one number and a set of fields. Plot "
                f"them separately."
            )
        field_columns: list[str] = []
        if multi:
            shared = set(multi[0].data_columns)
            for frame in multi[1:]:
                shared &= set(frame.data_columns)
            if not shared:
                raise ValueError(
                    f"{measures} share no fields — "
                    + "; ".join(
                        f"{f.name} has {sorted(f.data_columns)}" for f in multi
                    )
                    + ". Stacking them would put every field on its own subplot "
                    "with a single series, which is not a comparison."
                )
            differing = {
                f.name: sorted(set(f.data_columns) - shared)
                for f in multi
                if set(f.data_columns) != shared
            }
            if differing:
                # Not fatal — the shared fields still compare — but never
                # silent: a muscle missing from one variable would otherwise
                # look like a subplot that simply has less data.
                Log.warn(
                    "stacking %s on their %d shared field(s); these appear in "
                    "only one variable and are dropped: %s",
                    measures,
                    len(shared),
                    differing,
                    layer=LAYER,
                )

        shapes = {f.name: self._shape_of(f.name) for f in frames}
        distinct = set(shapes.values())
        if len(distinct) > 1:
            raise ValueError(
                f"Variables plotted together must hold the same kind of value; "
                f"got { {k: str(v) for k, v in shapes.items()} }. A scalar and a "
                f"1-D signal have no common axis to share."
            )

        levels = frames[0].levels
        mismatched = {f.name: f.levels for f in frames if f.levels != levels}
        if mismatched:
            # Deliberately refused rather than broadcast. Broadcasting a
            # subject-level value across that subject's trials would make one
            # observation look like several — fine for an x axis (one x per y,
            # which `x_measure` does) but a silent inflation of n when the rows
            # are the data. Say so instead of guessing.
            raise ValueError(
                f"Variables plotted together must sit at the same schema level; "
                f"{measures[0]} is at {levels} but { mismatched } differ. Plot "
                f"them separately, or pair them with x_measure for a relational "
                f"plot (which broadcasts the shallower one)."
            )

        primary = measures[0]
        stacked = []
        for frame in frames:
            if multi:
                # Melt to the SHARED fields only, and into the primary's value
                # column, so every variable contributes the same two columns.
                named, field_factor = self._melt_fields(
                    frame, primary, fields=sorted(shared)
                )
                if field_factor not in field_columns:
                    field_columns.append(field_factor)
            else:
                named = self._named_frame(frame, primary)
            named[VARIABLE_COLUMN] = frame.name
            stacked.append(named)
        combined = pd.concat(stacked, ignore_index=True, sort=False)
        combined, group_depths = self._attach_factor_variables(
            combined, levels, list(factor_variables or [])
        )

        variant_columns = list(
            dict.fromkeys(c for f in frames for c in f.variant_columns)
        )
        variant_axes = list(
            {axis["column"]: axis for f in frames for axis in f.variant_axes}.values()
        )
        latest_column = (
            LATEST_COLUMN if LATEST_COLUMN in combined.columns else None
        )

        factors = [c for c in variant_columns if c in combined.columns]
        factors.extend(key for key in levels if key in combined.columns)
        # Keys: the attached factor names, in the order they were joined.
        factors.extend(group_depths)
        factors.extend(c for c in field_columns if c in combined.columns)
        factors.append(VARIABLE_COLUMN)
        level_order = {
            name: self._ordered(
                name, [str(v) for v in combined[name].dropna().unique()]
            )
            for name in factors
        }
        # Declared order, not observed: the user listed the variables.
        level_order[VARIABLE_COLUMN] = list(measures)

        table = LongTable.from_frame(
            combined,
            factors=factors,
            measures=[primary],
            level_order=level_order,
            variant_factors=variant_columns,
            # Marked as fields so `default_roles` gives them one subplot each —
            # 13 muscles overplotted on one axis is not a figure anyone wanted,
            # and that must hold whether one dict variable is plotted or two.
            field_factors=field_columns,
            name=primary,
            # No latest pin: the flag means different things per variable, and
            # the rows the user named are what selects here.
            default_pin=None,
            latest_column=latest_column,
            factor_origins={axis["column"]: axis for axis in variant_axes},
            factor_depths=group_depths,
            schema_levels=levels,
            measure_labels={primary: " / ".join(measures)},
        )
        Log.info(
            "stacked %s into one value column: %d row(s), levels=%s",
            measures,
            len(combined),
            levels,
            layer=LAYER,
        )
        table.reducer = self._reducer()
        return table

    def default_variant_for(self, variable: str) -> dict:
        """The variant a GROUPING by ``variable`` should open on: latest.

        Exactly :func:`scistackplot.variants.default_selection` over that
        variable's own variant table — the same rule, and the same *code*, that
        decides what a plot opens on. Two rules for "latest" would be two
        answers, and this one has to agree with the Variants section beside it.

        Read :func:`~scistackplot.variants.default_selection` before touching
        this: "latest" here is the per-schema-location **boolean flag**, never
        the string ``"latest"``. The string stops resolving through the flag the
        moment anything else is pinned in the same selection, and silently drops
        every location never re-run under the newest code.

        Cheap: ``variant_table`` selects no data columns and is cached, so
        offering a default for every groupable variable costs a handful of
        metadata queries rather than reading any of them.
        """
        from scistackplot.variants import default_selection

        return default_selection(self.variant_table(variable))

    def _variant_frame(self, variable: str):
        """The variable's frame for PROVENANCE questions — no data columns.

        ``variants.variant_graph`` reads ``record_id``, the variant columns and
        ``variant_axes``, and nothing else; ``variant_table`` needs the same
        three. Neither touches a measure, so neither should pay to read one.

        Reuses the full frame when the panel has already loaded it — the common
        case, and free. Otherwise loads without data, which is the case that
        matters: the grouping picker asks this for variables nobody has
        plotted, where ``_variable_frame`` would read every sample of an EMG
        variable to answer a question about its loader's version.

        Its own cache, never ``_frames``: a data-less frame stored there would
        be handed to a later plot as though it held the measure.
        """
        if variable in self._frames:
            return self._frames[variable]
        if variable not in self._variant_frames:
            Log.info(
                "variant frame MISS: %s — loading provenance only (no data)",
                variable,
                layer=LAYER,
            )
            self._variant_frames[variable] = load_variable(
                self._db, variable, include_data=False
            )
        return self._variant_frames[variable]

    def variant_graph(self, variable: str, functions: list[str] | None = None) -> dict:
        """Variant axes and per-function versions for ``variable``.

        A method rather than a bare function so it reuses this source's caches:
        the picker often opens over a variable the panel has already loaded, and
        re-reading it to answer "what versions exist" would double the cost of
        opening a dialog.

        Over :meth:`_variant_frame`, so the case where it has NOT been loaded
        costs a provenance read rather than the whole variable.
        """
        from .variants import variant_graph

        return variant_graph(self._db, self._variant_frame(variable), functions)

    def _melt_fields(
        self, variable_frame, measure: str, *, fields: list[str] | None = None
    ):
        """
        Turn a dict/struct variable's columns into ONE measure plus a field
        factor.

        scidb stores a dict-valued variable in ``multi_column`` mode — one
        DuckDB column per key (13 muscles of an EMG record become 13 columns;
        see docs/claude/multi-column-save-schema.md). Those columns are
        parallel quantities, not separate variables, so melting them into long
        format makes the field name an ordinary factor. It then gets one
        subplot per level by default (``default_roles``), and the user can move
        it to colour or separate figures like any other factor — which beats
        hardcoding subplots into the renderer.
        """
        frame = variable_frame.frame
        usable, skipped = [], []
        for column in variable_frame.data_columns:
            if is_plottable(classify_column(frame[column])):
                usable.append(column)
            else:
                skipped.append(column)
        if skipped:
            Log.warn(
                "variable %r: %d field(s) are not plottable and were dropped: %s",
                variable_frame.name,
                len(skipped),
                skipped,
                layer=LAYER,
            )
        if not usable:
            raise ValueError(
                f"Variable {variable_frame.name!r} has no plottable fields "
                f"(columns: {variable_frame.data_columns})."
            )

        if fields is not None:
            # Stacking with another dict variable: melt only the fields they
            # share, so every variable contributes the same ColName levels.
            usable = [column for column in usable if column in set(fields)]

        id_vars = [c for c in frame.columns if c not in variable_frame.data_columns]
        field_factor = FIELD_FACTOR
        while field_factor in id_vars:  # never shadow a schema key
            field_factor += "_"

        # Timed because this is where the row count multiplies by the field
        # count — a 10-field record becomes 10 rows, each still holding a whole
        # signal — and every one of those cells is carried through the rest of
        # the pipeline whether or not the plot asks for that field.
        with Log.timer(
            f"melt_fields({variable_frame.name})",
            layer=LAYER,
            extra=f"{len(usable)} field(s)",
        ):
            melted = frame.melt(
                id_vars=id_vars,
                value_vars=usable,
                var_name=field_factor,
                value_name=measure,
            )
        Log.info(
            "melted %r: %d field(s) -> %d row(s), %s, field factor %r",
            variable_frame.name,
            len(usable),
            len(melted),
            format_extent(frame_extent(melted, [measure])),
            field_factor,
            layer=LAYER,
        )
        return melted, field_factor

    def _named_frame(self, variable_frame, measure: str):
        """The variable's frame with its data column renamed to the measure."""
        return variable_frame.frame.rename(
            columns={self._value_column(variable_frame): measure}
        )

    def _value_column(self, variable_frame) -> str:
        columns = variable_frame.data_columns
        if not columns:
            raise ValueError(
                f"Variable {variable_frame.name!r} has no data column to plot."
            )
        if len(columns) > 1:
            Log.warn(
                "variable %r has %d data columns %s — plotting the first",
                variable_frame.name,
                len(columns),
                columns,
                layer=LAYER,
            )
        return columns[0]

    def stackable_with(self, measure: str) -> list[str]:
        """Variables that can be plotted as another series alongside ``measure``.

        The offers only. :meth:`stackable_report` is the same computation with
        the refusals kept, for a caller that has to *show* the rejected
        candidates rather than silently omit them.
        """
        return self.stackable_report(measure)["offered"]

    def stackable_report(self, measure: str) -> dict:
        """Variables that can be plotted as another SERIES alongside ``measure``.

        Stacking needs the same shape (a scalar and a signal share no axis) and
        the same schema level (see :meth:`_stacked_table` on why the shallower
        one is not broadcast).

        Dict/struct variables stack with each other when they **share fields** —
        RawEMG and FilteredEMG, both keyed by muscle, is the archetypal case —
        and each is melted to those shared fields first. What cannot stack is a
        dict with a plain value: there is no correspondence between one number
        and a set of fields.

        Distinct from :meth:`joinable_with`, which answers the different
        question of what can supply an x axis.

        Returns ``{"offered": [...], "rejected": {name: reason}}``. The refusals
        were computed here from the beginning but only ever logged; the variant
        picker draws every variable on the pipeline canvas, so a candidate it
        cannot offer has to say **why** in place rather than be absent. "EMG is
        not clickable" with no reason is indistinguishable from a broken dialog.
        """
        own_shape = self._shape_of(measure)
        own_levels = self._levels_of(measure)
        own_columns = data_columns_for(self._db, measure)
        own_fields = set(own_columns) if len(own_columns) > 1 else None
        result: list[str] = []
        rejected: dict[str, str] = {}
        for candidate in registered_variables(self._db):
            if candidate == measure:
                continue
            shape = self._shape_of(candidate)
            levels = self._levels_of(candidate)
            columns = data_columns_for(self._db, candidate)
            fields = set(columns) if len(columns) > 1 else None
            if shape is not own_shape:
                rejected[candidate] = f"shape {shape} != {own_shape}"
            elif (fields is None) != (own_fields is None):
                rejected[candidate] = (
                    "one is a dict/struct and the other a single value"
                )
            elif fields is not None and not (fields & own_fields):
                rejected[candidate] = (
                    f"no shared fields (has {sorted(fields)})"
                )
            elif levels != own_levels:
                rejected[candidate] = f"schema level {levels} != {own_levels}"
            else:
                result.append(candidate)
        # Say why, per candidate. An empty dropdown is indistinguishable from a
        # missing feature, and the three criteria here are strict enough that a
        # variable a user expected to see is the likely case, not the rare one.
        Log.info(
            "stackable_with(%s): %d offered %s; %d rejected %s",
            measure,
            len(result),
            result,
            len(rejected),
            rejected or "",
            layer=LAYER,
        )
        return {"offered": result, "rejected": rejected}

    def groupable_with(self, measure: str) -> list[FactorVariable]:
        """Groupings offered for ``measure``. See :meth:`groupable_report`."""
        return [
            FactorVariable(offer["variable"], offer["column"])
            for offer in self.groupable_report(measure)["offered"]
        ]

    def groupable_report(self, measure: str) -> dict:
        """Variables — and columns of variables — usable as a grouping FACTOR.

        Anything recorded at or above the measure's level: a per-subject
        ``Condition``, a per-session ``Protocol``. A variable with ONE data
        column is offered whole; a wide table (a demographics sheet) is offered
        **one entry per categorical column**, which is what makes
        ``Demographics.InterventionGroup`` a factor without saving it as its own
        variable.

        Returns ``{"offered": [...], "rejected": {label: reason}}``, the same
        shape as :meth:`stackable_report` and for the same reason: a column the
        user can see in their spreadsheet and not in this list has to say why.
        Only candidates that got as far as their *columns* are refused here —
        a variable recorded deeper than the measure is not a near miss, it is
        most of the database.

        Numeric *columns* are refused rather than offered: grouping by ``Age``
        means grouping by RANGES of age, and offering the raw column would
        produce one level per distinct age. A numeric *variable* keeps the
        older, looser rule (a group coded 1/2 is a group), because a scalar
        saved once per subject is what the user recorded as that subject's
        value. Columns are also refused for holding one value (a constant is not
        a factor) or more than :data:`MAX_GROUP_LEVELS` of them.

        **This is now the COMPOSITION of the two halves it used to be**:
        :meth:`groupable_variables` (cheap, schema only) followed by one
        :meth:`groupable_columns` per wide variable. It stays because
        :meth:`groupable_with` is the library API and the CSV source answers the
        same question in one call — but it is the expensive form, and the GUI
        no longer opens a panel with it.
        """
        report = self.groupable_variables(measure)
        rejected: dict[str, str] = dict(report["rejected"])
        buckets: dict[str, list[dict]] = {
            "categorical": [],
            "numeric": [],
            "columns": [],
        }
        for entry in report["offered"]:
            if entry["kind"] != "columns":
                buckets[entry["kind"]].append(entry["offer"])
                continue
            # ONCE per wide variable. Asking twice — offers here, refusals in a
            # second pass — would double the very DISTINCT queries this split
            # exists to ration.
            columns = self.groupable_columns(measure, entry["variable"])
            buckets["columns"].extend(columns["offered"])
            rejected.update(columns["rejected"])

        offered = [*buckets["categorical"], *buckets["numeric"], *buckets["columns"]]
        Log.info(
            "groupable_with(%s): %d offered %s; %d rejected %s",
            measure,
            len(offered),
            [o["label"] for o in offered],
            len(rejected),
            rejected or "",
            layer=LAYER,
        )
        return {"offered": offered, "rejected": rejected}

    def groupable_variables(self, measure: str) -> dict:
        """Which VARIABLES may group ``measure`` — **from the schema alone**.

        The canvas half of the Grouping picker: one entry per variable node,
        answering "can I click this, and if not why not", with **no value read
        and no DISTINCT**. The columns of a wide variable are a separate, more
        expensive question, asked once per click by :meth:`groupable_columns`.

        Why the split. Answering both together cost ~8 s on every panel open of
        the user's project (scidb.log 2026-09-15): `FilteredDelsys` 4.2 s and
        `RawEMG` 5.4 s, both offering nothing, because classifying a column
        meant fetching one of its EMG traces. Stage 4 made *that* cheap; this
        makes the per-column work happen only when someone asks for a specific
        variable's columns.

        Each offer carries ``kind``:

        * ``"categorical"`` / ``"numeric"`` — a single-data-column variable,
          offerable whole. ``offer`` holds the ready-made entry, levels
          included, because deciding this at all already required the one
          ``DISTINCT`` that produces them.
        * ``"columns"`` — a wide table. ``column_count`` is how many of its
          columns could hold a label at all (container-typed ones are already
          out); WHICH of them qualify needs :meth:`groupable_columns`.

        ``kind`` also fixes the order :meth:`groupable_report` reassembles in —
        categorical groups lead — so the two cannot drift about what comes
        first.
        """
        own = self._levels_of(measure)
        offered: list[dict] = []
        rejected: dict[str, str] = {}

        for candidate in registered_variables(self._db):
            if candidate == measure:
                continue
            levels = self._levels_of(candidate)
            if len(levels) > len(own) or own[: len(levels)] != levels:
                # Deeper than the data, or a different branch: `_attach_factor_
                # variables` would refuse it, and there are far too many of
                # these to be worth listing as refusals.
                continue
            # Types, not just names: one information_schema read answers both
            # "how many data columns" and "which of them can hold a label".
            types = data_column_types_for(self._db, candidate)
            if len(types) > 1:
                labelled = [c for c, t in types.items() if not is_container_type(t)]
                if not labelled:
                    # Every column holds a signal. Silent, like the per-column
                    # branch: a struct of signals sits at the measure's own
                    # level and is obviously not a sheet of labels, so listing
                    # it would bury the refusals that ARE near misses.
                    continue
                offered.append(
                    {
                        "variable": candidate,
                        "kind": "columns",
                        "label": candidate,
                        "column_count": len(labelled),
                    }
                )
                continue
            entry, reason = self._whole_variable_offer(candidate, list(types))
            if reason:
                rejected[candidate] = reason
            elif entry:
                offered.append(entry)

        Log.info(
            "groupable_variables(%s): %d offered %s; %d rejected — no column "
            "query made",
            measure,
            len(offered),
            [(o["label"], o["kind"]) for o in offered],
            len(rejected),
            layer=LAYER,
        )
        return {"offered": offered, "rejected": rejected}

    def _whole_variable_offer(
        self, candidate: str, columns: list[str]
    ) -> tuple[dict | None, str | None]:
        """A single-data-column variable as a grouping: ``(offer, refusal)``."""
        shape = self._shape_of(candidate)
        if shape in (Shape.SERIES_1D, Shape.MATRIX_2D):
            return None, (
                f"holds {shape} values — a signal is data, not a group label"
            )
        if shape is Shape.CATEGORICAL:
            # Its levels ARE the groups, so they are worth the one query — the
            # panel can show them, and a text column with a value per record is
            # as much an identifier here as it is in a wide sheet. One query per
            # VARIABLE, not per column, which is why it stays on the cheap path.
            found = column_levels(
                self._db, candidate, columns[0], limit=MAX_GROUP_LEVELS + 1
            )
            if len(found) > MAX_GROUP_LEVELS:
                return None, (
                    f"more than {MAX_GROUP_LEVELS} distinct values — an "
                    f"identifier, not a group"
                )
            return {
                "variable": candidate,
                "kind": "categorical",
                "label": candidate,
                "column_count": 1,
                "offer": self._offer(
                    candidate, None, self._ordered(candidate, found)
                ),
            }, None
        # Numeric and offered whole, unlike a numeric COLUMN: a scalar saved
        # once per subject is what the user recorded as that subject's value,
        # and a group coded 1/2 is still a group.
        return {
            "variable": candidate,
            "kind": "numeric",
            "label": candidate,
            "column_count": 1,
            "offer": self._offer(candidate, None, []),
        }, None

    @staticmethod
    def _offer(variable: str, column: str | None, levels: list[str]) -> dict:
        """One entry of an ``offered`` list, however it was arrived at."""
        group = FactorVariable(variable, column)
        return {
            "variable": variable,
            "column": column,
            "label": group.label,
            "name": group.factor_name,
            "levels": levels,
            "level_count": len(levels),
        }

    def groupable_columns(self, measure: str, variable: str) -> dict:
        """Which COLUMNS of ``variable`` may group ``measure``.

        The click half of the picker, paid once per variable the user opens
        rather than once per panel — see :meth:`groupable_variables`. Same
        ``{"offered": [...], "rejected": {label: reason}}`` shape as every other
        report here.

        ``measure`` is taken so the signature matches the question being asked
        ("group THIS figure by a column of THAT variable") and so the level
        check can move here later; today the columns of a variable do not depend
        on it.
        """
        rejected: dict[str, str] = {}
        types = data_column_types_for(self._db, variable)
        offered = self._groupable_columns(variable, types, rejected, self._offer)
        return {"offered": offered, "rejected": rejected}

    def _groupable_columns(
        self, variable: str, types: dict[str, str], rejected: dict, offer
    ) -> list[dict]:
        """The columns of one wide table that may group a figure.

        One ``DISTINCT … LIMIT`` per candidate column, timed: a demographics
        sheet is small, but this runs for every wide variable at or above the
        measure's level every time the panel opens, and a slow describe is the
        kind of cost that is impossible to attribute afterwards.

        **Container-typed columns are refused from the schema alone**, before
        any value is read. Classifying them by value meant one whole EMG trace
        fetched per muscle and thrown away: 4.2 s for ``FilteredDelsys`` and
        5.4 s for ``RawEMG`` on the user's project (scidb.log 2026-09-15), ~8 s
        added to every panel open by two variables that offer nothing. The type
        check only ever REFUSES — see :func:`~scistackplotdb.load.
        is_container_type` for why that asymmetry is what keeps it safe.
        """
        found: list[dict] = []
        skipped_by_type = 0
        with Log.timer(f"groupable_columns({variable})", layer=LAYER):
            for column, data_type in types.items():
                label = f"{variable}.{column}"
                if is_container_type(data_type):
                    # Same outcome as the SERIES_1D/MATRIX_2D branch below and
                    # silent for the same reason — a struct of signals is
                    # obviously not a sheet of labels — but reached without
                    # reading a value.
                    skipped_by_type += 1
                    continue
                shape = classify_value(
                    sample_column_value(self._db, variable, column)
                )
                if shape in (Shape.SERIES_1D, Shape.MATRIX_2D, Shape.UNKNOWN):
                    # Silent, unlike the refusals below: a struct of signals
                    # (EMG, one column per muscle) sits at the measure's own
                    # level and is obviously not a sheet of labels, so naming
                    # every muscle as a rejected grouping would bury the
                    # refusals that ARE near misses under a dozen that are not.
                    continue
                if shape is not Shape.CATEGORICAL:
                    rejected[label] = (
                        "numeric column — grouping by ranges of it is not "
                        "offered yet"
                    )
                    continue
                levels = column_levels(
                    self._db, variable, column, limit=MAX_GROUP_LEVELS + 1
                )
                if len(levels) > MAX_GROUP_LEVELS:
                    rejected[label] = (
                        f"more than {MAX_GROUP_LEVELS} distinct values — an "
                        f"identifier, not a group"
                    )
                    continue
                if len(levels) < 2:
                    # A constant is not a factor (synthetic-factors invariant 6).
                    rejected[label] = (
                        f"one value ({levels[0]!r}) across every record"
                        if levels
                        else "no values"
                    )
                    continue
                found.append(offer(variable, column, self._ordered(column, levels)))
        if skipped_by_type:
            # At INFO: this is the difference between "the panel opened slowly"
            # and "the panel opened slowly because of these 13 columns", and the
            # count is the only visible sign the pre-filter is doing its job.
            Log.info(
                "groupable_columns(%s): %d of %d column(s) refused on their "
                "declared type alone — no value read",
                variable,
                skipped_by_type,
                len(types),
                layer=LAYER,
            )
        return found

    def joinable_with(self, measure: str) -> list[str]:
        """Variables that can supply an x axis for ``measure``."""
        own = self._levels_of(measure)
        result = []
        for candidate in registered_variables(self._db):
            if candidate == measure:
                continue
            if self._shape_of(candidate) is not Shape.SCALAR:
                continue  # an x axis must be scalar
            if len(data_columns_for(self._db, candidate)) > 1:
                continue  # a struct has no single value to put on an axis
            if joinable(own, self._levels_of(candidate)):
                result.append(candidate)
        return result

    def default_measure(self) -> str | None:
        for variable in registered_variables(self._db):
            if self._shape_of(variable) in (Shape.SCALAR, Shape.SERIES_1D):
                return variable
        return None

    def invalidate(self, variable: str | None = None) -> None:
        """Drop cached frames after a pipeline run has written new records."""
        if variable is None:
            self._frames.clear()
            self._variant_frames.clear()
            self._shapes.clear()
            self._levels.clear()
        else:
            self._frames.pop(variable, None)
            # Provenance goes stale with the data: a re-run writes new records
            # under a new function version, which is exactly what this cache
            # holds the answer to.
            self._variant_frames.pop(variable, None)
            self._shapes.pop(variable, None)
            self._levels.pop(variable, None)
        # Built tables are derived from those frames, so they are stale too —
        # and they are keyed by measure NAMES, which cannot say which variable a
        # stacked table drew from. Dropping all of them is the only answer that
        # is right for the per-variable case as well.
        self.invalidate_tables()
