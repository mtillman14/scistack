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
    LongTable,
    Shape,
    classify_column,
    classify_value,
    is_plottable,
    natural_sort_key,
)
from scistackplot.sources import BaseSource
from scistackplot.variants import VARIABLE_COLUMN

from .hierarchy import join_frames, joinable, joined_levels
from .load import (
    LATEST_COLUMN,
    data_columns_for,
    load_variable,
    registered_variables,
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


class ScidbSource(BaseSource):
    """
    Plot data straight out of a scidb database.

    ``db`` is an open ``DatabaseManager``. The caller owns it — in the GUI that
    means the ONE manager the server already holds, never a second connection
    (a second DuckDB handle reintroduces the write-lock contention the MATLAB
    run-ownership work resolved).
    """

    def __init__(self, db, *, name: str | None = None) -> None:
        self._db = db
        # `dataset_db_path`, not `db_path` — DatabaseManager has never had the
        # latter, so this silently fell through to "scidb" for every project.
        self.name = name or str(getattr(db, "dataset_db_path", None) or "scidb")
        self._frames: dict[str, Any] = {}
        self._shapes: dict[str, Shape] = {}
        self._levels: dict[str, list[str]] = {}

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
        if declared.get(key) == "numeric":
            def numeric_key(value: str):
                try:
                    return (0, float(value))
                except (TypeError, ValueError):
                    return (1, 0.0)

            return sorted(unique, key=numeric_key)
        return sorted(unique, key=natural_sort_key)

    # ---- data ------------------------------------------------------------

    def _variable_frame(self, variable: str):
        if variable not in self._frames:
            self._frames[variable] = load_variable(self._db, variable)
        return self._frames[variable]

    def get_table(
        self,
        measures: list[str],
        *,
        x_measure: str | None = None,
        factor_variables: list[str] | None = None,
    ) -> LongTable:
        """
        Build the long table for a plot's variables.

        ``measures`` are the y variables. Several of them **stack**: one value
        column plus a :data:`~scistackplot.variants.VARIABLE_COLUMN` saying
        which variable each row came from, which is what lets a variant row
        claim its own variable's rows and no others.

        ``x_measure`` is the x axis of a relational plot and **joins** instead —
        one x value per row of y, broadcast down the hierarchy when it lives at
        a shallower level (see :mod:`.hierarchy`).

        ``factor_variables`` are variables joined in as **factors** rather than
        plotted: a subject-level ``Condition`` holding stim/sham becomes a
        column every row carries, and takes a role like any other factor.

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
            *factor_variables,
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

        frame, group_columns = self._attach_factor_variables(
            frame, levels, factor_variables
        )

        # Variant columns first, and code versions lead within them (see
        # `attach_variants`). The variants are what a reader has to make a
        # decision about — a schema key is just where the data sits — so they
        # get the top of the factor list rather than whatever position the
        # schema happened to leave them.
        factors = [c for c in variant_columns if c in frame.columns]
        factors.extend(key for key in levels if key in frame.columns)
        factors.extend(group_columns)
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
        return table

    def _attach_factor_variables(
        self, frame, levels: list[str], factor_variables: list[str]
    ):
        """Join grouping variables onto the frame as ordinary factor columns.

        A ``Condition`` recorded per subject is broadcast down to every one of
        that subject's rows — the same prefix-merge :mod:`.hierarchy` performs
        for an x measure, and the reason "stim vs sham" needs no new concept
        once the database already records it.

        A grouping variable's own **variant columns are deliberately dropped**.
        Which version of the code produced a group label is not what the figure
        is comparing, and carrying those columns in would put a variant factor
        on screen that ``roles.validate`` then demands a role for. If a
        variable's variants are the subject, it belongs in a variant row.
        """
        attached: list[str] = []
        if not factor_variables:
            return frame, attached

        for name in factor_variables:
            variable = self._variable_frame(name)
            if len(variable.data_columns) > 1:
                raise ValueError(
                    f"{name!r} stores one column per dict/struct field, so it "
                    f"has no single value to group by."
                )
            if len(variable.levels) > len(levels) or levels[
                : len(variable.levels)
            ] != list(variable.levels):
                # Deeper than the data, or a different branch of the schema:
                # merging would multiply rows or match nothing, and either way
                # the figure would be quietly wrong about how many observations
                # it holds.
                raise ValueError(
                    f"{name!r} sits at {variable.levels}, which is not a prefix "
                    f"of {levels} — there is no unambiguous way to attach one "
                    f"of its values to each row. Group by a variable recorded "
                    f"at or above the level of the data."
                )
            on = list(variable.levels)
            right = self._named_frame(variable, name)[[*on, name]].drop_duplicates(
                subset=on
            )
            frame = frame.merge(right, on=on, how="left")
            attached.append(name)
            Log.info(
                "attached %r as a factor on %s (%d level(s))",
                name,
                on,
                frame[name].nunique(dropna=True),
                layer=LAYER,
            )
        return frame, attached

    def _stacked_table(
        self, measures: list[str], factor_variables: list[str] | None = None
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
        combined, group_columns = self._attach_factor_variables(
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
        factors.extend(group_columns)
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
        return table

    def variant_graph(self, variable: str, functions: list[str] | None = None) -> dict:
        """Variant axes and per-function versions for ``variable``.

        A method rather than a bare function so it reuses this source's frame
        cache: the picker opens over a variable the panel has already loaded,
        and re-reading it to answer "what versions exist" would double the cost
        of opening a dialog.
        """
        from .variants import variant_graph

        return variant_graph(self._db, self._variable_frame(variable), functions)

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

        melted = frame.melt(
            id_vars=id_vars,
            value_vars=usable,
            var_name=field_factor,
            value_name=measure,
        )
        Log.info(
            "melted %r: %d field(s) -> %d row(s), field factor %r",
            variable_frame.name,
            len(usable),
            len(melted),
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

    def groupable_with(self, measure: str) -> list[str]:
        """Variables usable as a grouping FACTOR for ``measure``.

        Anything recorded at or above the measure's level, with one data column
        — a per-subject ``Condition``, a per-session ``Protocol``. Categorical
        ones come first because that is what a group usually is, but numeric
        ones are offered too rather than guessed at: a group coded ``1``/``2``
        is a group, and ``sources/csv.py`` already documents that bare numeric
        IDs are indistinguishable from measurements by shape alone.
        """
        own = self._levels_of(measure)
        categorical: list[str] = []
        other: list[str] = []
        for candidate in registered_variables(self._db):
            if candidate == measure:
                continue
            if len(data_columns_for(self._db, candidate)) > 1:
                continue
            levels = self._levels_of(candidate)
            if len(levels) > len(own) or own[: len(levels)] != levels:
                continue
            target = (
                categorical
                if self._shape_of(candidate) is Shape.CATEGORICAL
                else other
            )
            target.append(candidate)
        return categorical + other

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
            self._shapes.clear()
            self._levels.clear()
        else:
            self._frames.pop(variable, None)
            self._shapes.pop(variable, None)
            self._levels.pop(variable, None)
