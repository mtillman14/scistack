"""
The ``DataSource`` protocol — the one seam that makes this package both
standalone and scistack-compatible.

``scistackplot`` ships CSV and DataFrame implementations; ``scistackplotdb``
ships the scidb one. The GUI talks only to this protocol and never learns which
implementation it has, so the same application serves a lone CSV file and a
full scidb project. Everything above this line is pure long-table logic.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Protocol, runtime_checkable

from scistacklog import Log

from ..dedup import SingleFlight
from ..framesize import format_extent, frame_extent
from ..spec import FactorVariable
from ..table import LongTable

LAYER = "scistackplot"

#: Guards the lazy per-source attributes below. A plain ``getattr``-or-create is
#: not safe here: the server runs a thread per request and several arrive together
#: (see :mod:`scistackplot.dedup`), so two threads could each build their own
#: ``SingleFlight`` and then let the builds race — defeating the deduplication
#: entirely, silently, and only under load.
_LAZY_LOCK = threading.Lock()


def _lazy_attr(obj: Any, name: str, factory: Callable[[], Any]) -> Any:
    """``obj.<name>``, created once by ``factory`` even under concurrency."""
    existing = getattr(obj, name, None)
    if existing is not None:
        return existing
    with _LAZY_LOCK:
        existing = getattr(obj, name, None)
        if existing is None:
            existing = factory()
            setattr(obj, name, existing)
        return existing


@runtime_checkable
class DataSource(Protocol):
    """Supplies long-format tables and the metadata needed to build controls."""

    def describe(self) -> dict:
        """
        Factors, measures and shapes — everything the GUI needs to render its
        controls before any data is fetched.
        """
        ...

    def get_table(
        self,
        measures: list[str],
        *,
        x_measure: str | None = None,
        factor_variables: "list[FactorVariable] | None" = None,
    ) -> LongTable:
        """The long-format table for a plot's measures.

        Several ``measures`` **stack** into one value column plus a
        ``Variable`` column; ``x_measure`` **joins** as an x axis instead.
        """
        ...

    def joinable_with(self, measure: str) -> list[str]:
        """
        Measures that can supply an x axis for ``measure``.

        Trivially "all the others" for a flat CSV. For scidb it is a real
        question — two variables can only share a plot if their schema levels
        can be joined — and answering it honestly keeps the GUI from offering
        combinations that cannot be built.
        """
        ...

    def stackable_with(self, measure: str) -> list[str]:
        """Measures that can be plotted as another series alongside ``measure``."""
        ...

    def groupable_with(self, measure: str) -> "list[FactorVariable]":
        """Groupings usable as a FACTOR for ``measure`` — a variable recorded at
        or above its schema level, or one column of such a variable, so each row
        gets exactly one value."""
        ...

    def groupable_report(self, measure: str) -> dict:
        """``{"offered": [...], "rejected": {label: reason}}``.

        The same offers with the refusals kept, for the GUI: a column a user can
        see in their spreadsheet and not in the Grouping list has to say why.
        """
        ...


#: How many built tables a source keeps. Small on purpose: the entries are whole
#: frames (millions of rows for a 1-D measure), and the access pattern they serve
#: is narrow — the panel asks for the SAME table over and over while the user
#: moves controls, and only changes which one when a variant row names another
#: variable. Four covers that with room to spare; a larger cache would mostly
#: hold frames nobody is going to ask for again.
TABLE_CACHE_ENTRIES = 4

#: Placeholder the stacked value column carries during the melt, before it is
#: renamed to the primary measure. See :meth:`BaseSource._stacked` for why the
#: rename is unavoidable. Deliberately not a plausible column name: it only has
#: to survive one call, and it must not collide with a real one.
_STACK_VALUE = "__stacked_value__"


class BaseSource:
    """Small shared implementation for sources backed by a single table.

    Also owns the built-table cache for *every* source, ``ScidbSource``
    included — see :meth:`get_table`.
    """

    name: str = "table"

    def _table(self) -> LongTable:  # pragma: no cover - overridden
        raise NotImplementedError

    def describe(self) -> dict:
        return self._table().describe()

    def get_table(
        self,
        measures: list[str],
        *,
        x_measure: str | None = None,
        factor_variables: "list[FactorVariable] | None" = None,
    ) -> LongTable:
        """The long table for these measures, built once per distinct request.

        **Why this is memoized.** Building the table is melt + stack + join, and
        the panel asks for the same one repeatedly: ``plot_resolve`` and
        ``plot_capabilities`` each build it on every control change, so a single
        click paid for it twice, and four concurrent resolves paid for it eight
        times. The 2026-09-11 log shows exactly that — ``melted 'FilteredEMG'``
        and ``stacked [...]`` repeating in pairs on every action while
        ``load_variable`` (the layer below, already cached) ran once.

        Keyed on everything that changes the result. Note ``measures`` is
        order-sensitive and stays a tuple rather than a set: the order decides
        which measure is primary and what the ``Variable`` column's level order
        is.

        **Sharing a table between callers is safe** because nothing downstream
        mutates it — ``apply_variant_sets`` copies before writing its factor,
        and every other step (filters, explode, aggregate) builds a new frame.
        There is no in-place write anywhere in ``scistackplot`` or
        ``scistackplotdb``, and ``test_a_cached_table_is_not_mutated_by_use``
        exists to keep it that way.

        Staleness is the caller's business, as it already was for the frames
        underneath: a run that writes records drops the whole source
        (``plot_service.invalidate``), and this cache goes with it.
        """
        memo = self._table_cache()
        key = (
            tuple(measures),
            x_measure,
            tuple(factor_variables or ()),
        )
        label = f"get_table({', '.join(measures)})"
        if key in memo:
            # Hit/miss at INFO because it changes the cost of a request by orders
            # of magnitude, and because a hold measured in milliseconds is only
            # explicable one of two ways — cached, or the data is smaller than it
            # looks. On 2026-09-13 a 16 ms `plot_resolve` hold could not be
            # attributed to either (.claude/plot-at-scale-plan.md §1).
            Log.info("%s: table cache HIT", label, layer=LAYER)
            return memo[key]

        def _build():
            # Re-checked inside the single-flight slot: another thread may have
            # finished this exact build while we were claiming it, in which case
            # there is nothing to do. Without this the owner of a slot claimed a
            # moment after a build completed would rebuild anyway.
            if key in memo:
                Log.info("%s: table cache HIT (filled while waiting)", label, layer=LAYER)
                return memo[key]

            Log.info("%s: table cache MISS — building", label, layer=LAYER)
            with Log.timer(label, layer=LAYER) as timer:
                with timer.phase("build_table"):
                    table = self._build_table(
                        measures, x_measure=x_measure, factor_variables=factor_variables
                    )
                # Size the result in cells and SAMPLES, not just rows: a row count
                # cannot tell 4190 short arrays from 4190 quarter-million-sample
                # ones, and that distinction is the whole question for a plot that
                # will not return. Scanning only the measure columns keeps this one
                # O(cells) pass over data the build just touched anyway.
                with timer.phase("measure_extent"):
                    measured = [m for m in (*measures, x_measure) if m]
                    extent = frame_extent(table.frame, measured)
            # Plain INFO, not timer.note: note() is silent on a non-live timer,
            # and this is the one number the diagnosis actually turns on.
            Log.info("%s: built %s", label, format_extent(extent), layer=LAYER)
            # Published BEFORE the single-flight slot is released, so a caller
            # arriving after the release finds the memo rather than a free slot.
            memo[key] = table
            while len(memo) > TABLE_CACHE_ENTRIES:
                # Insertion-ordered dict: the oldest key is the first one.
                memo.pop(next(iter(memo)))
            return table

        # Concurrent callers for the same key WAIT for one build rather than each
        # doing it. The panel fires several requests at once and they all want
        # this table; before this, two of them built the same 5.2 GB frame side by
        # side (.claude/plot-at-scale-plan.md §7.1).
        return self._table_single_flight().run(
            key,
            _build,
            on_wait=lambda: Log.info(
                "%s: build already in flight — waiting for it", label, layer=LAYER
            ),
        )

    def _table_cache(self) -> dict:
        """The memo, created on first use.

        Lazy rather than set in ``__init__`` so every source inherits the cache
        without having to remember to call up — including the ones that define
        no ``__init__`` at all.
        """
        return _lazy_attr(self, "_built_tables", dict)

    def _table_single_flight(self) -> SingleFlight:
        """The in-flight map for :meth:`get_table`, created on first use.

        MUST be one instance per source — two would each dedupe their own
        callers and let the builds race, which is the bug this exists to fix —
        hence the locked lazy init rather than ``getattr``-or-create.
        """
        return _lazy_attr(self, "_table_inflight", SingleFlight)

    def invalidate_tables(self) -> None:
        """Drop built tables. Call when the rows underneath may have changed."""
        self._table_cache().clear()

    def _build_table(
        self,
        measures: list[str],
        *,
        x_measure: str | None = None,
        factor_variables: "list[FactorVariable] | None" = None,
    ) -> LongTable:
        # `factor_variables` is accepted and ignored: a flat table's factors are
        # already columns of every row, so there is nothing to join in
        # (`groupable_with` returns none). Accepting it keeps one call shape for
        # every source rather than making callers branch on which they hold.
        table = self._table()
        requested = [*measures, *([x_measure] if x_measure else [])]
        unknown = [m for m in requested if m not in table.measure_names]
        if unknown:
            raise KeyError(
                f"Unknown measure(s) {unknown}. Available: {table.measure_names}"
            )
        # A flat table already carries every column, so an x measure needs no
        # join here — only stacking changes the frame's shape.
        return self._stacked(table, measures) if len(measures) > 1 else table

    def _stacked(self, table: LongTable, measures: list[str]) -> LongTable:
        """Melt several measure columns into one, plus a ``Variable`` column.

        The CSV equivalent of ``ScidbSource._stacked_table``: two columns of a
        gait CSV are as much "Raw vs Filtered" as two scidb variables are, and
        keeping the standalone path capable of it is what keeps the DataSource
        protocol honest rather than scidb-shaped.

        The melt goes via :data:`_STACK_VALUE` and is renamed afterwards. It has
        to: the stacked column takes the PRIMARY measure's name, the primary is
        always one of the columns being melted, and ``DataFrame.melt`` refuses a
        ``value_name`` that matches any column it is melting. Passing the name
        directly raised ``ValueError`` for every possible input, so stacking on
        a CSV or DataFrame source never worked at all — found 2026-09-11 by the
        first test to ask for it.
        """
        from ..variants import VARIABLE_COLUMN

        primary = measures[0]
        frame = table.frame
        id_vars = [c for c in frame.columns if c not in table.measure_names]
        melted = frame.melt(
            id_vars=id_vars,
            value_vars=list(measures),
            var_name=VARIABLE_COLUMN,
            value_name=_STACK_VALUE,
        ).rename(columns={_STACK_VALUE: primary})
        level_order = {f.name: list(f.levels) for f in table.factors}
        # Declared order, not observed: the user listed the measures.
        level_order[VARIABLE_COLUMN] = list(measures)
        return LongTable.from_frame(
            melted,
            factors=[*table.factor_names, VARIABLE_COLUMN],
            measures=[primary],
            level_order=level_order,
            index_column=table.index_column,
            name=primary,
            schema_levels=table.schema_levels,
            measure_labels={primary: " / ".join(measures)},
        )

    def joinable_with(self, measure: str) -> list[str]:
        return [m for m in self._table().measure_names if m != measure]

    def groupable_with(self, measure: str) -> "list[FactorVariable]":
        """A flat table's factors are already columns of every row.

        Grouping variables exist because scidb records a subject-level fact
        separately from trial-level data; a CSV has no such split, so there is
        nothing to join in.
        """
        return []

    def groupable_report(self, measure: str) -> dict:
        """Offers and refusals. Nothing to offer and nothing to explain here."""
        return {"offered": [], "rejected": {}}

    def stackable_with(self, measure: str) -> list[str]:
        """Same shape, so the two share an axis. A flat table has no levels to
        reconcile, which is the only other thing scidb has to check."""
        table = self._table()
        try:
            shape = table.shape_of(measure)
        except KeyError:
            return []
        return [
            name
            for name in table.measure_names
            if name != measure and table.shape_of(name) is shape
        ]

    def default_measure(self) -> str | None:
        measures = self._table().measure_names
        return measures[0] if measures else None

    def metadata(self) -> dict[str, Any]:
        return {"source": type(self).__name__, "name": self.name}
