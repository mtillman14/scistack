"""
The ``DataSource`` protocol — the one seam that makes this package both
standalone and scistack-compatible.

``scistackplot`` ships CSV and DataFrame implementations; ``scistackplotdb``
ships the scidb one. The GUI talks only to this protocol and never learns which
implementation it has, so the same application serves a lone CSV file and a
full scidb project. Everything above this line is pure long-table logic.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from ..table import LongTable


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
        factor_variables: list[str] | None = None,
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

    def groupable_with(self, measure: str) -> list[str]:
        """Variables usable as a grouping FACTOR for ``measure`` — recorded at
        or above its schema level, so each row gets exactly one value."""
        ...


class BaseSource:
    """Small shared implementation for sources backed by a single table."""

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
        factor_variables: list[str] | None = None,
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
        """
        from ..variants import VARIABLE_COLUMN

        primary = measures[0]
        frame = table.frame
        id_vars = [c for c in frame.columns if c not in table.measure_names]
        melted = frame.melt(
            id_vars=id_vars,
            value_vars=list(measures),
            var_name=VARIABLE_COLUMN,
            value_name=primary,
        )
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

    def groupable_with(self, measure: str) -> list[str]:
        """A flat table's factors are already columns of every row.

        Grouping variables exist because scidb records a subject-level fact
        separately from trial-level data; a CSV has no such split, so there is
        nothing to join in.
        """
        return []

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
