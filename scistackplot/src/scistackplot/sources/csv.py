"""
CSV source — the standalone entry point, and the direct descendant of the
R/Shiny proof of concept.

Its factor/measure split follows the same rule the proof of concept used
(``getColNames.R``: factor columns are the non-numeric ones), but derived from
observed value shapes rather than R's ``is.factor``, so a column of integer
subject IDs read from CSV can still be declared a factor explicitly.

This path is kept first-class — the VS Code extension routes "Plot" on a .csv
file here, with no project and no database — so the standalone claim stays
exercised rather than aspirational.

One inference limit worth knowing: a column of purely numeric IDs (``1, 2, 3``
for subject) is indistinguishable from a measurement by value alone, so it is
classified as a measure. Pass ``factors=["subject", ...]`` — or read it as text
with ``dtype={"subject": str}`` — when that is not what you want. A scidb
source never hits this: it knows which columns are schema keys.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from scistacklog import Log

from ..model.table import LongTable
from .base import BaseSource

LAYER = "scistackplot"


class CsvSource(BaseSource):
    def __init__(
        self,
        path: str | Path,
        *,
        factors: Iterable[str] | None = None,
        measures: Iterable[str] | None = None,
        level_order: dict[str, list[Any]] | None = None,
        index_column: str | None = None,
        **read_csv_kwargs: Any,
    ) -> None:
        self.path = Path(path)
        self.name = self.path.name
        frame = pd.read_csv(self.path, **read_csv_kwargs)
        self._long = LongTable.from_frame(
            frame,
            factors=factors,
            measures=measures,
            level_order=level_order,
            index_column=index_column,
            name=self.name,
        )
        Log.info(
            "loaded %s: %d row(s), %d factor(s), %d measure(s)",
            self.path.name,
            len(frame),
            len(self._long.factors),
            len(self._long.measures),
            layer=LAYER,
        )

    def _table(self) -> LongTable:
        return self._long
