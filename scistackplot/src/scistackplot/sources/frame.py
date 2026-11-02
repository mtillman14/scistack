"""In-memory DataFrame source — the scifor-equivalent entry point."""

from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from ..model.table import LongTable
from .base import BaseSource


class DataFrameSource(BaseSource):
    """
    Wrap a DataFrame you already have.

    This is the direct analogue of ``scifor.for_each`` taking a plain table:
    no database, no configuration, no file IO. Column roles are inferred from
    observed value shapes when not given (see ``LongTable.from_frame``).
    """

    def __init__(
        self,
        frame: pd.DataFrame,
        *,
        factors: Iterable[str] | None = None,
        measures: Iterable[str] | None = None,
        level_order: dict[str, list[Any]] | None = None,
        variant_factors: Iterable[str] = (),
        index_column: str | None = None,
        name: str = "dataframe",
    ) -> None:
        self.name = name
        self._long = LongTable.from_frame(
            frame,
            factors=factors,
            measures=measures,
            level_order=level_order,
            variant_factors=variant_factors,
            index_column=index_column,
            name=name,
        )

    def _table(self) -> LongTable:
        return self._long
