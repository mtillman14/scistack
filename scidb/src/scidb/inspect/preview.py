"""What a variable holds: each column's types and values, and its rows.

The one owner of "show me this variable". The CLI (``scidb show --types``) and
the GUI's data preview only display this result. It reads through
``load_all_as_df(layout="spread", stringify_schema=True)``, the loader
``for_each`` uses, so it shows what a function receives: list cells, NaN and
text keys included. It never writes.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from scidb.model.log import Log
from scicanonicalhash._cells import cell_stats

#: Rows returned by default; the column summaries still cover every row.
DEFAULT_ROW_LIMIT = 50
_SAMPLES = 5
_TEXT_LIMIT = 80


@dataclass
class ColumnPreview:
    name: str
    #: The DuckDB type, or ``"(all blank)"`` for a column with no value yet.
    stored_type: str
    #: Python type name -> how many cells hold it, most common first.
    loaded_types: dict[str, int]
    non_null: int
    n_rows: int
    #: Up to five distinct values, as text.
    samples: list[str] = field(default_factory=list)
    #: What is inside the cells: NaN elements, all-NaN / constant arrays
    #: (scicanonicalhash._cells, the owner; CellStats.to_dict()).
    cells: dict = field(default_factory=dict)


@dataclass
class DataPreview:
    variable: str
    schema_keys: list[str]
    location: dict[str, str]
    #: Values of each schema key that this variable has, for a location picker.
    levels: dict[str, list[str]]
    n_records: int
    n_rows: int
    columns: list[ColumnPreview]
    #: Row dicts, JSON-safe, at most the row limit: schema keys, then data.
    rows: list[dict[str, Any]]
    truncated: bool
    #: Requested columns this variable does not have.
    missing_columns: list[str] = field(default_factory=list)
    note: str | None = None


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    return isinstance(value, (float, np.floating)) and bool(np.isnan(value))


def _text(value: Any) -> str:
    text = str(value)
    return text if len(text) <= _TEXT_LIMIT else text[:_TEXT_LIMIT] + "…"


def _json_cell(value: Any) -> Any:
    """A cell for a JSON response: numbers and bools as they are, blanks as
    null, everything else (lists, arrays, datetimes) as clipped text."""
    if _is_missing(value):
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value)
    return _text(value)


def _column_preview(name: str, series: pd.Series, stored_type: str) -> ColumnPreview:
    cells = series.tolist()
    present = [c for c in cells if not _is_missing(c)]
    types = Counter(type(c).__name__ for c in present)
    samples: list[str] = []
    for cell in present:
        text = _text(cell)
        if text not in samples:
            samples.append(text)
            if len(samples) == _SAMPLES:
                break
    return ColumnPreview(
        name=name,
        stored_type=stored_type,
        loaded_types=dict(types.most_common()),
        non_null=len(present),
        n_rows=len(cells),
        samples=samples,
        cells=cell_stats(cells).to_dict(),
    )


def _stored_types(db, type_name: str) -> dict[str, str]:
    types = db.stored_data_column_types(f"{type_name}_data")
    metas = (db.get_dtype_meta(type_name) or {}).get("columns", {})
    return {
        col: "(all blank)" if (metas.get(col) or {}).get("empty") else stored
        for col, stored in types.items()
    }


def _variable_class(type_name: str) -> type:
    """The loaded class, or a stand-in when none is imported (the CLI). The
    loader needs only the name unless the variable has custom serialization."""
    from scidb.model.variable import BaseVariable

    known = BaseVariable._all_subclasses.get(type_name)
    return known if known is not None else type(type_name, (BaseVariable,), {})


def data_preview(
    db,
    variable: str,
    *,
    columns: "list[str] | None" = None,
    location: "dict | None" = None,
    limit: int = DEFAULT_ROW_LIMIT,
) -> DataPreview:
    """Each column's stored and loaded types, non-blank count and sample values,
    plus the first *limit* rows, at *location*. *columns* restricts the data
    columns, as a ``ColumnSelection`` does."""
    from sciduckdb import schema_str

    location = {k: schema_str(v) for k, v in (location or {}).items() if v not in (None, "")}
    keys = list(db.dataset_schema_keys)
    unknown = sorted(set(location) - set(keys))
    if unknown:
        raise ValueError(f"{unknown} are not schema keys. Schema keys: {keys}")

    index = db._find_record(variable, nested_metadata={"schema": {}, "version": {}}, version_id="latest")
    levels = {
        key: sorted({str(v) for v in index[key].tolist() if v is not None and not _is_missing(v)})
        for key in keys
        if key in index.columns
    }
    frame = db.load_all_as_df(
        _variable_class(variable),
        metadata=location or None,
        layout="spread",
        include_rid=True,
        stringify_schema=True,
    )

    data_columns = [c for c in frame.columns if c not in keys and not str(c).startswith("__")]
    missing_columns: list[str] = []
    if columns:
        missing_columns = [c for c in columns if c not in data_columns]
        data_columns = [c for c in columns if c in data_columns]

    key_columns = [k for k in keys if k in frame.columns and frame[k].notna().any()]
    stored = _stored_types(db, variable)
    previews = [
        _column_preview(c, frame[c], stored.get(c, "(not stored)")) for c in data_columns
    ]
    shown = frame[key_columns + data_columns].head(limit)
    rows = [{c: _json_cell(v) for c, v in row.items()} for row in shown.to_dict("records")]

    n_records = (
        int(frame["__record_id"].nunique()) if "__record_id" in frame.columns else len(frame)
    )
    note = None
    if key_columns and n_records and frame.drop_duplicates(key_columns).shape[0] < n_records:
        note = (
            "Some locations hold more than one variant; rows from every variant are "
            "shown. A run reads the variant its node selects."
        )
    Log.info(
        f"[preview] {variable}: {n_records} record(s), {len(frame)} row(s), "
        f"{len(data_columns)} column(s), location={location or 'all'}"
    )
    return DataPreview(
        variable=variable,
        schema_keys=keys,
        location=location,
        levels=levels,
        n_records=n_records,
        n_rows=len(frame),
        columns=previews,
        rows=rows,
        truncated=len(frame) > limit,
        missing_columns=missing_columns,
        note=note,
    )
