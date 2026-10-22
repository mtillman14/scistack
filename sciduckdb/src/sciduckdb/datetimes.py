"""Datetime storage — the one owner of the ``<col>__tz`` companion column.

Every datetime column (``python_type == "datetime"`` in a variable's dtype
metadata) is stored as TWO DuckDB columns:

- ``<col>``      ``TIMESTAMP_NS``: the UTC instant of an aware value, the wall
  clock of a naive one. TIMESTAMP_NS rather than TIMESTAMPTZ (user decision
  D5, 2026-10-22): TIMESTAMPTZ keeps only microseconds, so a pandas
  nanosecond value would be truncated and hash differently after its load. A
  TIMESTAMP_NS is never converted through the session time zone either.
- ``<col>__tz``  ``VARCHAR``: the value's zone label (``scicanonicalhash.
  zone_label``: an IANA name, ``"UTC"`` or ``"+02:00"``). NULL = naive. A NaT is
  NULL in both columns.

The companion exists from a column's first save even when it is all NULL
(user decision D4): the first save fixes a variable's columns, so a
companion that first appeared with an aware value would be a column-set
change. Per-value zones are allowed to differ, and naive and aware values
may mix.

The companion is a STORAGE column only. ``dtype_meta["columns"]`` (what every
consumer lists as a variable's data columns) never names it. Savers get it
through ``storage_column_types``, readers select ``storage_column_names`` and
fold it back with ``merge_tz_columns``. Readers of the bare table schema
(no dtype metadata) recognise it with ``stored_tz_companions``.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any

import numpy as np
import pandas as pd
from scicanonicalhash import (
    is_datetime_value,
    is_missing_datetime,
    utc_ns,
    zone_from_label,
    zone_label,
)

logger = logging.getLogger(__name__)

TZ_SUFFIX = "__tz"
INSTANT_TYPE = "TIMESTAMP_NS"
ZONE_TYPE = "VARCHAR"
DATETIME_PTYPE = "datetime"

# TIMESTAMP_NS is an int64 of ns; DuckDB reserves the two extremes for
# +/-infinity.
_NS_MIN = -(2**63) + 1
_NS_MAX = 2**63 - 2
_EPOCH = datetime.datetime(1970, 1, 1)


class DatetimeStorageError(ValueError):
    """A datetime value or column that cannot be stored as asked."""


def tz_column_name(col: Any) -> str:
    """The companion column of datetime column ``col``."""
    return f"{col}{TZ_SUFFIX}"


def datetime_meta(unit: str | None = None) -> dict:
    """The column metadata of a datetime column. ``unit`` is a datetime64
    column's resolution (``"ns"``, ``"us"``, ...), restored on load: the
    DataFrame hash includes the dtype, and pandas' default is ``us`` on
    some versions while TIMESTAMP_NS loads as ``ns``."""
    meta = {"python_type": DATETIME_PTYPE}
    if unit:
        meta["unit"] = unit
    return meta


def column_unit(dtype) -> str | None:
    """The resolution of a (naive or tz-aware) datetime64 dtype."""
    unit = getattr(dtype, "unit", None)  # DatetimeTZDtype
    if unit:
        return str(unit)
    try:
        return np.datetime_data(dtype)[0]
    except TypeError:
        return None


def is_datetime_meta(col_meta: dict | None) -> bool:
    return bool(col_meta) and col_meta.get("python_type") == DATETIME_PTYPE


def datetime_columns(dtype_meta: dict | None) -> list[str]:
    """The datetime columns of a variable, in column order."""
    cols = (dtype_meta or {}).get("columns", {}) or {}
    return [c for c, m in cols.items() if is_datetime_meta(m)]


def tz_companions(dtype_meta: dict | None) -> dict[str, str]:
    """``{companion: datetime column}`` for a variable's dtype metadata."""
    return {tz_column_name(c): c for c in datetime_columns(dtype_meta)}


def storage_column_types(col_types: dict, dtype_meta: dict) -> dict:
    """``col_types`` (the variable's data columns) with each datetime column
    followed by its companion: the columns as the table stores them, in
    order. THE owner of that order; savers build DDL, INSERT column lists
    and storage rows from it.

    Raises ``DatetimeStorageError`` when a data column is already named like
    a datetime column's companion (``t`` + ``t__tz``).
    """
    companions = tz_companions(dtype_meta)
    data_cols = (dtype_meta or {}).get("columns", {}) or {}
    clash = [c for c in col_types if str(c) in companions and c in data_cols]
    if clash:
        raise DatetimeStorageError(
            f"data column(s) {clash} have the name of a datetime column's zone "
            f"column (<column>{TZ_SUFFIX}, where scistack stores each value's "
            f"time zone). Rename them before saving."
        )
    dt_cols = set(datetime_columns(dtype_meta))
    out: dict = {}
    for col, ddb_type in col_types.items():
        if is_tz_column_of(col, col_types, dtype_meta):
            continue  # already expanded (idempotent)
        out[col] = ddb_type
        if col in dt_cols:
            out[tz_column_name(col)] = ZONE_TYPE
    return out


def is_tz_column_of(name: Any, col_types: dict, dtype_meta: dict) -> bool:
    """True when ``name`` is the companion of a datetime column in
    ``dtype_meta`` (not merely a name ending in ``__tz``)."""
    return str(name) in tz_companions(dtype_meta) and name not in (
        (dtype_meta or {}).get("columns", {}) or {}
    )


def user_column_types(col_types: dict, dtype_meta: dict) -> dict:
    """The inverse of ``storage_column_types``: companions dropped."""
    return {
        c: t for c, t in col_types.items()
        if not is_tz_column_of(c, col_types, dtype_meta)
    }


def storage_column_names(dtype_meta: dict) -> list[str]:
    """The stored columns a reader selects for a variable: its data columns
    plus every datetime column's companion."""
    cols = (dtype_meta or {}).get("columns", {}) or {}
    return list(storage_column_types({c: "" for c in cols}, dtype_meta))


def stored_tz_companions(stored_types: dict) -> dict[str, str]:
    """``{companion: datetime column}`` read from a table's own schema
    (``{column: DuckDB type}``), for readers that have no dtype metadata: a
    ``VARCHAR`` column ``<x>__tz`` beside a ``TIMESTAMP_NS`` column ``<x>``."""
    out = {}
    for name, ddb_type in stored_types.items():
        name = str(name)
        if not name.endswith(TZ_SUFFIX) or str(ddb_type).upper() != ZONE_TYPE:
            continue
        source = name[: -len(TZ_SUFFIX)]
        if str(stored_types.get(source, "")).upper() == INSTANT_TYPE:
            out[name] = source
    return out


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------


def _ns_text(ns: int) -> str:
    """ns from the epoch as ``YYYY-MM-DD HH:MM:SS.fffffffff`` — the text
    DuckDB casts to TIMESTAMP_NS without losing a digit."""
    secs, frac = divmod(ns, 10**9)
    dt = _EPOCH + datetime.timedelta(seconds=secs)
    return f"{dt.year:04d}-{dt:%m-%d %H:%M:%S}.{frac:09d}"


def _is_missing(value: Any) -> bool:
    if value is None or is_missing_datetime(value):
        return True
    if isinstance(value, float) and value != value:
        return True
    return value is pd.NA


def datetime_to_storage(value: Any) -> tuple[str | None, str | None]:
    """``(instant, zone label)`` — what one datetime cell stores.

    ``instant`` is ISO text with nine fractional digits (UTC for an aware
    value, the wall clock for a naive one); a missing value is
    ``(None, None)``. Raises ``DatetimeStorageError`` for a value that is not
    a datetime, or one outside TIMESTAMP_NS's range (1677-09-21 to
    2262-04-11).
    """
    if _is_missing(value):
        return None, None
    if isinstance(value, np.datetime64):
        value = pd.Timestamp(value)
    if not is_datetime_value(value):
        raise DatetimeStorageError(
            f"a datetime column received {type(value).__name__} {value!r}; "
            f"every value of a datetime column must be a datetime or NaT"
        )
    ns = utc_ns(value)
    if not _NS_MIN <= ns <= _NS_MAX:
        raise DatetimeStorageError(
            f"datetime {value!r} is outside the storable range "
            f"(1677-09-21 to 2262-04-11, nanosecond precision)"
        )
    return _ns_text(ns), zone_label(value)


def datetime_from_storage(instant: Any, label: Any) -> Any:
    """One stored ``(instant, zone label)`` back to its value: ``pd.NaT``,
    a naive ``pd.Timestamp``, or a ``pd.Timestamp`` in its own zone."""
    if instant is None or pd.isna(instant):
        return pd.NaT
    ts = pd.Timestamp(instant)
    if ts.tzinfo is not None:  # defensive: an instant never carries a zone
        ts = ts.tz_convert("UTC").tz_localize(None)
    if label is None or (isinstance(label, float) and label != label):
        return ts
    return ts.tz_localize("UTC").tz_convert(zone_from_label(str(label)))


def datetime_series_to_storage(values) -> tuple[list, list, dict]:
    """A column of datetime cells -> ``(instants, labels, counts)``;
    ``counts`` = ``{"naive": n, "aware": n, "missing": n}`` for the log."""
    instants, labels = [], []
    counts = {"naive": 0, "aware": 0, "missing": 0}
    for v in values:
        instant, label = datetime_to_storage(v)
        instants.append(instant)
        labels.append(label)
        if instant is None:
            counts["missing"] += 1
        elif label is None:
            counts["naive"] += 1
        else:
            counts["aware"] += 1
    return instants, labels, counts


def _as_unit(series: pd.Series, unit: str | None) -> pd.Series:
    if not unit or not hasattr(series.dt, "as_unit"):  # pandas < 2: ns only
        return series
    try:
        return series.dt.as_unit(unit)
    except (ValueError, TypeError) as err:
        logger.warning("datetime column %r: could not restore unit %r (%s)", series.name, unit, err)
        return series


def restore_datetime_series(instants: pd.Series, labels, unit: str | None = None) -> pd.Series:
    """A stored instant column plus its labels -> the loaded column.

    No labels: naive ``datetime64[<unit>]``. One zone for every value (NaT
    aside): ``datetime64[<unit>, <zone>]``. Anything else (several zones, or
    naive and aware mixed): an object column of Timestamps, each in its own
    zone, since pandas holds nothing else. ``unit`` (the saved column's
    resolution, ``datetime_meta``) defaults to what the fetch gave (ns).
    """
    instants = pd.to_datetime(instants)
    if getattr(instants.dt, "tz", None) is not None:
        instants = instants.dt.tz_convert("UTC").dt.tz_localize(None)
    labels = pd.Series(list(labels), index=instants.index, dtype=object)
    present = instants.notna()
    has_label = labels.notna()
    if not has_label.any():
        return _as_unit(instants, unit)
    distinct = set(labels[has_label])
    if len(distinct) == 1 and bool((has_label | ~present).all()):
        (label,) = distinct
        aware = instants.dt.tz_localize("UTC").dt.tz_convert(zone_from_label(label))
        return _as_unit(aware, unit)
    return pd.Series(
        [datetime_from_storage(i, lab) for i, lab in zip(instants, labels, strict=False)],
        index=instants.index,
        dtype=object,
        name=instants.name,
    )


def merge_tz_columns(df: pd.DataFrame, dtype_meta: dict, *, where: str = "") -> pd.DataFrame:
    """Fold every datetime column of a loaded storage frame with its
    companion and drop the companion: THE load-side owner, called by every
    reader right after its fetch. Returns ``df`` (changed in place).

    A datetime column whose companion was not fetched (a table saved before
    the companion existed) loads naive, with a WARNING naming it.
    """
    col_metas = (dtype_meta or {}).get("columns", {}) or {}
    for col in datetime_columns(dtype_meta):
        if col not in df.columns:
            continue
        unit = col_metas[col].get("unit")
        comp = tz_column_name(col)
        if comp in df.columns:
            labels = df[comp].tolist()
        else:
            logger.warning(
                "%s: datetime column %r has no %r zone column in this load; "
                "its values load as naive", where or "load", col, comp,
            )
            labels = [None] * len(df)
        restored = restore_datetime_series(df[col], labels, unit)
        if comp in df.columns:
            df.drop(columns=[comp], inplace=True)
        df[col] = restored
        n_aware = sum(1 for lab in labels if lab is not None and lab == lab)
        logger.debug(
            "%s: datetime column %r restored: %d row(s), %d aware (%d zone(s)), "
            "%d NaT, dtype %s",
            where or "load", col, len(restored), n_aware,
            len({lab for lab in labels if lab is not None and lab == lab}),
            int(restored.isna().sum()), restored.dtype,
        )
    return df
