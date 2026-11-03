"""Flat-table CSV export shared by BaseVariable, ColumnSelection, and Merge.

``to_csv`` writes one row per unique schema_id, with one column per schema key
plus one or more value columns.  The constraint is **one row per schema_id** —
a record's data may have multiple *columns* (a single-row table, or the columns
contributed by each ``Merge`` constituent), but it may not occupy multiple
*rows*.  Bare vectors (DOUBLE[]/DOUBLE[][]) have no column structure and are
rejected; multi-row tables per schema_id are rejected.

Entry point: :func:`export_csv`.  ``spec`` may be a BaseVariable subclass, a
``ColumnSelection`` (``MyVar["col"]``), or a ``Merge`` (whose constituents may be
classes, ColumnSelections, ``Fixed``, or ``Variant`` wrappers).
"""

import numbers

import numpy as np
import pandas as pd


def is_scalar_value(value) -> bool:
    """Return True if ``value`` is a single scalar suitable for a CSV cell.

    Scalars: None, Python/numpy numbers, bools, str, bytes. Non-scalars
    (rejected): numpy arrays (DOUBLE[]/DOUBLE[][]), lists, tuples, dicts, sets,
    and pandas DataFrame/Series.
    """
    if value is None:
        return True
    if isinstance(value, np.ndarray):
        return False
    if isinstance(value, np.generic):  # numpy scalar (e.g. np.float64)
        return True
    return isinstance(value, (bool, numbers.Number, str, bytes))


def _is_spec(obj) -> bool:
    """True if ``obj`` is a variable spec (class, instance, or wrapper)."""
    from scifor import ColumnSelection, Fixed, Merge

    from scidb.model.variable import BaseVariable
    from scidb.model.variant import Variant

    if isinstance(obj, (Merge, ColumnSelection, Fixed, Variant, BaseVariable)):
        return True
    return isinstance(obj, type) and issubclass(obj, BaseVariable)


def _spec_name(obj) -> str:
    return getattr(obj, "__name__", None) or type(obj).__name__


def _reject_extra_args(receiver, args) -> None:
    """Raise a clear error for any positional arg passed after ``filename``.

    ``to_csv`` exports the thing it is called on; it does not absorb other
    variables. A variable spec here almost always means the caller wanted a
    join — point them at ``Merge(...).to_csv(...)``.
    """
    if not args:
        return
    specs = [a for a in args if _is_spec(a)]
    if specs:
        raise ValueError(
            f"to_csv() exports a single variable (or one Merge/ColumnSelection), "
            f"not {_spec_name(receiver)} plus extra variables. To export several "
            f"variables together, build the Merge explicitly and call to_csv on it: "
            f"Merge({_spec_name(receiver)}, {_spec_name(specs[0])}).to_csv(...). "
            f"Everything after 'filename' must be keyword metadata "
            f"(e.g. subject=1, where=..., version='all')."
        )
    raise TypeError(
        f"to_csv() takes no positional arguments after 'filename', but got "
        f"{len(args)} more. Pass metadata as keywords, e.g. "
        f"to_csv('out.csv', subject=1)."
    )


def _resolve_constituent(spec):
    """Normalize a spec into ``(var_type, columns_or_None, extra_kwargs)``.

    ``columns`` restricts a table variable to a subset (``ColumnSelection``).
    ``extra_kwargs`` are additional load kwargs contributed by ``Fixed``
    (metadata overrides) or ``Variant`` (branch-param pins) wrappers, merged
    onto the caller's metadata for this constituent only.
    """
    from scifor import ColumnSelection, Fixed

    from scidb.model.variable import BaseVariable
    from scidb.model.variant import Variant

    if isinstance(spec, ColumnSelection):
        return spec.data, list(spec.columns), {}
    if isinstance(spec, Fixed):
        var_type, columns, extra = _resolve_constituent(spec.data)
        return var_type, columns, {**extra, **spec.fixed_metadata}
    if isinstance(spec, Variant):
        var_type, columns, extra = _resolve_constituent(spec.var_type)
        return var_type, columns, {**extra, **spec.branch_params}
    if isinstance(spec, type) and issubclass(spec, BaseVariable):
        return spec, None, {}
    if hasattr(spec, "load"):  # duck-typed loadable
        return spec, None, {}
    raise TypeError(
        f"Cannot export a spec of type {type(spec).__name__} to CSV; expected a "
        f"BaseVariable subclass, ColumnSelection, or Merge."
    )


def _expand_data(value, var_name, columns):
    """Turn one record's data cell into ``{column: scalar}`` for a single row.

    Raises ``ValueError`` if the data cannot occupy exactly one CSV row
    (multi-row table, vector, or a cell that is itself non-scalar).
    """
    # Single-row (multi-column allowed) table variable.
    if isinstance(value, pd.DataFrame):
        df = value
        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                raise ValueError(
                    f"{var_name}.to_csv(): selected column(s) {missing} not found "
                    f"(available: {list(df.columns)})."
                )
            df = df[columns]
        if len(df) != 1:
            raise ValueError(
                f"{var_name}.to_csv() requires one row per schema_id, but a record "
                f"holds a {len(df)}-row table. Multi-row tables cannot be flattened "
                f"to a single CSV row."
            )
        row = df.iloc[0]
        out = {}
        for col in df.columns:
            cell = row[col]
            if not is_scalar_value(cell):
                raise ValueError(
                    f"{var_name}.to_csv(): column {col!r} holds non-scalar data of "
                    f"type {type(cell).__name__}; cannot export to flat CSV."
                )
            out[col] = cell
        return out

    # Plain scalar variable.
    if is_scalar_value(value):
        if columns:
            raise ValueError(
                f"Column selection {columns} was given, but {var_name} is a scalar "
                f"variable with no columns to select."
            )
        return {var_name: value}

    # ndarray / list / dict / etc.
    raise ValueError(
        f"{var_name}.to_csv() only supports scalar or single-row table data per "
        f"schema_id, but a record holds non-scalar data of type "
        f"{type(value).__name__} (e.g. a vector DOUBLE[]/DOUBLE[][])."
    )


def _constituent_flat_df(var_type, columns, *, where, version, db, metadata):
    """Load one constituent → (DataFrame[schema cols + data cols], schema_cols).

    One row per loaded record. Schema-key columns that are entirely null (a
    variable used at a coarser level than the full schema) are dropped so the
    table reflects the variable's true schema level.
    """
    from scidb.store.database import get_database

    _db = db or get_database()

    # Packed layout: one row per record, schema-key columns + a 'data' column.
    # Propagates NotFoundError when nothing matches.
    df = var_type.load(as_df=True, version=version, where=where, db=db, **metadata)

    if "data" not in df.columns:
        raise ValueError(
            f"{var_type.__name__}.to_csv(): loaded DataFrame has no 'data' column "
            f"(got columns {list(df.columns)})."
        )

    schema_cols = [
        k for k in _db.dataset_schema_keys if k in df.columns and not df[k].isna().all()
    ]

    rows = []
    for _, rec in df.iterrows():
        schema_part = {k: rec[k] for k in schema_cols}
        data_part = _expand_data(rec["data"], var_type.__name__, columns)
        rows.append({**schema_part, **data_part})

    return pd.DataFrame(rows), schema_cols


def build_flat_table(spec, *, where, version, db, metadata):
    """Resolve ``spec`` and return the flat export DataFrame.

    For a ``Merge``, each constituent is loaded independently and joined on
    its shared schema keys by THE Merge rule (``scifor.merge.merge_frames``),
    so e.g. a subject-level covariate broadcasts across a trial-level measure.
    """
    from scifor import Merge
    from scifor.merge import merge_frames

    from scidb.store.database import get_database

    _db = db or get_database()

    if isinstance(spec, Merge):
        constituents = [_resolve_constituent(s) for s in spec.tables]
    else:
        constituents = [_resolve_constituent(spec)]

    part_dfs = []
    for var_type, columns, extra in constituents:
        part, _ = _constituent_flat_df(
            var_type,
            columns,
            where=where,
            version=version,
            db=db,
            metadata={**metadata, **extra},
        )
        part_dfs.append(part)

    # THE Merge rule (scifor.merge.merge_frames), over whole tables: join on
    # shared schema keys; a shared data column, a location on one side only or
    # no shared key is an error, never a silent drop.
    labels = [var_type.__name__ for var_type, _c, _e in constituents]
    result = (
        part_dfs[0]
        if len(part_dfs) == 1
        else merge_frames(
            part_dfs, list(_db.dataset_schema_keys), labels=labels, positional=False
        )
    )

    # Order: schema keys (in dataset order) first, then data columns.
    ordered_schema = [k for k in _db.dataset_schema_keys if k in result.columns]
    data_cols = [c for c in result.columns if c not in ordered_schema]
    return result[ordered_schema + data_cols]


def export_csv(spec, filename, *args, **kwargs):
    """Validate, build, and write the flat CSV for ``spec``.

    ``kwargs`` mirror ``load()``: ``where=``, ``version=``, ``db=``, and schema /
    branch-param metadata. ``as_df`` / ``introspect`` are ignored. Positional
    args after ``filename`` are rejected (see :func:`_reject_extra_args`).
    """
    from scidb.model.log import Log

    _reject_extra_args(spec, args)

    if not isinstance(filename, str) or not filename.endswith(".csv"):
        raise ValueError(
            f"to_csv() filename must be a string ending with '.csv', got {filename!r}."
        )

    kwargs.pop("as_df", None)
    kwargs.pop("introspect", None)
    where = kwargs.pop("where", None)
    version = kwargs.pop("version", "latest")
    db = kwargs.pop("db", None)
    metadata = kwargs  # whatever remains is schema / branch-param metadata

    out = build_flat_table(spec, where=where, version=version, db=db, metadata=metadata)

    Log.info(
        f"to_csv: writing {len(out)} row(s) x {out.shape[1]} column(s) "
        f"to {filename!r} (columns: {list(out.columns)})"
    )
    out.to_csv(filename, index=False)
