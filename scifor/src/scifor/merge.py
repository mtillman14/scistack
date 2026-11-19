"""Merge input wrapper for for_each: combines multiple DataFrames into one."""

from typing import Any

from scistacklog import Log

from .column_selection import _display_name

#: The join methods a Merge accepts. THE list: the GUI dropdown and MATLAB's
#: validation read it from here (docs/claude/merge-node.md).
#:   exact - every location on both sides, else an error (no row dropped)
#:   inner - locations on both sides; one-side-only rows are dropped
#:   outer - every location; missing values are NaN
#:   left  - every left location; right-only rows dropped, gaps NaN
#:   right - every right location; left-only rows dropped, gaps NaN
MERGE_HOWS = ("exact", "inner", "outer", "left", "right")


def merge_key(name: str, how: str, table_keys: "list[str]") -> str:
    """The identity key of a Merge -- what :meth:`Merge.to_key` returns. THE
    one spelling: the GUI predicts a canvas Merge's key through this (with the
    constituents' type names) so canvas and history agree by construction,
    as ``pathinput.path_input_key`` does for PathInputs."""
    import json

    return json.dumps(
        {"__type": "Merge", "name": name, "how": how, "tables": list(table_keys)},
        sort_keys=True,
    )


class Merge:
    """
    Combines 2+ DataFrame inputs into a single pandas DataFrame for use
    in for_each() inputs.

    Each constituent is filtered individually per iteration, then combined
    by THE Merge rule (``merge_frames`` below; spec docs/claude/merge-cases.json):
    shared varying schema keys are join keys, a shared non-key column or a
    location on one side only is an error.

    Constituents can be:
    - Plain pandas DataFrames
    - Fixed wrappers (DataFrames with overridden metadata)
    - ColumnSelection wrappers (DataFrame with column extraction)

    Example:
        for_each(
            analyze,
            inputs={
                "combined": Merge(gait_df, force_df, name="gait_force"),
            },
            subject=[1, 2, 3],
        )

        # With Fixed
        for_each(
            analyze,
            inputs={
                "combined": Merge(
                    gait_df,
                    Fixed(paretic_df, session="BL"),
                    name="gait_paretic",
                ),
            },
            subject=[1, 2, 3],
            session=["A", "B"],
        )
    """

    def __init__(self, *tables: Any, name: str, how: str = "exact"):
        if len(tables) < 2:
            raise ValueError(f"Merge requires at least 2 inputs, got {len(tables)}.")
        for t in tables:
            if isinstance(t, Merge):
                raise TypeError("Cannot nest Merge inside another Merge.")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(
                f"Merge requires a non-empty name= (got {name!r}); it identifies "
                f"the Merge node on the canvas and in history."
            )
        if how not in MERGE_HOWS:
            raise ValueError(
                f"Merge how={how!r} is not one of {list(MERGE_HOWS)}. 'exact' "
                f"(the default) errors on any location present on one side only; "
                f"the others drop or NaN-pad such rows, like a SQL join."
            )
        self.tables = tables
        self.name = name.strip()
        self.how = how

    def table_keys(self) -> list[str]:
        """Each constituent's identity spelling, in join order."""
        parts = []
        for spec in self.tables:
            if hasattr(spec, "to_key"):
                parts.append(spec.to_key())
            elif isinstance(spec, type):
                parts.append(spec.__name__)
            else:
                parts.append(repr(spec))
        return parts

    def to_key(self) -> str:
        """The IDENTITY of this Merge as a JSON string: name, how and the
        constituents in order (any of them changing is another computation).

        The spelling is :func:`merge_key`'s; the ONE parser is
        ``scidb.provenance.graph.parse_merge_spec``.
        """
        return merge_key(self.name, self.how, self.table_keys())

    def to_csv(
        self, filename: str, where=None, verbose: bool = False, **metadata: Any
    ) -> None:
        """Join the constituents and write the result to a flat CSV.

        Each constituent DataFrame is row-filtered by ``**metadata`` (scalar →
        equality, list/tuple/set → membership) on any matching schema column and
        then the constituents are joined on their shared schema columns by THE
        Merge rule — keeping a single copy of those columns, erroring on a
        location present on one side only — and an optional ``where=``
        ColName/Col filter is applied to the joined table (so it may reference a
        column from any constituent). Non-schema columns may not overlap. ``filename`` must end with ``.csv``.

        Same rule as the ``for_each`` merge path, over whole tables (no
        per-combo iteration, no positional fallback); see
        ``csv_export.export_merge_csv``.

        Args:
            filename: Output path; must end with ``.csv``.
            where: Optional scifor ColName/Col filter applied to every constituent.
            verbose: If True, log the join diagnostics at INFO (visible at
                the default log level) instead of DEBUG (NOTE 2).
            **metadata: Per-constituent row filters on matching schema columns.

        Example:
            # subject,trial,StepLength,Speed
            Merge(step_df, speed_df, name="gait").to_csv("gait.csv", subject=[1, 2])
        """
        from .csv_export import export_merge_csv

        export_merge_csv(
            self,
            filename,
            where=where,
            verbose=verbose,
            **metadata,
        )

    def as_df(self, where=None, verbose: bool = False, **metadata: Any):
        """Join the constituents and return the result as a pandas DataFrame.

        Same join and filter semantics as :meth:`to_csv` (per-constituent
        ``**metadata`` filters, post-join ``where=``, one copy of the shared
        schema columns), but returns the DataFrame instead of writing a file.
        ``pd.DataFrame(Merge(...))`` is intentionally not supported — use this.

        Args:
            where: Optional scifor ColName/Col filter applied to the joined table.
            verbose: If True, log the join diagnostics at INFO (visible at
                the default log level) instead of DEBUG (NOTE 2).
            **metadata: Per-constituent row filters on matching schema columns.

        Example:
            merged = Merge(step_df, speed_df, name="gait")
            df = merged.as_df(subject=[1, 2])
        """
        from .csv_export import merge_to_dataframe

        return merge_to_dataframe(
            self,
            where=where,
            verbose=verbose,
            **metadata,
        )

    @property
    def __name__(self) -> str:
        """Display name for format_inputs and error messages."""
        from .column_selection import ColumnSelection
        from .fixed import Fixed

        parts = []
        for spec in self.tables:
            if isinstance(spec, Fixed):
                inner = spec.data
                if isinstance(inner, ColumnSelection):
                    inner_name = inner.__name__
                else:
                    inner_name = _display_name(inner)
                fixed_str = ", ".join(
                    f"{k}={v}" for k, v in spec.fixed_metadata.items()
                )
                parts.append(f"Fixed({inner_name}, {fixed_str})")
            elif isinstance(spec, ColumnSelection):
                parts.append(spec.__name__)
            else:
                parts.append(_display_name(spec))
        how = "" if self.how == "exact" else f", how={self.how!r}"
        return f"Merge({', '.join(parts)}, name={self.name!r}{how})"



# ---------------------------------------------------------------------------
# THE Merge rule (one owner: docs/claude/merge-cases.json is its spec).
#
# Consumers: scifor's per-combo loop (foreach._prepare_merge), scidb's
# per-combo loader (run.foreach._resolve_per_combo_merge), the flat-table
# exports (scifor.csv_export, scidb.csv_export). MATLAB's loop runs the same
# rule natively on MATLAB tables (+scifor/merge_tables.m,
# +scifor/drop_constant_keys.m) and is held to the same case file.
#
#   1. A per-combo constituent drops its schema-key columns that are CONSTANT
#      within it (they repeat the combo); varying ones (e.g. trial inside a
#      session combo) stay and become join keys.
#   2. Two constituents join on the schema-key columns they share. A shared
#      NON-key column is an error: the merge never joins on data values.
#   3. Joining never drops a row silently: key values present on one side
#      only are an error, and so is a many-to-many pairing. Rows come out in
#      the order of the side whose keys repeat (else the left side); columns
#      are left's, then right's minus the keys.
#   4. No shared key: column-wise concatenation, a 1-row constituent
#      broadcast; other height mismatches are an error. (A flat-table export
#      has no positional meaning, so it passes ``positional=False`` and gets
#      an error instead.)
#   5. A constituent with no rows is an error.
# ---------------------------------------------------------------------------

_MAX_LISTED = 5


def drop_constant_keys(df, schema_keys) -> "tuple[Any, dict]":
    """Rule 1: drop *df*'s schema-key columns that hold one value.

    Returns ``(frame, dropped)`` where ``dropped`` maps each dropped key to its
    value. Nothing is dropped when that would leave no columns.
    """
    import pandas as pd

    dropped: dict = {}
    for key in schema_keys:
        if key not in df.columns:
            continue
        col = df[key]
        if len(df) <= 1 or col.nunique(dropna=False) <= 1:
            dropped[key] = col.iloc[0] if len(df) else None
    keep = [c for c in df.columns if c not in dropped]
    if not keep or len(keep) == len(df.columns):
        return df.reset_index(drop=True), {}
    out = df[keep].reset_index(drop=True)
    return (out if isinstance(out, pd.DataFrame) else out.to_frame()), dropped


def _is_null(v) -> bool:
    import pandas as pd

    try:
        return bool(pd.isna(v))
    except (TypeError, ValueError):  # array-like cell: never a null key
        return False


def _key_tuple(row) -> tuple:
    """A row's key values, nulls as None (NaN != NaN would never match)."""
    return tuple(None if _is_null(v) else v for v in row)


def _key_rows(df, keys) -> list:
    return [_key_tuple(r) for r in df[keys].itertuples(index=False, name=None)]


def _repeated(keys) -> list:
    seen: set = set()
    rep: dict = {}
    for k in keys:
        if k in seen:
            rep[k] = None
        seen.add(k)
    return list(rep)


def _listed(values) -> str:
    vals = list(values)
    shown = ", ".join(repr(v[0] if len(v) == 1 else v) for v in vals[:_MAX_LISTED])
    more = f" (+{len(vals) - _MAX_LISTED} more)" if len(vals) > _MAX_LISTED else ""
    return f"[{shown}]{more}"


def _join_how(left, right, keys, lk, rk, how, l_label, r_label, only_l, only_r):
    """Rule 3 relaxed by an explicit ``how``: a key on one side only is kept
    (NaN-padded) or dropped as a SQL join would, and each such row is logged
    at WARN. Rows follow the driving side (right for ``right``, else left);
    an ``outer`` join appends the right-only rows after. Columns and the
    many-to-many / clash / empty errors are exactly as for ``exact``.
    """
    import pandas as pd

    r_pos: dict = {}
    for j, k in enumerate(rk):
        r_pos.setdefault(k, []).append(j)
    l_pos: dict = {}
    for i, k in enumerate(lk):
        l_pos.setdefault(k, []).append(i)

    pairs: list[tuple] = []  # (left row or None, right row or None)
    if how == "right":
        for j, k in enumerate(rk):
            pairs.extend((i, j) for i in l_pos.get(k, [None]))
    else:
        for i, k in enumerate(lk):
            if k in r_pos:
                pairs.extend((i, j) for j in r_pos[k])
            elif how in ("left", "outer"):
                pairs.append((i, None))
        if how == "outer":
            pairs.extend((None, j) for j, k in enumerate(rk) if k not in l_pos)

    def _side(df, idx):
        return df.reindex([-1 if x is None else x for x in idx]).reset_index(drop=True)

    l_idx = [p[0] for p in pairs]
    r_idx = [p[1] for p in pairs]
    out_left = _side(left.reset_index(drop=True), l_idx)
    # Key values come from whichever side has the row, so a right-only row
    # keeps its key (and the column keeps its dtype when nothing is missing).
    for key in keys:
        out_left[key] = pd.Series(
            [left[key].iat[i] if i is not None else right[key].iat[j] for i, j in pairs],
            dtype=left[key].dtype if None not in l_idx else None,
        )
    out = pd.concat(
        [out_left, _side(right.drop(columns=keys).reset_index(drop=True), r_idx)],
        axis=1,
    )

    dropped_l = only_l if how in ("inner", "right") else []
    dropped_r = only_r if how in ("inner", "left") else []
    padded_l = only_r if how in ("right", "outer") else []  # left columns NaN
    padded_r = only_l if how in ("left", "outer") else []  # right columns NaN
    parts = []
    if dropped_l:
        parts.append(f"dropped {l_label}-only {keys} {_listed(dropped_l)}")
    if dropped_r:
        parts.append(f"dropped {r_label}-only {keys} {_listed(dropped_r)}")
    if padded_l:
        parts.append(f"NaN-padded {l_label} columns at {_listed(padded_l)}")
    if padded_r:
        parts.append(f"NaN-padded {r_label} columns at {_listed(padded_r)}")
    if parts:
        Log.warn(
            f"[merge] {l_label} + {r_label} how={how!r}: " + "; ".join(parts),
            layer="scifor",
        )
    Log.debug(
        f"[merge] {l_label} + {r_label}: how={how!r} on {keys} "
        f"({len(left)} x {len(right)} -> {len(out)} row(s))",
        layer="scifor",
    )
    return out


def _join_empty(left, right, how, l_label, r_label):
    """A non-exact join where one side has no rows (at this combo, or at
    all): the kept side's rows with the other side's columns NaN, or no rows.
    The caller turns a 0-row result into "no data" (a skipped combo)."""
    import pandas as pd

    columns = list(left.columns) + [c for c in right.columns if c not in left.columns]
    if len(right) == 0 and len(left) > 0 and how in ("left", "outer"):
        out, kept, empty = left.reindex(columns=columns), l_label, r_label
    elif len(left) == 0 and len(right) > 0 and how in ("right", "outer"):
        out, kept, empty = right.reindex(columns=columns), r_label, l_label
    else:
        Log.warn(
            f"[merge] {l_label} + {r_label} how={how!r}: "
            f"{l_label if len(left) == 0 else r_label} has no rows -> no rows "
            f"(dropped {len(left) + len(right)})",
            layer="scifor",
        )
        return pd.DataFrame(columns=columns)
    Log.warn(
        f"[merge] {l_label} + {r_label} how={how!r}: {empty} has no rows; "
        f"kept {len(out)} {kept} row(s) with {empty} columns NaN-padded",
        layer="scifor",
    )
    return out.reset_index(drop=True)


def _merge_two(left, right, key_set, l_label, r_label, positional, how="exact"):
    import pandas as pd

    shared = [c for c in left.columns if c in right.columns]
    keys = [c for c in shared if key_set is None or c in key_set]
    clash = [c for c in shared if c not in keys]
    if clash:
        raise KeyError(
            f"Column name conflict in Merge: column(s) {clash} appear in both "
            f"{l_label} and {r_label}. Merge joins only on schema keys "
            f"({sorted(key_set) if key_set else 'none'}); select the column "
            f"from one constituent with ColumnSelection, or rename it."
        )
    if how != "exact" and (len(left) == 0 or len(right) == 0):
        return _join_empty(left, right, how, l_label, r_label)
    for label, df in ((l_label, left), (r_label, right)):
        if len(df) == 0:
            raise ValueError(
                f"Cannot merge: {label} has no rows (columns {list(df.columns)})."
            )

    if not keys:
        if not positional:
            raise ValueError(
                f"Cannot join {l_label} and {r_label}: no shared schema column "
                f"to join on. {l_label} columns: {list(left.columns)}; "
                f"{r_label} columns: {list(right.columns)}. Schema keys: "
                f"{sorted(key_set) if key_set else '(unset — call set_schema())'}."
            )
        lh, rh = len(left), len(right)
        if lh == 1 and rh > 1:
            left = pd.concat([left] * rh, ignore_index=True)
        elif rh == 1 and lh > 1:
            right = pd.concat([right] * lh, ignore_index=True)
        elif lh != rh:
            raise ValueError(
                f"Cannot merge constituents with different row counts: "
                f"{l_label} has {lh}, {r_label} has {rh}."
            )
        Log.debug(
            f"[merge] {l_label} + {r_label}: no shared key, column-wise "
            f"({len(left)} row(s))",
            layer="scifor",
        )
        return pd.concat(
            [left.reset_index(drop=True), right.reset_index(drop=True)], axis=1
        )

    lk, rk = _key_rows(left, keys), _key_rows(right, keys)
    l_dup, r_dup = len(set(lk)) < len(lk), len(set(rk)) < len(rk)
    if l_dup and r_dup:
        raise ValueError(
            f"Cannot merge {l_label} and {r_label} on {keys}: both repeat key "
            f"values, so rows cannot be paired. Repeated in {l_label}: "
            f"{_listed(_repeated(lk))}."
        )
    l_set, r_set = set(lk), set(rk)
    only_l = [k for k in dict.fromkeys(lk) if k not in r_set]
    only_r = [k for k in dict.fromkeys(rk) if k not in l_set]
    if how != "exact":
        return _join_how(left, right, keys, lk, rk, how, l_label, r_label, only_l, only_r)
    if only_l or only_r:
        raise ValueError(
            f"Merge would drop rows: {l_label} and {r_label} do not cover the "
            f"same {keys}. Only in {l_label}: {_listed(only_l)}; only in "
            f"{r_label}: {_listed(only_r)}. Filter both constituents to the "
            f"same locations first (where= or metadata)."
        )
    if r_dup:
        index = {k: i for i, k in enumerate(lk)}
        l_idx, r_idx = [index[k] for k in rk], list(range(len(rk)))
    else:
        index = {k: i for i, k in enumerate(rk)}
        l_idx, r_idx = list(range(len(lk))), [index[k] for k in lk]
    out = pd.concat(
        [
            left.iloc[l_idx].reset_index(drop=True),
            right.drop(columns=keys).iloc[r_idx].reset_index(drop=True),
        ],
        axis=1,
    )
    Log.debug(
        f"[merge] {l_label} + {r_label}: joined on {keys} "
        f"({len(left)} x {len(right)} -> {len(out)} row(s))",
        layer="scifor",
    )
    return out


def merge_frames(
    parts, key_columns=None, *, labels=None, positional: bool = True, how: str = "exact"
):
    """Rules 2-5: combine constituent frames into one, left to right.

    Args:
        parts: the constituent DataFrames (already filtered, and for a
            per-combo merge already through :func:`drop_constant_keys`).
        key_columns: the schema keys; shared columns among them are join
            keys. ``None`` treats every shared column as a key (a flat-table
            export with no schema set).
        labels: names for error messages (default ``merge[i]``).
        positional: allow the no-shared-key column-wise concatenation.
        how: one of :data:`MERGE_HOWS`, applied at every left-to-right step.
            Only changes what happens to a key on one side only (rule 3).
    """
    if how not in MERGE_HOWS:
        raise ValueError(f"Merge how={how!r} is not one of {list(MERGE_HOWS)}.")
    if not parts:
        raise ValueError("Merge has no constituents.")
    labels = list(labels) if labels else [f"merge[{i}]" for i in range(len(parts))]
    key_set = None if key_columns is None else set(key_columns)
    acc, acc_label = parts[0].reset_index(drop=True), labels[0]
    if len(parts) == 1 and len(acc) == 0:
        raise ValueError(f"Cannot merge: {acc_label} has no rows.")
    for label, part in zip(labels[1:], parts[1:]):
        acc = _merge_two(
            acc, part.reset_index(drop=True), key_set, acc_label, label, positional, how
        )
        acc_label = f"{acc_label}+{label}"
    return acc
