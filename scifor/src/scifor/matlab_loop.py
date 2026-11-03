"""MATLAB's ``scifor.for_each``: the plan and the summary, decided here.

``+scifor/for_each.m`` keeps only what has to run in MATLAB: slicing its own
tables, calling the user's function per combination, and stacking the
results. Everything else -- combos, ``[]`` resolution, PathInput discovery,
``locations=``, the distribute target, ``as_table``, column groups,
``share_limits``, per-combo paths, the banner, the summary, the journal --
is decided by Python's ``scifor.foreach`` through :func:`plan` and
:func:`assemble` (design review 05, M3). MATLAB never re-derives any of it.

MATLAB tables cannot become DataFrames cheaply, so MATLAB *describes* them:
column names, row count, and the values of the key columns only. This
module rebuilds each input as a **surrogate** (a real DataFrame / ``Fixed`` /
``ColumnSelection`` / ``Merge`` / ``PathInput`` holding those key columns and
placeholder data columns) and runs ``scifor.foreach.plan_run`` on it, so the
planning code is the same code Python's own loop runs.

The wire (every value crossing is ``str`` or ``bytes``, so no datetime
scalar can cross bare -- the hazard ``scimatlab.bridge.crossing`` guards;
this module cannot import it, because anything under ``scimatlab`` imports
scidb and MATLAB scifor must stay scidb-free):

MATLAB -> :func:`plan`
    ``description_json``: see :class:`_Description`. Numbers travel in
    ``numbers`` (one ``bytes`` of little-endian float64), referenced as
    ``[offset, count]``; text travels in the JSON. scidb's loop passes its
    pre-built ``full_combos``, ``row_selection`` and
    ``resolved_path_outputs`` as the Python objects ``for_each_prepare``
    returned (they never become MATLAB values).

:func:`plan` -> MATLAB, a dict of
    ``json``: the plan (:func:`_plan_wire`);
    ``combo_index``: int32, ``n_combos x n_keys``, column-major, 1-based
    index into each key's value pool (0: the combo lacks the key);
    ``row_index`` / ``row_offsets``: int64; the rows (1-based) slice ``s``
    reads at combo ``c`` are ``row_index[row_offsets[s, c] : row_offsets[s, c+1]]``;
    ``limits``: float64, ``n_combos x n_limits x 2`` (NaN: no limits).

MATLAB -> :func:`assemble` -> MATLAB
    what happened (counts, failure records by reason, skipped outputs);
    back come the ``summary_fn`` struct and the combine column list.
    :func:`close` writes the run's journal block.
"""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import io
import itertools
import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# numpy and pandas are not dependencies of scifor itself, but this module is
# imported only by MATLAB's loop, whose stack (scimatlab -> scidb) requires
# both. Importing them here keeps every function below import-free.
import numpy as np
import pandas as pd
from scistacklog import Log

from .colname import ColName
from .column_selection import SPLIT, ColumnGroupsError, ColumnSelection
from .failures import ComboFailure, FailureGroup, groups_from_wire
from .fixed import Fixed
from .foreach import (
    RunPlan,
    RunTally,
    _all_data_columns,
    _announce,
    _apply_exclusions,
    _check_column_drift,
    _combine_output_columns,
    _compute_shared_limits,
    _excluded_columns,
    _inject_shared_limits,
    _is_per_combo_df,
    _log_summary,
    _option_parts,
    _print_dry_run_iteration,
    _resolve_data_spec,
    _unwrap_column_selection,
    bind_path_inputs,
    iterated_schema_keys,
    plan_run,
    progress_guards,
    progress_line,
    stop_line,
    ProgressReporter,
)
from .hooks import LoopHooks
from .keys import key_text as matlab_text
from .keys import numeric_mask, text_mask
from .locations import filter_combo_indices, locations_from_text
from .merge import Merge
from .outputs import EMPTY_TABLE_DETAIL, NoOutput
from .pathinput import PathInput, matlab_error_id
from .pathoutput import PathOutput
from .project.root import project_root as current_project_root
from .schema_types import SchemaKeyTypeError, validate_key_types

LAYER = "scifor"

#: How MATLAB spells a ``__`` column name once it has crossed the bridge
#: (``__record_id`` -> ``x__record_id``); a surrogate restores ``__`` so
#: Python's internal-column rule (``_all_data_columns``) applies unchanged.
_MATLAB_INTERNAL_PREFIX = "x__"


#: MATLAB error identifiers that end the run instead of failing one combo:
#: they would fail every combo identically. The MATLAB spellings of Python's
#: rule (an exception marked ``scifor_fatal``, e.g. scidb's SchemaKeyTypeError,
#: and ``ForColumnsError``), which ``run_combo`` lets propagate.
FATAL_IDS = (
    "scidb:SchemaKeyTypeError",
    "scifor:SchemaKeyTypeError",
    "scifor:for_each:forColumnsDuplicate",
    "scifor:for_each:forColumnsBadReturn",
)


class MatlabWireError(ValueError):
    """The description MATLAB sent does not have the documented shape. A
    SciStack bug (``+scifor/for_each.m`` and this module disagree), never a
    user error."""


# ---------------------------------------------------------------------------
# Value spelling shared with MATLAB
# ---------------------------------------------------------------------------


def matlab_number(x: float) -> "int | float":
    """A MATLAB double as Python sees it: whole numbers become ``int`` (so a
    combo reads ``subject=1``, as ``scidb`` sends whole-number metadata)."""
    if math.isfinite(x) and x == int(x) and abs(x) < 2**53:
        return int(x)
    return float(x)


# matlab_text (MATLAB's string() spelling) and the number parse are THE
# key-match rule's, owned by scifor.keys (spec: key-match-cases.json).


# ---------------------------------------------------------------------------
# The description MATLAB sends
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _KeyColumn:
    """One key column of a MATLAB table: its MATLAB class, and its values in
    the two forms the plan needs (surrogate values; matching values)."""

    name: str
    matlab_class: str
    #: Python values for the surrogate frame (None: missing).
    values: list
    #: For row matching: float64 array (numeric) or object array (text).
    match: np.ndarray
    numeric: bool
    categories: tuple = ()
    ordinal: bool = False


@dataclass(frozen=True)
class _Table:
    columns: tuple
    n_rows: int
    keys: "dict[str, _KeyColumn]"
    record_ids: "np.ndarray | None"
    #: ``(n_rows, n_columns, 2)`` per-row ``[min, max]`` of each column
    #: (share_limits inputs only).
    extents: "np.ndarray | None"


@dataclass(frozen=True)
class _Input:
    name: str
    kind: str
    table: "_Table | None" = None
    inner: "_Input | None" = None
    fixed: dict = field(default_factory=dict)
    columns: tuple = ()
    groups: tuple = ()
    iterate: bool = False
    constituents: tuple = ()
    template: str = ""
    text: str = ""
    path_input: Any = None

    @property
    def raw_table(self) -> "_Table | None":
        """The table ``_get_raw_df`` would read (bare, ``Fixed``-wrapped or
        under a ``ColumnSelection``; never a Merge constituent)."""
        if self.kind in ("table", "column_selection"):
            return self.table
        if self.kind == "fixed" and self.inner is not None and self.inner.kind == "table":
            return self.inner.table
        return None

    @property
    def filter_table(self) -> "_Table | None":
        """The table the per-combo filter reads (Fixed(ColumnSelection) too)."""
        if self.kind in ("table", "column_selection"):
            return self.table
        if self.kind == "fixed" and self.inner is not None:
            return self.inner.table
        return None


@dataclass(frozen=True)
class _Description:
    fn_name: str
    fn_nargin: int
    fn_nargout: int
    call_nargout: int
    outputs: dict
    metadata: "dict[str, list]"
    schema_keys: "list[str] | None"
    options: dict
    inputs: "tuple[_Input, ...]"


def _listify(value: Any) -> list:
    """A JSON list field as MATLAB's ``jsonencode`` writes it: a one-element
    list may arrive as the bare element, an empty one as ``[]`` or null."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _object(value: Any) -> dict:
    """A JSON object field; anything else (MATLAB's `[]` for "none") is {}."""
    return value if isinstance(value, dict) else {}


def _numbers_at(ref: Any, numbers: np.ndarray) -> np.ndarray:
    ref = _listify(ref)
    if len(ref) != 2:
        raise MatlabWireError(f"numeric reference must be [offset, count], got {ref!r}")
    offset, count = int(ref[0]), int(ref[1])
    if offset < 0 or offset + count > numbers.size:
        raise MatlabWireError(
            f"numeric reference [{offset}, {count}] outside the {numbers.size}-value buffer"
        )
    return numbers[offset : offset + count]


def _decode_values(raw: Any, numbers: np.ndarray) -> list:
    """A metadata iterable's values: ``{"numbers": ref}``, ``{"texts": [...]}``
    or ``{"items": [...]}`` (a mixed cell, each element a JSON scalar)."""
    if not isinstance(raw, dict):
        raise MatlabWireError(f"metadata values must be an object, got {raw!r}")
    if "numbers" in raw:
        return [matlab_number(float(x)) for x in _numbers_at(raw["numbers"], numbers)]
    if "texts" in raw:
        return [str(t) for t in _listify(raw["texts"])]
    if "items" in raw:
        return [matlab_number(float(v)) if isinstance(v, (int, float)) and not isinstance(v, bool)
                else v for v in _listify(raw["items"])]
    raise MatlabWireError(f"metadata values need numbers, texts or items: {raw!r}")


def _decategorized(labels: list) -> "list | None":
    """A categorical key column's labels as numbers when EVERY label is a
    number's MATLAB spelling (``"1"`` -> 1); else None (kept as text). A
    categorical erases whether its source was numeric or text, and schema-key
    identity must not change type because of it; ``"01"`` stays text
    (spelling is identity for undeclared keys, docs/claude/schema-key-types.md)."""
    if not labels or any(label is None for label in labels):
        return None
    out = []
    for label in labels:
        try:
            x = float(label)
        except ValueError:
            return None
        if math.isnan(x) or matlab_text(matlab_number(x)) != label:
            return None
        out.append(matlab_number(x))
    return out


def _text_or_none(value: Any) -> "str | None":
    """A text cell; MATLAB writes a missing one as `[]` (jsonencode has no null)."""
    return None if value is None or isinstance(value, list) else str(value)


def _decode_key_column(name: str, raw: dict, numbers: np.ndarray, n_rows: int) -> _KeyColumn:
    cls = str(raw.get("class") or "")
    if "numbers" in raw:
        arr = np.asarray(_numbers_at(raw["numbers"], numbers), dtype="float64")
        if arr.size != n_rows:
            raise MatlabWireError(f"key column {name!r}: {arr.size} value(s) for {n_rows} row(s)")
        if cls == "logical":
            values: list = [None if math.isnan(x) else bool(x) for x in arr]
        else:
            values = [None if math.isnan(x) else matlab_number(float(x)) for x in arr]
        return _KeyColumn(name, cls, values, arr, numeric=True)
    if "texts" in raw:
        texts = [_text_or_none(t) for t in _listify(raw["texts"])]
        if len(texts) != n_rows:
            raise MatlabWireError(f"key column {name!r}: {len(texts)} value(s) for {n_rows} row(s)")
        match = np.asarray(texts, dtype=object)
        categories = tuple(str(c) for c in _listify(raw.get("categories")))
        if cls == "categorical":
            as_numbers = _decategorized(texts)
            Log.debug(
                f"[matlab-plan] key column {name!r}: categorical kept as "
                f"{'numbers' if as_numbers is not None else 'text'}",
                layer=LAYER,
            )
            if as_numbers is not None:
                return _KeyColumn(name, cls, as_numbers, match, numeric=False,
                                  categories=categories, ordinal=bool(raw.get("ordinal")))
        return _KeyColumn(name, cls, texts, match, numeric=False,
                          categories=categories, ordinal=bool(raw.get("ordinal")))
    # A column MATLAB could not spell as numbers or text: its class still
    # counts for the round-trip types, but nothing can match it.
    return _KeyColumn(name, cls, [None] * n_rows, np.full(n_rows, None, dtype=object), numeric=False)


def _decode_table(raw: Any, numbers: np.ndarray) -> _Table:
    if not isinstance(raw, dict):
        raise MatlabWireError(f"table must be an object, got {raw!r}")
    columns = tuple(str(c) for c in _listify(raw.get("columns")))
    n_rows = int(raw.get("n_rows") or 0)
    keys_raw = raw.get("keys") or {}
    if not isinstance(keys_raw, dict):
        keys_raw = {}
    keys = {
        str(name): _decode_key_column(str(name), spec, numbers, n_rows)
        for name, spec in keys_raw.items()
        if isinstance(spec, dict)
    }
    rids_raw = raw.get("record_ids")
    record_ids = None
    if rids_raw not in (None, []):
        rids = [str(r) for r in _listify(rids_raw)]
        if len(rids) != n_rows:
            raise MatlabWireError(f"{len(rids)} record id(s) for {n_rows} row(s)")
        record_ids = np.asarray(rids, dtype=object)
    extents = None
    ext_raw = raw.get("extents")
    if isinstance(ext_raw, dict):
        flat = np.asarray(_numbers_at(ext_raw.get("numbers"), numbers), dtype="float64")
        if flat.size != n_rows * len(columns) * 2:
            raise MatlabWireError(
                f"extents: {flat.size} value(s) for {n_rows} row(s) x {len(columns)} column(s) x 2"
            )
        extents = flat.reshape((n_rows, len(columns), 2), order="F")
    return _Table(columns, n_rows, keys, record_ids, extents)


def _decode_input(raw: Any, numbers: np.ndarray, path_inputs: dict) -> _Input:
    if not isinstance(raw, dict) or "kind" not in raw:
        raise MatlabWireError(f"input must be an object with a kind, got {raw!r}")
    name = str(raw.get("name") or "")
    kind = str(raw["kind"])
    table = _decode_table(raw["table"], numbers) if raw.get("table") is not None else None
    inner = _decode_input(raw["inner"], numbers, path_inputs) if raw.get("inner") is not None else None
    groups = tuple(
        (str(g.get("name") or ""), tuple(str(c) for c in _listify(g.get("columns"))))
        for g in _listify(raw.get("groups"))
        if isinstance(g, dict)
    )
    constituents = tuple(
        _decode_input(c, numbers, path_inputs) for c in _listify(raw.get("constituents"))
    )
    fixed = {
        str(k): matlab_number(float(v)) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
        for k, v in _object(raw.get("fixed")).items()
    }
    path_input = None
    if kind in ("path_input", "fixed_path_input"):
        path_input = path_inputs.get(name)
        if not isinstance(path_input, PathInput):
            raise MatlabWireError(f"input {name!r}: no Python PathInput was passed for it")
    if kind not in _KINDS:
        raise MatlabWireError(f"input {name!r}: unknown kind {kind!r}")
    return _Input(
        name=name,
        kind=kind,
        table=table,
        inner=inner,
        fixed=fixed,
        columns=tuple(str(c) for c in _listify(raw.get("columns"))),
        groups=groups,
        iterate=bool(raw.get("iterate")),
        constituents=constituents,
        template=str(raw.get("template") or ""),
        text=str(raw.get("text") or ""),
        path_input=path_input,
    )


_KINDS = frozenset({
    "table", "fixed", "column_selection", "merge", "path_input",
    "fixed_path_input", "path_output", "colname_deferred", "colname_static",
    "constant",
})


def _decode(description_json: str, numbers: bytes, path_inputs: "dict | None") -> _Description:
    raw = json.loads(str(description_json))
    if not isinstance(raw, dict):
        raise MatlabWireError("the description must be a JSON object")
    buffer = np.frombuffer(bytes(numbers), dtype="<f8") if numbers else np.zeros(0)
    pis = dict(path_inputs or {})
    metadata = {}
    for entry in _listify(raw.get("metadata")):
        if not isinstance(entry, dict) or "key" not in entry:
            raise MatlabWireError(f"metadata entry must be {{key, values}}: {entry!r}")
        metadata[str(entry["key"])] = _decode_values(entry.get("values"), buffer)
    schema_keys = raw.get("schema_keys")
    return _Description(
        fn_name=str(raw.get("fn_name") or "fn"),
        fn_nargin=int(raw.get("fn_nargin", -1)),
        fn_nargout=int(raw.get("fn_nargout", -1)),
        call_nargout=int(raw.get("call_nargout", 1)),
        outputs=_object(raw.get("outputs")) or {"kind": "default"},
        metadata=metadata,
        schema_keys=[str(k) for k in _listify(schema_keys)] or None,
        options=_object(raw.get("options")),
        inputs=tuple(_decode_input(i, buffer, pis) for i in _listify(raw.get("inputs"))),
    )


# ---------------------------------------------------------------------------
# Surrogates: the MATLAB inputs as the Python planner reads them
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _MatlabValue:
    """A MATLAB value the planner only classifies and shows (a constant)."""

    text: str

    def __repr__(self) -> str:
        return self.text


@dataclass(frozen=True)
class _MatlabWhere:
    """MATLAB's ``where=`` filter: applied by MATLAB to its own rows; the
    planner only shows it on the options line."""

    text: str

    def __repr__(self) -> str:
        return self.text


class _MatlabFunction:
    """The user's MATLAB function as the planner sees it: a name."""

    def __init__(self, name: str):
        self.__name__ = name

    def __call__(self, *args: Any, **kwargs: Any) -> Any:  # never called
        raise RuntimeError("a MATLAB function runs in MATLAB")


def _surrogate_column(name: str, table: _Table, extents_for: "set[str] | None") -> list:
    if extents_for is not None and name in extents_for and table.extents is not None:
        col = table.columns.index(name)
        return [
            np.asarray(table.extents[r, col, :]) if not math.isnan(table.extents[r, col, 0]) else math.nan
            for r in range(table.n_rows)
        ]
    key = table.keys.get(name)
    if key is not None:
        return list(key.values)
    return [None] * table.n_rows


def _python_column_name(name: str) -> str:
    if name.startswith(_MATLAB_INTERNAL_PREFIX):
        return name[1:]
    return name


def _surrogate_frame(table: _Table, *, extents_for: "set[str] | None" = None) -> pd.DataFrame:
    """The table as a DataFrame: key columns hold their values, the others
    placeholders (or, for share_limits, each row's ``[min, max]``)."""
    data = {
        _python_column_name(c): pd.Series(_surrogate_column(c, table, extents_for), dtype=object)
        for c in table.columns
    }
    frame = pd.DataFrame(data, index=pd.RangeIndex(table.n_rows))
    for c in table.keys:
        pc = _python_column_name(c)
        if pc in frame.columns:
            frame[pc] = frame[pc].infer_objects()
    return frame


def _column_spec(item: _Input) -> Any:
    if not item.groups:
        return list(item.columns)
    if all(name == "" for name, _ in item.groups):
        return [list(cols) for _, cols in item.groups]
    return {name: list(cols) for name, cols in item.groups}


def _surrogate(item: _Input, *, extents_for: "set[str] | None" = None) -> Any:
    kind = item.kind
    if kind == "table" and item.table is not None:
        return _surrogate_frame(item.table, extents_for=extents_for)
    if kind == "column_selection" and item.table is not None:
        return ColumnSelection(
            _surrogate_frame(item.table, extents_for=extents_for), _column_spec(item), item.iterate
        )
    if kind == "fixed" and item.inner is not None:
        return Fixed(_surrogate(item.inner, extents_for=extents_for), **item.fixed)
    if kind == "merge":
        return Merge(*(_surrogate(c) for c in item.constituents))
    if kind == "path_input":
        return item.path_input
    if kind == "path_output":
        return PathOutput(item.template)
    if kind == "colname_deferred":
        return ColName()
    if kind == "colname_static" and item.table is not None:
        return ColName(_surrogate_frame(item.table))
    return _MatlabValue(item.text or f"<{kind}>")


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


@dataclass
class _PlanState:
    """What :func:`assemble`, :func:`log_progress` and :func:`close` need."""

    run: RunPlan
    labels: "list[str]"
    progress_key: str
    marks: "dict[int, tuple[int, Any]]"
    progress_total: int
    started: float
    summary: "dict | None" = None
    completed: int = 0
    skipped: int = 0


_PLANS: "dict[int, _PlanState]" = {}
_TOKENS = itertools.count(1)


def matlab_output_names(outputs: dict, fn_nargout: int, call_nargout: int) -> "list[str]":
    """MATLAB's result column names. Every MATLAB output is its own table, so
    a count (or the function's ``nargout``) names each one ``output``; names
    arrive already spelled (``scifor.output_column_name`` ran per object)."""
    kind = outputs.get("kind")
    if kind == "names":
        return [str(n) for n in _listify(outputs.get("names"))]
    if kind == "count":
        return ["output"] * int(outputs.get("n") or 0)
    n = fn_nargout if fn_nargout >= 0 else max(call_nargout, 1)
    return ["output"] * n


def _limits_fit(fn_nargin: int, n_inputs: int, n_limits: int) -> bool:
    """MATLAB passes ``<input>_limits`` as trailing positional arguments, so
    they are passed only when the function has room for them (varargin
    always has)."""
    return fn_nargin < 0 or fn_nargin >= n_inputs + n_limits


def _warn_positional_arity(fn_name: str, input_names: "list[str]", n_limit_args: int, fn_nargin: int) -> None:
    """WARN when the inputs struct cannot fill the function's arguments one
    for one. MATLAB binds inputs BY FIELD ORDER (names never reach the
    function), so a struct with the wrong number of fields shifts every later
    argument onto the wrong parameter without an error (2026-09-02:
    ``fieldnames()`` on a sampling rate). Advisory: a function may branch on
    ``nargin``. Skipped for varargin and handles MATLAB cannot introspect
    (``fn_nargin < 0``)."""
    if fn_nargin < 0 or len(input_names) + n_limit_args == fn_nargin:
        return
    Log.warn(
        f"{fn_name}: the inputs struct has {len(input_names)} field(s) "
        f"[{', '.join(input_names)}] but the function declares {fn_nargin} "
        f"argument(s). Inputs bind to arguments BY POSITION (field names are "
        f"not used), so argument {min(len(input_names), fn_nargin) + 1} onward "
        f"will receive a value meant for a different parameter, or none at all. "
        f"Check that every parameter is wired and that the struct is built in "
        f"signature order.",
        layer=LAYER,
    )


def _pool_key(value: Any) -> tuple:
    if isinstance(value, (bool, np.bool_)):
        return ("b", bool(value))
    if isinstance(value, (int, float, np.integer, np.floating)):
        f = float(value)
        return ("nan",) if math.isnan(f) else ("n", f)
    if value is None:
        return ("none",)
    return ("s", str(value))


def _wire_value(value: Any) -> Any:
    """A combo value as JSON MATLAB decodes to the same MATLAB value the
    element-wise conversion gave (number -> double, text -> char, None -> [])."""
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {"nonfinite": "NaN" if math.isnan(value) else ("Inf" if value > 0 else "-Inf")}
    return str(value)


def _combo_wire(combos: "list[dict]") -> "tuple[list[str], list[list], np.ndarray]":
    """Combos as key names, one value pool per key, and an index matrix."""
    keys: "list[str]" = list(dict.fromkeys(k for c in combos for k in c))
    pools: "list[list]" = [[] for _ in keys]
    seen: "list[dict]" = [{} for _ in keys]
    index = np.zeros((len(combos), len(keys)), dtype="<i4")
    for ci, combo in enumerate(combos):
        for ki, key in enumerate(keys):
            if key not in combo:
                continue
            value = combo[key]
            pk = _pool_key(value)
            slot = seen[ki].get(pk)
            if slot is None:
                pools[ki].append(_wire_value(value))
                slot = seen[ki][pk] = len(pools[ki])
            index[ci, ki] = slot
    return keys, pools, index


def _combo_label(combo: dict) -> str:
    """``subject=1, session=A`` (the same text Python's loop logs)."""
    return ", ".join(f"{k}={v}" for k, v in combo.items())


def _key_mask(column: _KeyColumn, value: Any) -> np.ndarray:
    """Rows of a MATLAB key column equal to a combo value, by MATLAB's rule:
    a numeric column compares numbers (text values parsed), every other
    column compares text."""
    if column.numeric:
        return numeric_mask(column.match, value)
    return text_mask(column.match, value)


class _Slicer:
    """Row indices per (slice, combo): the schema filter, then scidb's
    record-id selection. Python decides which rows a combination reads;
    MATLAB only indexes."""

    def __init__(self, n_combos: int):
        self.n_combos = n_combos
        self.rows: "list[list[np.ndarray]]" = []

    def add(self, table: _Table, filter_keys: "list[str]", combos: "list[dict]",
            fixed: dict, selections: "list[list[str] | None] | None") -> int:
        """Add one slice; returns its 1-based number."""
        rid_rows: "dict[str, list[int]]" = {}
        # A table without a record-id column (scidb strips it from Fixed,
        # ColumnSelection and Merge inputs) has nothing to select by: its
        # selection is ignored, as the hook leaves a frame it cannot narrow.
        if table.record_ids is None:
            if selections is not None and any(selections):
                Log.debug(
                    "[matlab-plan] record-id selection ignored for a table without "
                    "a record-id column (Fixed / ColumnSelection / Merge input)",
                    layer=LAYER,
                )
            selections = None
        if selections is not None and table.record_ids is not None:
            for i, rid in enumerate(table.record_ids):
                rid_rows.setdefault(rid, []).append(i)
        per_combo: "list[np.ndarray]" = []
        cache: "dict[tuple, np.ndarray]" = {}
        all_rows = np.arange(table.n_rows, dtype="<i8")
        keys_here = [k for k in filter_keys if k in table.keys]
        for ci, combo in enumerate(combos):
            effective = {**combo, **fixed}
            used = [k for k in keys_here if k in effective]
            signature = tuple(_pool_key(effective[k]) for k in used)
            rows = cache.get(signature)
            if rows is None:
                mask = np.ones(table.n_rows, dtype=bool)
                for k in used:
                    mask &= _key_mask(table.keys[k], effective[k])
                rows = all_rows[mask]
                cache[signature] = rows
            sel = selections[ci] if selections is not None else None
            if sel:
                picked = np.unique(
                    np.asarray([i for rid in sel for i in rid_rows.get(str(rid), ())], dtype="<i8")
                )
                rows = np.intersect1d(rows, picked, assume_unique=True)
            per_combo.append(rows)
        self.rows.append(per_combo)
        return len(self.rows)

    def buffers(self) -> "tuple[bytes, bytes, int]":
        n_slices = len(self.rows)
        offsets = np.zeros((n_slices, self.n_combos + 1), dtype="<i8")
        chunks: "list[np.ndarray]" = []
        total = 0
        for s, per_combo in enumerate(self.rows):
            for c, rows in enumerate(per_combo):
                offsets[s, c] = total
                chunks.append(rows + 1)
                total += rows.size
            offsets[s, self.n_combos] = total
        flat = np.concatenate(chunks) if chunks else np.zeros(0, dtype="<i8")
        return flat.astype("<i8").tobytes(), offsets.tobytes(order="F"), int(total)


def _selections_for(name: str, row_selection: "list | None", sources: "list[int] | None") -> "list | None":
    if row_selection is None or sources is None:
        return None
    out = []
    for src in sources:
        sel = row_selection[src] if src < len(row_selection) else None
        rids = sel.get(name) if isinstance(sel, dict) else None
        out.append([str(r) for r in rids] if rids else None)
    return out


def _selected_columns(item: _Input, surrogate: Any, filter_keys: "list[str]") -> "list[str] | None":
    """A column selection's columns as the planner resolves them (an empty
    selection is every data column), in MATLAB spelling; None: no selection."""
    df, _meta, selection = _resolve_data_spec(surrogate, {})
    if selection is None:
        return None
    cols = list(selection) or _all_data_columns(df, filter_keys)
    return _apply_exclusions(cols, _excluded_columns(surrogate))


def _delivery(item: _Input, surrogate: Any, run: RunPlan, filter_keys: "list[str]",
              slicer: _Slicer, combos: "list[dict]", row_selection: "list | None",
              sources: "list[int] | None", options: dict) -> dict:
    """How MATLAB hands one data input to the function at each combo."""
    as_table = item.name in run.as_table_set
    iterate = run.for_columns is not None and item.name in run.for_columns.params
    rid_col = str(options.get("record_id_column") or "")
    if item.kind == "merge":
        constituents = []
        for part, part_surrogate in zip(item.constituents, surrogate.tables, strict=True):
            table = part.filter_table
            if table is None:
                raise MatlabWireError(f"Merge constituent of {item.name!r} has no table")
            per_combo = _is_per_combo_df(_resolve_data_spec(part_surrogate, {})[0], filter_keys)
            constituents.append({
                "per_combo": per_combo,
                "slice": slicer.add(table, filter_keys, combos, part.fixed, None) if per_combo else 0,
                "columns": _selected_columns(part, part_surrogate, filter_keys),
            })
        return {"name": item.name, "kind": "data", "shape": "merge", "constituents": constituents}
    table = item.filter_table
    if table is None:
        raise MatlabWireError(f"data input {item.name!r} has no table")
    df = _resolve_data_spec(surrogate, {})[0]
    per_combo = _is_per_combo_df(df, filter_keys)
    fixed = item.fixed if item.kind == "fixed" else {}
    selections = _selections_for(item.name, row_selection, sources)
    columns = None if iterate else _selected_columns(item, surrogate, filter_keys)
    mapping = (options.get("mapping_inputs") or {}).get(item.name)
    return {
        "name": item.name,
        "kind": "data",
        "shape": "iterate" if iterate else "table",
        "per_combo": per_combo,
        "slice": slicer.add(table, filter_keys, combos, fixed, selections) if per_combo else 0,
        "as_table": as_table,
        "columns": columns,
        "mapping": [str(c) for c in _listify(mapping)] if (mapping and not as_table and columns is None) else [],
        "drop": [rid_col] if rid_col and rid_col in table.columns else [],
        "no_data_when_empty": iterate or not as_table,
    }


def _path_delivery(name: str, paths: "list[str]", errors: "list[tuple[str, str]]", value_class: str) -> dict:
    """A path per combo, with parallel error lists ("" where it resolved)
    and the MATLAB class the path arrives as (PathInput.load gives a
    ``string``, PathOutput.resolve a ``char``)."""
    return {
        "name": name,
        "kind": "path",
        "value_class": value_class,
        "paths": paths,
        "error_ids": [e[0] for e in errors],
        "error_messages": [e[1] for e in errors],
    }


def _resolved_paths(item: _Input, run: RunPlan, combos: "list[dict]") -> dict:
    """Every combo's path for a PathInput constant (Fixed overrides applied,
    the declared-string-key resolver when the plan has one), or the error
    MATLAB raises at that combo: a missing file fails that combo only, as in
    Python's loop."""
    pi = item.path_input
    resolver = run.path_input_resolver or (lambda p, m: p.load(**m))
    paths: "list[str]" = []
    errors: "list[tuple[str, str]]" = []
    for combo in combos:
        metadata = {**combo, **item.fixed} if item.kind == "fixed_path_input" else combo
        try:
            paths.append(str(resolver(pi, metadata)))
            errors.append(("", ""))
        except Exception as e:  # recorded as that combo's failure in MATLAB
            paths.append("")
            errors.append((matlab_error_id(str(e)), str(e)))
    n_failed = sum(1 for e in errors if e[0])
    Log.debug(
        f"[matlab-plan] {item.name}: {len(paths) - n_failed} path(s) resolved, {n_failed} failed",
        layer=LAYER,
    )
    return _path_delivery(item.name, paths, errors, "string")


def _constant_delivery(item: _Input, run: RunPlan, combos: "list[dict]",
                       resolved_path_outputs: "dict | None", sources: "list[int] | None",
                       options: dict) -> dict:
    """How MATLAB hands one non-data input to the function at each combo."""
    if item.kind == "path_output":
        given = (resolved_path_outputs or {}).get(item.name)
        if given is not None and sources is not None:
            paths = [str(given[s]) for s in sources]
        else:
            paths = [str(PathOutput(item.template).resolve(c, None)) for c in combos]
        return _path_delivery(item.name, paths, [("", "")] * len(paths), "char")
    if item.kind in ("path_input", "fixed_path_input"):
        if not options.get("resolve_pathinput", True):
            return {"name": item.name, "kind": "constant"}
        if options.get("has_pathinput_loader"):
            return {"name": item.name, "kind": "pathinput_loader"}
        return _resolved_paths(item, run, combos)
    if item.kind == "colname_static":
        return {"name": item.name, "kind": "value", "value": str(run.constant_inputs.get(item.name))}
    return {"name": item.name, "kind": "constant"}


def _column_types(items: "tuple[_Input, ...]", keys: "list[str]") -> dict:
    """Per key, the MATLAB class of the input column it came from (first
    seen), with categories; ``conflict`` when the input tables disagree. The
    MATLAB twin of ``_capture_schema_column_dtypes`` (classes, not dtypes)."""
    out: dict = {}
    for item in items:
        table = item.raw_table
        if table is None:
            continue
        for key in keys:
            col = table.keys.get(key)
            if col is None:
                continue
            if key not in out:
                out[key] = {"class": col.matlab_class, "categories": list(col.categories),
                            "ordinal": col.ordinal, "conflict": False}
                Log.debug(
                    f"schema key '{key}' (input '{item.name}'): input column type {col.matlab_class}",
                    layer=LAYER,
                )
            elif out[key]["class"] != col.matlab_class:
                out[key]["conflict"] = True
    return out


def _limits(desc: _Description, run: RunPlan, combos: "list[dict]") -> "tuple[list[str], np.ndarray]":
    """``share_limits``: names in order, and each combo's ``[min, max]``."""
    share_limits = desc.options.get("share_limits") or {}
    share_limits = {str(k): [str(v) for v in _listify(vs)] for k, vs in share_limits.items()}
    if not share_limits:
        return [], np.zeros((len(combos), 0, 2))
    limit_inputs = {}
    for item in desc.inputs:
        if item.name in share_limits and item.name in run.data_inputs:
            table = item.filter_table
            if table is not None and table.extents is not None:
                extent_cols = {c for c in table.columns if _python_column_name(c) not in run.schema_keys}
                limit_inputs[item.name] = _surrogate(item, extents_for=extent_cols)
    shared = _compute_shared_limits(share_limits, limit_inputs, run.schema_keys)
    names = list(shared.keys())
    with_limits = dataclasses.replace(run, shared_limits=shared, limits_accepted=None)
    out = np.full((len(combos), len(names), 2), np.nan)
    for ci, combo in enumerate(combos):
        injected: dict = {}
        _inject_shared_limits(with_limits, injected, combo)
        for li, name in enumerate(names):
            pair = injected.get(f"{name}_limits")
            if pair is not None:
                out[ci, li, :] = pair
    Log.debug(f"compute_shared_limits: {names} (MATLAB: positional)", layer=LAYER)
    return names, out


def _progress_marks(run: RunPlan) -> "tuple[str, int, dict[int, tuple[int, Any]]]":
    """Where the outermost key's value changes (never the first), so MATLAB
    asks for a progress line only there."""
    reporter = ProgressReporter(run, LoopHooks(), RunTally())
    key = reporter.key
    if key is None:
        return "", 0, {}
    marks: "dict[int, tuple[int, Any]]" = {}
    last: Any = object()
    seen = 0
    for ci, combo in enumerate(run.combos):
        value = combo.get(key)
        if seen and value == last:
            continue
        last = value
        seen += 1
        if seen > 1:
            marks[ci] = (seen, value)
    return key, reporter.key_total, marks


def _dry_run_text(run: RunPlan) -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _announce(run)
        for combo in run.combos:
            _print_dry_run_iteration(run.inputs, combo, run.constant_inputs, run.distribute_key)
        print(f"[dry-run] would process {run.total} iterations")
    return buf.getvalue()


def refusal_id(exc: BaseException) -> str:
    """The MATLAB identifier for a call the planner refuses (the ones
    MATLAB code and tests catch): column groups, ColName, declared key
    types, else `scifor:for_each`."""
    if isinstance(exc, ColumnGroupsError):
        return "scifor:for_each:forColumnsGroups"
    if isinstance(exc, SchemaKeyTypeError):
        return "scifor:SchemaKeyTypeError"
    if str(exc).startswith("ColName("):
        return "scifor:ColName"
    return "scifor:for_each"


def _bound_items(desc: _Description) -> "tuple[_Input, ...]":
    """The inputs with every PathInput bound to the run's project root
    (``bind_path_inputs``, the one binder): the caller's ``project_root``,
    else the process-wide root, decided once here as Python's loop does."""
    root_text = str(desc.options.get("project_root") or "")
    root = Path(root_text).resolve() if root_text else current_project_root()
    bound = bind_path_inputs(
        {item.name: item.path_input for item in desc.inputs if item.path_input is not None}, root
    )
    return tuple(
        dataclasses.replace(item, path_input=bound[item.name]) if item.name in bound else item
        for item in desc.inputs
    )


def _as_table_option(raw: Any) -> "list[str] | bool | None":
    if isinstance(raw, (list, str)):
        return [str(a) for a in _listify(raw)]
    return bool(raw) if raw is not None else None


def _run_plan(desc: _Description, items: "tuple[_Input, ...]", prebuilt: "list | None") -> "tuple[RunPlan, list[int]]":
    """``plan_run`` on the surrogates, then ``locations=`` by index (so each
    kept combo keeps its place in scidb's row selection and pre-resolved
    paths; ``filter_combos`` would copy the dicts). Returns the plan and the
    kept indices."""
    options = desc.options
    as_table = _as_table_option(options.get("as_table"))
    where_text = options.get("where")
    locations = locations_from_text(options.get("locations") or None)
    run = plan_run(
        _MatlabFunction(desc.fn_name),
        {item.name: _surrogate(item) for item in items},
        matlab_output_names(desc.outputs, desc.fn_nargout, desc.call_nargout),
        copy.deepcopy(desc.metadata),
        hooks=LoopHooks(all_combos=prebuilt),
        key_types=validate_key_types(options.get("schema_key_types") or None),
        dry_run=bool(options.get("dry_run")),
        as_table=as_table,
        distribute=bool(options.get("distribute")),
        where=_MatlabWhere(str(where_text)) if where_text else None,
        locations=None,
        schema_keys=desc.schema_keys,
        on_error=str(options.get("on_error") or "continue"),
    )
    kept = filter_combo_indices(run.combos, locations, context=f"for_each({desc.fn_name})")
    run = dataclasses.replace(
        run,
        combos=[run.combos[i] for i in kept],
        option_parts=_option_parts(
            run.dry_run, bool(options.get("distribute")), as_table, run.where, locations
        ),
    )
    # The iterate columns must exist in every combo's table. A MATLAB
    # table is the same at every combo, so Python's per-combo check runs once
    # here; a missing column refuses the call (scifor:for_each).
    if run.for_columns is not None:
        _check_column_drift(
            run,
            {name: _resolve_data_spec(run.data_inputs[name], {})[0] for name in run.for_columns.params},
            "every combination (MATLAB tables are the same at each)",
        )
    return run, kept


def _register(run: RunPlan, labels: "list[str]") -> "tuple[int, dict]":
    """Keep the plan for assemble/close; returns its token and the progress
    schedule MATLAB follows (marks are 0-based combo indices)."""
    token = next(_TOKENS)
    key, progress_total, marks = _progress_marks(run)
    _PLANS[token] = _PlanState(
        run=run, labels=labels, progress_key=key, marks=marks,
        progress_total=progress_total, started=time.time(),
    )
    start_delay, min_interval = progress_guards()
    return token, {
        "key": key,
        "marks": sorted(marks),
        "start_delay": start_delay,
        "min_interval": min_interval,
    }


def plan(
    description_json: str,
    numbers: bytes = b"",
    *,
    prebuilt_combos: "list | None" = None,
    row_selection: "list | None" = None,
    resolved_path_outputs: "dict | None" = None,
    path_inputs: "dict | None" = None,
) -> "dict[str, str | bytes]":
    """Plan one MATLAB ``scifor.for_each`` run (see the module docstring)."""
    t0 = time.perf_counter()
    desc = _decode(description_json, numbers, path_inputs)
    prebuilt = list(prebuilt_combos) if prebuilt_combos is not None else None
    Log.info(
        f"[matlab-plan] received {len(desc.inputs)} input(s), {len(numbers or b'')} numeric "
        f"byte(s), {len(description_json)} JSON char(s), prebuilt combos="
        f"{'none' if prebuilt is None else len(prebuilt)}, decode {time.perf_counter() - t0:.3f}s",
        layer=LAYER,
    )
    items = _bound_items(desc)
    try:
        run, kept = _run_plan(desc, items, prebuilt)
    except MatlabWireError:
        raise
    except (ValueError, TypeError) as e:
        # A call that cannot run at all: MATLAB raises it with this
        # identifier, inside the user's for_each call.
        error_id = refusal_id(e)
        Log.info(f"[matlab-plan] refused: {error_id}: {e}", layer=LAYER)
        return {"json": json.dumps({"error_id": error_id, "error_message": str(e)})}
    _warn_positional_arity(
        desc.fn_name, [i.name for i in items],
        len(desc.options.get("share_limits") or {}), desc.fn_nargin,
    )
    if run.dry_run:
        Log.info(f"[matlab-plan] dry run: {run.total} combo(s)", layer=LAYER)
        return {"json": json.dumps({"dry_run": True, "dry_run_text": _dry_run_text(run)})}
    _announce(run)
    sources = kept if prebuilt is not None else None
    wire, buffers = _plan_wire(desc, items, run, sources, row_selection, resolved_path_outputs)
    token, progress = _register(run, wire["labels"])
    wire.update(token=token, dry_run=False, progress=progress)
    text = json.dumps(wire)
    Log.info(
        f"[matlab-plan] planned {run.total} combo(s) x {len(wire['keys'])} key(s), "
        f"{wire['n_slices']} slice(s), {wire['n_row_indices']} row index(es), "
        f"{len(text)} JSON char(s) + {sum(len(b) for b in buffers.values())} buffer byte(s) "
        f"out, token {token}, in {time.perf_counter() - t0:.3f}s",
        layer=LAYER,
    )
    return {"json": text, **buffers}


def _plan_wire(desc: _Description, items: "tuple[_Input, ...]", run: RunPlan,
               sources: "list[int] | None", row_selection: "list | None",
               resolved_path_outputs: "dict | None") -> "tuple[dict, dict[str, bytes]]":
    combos = run.combos
    # The effective keys (RunPlan.filter_keys owns the rule, for both
    # languages): which columns filter a table and which extraction drops.
    filter_keys = run.filter_keys
    keys, pools, index = _combo_wire(combos)
    slicer = _Slicer(len(combos))
    deliveries = []
    for item in items:
        if item.name in run.data_inputs:
            deliveries.append(_delivery(item, run.data_inputs[item.name], run, filter_keys, slicer,
                                        combos, row_selection, sources, desc.options))
        else:
            deliveries.append(_constant_delivery(item, run, combos, resolved_path_outputs,
                                                 sources, desc.options))
    row_index, row_offsets, n_rows = slicer.buffers()
    limit_names, limits = _limits(desc, run, combos)
    type_keys = list(dict.fromkeys(
        [*filter_keys, *run.keys] + ([run.distribute_key] if run.distribute_key else [])
    ))
    fc = run.for_columns
    wire = {
        "fn_name": run.fn_name,
        "n_combos": run.total,
        "output_names": run.output_names,
        "keys": keys,
        "pools": [{"v": pool} for pool in pools],
        "labels": [_combo_label(c) for c in combos],
        "filter_keys": filter_keys,
        "schema_keys": run.schema_keys,
        "inputs": deliveries,
        "n_slices": len(slicer.rows),
        "n_row_indices": n_rows,
        "for_columns": None if fc is None else {
            "params": fc.params,
            "columns": fc.columns,
            "mode": "split" if fc.mode == SPLIT else "combine",
            "groups": [{"name": name, "columns": cols} for name, cols in fc.groups],
        },
        "distribute_key": run.distribute_key or "",
        "distribute_top": bool(run.distribute_key)
        and not iterated_schema_keys(run.schema_keys, run.metadata_iterables),
        "column_types": _column_types(items, type_keys),
        "fatal_ids": list(FATAL_IDS),
        # scifor.outputs owns the empty-table rule; MATLAB applies it with
        # this detail so both loops report and group it identically.
        "empty_table_detail": EMPTY_TABLE_DETAIL,
        "limits": {
            "names": limit_names,
            "inject": _limits_fit(desc.fn_nargin, len(items), len(limit_names)),
        },
    }
    buffers = {
        "combo_index": index.tobytes(order="F"),
        "row_index": row_index,
        "row_offsets": row_offsets,
        "limits": np.ascontiguousarray(limits).astype("<f8").tobytes(order="F"),
    }
    return wire, buffers


# ---------------------------------------------------------------------------
# During the loop
# ---------------------------------------------------------------------------


def log_progress(token: int, combo_index: int, completed: int, failed: int,
                 no_data: int, elapsed: float) -> None:
    """Log the progress line at a mark (``combo_index``: 0-based, the combos
    done before it). MATLAB calls this only at a plan mark, once its time
    guards (from the plan) pass."""
    state = _PLANS.get(int(token))
    if state is None:
        Log.debug(f"[matlab-progress] unknown token {token}", layer=LAYER)
        return
    mark = state.marks.get(int(combo_index))
    if mark is None:
        Log.debug(f"[matlab-progress] combo {combo_index} is not a mark", layer=LAYER)
        return
    seen, value = mark
    Log.info(
        progress_line(
            state.progress_key,
            value, seen=seen, key_total=state.progress_total, combo_idx=int(combo_index),
            total=state.run.total, completed=int(completed), failed=int(failed),
            no_data=int(no_data), elapsed=float(elapsed),
        ),
        layer=LAYER,
    )


# ---------------------------------------------------------------------------
# Assemble and close
# ---------------------------------------------------------------------------


class MatlabError(Exception):
    """An MException as the Python summary sees it (its identifier)."""


def _failure_groups(raw: list, labels: "list[str]") -> "dict[str, FailureGroup]":
    groups: "dict[str, FailureGroup]" = {}
    for entry in raw:
        if not isinstance(entry, dict) or not isinstance(entry.get("record"), dict):
            raise MatlabWireError(f"failure entry must be {{record, combos}}: {str(entry)[:200]}")
        record = entry["record"]
        (normalized,) = groups_from_wire([{**record, "reason": record.get("reason", ""), "example": record}])
        failure = ComboFailure.from_dict(normalized["example"])
        combos = [labels[int(i) - 1] for i in _listify(entry.get("combos")) if 0 < int(i) <= len(labels)]
        group = groups.get(failure.reason)
        if group is None:
            groups[failure.reason] = FailureGroup(example=failure, combos=combos)
        else:
            group.combos.extend(combos)
    return groups


def _output_skips(raw: list, labels: "list[str]") -> "dict[str, list[tuple[str, NoOutput]]]":
    out: "dict[str, list[tuple[str, NoOutput]]]" = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        idx = int(entry.get("combo") or 0)
        label = labels[idx - 1] if 0 < idx <= len(labels) else ""
        reason = str(entry.get("reason") or "no_data")
        out.setdefault(str(entry.get("output") or ""), []).append(
            (label, NoOutput(reason if reason in ("no_data", "failed", "skipped") else "failed",
                             str(entry.get("detail") or "")))
        )
    return out


def _empty_returns(raw: list, labels: "list[str]") -> "dict[str, list[str]]":
    """MATLAB's report of outputs returned as empty tables -> the tally's
    ``empty_returns`` (output -> [combo label]), which _log_summary WARNs."""
    out: "dict[str, list[str]]" = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        idx = int(entry.get("combo") or 0)
        label = labels[idx - 1] if 0 < idx <= len(labels) else ""
        out.setdefault(str(entry.get("output") or ""), []).append(label)
    return out


def assemble(token: int, outcome_json: str) -> str:
    """The end-of-run summary of a MATLAB run, from what MATLAB reports:
    ``{completed, skipped, elapsed, failures: [{record, combos}], stop:
    {group} | null, output_skips: [{output, combo, reason, detail}],
    empty_returns: [{output, combo}],
    group_columns: [{name, columns}], no_data_groups: [{name, combos}],
    distribute_from_column}`` (combos are 1-based plan indices). Logs the
    summary (``_log_summary``, the same lines as Python's loop) and returns
    ``{summary, combine_columns}`` as JSON."""
    t0 = time.perf_counter()
    state = _PLANS.get(int(token))
    if state is None:
        raise MatlabWireError(f"assemble: unknown plan token {token}")
    outcome = json.loads(str(outcome_json))
    labels = state.labels
    groups = _failure_groups(_listify(outcome.get("failures")), labels)
    tally = RunTally(completed=int(outcome.get("completed") or 0),
                     skipped=int(outcome.get("skipped") or 0))
    tally.failure_groups = groups
    tally.output_skips = _output_skips(_listify(outcome.get("output_skips")), labels)
    tally.empty_returns = _empty_returns(_listify(outcome.get("empty_returns")), labels)
    for g in _listify(outcome.get("no_data_groups")):
        if isinstance(g, dict):
            tally.no_data_groups[str(g.get("name"))] = [
                labels[int(i) - 1] for i in _listify(g.get("combos")) if 0 < int(i) <= len(labels)
            ]
    for g in _listify(outcome.get("group_columns")):
        if isinstance(g, dict):
            tally.group_columns[str(g.get("name"))] = [str(c) for c in _listify(g.get("columns"))]
    stop = outcome.get("stop")
    stop_failure = None
    if isinstance(stop, dict):
        index = int(stop.get("group") or 0)
        examples = list(groups.values())
        if 0 < index <= len(examples):
            stop_failure = examples[index - 1].example
            tally.stop = (MatlabError(stop_failure.exc_type), stop_failure)
    n_from_column = int(outcome.get("distribute_from_column") or 0)
    if n_from_column and state.run.distribute_key:
        Log.info(
            f"distribute: {n_from_column} output(s) carried a '{state.run.distribute_key}' "
            f"column — its values address the pieces and the column is removed from the "
            f"data (row ordinals are not used)",
            layer=LAYER,
        )
    summary = _log_summary(state.run, tally, float(outcome.get("elapsed") or 0.0))
    if stop_failure is not None:
        Log.info(stop_line(state.run.fn_name, stop_failure, tally.completed), layer=LAYER)
    first_reason = next((g.reason for g in groups.values() if not g.no_data), "")
    state.summary = summary
    state.completed, state.skipped = tally.completed, tally.skipped
    fc = state.run.for_columns
    combine_columns = None
    if fc is not None and fc.mode != SPLIT and tally.completed:
        combine_columns = _combine_output_columns(fc.groups, tally.group_columns)
    wire = {
        "summary": {
            "fn_name": state.run.fn_name,
            "completed": summary["completed"],
            "failed": summary["failed"],
            "no_data": summary["no_data"],
            "total": summary["total"],
            "first_reason": first_reason,
            "failures": summary["failures"],
            "stopped": summary["stopped"],
        },
        "combine_columns": combine_columns,
    }
    Log.info(
        f"[matlab-assemble] {len(groups)} failure reason(s), "
        f"{sum(len(v) for v in tally.output_skips.values())} output skip(s), "
        f"in {time.perf_counter() - t0:.3f}s",
        layer=LAYER,
    )
    return json.dumps(wire)


def close(token: int, error_json: str = "") -> str:
    """Write the run's journal block (``error_json``: the failure record of
    the error that ended the run, or empty) and free the plan. Idempotent."""
    state = _PLANS.pop(int(token), None)
    if state is None:
        Log.debug(f"[matlab-close] token {token} already closed", layer=LAYER)
        return ""
    if state.summary is None:
        Log.debug(f"[matlab-close] token {token}: no summary, journal skipped", layer=LAYER)
        return ""
    error = None
    if error_json:
        raw = json.loads(str(error_json))
        if isinstance(raw, dict):
            (normalized,) = groups_from_wire([{**raw, "reason": raw.get("reason", ""), "example": raw}])
            error = ComboFailure.from_dict(normalized["example"]).to_dict()
    event = {
        "kind": "run",
        "name": state.run.fn_name,
        "language": "matlab",
        "started": state.started,
        "elapsed": time.time() - state.started,
        "completed": state.summary["completed"],
        "failed": state.summary["failed"],
        "no_data": state.summary["no_data"],
        "total": state.summary["total"],
        "stopped": state.summary["stopped"],
        "cancelled": False,
        "failures": state.summary["failures"],
        "error": error,
    }
    written = Log.journal(event)
    Log.info(f"[matlab-close] token {token}: journal block written", layer=LAYER)
    return str(written or "")


def discard(token: int) -> None:
    """Free a plan without a journal block (MATLAB's cleanup when the run
    ended before its summary)."""
    if _PLANS.pop(int(token), None) is not None:
        Log.debug(f"[matlab-close] token {token} discarded", layer=LAYER)


def open_plans() -> "list[int]":
    """Tokens of plans not yet closed (tests and leak diagnostics)."""
    return sorted(_PLANS)


__all__ = [
    "MatlabWireError",
    "assemble",
    "close",
    "discard",
    "log_progress",
    "matlab_number",
    "matlab_output_names",
    "matlab_text",
    "open_plans",
    "plan",
    "refusal_id",
]
