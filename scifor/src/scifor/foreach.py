"""Pure for_each loop — works with DataFrames, no DuckDB/database knowledge.

Filesystem access via PathInput/PathOutput is fine and by design; the
boundary this module holds is "no DuckDB knowledge," not "no I/O."

Logging: emits through the scistacklog facade with ``layer="scifor"``.
INFO carries the run narrative (banner, periodic progress, end-of-run
summary with failure reasons); DEBUG carries per-iteration detail
([run]/[skip]/[done] lines, internal flow). Dry-run output goes to stdout
via print() — it is the requested result, not logging.

Shape: ``for_each`` = ``plan_run`` (a frozen ``RunPlan``) -> ``run_combo``
per combination (a ``ComboResult``; failures are returned, not raised) ->
``assemble`` (summary + result table). Hooks from a layer above arrive as one
``LoopHooks`` (``scifor.hooks``).
"""

import copy
import sys
import threading
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scistacklog import Log

from .distribute import (
    DistributeError,
    MissingSchemaKeyError,
    group_ordinals,
    multi_row_groups,
    resolve_target,
)

if TYPE_CHECKING:
    import pandas as pd

from .colname import ColName
from .column_selection import (
    SPLIT,
    ColumnGroupsError,
    ColumnSelection,
    column_value,
    describe_column_groups,
    overlapping_columns,
    resolve_column_groups,
)
from .missing import frame_has_no_data
from .each_of import EachOf, require_alternatives
from .failures import (
    ComboFailure,
    FailureGroup,
    StoppedRun,
    UserValueError,
    attach_stopped_run,
    call_user_function,
    describe_value_kinds,
    exception_reason,
    failure_from_exception,
    format_user_frame,
    groups_to_wire,
    journaled,
    stopped_run_of,
    user_value_seam,
    validate_on_error,
)
from .fixed import Fixed
from .hooks import LoopHooks
from .locations import LocationFilter, filter_combos
from .keys import series_mask, warn_if_type_mismatch
from .merge import Merge, drop_constant_keys, merge_frames
from .outputs import (
    NoOutput,
    discard_db_only_kwargs,
    empty_tables_to_no_output,
    is_no_output,
    output_column_name,
)
from .pathinput import PathInput
from .pathoutput import PathOutput
from .project.root import project_root as current_project_root
from .schema import expand_schema_keys, get_schema
from .schema_types import (
    canonicalize_combos,
    canonicalize_values,
    numeric_match_keys,
    validate_key_types,
)
from .schema_types import describe as describe_key_types

# Periodic progress guards (module-level so tests can monkeypatch).
# A progress line is emitted when the outermost iterated key's value changes,
# but never at the very first combo, never within the first
# _PROGRESS_START_DELAY_S of the loop (fast runs emit none), and never more
# often than _PROGRESS_MIN_INTERVAL_S (degenerate shapes: huge or only key).
_PROGRESS_MIN_INTERVAL_S = 2.0
_PROGRESS_START_DELAY_S = 5.0
# Value-list preview length in the banner, and combos listed per failure
# reason in the end-of-run summary.
_VALUES_PREVIEW_MAX = 4
_SUMMARY_COMBOS_MAX = 5


def _format_value_list(values) -> str:
    """``12 values [1, 2, 3, …, 12]`` — truncated preview of an iterable."""
    n = len(values)
    shown = [repr(v) for v in values]
    if n > _VALUES_PREVIEW_MAX:
        shown = shown[: _VALUES_PREVIEW_MAX - 1] + ["…", repr(values[-1])]
    return f"{n} value{'s' if n != 1 else ''} [{', '.join(shown)}]"


def _record_iteration_failure(
    groups: "dict[str, FailureGroup]",
    exc: Exception,
    *,
    fn_name: str,
    metadata: dict,
    metadata_str: str,
    stage: str,
    context: str,
) -> ComboFailure:
    """Track a per-iteration failure for the end-of-run summary.

    The first occurrence of each distinct reason builds the full
    ``ComboFailure`` (traceback + user frame — ``scifor.failures`` owns that)
    and logs it at WARN with the traceback, so the default (INFO) log still
    answers "what failed and why". Later occurrences only add their combo to
    the group and return the group's example (no traceback extraction per
    combo). NoDataError is an expected outcome (this combo has no backing
    data), so it never escalates to WARN and carries no traceback.
    """
    reason = exception_reason(exc)
    Log.debug(f"[skip] {metadata_str}: {context}: {exc}", layer="scifor")
    group = groups.get(reason)
    if group is not None:
        group.combos.append(metadata_str)
        return group.example
    no_data = isinstance(exc, NoDataError)
    failure = failure_from_exception(
        exc, fn_name=fn_name, metadata=metadata, stage=stage, no_data=no_data
    )
    groups[reason] = FailureGroup(example=failure, combos=[metadata_str])
    if no_data:
        return failure
    Log.warn(
        f"iteration failed: {metadata_str} — {context}: {exc} "
        f"[{format_user_frame(failure.user_frame)}] "
        f"(first occurrence; traceback follows)",
        layer="scifor",
        exc_info=True,
    )
    return failure


def _no_data_count(groups: "dict[str, FailureGroup]") -> int:
    return sum(g.count for g in groups.values() if g.no_data)


def _skip_failure_payload(failure: ComboFailure) -> dict:
    """The small per-combo form a ``combo_skip`` event carries (no traceback;
    the summary's groups carry one example traceback per reason)."""
    return {
        "reason": failure.reason,
        "no_data": failure.no_data,
        "user_frame": failure.user_frame.to_dict() if failure.user_frame else None,
    }


@dataclass(frozen=True)
class ForColumnsPlan:
    """The column axis of a for_columns run (``column_selection`` owns the
    grouping rules; this is their result for one run)."""

    #: Inputs iterated column-wise (zipped by column name / group).
    params: "list[str]"
    #: Every source column the iterate inputs carry.
    columns: "list[str]"
    #: ``[(group name, [source columns])]`` the loop runs over.
    groups: "list[tuple[str, list[str]]]"
    #: ``COMBINE`` (one wide row per combo) or ``SPLIT`` (one output per group).
    mode: str


@dataclass(frozen=True)
class RunPlan:
    """Everything ``for_each`` decides before the first combination runs.

    Built once by :func:`plan_run`; read by :func:`run_combo` and
    :func:`assemble`. Nothing in it changes while the loop runs.
    """

    fn: Callable
    fn_name: str
    #: The call's inputs after PathInput binding and ColName resolution.
    inputs: "dict[str, Any]"
    #: Result column names, one per output slot.
    output_names: "list[str]"
    #: Iterated keys in iteration order, and their (resolved) value lists.
    keys: "list[str]"
    metadata_iterables: "dict[str, Any]"
    #: The combinations to run, after ``locations=``.
    combos: "list[dict]"
    #: The dataset schema (``get_schema()``), read once at entry.
    schema_keys: "list[str]"
    #: Input schema-key column dtypes, restored on the output table.
    schema_col_dtypes: dict
    #: ``distribute=True``: the schema key the pieces land at.
    distribute_key: "str | None"
    data_inputs: "dict[str, Any]"
    constant_inputs: "dict[str, Any]"
    #: Data inputs that keep their schema-key columns (``as_table``).
    as_table_set: "set[str]"
    for_columns: "ForColumnsPlan | None"
    #: ``share_limits``: ``{input: (group keys, {group: (lo, hi)})}`` and the
    #: parameter names the function accepts them under (None: any).
    shared_limits: dict
    limits_accepted: "set[str] | None"
    #: Per-combo PathInput resolver (hooks', or the exact-match one a
    #: declared string key needs).
    path_input_resolver: "Callable[[PathInput, dict], Any] | None"
    where: Any
    on_error: str
    dry_run: bool
    #: Non-default options, for the run banner.
    option_parts: "tuple[str, ...]"

    @property
    def filter_keys(self) -> "list[str]":
        """The keys that filter a per-combo table and that extraction drops:
        the schema, or with no schema the iterated keys. One rule for both
        languages (MATLAB's plan wire sends this list); decided 2026-11-02,
        plan stage 3. Before, a Python run with no schema handed every combo
        the whole table."""
        return list(self.schema_keys) or list(self.keys)

    @property
    def total(self) -> int:
        return len(self.combos)

    @property
    def n_outputs(self) -> int:
        return len(self.output_names)

    @property
    def iterate_params(self) -> "list[str]":
        return self.for_columns.params if self.for_columns is not None else []

    @property
    def display_keys(self) -> "list[str]":
        """Iterated keys shown to the user (``__`` keys are bookkeeping)."""
        return [k for k in self.keys if not k.startswith("__")]


@dataclass
class RunTally:
    """What the loop has accumulated so far; :func:`assemble` reads it."""

    completed: int = 0
    skipped: int = 0
    cancelled: bool = False
    #: ``(metadata, result tuple)`` per result row, in loop order.
    rows: "list[tuple[dict, tuple]]" = field(default_factory=list)
    #: Failure aggregation for the end-of-run summary: reason -> group
    #: (``scifor.failures`` owns the record).
    failure_groups: "dict[str, FailureGroup]" = field(default_factory=dict)
    #: The (exception, failure) that ended an ``on_error="stop"`` run.
    stop: "tuple[Exception, ComboFailure] | None" = None
    #: Outputs skipped at a combination whose other outputs were kept
    #: (NoOutput slots): output label -> [(combo text, NoOutput)].
    output_skips: "dict[str, list[tuple[str, NoOutput]]]" = field(default_factory=dict)
    #: Output label -> [combo text] where the function returned an EMPTY
    #: table (scifor.outputs.is_empty_table). Summarised as one WARN per
    #: output; the slots themselves are NoOutput (or the combo is no data).
    empty_returns: "dict[str, list[str]]" = field(default_factory=dict)
    #: for_columns combine: group name -> [combo text] where it had no data.
    no_data_groups: "dict[str, list[str]]" = field(default_factory=dict)
    #: for_columns combine: group name -> output columns, first-seen order.
    group_columns: "dict[str, list[str]]" = field(default_factory=dict)
    #: distribute: outputs whose key columns have been reported, once per run:
    #: they change every saved address, so they must be in scistack.log.
    distribute_column_reported: "set[str]" = field(default_factory=set)
    #: distribute: output label -> its target. One output has one level.
    distribute_targets: "dict[str, str]" = field(default_factory=dict)

    @property
    def no_data(self) -> int:
        return _no_data_count(self.failure_groups)

    @property
    def failed(self) -> int:
        return self.skipped - self.no_data


@dataclass(frozen=True)
class ComboResult:
    """One combination's outcome.

    A failure is returned, not raised: the loop decides what it means
    (skip, or stop under ``on_error="stop"``). Only errors that would fail
    every combination identically (``ForColumnsError``, column drift, a
    ``scifor_fatal`` exception) propagate out of :func:`run_combo`.
    """

    rows: "list[tuple[dict, tuple]]" = field(default_factory=list)
    failure: "ComboFailure | None" = None
    #: The original exception behind ``failure``.
    error: "Exception | None" = None


_NO_VALUE = object()


class ProgressReporter:
    """Loop events for ``hooks.progress_fn`` and the periodic progress line.

    The progress line is emitted when the outermost iterated key's value
    changes, but never at the very first combo, never within the first
    ``_PROGRESS_START_DELAY_S`` of the loop (fast runs emit none), and never
    more often than ``_PROGRESS_MIN_INTERVAL_S``.
    """

    def __init__(self, plan: RunPlan, hooks: LoopHooks, tally: RunTally):
        self._plan = plan
        self._hooks = hooks
        self._tally = tally
        display_keys = plan.display_keys
        self._key = display_keys[0] if display_keys else None
        if self._key is not None:
            try:
                self._key_total = len(dict.fromkeys(c.get(self._key) for c in plan.combos))
            except TypeError:  # unhashable values — fall back to declared list
                self._key_total = len(plan.metadata_iterables.get(self._key, []))
        else:
            self._key_total = 0
        self._last_value = _NO_VALUE
        self._seen = 0
        self._last_emit = 0.0
        self._t0 = time.perf_counter()

    def elapsed(self) -> float:
        return time.perf_counter() - self._t0

    def event(self, name: str, combo_idx: int, **extra: Any) -> None:
        """Send one ``{event, current, total, completed, skipped, ...}`` dict."""
        if self._hooks.progress_fn is None:
            return
        self._hooks.emit(
            {
                "event": name,
                "current": combo_idx + 1,
                "total": self._plan.total,
                "completed": self._tally.completed,
                "skipped": self._tally.skipped,
                **extra,
            }
        )

    def observe(self, combo_idx: int, metadata: dict) -> None:
        """Log the periodic progress line if this combo starts a new
        outermost-key value and the guards allow it."""
        if self._key is None or self._plan.dry_run:
            return
        value = metadata.get(self._key, _NO_VALUE)
        if value == self._last_value:
            return
        self._last_value = value
        self._seen += 1
        now = self.elapsed()
        if not (
            self._seen > 1
            and now >= _PROGRESS_START_DELAY_S
            and now - self._last_emit >= _PROGRESS_MIN_INTERVAL_S
        ):
            return
        self._last_emit = now
        tally = self._tally
        Log.info(
            progress_line(
                self._key,
                value,
                seen=self._seen,
                key_total=self._key_total,
                combo_idx=combo_idx,
                total=self._plan.total,
                completed=tally.completed,
                failed=tally.failed,
                no_data=tally.no_data,
                elapsed=now,
            ),
            layer="scifor",
        )

    @property
    def key(self) -> "str | None":
        """The outermost displayed key, whose value changes start a line."""
        return self._key

    @property
    def key_total(self) -> int:
        """How many distinct values of :attr:`key` the combos hold."""
        return self._key_total


def progress_line(
    key: str,
    value: Any,
    *,
    seen: int,
    key_total: int,
    combo_idx: int,
    total: int,
    completed: int,
    failed: int,
    no_data: int,
    elapsed: float,
) -> str:
    """The periodic progress line's text (Python's loop and MATLAB's)."""
    return (
        f"progress: {key}={value} ({seen}/{key_total}) — "
        f"{combo_idx}/{total} combos ({100.0 * combo_idx / total:.1f}%), "
        f"completed={completed}, failed={failed}, no_data={no_data}, "
        f"elapsed={elapsed:.1f}s"
    )


def progress_guards() -> "tuple[float, float]":
    """``(start delay, min interval)`` in seconds for the progress line. Read
    at call time so a test's monkeypatch of the module constants applies."""
    return _PROGRESS_START_DELAY_S, _PROGRESS_MIN_INTERVAL_S


@journaled
def for_each(
    fn: Callable,
    inputs: dict[str, Any],
    outputs: list[str] | int | None = None,
    dry_run: bool = False,
    as_table: list[str] | bool | None = None,
    distribute: bool = False,
    where=None,
    locations: "LocationFilter | dict | None" = None,
    share_limits: "dict[str, list[str]] | None" = None,
    schema_keys: "list[str] | None" = None,
    schema_key_types: "dict[str, str] | None" = None,
    on_error: str = "continue",
    project_root: "str | Path | None" = None,
    hooks: "LoopHooks | None" = None,
    **metadata_iterables: list[Any],
) -> "pd.DataFrame | None":
    """
    Execute a function for all combinations of metadata, filtering
    DataFrame inputs per iteration.

    This is a pure loop orchestrator — no DuckDB/database knowledge.
    Filesystem access via PathInput/PathOutput inputs is fine and expected.
    All other inputs must be DataFrames or constants.

    Args:
        fn: The function to execute.
        inputs: Dict mapping parameter names to DataFrames, Fixed wrappers,
                Merge wrappers, ColumnSelection wrappers, or constant values.
        outputs: Names for result columns. list[str] names them;
                 int N auto-names (output_1, ..., output_N);
                 None defaults to ["output"] (single output). Same name
                 and position as scidb.for_each's ``outputs`` so the two
                 are drop-in replacements for one another.
        dry_run: If True, only print what would happen without executing.
        as_table: Controls which DataFrame inputs keep schema key columns.
                  True = all; list of names = selected; False/None = none.
        distribute: If True, split outputs by element/row and expand them
                    into the result table at the schema level below the
                    deepest iterated key. If no metadata_iterable is a
                    schema key (e.g. a fully static PathInput with no
                    {key} placeholders), distributes to the top of the
                    schema instead.
        where: Optional scifor.ColFilter/CompoundFilter to filter DataFrame
               rows after combo filtering.
        locations: Optional scifor.LocationFilter (or its mapping form) naming
               which schema locations to run: ragged ``include`` prefixes plus
               a standing ``exclude_levels`` rule. Filters COMBOS, where
               ``where=`` filters ROWS within a combo — the two are unrelated
               and compose. An empty/omitted filter is inert. See
               docs/claude/location-filter-semantics.md.
        share_limits: Optional ``{input_name: [schema_keys_to_hold_fixed]}``.
                      For each named input, computes the global numeric
                      ``(min, max)`` extent of that input's data within each
                      group defined by the held-fixed schema keys (spanning all
                      other iterated keys), and injects it as a
                      ``{input_name}_limits`` keyword argument on each call —
                      but only if the function's signature accepts that name
                      (or ``**kwargs``). Used so e.g. every per-trial plot within
                      a subject shares one y-axis range
                      (``share_limits={"signal": ["subject"]}``).
        schema_keys: Optional list of schema key names to iterate, structural
                    sugar for passing ``key=[]`` for each one by hand (each
                    still auto-resolves via a DataFrame scan of the inputs).
                    Mutually exclusive with explicit **metadata_iterables.
        schema_key_types: Optional ``{key: "numeric" | "string"}``
                    (``scifor.schema_types``). ``numeric``: every spelling of
                    one number is one value, from every source (explicit
                    values, discovered files). ``string``: spelling is kept
                    and a PathInput matches the key exactly (no
                    zero-padding). Keys not listed keep the automatic
                    behaviour: digit-only DISCOVERED values become numbers.
                    In scidb.for_each the database owns this instead; there
                    the argument records the type in the database.
        on_error: ``"continue"`` (default) skips a combo whose function or
                    input preparation raises, and reports it in the
                    end-of-run summary. ``"stop"`` stops the loop at the
                    first such failure and re-raises the ORIGINAL exception
                    (so the traceback ends in the user's code), carrying a
                    ``scifor.StoppedRun`` with the completed combos' results
                    (``scifor.stopped_run_of(exc)``). ``NoDataError`` (a combo
                    with no data) never stops a run. See
                    docs/claude/error-surfacing.md.
        project_root: The folder a PathInput without ``root_folder``
                    resolves its relative template against. Default: the
                    process-wide root (``scifor.project.root.project_root``:
                    the pinned root, else the cwd), read once here.
        hooks: Optional :class:`scifor.LoopHooks`: how a layer above (scidb)
                    drives the loop — pre-built combos, row selection,
                    progress events, cancellation. Standalone use never needs
                    it; each field is documented on the class.
        **metadata_iterables: Iterables of metadata values to combine.

    Returns:
        A pandas DataFrame of results, or None when dry_run=True.

    The run is three phases: :func:`plan_run` decides everything up front
    (a :class:`RunPlan`), :func:`run_combo` runs one combination, and
    :func:`assemble` turns the collected rows into the result table.
    """
    hooks = hooks if hooks is not None else LoopHooks()
    _reject_private_kwargs(metadata_iterables)
    # scidb-only kwargs (save=, db=, ...) arrive in **metadata_iterables
    # here. Set them aside before anything reads metadata_iterables as
    # iteration axes, so a call written for scidb.for_each runs unchanged.
    metadata_iterables = discard_db_only_kwargs(metadata_iterables)
    key_types = validate_key_types(schema_key_types)
    validate_on_error(on_error)
    # The one place a rootless PathInput learns where it resolves: the
    # caller's root, else the process-wide one, decided once at entry and
    # bound into the inputs (PathInput.bound_to) -- not read again per combo.
    root = Path(project_root).resolve() if project_root is not None else current_project_root()
    inputs = bind_path_inputs(inputs, root)

    # EachOf expansion comes before any other logic: each alternative is an
    # independent recursive for_each() call.
    each_of_axes = _each_of_axes(inputs, where)
    if each_of_axes:
        return _for_each_alternatives(
            fn,
            inputs,
            where,
            each_of_axes,
            hooks,
            dict(
                outputs=outputs,
                dry_run=dry_run,
                as_table=as_table,
                distribute=distribute,
                locations=locations,
                share_limits=share_limits,
                schema_keys=schema_keys,
                schema_key_types=schema_key_types,
                on_error=on_error,
                project_root=root,
                **metadata_iterables,
            ),
        )

    plan = plan_run(
        fn,
        inputs,
        outputs,
        metadata_iterables,
        hooks=hooks,
        key_types=key_types,
        dry_run=dry_run,
        as_table=as_table,
        distribute=distribute,
        where=where,
        locations=locations,
        share_limits=share_limits,
        schema_keys=schema_keys,
        on_error=on_error,
    )
    _announce(plan)
    tally = RunTally()
    progress = ProgressReporter(plan, hooks, tally)
    for combo_idx, metadata in enumerate(plan.combos):
        # Cooperative cancel: check between combos (before any work for this combo).
        if hooks.cancelled():
            tally.cancelled = True
            Log.info(
                f"for_each({plan.fn_name}) cancelled at combo {combo_idx + 1}/{plan.total} "
                f"(completed={tally.completed}, failed={tally.failed}, no_data={tally.no_data})",
                layer="scifor",
            )
            progress.event("cancelled", combo_idx)
            break
        progress.observe(combo_idx, metadata)
        progress.event("combo_start", combo_idx, metadata=metadata)
        if plan.dry_run:
            _print_dry_run_iteration(
                plan.inputs, metadata, plan.constant_inputs, plan.distribute_key
            )
            tally.completed += 1
            continue
        outcome = run_combo(plan, metadata, hooks, tally)
        if outcome.failure is not None:
            tally.skipped += 1
            progress.event(
                "combo_skip",
                combo_idx,
                metadata=metadata,
                failure=_skip_failure_payload(outcome.failure),
            )
            if (
                plan.on_error == "stop"
                and not outcome.failure.no_data
                and outcome.error is not None
            ):
                tally.stop = (outcome.error, outcome.failure)
                break
            continue
        tally.rows.extend(outcome.rows)
        tally.completed += 1
        progress.event("combo_done", combo_idx, metadata=metadata)
    return assemble(plan, tally, hooks, elapsed=progress.elapsed())


def _reject_private_kwargs(metadata_iterables: dict) -> None:
    """A ``_name=`` keyword is never an iteration axis.

    Loop hooks travel in ``hooks=LoopHooks(...)``. A leftover ``_progress_fn=``
    (or any other single-underscore name) would otherwise become a silent
    bogus metadata axis — the failure mode ``schema_level=`` had in the GUI.
    ``__`` keys are left alone: they are bookkeeping axes some callers use.
    """
    private = sorted(
        k for k in metadata_iterables if k.startswith("_") and not k.startswith("__")
    )
    if private:
        raise TypeError(
            f"for_each() got unexpected keyword argument(s) {private}. Schema keys "
            f"cannot start with '_'; loop hooks are passed as hooks=LoopHooks(...)."
        )


def _each_of_axes(inputs: dict, where: Any) -> "list[tuple[str, str | None, list]]":
    """``[(kind, param, alternatives)]`` for every EachOf in the call."""
    axes: "list[tuple[str, str | None, list]]" = []
    for param, val in inputs.items():
        if isinstance(val, EachOf):
            require_alternatives(val, kind="input", param=param)
            axes.append(("input", param, val.alternatives))
    if isinstance(where, EachOf):
        require_alternatives(where, kind="where")
        axes.append(("where", None, where.alternatives))
    return axes


def _for_each_alternatives(
    fn: Callable,
    inputs: dict,
    where: Any,
    each_of_axes: "list[tuple[str, str | None, list]]",
    hooks: LoopHooks,
    call_kwargs: dict,
) -> "pd.DataFrame | None":
    """Run one ``for_each`` per EachOf alternative and concatenate the results.

    Mirrors scidb.for_each's own EachOf expansion (which additionally threads
    save/db/lineage per alternative — concepts this pure layer doesn't have),
    so a standalone/no-DB pipeline can use EachOf too. Every alternative gets
    the same ``call_kwargs`` and ``hooks``: a selection (``locations=``, the
    pre-built combos) is about which data exists, not which variant produced
    it. (Dropping a kwarg in this recursion is a silent, well-worn failure —
    `glue=` went missing from it in 2026-09-10; one dict now carries them.)
    """
    import pandas as pd

    results = []
    for combo in product(*(axis[2] for axis in each_of_axes)):
        concrete_inputs = dict(inputs)
        concrete_where = where
        for (kind, param, _alts), value in zip(each_of_axes, combo, strict=False):
            if kind == "input":
                concrete_inputs[param] = value
            elif kind == "where":
                concrete_where = value
        try:
            result = for_each(
                fn, concrete_inputs, where=concrete_where, hooks=hooks, **call_kwargs
            )
        except Exception as e:
            stopped = stopped_run_of(e)
            if stopped is None or not results:
                raise
            # on_error="stop" in a later alternative: the earlier
            # alternatives' results are part of the partial result too.
            parts = [*results]
            if stopped.partial_result is not None:
                parts.append(stopped.partial_result)
            attach_stopped_run(
                e,
                StoppedRun(
                    failure=stopped.failure,
                    partial_result=pd.concat(parts, ignore_index=True),
                    completed=stopped.completed + sum(len(r) for r in results),
                ),
                note=False,
            )
            raise
        if result is not None:
            results.append(result)
        # Cooperative cancel: stop iterating across EachOf alternatives
        # as soon as the user cancels — don't start the next concrete run.
        if hooks.cancelled():
            break
    return pd.concat(results, ignore_index=True) if results else None


# ---------------------------------------------------------------------------
# Phase 1: plan
# ---------------------------------------------------------------------------


def plan_run(
    fn: Callable,
    inputs: "dict[str, Any]",
    outputs: "list[str] | int | None",
    metadata_iterables: "dict[str, Any]",
    *,
    hooks: LoopHooks,
    key_types: dict,
    dry_run: bool = False,
    as_table: "list[str] | bool | None" = None,
    distribute: bool = False,
    where: Any = None,
    locations: "LocationFilter | dict | None" = None,
    share_limits: "dict[str, list[str]] | None" = None,
    schema_keys: "list[str] | None" = None,
    on_error: str = "continue",
) -> RunPlan:
    """Decide everything about a run before its first combination.

    ``inputs`` are already bound to the project root, ``metadata_iterables``
    already stripped of scidb-only kwargs, ``key_types`` validated, and EachOf
    already expanded (``for_each``'s entry does all four). Raises for a call
    that cannot run at all (a distribute with no level below, a deferred
    ColName with no for_columns input, mismatched for_columns inputs).
    """
    full_schema_keys = get_schema()
    inputs, bare_colname_params = _instantiate_bare_colnames(inputs)
    output_names = _output_names(outputs)

    # schema_keys= structural sugar — seed metadata_iterables with an empty
    # list per requested key, before the empty-list resolver runs. Shared
    # with scidb.for_each() via expand_schema_keys().
    if schema_keys is not None:
        metadata_iterables = expand_schema_keys(schema_keys, metadata_iterables)
        Log.debug(
            "expand_schema_keys: seeded metadata_iterables for %s",
            schema_keys,
            layer="scifor",
        )
    else:
        metadata_iterables = dict(metadata_iterables)

    # Standalone = no caller pre-built the combos. scidb always does, after
    # applying the DATABASE's key types itself, so declared key types only
    # ever govern a standalone loop.
    prebuilt = hooks.all_combos
    path_input_resolver = hooks.path_input_resolver
    if prebuilt is None:
        metadata_iterables, prebuilt = _resolve_empty_iterables(
            inputs, metadata_iterables, key_types
        )
        if key_types:
            prebuilt, path_input_resolver = _apply_declared_key_types(
                metadata_iterables, prebuilt, key_types, path_input_resolver
            )

    distribute_key = (
        _distribute_target(full_schema_keys, metadata_iterables) if distribute else None
    )

    # Capture input schema-key column dtypes for output round-trip: output
    # metadata columns must come back as EXACTLY the input column's dtype
    # (int stays int, object/str stays object with values verbatim,
    # categorical stays categorical with its categories and orderedness).
    # Restored in _results_to_output_dataframe; mirrors the MATLAB scifor
    # loop's capture_schema_column_types/restore_schema_column_types.
    type_keys = list(
        dict.fromkeys(
            [*full_schema_keys, *metadata_iterables.keys()]
            + ([distribute_key] if distribute_key is not None else [])
        )
    )
    schema_col_dtypes = _capture_schema_column_dtypes(inputs, type_keys)

    inputs = _resolve_static_colnames(inputs, full_schema_keys)
    data_inputs, constant_inputs = _classify_inputs(inputs)
    if distribute_key is not None and distribute_key in constant_inputs:
        raise ValueError(
            f"distribute target '{distribute_key}' conflicts with a constant input named '{distribute_key}'."
        )
    as_table_set = _as_table_set(as_table, data_inputs)
    for_columns = _plan_for_columns(
        data_inputs,
        constant_inputs,
        full_schema_keys,
        output_names,
        bare_colname_params=bare_colname_params,
        distribute_key=distribute_key,
    )

    fn_name = getattr(fn, "__name__", repr(fn))
    keys = list(metadata_iterables.keys())
    combos = _build_combos(keys, metadata_iterables, prebuilt)
    # locations= narrows the combo list to the selected schema locations.
    # Applied to pre-built combos too, on purpose: a filter that only touched
    # the Cartesian branch would be silently ignored on every database-driven
    # run, which is the path that has one (scidb hands its pre-filtered
    # combos in through hooks.all_combos).
    if locations is not None:
        combos = filter_combos(combos, locations, context=f"for_each({fn_name})")

    shared_limits, limits_accepted = _plan_shared_limits(
        fn, share_limits, data_inputs, full_schema_keys
    )

    return RunPlan(
        fn=fn,
        fn_name=fn_name,
        inputs=inputs,
        output_names=output_names,
        keys=keys,
        metadata_iterables=metadata_iterables,
        combos=combos,
        schema_keys=list(full_schema_keys),
        schema_col_dtypes=schema_col_dtypes,
        distribute_key=distribute_key,
        data_inputs=data_inputs,
        constant_inputs=constant_inputs,
        as_table_set=as_table_set,
        for_columns=for_columns,
        shared_limits=shared_limits,
        limits_accepted=limits_accepted,
        path_input_resolver=path_input_resolver,
        where=where,
        on_error=on_error,
        dry_run=dry_run,
        option_parts=_option_parts(dry_run, distribute, as_table, where, locations),
    )


def _instantiate_bare_colnames(inputs: dict) -> "tuple[dict, list[str]]":
    """Forgive a bare ColName class passed without parentheses.

    `scifor.ColName` (uninstantiated) can only mean the no-arg deferred form,
    since there is no DataFrame to attach. Normalize it to ColName() so all
    downstream isinstance(v, ColName) checks treat it uniformly. Returns the
    names that arrived this way too, for a clearer error if they turn out to
    lack a for_columns input to resolve against.
    """
    bare = [name for name, v in inputs.items() if isinstance(v, type) and issubclass(v, ColName)]
    if not bare:
        return inputs, bare
    return {
        name: (v() if (isinstance(v, type) and issubclass(v, ColName)) else v)
        for name, v in inputs.items()
    }, bare


def _output_names(outputs: "list[str] | int | None") -> "list[str]":
    """Resolve ``outputs=`` into result column names."""
    if outputs is None:
        names = ["output"]
    elif isinstance(outputs, int):
        names = [f"output_{i + 1}" for i in range(outputs)]
    else:
        names = [output_column_name(o) for o in outputs]
    Log.debug(
        "resolve_outputs: %d output name(s): %s (from %r)",
        len(names),
        names,
        outputs,
        layer="scifor",
    )
    return names


def _resolve_empty_iterables(
    inputs: dict, metadata_iterables: dict, key_types: dict
) -> "tuple[dict, list[dict] | None]":
    """Resolve empty lists ``[]`` in standalone mode.

    Pass 1 scans DataFrame inputs (non-raising); pass 2 fills whatever's
    still empty from PathInput filesystem discovery. Keys the user passed
    with explicit (non-empty) values assert intent and are never
    overwritten. Returns ``(metadata_iterables, combos)``: combos are the
    discovered ones when discovery drives the iteration directly, else None.
    Mutates and returns the given dict (discovery may return a new one).
    """
    combos: "list[dict] | None" = None
    user_explicit_keys = {
        k
        for k, v in metadata_iterables.items()
        if not (isinstance(v, list) and len(v) == 0)
    }
    needs_resolve = [
        k for k, v in metadata_iterables.items() if isinstance(v, list) and len(v) == 0
    ]
    if needs_resolve:
        Log.debug(
            "resolve_empty_lists: scanning DataFrame inputs for %s",
            needs_resolve,
            layer="scifor",
        )
        for key in needs_resolve:
            values = _distinct_values_from_inputs(inputs, key)
            if values:
                Log.debug(
                    "resolved '%s' to %d values: %s",
                    key,
                    len(values),
                    values,
                    layer="scifor",
                )
            metadata_iterables[key] = values

    # PathInput filesystem discovery for keys DataFrames didn't resolve. The
    # Case A/B decision (and whether discovered combos drive iteration
    # directly) is owned by PathInput.apply_discovery so scifor and scidb
    # share one implementation.
    pi = _find_pathinput(inputs)
    if pi is not None and (
        not metadata_iterables
        or any(isinstance(v, list) and len(v) == 0 for v in metadata_iterables.values())
    ):
        metadata_iterables, discovered_combos = resolve_pathinput_discovery(
            pi,
            metadata_iterables,
            user_explicit_keys,
            log=lambda msg: Log.debug(msg, layer="scifor"),
            condense_numeric=True,
            key_types=key_types,
        )
        if discovered_combos is not None:
            combos = discovered_combos
            Log.debug(
                "pathinput_discovery: using %d disk combos",
                len(discovered_combos),
                layer="scifor",
            )

    # Any key still unresolved: warn (0 iterations) when a source exists but
    # yields no values; error when no input provides the key at all. (A key a
    # fully static PathInput can never supply was already dropped above by
    # resolve_pathinput_discovery, so it won't reach this loop.)
    for key in list(metadata_iterables.keys()):
        v = metadata_iterables[key]
        if isinstance(v, list) and len(v) == 0:
            if _key_has_source(inputs, key, pi):
                Log.warn(
                    f"no values found for '{key}' in inputs, 0 iterations",
                    layer="scifor",
                )
            else:
                raise ValueError(
                    f"Empty list [] was passed for '{key}', but no input "
                    f"DataFrame has that column and no PathInput template "
                    f"has a {{{key}}} placeholder. Provide values "
                    f"explicitly, add a DataFrame input with a '{key}' "
                    f"column, or use a PathInput with a {{{key}}} "
                    f"placeholder."
                )
    return metadata_iterables, combos


def _apply_declared_key_types(
    metadata_iterables: dict,
    combos: "list[dict] | None",
    key_types: dict,
    resolver: "Callable[[PathInput, dict], Any] | None",
) -> "tuple[list[dict] | None, Callable[[PathInput, dict], Any] | None]":
    """Declared key types (standalone only), applied in place.

    Explicit values of a numeric key collapse to one spelling, so trial=[1]
    and a discovered "001" are the same trial; discovered combos were
    already converted by apply_discovery. A string key turns off the
    zero-padding bridge when a PathInput is resolved per combo.
    """
    Log.info(
        "schema key types: %s (declared in this call)",
        describe_key_types(key_types),
        layer="scifor",
    )
    for key, key_type in key_types.items():
        values = metadata_iterables.get(key)
        if isinstance(values, list) and values:
            metadata_iterables[key] = canonicalize_values(key, values, key_type)
    if combos is not None:
        combos = canonicalize_combos(combos, key_types)
    if resolver is None and any(t == "string" for t in key_types.values()):
        resolver = _exact_string_key_resolver(key_types)
    return combos, resolver


def iterated_schema_keys(schema_keys: "list[str]", metadata_iterables: dict) -> "list[str]":
    """The schema keys this run iterates, in schema order. Empty means
    ``distribute=True`` falls back to the top of the schema (nothing at a
    schema level is iterated), which MATLAB's loop marks on its result."""
    return [k for k in schema_keys if k in metadata_iterables]


def _distribute_target(schema_keys: "list[str]", metadata_iterables: dict) -> str:
    """The schema key ``distribute=True`` expands outputs to.

    The schema is the caller's dataset schema (row selection is the
    ``select_rows`` hook, not extra schema keys).

    The resolved target is reported at INFO, not DEBUG. This line is the one
    observable difference between "distribute ran" and "distribute was
    silently dropped somewhere upstream", and the latter fails invisibly:
    the run still succeeds and still saves, just at the wrong granularity.
    At DEBUG the two were indistinguishable in scistack.log, which is how a
    MATLAB run with distribute=true went undiagnosed until the saved columns
    were inspected by hand (2026-09-14).
    """
    real_schema_keys = list(schema_keys)
    if not real_schema_keys:
        raise ValueError(
            "distribute=True requires a schema. Call set_schema() or "
            "configure_database() first."
        )
    iter_keys_in_schema = iterated_schema_keys(real_schema_keys, metadata_iterables)
    if not iter_keys_in_schema:
        # Nothing is being iterated at a schema level (e.g. a fully static
        # PathInput with no {key} placeholders, or no metadata_iterables at
        # all) — distribute to the top of the schema rather than erroring.
        target = real_schema_keys[0]
        Log.info(
            "resolve_distribute_target: '%s' (top of schema; nothing iterated)",
            target,
            layer="scifor",
        )
        return target
    deepest_iterated = iter_keys_in_schema[-1]
    deepest_idx = real_schema_keys.index(deepest_iterated)
    if deepest_idx + 1 >= len(real_schema_keys):
        raise ValueError(
            f"distribute=True but '{deepest_iterated}' is the deepest schema key. "
            f"There is no lower level to distribute to. "
            f"Schema order: {real_schema_keys}"
        )
    target = real_schema_keys[deepest_idx + 1]
    Log.info(
        "resolve_distribute_target: '%s' (one level below '%s')",
        target,
        deepest_iterated,
        layer="scifor",
    )
    return target


def _resolve_static_colnames(inputs: dict, schema_keys: "list[str]") -> dict:
    """Resolve static ColName(df) wrappers before the data/constant split.

    Deferred ColName() markers (no DataFrame) are left in place — they
    resolve per-column inside the for_columns iteration loop.
    """
    static_count = sum(1 for v in inputs.values() if isinstance(v, ColName) and not v.is_deferred)
    deferred_count = sum(1 for v in inputs.values() if isinstance(v, ColName) and v.is_deferred)
    if static_count or deferred_count:
        Log.debug(
            "resolve_colnames: %d static ColName(df) wrapper(s); deferring "
            "%d no-arg ColName() marker(s) to for_columns iteration",
            static_count,
            deferred_count,
            layer="scifor",
        )
    return _resolve_colnames(inputs, schema_keys)


def _classify_inputs(inputs: dict) -> "tuple[dict, dict]":
    """Split inputs into ``(data inputs, constants)``."""
    data_inputs = {}
    constant_inputs = {}
    for param_name, var_spec in inputs.items():
        if _is_data_input(var_spec):
            data_inputs[param_name] = var_spec
        else:
            constant_inputs[param_name] = var_spec
    Log.debug(
        "classify_inputs: %d data input(s), %d constant(s)",
        len(data_inputs),
        len(constant_inputs),
        layer="scifor",
    )
    return data_inputs, constant_inputs


def _as_table_set(as_table: "list[str] | bool | None", data_inputs: dict) -> "set[str]":
    """Input names to keep as full DataFrames (with schema columns)."""
    if as_table is True:
        names = set(data_inputs.keys())
    elif as_table:
        names = set(as_table)
    else:
        names = set()
    if names:
        Log.debug("as_table inputs: %s", sorted(names), layer="scifor")
    return names


def _plan_for_columns(
    data_inputs: dict,
    constant_inputs: dict,
    schema_keys: "list[str]",
    output_names: "list[str]",
    *,
    bare_colname_params: "list[str]",
    distribute_key: "str | None",
) -> "ForColumnsPlan | None":
    """Detect iterate-mode ColumnSelection inputs (for_columns).

    These fan out column-wise: fn runs once per column (or group) and the
    per-column results are reassembled into one wide row per combo. All
    iterate inputs share a single column axis (zipped by name).
    """
    iterate_params = [
        name
        for name, spec in data_inputs.items()
        if getattr(_unwrap_column_selection(spec), "iterate", False)
    ]
    _require_iterate_input(constant_inputs, iterate_params, bare_colname_params)
    if not iterate_params:
        return None

    col_sets = {
        name: _resolve_iterate_columns(data_inputs[name], schema_keys)
        for name in iterate_params
    }
    iterate_columns = col_sets[iterate_params[0]]
    first_cs = _unwrap_column_selection(data_inputs[iterate_params[0]])
    for name in iterate_params[1:]:
        other_cs = _unwrap_column_selection(data_inputs[name])
        if first_cs.groups is not None or other_cs.groups is not None:
            if first_cs.groups != other_cs.groups:
                raise ColumnGroupsError(
                    f"for_columns inputs are zipped group by group, so they "
                    f"must declare the same groups. '{iterate_params[0]}' has "
                    f"{_groups_text(first_cs)} but '{name}' has "
                    f"{_groups_text(other_cs)}."
                )
        elif set(col_sets[name]) != set(iterate_columns):
            raise ValueError(
                f"for_columns inputs must iterate over the same columns "
                f"(zipped by name). '{iterate_params[0]}' has "
                f"{iterate_columns} but '{name}' has {col_sets[name]}."
            )
    mode, column_groups = resolve_column_groups(
        iterate_columns,
        first_cs.groups,
        output_names,
        every_column=not first_cs.columns,
    )
    if distribute_key is not None:
        raise ValueError("for_columns cannot be combined with distribute=True.")
    Log.info(
        _describe_for_columns_plan(iterate_params, mode, column_groups, output_names),
        layer="scifor",
    )
    shared = overlapping_columns(column_groups)
    if shared:
        Log.warn(
            "[for_columns] column(s) in more than one group: "
            + "; ".join(f"{c} in {names}" for c, names in shared.items())
            + " — each group still receives it",
            layer="scifor",
        )
    return ForColumnsPlan(
        params=iterate_params, columns=iterate_columns, groups=column_groups, mode=mode
    )


def _require_iterate_input(
    constant_inputs: dict, iterate_params: "list[str]", bare_colname_params: "list[str]"
) -> None:
    """Deferred ColName() markers and ``{ColName}`` PathOutput templates
    resolve to the current for_columns column, so they need an iterate input.

    (Static ColName(df) was already resolved to a string, so only no-arg
    markers remain here; metadata-only PathOutput templates resolve per
    combo and are fine without one.)
    """
    if iterate_params:
        return
    deferred_colname_params = [
        name for name, v in constant_inputs.items() if isinstance(v, ColName) and v.is_deferred
    ]
    if deferred_colname_params:
        bare_here = [p for p in deferred_colname_params if p in bare_colname_params]
        if bare_here:
            hint = (
                f"Input(s) {bare_here} were given the bare ColName class "
                f"(no parentheses), which is treated as the deferred ColName() form. "
                f"If you meant the static single-column form, instantiate it with a "
                f"DataFrame: ColName(df). Otherwise add a for_columns input to "
                f"iterate over."
            )
        else:
            hint = "Use ColName(df) for the static single-column form instead."
        raise ValueError(
            f"ColName() with no argument resolves to the current for_columns "
            f"column, so it requires at least one iterate input "
            f"(for_columns() / ColumnSelection(..., iterate=True)). "
            f"Deferred ColName() input(s): {deferred_colname_params}. "
            f"{hint}"
        )
    path_output_column_params = [
        name
        for name, v in constant_inputs.items()
        if isinstance(v, PathOutput) and v.has_column_token
    ]
    if path_output_column_params:
        raise ValueError(
            f"PathOutput template uses the {{ColName}} token, which resolves to "
            f"the current for_columns column, so it requires at least one iterate "
            f"input (for_columns() / ColumnSelection(..., iterate=True)). "
            f"PathOutput input(s): {path_output_column_params}."
        )


def _build_combos(
    keys: "list[str]", metadata_iterables: dict, prebuilt: "list[dict] | None"
) -> "list[dict]":
    """The combo list: pre-built (hooks / discovery), else the Cartesian product."""
    if prebuilt is not None:
        Log.debug(
            "expand_combos: using %d pre-built combos (from DB wrapper)",
            len(prebuilt),
            layer="scifor",
        )
        return prebuilt
    value_lists = [metadata_iterables[k] for k in keys]
    combos = [dict(zip(keys, combo, strict=False)) for combo in product(*value_lists)]
    Log.debug(
        "expand_combos: built %d combos from Cartesian product of %s",
        len(combos),
        keys,
        layer="scifor",
    )
    return combos


def _plan_shared_limits(
    fn: Callable,
    share_limits: "dict[str, list[str]] | None",
    data_inputs: dict,
    schema_keys: "list[str]",
) -> "tuple[dict, set[str] | None]":
    """share_limits prepass: per-group numeric extents, so all combos in a
    group (e.g. all trials within a subject) can share axis limits. Returns
    the map and the parameter names the function accepts the ``*_limits``
    kwargs under (None: any, via ``**kwargs``)."""
    if not share_limits:
        return {}, None
    shared = _compute_shared_limits(share_limits, data_inputs, schema_keys)
    accepted = _accepted_param_names(fn)
    Log.debug(
        "compute_shared_limits: %s (fn accepts: %s)",
        list(shared.keys()),
        sorted(accepted) if accepted is not None else "any (**kwargs)",
        layer="scifor",
    )
    return shared, accepted


def _option_parts(
    dry_run: bool,
    distribute: bool,
    as_table: "list[str] | bool | None",
    where: Any,
    locations: "LocationFilter | dict | None",
) -> "tuple[str, ...]":
    """The non-default options, as the banner's ``options:`` line shows them."""
    parts = []
    if dry_run:
        parts.append("dry_run=True")
    if distribute:
        parts.append("distribute=True")
    if as_table:
        parts.append(f"as_table={as_table!r}")
    if where is not None:
        parts.append(f"where={where!r}")
    if locations is not None and not LocationFilter.of(locations).is_empty():
        location_filter = LocationFilter.of(locations)
        parts.append(
            f"locations={len(location_filter.include)} prefix(es), "
            f"excluding {dict(location_filter.exclude_levels) or '{}'}"
        )
    return tuple(parts)


def _announce(plan: RunPlan) -> None:
    """The run banner: one INFO line with a truncated per-key value preview;
    the full value lists and input details follow at DEBUG."""
    display_keys = plan.display_keys
    total = plan.total
    meta_summary = (
        ", ".join(f"{k}={_format_value_list(plan.metadata_iterables[k])}" for k in display_keys)
        if display_keys
        else "no metadata"
    )
    # The per-key previews are each key's FULL value list, so their product can
    # exceed the real iteration count — combos get filtered out beforehand
    # (PathInput discovery dropping combos with no file on disk, where=,
    # skip_computed). Reporting "4 iterations: subject=3 values, session=2
    # values" without saying so reads as an arithmetic error.
    cartesian = 1
    for k in display_keys:
        cartesian *= len(plan.metadata_iterables[k])
    pruned_note = ""
    if display_keys and cartesian > total:
        pruned_note = (
            f" (of {cartesian} possible combination(s); "
            f"{cartesian - total} filtered out before iteration)"
        )
    Log.info(
        f"for_each({plan.fn_name}) — {total} iteration{'s' if total != 1 else ''}"
        f"{pruned_note}: {meta_summary}",
        layer="scifor",
    )
    # Which combos were dropped is the actual diagnostic question. Only worth
    # materialising the product for a small one.
    if pruned_note and cartesian <= 1000:
        kept = {tuple(str(c.get(k)) for k in display_keys) for c in plan.combos}
        dropped = [
            dict(zip(display_keys, combo, strict=False))
            for combo in product(*(plan.metadata_iterables[k] for k in display_keys))
            if tuple(str(v) for v in combo) not in kept
        ]
        Log.debug(
            "filtered out %d combination(s): %s",
            len(dropped),
            dropped,
            layer="scifor",
        )

    Log.info(f"inputs: {_format_inputs(plan.inputs)}", layer="scifor")
    if not plan.schema_keys and plan.keys:
        Log.info(
            f"[plan] no schema: filtering inputs by the iterated keys {plan.keys}",
            layer="scifor",
        )
    _warn_repeated_static_path_inputs(plan.fn_name, plan.inputs, total, display_keys)
    for k in display_keys:
        formatted = ", ".join(repr(v) for v in plan.metadata_iterables[k])
        Log.debug("%s=[%s]", k, formatted, layer="scifor")
    if plan.option_parts:
        Log.info(f"options: {', '.join(plan.option_parts)}", layer="scifor")

    if plan.dry_run:
        # Dry-run output is the requested result, not logging: print to stdout.
        print(f"[dry-run] for_each({plan.fn_name})")
        print(f"[dry-run] {total} iterations over: {plan.keys}")
        print(f"[dry-run] inputs: {_format_inputs(plan.inputs)}")
        if plan.distribute_key is not None:
            print(
                f"[dry-run] distribute: '{plan.distribute_key}' (split outputs by "
                f"element/row, numbered from 1; a returned table carrying "
                f"'{plan.distribute_key}' files one level lower per group)"
            )
        print()


# ---------------------------------------------------------------------------
# Phase 2: one combination
# ---------------------------------------------------------------------------


@contextmanager
def _combo_named_thread(fn_name: str, metadata_str: str):
    """Name the thread after the combination while a debugger is loaded, so
    VS Code's Call Stack panel says which schema key the function is on. Nothing
    happens otherwise: a thread name is process state, and a loop needs none."""
    if "debugpy" not in sys.modules:
        yield
        return
    thread = threading.current_thread()
    previous = thread.name
    thread.name = f"{fn_name} {metadata_str}"
    try:
        yield
    finally:
        thread.name = previous


def run_combo(plan: RunPlan, metadata: dict, hooks: LoopHooks, tally: RunTally) -> ComboResult:
    """Run the function at one combination and return its result rows.

    Prepares each input (schema filter, ``hooks.select_rows``, extraction),
    calls the function (column-wise under for_columns), and splits the
    result under ``distribute``. A failing input or call is recorded in
    ``tally.failure_groups`` and returned as the result's ``failure``.
    """
    metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
    prepared = _prepare_combo_inputs(plan, metadata, metadata_str, hooks, tally)
    if isinstance(prepared, ComboResult):
        return prepared
    filtered_inputs, iterate_dfs = prepared
    _check_column_drift(plan, iterate_dfs, metadata_str)
    _log_empty_inputs(filtered_inputs, iterate_dfs, metadata_str)

    all_param_names = (
        list(filtered_inputs.keys()) + list(plan.iterate_params) + list(plan.constant_inputs.keys())
    )
    if plan.for_columns is not None:
        msg = (
            f"[run] {metadata_str}: {plan.fn_name} x {len(plan.for_columns.groups)} group(s) "
            f"({', '.join(all_param_names)})"
        )
    else:
        msg = f"[run] {metadata_str}: {plan.fn_name}({', '.join(all_param_names)})"
    Log.debug(msg, layer="scifor")

    filtered_inputs.update(plan.constant_inputs)
    _inject_shared_limits(plan, filtered_inputs, metadata)

    try:
        with _combo_named_thread(plan.fn_name, metadata_str):
            result = _call_at_combo(plan, filtered_inputs, iterate_dfs, metadata, metadata_str, tally)
    except ForColumnsError:
        # Structural for_columns errors are deterministic across combos and
        # indicate a return-contract bug — surface immediately, don't skip.
        raise
    except Exception as e:
        if getattr(e, "scifor_fatal", False):
            # Deterministic configuration errors marked fatal by a higher
            # layer (e.g. scidb's SchemaKeyTypeError) would fail every combo
            # identically — abort the run instead of skipping through N
            # copies of the same failure.
            Log.error(f"[fatal] {metadata_str}: {e}", layer="scifor", exc_info=True)
            raise
        failure = _record_iteration_failure(
            tally.failure_groups,
            e,
            fn_name=plan.fn_name,
            metadata=metadata,
            metadata_str=metadata_str,
            stage="call",
            context=f"{plan.fn_name} raised",
        )
        return ComboResult(failure=failure, error=e)

    if not isinstance(result, tuple):
        result = (result,)
    # Some (never all, see _raise_if_every_output_skipped) outputs have
    # nothing to save here: tally them per output for the summary. The slots
    # stay in the row; scidb's save writes no record for them.
    for i, slot in enumerate(result):
        if is_no_output(slot):
            tally.output_skips.setdefault(_output_label(plan.output_names, i), []).append(
                (metadata_str, slot)
            )
    if plan.distribute_key is None:
        return ComboResult(rows=[(metadata, result)])
    return ComboResult(rows=_distribute_rows(plan, metadata, metadata_str, result, tally))


def _prepare_combo_inputs(
    plan: RunPlan, metadata: dict, metadata_str: str, hooks: LoopHooks, tally: RunTally
) -> "tuple[dict, dict] | ComboResult":
    """Filter/prepare every data input for this combo:
    ``(per-combo inputs, {for_columns input: full per-combo frame})``, or the
    failure of the first input that could not be prepared."""
    filtered_inputs: dict = {}
    iterate_dfs: "dict[str, Any]" = {}
    for param_name, var_spec in plan.data_inputs.items():
        try:
            if param_name in plan.iterate_params:
                # Keep the full per-combo DataFrame; sliced per column later.
                iterate_dfs[param_name] = _prepare_iterate_df(
                    var_spec,
                    metadata,
                    plan.filter_keys,
                    plan.where,
                    select_rows=hooks.select_rows,
                    param_name=param_name,
                )
                continue
            filtered_inputs[param_name] = _prepare_input(
                var_spec,
                metadata,
                plan.filter_keys,
                param_name in plan.as_table_set,
                plan.where,
                mapping_cols=(hooks.mapping_inputs or {}).get(param_name),
                select_rows=hooks.select_rows,
                param_name=param_name,
            )
        except Exception as e:
            failure = _record_iteration_failure(
                tally.failure_groups,
                e,
                fn_name=plan.fn_name,
                metadata=metadata,
                metadata_str=metadata_str,
                stage="prepare_inputs",
                context=f"failed to filter {param_name}",
            )
            return ComboResult(failure=failure, error=e)
    return filtered_inputs, iterate_dfs


def _check_column_drift(plan: RunPlan, iterate_dfs: dict, metadata_str: str) -> None:
    """Column drift is a hard error (not a per-combo skip): the iterate
    column set is fixed up front, so a combo missing one of those columns
    means the stored data is inconsistent and must be surfaced."""
    if plan.for_columns is None:
        return
    columns = plan.for_columns.columns
    for name in plan.for_columns.params:
        missing = [c for c in columns if c not in iterate_dfs[name].columns]
        if missing:
            raise ValueError(
                f"for_columns column drift: column(s) {missing} missing from "
                f"input '{name}' for combo {metadata_str}. The iterate column "
                f"set {columns} must be present in every combo."
            )


def _log_empty_inputs(filtered_inputs: dict, iterate_dfs: dict, metadata_str: str) -> None:
    """Surface empty per-combo inputs explicitly. After upstream existence
    filtering an empty input usually means this combo has no backing data
    for that input — the function is still called (it may handle empties),
    but the emptiness must never be silent."""
    empty = [
        name
        for name, val in (list(filtered_inputs.items()) + list(iterate_dfs.items()))
        if _input_is_empty(val)
    ]
    if empty:
        Log.debug(
            "[empty-combo] %s: input(s) %s had 0 rows",
            metadata_str,
            ", ".join(empty),
            layer="scifor",
        )


def _inject_shared_limits(plan: RunPlan, call_inputs: dict, metadata: dict) -> None:
    """Add this combo's group's shared axis limits (share_limits), in place."""
    for input_name, (group_keys, limits) in plan.shared_limits.items():
        param = f"{input_name}_limits"
        if plan.limits_accepted is not None and param not in plan.limits_accepted:
            continue  # fn signature doesn't accept it and has no **kwargs
        gkey = tuple(str(metadata.get(k, "")) for k in group_keys)
        if gkey in limits:
            call_inputs[param] = limits[gkey]


def _call_at_combo(
    plan: RunPlan,
    call_inputs: dict,
    iterate_dfs: dict,
    metadata: dict,
    metadata_str: str,
    tally: RunTally,
) -> Any:
    """Call the function once (or once per column group) at this combo."""
    fn_t0 = time.perf_counter()
    fc = plan.for_columns
    if fc is not None:
        iterated = _run_column_iteration(
            plan.fn,
            call_inputs,
            iterate_dfs,
            fc.groups,
            plan.filter_keys,
            plan.as_table_set,
            metadata,
            path_input_resolver=plan.path_input_resolver,
            mode=fc.mode,
            metadata_str=metadata_str,
            no_data_groups=tally.no_data_groups,
            group_columns=tally.group_columns,
        )
        # Split: one slot per output already. Combine: the one row.
        result = iterated if fc.mode == SPLIT else (iterated,)
    else:
        # PathOutput/PathInput constants resolve to a finished path from
        # this combo's metadata (no column outside for_columns).
        resolved = _resolve_path_outputs(call_inputs, metadata, None)
        resolved = _resolve_path_inputs(resolved, metadata, resolver=plan.path_input_resolver)
        result = _call_fn(plan.fn, resolved, plan.n_outputs)
        result = _mark_empty_returns(result, plan.output_names, metadata_str, tally)
    Log.debug(
        "[done] %s: %s completed in %.3fs",
        metadata_str,
        plan.fn_name,
        time.perf_counter() - fn_t0,
        layer="scifor",
    )
    _raise_if_every_output_skipped(result, plan.output_names)
    return result


#: A result row's slot before any output filled it. A list output may hold None.
_NO_SLOT = object()


def _distribute_rows(
    plan: RunPlan, metadata: dict, metadata_str: str, result: tuple, tally: RunTally
) -> "list[tuple[dict, tuple]]":
    """Expand one combo's outputs into one result row per address.

    Each row holds one slot per output, because port i is return value i. An
    output with no piece at an address saves nothing there (a ``NoOutput``
    slot). Rows used to hold only the piece of the output being split, so every
    output of a multi-output run was filed under the first output's name.
    """
    by_address: "dict[tuple, tuple[dict, list]]" = {}
    n_outputs = len(result)
    for index, output_value in enumerate(result):
        if is_no_output(output_value):
            # Nothing to split (e.g. an empty table); already tallied.
            continue
        label = _output_label(plan.output_names, index)
        try:
            target, carried, pieces = _distribute_pieces(
                output_value,
                plan.distribute_key,
                plan.schema_keys,
                output_name=label,
                where=metadata_str,
            )
        except TypeError as e:
            Log.warn(f"{metadata_str}: cannot distribute: {e}", layer="scifor")
            continue
        previous = tally.distribute_targets.setdefault(label, target)
        if previous != target:
            raise DistributeError(
                f"distribute=True but {label} would be filed at {previous!r} at one "
                f"combination and at {target!r} at {metadata_str}: its returned tables "
                f"do not carry the same schema key columns. One output has one level."
            )
        if carried and label not in tally.distribute_column_reported:
            tally.distribute_column_reported.add(label)
            Log.info(
                "distribute: %s carries %s, so its rows are numbered within each "
                "group at '%s' and those columns are removed from the data",
                label,
                carried,
                target,
                layer="scifor",
            )
        for address, piece in pieces:
            key = tuple(sorted((k, str(v)) for k, v in address.items()))
            _meta, slots = by_address.setdefault(
                key, ({**metadata, **address}, [_NO_SLOT] * n_outputs)
            )
            slots[index] = piece

    rows: "list[tuple[dict, tuple]]" = []
    for row_metadata, slots in by_address.values():
        for index, slot in enumerate(slots):
            if slot is not _NO_SLOT:
                continue
            original = result[index]
            if is_no_output(original):
                slots[index] = original
                continue
            skipped = NoOutput("skipped", "no row of this output at this address")
            slots[index] = skipped
            tally.output_skips.setdefault(_output_label(plan.output_names, index), []).append(
                (metadata_str, skipped)
            )
        rows.append((row_metadata, tuple(slots)))
    return rows


# ---------------------------------------------------------------------------
# Phase 3: assemble
# ---------------------------------------------------------------------------


def assemble(
    plan: RunPlan, tally: RunTally, hooks: LoopHooks, *, elapsed: float
) -> "pd.DataFrame | None":
    """End-of-run summary and the result table.

    Logs the summary (counts, one line per failure reason), sends the
    ``summary`` event to the journal and ``hooks.progress_fn``, and builds
    the output DataFrame. Under ``on_error="stop"`` it re-raises the original
    exception with the partial result attached instead of returning.
    """
    if plan.dry_run:
        print(f"[dry-run] would process {plan.total} iterations")
        return None

    summary_event = _log_summary(plan, tally, elapsed)
    # The same summary feeds the journal block of the run this loop belongs
    # to (scifor.failures.journal_run, opened by for_each's entry).
    journal = Log.current_run()
    if journal is not None:
        journal.add_summary(summary_event)
    hooks.emit(summary_event)

    result_df = _build_result_table(plan, tally)
    if tally.output_skips and not hooks.keep_no_output:
        result_df = _no_output_to_missing(result_df, plan.output_names)
    if tally.stop is not None:
        stop_exc, stop_failure = tally.stop
        Log.info(
            stop_line(plan.fn_name, stop_failure, tally.completed),
            layer="scifor",
        )
        attach_stopped_run(
            stop_exc,
            StoppedRun(failure=stop_failure, partial_result=result_df, completed=tally.completed),
        )
        raise stop_exc
    return result_df


def stop_line(fn_name: str, failure: ComboFailure, completed: int) -> str:
    """The ``on_error="stop"`` line (Python's loop and MATLAB's)."""
    return (
        f"for_each({fn_name}): on_error=stop — stopped at "
        f"{failure.exc_type} {format_user_frame(failure.user_frame)}; "
        f"{completed} combo(s) completed before it, re-raising the original exception"
    )


def _log_summary(plan: RunPlan, tally: RunTally, elapsed: float) -> dict:
    """Log the end-of-run summary and return the ``summary`` event."""
    fn_name = plan.fn_name
    total = plan.total
    cancelled_suffix = ", cancelled" if tally.cancelled else ""
    if tally.stop is not None:
        cancelled_suffix += ", stopped (on_error=stop)"
    # NoDataError means the combo simply has no backing data (expected in a
    # sparse schema-key cross-product) — split it out from genuine `fn`
    # failures so the summary doesn't cry "failed" over missing data.
    no_data_count = tally.no_data
    failed_count = tally.failed
    Log.info(
        f"for_each({fn_name}) done in {elapsed:.1f}s: completed={tally.completed}, "
        f"failed={failed_count}, no_data={no_data_count}, total={total}"
        f"{cancelled_suffix}",
        layer="scifor",
    )
    # Everything failed: the caller gets an EMPTY DataFrame, which is exactly
    # what "there was nothing to iterate" looks like from the outside. The two
    # have opposite causes and the same symptom, so this one is a WARNING —
    # found twice in one afternoon (2026-09-14), both times read as a discovery
    # or filter bug when in fact every call had raised the same TypeError.
    if total and tally.completed == 0 and failed_count and tally.stop is None:
        Log.warn(
            f"for_each({fn_name}): every one of the {total} iteration(s) "
            f"failed, so the result is EMPTY — this is not an empty schema. "
            f"Reasons follow.",
            layer="scifor",
        )
    # One line per distinct failure reason, so the default (INFO) log always
    # answers "what failed and why" without per-iteration lines.
    for group in tally.failure_groups.values():
        shown = group.combos[:_SUMMARY_COMBOS_MAX]
        more = f" (+{group.count - len(shown)} more)" if group.count > len(shown) else ""
        if group.no_data:
            Log.info(
                f'no data: {group.count} × "{group.reason}" — {"; ".join(shown)}{more}',
                layer="scifor",
            )
        else:
            Log.info(
                f'failed: {group.count} × "{group.reason}" — {"; ".join(shown)}{more} '
                f"[{format_user_frame(group.example.user_frame)}]",
                layer="scifor",
            )
    for line in _describe_output_skips(tally.output_skips):
        Log.info(line, layer="scifor")
    for line in _describe_empty_returns(tally.empty_returns):
        Log.warn(line, layer="scifor")
    for name, combos in tally.no_data_groups.items():
        shown = combos[:_SUMMARY_COMBOS_MAX]
        more = f" (+{len(combos) - len(shown)} more)" if len(combos) > len(shown) else ""
        Log.info(
            f"for_columns group {name}: no data × {len(combos)} — "
            f"{'; '.join(shown)}{more} (left missing in the row)",
            layer="scifor",
        )
    return {
        "event": "summary",
        "current": total,  # keeps positional consumers (GUI) safe
        "total": total,
        "completed": tally.completed,
        "failed": failed_count,
        "no_data": no_data_count,
        "skipped": tally.skipped,  # every non-completed combo (failed + no_data)
        "cancelled": tally.cancelled,
        "stopped": tally.stop is not None,
        "failures": groups_to_wire(tally.failure_groups),
        "output_skips": _output_skips_to_wire(tally.output_skips),
    }


def _build_result_table(plan: RunPlan, tally: RunTally) -> "pd.DataFrame":
    """Combine every combo's result row into the output DataFrame."""
    rows = tally.rows
    Log.debug(
        "building output DataFrame from %d result row(s)",
        len(rows),
        layer="scifor",
    )
    fc = plan.for_columns
    if fc is not None and fc.mode != SPLIT and rows:
        # Every combination's row gets every group's columns, in group order:
        # a group with no data somewhere (or everywhere) is still a column,
        # so the output's column set does not depend on which data existed.
        expected = _combine_output_columns(fc.groups, tally.group_columns)
        rows = [(meta, (row.reindex(columns=expected),)) for meta, (row,) in rows]

    # Combining every combo's return into one table is where a function that
    # returns different kinds of value for different combos fails — inside
    # SciStack, but fixable by the user. The seam says so (and names the
    # combos, via describe_value_kinds) instead of blaming SciStack.
    with user_value_seam(
        "combine_outputs",
        fn_name=plan.fn_name,
        fn=plan.fn,
        diagnose=lambda: _describe_output_kinds(rows, plan.output_names, plan.fn_name),
    ):
        return _results_to_output_dataframe(
            rows, plan.output_names, plan.schema_col_dtypes, plan.schema_keys
        )


def _call_fn(fn, kwargs, n_outputs):
    """Call fn with the right number of output captures."""
    return call_user_function(fn, **kwargs)


def _resolve_path_outputs(kwargs: dict, metadata: dict, column: "str | None") -> dict:
    """Return a copy of kwargs with any PathOutput resolved to a finished path.

    Substitutes the combo metadata and (inside for_columns) the current column.
    Non-PathOutput entries pass through untouched.
    """
    if not any(isinstance(v, PathOutput) for v in kwargs.values()):
        return kwargs
    return {
        name: (v.resolve(metadata, column) if isinstance(v, PathOutput) else v)
        for name, v in kwargs.items()
    }


def _exact_string_key_resolver(key_types: dict):
    """A per-combo PathInput resolver under which ``string``-declared keys
    match only literally (no ``1`` ↔ ``"001"`` bridge); every other key keeps
    the numeric fallback (``schema_types.numeric_match_keys``)."""

    def resolve(pi: PathInput, metadata: dict):
        path, _ = pi.load_with_captures(
            metadata, numeric_match=numeric_match_keys(metadata.keys(), key_types)
        )
        return path

    return resolve


def bind_path_inputs(inputs: dict, root: Path) -> dict:
    """*inputs* with every PathInput -- bare, under ``Fixed``, or an ``EachOf``
    alternative -- replaced by a copy bound to *root* (``PathInput.bound_to``).

    Identity is unchanged (``to_key`` is the name), so version keys, call ids
    and the skip gate see exactly what the caller passed. Shared by scifor's
    and scidb's ``for_each`` so both bind the same shapes.
    """

    def bind(value: Any) -> Any:
        if isinstance(value, PathInput):
            return value.bound_to(root)
        if isinstance(value, Fixed) and isinstance(value.data, PathInput):
            clone = copy.copy(value)
            clone.data = value.data.bound_to(root)
            return clone
        if isinstance(value, EachOf) and any(
            isinstance(a, (PathInput, Fixed)) for a in value.alternatives
        ):
            clone = copy.copy(value)
            clone.alternatives = [bind(a) for a in value.alternatives]
            return clone
        return value

    return {name: bind(value) for name, value in inputs.items()}


def _resolve_path_inputs(kwargs: dict, metadata: dict, resolver=None) -> dict:
    """Return a copy of kwargs with any PathInput resolved to a path.

    Substitutes the combo metadata into the template. Non-PathInput entries
    pass through untouched. ``resolver(pathinput, metadata) -> path``
    overrides the default ``pathinput.load(**metadata)`` — used by DB
    wrappers that need schema-key-type-aware resolution.
    """
    if not any(isinstance(v, PathInput) for v in kwargs.values()):
        return kwargs
    resolve_fn = resolver or (lambda pi, m: pi.load(**m))
    return {
        name: (resolve_fn(v, metadata) if isinstance(v, PathInput) else v)
        for name, v in kwargs.items()
    }


class NoDataError(RuntimeError):
    """A per-combo DataFrame input had no matching rows after combo/where
    filtering. This is expected whenever the schema-key cross-product is
    sparser than the full grid (e.g. not every subject ran every trial), so
    it is reported separately from genuine ``fn`` failures in the end-of-run
    summary rather than as "failed"."""


class ForColumnsError(UserValueError, ValueError):
    """A structural error in for_columns reassembly (e.g. a colliding output
    column or a non-collapsible return). These are deterministic across combos
    and indicate a function/return-contract bug, so they propagate as hard
    errors rather than being swallowed as a per-combo ``[skip]``. Raised inside
    SciStack about what the user's function returned, so a ``UserValueError``."""


class ColumnFunctionError(ValueError):
    """The for_columns function raised on one or more iterated columns.

    Rather than skipping the whole combo on the first column that fails, the
    column loop runs every column, collects each failure, and raises this once
    with the complete list. A per-column failure is almost always deterministic
    across combos (e.g. a non-numeric column the function can't process), so the
    full list is the actionable signal: it names exactly which columns to drop
    from the selection or fix upstream. The failing ``(column, message)`` pairs
    are available on the ``failures`` attribute."""

    def __init__(self, message: str, failures: "list[tuple[str, str]]"):
        super().__init__(message)
        self.failures = failures


def _output_label(output_names: "list[str]", i: int) -> str:
    """The summary label of output slot ``i`` (its result column name)."""
    return output_names[i] if i < len(output_names) else f"output_{i + 1}"


def _mark_empty_returns(
    result: Any, output_names: "list[str]", metadata_str: str, tally: "RunTally"
) -> Any:
    """Each output the function returned as an EMPTY table becomes a
    ``NoOutput("no_data", EMPTY_TABLE_DETAIL)`` slot (``scifor.outputs``
    owns the rule) and is tallied for the end-of-run WARN. When every slot is
    empty, ``_raise_if_every_output_skipped`` then counts the combination as
    no data; otherwise only the empty outputs save nothing here."""
    result, empty = empty_tables_to_no_output(result)
    for i in empty:
        label = _output_label(output_names, i)
        tally.empty_returns.setdefault(label, []).append(metadata_str)
        Log.debug(
            "%s: output %s is an empty table — nothing to save here",
            metadata_str,
            label,
            layer="scifor",
        )
    return result


def _describe_empty_returns(empty_returns: "dict[str, list[str]]") -> "list[str]":
    """The end-of-run WARN lines for outputs returned as empty tables: one
    per output, naming (up to the summary cap) the combinations."""
    lines = []
    for name, combos in empty_returns.items():
        shown = combos[:_SUMMARY_COMBOS_MAX]
        more = f" (+{len(combos) - len(shown)} more)" if len(combos) > len(shown) else ""
        lines.append(
            f"output {name}: the function returned an empty table at "
            f"{len(combos)} combination(s) — nothing saved there: "
            f"{'; '.join(shown)}{more}"
        )
    return lines


def _raise_if_every_output_skipped(result: Any, output_names: "list[str]") -> None:
    """A call whose EVERY output slot is a ``NoOutput`` saved nothing, so it
    is one no-data or failed combination, not a completed one: raise so the
    loop's ordinary skip path counts and groups it. All ``no_data`` ->
    ``NoDataError``; otherwise ``ColumnFunctionError`` naming each slot."""
    slots = result if isinstance(result, tuple) else (result,)
    if not slots or not all(is_no_output(s) for s in slots):
        return
    pairs = [
        (_output_label(output_names, i), s.detail or s.reason) for i, s in enumerate(slots)
    ]
    details = "; ".join(f"{name}: {why}" for name, why in pairs)
    if all(s.reason == "no_data" for s in slots):
        raise NoDataError(f"No data for any output ({details}).")
    raise ColumnFunctionError(f"No output could be produced ({details}).", pairs)


def _no_output_to_missing(result_df: Any, output_names: "list[str]") -> Any:
    """A standalone result shows a skipped output as a missing cell: the
    ``NoOutput`` marker is for a saving caller only."""
    if result_df is None or getattr(result_df, "empty", True):
        return result_df
    result_df = result_df.copy()
    for name in output_names:
        if name in result_df.columns:
            result_df[name] = [None if is_no_output(v) else v for v in result_df[name]]
    return result_df


def _group_output_skips(
    skips: "list[tuple[str, NoOutput]]",
) -> "list[tuple[str, str, list[str]]]":
    """``[(reason, detail, [combo text])]`` in first-seen order: one entry
    per distinct reason, like the per-combo failure groups."""
    groups: "dict[tuple[str, str], list[str]]" = {}
    for combo, slot in skips:
        groups.setdefault((slot.reason, slot.detail), []).append(combo)
    return [(reason, detail, combos) for (reason, detail), combos in groups.items()]


def _combine_output_columns(
    column_groups: "list[tuple[str, list[str]]]",
    group_columns: "dict[str, list[str]]",
) -> "list[str]":
    """The combine output's full column list, in group order.

    A group that produced columns anywhere in the run contributes those. A
    group with no data at EVERY combination was never called, so its names
    are inferred from the other groups (same function, same shape): scalars
    -> ``<name>``; the same dict keys -> ``<name>__<key>``. When the other
    groups disagree, ``<name>`` with a warning, since a later run with data
    may produce different columns.
    """
    sep = FOR_COLUMNS_OUTPUT_SEP

    def shape(name: str, cols: "list[str]") -> "tuple":
        return tuple(None if c == name else c[len(name) + len(sep):] for c in cols)

    shapes = {shape(n, group_columns[n]) for n, _ in column_groups if n in group_columns}
    columns: "list[str]" = []
    for name, _cols in column_groups:
        if name in group_columns:
            columns.extend(group_columns[name])
            continue
        if len(shapes) == 1:
            (only,) = shapes
            added = [name if k is None else f"{name}{sep}{k}" for k in only]
            Log.info(
                f"[for_columns] group {name} had no data at any combination; "
                f"added empty column(s) {added}, shaped like the other groups",
                layer="scifor",
            )
        else:
            added = [name]
            Log.warn(
                f"[for_columns] group {name} had no data at any combination and "
                f"the other groups returned different shapes; added an empty "
                f"column {added}. A later run where {name} has data may produce "
                f"different columns.",
                layer="scifor",
            )
        columns.extend(added)
    return columns


def _describe_output_skips(
    output_skips: "dict[str, list[tuple[str, NoOutput]]]",
) -> "list[str]":
    """The end-of-run lines for outputs skipped at combinations whose other
    outputs were kept: ``output SixMWT: no data × 1 — subject=2``. One line
    per output and reason; nothing when no output was skipped."""
    lines = []
    for name, skips in output_skips.items():
        for reason, detail, combos in _group_output_skips(skips):
            shown = combos[:_SUMMARY_COMBOS_MAX]
            more = f" (+{len(combos) - len(shown)} more)" if len(combos) > len(shown) else ""
            label = "no data" if reason == "no_data" else "failed"
            why = f' "{detail}"' if detail else ""
            lines.append(
                f"output {name}: {label} × {len(combos)}{why} — {'; '.join(shown)}{more}"
            )
    return lines


def _output_skips_to_wire(
    output_skips: "dict[str, list[tuple[str, NoOutput]]]",
) -> "dict[str, list[dict]]":
    """The summary event's ``output_skips``: ``{output: [{reason, detail,
    count, combos}]}``, combos capped like the failure groups."""
    return {
        name: [
            {
                "reason": reason,
                "detail": detail,
                "count": len(combos),
                "combos": combos[:_SUMMARY_COMBOS_MAX],
            }
            for reason, detail, combos in _group_output_skips(skips)
        ]
        for name, skips in output_skips.items()
    }


# Separator joining a source column name to a per-column output key when a
# for_columns function returns multiple named values (dict / Series / 1-row
# DataFrame) instead of a single scalar.
FOR_COLUMNS_OUTPUT_SEP = "__"

# How many groups the run-start for_columns line spells out before eliding;
# a wide all-columns table would otherwise put hundreds of names on one line.
_FOR_COLUMNS_PLAN_SHOWN = 10


def _groups_text(cs: Any) -> str:
    """A ColumnSelection's columns or groups as they read in code."""
    if cs.groups is not None:
        return describe_column_groups(cs.groups)
    return str(cs.columns) if cs.columns else "every column"


def _describe_for_columns_plan(
    iterate_params: "list[str]",
    mode: str,
    groups: "list[tuple[str, list[str]]]",
    output_names: "list[str]",
) -> str:
    """The ONE run-start line saying what a for_columns call will do: which
    input(s), combine or split, and which source columns feed which output
    name. Logged once per run so a mis-ordered split (group i paired with
    the wrong output) is visible before anyone reads the saved data.

    ``groups`` is ``[(name, [source columns])]``: in combine mode ``name`` is
    the output column; in split mode it is the paired output variable.
    """
    params = ", ".join(f"'{p}'" for p in iterate_params)
    shown = groups[:_FOR_COLUMNS_PLAN_SHOWN]
    pairs = "; ".join(f"{name} ← [{', '.join(cols)}]" for name, cols in shown)
    if len(groups) > len(shown):
        pairs += f"; … (+{len(groups) - len(shown)} more)"
    if mode == "split":
        return f"[for_columns] {params} split into {len(groups)} output(s): {pairs}"
    target = output_names[0] if output_names else "output"
    return (
        f"[for_columns] {params} combine → {target}, "
        f"{len(groups)} group(s): {pairs}"
    )


def _run_column_iteration(
    fn,
    base_kwargs,
    iterate_dfs,
    column_groups,
    schema_keys,
    as_table_set,
    metadata,
    path_input_resolver=None,
    mode="combine",
    metadata_str="",
    no_data_groups=None,
    group_columns=None,
):
    """Run fn once per column group at one combination.

    ``column_groups`` is ``[(name, [source columns])]`` from
    ``column_selection.resolve_column_groups``; a plain ``for_columns`` list
    is one single-column group per column, named after it. For each group,
    every iterate input is sliced to the group's columns and passed to fn;
    non-iterate inputs/constants in ``base_kwargs`` pass through unchanged.
    A one-column group arrives as ``column_value`` gives it (the cell for a
    one-row combo, else a numpy array); several columns arrive as a
    DataFrame of those columns, exactly as ``MyVar[[...]]`` gives them. An
    iterate input named in ``as_table_set`` arrives as a DataFrame of all
    schema key columns plus the group's columns. Deferred ``ColName()`` and
    PathOutput ``{ColName}`` resolve to the group's name.

    **No data:** a group whose every value, in every row and column, is
    missing in every iterate input (``scifor.missing``) is not called.

    **COMBINE** (one output) returns ONE row: each group's return expanded by
    ``_expand_column_result`` under the group's name (scalar -> ``<name>``;
    dict / Series / 1-row frame -> ``<name><sep><key>``), ordered; a group
    with no data is left out of the row (missing in the result table, and
    tallied in ``no_data_groups``; ``_combine_output_columns`` puts the
    column back, empty, so every row has the same columns). The names each
    group produced accumulate in ``group_columns``. Any group raising fails
    the combination
    with one ``ColumnFunctionError`` naming every failing group.

    **SPLIT** (several outputs) returns a tuple with one slot per group,
    each holding that group's return as-is (any shape). A group with no data
    or one fn raised on is a ``NoOutput`` slot, so the other groups still
    save. A combination where every group is skipped raises instead:
    ``NoDataError`` when all had no data, else ``ColumnFunctionError``.
    """
    import pandas as pd

    ordered: list[tuple[str, Any]] = []
    slots: list[Any] = []
    # Per-group failures, collected across the whole loop so we can report
    # every offending group at once instead of dying on the first one.
    failures: list[tuple[str, str]] = []
    first_error: "Exception | None" = None
    skipped_no_data: list[str] = []
    # Constant inputs that are deferred ColName() markers resolve to the name of
    # the group currently being iterated (recomputed each pass below).
    deferred_colname_params = [
        name
        for name, v in base_kwargs.items()
        if isinstance(v, ColName) and v.is_deferred
    ]
    # PathOutput constants resolve per-group (current combo metadata + name).
    path_output_params = [
        name for name, v in base_kwargs.items() if isinstance(v, PathOutput)
    ]
    # PathInput constants have no per-column token (unlike PathOutput's
    # {ColName}) -- the same path applies to every group in this combo, so
    # resolve once up front rather than inside the loop.
    base_kwargs = _resolve_path_inputs(base_kwargs, metadata, resolver=path_input_resolver)
    for group_name, group_cols in column_groups:
        if iterate_dfs and all(
            frame_has_no_data(df[group_cols]) for df in iterate_dfs.values()
        ):
            Log.debug(
                "[for_columns] %s: group %s has no data (all %d column(s) "
                "missing) — not called",
                metadata_str,
                group_name,
                len(group_cols),
                layer="scifor",
            )
            skipped_no_data.append(group_name)
            slots.append(NoOutput("no_data"))
            continue
        call_kwargs = dict(base_kwargs)
        for name in deferred_colname_params:
            call_kwargs[name] = group_name
        for name in path_output_params:
            # PathOutput resolves to a finished path using this combo's metadata
            # and the current group's name ({ColName}).
            call_kwargs[name] = base_kwargs[name].resolve(metadata, group_name)
        for name, df in iterate_dfs.items():
            if name in as_table_set:
                # Keep the schema keys + the group's columns.
                keep = [c for c in df.columns if c in schema_keys] + list(group_cols)
                call_kwargs[name] = df[keep]
            elif len(group_cols) == 1:
                call_kwargs[name] = column_value(df[group_cols[0]])
            else:
                call_kwargs[name] = df[list(group_cols)]
        try:
            res = call_user_function(fn, **call_kwargs)
        except Exception as e:
            # Record and keep going so the raised error names every bad group.
            failures.append((group_name, f"{type(e).__name__}: {e}"))
            if first_error is None:
                # Chained as __cause__ so the failure record finds the
                # user line (scifor.failures.failure_from_exception).
                first_error = e
            if mode == SPLIT:
                Log.warn(
                    f"[for_columns] {metadata_str}: {group_name} not saved — "
                    f"{type(e).__name__}: {e}",
                    layer="scifor",
                )
            slots.append(NoOutput("failed", f"{type(e).__name__}: {e}"))
            continue
        if isinstance(res, tuple):
            # Each group is ONE output; a tuple return is collapsed to its
            # first element. Return a dict/Series/1-row frame to emit
            # multiple named values per group.
            res = res[0]
        slots.append(res)
        if mode != SPLIT:
            expanded = _expand_column_result(group_name, res)
            ordered.extend(expanded)
            if group_columns is not None:
                seen = group_columns.setdefault(group_name, [])
                seen.extend(n for n, _ in expanded if n not in seen)

    fn_name = getattr(fn, "__name__", repr(fn))
    every_column = all(len(cols) == 1 for _, cols in column_groups)
    unit = "column" if every_column else "group"

    def column_function_error() -> ColumnFunctionError:
        detail = "\n".join(f"  - {name}: {msg}" for name, msg in failures)
        message = (
            f"for_columns: {fn_name} raised on {len(failures)} of "
            f"{len(column_groups)} iterated {unit}(s). These {unit}s could not "
            f"be processed:\n{detail}\n"
        )
        if every_column:
            message += (
                f"Each call gets one column of this combo: its value when the combo "
                f"has one row, else a numpy array of the column. Schema keys are "
                f"already excluded; to skip columns the function cannot handle, "
                f"narrow the selection (ColumnSelection(df, columns=[...]) or "
                f"excl_columns=[...])."
            )
        else:
            message += (
                "Each call gets one group of this combo: a one-column group as "
                "its value (one row) or numpy array, a larger group as a "
                "DataFrame of its columns."
            )
        return ColumnFunctionError(message, failures)

    if mode == SPLIT:
        if all(is_no_output(s) for s in slots):
            if failures:
                raise column_function_error() from first_error
            raise NoDataError(
                f"No data in any for_columns group ({', '.join(skipped_no_data)})."
            )
        return tuple(slots)

    if failures:
        raise column_function_error() from first_error
    if skipped_no_data and len(skipped_no_data) == len(column_groups):
        raise NoDataError(
            f"No data in any for_columns {unit} ({', '.join(skipped_no_data)})."
        )
    if no_data_groups is not None:
        for group_name in skipped_no_data:
            no_data_groups.setdefault(group_name, []).append(metadata_str)

    # Assemble a one-row DataFrame, preserving encounter order and rejecting
    # collisions (two produced names mapping to the same output column).
    data: dict[str, list] = {}
    for out_name, value in ordered:
        if out_name in data:
            raise ForColumnsError(
                f"for_columns produced a duplicate output column '{out_name}'. "
                f"Per-column output keys must be unique after prefixing with the "
                f"source column name (separator '{FOR_COLUMNS_OUTPUT_SEP}')."
            )
        data[out_name] = [value]
    return pd.DataFrame(data)


def _expand_column_result(col: str, res: Any) -> "list[tuple[str, Any]]":
    """Expand one source column's return into (output_name, value) pairs.

    - scalar (or any non-mapping / non-frame value) -> ``[(col, res)]``
    - dict / pandas Series -> one pair per item, named ``"<col><sep><key>"``
    - 1-row pandas DataFrame -> one pair per column, ``"<col><sep><column>"``

    A multi-row DataFrame is rejected: for_columns reassembles to a single row
    per combo, so each source column must collapse to scalar output value(s).
    """
    import pandas as pd

    sep = FOR_COLUMNS_OUTPUT_SEP
    if isinstance(res, dict):
        return [(f"{col}{sep}{k}", v) for k, v in res.items()]
    if isinstance(res, pd.Series):
        return [(f"{col}{sep}{k}", v) for k, v in res.items()]
    if isinstance(res, pd.DataFrame):
        if len(res) != 1:
            raise ForColumnsError(
                f"for_columns function returned a {len(res)}-row DataFrame for "
                f"column '{col}'; expected a single row (one value per output "
                f"key). Return a scalar, dict, pandas Series, or 1-row DataFrame."
            )
        return [(f"{col}{sep}{c}", res[c].iloc[0]) for c in res.columns]
    return [(col, res)]


# ---------------------------------------------------------------------------
# Input classification
# ---------------------------------------------------------------------------


def _warn_repeated_static_path_inputs(
    fn_name: str, inputs: dict, total: int, iterated_keys: list
) -> bool:
    """WARN when every call of this run is the same call: more than one
    iteration, yet no input varies with the combo — each is a PathInput with
    no template placeholder, or a constant. Each iteration then reads the
    same file(s); the only difference between calls is the address the
    result is saved under.

    scistack.log run n1irqety (2026-09-29): ``pandas.read_csv`` on a
    placeholder-free demographics CSV iterated per subject, read the whole
    file 18 times, and its ``subject`` column was dropped as pinned. Returns
    whether it warned (for tests).
    """
    if total <= 1:
        return False
    static_pis = []
    for spec in inputs.values():
        if isinstance(spec, PathInput):
            if spec.placeholder_keys():
                return False
            static_pis.append(spec)
        elif _is_data_input(spec) or isinstance(spec, (EachOf, PathOutput)):
            return False
    if not static_pis:
        return False
    Log.warn(
        f"for_each({fn_name}): {total} iterations over {iterated_keys} but no "
        f"input varies with them — PathInput(s) "
        f"{[pi.path_template for pi in static_pis]} have no template "
        f"placeholder, so every iteration reads the same file(s). Unless the "
        f"function uses the combo's metadata, iterate nothing (one call) and "
        f"return a schema-key column to file its rows separately.",
        layer="scifor",
    )
    return True


def _is_data_input(var_spec: Any) -> bool:
    """Check if an input is a data input (DataFrame, Fixed, Merge, ColumnSelection)."""
    if _is_dataframe(var_spec):
        return True
    if isinstance(var_spec, (Fixed, Merge, ColumnSelection)):
        return True
    return False


def _is_dataframe(value: Any) -> bool:
    """Return True if value is a pandas DataFrame."""
    try:
        import pandas as pd

        return isinstance(value, pd.DataFrame)
    except ImportError:
        return False


def _input_is_empty(value: Any) -> bool:
    """Return True if a prepared per-combo input carries zero rows.

    Used only for logging — a DataFrame with no rows, or a sized container
    (array/list/Series) of length 0. Scalars and unsized values are never
    considered empty.
    """
    if _is_dataframe(value):
        return value.empty
    if value is None:
        return False
    try:
        return len(value) == 0
    except TypeError:
        return False


# ---------------------------------------------------------------------------
# ColName resolution
# ---------------------------------------------------------------------------


def _resolve_colnames(inputs: dict[str, Any], schema_keys: list[str]) -> dict[str, Any]:
    """Replace static ColName(df) wrappers with the resolved column name string.

    For each static ColName(df) in inputs:
    1. Get the inner DataFrame
    2. Compute data_cols = columns not in schema_keys
    3. If exactly 1 data column -> replace with the string name
    4. Otherwise -> raise ValueError

    Deferred (no-arg) ColName() markers are passed through unchanged — they are
    resolved per-column during for_columns iteration (_run_column_iteration).
    """
    resolved = {}
    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, ColName) and var_spec.is_deferred:
            # No-arg ColName() — leave in place; resolved per-column during
            # for_columns iteration (see _run_column_iteration).
            resolved[param_name] = var_spec
        elif isinstance(var_spec, ColName):
            df = var_spec.data
            if not _is_dataframe(df):
                raise TypeError(
                    f"ColName({param_name}) expected a DataFrame, "
                    f"got {type(df).__name__}"
                )
            data_cols = [c for c in df.columns if c not in schema_keys]
            if len(data_cols) == 1:
                resolved[param_name] = data_cols[0]
            elif len(data_cols) == 0:
                raise ValueError(
                    f"ColName({param_name}): DataFrame has no data columns "
                    f"(all columns are schema keys). "
                    f"Columns: {list(df.columns)}, schema keys: {schema_keys}"
                )
            else:
                raise ValueError(
                    f"ColName({param_name}): DataFrame has {len(data_cols)} "
                    f"data columns ({data_cols}), expected exactly 1. "
                    f"Schema keys: {schema_keys}"
                )
        else:
            resolved[param_name] = var_spec
    return resolved


# ---------------------------------------------------------------------------
# DataFrame filtering
# ---------------------------------------------------------------------------


def _is_per_combo_df(df: "pd.DataFrame", schema_keys: list[str]) -> bool:
    """True if df has at least one column that is a schema key."""
    return bool(set(df.columns) & set(schema_keys))


def _accepted_param_names(fn) -> "set[str] | None":
    """Return the set of keyword names ``fn`` accepts, or None if it takes **kwargs.

    None means "inject anything" (the function has a ``**kwargs`` catch-all).
    Read through :func:`scifor.signature.function_arguments` (the one owner),
    which falls back to ``__scidb_params__`` when the signature can't be
    introspected.
    """
    import inspect

    from .signature import function_arguments

    args = function_arguments(fn)
    if args is None:
        return set()
    if any(a.kind == inspect.Parameter.VAR_KEYWORD for a in args):
        return None  # **kwargs — accepts any keyword
    return {a.name for a in args if a.named}


def _numeric_extent(df: "pd.DataFrame") -> "tuple[float, float] | tuple[None, None]":
    """Return (min, max) over all numeric values in df, flattening array cells.

    Handles both scalar-valued cells and ndarray/list-valued cells (timeseries).
    Returns (None, None) when no finite numeric values are present.
    """
    import numpy as np

    lo = None
    hi = None
    for col in df.columns:
        for val in df[col].to_numpy():
            arr = (
                np.asarray(val, dtype="float64").ravel()
                if not np.isscalar(val)
                else np.asarray([val], dtype="float64")
            )
            arr = arr[np.isfinite(arr)]
            if arr.size == 0:
                continue
            cmn = float(arr.min())
            cmx = float(arr.max())
            lo = cmn if lo is None else min(lo, cmn)
            hi = cmx if hi is None else max(hi, cmx)
    if lo is None:
        return (None, None)
    return (lo, hi)


def _compute_shared_limits(
    share_limits: dict, data_inputs: dict, schema_keys: list[str]
) -> dict:
    """Compute per-group numeric extents for each input named in share_limits.

    Returns ``{input_name: (group_keys_present, {group_key_tuple: (min, max)})}``
    where ``group_keys_present`` is the subset of the requested held-fixed schema
    keys actually present in the input's DataFrame, and each group spans all rows
    sharing those key values (i.e. across every other iterated key).
    """
    import pandas as pd

    result: dict = {}
    for input_name, group_keys in share_limits.items():
        var_spec = data_inputs.get(input_name)
        if var_spec is None:
            continue
        df, _eff_meta, column_selection = _resolve_data_spec(var_spec, {})
        if not isinstance(df, pd.DataFrame):
            continue
        data_cols = [
            c
            for c in df.columns
            if c not in schema_keys and not str(c).startswith("__")
        ]
        if column_selection:
            data_cols = [c for c in data_cols if c in column_selection]
        if not data_cols:
            continue

        present_group_keys = [k for k in group_keys if k in df.columns]
        limits: dict = {}
        if present_group_keys:
            for gvals, gdf in df.groupby(present_group_keys, sort=False):
                key = gvals if isinstance(gvals, tuple) else (gvals,)
                gkey = tuple(str(v) for v in key)
                mn, mx = _numeric_extent(gdf[data_cols])
                if mn is not None:
                    limits[gkey] = (mn, mx)
        else:
            mn, mx = _numeric_extent(df[data_cols])
            if mn is not None:
                limits[()] = (mn, mx)
        result[input_name] = (present_group_keys, limits)
    return result


def _filter_df_for_combo(
    df: "pd.DataFrame", metadata: dict, schema_keys: list[str]
) -> "pd.DataFrame":
    """Filter df rows to match combo metadata for schema key columns present
    in df, by THE key-match rule (``scifor.keys``; spec
    docs/claude/key-match-cases.json): numeric columns parse text values,
    text columns compare exact text, missing cells match nothing."""
    import numpy as np

    mask = np.ones(len(df), dtype=bool)
    for key in schema_keys:
        if key in df.columns and key in metadata:
            col_mask = series_mask(df[key], metadata[key])
            if len(df) and not col_mask.any():
                warn_if_type_mismatch(key, df[key], metadata[key])
            mask &= col_mask
    return df[mask].reset_index(drop=True)


def _apply_where_filter(df: "pd.DataFrame", where) -> "pd.DataFrame":
    """Apply a scifor Col filter to a DataFrame."""
    if where is None:
        return df
    mask = where.apply(df)
    return df[mask].reset_index(drop=True)


def _extract_data(
    df: "pd.DataFrame",
    schema_keys: list[str],
    as_table: bool,
    mapping_cols: "list[str] | None" = None,
) -> Any:
    """Extract data from a filtered DataFrame.

    If as_table: return the full DataFrame (schema columns + data columns).
    Otherwise: drop schema key columns; if 1 row + 1 data col -> extract scalar.

    ``mapping_cols`` (see ``LoopHooks.mapping_inputs``) says the rows are
    dict records spread one column per key. A single row is then rebuilt into
    that dict, which is the value the caller saved. Without it a dict-valued
    variable came back as a one-row frame, because the 1-row unwrap below only
    fires for a *single* data column and a dict has as many columns as keys.
    """
    if as_table:
        return df

    if mapping_cols and len(df) == 1:
        present = [c for c in mapping_cols if c in df.columns]
        if present:
            Log.debug(
                f"_extract_data: rebuilding dict-valued record from "
                f"{len(present)} column(s)",
                layer="scifor",
            )
            return {c: df[c].iloc[0] for c in present}
        # Fall through: the selection/rename upstream left none of the declared
        # columns, so treat it as an ordinary frame rather than an empty dict.

    data_cols = [c for c in df.columns if c not in schema_keys]
    if len(df) == 1 and len(data_cols) == 1:
        return df[data_cols[0]].iloc[0]
    if len(data_cols) == 1:
        # Single data column, multiple rows → 2D numpy column vector
        return df[data_cols].reset_index(drop=True).values
    if data_cols and set(data_cols) != set(df.columns):
        return df[data_cols].reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Input preparation per combo
# ---------------------------------------------------------------------------


def _prepare_input(
    var_spec: Any,
    metadata: dict,
    schema_keys: list[str],
    as_table: bool,
    where=None,
    mapping_cols: "list[str] | None" = None,
    select_rows=None,
    param_name: str = "",
) -> Any:
    """Prepare a single data input for the current combo.

    ``mapping_cols`` marks the input as dict-valued (see
    ``LoopHooks.mapping_inputs``); it only reaches ``_extract_data``, so a
    column selection or ``as_table`` still wins. ``select_rows`` is
    ``LoopHooks.select_rows``, applied to ``param_name``'s frame right after
    the schema filter.
    """
    if isinstance(var_spec, Merge):
        return _prepare_merge(var_spec, metadata, schema_keys, where)

    if isinstance(var_spec, Fixed) and isinstance(var_spec.data, Merge):
        raise TypeError(
            "Fixed cannot wrap a Merge. Use Fixed on individual "
            "constituents inside the Merge instead: "
            "Merge(Fixed(df1, ...), df2)"
        )

    # Resolve the raw DataFrame and effective metadata
    df, effective_metadata, column_selection = _resolve_data_spec(var_spec, metadata)

    excl = _excluded_columns(var_spec)

    if not _is_per_combo_df(df, schema_keys):
        # Constant DataFrame — pass unchanged every iteration
        if column_selection is not None:
            # An empty selection means "all data columns".
            cols = column_selection or _all_data_columns(df, schema_keys)
            cols = _apply_exclusions(cols, excl)
            return _apply_column_selection(df, cols)
        return df

    filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
    if select_rows is not None:
        filtered = select_rows(param_name, filtered, metadata)
    filtered = _apply_where_filter(filtered, where)

    # No matching rows -> skip this combo (unless as_table, where an empty
    # table is valid output).
    if not as_table and len(filtered) == 0:
        raise NoDataError("No data for this combo after filtering.")

    if column_selection is not None:
        # An empty selection means "all data columns".
        cols = column_selection or _all_data_columns(filtered, schema_keys)
        cols = _apply_exclusions(cols, excl)
        if as_table:
            # Keep the schema columns alongside the selected data columns.
            keep = [c for c in filtered.columns if c in schema_keys] + cols
            return filtered[keep]
        return _apply_column_selection(filtered, cols)

    return _extract_data(filtered, schema_keys, as_table, mapping_cols=mapping_cols)


def _resolve_data_spec(
    var_spec: Any, metadata: dict
) -> tuple["pd.DataFrame", dict, list[str] | None]:
    """Resolve a var_spec into (DataFrame, effective_metadata, column_selection)."""
    column_selection = None

    if isinstance(var_spec, Fixed):
        effective_metadata = {**metadata, **var_spec.fixed_metadata}
        inner = var_spec.data
        if isinstance(inner, ColumnSelection):
            column_selection = inner.columns
            df = inner.data
        else:
            df = inner
    elif isinstance(var_spec, ColumnSelection):
        effective_metadata = metadata
        column_selection = var_spec.columns
        df = var_spec.data
    else:
        # Plain DataFrame
        effective_metadata = metadata
        df = var_spec

    return df, effective_metadata, column_selection


def _unwrap_column_selection(var_spec: Any) -> Any:
    """Return the ColumnSelection inside a spec (bare or Fixed-wrapped), else the spec."""
    if isinstance(var_spec, Fixed) and isinstance(var_spec.data, ColumnSelection):
        return var_spec.data
    return var_spec


def _excluded_columns(var_spec: Any) -> list[str]:
    """Return the ColumnSelection's ``excl_columns`` (bare or Fixed-wrapped)."""
    cs = _unwrap_column_selection(var_spec)
    if isinstance(cs, ColumnSelection):
        return cs.excl_columns
    return []


def _apply_exclusions(cols: list[str], excl: list[str]) -> list[str]:
    """Drop ``excl`` names from ``cols``, preserving order. No-op if ``excl`` empty."""
    if not excl:
        return cols
    excl_set = set(excl)
    return [c for c in cols if c not in excl_set]


def _all_data_columns(df: "pd.DataFrame", schema_keys: list[str]) -> list[str]:
    """Return a DataFrame's data columns: everything that is not a schema key
    or an internal ``__*`` column. Used to expand an empty (all-columns)
    ColumnSelection to a concrete list."""
    return [
        c for c in df.columns if c not in schema_keys and not str(c).startswith("__")
    ]


def _resolve_iterate_columns(var_spec: Any, schema_keys: list[str]) -> list[str]:
    """Resolve an iterate-mode ColumnSelection's column list.

    An explicit list is returned as-is; an empty list (the all-columns
    sentinel) is expanded to every data column of the underlying DataFrame.
    Any ``excl_columns`` are removed from whichever list is produced.
    """
    cs = _unwrap_column_selection(var_spec)
    excl = _excluded_columns(var_spec)
    cols = list(cs.columns)
    if cols:
        return _apply_exclusions(cols, excl)
    df = cs.data if isinstance(cs, ColumnSelection) else None
    if df is None or not _is_dataframe(df):
        raise ValueError(
            "for_columns(): cannot resolve all columns — the input is not a "
            "DataFrame-backed ColumnSelection. Pass an explicit column list."
        )
    resolved = _apply_exclusions(_all_data_columns(df, schema_keys), excl)
    if not resolved:
        raise ValueError(
            f"for_columns(): no data columns found to iterate over "
            f"(columns were {list(df.columns)}, schema keys {schema_keys}, "
            f"excluded {excl})."
        )
    return resolved


def _prepare_iterate_df(
    var_spec: Any,
    metadata: dict,
    schema_keys: list[str],
    where=None,
    select_rows=None,
    param_name: str = "",
) -> "pd.DataFrame":
    """Prepare the per-combo DataFrame for an iterate-mode ColumnSelection.

    Returns the combo-filtered DataFrame retaining the iterate columns so the
    caller can slice one column at a time. Unlike ``_prepare_input`` it does
    not collapse to a single column. ``select_rows`` / ``param_name``: see
    ``_prepare_input``.
    """
    df, effective_metadata, _column_selection = _resolve_data_spec(var_spec, metadata)
    if not _is_per_combo_df(df, schema_keys):
        return df
    filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
    if select_rows is not None:
        filtered = select_rows(param_name, filtered, metadata)
    filtered = _apply_where_filter(filtered, where)
    if len(filtered) == 0:
        raise NoDataError("No data for this combo after filtering.")
    return filtered


def _apply_column_selection(df: "pd.DataFrame", columns: list[str]) -> Any:
    """Extract selected columns from a DataFrame."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise KeyError(
            f"Column(s) {missing} not found. Available columns: {list(df.columns)}"
        )
    if len(columns) == 1:
        return column_value(df[columns[0]])
    return df[columns]


# ---------------------------------------------------------------------------
# Merge handling
# ---------------------------------------------------------------------------


def _prepare_merge(
    merge_spec: Merge,
    metadata: dict,
    schema_keys: list[str],
    where=None,
) -> "pd.DataFrame":
    """Filter each constituent of a Merge and combine them by THE Merge rule
    (``scifor.merge.merge_frames``; spec: docs/claude/merge-cases.json).

    Each per-combo constituent is filtered to the combo and drops its
    constant schema-key columns (varying ones become join keys); ``where=``
    applies once to the merged frame, so it may name a column from any
    constituent (the flat-table exports and MATLAB do the same).
    """
    import pandas as pd

    parts = []
    labels = []

    for i, spec in enumerate(merge_spec.tables):
        label = f"merge[{i}]"

        df, effective_metadata, column_selection = _resolve_data_spec(spec, metadata)

        if _is_per_combo_df(df, schema_keys):
            filtered = _filter_df_for_combo(df, effective_metadata, schema_keys)
            if len(filtered) == 0:
                # A sparse grid, as for any other per-combo input: an expected
                # skip, never a NaN-padded merge.
                raise NoDataError(f"No data for this combo in {label}.")
            part_df, _dropped = drop_constant_keys(filtered, schema_keys)
        else:
            part_df = df.reset_index(drop=True)

        if column_selection is not None:
            missing = [c for c in column_selection if c not in part_df.columns]
            if missing:
                raise KeyError(
                    f"Column(s) {missing} not found in {label}. "
                    f"Available: {list(part_df.columns)}"
                )
            if len(column_selection) == 1:
                part_df = pd.DataFrame(
                    {column_selection[0]: part_df[column_selection[0]]}
                )
            else:
                part_df = part_df[column_selection]

        parts.append(part_df)
        labels.append(label)

    merged = _apply_where_filter(merge_frames(parts, schema_keys, labels=labels), where)
    if len(merged) == 0:
        raise NoDataError("No data for this combo after merge.")
    return merged


# ---------------------------------------------------------------------------
# Empty-list resolution from DataFrame inputs
# ---------------------------------------------------------------------------


def _distinct_values_from_inputs(inputs: dict, key: str) -> list:
    """Find distinct values for `key` by scanning DataFrame inputs.

    Non-raising: a PathInput may still supply the key via filesystem
    discovery (handled by the caller), which decides whether an
    unresolved key warns, is dropped, or errors.
    """
    all_values = set()
    for _param_name, var_spec in inputs.items():
        df = _get_raw_df(var_spec)
        if df is not None and key in df.columns:
            all_values.update(df[key].dropna().unique().tolist())
    if not all_values:
        return []
    try:
        return sorted(all_values)
    except TypeError:
        return list(all_values)


def _find_pathinput(inputs: dict) -> "PathInput | None":
    """Return the first bare PathInput in inputs, or None.

    Bare only — ``Fixed(PathInput(...))`` is a scidb-only pattern (scifor's
    ``Fixed`` wraps DataFrames, not loadable specs).
    """
    for var_spec in inputs.values():
        if isinstance(var_spec, PathInput):
            return var_spec
    return None


def resolve_pathinput_discovery(
    pi: "PathInput | None",
    metadata_iterables: dict,
    user_explicit_keys: "set | None" = None,
    log=None,
    condense_numeric: bool = False,
    unsupplyable_keys: "set | None" = None,
    key_types: "dict[str, str] | None" = None,
) -> "tuple[dict, list[dict] | None]":
    """Fill empty metadata iterables from PathInput filesystem discovery,
    then drop any key a fully static PathInput (no ``{key}`` placeholders)
    can never supply, instead of leaving it as an empty list that would
    zero out the Cartesian product.

    Shared by scifor's own standalone empty-list resolution and scidb's
    DB-backed ``for_each`` (which calls this after its own DB-based
    resolution leaves some keys still empty), so both layers make the
    same discovery/leniency decision.

    Args:
        pi: The PathInput to discover against, or None if no PathInput is
            present (metadata_iterables is returned unchanged, combos=None).
        metadata_iterables: Mutable mapping of key -> list of values.
        user_explicit_keys: Keys the caller passed with explicit non-empty
            values (not delegated to resolution).
        log: Optional ``log(msg)`` callback.
        condense_numeric: Forwarded to ``PathInput.apply_discovery`` — see
            there. Defaults to False so scidb's declared-only
            ``schema_key_types`` contract is unaffected; the standalone
            scifor call site opts in explicitly.
        key_types: Declared schema-key types, forwarded to
            ``PathInput.apply_discovery`` (``scifor.schema_types``).
        unsupplyable_keys: Keys that provably have NO source — neither the
            caller, nor the DB, nor this template can fill them — and which
            the caller is therefore going to drop from the iteration anyway.
            They are excluded from the "every iterated key is a placeholder"
            test below, so a key that is about to disappear cannot force the
            Cartesian-product fallback on its way out. Membership must be
            decidable WITHOUT walking the disk (the caller knows what its own
            resolution failed to fill; ``pi.placeholder_keys()`` says what this
            template could ever supply), which is why this is an input rather
            than something computed here after discovery.

    Returns:
        ``(metadata_iterables, discovered_combos | None)``.
    """
    if pi is None:
        return metadata_iterables, None
    metadata_iterables, discovered_combos = pi.apply_discovery(
        metadata_iterables,
        user_explicit_keys,
        log=log,
        condense_numeric=condense_numeric,
        key_types=key_types,
    )
    placeholder_keys = set(pi.placeholder_keys())
    if any(isinstance(v, list) and len(v) == 0 for v in metadata_iterables.values()):
        pi_is_static = not placeholder_keys
        if pi_is_static:
            for key in list(metadata_iterables.keys()):
                v = metadata_iterables[key]
                if isinstance(v, list) and len(v) == 0:
                    if log is not None:
                        log(
                            f"'{key}' has no source and PathInput "
                            f"{pi.path_template!r} has no template placeholders; "
                            f"ignoring '{key}' (treating it as if it were never "
                            f"requested)"
                        )
                    del metadata_iterables[key]

    # Discovered combos only drive iteration directly when every iterated key
    # is one of this PathInput's own template placeholders -- otherwise a
    # Cartesian product with keys resolved from elsewhere (tables/DB) is
    # still required, and using the bare discovered combos would silently
    # drop that other dimension's iteration. (metadata_iterables has already
    # been filled per-key above, so the Cartesian-product fallback still
    # picks up the disk-discovered values either way.)
    #
    # ``unsupplyable_keys`` is subtracted first. A key nothing can fill is not a
    # real iteration dimension -- the caller drops it moments later -- but while
    # it is still in the dict it used to fail this subset test and force the
    # Cartesian product. That is how a 5-key schema whose template supplies 4
    # turned 419 real files into 952 combos, 533 of which could only fail on a
    # missing file (2026-09-13; see
    # .claude/matlab-run-timing-and-phantom-combos-plan.md). The set is decided
    # before discovery runs, so a key on its way out no longer votes.
    _iterated_keys = set(metadata_iterables.keys()) - set(unsupplyable_keys or ())
    if discovered_combos is not None and not _iterated_keys <= placeholder_keys:
        if log is not None:
            log(
                f"discovery combos dropped: iterated keys "
                f"{sorted(_iterated_keys - placeholder_keys)} are not template "
                f"placeholders of {pi.path_template!r} — Cartesian product of "
                f"iterables will drive combos"
            )
        discovered_combos = None
    return metadata_iterables, discovered_combos


def _get_raw_df(var_spec: Any) -> "pd.DataFrame | None":
    """Extract the DataFrame from a var_spec, if it contains one."""
    if _is_dataframe(var_spec):
        return var_spec
    if isinstance(var_spec, Fixed) and _is_dataframe(var_spec.data):
        return var_spec.data
    if isinstance(var_spec, ColumnSelection) and _is_dataframe(var_spec.data):
        return var_spec.data
    return None


def _key_has_source(inputs: dict, key: str, pi: "PathInput | None") -> bool:
    """True if any DataFrame column or PathInput placeholder provides key."""
    for var_spec in inputs.values():
        df = _get_raw_df(var_spec)
        if df is not None and key in df.columns:
            return True
    if pi is not None and key in pi.placeholder_keys():
        return True
    return False


def _capture_schema_column_dtypes(inputs: dict, keys: list) -> dict:
    """Record each schema-key input column's pandas dtype.

    Scans the raw DataFrame inputs and records, per key, the column dtype
    (a CategoricalDtype carries its categories and orderedness), so output
    metadata columns can round-trip as EXACTLY the input column dtype. A key
    whose input DataFrames disagree on dtype is recorded as None and left at
    the natural output dtype (warned at restore time).
    """
    dtypes: dict = {}
    conflicts: set = set()
    for param_name, var_spec in inputs.items():
        df = _get_raw_df(var_spec)
        if df is None:
            continue
        for key in keys:
            if key not in df.columns:
                continue
            dt = df[key].dtype
            if key not in dtypes:
                dtypes[key] = dt
                Log.debug(
                    "schema key '%s' (input '%s'): input column dtype %s",
                    key,
                    param_name,
                    dt,
                    layer="scifor",
                )
            elif dtypes[key] != dt:
                conflicts.add(key)
    for key in conflicts:
        dtypes[key] = None
    return dtypes


def _restore_schema_column_dtypes(
    result: "pd.DataFrame", col_dtypes: "dict | None"
) -> "pd.DataFrame":
    """Cast output metadata columns back to the captured input dtypes.

    Keys without a captured dtype (explicit iterables with no DataFrame
    column) keep the iterable's natural dtype. A cast that cannot be
    performed losslessly leaves the column unchanged and warns, so identity
    errors are visible rather than silent.
    """
    import pandas as pd

    if not col_dtypes or result.empty:
        return result
    dup_labels = list(result.columns[result.columns.duplicated()].unique())
    if dup_labels:
        # Typically a function returning its input DataFrame with the
        # metadata columns still inside, so combo metadata gets appended a
        # second time under the same label.
        Log.warn(
            "output DataFrame has DUPLICATE column label(s) %s — the "
            "function's returned DataFrame likely already contains "
            "metadata column(s) that for_each appends per combo; dtype "
            "restore skips these columns",
            dup_labels,
            layer="scifor",
        )
    for key, dtype in col_dtypes.items():
        if key not in result.columns:
            continue
        col = result[key]
        if isinstance(col, pd.DataFrame):  # duplicated label — warned above
            continue
        if dtype is None:
            Log.warn(
                "schema key '%s': input DataFrames disagree on column "
                "dtype — leaving the output column as %s",
                key,
                col.dtype,
                layer="scifor",
            )
            continue
        if col.dtype == dtype:
            continue
        converted = None
        try:
            converted = col.astype(dtype)
            same = (converted == col) | (converted.isna() & col.isna())
            lossless = bool(same.all())
        except (ValueError, TypeError):
            lossless = False
        if not lossless and isinstance(dtype, pd.CategoricalDtype):
            # Values outside the captured category set (e.g. distribute
            # indices): fall back to a plain categorical of the values,
            # mirroring the MATLAB categorical(col) fallback.
            converted = col.astype("category")
            lossless = True
        if not lossless:
            Log.warn(
                "schema key '%s': cannot restore input column dtype %s "
                "losslessly — leaving as %s",
                key,
                dtype,
                col.dtype,
                layer="scifor",
            )
            continue
        result[key] = converted
        Log.debug(
            "schema key '%s': output column restored to input dtype %s",
            key,
            dtype,
            layer="scifor",
        )
    return result


# ---------------------------------------------------------------------------
# Result collection
# ---------------------------------------------------------------------------


def _spread_decision(
    collected_rows: list[tuple[dict, tuple]],
    schema_keys: "list | tuple",
) -> tuple[bool, list[str], list[str], int]:
    """Should a DataFrame return value's ROWS become separate records?

    Returns ``(spread, discriminating_keys, pinned_collisions, max_rows)``.

    **Rows spread unless spreading would silently multiply records at one
    address.** A record's address is its schema keys, so rows spread when:

    1. the DataFrame carries a schema key the combination does NOT already
       pin — each row then addresses its own location, and scidb files it
       there (it reads every non-``__`` column of the row as that record's
       metadata). This is what the spread is FOR: a function iterating
       ``subject`` alone returning one row per ``session``; **or**
    2. there is at most one row — spreading cannot multiply anything, so the
       wide row simply becomes the result table's columns. This is the
       ``for_columns`` shape (one reassembled ``1 x N`` row per combo) and
       every ``distribute`` piece (``df.iloc[[i]]``), both of which depend on
       that spread and produce exactly one record per combo either way.

    Everything else — a MULTI-row table with nothing to distinguish where its
    rows go — is one record per combination. That is the only case whose
    behavior changes, and it is the bug: one 322-row CSV became 322 records
    at a single ``(subject, session)``, distinguishable only by their
    contents.

    ``distribute=True`` needs no clause here: it splits upstream of result
    collection and stamps each piece with its own ``distribute_key``, so the
    pieces arrive as pinned single rows and take condition 2.
    """
    real_keys = list(schema_keys or [])
    discriminating: set[str] = set()
    collisions: set[str] = set()
    combos_with: int = 0
    max_rows = 0
    for metadata, result_tuple in collected_rows:
        pinned = set(metadata.keys())
        row_disc: set[str] = set()
        for value in result_tuple:
            cols = {str(c) for c in value.columns}
            max_rows = max(max_rows, len(value))
            row_disc |= {k for k in real_keys if k in cols and k not in pinned}
            collisions |= {k for k in real_keys if k in cols and k in pinned}
        if row_disc:
            combos_with += 1
        discriminating |= row_disc

    if discriminating and combos_with != len(collected_rows):
        # Some combos supply the finer address and others don't, so the same
        # output would be filed at two different granularities. Spread (the
        # rows that DO carry the key must reach their own locations) and say
        # so — this is an authoring bug in the function, not a config choice.
        Log.warn(
            "inconsistent output addressing: %d of %d combination(s) return "
            "schema key(s) %s, the rest do not — the records will not all be "
            "filed at the same granularity",
            combos_with,
            len(collected_rows),
            sorted(discriminating),
            layer="scifor",
        )
    spread = bool(discriminating) or max_rows <= 1
    return spread, sorted(discriminating), sorted(collisions), max_rows


def _crowded_addresses(
    collected_rows: list[tuple[dict, tuple]],
    disc_keys: list[str],
    schema_keys: "list | tuple",
) -> tuple[int, int, list[str]]:
    """After a spread, how many addresses still receive SEVERAL rows.

    Returns ``(crowded, max_per_address, absent_keys)``: the number of
    ``(combination, discriminating-key values)`` addresses that more than one
    row lands at, the most rows any one of them gets, and the unpinned schema
    keys no returned DataFrame carries -- the usual reason. A spread by
    ``brand`` alone of a 300-row ratings table whose person column is named
    ``participant`` files 100 records at each brand (scistack.log 2026-10-17);
    each is distinguishable only by its contents, the same silent
    multiplication :func:`_spread_decision` refuses for a table with NO
    discriminating key.
    """
    crowded = 0
    max_per = 0
    seen_cols: set[str] = set()
    pinned_any: set[str] = set()
    for metadata, result_tuple in collected_rows:
        pinned_any |= set(metadata.keys())
        for value in result_tuple:
            cols = [k for k in disc_keys if k in value.columns]
            seen_cols |= {str(c) for c in value.columns}
            if not cols or len(value) <= 1:
                continue
            counts = value.groupby(cols, dropna=False).size()
            crowded += int((counts > 1).sum())
            max_per = max(max_per, int(counts.max()))
    absent = [
        k for k in (schema_keys or []) if k not in seen_cols and k not in pinned_any
    ]
    return crowded, max_per, absent


def _strip_pinned_key_columns(
    df: "pd.DataFrame", pinned: list[str]
) -> "pd.DataFrame":
    """Drop the schema-key columns in ``pinned`` from a returned DataFrame.

    See the ``collisions`` branch of :func:`_results_to_output_dataframe`: a
    pinned schema key is the record's address, and storing a copy of it as a
    data column corrupts the variable's table for every later reader.
    """
    present = [c for c in pinned if c in df.columns]
    return df.drop(columns=present) if present else df


def _describe_output_kinds(
    collected_rows: "list[tuple[dict, tuple]]", output_names: list[str], fn_name: str
) -> "str | None":
    """The first output whose values differ in kind across combos, as a
    message naming the combos (``describe_value_kinds``); ``None`` if none.
    The diagnosis of the ``combine_outputs`` seam — runs only after combining
    has failed."""
    width = max((len(result) for _meta, result in collected_rows), default=0)
    for i in range(width):
        labelled = [
            (
                ", ".join(f"{k}={v}" for k, v in meta.items()) or "the only combo",
                result[i] if i < len(result) else None,
            )
            for meta, result in collected_rows
        ]
        name = output_names[i] if i < len(output_names) else None
        message = describe_value_kinds(labelled, fn_name=fn_name, output=name)
        if message:
            return message
    return None


def _results_to_output_dataframe(
    collected_rows: list[tuple[dict, tuple]],
    output_names: list[str],
    col_dtypes: "dict | None" = None,
    schema_keys: "list | tuple" = (),
) -> "pd.DataFrame":
    """Build a combined DataFrame from all for_each results."""
    import pandas as pd

    if not collected_rows:
        return pd.DataFrame()

    # Check if all outputs are DataFrames (candidates for row spreading)
    all_dataframes = all(
        isinstance(value, pd.DataFrame)
        for _, result_tuple in collected_rows
        for value in result_tuple
    )

    spread = False
    if all_dataframes:
        spread, disc_keys, collisions, max_rows = _spread_decision(
            collected_rows, schema_keys
        )
        _out = ", ".join(str(n) for n in output_names) or "output"
        per_output = spread and len(output_names) > 1
        if per_output:
            # Spreading concatenates every output's frame into ONE row set,
            # after which nothing says which column belongs to which output:
            # scidb's flatten save then built EVERY output's record from ALL
            # the columns, and same-named columns collided (2026-10-28,
            # scidb/tests/test_multi_output_frames.py). Each output keeps its
            # own frame in its own cell instead. The save then files each
            # output's rows by that output's own key columns
            # (`spread_output_frame`), so two outputs can land at two levels.
            Log.info(
                "outputs %s: %d outputs, so each output keeps its own DataFrame "
                "(rows are not merged across outputs%s)",
                _out,
                len(output_names),
                f"; schema key(s) {disc_keys} in the frames address that "
                "output's rows on save"
                if disc_keys
                else "",
                layer="scifor",
            )
            spread = False
        if collisions:
            # A schema key is an ADDRESS, never data. The combination already
            # says where these rows go, so the returned column can add nothing
            # and must not be stored: a data column named after a schema key
            # lands in the variable's DuckDB table beside the real key, and the
            # plot loader then sees two "subject" columns and fails
            # ("The truth value of a Series is ambiguous", 2026-09-16). The
            # spread path already files rows by the combo address; this makes
            # the one-record-per-combo path drop the column the same way.
            Log.warn(
                "output %s returns schema key column(s) %s that the combination "
                "already pins — the combination's address wins and the data "
                "column(s) are dropped before saving. Rename or drop them in "
                "the function to silence this.",
                _out,
                collisions,
                layer="scifor",
            )
            collected_rows = [
                (
                    metadata,
                    tuple(
                        _strip_pinned_key_columns(df, collisions)
                        for df in result_tuple
                    ),
                )
                for metadata, result_tuple in collected_rows
            ]
        # max_rows is the LARGEST row count across the combinations, which each
        # returned their own DataFrame — quoting it bare read as though every
        # combination returned that many rows (examples/vo2max/scidb.model.log
        # reported "372-row DataFrame" for combinations of 322/372/304/366).
        if spread and disc_keys:
            Log.info(
                "output %s: %d DataFrame(s), up to %d row(s) each, discriminated "
                "by unpinned schema key(s) %s — spreading rows into separate "
                "records",
                _out,
                len(collected_rows),
                max_rows,
                disc_keys,
                layer="scifor",
            )
            crowded, max_per, absent = _crowded_addresses(
                collected_rows, disc_keys, schema_keys
            )
            if crowded:
                Log.warn(
                    "output %s: spreading by %s still files several rows at one "
                    "address — %d location(s) get up to %d record(s) each, "
                    "told apart only by their contents. %s",
                    _out,
                    disc_keys,
                    crowded,
                    max_per,
                    (
                        f"Schema key(s) {absent} are not a column of the "
                        "returned DataFrame; if one of its columns holds them "
                        "under another name, rename it to the schema key."
                        if absent
                        else "Check that the returned columns name every "
                        "schema key the rows differ by."
                    ),
                    layer="scifor",
                )
        elif not spread and not per_output:
            Log.info(
                "output %s: %d DataFrame(s), up to %d row(s) each, carry no "
                "unpinned schema-key column, so every row shares one address — "
                "saving each whole table as ONE record per combination. To file "
                "rows separately they need a finer address: return a schema-key "
                "column, or pass distribute=True to spread them one level below "
                "the deepest iterated key.",
                _out,
                len(collected_rows),
                max_rows,
                layer="scifor",
            )

    if spread:
        # Output names (outputs= strings, class names, column_name) only label
        # an output that lands in one cell. Spread rows keep the DataFrame's
        # own columns, so say when explicit names go unused.
        _explicit = [
            n for n in output_names if n != "output" and not str(n).startswith("output_")
        ]
        if _explicit:
            Log.info(
                "collect_results: output name(s) %s not applied — every output "
                "is a DataFrame and its rows spread, so the DataFrame's own "
                "columns are used",
                _explicit,
                layer="scifor",
            )
        parts = []
        for metadata, result_tuple in collected_rows:
            combined_data = pd.concat(
                [df.reset_index(drop=True) for df in result_tuple], axis=1
            )
            nr = len(combined_data)
            meta_df = pd.DataFrame({k: [v] * nr for k, v in metadata.items()})
            parts.append(
                pd.concat([meta_df.reset_index(drop=True), combined_data], axis=1)
            )
        result = pd.concat(parts, ignore_index=True)
        Log.debug(
            "collect_results (spread mode): DataFrame with %d row(s), %d column(s)",
            len(result),
            len(result.columns),
            layer="scifor",
        )
    else:
        rows = []
        for metadata, result_tuple in collected_rows:
            row = dict(metadata)
            for name, value in zip(output_names, result_tuple, strict=False):
                row[name] = value
            rows.append(row)
        result = pd.DataFrame(rows)
        Log.debug(
            "collect_results (scalar mode): DataFrame with %d row(s), %d column(s)",
            len(result),
            len(result.columns),
            layer="scifor",
        )

    # Round-trip metadata column dtypes: cast each metadata column back to
    # the exact dtype of the input column it was resolved from.
    return _restore_schema_column_dtypes(result, col_dtypes)


def spread_output_frame(
    frame: "pd.DataFrame",
    metadata: dict,
    schema_keys: "list | tuple | set",
    *,
    output_name: str = "output",
) -> "list[tuple[dict, pd.DataFrame]] | None":
    """File ONE output's rows by that output's own schema-key columns.

    The spread rule (``_spread_decision``) for a call with several outputs,
    where each output has its own level. Returns ``None`` when the frame
    carries no schema key that *metadata* leaves unpinned: it stays one record
    per combination. Otherwise returns ``(address, one-row piece)`` per row,
    with the key columns moved from the data into the address. A frame holding
    only key columns has nothing to store, so it returns ``[]``.

    A blank key raises :class:`MissingSchemaKeyError` naming the rows: dropping
    them would lose data, and filing them under a blank key would hide the bug.
    """
    pinned = {k for k, v in metadata.items() if v is not None}
    columns = {str(c) for c in frame.columns}
    keys = [k for k in schema_keys if k in columns and k not in pinned]
    if not keys:
        return None

    blank = frame[keys].isna()
    if blank.to_numpy().any():
        where = ", ".join(f"{k}={metadata[k]}" for k in schema_keys if k in pinned)
        details = "; ".join(
            f"{key}: row(s) {[int(i) for i in (blank[key].to_numpy()).nonzero()[0]][:20]}"
            for key in keys
            if blank[key].any()
        )
        raise MissingSchemaKeyError(
            f"{output_name}{f' at {where}' if where else ''}: blank schema key "
            f"value(s), so these rows have no address ({details}; 0-based rows of "
            f"the returned table, {len(frame)} row(s) in total). Nothing from this "
            f"call was saved. Give every row a value, or leave these rows out in "
            f"your function."
        )

    data = frame.drop(columns=keys)
    if data.shape[1] == 0:
        Log.warn(
            "output %s: the returned table holds only schema key column(s) %s, "
            "so there is nothing to store",
            output_name,
            keys,
            layer="scifor",
        )
        return []
    return [
        (
            {key: frame[key].iloc[i] for key in keys},
            data.iloc[[i]].reset_index(drop=True),
        )
        for i in range(len(frame))
    ]


def spread_nested_results(
    result_tbl: "pd.DataFrame",
    output_names: list[str],
    schema_keys: "list | tuple" = (),
    col_dtypes: "dict | None" = None,
) -> "pd.DataFrame":
    """Re-run for_each's result assembly on a NESTED-mode result table.

    A nested-mode table has one row per combination: the metadata columns
    plus one column per output whose cells hold that combination's raw
    return value (a DataFrame, an array, a scalar). It is what the MATLAB
    loop (``+scifor/for_each.m`` with ``_nest_table_outputs=true``) hands
    back across the bridge, and what this module's own loop holds in
    ``collected_rows`` before ``_results_to_output_dataframe``.

    This is the one owner of the spread rule (``_spread_decision``) for both
    languages: the MATLAB bridge calls it before saving so a returned table
    that carries an unpinned schema key is filed row by row exactly as the
    Python loop would file it, with the same log lines. Without it the
    MATLAB path saved a 73-row ``(subject, session)``-labelled table as ONE
    dataset-level record while Python saved 73 (2026-09-15).

    Output columns absent from ``result_tbl`` are ignored; a table with no
    output column at all (already flat) is returned unchanged.
    """
    import pandas as pd

    if result_tbl is None or not isinstance(result_tbl, pd.DataFrame):
        return result_tbl
    present = [n for n in output_names if n in result_tbl.columns]
    if not present or result_tbl.empty:
        return result_tbl
    meta_cols = [c for c in result_tbl.columns if c not in present]
    collected_rows: list[tuple[dict, tuple]] = []
    for row in result_tbl.to_dict("records"):
        metadata = {c: row[c] for c in meta_cols}
        collected_rows.append((metadata, tuple(row[n] for n in present)))
    Log.debug(
        "spread_nested_results: %d combination row(s), outputs %s, %d metadata "
        "column(s)",
        len(collected_rows),
        present,
        len(meta_cols),
        layer="scifor",
    )
    return _results_to_output_dataframe(
        collected_rows, present, col_dtypes, schema_keys
    )


# ---------------------------------------------------------------------------
# Distribute
# ---------------------------------------------------------------------------


def _distribute_pieces(
    data: Any, distribute_key: str, schema_keys: "list[str]", *, output_name: str, where: str
) -> "tuple[str, list[str], list[tuple[dict, Any]]]":
    """Split one output for ``distribute``: ``(target, carried, [(address, piece)])``.

    A DataFrame's key columns from ``distribute_key`` down are part of each
    row's address, and move out of the data. The rule is
    :mod:`scifor.distribute`. Anything else (a numpy array, a list) is numbered
    from 1 at ``distribute_key``.
    """
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            target, carried = resolve_target(
                schema_keys, distribute_key, data.columns, output_name=output_name
            )
            # None is the owner's one spelling of "blank" (NaN, NaT, pd.NA).
            columns = [
                [
                    None if blank else value
                    for value, blank in zip(
                        data[key].tolist(), data[key].isna().tolist(), strict=True
                    )
                ]
                for key in carried
            ]
            ordinals = (
                group_ordinals(carried, columns, output_name=output_name, where=where)
                if carried
                else list(range(1, len(data) + 1))
            )
            if carried:
                for group, count in multi_row_groups(carried, columns):
                    Log.info(
                        "distribute: %s %s has %d rows, filed at %s = 1..%d in table order",
                        output_name,
                        group,
                        count,
                        target,
                        count,
                        layer="scifor",
                    )
            stripped = data.drop(columns=carried)
            pieces = [
                (
                    {
                        **{key: column[i] for key, column in zip(carried, columns, strict=True)},
                        target: ordinals[i],
                    },
                    stripped.iloc[[i]],
                )
                for i in range(len(data))
            ]
            return target, carried, pieces
    except ImportError:
        pass
    pieces = [
        ({distribute_key: i}, piece)
        for i, piece in enumerate(_split_for_distribute(data), start=1)
    ]
    return distribute_key, [], pieces


def _split_for_distribute(data: Any) -> list[Any]:
    """Split data into elements for distribute-style expansion."""
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            return [data.iloc[[i]] for i in range(len(data))]
    except ImportError:
        pass

    try:
        import numpy as np

        if isinstance(data, np.ndarray):
            if data.ndim == 1:
                return [data[i] for i in range(len(data))]
            elif data.ndim == 2:
                return [data[i, :] for i in range(data.shape[0])]
            else:
                raise TypeError(
                    f"distribute does not support numpy arrays with {data.ndim} dimensions. "
                    f"Only 1D (split by element) and 2D (split by row) are supported."
                )
    except ImportError:
        pass

    if isinstance(data, list):
        return list(data)

    raise TypeError(
        f"distribute does not support type {type(data).__name__}. "
        f"Supported types: numpy 1D/2D array, list, pandas DataFrame."
    )


# ---------------------------------------------------------------------------
# Display / dry-run
# ---------------------------------------------------------------------------


def _format_inputs(inputs: dict[str, Any]) -> str:
    """Format inputs dict for display."""
    parts = []
    for name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            parts.append(f"{name}: {var_spec.__name__}")
        elif isinstance(var_spec, Fixed):
            fixed_str = ", ".join(
                f"{k}={v}" for k, v in var_spec.fixed_metadata.items()
            )
            inner = var_spec.data
            if isinstance(inner, ColumnSelection):
                inner_name = inner.__name__
            elif _is_dataframe(inner):
                inner_name = f"DataFrame{list(inner.columns)}"
            else:
                inner_name = repr(inner)
            parts.append(f"{name}: Fixed({inner_name}, {fixed_str})")
        elif isinstance(var_spec, ColumnSelection):
            parts.append(f"{name}: {var_spec.__name__}")
        elif _is_dataframe(var_spec):
            parts.append(f"{name}: DataFrame{list(var_spec.columns)}")
        elif _is_data_input(var_spec):
            parts.append(f"{name}: {repr(var_spec)}")
        else:
            parts.append(f"{name}: {var_spec!r}")
    return "{" + ", ".join(parts) + "}"


def _print_dry_run_iteration(
    inputs: dict[str, Any],
    metadata: dict[str, Any],
    constant_inputs: dict[str, Any],
    distribute: str | None = None,
) -> None:
    """Print what would happen for one iteration in dry-run mode."""
    metadata_str = ", ".join(f"{k}={v}" for k, v in metadata.items())
    print(f"[dry-run] {metadata_str}:")

    for param_name, var_spec in inputs.items():
        if isinstance(var_spec, Merge):
            print(f"  merge {param_name}:")
            for i, sub_spec in enumerate(var_spec.tables):
                _print_constituent_filter(sub_spec, metadata, i)
        elif isinstance(var_spec, Fixed):
            filter_metadata = {**metadata, **var_spec.fixed_metadata}
            inner = var_spec.data
            if isinstance(inner, ColumnSelection):
                col_str = ", ".join(inner.columns)
                print(
                    f"  filter {param_name} with {filter_metadata} -> columns: [{col_str}]"
                )
            elif _is_dataframe(inner):
                print(f"  filter {param_name} = DataFrame with {filter_metadata}")
            else:
                print(f"  filter {param_name} with {filter_metadata}")
        elif isinstance(var_spec, ColumnSelection):
            col_str = ", ".join(var_spec.columns)
            print(f"  filter {param_name} with {metadata} -> columns: [{col_str}]")
        elif _is_dataframe(var_spec):
            print(f"  filter {param_name} = DataFrame with {metadata}")
        elif _is_data_input(var_spec):
            print(f"  filter {param_name} with {metadata}")
        else:
            print(f"  constant {param_name} = {var_spec!r}")

    if distribute is not None:
        print(f"  distribute by '{distribute}' (1-based indexing)")


def _print_constituent_filter(spec: Any, metadata: dict[str, Any], index: int) -> None:
    """Print a single Merge constituent's filter line for dry-run display."""
    if isinstance(spec, Fixed):
        filter_metadata = {**metadata, **spec.fixed_metadata}
        inner = spec.data
        if isinstance(inner, ColumnSelection):
            col_str = ", ".join(inner.columns)
            print(
                f"    [{index}] filter with {filter_metadata} -> columns: [{col_str}]"
            )
        else:
            print(f"    [{index}] filter with {filter_metadata}")
    elif isinstance(spec, ColumnSelection):
        col_str = ", ".join(spec.columns)
        print(f"    [{index}] filter with {metadata} -> columns: [{col_str}]")
    else:
        print(f"    [{index}] filter with {metadata}")
