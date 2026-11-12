"""for_each: saving results — the save plan (phase 1) and applying it (phase 2).

Moved verbatim out of foreach.py (grug follow-up, docs/claude/grugbrain-principles.md).
"""

import math
import time
import warnings
from dataclasses import dataclass
from dataclasses import field
from scidb.model.input_spec import type_name
from scidb.model.log import Log
from scidb.provenance.save import GraphRecord as _GraphRecord
from scidb.run.bindings import COMBO_KEY
from scidb.run.bindings import RunBindings
from scidb.run.selection_axis import SELECTION_PARAM
from scidb.store.database import dataset_schema_keys_of
from scifor.foreach import spread_output_frame
from scifor.outputs import is_no_output as _is_no_output
from scifor.project.names import function_name as _function_name
from typing import Any

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager
    import pandas as pd

from scidb.run.foreach_common import (
    _ForEachState,
    _capture_function_source,
    _describe_save_data,
    _output_type_label,
    _report_no_output_skips,
    _save_seam_error,
)
from scidb.run.foreach_stamping import (
    _build_stamp_blob,
    _endpoint_artifact_path,
    _stamp_inputs_from_edges,
)


# ---------------------------------------------------------------------------
# Saving results
# ---------------------------------------------------------------------------


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.artifact_stamp import stamp_artifact
from scidb.model.exceptions import OutputSaveError
from scidb.provenance.graph import insert_record_entity
from scidb.provenance.save import check_selector_round_trip
from scidb.provenance.save import invocation_identity
# record_run is looked up on its module at call time (not bound here) so a test
# can patch scidb.provenance.save.record_run (test_save_provenance_failure.py,
# test_hist_foreach.py). Do not hoist it into a from-import.
import scidb.provenance.save as _provenance_save
from scidb.provenance.save import variable_input_params
from scidb.store.database import get_database
@dataclass(frozen=True)
class SaveItem:
    """One record a ``for_each`` save will write: its value, its save
    metadata (schema keys, config keys, ``__branch_params``,
    ``__invocation_id``), and the typed input edges it was computed from."""

    data: Any
    metadata: dict
    edges: Any  # tuple[GraphEdge, ...] from RunBindings.for_combo


@dataclass
class SavePlan:
    """What a ``for_each`` save will write, decided before any write
    (:func:`build_save_plan`); :func:`apply_save_plan` writes it."""

    fn_name: str
    outputs: "list[Any]"
    output_names: "list[str]"
    #: The dataset schema keys (a meta column that is one is never a
    #: dynamic discriminator, nor a flatten-path data column).
    schema_keys: set
    #: ``(output slot, "normal" | "flatten") -> items``; ``flatten`` is a
    #: DataFrame return spread over the result table's columns.
    batches: "dict[tuple[int, str], list[SaveItem]]" = field(default_factory=dict)
    #: generates_file outputs: ``(output object, output slot, item)``,
    #: lineage-only (no data row).
    generated: "list[tuple[Any, int, SaveItem]]" = field(default_factory=list)
    #: output slot -> the NoOutput reasons of the combinations it was skipped at
    no_output_reasons: "dict[int, list[str]]" = field(default_factory=dict)
    #: output name -> ``{"keys", "records"}`` for each output of a multi-output
    #: call whose rows were filed by its own key columns.
    spread_outputs: "dict[str, dict]" = field(default_factory=dict)
    n_rows: int = 0

    def add(self, output_idx: int, path: str, item: SaveItem) -> None:
        self.batches.setdefault((output_idx, path), []).append(item)


@dataclass
class SaveReport:
    """What :func:`apply_save_plan` wrote."""

    total_saved: int = 0
    graph_records: list = field(default_factory=list)
    run_id: "str | None" = None
    #: One line per write that failed, and the exception behind each.
    failures: "list[str]" = field(default_factory=list)
    causes: "list[BaseException]" = field(default_factory=list)

    def fail(self, line: str, cause: BaseException) -> None:
        self.failures.append(line)
        self.causes.append(cause)


def _save_results(
    result_tbl: "pd.DataFrame",
    outputs: list[Any],
    state: "_ForEachState",
    db: "DatabaseManager | None",
) -> None:
    """Save results from the result table to output variable types using batch operations.

    Two phases: :func:`build_save_plan` (every record, its metadata and its
    input edges, decided before any write) and :func:`apply_save_plan` (the
    writes, in one transaction: all-or-nothing, decision D9).

    Everything this needs beyond the table is a fact about the CALL, and the
    call is ``state`` (`_ForEachState`) — the output names, the config keys,
    the glue, the function object, whether it generates a file, which
    endpoint kind it is. Every question about what a row CONSUMED is answered
    by ``state.bindings`` (`scidb.run.bindings.RunBindings`).
    """
    if db is None:
        db = get_database()
    batch_start_time = time.perf_counter()
    plan = build_save_plan(state, result_tbl, outputs, db)
    report = apply_save_plan(plan, state, db)

    batch_total_elapsed = time.perf_counter() - batch_start_time
    _journal = Log.current_run()
    if _journal is not None:
        _journal.count("saved", report.total_saved)
    Log.info(
        f"[batch_save] {plan.fn_name}: committed {report.total_saved} record(s) + graph in one "
        f"transaction in {batch_total_elapsed:.3f}s "
        f"({report.total_saved / batch_total_elapsed:.1f} records/s)"
    )


# ---------------------------------------------------------------------------
# Save phase 1: plan
# ---------------------------------------------------------------------------


def build_save_plan(
    state: "_ForEachState",
    result_tbl: "pd.DataFrame",
    outputs: "list[Any]",
    db: "DatabaseManager",
) -> SavePlan:
    """Every record this save writes, with its metadata and input edges.

    Per result row: the row's input edges (``RunBindings.for_combo``), the
    branch params it inherits, the save metadata (schema keys + config keys
    + constants + ``__branch_params``), and ``__invocation_id`` — one per
    distinct edge set. Writes nothing. Logs the input-provenance sources and
    checks that every selection the call asked for reaches an edge.
    """
    output_names = state.output_names
    config_keys = state.config_keys
    fn_name = config_keys.get("__fn", "")
    schema_keys_set = (
        set(db.dataset_schema_keys)
        if db is not None and hasattr(db, "dataset_schema_keys")
        else set(dataset_schema_keys_of(None))
    )
    _log_pinned_outputs(db, outputs, fn_name)
    _log_input_provenance_sources(state, result_tbl, fn_name)

    plan = SavePlan(
        fn_name=fn_name,
        outputs=list(outputs),
        output_names=list(output_names),
        schema_keys=schema_keys_set,
        n_rows=len(result_tbl),
    )
    prep_start = time.perf_counter()
    Log.info(f"[batch_save] Preparing {len(result_tbl)} result row(s) for batch save")

    # Columns that are metadata (not output names)
    meta_cols = [c for c in result_tbl.columns if c not in output_names]
    direct_constants = config_keys.get("__constants") or {}
    # Write-side round-trip guard (docs/claude/input-binding-round-trip.md §5):
    # what the call ASKED for is `input_selectors`; what the edges will RECORD
    # is only what lands in the edges. Accumulated across rows and checked
    # once, because the loss is a property of the save PATH, not of any one
    # row — a per-row warning would print once per record.
    selectors_recorded: dict = {}
    rows_without_bindings = 0
    # One invocation id per distinct edge set: a fan-out emits thousands of
    # rows sharing one invocation, and the id is a hash over every edge.
    inv_id_cache: dict = {}
    # list of dicts: 10-100x faster iteration than iterrows()
    rows = result_tbl.to_dict("records")
    for row in rows:
        # The row's input edges — every consumed record with its selector —
        # from the ONE assembly (`RunBindings.for_combo`): full-iteration rows
        # carry `__rid_*` columns, aggregation rows carry `__vsig_*` and name
        # their group in each input's pool, and a pinned (Fixed) rid is read
        # off the binding in either mode.
        edges = state.bindings.for_combo(row)
        if edges:
            selectors_recorded.update({e.param: e.selector for e in edges if e.selector})
        else:
            rows_without_bindings += 1
        save_metadata = _row_save_metadata(
            row, edges, meta_cols, config_keys, direct_constants, schema_keys_set,
            fn_name, state.bindings, inv_id_cache,
        )
        _collect_row_items(plan, row, save_metadata, edges, meta_cols, state.generates_file)

    for output_name, stats in plan.spread_outputs.items():
        Log.info(
            f"[batch_save] {output_name}: {stats['records']} record(s) filed by its "
            f"own schema key column(s) {sorted(stats['keys'])}"
        )

    # Did every selection the call asked for reach an edge? Once per run.
    input_selectors = state.bindings.selectors
    if any(input_selectors.values()):
        check_selector_round_trip(
            _function_name(state.fn),
            input_selectors,
            selectors_recorded,
            context=(
                f"{rows_without_bindings}/{len(rows)} saved row(s) named no "
                f"selection with records (and no pin), so they bound no input"
                if rows_without_bindings
                else ""
            ),
        )

    prep_elapsed = time.perf_counter() - prep_start
    Log.info(
        f"[batch_save] Preparation complete in {prep_elapsed:.3f}s: "
        f"{len(plan.batches)} batch group(s), {len(plan.generated)} generates_file item(s)"
    )
    return plan


def _log_pinned_outputs(db: Any, outputs: "list[Any]", fn_name: str) -> None:
    """A pinned default is sticky (D6): saving new output to a pinned variable
    never moves the pin. The GUI asks before such a run; a terminal run is
    told here, once per call, so "my new results aren't showing up" has an
    answer in the log."""
    if db is None or not hasattr(db, "_duck"):
        return
    from scidb.store.variant_pins import active_pins

    pins = active_pins(db)
    for out in outputs:
        pin = pins.get(getattr(out, "__name__", str(out)))
        if pin is not None:
            Log.info(
                f"[default] {fn_name}: saving to {pin.variable}, which is "
                f"pinned to {pin.selection} (pin {pin.pin_id}). Records "
                f"this run writes outside that variant are NOT the default; "
                f"move the pin to make them so."
            )


def _log_input_provenance_sources(
    state: "_ForEachState", result_tbl: "pd.DataFrame", fn_name: str
) -> None:
    """Input-provenance diagnostic. A row's _invocation_input edges come from
    the `Selection` its `COMBO_KEY` names (every record the call read) plus
    any pinned rid. If NONE can bind, the output records are saved with NO
    consumed-input edges → broken lineage AND the precondition for the
    re-run orphan/duplicate cascade. Cheap: inspects columns once."""
    run_bindings = state.bindings
    handle_present = COMBO_KEY in result_tbl.columns
    selections_have_rids = any(s.rids for s in run_bindings.selections)
    pinned = run_bindings.pinned_rids
    Log.debug(
        f"[batch_save] input-provenance sources for {fn_name!r}: "
        f"bindings={ {p: b.kind.value for p, b in run_bindings.inputs.items()} }, "
        f"{COMBO_KEY} in result_tbl={handle_present}, "
        f"selections={len(run_bindings.selections)} "
        f"({'with rids' if selections_have_rids else 'none with rids'}), "
        f"fixed_rids={list(pinned)}"
    )
    if (handle_present and selections_have_rids) or pinned:
        return
    # Only WARN for the case that is actually a bug. With no scidb-variable
    # input there is nothing to bind, so having no input edges is the
    # correct outcome, not a symptom — warning on it every PathInput- or
    # constant-only run (the common case) trains the reader to scroll past
    # the one message that flags a genuinely severed lineage.

    var_params = variable_input_params(state.config_keys)
    if var_params:
        Log.warn(
            f"[batch_save] {fn_name!r}: NO variable input-binding source (the "
            f"result table carries no {COMBO_KEY} handle or the selections "
            f"it names hold no records, and there are no fixed rids) — saved "
            f"records will have NO _invocation_input edges, yet scidb-variable "
            f"input(s) {var_params} WERE consumed. This severs lineage and is "
            f"the precondition for the re-run orphan/duplicate cascade — the "
            f"{COMBO_KEY} column was likely dropped before save, e.g. "
            f"distribute fan-out or the result round-trip."
        )
    else:
        Log.info(
            f"[batch_save] {fn_name!r}: no _invocation_input edges — expected, "
            f"the inputs are files (PathInput) and/or constants, so there are "
            f"no consumed records to bind."
        )


def _row_save_metadata(
    row: dict,
    edges: Any,
    meta_cols: "list[str]",
    config_keys: dict,
    direct_constants: dict,
    schema_keys_set: set,
    fn_name: str,
    run_bindings: RunBindings,
    inv_id_cache: dict,
) -> dict:
    """One result row's save metadata: its non-``__`` meta columns, the
    call's config keys and constants, ``__branch_params`` and
    ``__invocation_id``."""

    # The branch params the output inherits: every consumed record's,
    # merged. A conflict means two variant groups reached one call —
    # exactly what the aggregation auto-split exists to prevent.
    merged_bp, bp_conflicts = run_bindings.branch_params_for(edges)
    for conflict in bp_conflicts:
        warnings.warn(
            f"branch_params key {conflict} overwritten. Use version= for precise selection.",
            UserWarning,
            stacklevel=5,
        )
    # Constants namespaced by function name (for branch_params tracking)
    for k, v in direct_constants.items():
        merged_bp[f"{fn_name}.{k}"] = v
    # What a selection narrowed for this call: a coordinate like a constant, and
    # identity through its edge (provenance_save._selection_bindings).
    selection = run_bindings.selection_of(row)
    axis = selection.axis if selection is not None else None
    if axis is not None:
        merged_bp[f"{fn_name}.{SELECTION_PARAM}"] = axis.label
    # Dynamic discriminators: non-schema, non-__ meta columns with scalar values
    scalar_types = (bool, int, float, str)
    for col in meta_cols:
        if col.startswith("__") or col in schema_keys_set:
            continue
        val = row.get(col)
        if val is None or (isinstance(val, float) and math.isnan(val)):
            continue
        if not isinstance(val, scalar_types):
            continue  # Skip numpy arrays and other complex types
        if col in merged_bp and merged_bp[col] != val:
            warnings.warn(
                f"branch_params key '{col}' overwritten: "
                f"{merged_bp[col]!r} → {val!r}. "
                f"Use version= for precise selection.",
                UserWarning,
                stacklevel=5,
            )
        merged_bp[col] = val

    # Non-__ cols (schema keys etc.) + config_keys + __branch_params. Exclude
    # __rid_* and other internal __ columns from version keys.
    save_metadata = {col: row[col] for col in meta_cols if not col.startswith("__")}
    save_metadata.update(config_keys)
    # Constants as direct keys too, so downstream consumers see them in the
    # metadata dict. They are also stored under __constants in config_keys,
    # so _stringify_meta can strip them when loading back — preventing them
    # from polluting input DataFrames.
    for k, v in direct_constants.items():
        if k not in save_metadata:
            save_metadata[k] = v
    save_metadata["__branch_params"] = merged_bp
    if axis is not None:
        save_metadata["__selection"] = axis.to_json()

    # The record's identity includes the exact invocation that produced it:
    # `__invocation_id` goes into the version keys (so identical content from
    # different inputs, code or options is a different record), and record_run
    # recomputes it from these same edges and refuses to write a graph that
    # disagrees.
    edge_key = (
        tuple(sorted(tuple(e) for e in edges)),
        axis.to_json() if axis is not None else None,
    )
    inv_id = inv_id_cache.get(edge_key)
    if inv_id is None:
        inv_id = invocation_identity(save_metadata, edges)
        inv_id_cache[edge_key] = inv_id
    save_metadata["__invocation_id"] = inv_id
    return save_metadata


def _item_metadata(save_metadata: dict, exclude: "set[str]" = frozenset()) -> dict:
    """A per-item copy of a row's save metadata: items must not share the
    ``__branch_params`` dict across rows or outputs."""
    meta = {k: v for k, v in save_metadata.items() if k not in exclude}
    if "__branch_params" in meta:
        meta["__branch_params"] = dict(meta["__branch_params"])
    return meta


def _collect_row_items(
    plan: SavePlan,
    row: dict,
    save_metadata: dict,
    edges: Any,
    meta_cols: "list[str]",
    generates_file: bool,
) -> None:
    """Add one result row's records (one per output slot) to the plan."""
    import pandas as pd

    for output_idx, (output_obj, output_name) in enumerate(
        zip(plan.outputs, plan.output_names, strict=False)
    ):
        if output_name not in row:
            # Flatten/distribute mode: fn returned a DataFrame whose columns are
            # spread directly in result_tbl (scifor all_dataframes flatten mode).
            # Build a 1-row DataFrame from non-schema, non-__ data columns.
            data_cols = [
                c for c in meta_cols if not c.startswith("__") and c not in plan.schema_keys
            ]
            if not data_cols:
                continue
            plan.add(
                output_idx,
                "flatten",
                SaveItem(
                    pd.DataFrame({c: [row[c]] for c in data_cols}),
                    _item_metadata(save_metadata, exclude=set(data_cols)),
                    edges,
                ),
            )
            continue

        output_value = row[output_name]
        # This output has nothing to save at this combination while the
        # call's other outputs do (for_columns split mode: a group with no
        # data, or one the function raised on). No record is written; scifor
        # already counted and logged why.
        if _is_no_output(output_value):
            plan.no_output_reasons.setdefault(output_idx, []).append(output_value.reason)
            continue
        # generates_file output: side-effect function (writes a file, returns
        # no storable data). Saved lineage-only — graph + metadata, no data
        # row — keyed generated:{invocation_id}. The function's return value
        # is intentionally discarded.
        if generates_file:
            plan.generated.append(
                (output_obj, output_idx, SaveItem(None, _item_metadata(save_metadata), edges))
            )
            continue
        # Several outputs: each files its own rows by its own key columns, so
        # one call can save at [subject, bout] and [subject, bout, date]. One
        # output never gets here with such a frame: scifor spread it already.
        if len(plan.output_names) > 1 and isinstance(output_value, pd.DataFrame):
            pieces = spread_output_frame(
                output_value,
                save_metadata,
                sorted(plan.schema_keys),
                output_name=str(output_name),
            )
            if pieces is not None:
                stats = plan.spread_outputs.setdefault(
                    str(output_name), {"keys": set(), "records": 0}
                )
                stats["records"] += len(pieces)
                for address, piece in pieces:
                    stats["keys"].update(address)
                    plan.add(
                        output_idx,
                        "normal",
                        SaveItem(piece, {**_item_metadata(save_metadata), **address}, edges),
                    )
                continue
        plan.add(output_idx, "normal", SaveItem(output_value, _item_metadata(save_metadata), edges))


# ---------------------------------------------------------------------------
# Save phase 2: apply
# ---------------------------------------------------------------------------


def apply_save_plan(plan: SavePlan, state: "_ForEachState", db: "DatabaseManager") -> SaveReport:
    """Write a :class:`SavePlan` in ONE transaction (decision D9,
    all-or-nothing): the data rows (``save_batch`` per output), the
    lineage-only ``generates_file`` records, the provenance graph
    (``record_run``) and the function source commit together, or nothing
    from this call is saved — any failure raises ``OutputSaveError`` inside
    the transaction, which rolls it back. ``save_batch`` and ``record_run``
    each open a level of their own; ``SciDuck.transaction`` nests them.
    """
    report = SaveReport()
    with db.transaction(f"for_each_save({plan.fn_name})"):
        _save_batches(plan, state, db, report)
        _report_no_output_skips(plan.no_output_reasons, plan.batches, plan.outputs, db)
        if plan.generated and not report.failures:
            _save_generated(plan, state, db, report)
        if report.failures:
            Log.warn(
                f"[provenance] {plan.fn_name}: graph not written — an output failed, "
                f"so this call's save rolls back"
            )
        elif report.graph_records:
            _record_provenance(plan, state, db, report)

        if report.failures:
            Log.error(
                f"[batch_save] {plan.fn_name}: rolled back — nothing from this call was "
                f"saved ({report.total_saved} record(s) discarded); the run fails: "
                + "; ".join(report.failures)
            )
            raise OutputSaveError(plan.fn_name, report.failures, 0, causes=report.causes)
    return report


def _output_class(output_obj: Any) -> type:
    return output_obj if isinstance(output_obj, type) else type(output_obj)


def _save_batches(
    plan: SavePlan, state: "_ForEachState", db: "DatabaseManager", report: SaveReport
) -> None:
    """``save_batch`` per (output, path) group; each saved record becomes a
    graph record. One failure stops the rest (the transaction is doomed)."""
    for (output_idx, save_path), items in plan.batches.items():
        output_obj = plan.outputs[output_idx]
        if not items:
            continue
        if report.failures:
            # The transaction is doomed (and DuckDB has aborted it if the
            # failure was SQL), so a later output's save could only fail
            # with "transaction aborted" or be rolled back.
            Log.warn(
                f"[batch_save] {_output_type_label(output_obj)}: not attempted — "
                f"an earlier output failed and this call's save rolls back"
            )
            continue
        pairs = [(i.data, i.metadata) for i in items]
        try:
            _save_one_batch(plan, state, db, report, output_idx, save_path, items)
        except Exception as e:
            import traceback

            Log.error(
                f"[batch_save] Failed to save batch for "
                f"{_output_type_label(output_obj)} ({len(items)} record(s)): "
                f"{type(e).__name__}: {e}"
            )
            # Full traceback pinpoints the failing operation; the per-record
            # cause (if it is one record) is best surfaced by the schema
            # validation in save_batch, which skips bad records individually.
            Log.error("[batch_save] traceback:\n" + traceback.format_exc())
            Log.error(
                f"failed to save {_output_type_label(output_obj)}: {type(e).__name__}: {e}"
            )
            report.fail(
                f"{_output_type_label(output_obj)}: {len(items)} record(s) not saved "
                f"({type(e).__name__}: {e})",
                _save_seam_error(e, plan.fn_name, state, output_obj, pairs),
            )
            # NOTE: these are the FIRST rows of the batch for context — NOT
            # necessarily the row that raised. A batch insert fails atomically,
            # so the offending record is not identifiable from the exception
            # alone; rely on the SKIPPED warnings to find bad records.
            for _data, meta in pairs[:3]:
                meta_str = ", ".join(f"{k}={v}" for k, v in meta.items() if not k.startswith("__"))
                Log.error(
                    f"[error] (first batch rows, not necessarily the "
                    f"culprit) {meta_str}: {_output_type_label(output_obj)}"
                )


def _pooled_params_of(state: "_ForEachState") -> "list[str] | tuple":
    """The inputs recorded in ``_invocation_pooled``. A where= chose among these
    rows, so its locations do say which computation this is; only an unfiltered
    aggregating run pools whatever the location holds."""
    if state.aggregation_mode and "__where" not in (state.config_keys or {}):
        return state.bindings.pooled_params
    return ()


def _save_one_batch(
    plan: SavePlan,
    state: "_ForEachState",
    db: "DatabaseManager",
    report: SaveReport,
    output_idx: int,
    save_path: str,
    items: "list[SaveItem]",
) -> None:
    """``save_batch`` one (output, path) group and turn its saved records
    into graph records (stamping endpoint artifacts in record mode)."""
    output_obj = plan.outputs[output_idx]
    label = _output_type_label(output_obj)
    pairs = [(i.data, i.metadata) for i in items]
    Log.info(f"[batch_save] Saving {len(items)} record(s) for {label} ({save_path} path)")

    save_t0 = time.perf_counter()
    out_cls = _output_class(output_obj)
    record_ids = db.save_batch(out_cls, pairs, profile=False)
    save_elapsed = time.perf_counter() - save_t0
    # save_batch returns None in the slot of any record it skipped
    # (schema-incompatible); count only the records actually persisted.
    n_saved = sum(1 for r in record_ids if isinstance(r, str))
    report.total_saved += n_saved
    if len(items) - n_saved:
        Log.warn(
            f"[batch_save] {label}: {len(items) - n_saved} of "
            f"{len(items)} record(s) SKIPPED (schema-incompatible, e.g. "
            f"empty/missing-key results); the rest were saved. See the "
            f"per-record SKIPPED warning(s) in the log for details."
        )

    # Collect for the bipartite provenance graph. output_idx is the output
    # slot (output_num); items align with record_ids in order.
    schema_version = getattr(out_cls, "schema_version", 1)
    pooled_params = _pooled_params_of(state)
    for item, rid in zip(items, record_ids, strict=False):
        if not isinstance(rid, str):
            continue
        report.graph_records.append(
            _GraphRecord(
                out_cls.__name__,
                schema_version,
                output_idx,
                rid,
                item.metadata,
                bindings=item.edges,
                invocation_id=item.metadata.get("__invocation_id"),
                pooled_params=pooled_params,
            )
        )
        # Endpoint artifact stamping (D4, record mode): the one point where
        # the artifact path (in data), the consumed rids (in the edges), and
        # the saved record_id all coexist.
        if state.endpoint_kind:
            _stamp_recorded_artifact(plan, state, db, item, rid)

    # Log summary (first few records)
    for item, rid in zip(items[:3], record_ids[:3], strict=False):
        meta_str = ", ".join(
            f"{k}={v}" for k, v in item.metadata.items() if not k.startswith("__")
        )
        rid_short = rid[:12] if isinstance(rid, str) else str(rid)
        suffix = " [flatten]" if save_path == "flatten" else ""
        Log.info(
            f"[save] {meta_str}: {label} -> record_id={rid_short} "
            f"({_describe_save_data(item.data)}){suffix}"
        )
    if len(items) > 3:
        Log.info(f"[save] ... and {len(items) - 3} more record(s)")
    Log.info(
        f"[batch_save] Completed {len(items)} save(s) for {label} in {save_elapsed:.3f}s "
        f"({len(items) / save_elapsed:.1f} records/s)"
    )


def _stamp_recorded_artifact(
    plan: SavePlan, state: "_ForEachState", db: "DatabaseManager", item: SaveItem, rid: str
) -> None:
    """Stamp an endpoint's artifact with its provenance blob (record mode)."""
    apath = _endpoint_artifact_path(state.endpoint_kind, item.data)
    if not apath:
        return

    blob = _build_stamp_blob(
        fn_name=plan.fn_name,
        inputs_map=_stamp_inputs_from_edges(item.edges),
        schema={
            k: v
            for k, v in item.metadata.items()
            if k in plan.schema_keys and v is not None and not str(k).startswith("__")
        },
        db=db,
        record_id=rid,
    )
    stamp_artifact(apath, blob)


def _save_generated(
    plan: SavePlan, state: "_ForEachState", db: "DatabaseManager", report: SaveReport
) -> None:
    """generates_file outputs — lineage-only save (graph + metadata, no data
    row), keyed ``generated:{invocation_id}``. Built entirely from each row's
    save_metadata bindings; the function's return value is discarded."""
    from datetime import datetime


    Log.info(
        f"[batch_save] Saving {len(plan.generated)} generates_file item(s) (lineage-only)"
    )
    for output_obj, output_idx, item in plan.generated:
        try:
            cls = _output_class(output_obj)
            out_name = cls.__name__
            sv = getattr(cls, "schema_version", 1)
            gen_inv_id = invocation_identity(item.metadata, item.edges)
            generated_id = f"generated:{gen_inv_id}"
            schema_keys = {k: v for k, v in item.metadata.items() if k in plan.schema_keys}
            schema_level = db._infer_schema_level(schema_keys)
            schema_id = (
                db._duck.get_or_create_schema_id(schema_level, schema_keys)
                if schema_level is not None and schema_keys
                else 0
            )
            ts = datetime.now().isoformat()
            # The generated record's producing invocation (written by
            # record_run) carries the function identity, so the graph-based
            # skip_computed gate finds it.
            db._save_record_event(record_id=generated_id, timestamp=ts)
            insert_record_entity(
                db._duck,
                record_id=generated_id,
                created_at=ts,
                type_name=out_name,
                schema_id=schema_id,
                content_hash=None,
                schema_version=sv,
            )
            report.graph_records.append(
                _GraphRecord(
                    out_name,
                    sv,
                    output_idx,
                    generated_id,
                    item.metadata,
                    bindings=item.edges,
                    invocation_id=gen_inv_id,
                    pooled_params=_pooled_params_of(state),
                )
            )
            report.total_saved += 1
            meta_str = ", ".join(
                f"{k}={v}" for k, v in item.metadata.items() if not k.startswith("__")
            )
            Log.info(f"[save] {meta_str}: {out_name} -> {generated_id[:20]} [generates_file]")
        except Exception as e:
            Log.error(
                f"[error] failed generates_file save for {_output_type_label(output_obj)}: {e}"
            )
            report.fail(
                f"{_output_type_label(output_obj)} (generates_file): 1 item not saved "
                f"({type(e).__name__}: {e})",
                _save_seam_error(e, plan.fn_name, state, output_obj, []),
            )


def _record_provenance(
    plan: SavePlan, state: "_ForEachState", db: "DatabaseManager", report: SaveReport
) -> None:
    """Write the bipartite provenance graph + append-only _run audit, then
    the function source. The graph is the only source of truth for where a
    record came from, so failing to write either fails the save. Idempotent
    for the graph; a fresh _run row is appended per execution."""
    fn = state.fn
    try:
        from scidb.provenance.intent import current_output_names as _current_output_names

        report.run_id = _provenance_save.record_run(
            db,
            report.graph_records,
            function_name=plan.fn_name,
            where_clause=state.config_keys.get("__where"),
            glue_virtual=state.glue_virtual,
            glue_chains=state.glue_chains,
            parameter_names=state.parameter_names,
            path_input_specs=state.path_input_specs,
            output_types=[_output_type_label(o) for o in plan.outputs],
            # A MATLAB function's signature output names (the bridge's
            # sentinel carries them), or a GUI run's declared Python output
            # ports (scidb.provenance.intent.run_output_names); otherwise none.
            output_params=getattr(fn, "output_params", None) or _current_output_names(),
        )
        Log.info(
            f"[provenance] recorded run_id={report.run_id} for {len(report.graph_records)} "
            f"record(s) of fn={plan.fn_name}"
        )
    except Exception as e:
        Log.error(
            f"[provenance] record_run failed for fn={plan.fn_name}: "
            f"{type(e).__name__}: {e}",
            exc_info=True,
        )
        report.fail(f"provenance graph not recorded: {type(e).__name__}: {e}", e)
        return
    # A failed INSERT aborts the DuckDB transaction, so it cannot be skipped
    # and the rest committed: it fails the save like any write.
    try:
        _capture_function_source(db, fn, plan.fn_name, state.config_keys.get("__fn_hash"))
    except Exception as e:
        Log.error(
            f"[provenance] function source not recorded for fn={plan.fn_name}: "
            f"{type(e).__name__}: {e}",
            exc_info=True,
        )
        report.fail(f"function source not recorded: {type(e).__name__}: {e}", e)
