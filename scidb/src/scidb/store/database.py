"""Database connection and management using SciDuck backend."""

import importlib.util
import json
import os
import threading
import time
import warnings
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

from scicanonicalhash import canonical_hash, datetime_key_text, frame_path_counts, generate_record_id
from scifor.outputs import is_empty_table
from sciduckdb import (
    bulk_df_to_storage_rows,
    column_type_change,
    dataframe_to_storage_rows,
    infer_data_columns,
    record_schema_mismatch,
    restore_types,
    storage_to_python,
    storage_to_python_column,
    unflatten_dict,
    value_to_storage_row,
    count_null_list_elements,
    is_datetime_meta,
    merge_tz_columns,
    storage_column_names,
    storage_column_types,
    stored_tz_companions,
    tz_companions,
    user_column_types,
)

from scidb.provenance import query as provenance_query
from scidb.store import schema_types
from scidb.run.bindings import variant_signature
from scidb.model.exceptions import (
    AmbiguousVersionError,
    DatabaseNotConfiguredError,
    NotFoundError,
    NotRegisteredError,
)
from scidb.model.log import Log
from scidb.model.parameter import parameter_node_name
from scidb.store.record_store import BatchRows, RecordStore
from scidb.store.sciduck import SciDuck
from scidb.store.schema_values import (
    canonical_numeric_value,
    from_datetime_key_text,
    from_schema_str,
    schema_str,
)
from scidb.model.variable import BaseVariable

if TYPE_CHECKING:
    from scidb.run.pipeline import Pipeline

# Moved into sibling modules (grug follow-up); imported back for this module
# and for code that imports these names from here.
from scidb.store.db_helpers import (  # noqa: F401 — re-exported
    _colliding_locations,
    _data_column_names,
    _declared_rank_column,
    _describe_data,
    _differing_columns,
    _filter_records_by_branch_params,
    _filter_records_by_code_version,
    _filter_records_by_run_options,
    _flatten_struct_columns,
    _get_leaf_paths,
    _get_nested_value,
    _log_first_content_hash,
    _match_branch_param,
    _natural_text_key,
    _pin_values,
    _pyarrow_available,
    _sample_values,
    _set_nested_value,
    _unflatten_struct_columns,
    _value_fingerprint,
)


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.model.log import attach_log_file
from scidb.provenance.graph import parse_path_input_spec as _parse_path_input
from scidb.provenance.graph import split_call_site_outputs
from scidb.run.pipeline import Pipeline
# Global database instance (thread-local for safety)
_local = threading.local()


def configure_database(
    dataset_db_path: str | Path,
    dataset_schema_keys: list[str],
    schema_key_types: "dict[str, str] | None" = None,
) -> "DatabaseManager":
    """
    Configure the global database connection.

    Single-call setup that creates the database, auto-registers all known
    BaseVariable subclasses, and enables lineage caching.

    Args:
        dataset_db_path: Path to the DuckDB database file
        dataset_schema_keys: List of metadata keys that define the dataset schema
            (e.g., ["subject", "visit", "channel"]). These keys identify the
            logical location of data and are used for the folder hierarchy.
            Any metadata keys not in this list are treated as version parameters
            that distinguish different computational versions of the same data.
        schema_key_types: Optional per-key type declaration, e.g.
            ``{"trial": "numeric"}``.  ``"numeric"`` keys canonicalize every
            spelling of the same number to one identity ("001", 1, and 1.0
            all store as "1"; zero-padded filenames still resolve via
            PathInput's numeric fallback).  ``"string"`` keys are verbatim —
            spelling IS identity, and PathInput matches them exactly only.
            Undeclared keys behave as strings until a PathInput resolution
            has to bridge spellings (e.g. trial=1 matching "6MWT-001.mat"),
            which raises SchemaKeyTypeError asking for a declaration.

            The DATABASE owns the declaration (``scidb.store.schema_types``); this
            argument writes to it. A key with no stored type gets this one,
            the same stored type is a no-op, and a different stored type
            raises ``SchemaKeyTypeConflictError``. Omitting the argument
            uses whatever is stored. Change a stored type with
            ``db.set_schema_key_type`` or the GUI's Numeric checkbox.

    Returns:
        The DatabaseManager instance
    """
    # Set log file path next to the database file (a no-op if a caller —
    # e.g. the GUI server, which attaches before its startup discovery pass
    # — already pointed the sink here).
    #
    # Attached BEFORE the connection is built, not after: a MATLAB host calls
    # this as its first scidb entry point, so anything logged during
    # construction — including the phase timings below and any DDL failure —
    # would otherwise land on stderr only and be missing from the very log
    # file being set up.

    attach_log_file(dataset_db_path)

    # Phase timing: a MATLAB run pays this cost on every invocation (the
    # script must close the database at the end to hand the write lock back
    # to the GUI, so nothing is reused), and it was measured at ~170ms
    # against a database on a network share. The breakdown says whether that
    # is the DuckDB open itself or the idempotent DDL (scidb.store.tables)
    # round trips re-run each time.
    with Log.timer(
        "configure_database", extra=Path(dataset_db_path).name
    ) as timer:
        with timer.phase("database_manager"):
            db = DatabaseManager(
                dataset_db_path,
                dataset_schema_keys=dataset_schema_keys,
                dataset_schema_key_types=schema_key_types,
            )
        with timer.phase("register_types"):
            for cls in BaseVariable._all_subclasses.values():
                db.register(cls)
        db.set_current_db()

        # Propagate schema keys to scifor so that DataFrame detection and
        # distribute=True work identically in DB-backed and standalone modes.
        with timer.phase("scifor_set_schema"):
            try:
                import scifor

                scifor.set_schema(list(dataset_schema_keys))
            except ImportError:
                pass

    # Run-context header: makes every log file self-describing (which
    # versions produced it) when debugging an archived log or a colleague's.
    Log.info(
        f"configure_database: path={dataset_db_path}, "
        f"schema_keys={list(dataset_schema_keys)}, "
        f"schema_key_types={schema_types.describe(db.dataset_schema_key_types)} "
        f"(stored in the database) | {_run_context()}"
    )

    return db


def _run_context() -> str:
    """One-line host/version fingerprint for the log header.

    Includes the interpreter PATH (``sys.executable``), not only its version:
    the GUI runs under VS Code's selected interpreter and ``scidb`` in a
    terminal under whichever one installed it, and "which environment wrote
    this line" is the question when one works and the other is missing
    packages (2026-10-01).
    """
    import os
    import platform
    import sys
    from importlib import metadata

    versions = []
    for pkg in ("scidb", "scifor", "sciduckdb", "scicanonicalhash", "scistacklog"):
        try:
            versions.append(f"{pkg}={metadata.version(pkg)}")
        except Exception:
            pass
    return (
        f"python={platform.python_version()} ({sys.executable}), pid={os.getpid()}"
    ) + (
        ", " + ", ".join(versions) if versions else ""
    )


def get_database() -> "DatabaseManager":
    """
    Get the global database connection.

    Returns:
        The DatabaseManager instance

    Raises:
        DatabaseNotConfiguredError: If configure_database() hasn't been called
    """
    db = getattr(_local, "database", None)
    if db is None:
        raise DatabaseNotConfiguredError(
            "Database not configured. Call configure_database(path) first."
        )
    if getattr(db, "_closed", False):
        db.reopen()
    return db


def set_current_database(db) -> None:
    """Install *db* as the thread's current database — what ``get_database``
    returns. Public so a caller that supplies its own database object does not
    reach into ``_local``."""
    _local.database = db


def clear_current_database() -> None:
    """Forget the thread's current database, if any (a tool that configured
    one only to scaffold a project leaves none behind)."""
    if hasattr(_local, "database"):
        delattr(_local, "database")


@contextmanager
def using_db(db: "DatabaseManager"):
    """THE way a thread enters a database: *db* is this thread's current
    database (what ``get_database`` returns) inside the block, and whatever was
    current before is current again after it.

    The current database is thread-local, so a new thread starts with none.
    Calling ``set_current_db()`` by hand at each thread start was a convention
    that every new thread had to remember (docs/claude/grugbrain-principles.md);
    entering through this context manager makes it the only spelling.
    """
    previous = getattr(_local, "database", None)
    thread = threading.current_thread().name
    Log.debug(
        f"using_db enter: thread={thread} db={getattr(db, 'dataset_db_path', db)!s} "
        f"previous={'none' if previous is None else getattr(previous, 'dataset_db_path', previous)!s}"
    )
    db.set_current_db()
    try:
        yield db
    finally:
        if previous is None:
            clear_current_database()
        else:
            set_current_database(previous)
        Log.debug(f"using_db exit: thread={thread} restored={'none' if previous is None else 'previous'}")


def database_or_none(db: "DatabaseManager | None" = None) -> "DatabaseManager | None":
    """*db* if given, else the configured database, else ``None`` — for a
    caller that can proceed without one.

    THE one spelling of "which database does this call use". Only "nothing
    configured" (:class:`DatabaseNotConfiguredError`) reads as ``None``; any
    other failure — a reopen that cannot get the file lock, above all —
    propagates. ``foreach`` used to spell this as ``try: get_database()
    except Exception: pass`` in a dozen places, so a locked database looked
    exactly like no database and a run silently carried on with different
    schema keys (cleanup-audit F5).
    """
    if db is not None:
        return db
    try:
        return get_database()
    except DatabaseNotConfiguredError:
        return None


def dataset_schema_keys_of(db: "DatabaseManager | None" = None) -> list[str]:
    """The dataset's schema keys, from the database (the one holder).

    ``scifor.set_schema`` keeps a COPY for scifor's standalone mode; scidb
    reads the database, never that copy back (cleanup-audit F14). ``[]``
    when no database is configured.
    """
    active = database_or_none(db)
    return list(getattr(active, "dataset_schema_keys", None) or [])


def aggregate_pipeline_variants(variants: list[dict]) -> dict:
    """Pipeline variant rows (``list_pipeline_variants``) -> the aggregated
    shape :meth:`DatabaseManager.get_aggregated_variants` returns — PURE, no
    database. THE one conversion: the GUI used to keep a second, test-only copy
    of it (``graph_builder.aggregate_variants``), so its tests exercised a path
    production never ran (cleanup-audit F19).

    ``variables`` carries ``record_count: None``; the method fills the real
    counts. ``path_inputs`` is keyed by the PathInput SPEC (its JSON), each
    entry ``{"template", "root_folder", "functions": [(fkey, param_name), ...],
    "declared_names": {(fkey, param_name): declared PathInput name}}`` — the
    latter only where the run recorded one (cleanup-audit F38).
    """
    from collections import defaultdict


    # Initialize result structure
    functions = defaultdict(
        lambda: {
            "input_params": {},
            "outputs": [],
            "constants": defaultdict(list),
            "parameter_names": {},
            # {argument: declared PathInput name} where the run recorded it
            # (cleanup-audit F38) — what `provenance.call_site_wiring_ids`
            # groups steps by.
            "path_input_names": {},
            # {argument: [glue node names]} the recorded runs passed the
            # argument through — either head (a variable or a Parameter).
            # Glue names are part of the call_id, so every variant of one
            # call site agrees. The canvas uses it to draw a glued history
            # input through its glue node, not straight into the function.
            "glue_chains": {},
            # Step names (for_each(step=...)) of the runs behind this call
            # site, oldest first. Reported, never identity.
            "steps": [],
            "variant_count": 0,
            "variants": [],
        }
    )
    all_var_types = set()
    const_counts = defaultdict(lambda: defaultdict(int))
    const_fns = defaultdict(set)
    fn_constants_map = defaultdict(set)
    path_inputs = {}

    # Aggregate variants by (fn_name, call_id)
    for v in variants:
        fn = v["function_name"]
        cid = v.get("call_id", "")
        if not cid:
            continue  # Skip legacy variants without call_id

        fkey = (fn, cid)
        out = v["output_type"]
        inputs = v["input_types"]
        constants = v["constants"]
        count = v["record_count"]

        # Track variable types
        all_var_types.add(out)

        # Process inputs
        for param_name, type_val in inputs.items():
            # Check if it's a PathInput
            pi = _parse_path_input(type_val)
            if pi is not None:
                # Keyed by the SPEC, never the parameter name: two
                # functions whose PathInput parameters share a name
                # (`filePath`) but not a template used to be filed under
                # the FIRST one's template (cleanup-audit F37).
                spec_key = json.dumps(pi, sort_keys=True, default=str)
                entry = path_inputs.setdefault(
                    spec_key, {**pi, "functions": set(), "declared_names": {}}
                )
                entry["functions"].add((fkey, param_name))
                recorded = (v.get("path_input_names") or {}).get(param_name)
                if recorded:
                    entry["declared_names"][(fkey, param_name)] = recorded
                    functions[fkey]["path_input_names"][param_name] = recorded
            else:
                all_var_types.add(type_val)
                functions[fkey]["input_params"][param_name] = type_val

        for param_name, names in (v.get("glue_chains") or {}).items():
            functions[fkey]["glue_chains"][param_name] = list(names)
        for step in v.get("steps") or ():
            if step not in functions[fkey]["steps"]:
                functions[fkey]["steps"].append(step)

        # Track outputs
        if out not in functions[fkey]["outputs"]:
            functions[fkey]["outputs"].append(out)

        # Track constants. A constant's NODE is the declared Parameter its
        # run recorded, else the argument it filled (cleanup-audit B1):
        # the value lives under the argument in `functions[...]` (that is
        # what the function receives), and under the node everywhere a
        # Parameter is named (`constants`, `parameter_names`).
        recorded_names = v.get("parameter_names") or {}
        for k, val in constants.items():
            node = parameter_node_name(k, recorded_names)
            seen = functions[fkey]["parameter_names"].get(k)
            if seen is not None and seen != node:
                Log.warn(
                    f"aggregate_pipeline_variants: {fn} call {cid} argument "
                    f"{k!r} was fed by Parameter {seen!r} in one run and "
                    f"{node!r} in another; showing {seen!r}"
                )
                node = seen
            functions[fkey]["parameter_names"][k] = node
            const_counts[node][str(val)] += count
            const_fns[node].add(fkey)
            fn_constants_map[fkey].add(k)
            if val not in functions[fkey]["constants"][k]:
                functions[fkey]["constants"][k].append(val)

        # Track variant. output_num is carried through because for a
        # MULTI-OUTPUT fn it tells the GUI which slot of the signature
        # produced this output, and for MATLAB fns that is the sole
        # DB-derived source for the fn->output edge (see api/pipeline.py
        # matlab_param_to_class). Dropping it silently forced that edge to
        # depend on a hand-drawn manual edge instead.
        #
        # It is NOT variant identity (2026-09-22): a distributed run
        # numbers its slices, so keying on it made one call N variants.
        # Each output_type is now its own group and this is that type's
        # own (lowest) slot — which is what the GUI wanted all along.
        # A single-output fn must ignore it entirely; see
        # _matlab_param_to_class_from_db for the two cases where the
        # number is not a signature position at all.
        functions[fkey]["variants"].append(
            {
                "input_types": inputs,
                "constants": constants,
                "parameter_names": dict(recorded_names),
                "output_type": out,
                "output_num": v.get("output_num"),
                "record_count": count,
            }
        )
        functions[fkey]["variant_count"] += 1

    # Record counts need the database; the caller fills them in.
    variables = {t: {"record_count": None} for t in sorted(all_var_types)}

    # Build constants result
    constants_result = {}
    for const_name in const_counts:
        values = [
            {"value": val, "record_count": cnt}
            for val, cnt in sorted(const_counts[const_name].items())
        ]
        constants_result[const_name] = {
            "values": values,
            "functions": list(const_fns[const_name]),
        }

    # Convert functions dict to regular dict (remove defaultdict)
    functions_result = {}
    for fkey, data in functions.items():
        functions_result[fkey] = {
            "input_params": dict(data["input_params"]),
            "outputs": data["outputs"],
            "constants": {k: list(v) for k, v in data["constants"].items()},
            "parameter_names": dict(data["parameter_names"]),
            "path_input_names": dict(data["path_input_names"]),
            "glue_chains": {p: list(n) for p, n in data["glue_chains"].items()},
            "steps": list(data["steps"]),
            "variant_count": data["variant_count"],
            "variants": data["variants"],
        }

    # Functions as a sorted list of (fkey, param_name) pairs.
    for entry in path_inputs.values():
        entry["functions"] = sorted(entry["functions"])

    return {
        "functions": functions_result,
        "variables": variables,
        "constants": constants_result,
        "path_inputs": path_inputs,
    }


def call_site_path_input_names(path_inputs: dict, fallback=None) -> dict:
    """``{fkey: {param: PathInput name}}`` for the ``path_inputs`` of
    :func:`aggregate_pipeline_variants` — the PathInput term of a step's wiring.

    The name a run RECORDED (``declared_names``) always wins: it is the fact.
    Only a run recorded without one — before 2026-09-24, or a hand-written
    script whose PathInput was never declared — takes ``fallback(entry)``,
    the caller's best guess (the GUI matches its registry; the CLI has none and
    uses the spec, the default here). The spec is template AND root folder —
    the key the GUI's registry match uses too: one template under two roots
    names two different sets of files.
    """
    fallback = fallback or (
        lambda entry: "spec:"
        + json.dumps(
            {"template": entry.get("template"), "root_folder": entry.get("root_folder")},
            sort_keys=True,
            default=str,
        )
    )
    out: dict = {}
    for entry in path_inputs.values():
        recorded = entry.get("declared_names") or {}
        guess = None
        for fkey, param in entry.get("functions") or ():
            fkey = tuple(fkey)
            name = recorded.get((fkey, param))
            if name is None:
                if guess is None:
                    guess = fallback(entry)
                name = guess
            out.setdefault(fkey, {})[param] = name
    return out


def call_site_wiring_ids(aggregate: dict, fallback=None) -> dict:
    """``{fkey: wiring_id}`` — which recorded call sites form ONE pipeline
    step, for the output of :func:`aggregate_pipeline_variants`.

    THE step grouping (cleanup-audit F38). ``scidb graph`` groups through
    this function; the GUI groups by the same recipe
    (``provenance.compute_wiring_id``) over the same facts, its PathInput
    names coming from :func:`call_site_path_input_names`' rule (recorded
    first). Constants are not in the key: a sweep is one step with several
    variants.
    """

    # No `_node_wiring` claims here (they are GUI-side), so the owner answers
    # one wiring per call site — the union of its outputs, as before.
    names = call_site_path_input_names(aggregate.get("path_inputs") or {}, fallback)
    return {
        tuple(fkey): split_call_site_outputs(
            fkey[0],
            data.get("input_params") or {},
            data.get("outputs") or [],
            names.get(tuple(fkey), {}),
        )[0][0]
        for fkey, data in (aggregate.get("functions") or {}).items()
    }


# The class's methods, grouped by job (grug follow-up). Each module holds one
# mixin; DatabaseManager inherits them all, so its API is unchanged.
from scidb.store.db_schema_keys import SchemaKeysMixin
from scidb.store.db_save import SaveMixin
from scidb.store.db_save_batch import BatchSaveMixin
from scidb.store.db_load import LoadMixin
from scidb.store.db_versions import VersionsMixin


class DatabaseManager(SchemaKeysMixin, SaveMixin, BatchSaveMixin, LoadMixin, VersionsMixin):
    """
    Manages data storage and lineage persistence (both in DuckDB via SciDuck).

    Example:
        db = configure_database("experiment.duckdb", ["subject", "session"])

        RawSignal.save(np.eye(3), subject=1, session=1)
        loaded = RawSignal.load(subject=1, session=1)
    """

    def __init__(
        self,
        dataset_db_path: str | Path,
        dataset_schema_keys: list[str],
        read_only: bool = False,
        dataset_schema_key_types: "dict[str, str] | None" = None,
    ):
        """
        Initialize database connection.

        Args:
            dataset_db_path: Path to DuckDB database file (created if doesn't exist)
            dataset_schema_keys: List of metadata keys that define the dataset schema
                (e.g., ["subject", "visit", "channel"]). These keys identify the
                logical location of data. Any other metadata keys are treated as
                version parameters.
            read_only: Open the database read-only (it must already exist). No
                DDL runs and every write on the connection fails at the DuckDB
                level — used by inspection tooling (scidb.inspect / the scidb
                CLI) so it can never contend for the write lock.
            dataset_schema_key_types: Declared key types to record in the
                database (see ``configure_database(schema_key_types=...)``).
        """
        self.dataset_db_path = Path(dataset_db_path)

        if isinstance(dataset_schema_keys, (set, frozenset)):
            raise TypeError(
                "dataset_schema_keys must be an ordered sequence (list or tuple), "
                "not a set. Schema key order defines the dataset hierarchy."
            )
        self.dataset_schema_keys = list(dataset_schema_keys)

        # Validated before the file is opened; recorded in the database once
        # its tables exist (scidb.store.schema_types.declare, below). The database
        # is the one owner of the declaration: see dataset_schema_key_types.
        declared_key_types = schema_types.validate(
            dataset_schema_key_types, self.dataset_schema_keys
        )
        self._schema_key_types: dict[str, str] = {}

        # Declared level order (`[schema_keys]`) is NOT snapshotted here: see
        # the `dataset_schema_key_order` property. Asked once now so the
        # config it came from is logged, and a typo reported, when the
        # database opens.
        self._validated_key_order: "dict | None" = None
        _ = self.dataset_schema_key_order

        self.read_only = bool(read_only)
        self._registered_types: dict[str, type[BaseVariable]] = {}

        # Open the SciDuck backend: connect, create any missing table
        # (scidb.store.tables.ensure_core, idempotent; none on a read-only open),
        # then check the schema columns. Timed: this runs on every MATLAB
        # invocation (see configure_database), and "opening the file" and
        # "re-running the idempotent DDL" have very different fixes when the
        # database lives on a network share -- the DDL's own time is the
        # `[tables] ensured ... in N ms` line inside `duck_open`.
        with Log.timer(
            "DatabaseManager.__init__", extra=self.dataset_db_path.name
        ) as timer:
            with timer.phase("duck_open"):
                self._duck = SciDuck(
                    self.dataset_db_path,
                    dataset_schema=dataset_schema_keys,
                    read_only=self.read_only,
                )
                self._duck.add_rollback_listener(self._forget_uncommitted_caches)

            with timer.phase("schema_key_types"):
                self._refresh_schema_key_types()
                if declared_key_types:
                    try:
                        schema_types.declare(self, declared_key_types)
                    except Exception:
                        # A refused declaration must not leave the file
                        # locked: the caller never gets a manager to close.
                        self._duck.close()
                        raise

        self._closed = False  # Track connection open/closed state
        self._inspector = None  # lazy scidb.inspect.Inspector (see .inspect)


    def get_aggregated_variants(
        self,
        fn_name: str | None = None,
        call_id: str | None = None,
    ) -> dict:
        """
        Get aggregated variant data for pipeline visualization.

        Aggregates variants by (fn_name, call_id) and collects metadata about
        functions, variables, constants, and path inputs. This provides all the
        data needed to build a pipeline graph in one query.

        Args:
            fn_name: Optional function name to filter results.
            call_id: Optional call_id to filter to a specific for_each call site.

        Returns:
            Dict with keys:

            ``"functions"`` (dict)
                Keyed by (fn_name, call_id) tuples::

                    {
                        (fn_name, call_id): {
                            "input_params": {param: var_type},
                            "outputs": [var_type1, var_type2],
                            "constants": {param: [val1, val2]},
                            "parameter_names": {param: parameter node name},
                            "glue_chains": {param: [glue node name, ...]},
                            "variant_count": int,
                            "variants": [
                                {
                                    "input_types": {param: type_name},
                                    "constants": {param: value},
                                    "output_type": str,
                                    "output_num": int | None,
                                    "record_count": int,
                                }
                            ],
                        }
                    }

            ``"variables"`` (dict)
                Variable metadata keyed by variable name::

                    {
                        var_type: {
                            "record_count": int,
                        }
                    }

            ``"constants"`` (dict)
                Constants used across functions, keyed by PARAMETER NODE name:
                the declared Parameter the run recorded, else the argument.
                Each function's ``"parameter_names"`` (``{argument: node}``)
                says which argument each node filled — the handle an edge
                from it lands on::

                    {
                        const_name: {
                            "values": [{"value": val, "record_count": N}],
                            "functions": [(fn_name, call_id), ...],
                        }
                    }

            ``"path_inputs"`` (dict)
                PathInputs, keyed by SPEC (its JSON), not by parameter name —
                two functions may name different PathInputs alike::

                    {
                        spec_json: {
                            "template": str,
                            "root_folder": str | None,
                            "functions": [((fn_name, call_id), param_name), ...],
                        }
                    }

            The conversion itself is :func:`aggregate_pipeline_variants`
            (pure); this method adds the database-only record counts.
        """
        variants = self.list_pipeline_variants()
        if fn_name is not None:
            variants = [v for v in variants if v["function_name"] == fn_name]
        if call_id is not None:
            variants = [v for v in variants if v.get("call_id") == call_id]

        result = aggregate_pipeline_variants(variants)

        # Every variable type's record count in ONE query (it was one per type).
        types = list(result["variables"])
        counts: dict = {}
        if types:
            ph = ", ".join("?" for _ in types)
            counts = {
                t: n
                for t, n in self._duck.fetchall(
                    "SELECT type, COUNT(DISTINCT record_id) FROM _record "  # noqa: S608
                    f"WHERE type IN ({ph}) AND COALESCE(excluded, FALSE) = FALSE "
                    "GROUP BY type",
                    types,
                )
            }
        for t in types:
            result["variables"][t] = {"record_count": counts.get(t, 0)}
        return result


    # -------------------------------------------------------------------------
    # Export Methods
    # -------------------------------------------------------------------------

    def export_to_csv(
        self,
        variable_class: type[BaseVariable],
        path: str,
        **metadata,
    ) -> int:
        """Export matching variables to a CSV file."""
        results = list(self.load(variable_class, metadata))

        if not results:
            raise NotFoundError(
                f"No {variable_class.__name__} found matching metadata: {metadata}"
            )

        all_dfs = []
        for var in results:
            df = variable_class(var.data).to_db()
            df["_record_id"] = var.record_id
            if var.metadata:
                for key, value in var.metadata.items():
                    df[f"_meta_{key}"] = value
            all_dfs.append(df)

        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_csv(path, index=False)

        return len(results)

    def _get_variable_class(self, type_name: str):
        """Get a variable class by name (class name, not table name)."""
        if type_name in self._registered_types:
            return self._registered_types[type_name]

        return BaseVariable.get_subclass_by_name(type_name)


    def transaction(self, label: str = ""):
        """One transaction around the block (re-entrant; see
        :meth:`sciduckdb.DuckConnection.transaction`). Saves inside it commit
        together or not at all."""
        return self._duck.transaction(label)

    def _forget_uncommitted_caches(self) -> None:
        """Rollback listener: drop caches that statements rolled back may
        have filled (a type registered inside the transaction, schema-key
        kinds read from rows that are now gone). Both refill from the DB."""
        Log.debug(
            f"[transaction] rolled back; forgetting {len(self._registered_types)} "
            f"registered-type cache entr(ies) and schema-key kinds"
        )
        self._registered_types.clear()
        self._forget_schema_key_kinds()


    def list_variables(self) -> "pd.DataFrame":
        """Return all variable types stored in this database.

        Queries the ``_variables`` table and returns a DataFrame with columns:
        ``variable_name``, ``schema_level``, ``created_at``, ``description``.

        Useful for discovering what variable types exist in a database file
        without needing the original Python class definitions.
        """
        return self._duck.list_variables()

    # -------------------------------------------------------------------------
    # Variable Groups
    # -------------------------------------------------------------------------

    @staticmethod
    def _resolve_var_name(v) -> str:
        """Resolve a single variable to its name string.

        Accepts a Python str, a BaseVariable subclass (class object),
        or a MATLAB BaseVariable instance (matlab.object with class name).
        """
        if isinstance(v, str):
            return v
        if isinstance(v, type) and issubclass(v, BaseVariable):
            return v.table_name()
        # MATLAB objects cross the bridge as matlab.object; try str()
        # to extract the class name (e.g. "StepLength").
        s = str(v)
        if s:
            return s
        raise TypeError(f"Expected a string or BaseVariable subclass, got {type(v)}")

    @staticmethod
    def _resolve_var_names(variables) -> list:
        """Resolve a single or list/iterable of variables to name strings.

        Each element can be a string, a BaseVariable subclass, or a MATLAB
        object.  Accepts Python lists, MATLAB cell arrays, and MATLAB string
        arrays (any iterable).
        """
        # Scalar: single string or single class
        if isinstance(variables, (str, type)):
            return [DatabaseManager._resolve_var_name(variables)]
        # Any iterable (Python list, MATLAB cell array, MATLAB string array)
        try:
            return [DatabaseManager._resolve_var_name(v) for v in variables]
        except TypeError:
            # Not iterable — treat as a single item
            return [DatabaseManager._resolve_var_name(variables)]

    def add_to_var_group(self, group_name: str, variables):
        """Add one or more variables to a variable group.

        Args:
            group_name: Name of the group.
            variables: A BaseVariable subclass, a variable name string,
                or a list of either.
        """
        self._duck.add_to_group(group_name, self._resolve_var_names(variables))

    def remove_from_var_group(self, group_name: str, variables):
        """Remove one or more variables from a variable group.

        Args:
            group_name: Name of the group.
            variables: A BaseVariable subclass, a variable name string,
                or a list of either.
        """
        self._duck.remove_from_group(group_name, self._resolve_var_names(variables))

    def list_var_groups(self) -> list:
        """List all variable group names.

        Returns:
            Sorted list of distinct group names.
        """
        return self._duck.list_groups()

    def get_var_group(self, group_name: str) -> list:
        """Get all variable classes in a variable group.

        Args:
            group_name: Name of the group.

        Returns:
            Sorted list of BaseVariable subclasses in the group.
        """
        names = self._duck.get_group(group_name)
        classes = []
        for name in names:
            cls = BaseVariable.get_subclass_by_name(name)
            if cls is None:
                raise NotRegisteredError(
                    f"Variable '{name}' in group '{group_name}' has no "
                    f"registered BaseVariable subclass."
                )
            classes.append(cls)
        return classes

    def close(self):
        """Close the database connection."""
        self._duck.close()
        # remove global reference
        if getattr(_local, "database", None) is self:
            self._closed = True

    def reopen(self):
        # reopen DuckDB (preserving read-only mode)
        if self._duck is None:
            self._duck = SciDuck(
                self.dataset_db_path,
                dataset_schema=self.dataset_schema_keys,
                read_only=getattr(self, "read_only", False),
            )
            self._duck.add_rollback_listener(self._forget_uncommitted_caches)
        else:
            self._duck.reopen()
        self._closed = False
        # Another process (the GUI, a MATLAB run) may have changed the
        # declaration while this connection was closed.
        self._refresh_schema_key_types()

    @property
    def record_store(self) -> RecordStore:
        """THE storage-table SQL (``scidb.store.record_store``) over this database's
        connection; rebuilt when ``reopen`` replaced the connection."""
        store = self.__dict__.get("_record_store")
        if store is None or store.connection is not self._duck:
            store = RecordStore(self._duck)
            self.__dict__["_record_store"] = store
        return store

    @property
    def inspect(self):
        """Lazy read-side observability facade (scidb.inspect.Inspector)."""
        if self._inspector is None:
            from scidb.inspect.api import Inspector

            self._inspector = Inspector(self)
        return self._inspector

    def pipeline(self, name: str, uses=()) -> "Pipeline":
        """Create a named Pipeline bound to this database. It does NOT
        activate: ``for_each`` calls register only inside its block or
        through ``pipe.for_each(...)``::

            with db.pipeline("gait") as pipe:
                for_each(...)          # registers (deferred)
            pipe.run_all()

        ``uses=`` declares other Pipelines as dependencies: their steps join
        this pipeline's graph. See ``scidb.run.pipeline.Pipeline``.
        """

        pipe = Pipeline(name, db=self, uses=uses)
        pipe._from_db_pipeline = True
        return pipe

    def set_current_db(self):
        """Set this DatabaseManager as the active global database."""
        set_current_database(self)
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
