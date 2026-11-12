"""for_each: the prepare/save seam shared by the Python and MATLAB paths — iterable resolution, variant expansion, path placeholders and input normalisation.

Moved verbatim out of foreach.py (grug follow-up, docs/claude/grugbrain-principles.md).
"""

import hashlib
import json
import os
import re
import scifor as _scifor
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from itertools import product
from pathlib import Path as _Path
from scidb.model.across_variants import AcrossVariants
from scidb.model.exceptions import AmbiguousParamError
from scidb.model.exceptions import DatabaseNotConfiguredError
from scidb.model.input_spec import find_pathinput
from scidb.model.log import Log
from scidb.model.parameter import path_input_specs_of
from scidb.model.variant import match_bare_name
from scidb.project.entities import declared_spelling
from scidb.run import glue as _glue
from scidb.run.bindings import COMBO_KEY
from scidb.run.bindings import EMPTY_SIGNATURE
from scidb.run.bindings import InputKind
from scidb.run.bindings import merge_branch_params
from scidb.run.bindings import RECORD_ID_COLUMN
from scidb.run.bindings import RecordPool
from scidb.run.bindings import RunBindings
from scidb.run.bindings import Selection
from scidb.run.bindings import signature_conflicts_with
from scidb.run.bindings import variant_signature
from scidb.run.bindings import VariantGroup
from scidb.run.foreach_call import RunContext
from scidb.run.foreach_call import RunSelection
from scidb.run.foreach_config import ForEachConfig
from scidb.run.foreach_config import RunOptions
from scidb.run.selection_axis import selection_axes_of
from scidb.store.database import database_or_none
from scidb.store.database import dataset_schema_keys_of
from scidb.store.schema_values import schema_str
from scidb.store.schema_values import scidb_key_type_errors
from scifor import ColName
from scifor import Fixed
from scifor import for_each as _scifor_for_each
from scifor import LoopHooks
from scifor import output_column_name as _output_column_name
from scifor import schema_types as _scifor_schema_types
from scifor.foreach import bind_path_inputs
from scifor.foreach import resolve_pathinput_discovery as _scifor_resolve_pathinput_discovery
from scifor.project.root import project_root as current_project_root
from typing import Any

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from scidb.run.foreach_common import (
    _ForEachState,
    _active_database,
    _build_run_bindings,
    _has_pathinput,
    _output_type_label,
    _propagate_schema,
)
from scidb.run.foreach_stamping import (
    _iterate_column_selection,
    _stamp_draft_endpoint_artifacts,
)
from scidb.run.foreach_load import (
    _compute_fixed_input_rids,
    _convert_inputs,
    _convert_inputs_for_display,
    _resolve_mapping_inputs,
)
from scidb.run.foreach_save import (
    _save_results,
)


# ---------------------------------------------------------------------------
# PathOutput variant placeholders (per-group artifact paths; no clobbering)
# ---------------------------------------------------------------------------
# PathOutput resolution is a literal str.replace of "{key}" per combo-metadata
# key (scifor/pathoutput.py::resolve — NOT str.format, so dotted names are
# legal). Branch params therefore need no new syntax: scidb injects per-group
# values into the expanded combos under the exact placeholder text, scifor's
# generic resolver substitutes them, and scidb strips the injected keys before
# save/introspect so they never pollute records or branch_params.

# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scidb.provenance.save import constant_bindings
from scidb.store.database import get_database
from scidb.store.exclusions import filter_excluded_combos
_PLACEHOLDER_RE = re.compile(r"\{([^{}]+)\}")
_VARIANT_TOKEN = "variant"


def _pathoutput_placeholders(inputs: dict, exclude_keys: set) -> "tuple[set, list]":
    """Placeholder names used by PathOutput inputs that are NOT combo-supplied
    (schema keys / metadata iterables / ColName), plus the PathOutput specs.
    """
    names: set = set()
    path_outputs: list = []
    for v in inputs.values():
        if isinstance(v, _scifor.PathOutput):
            path_outputs.append(v)
            names |= set(_PLACEHOLDER_RE.findall(str(v.template)))
    names.discard("ColName")
    return {n for n in names if n not in exclude_keys}, path_outputs


def _sanitize_path_value(value: Any) -> str:
    """Path-safe rendering of a branch_param value for filename substitution.

    Integral floats render WITHOUT the trailing ``.0``: MATLAB constants
    cross the bridge as doubles (``low_hz=20`` arrives as ``20.0``), and the
    artifact filename must not depend on which language ran the pipeline
    (``r_20.pdf`` from both, matching MATLAB's ``%g`` and Python-int runs).
    """
    if isinstance(value, float) and value.is_integer():
        s = str(int(value))
    else:
        s = str(value)
    for ch in (os.sep, "/", "\x00"):
        s = s.replace(ch, "-")
    return s


def _resolve_bp_placeholder(
    name: str, merged_bp: dict, conflicted: "dict[str, list]"
) -> "str | None":
    """Resolve one PathOutput placeholder from a variant group's branch_params.

    Bare names suffix-match namespaced keys (``variant.match_bare_name`` — the
    Variant()/branch_param() contract); ambiguity is a hard error naming the
    candidates. ``{variant}`` is an 8-char digest of the group's canonical
    signature (``bindings.variant_signature``, the ONE recipe) — the shorthand
    for many-param sweeps. Returns None for a key absent from the group
    (caller warns; the literal ``{name}`` stays in the path).
    """
    if name == _VARIANT_TOKEN:
        # Digest the canonical signature of the group's own coordinate, so
        # one template resolves to one directory. Until 2026-09-20 full
        # iteration digested the merged branch params and aggregation
        # digested the joined per-input signatures — two texts, two
        # directories, for the same group.
        return hashlib.sha256(
            variant_signature(merged_bp).encode("utf-8")
        ).hexdigest()[:8]
    try:
        key = match_bare_name(merged_bp, name)
    except AmbiguousParamError as exc:
        raise ValueError(
            f"PathOutput placeholder '{{{name}}}' is ambiguous: {exc} Use the "
            f"namespaced form."
        ) from exc
    if key is None:
        return None
    if key in conflicted:
        raise ValueError(
            f"PathOutput placeholder '{{{name}}}' matches branch_param '{key}', "
            f"which has CONFLICTING values across this call's inputs — one path "
            f"cannot represent both. Pin the inputs (Variant) or use '{{variant}}'."
        )
    return _sanitize_path_value(merged_bp[key])


def _inject_path_placeholders(
    fc: dict,
    names: set,
    merged_bp: dict,
    conflicted: "dict[str, list]",
    missing_out: set,
) -> None:
    """Inject resolved placeholder values into an expanded combo dict."""
    for name in names:
        val = _resolve_bp_placeholder(name, merged_bp, conflicted)
        if val is None:
            missing_out.add(name)
        elif name not in fc:
            fc[name] = val


def _guard_pathoutput_collisions(
    path_outputs: list,
    full_combos: list,
    colname_columns: "list | None",
    bindings: RunBindings,
    placeholder_names: set,
) -> None:
    """Hard-error when one resolved artifact path is shared by combos that
    agree on their SCHEMA/iterable identity but differ in VARIANT identity
    (their ``Selection``) — silent file loss with a one-line fix to name.
    Collisions from omitted schema keys are deliberately not errors
    (pre-existing, possibly intentional overwrites); in full iteration, combos
    at different schema locations also read different records, so variant
    difference alone must NOT trigger the guard.

    ``colname_columns``: the concrete for_columns axis (resolved before
    prepare), so ``{ColName}`` templates are covered by previewing the
    combos × columns cross product. Injected placeholder keys are excluded
    from the base identity (they derive from variant identity).
    """

    def _base_identity(fc: dict) -> tuple:
        return tuple(
            sorted(
                (k, str(v))
                for k, v in fc.items()
                if k != COMBO_KEY and k not in placeholder_names
            )
        )

    def _variant_identity(fc: dict) -> tuple:
        sel = bindings.selection_of(fc)
        if sel is None:
            return ()
        return (
            tuple(sorted((p, tuple(r)) for p, r in sel.rids.items())),
            tuple(sorted(sel.groups.items())),
        )

    def _group_bp(fc: dict) -> dict:
        sel = bindings.selection_of(fc)
        if sel is None:
            return {}
        merged, _ = merge_branch_params(
            bindings.rid_to_bp.get(r, {}) for rs in sel.rids.values() for r in rs
        )
        for sig in sel.groups.values():
            try:
                merged.update(json.loads(sig))
            except (ValueError, TypeError):
                pass
        return merged

    for po in path_outputs:
        columns = (
            colname_columns if (po.has_column_token and colname_columns) else [None]
        )
        seen: dict = {}
        for fc in full_combos:
            for col in columns:
                resolved = str(po.resolve(fc, col))
                key = (resolved, col)
                prev = seen.get(key)
                if prev is None:
                    seen[key] = fc
                    continue
                if _base_identity(prev) != _base_identity(fc):
                    continue  # schema-driven collision: user's business
                if _variant_identity(prev) == _variant_identity(fc):
                    continue  # identical combo re-reference
                bp_a, bp_b = _group_bp(prev), _group_bp(fc)
                diff_keys = sorted(
                    k for k in set(bp_a) | set(bp_b) if bp_a.get(k) != bp_b.get(k)
                )
                suggestion = (
                    "{" + str(diff_keys[0]).rsplit(".", 1)[-1] + "}"
                    if diff_keys
                    else "{variant}"
                )
                raise ValueError(
                    f"PathOutput {str(po.template)!r} resolves to the SAME path "
                    f"{resolved!r} for multiple variant groups"
                    + (f" (differing branch_params: {diff_keys})" if diff_keys else "")
                    + f" — each group's file would overwrite the previous one. "
                    f"Add a distinguishing placeholder, e.g. '...{suggestion}...' "
                    f"or '...{{variant}}...'."
                )


# ---------------------------------------------------------------------------
# Prepare / save seam functions
#
# These factor the Python-driven body of ``for_each`` so that:
#   - Python pipelines (the ``for_each`` orchestration above) call them
#     in sequence with a Python ``for`` loop in between (delegated to
#     ``scifor.for_each``).
#   - MATLAB pipelines (the ``scimatlab.bridge.for_each_prepare`` /
#     ``for_each_save`` bridge entries) call them in sequence with the
#     MATLAB-side ``+scifor/for_each.m`` running the inner loop in
#     between.
#
# Both callers get identical correctness (variant expansion,
# branch_params, ``__upstream``, lineage save) because they share the
# same prepare and save code.
# ---------------------------------------------------------------------------


def _normalize_glue(
    glue: "dict[str, Any] | None", inputs: dict, glue_language: str
) -> "tuple[dict, dict, dict, dict]":
    """Prepare stage: normalize and validate the call's glue chains, and apply
    the constant-fed ones. Returns ``(glue_chains, deferred_glue_chains,
    inputs, constant_glue)``.

    Runs first so a malformed chain fails before any data is loaded (and
    before the dry-run shortcut, which must display the same call the real
    run would make). ``inputs`` comes back as a COPY when a Parameter-fed
    chain was applied to it, else unchanged — it holds what the FUNCTION
    receives. ``constant_glue`` is ``{param: glue.ConstantGlue}`` for each
    glued constant: its raw value is what identity records.
    """
    glue_chains = _glue.normalize_glue(glue)
    # Chains still to apply downstream (the fusion point, or MATLAB's loop).
    # ``glue_chains`` itself stays WHOLE: it feeds ForEachConfig, where glue
    # names contribute to the call_id, and a Parameter-fed chain is just as
    # much part of the call site as a variable-fed one.
    deferred_glue_chains = glue_chains
    constant_glue: dict = {}
    if glue_chains:
        _glue.log_chains(glue_chains)
        _glue.refuse_pathinput_glue(inputs, glue_chains)

        # Split by what feeds each param, then validate BOTH halves before
        # running any body — the two halves obey opposite language rules
        # (docs/claude/free-code-glue-nodes.md §1), and a run that is going to
        # be refused should be refused before it reshapes anything.
        constant_chains, deferred_glue_chains = _glue.split_constant_chains(
            inputs, glue_chains
        )
        _glue.check_constant_glue_language(constant_chains, glue_language)
        _glue.check_run_language(deferred_glue_chains, glue_language)

        # Constant-fed glue (the Parameter case) is applied HERE, before Step 8
        # builds the version keys, so the glued value's hash can enter
        # identity (as a history hop; the RAW value stays the constant).
        # ``inputs`` is copied first: it is the caller's dict, and mutating it
        # would double-apply the chain if the caller reused it for a second
        # for_each call.
        if constant_chains:
            inputs = dict(inputs)
            constant_glue = _glue.apply_constant_glue(inputs, constant_chains)
    else:
        Log.debug("[glue] no glue chains on this call")
    return glue_chains, deferred_glue_chains, inputs, constant_glue


def _resolve_iterables(
    metadata_iterables: dict, inputs: dict, db, distribute: bool
) -> "tuple[dict, list | None, list, Any]":
    """Prepare stage: settle the metadata iterables before any data loads.

    Records which keys the caller passed explicitly, fills ``[]`` from the
    database, lets PathInput discovery fill (or drive) the rest and reports
    what nothing could fill, propagates the schema to scifor, and stringifies
    schema-key values (numeric keys canonicalized, discovered combos too).

    Returns ``(metadata_iterables, discovered_combos, needs_resolve,
    resolved_db)`` — discovery may hand back a NEW iterables dict;
    ``discovered_combos`` is None unless discovery drives iteration.
    """
    # Track which keys the user passed with explicit (non-empty) values.
    # Keys passed as an empty sequence ([], (), empty numpy array) are
    # about to be resolved from the DB (Step 2) or the filesystem
    # (Step 3) — those should not count as "user explicit" in Step 3's
    # discovery branch, since the user delegated their values to
    # resolution rather than asserting intent.
    #
    # Defensively accept any sized non-string sequence so that callers
    # that pass numpy arrays / tuples (in addition to the usual lists)
    # are classified the same way the MATLAB bridge's [] normalization
    # produces.
    def _is_empty_sequence(v):
        if isinstance(v, str):
            return False  # strings are scalar values, never "empty iterables"
        try:
            return len(v) == 0
        except TypeError:
            return False

    user_explicit_keys = {
        k for k, v in metadata_iterables.items() if not _is_empty_sequence(v)
    }
    Log.debug(
        f"entry: metadata_iterables keys={list(metadata_iterables.keys())}, "
        f"types={[type(v).__name__ for v in metadata_iterables.values()]}, "
        f"lens={[(len(v) if hasattr(v, '__len__') else 'N/A') for v in metadata_iterables.values()]}, "
        f"user_explicit_keys={sorted(user_explicit_keys)}"
    )

    # Step 2: Resolve empty lists to all distinct values from the database
    needs_resolve = [
        k for k, v in metadata_iterables.items() if isinstance(v, list) and len(v) == 0
    ]
    resolved_db = None
    # Keys the database could not fill. NOT warned about here: PathInput
    # discovery (Step 3) fills schema keys from disk and is the normal source
    # on a first run against a fresh database. Warning at this point produced a
    # false "0 iterations" line that the real iteration count contradicted
    # milliseconds later. The warning now fires after Step 3, for keys nothing
    # could fill — the case that really does mean zero iterations.
    _unresolved_from_db: list[str] = []
    if needs_resolve:
        Log.debug(
            f"resolving {len(needs_resolve)} empty list(s) from database: {needs_resolve}"
        )
        resolved_db = db
        if resolved_db is None:
            try:
                resolved_db = get_database()
            except DatabaseNotConfiguredError:
                raise ValueError(
                    f"Empty list [] was passed for {needs_resolve}, which means "
                    f"'use all levels', but no database is available. Either pass "
                    f"db= to for_each or call configure_database() first."
                )
        for key in needs_resolve:
            values = resolved_db.distinct_schema_values(key)
            if not values:
                _unresolved_from_db.append(key)
                Log.debug(
                    f"no values found for '{key}' in database — deferring to "
                    f"PathInput discovery before deciding this is 0 iterations"
                )
            else:
                Log.debug(f"resolved '{key}' from database: {len(values)} values")
            metadata_iterables[key] = values
    else:
        Log.debug("no empty lists to resolve from database")

    # --- Step 3: PathInput discovery.  Discovery runs whenever a PathInput
    # is present; delegated to scifor.foreach.resolve_pathinput_discovery so
    # scidb and scifor make the same discovery/leniency decision — including
    # dropping a key a fully static (no {key} placeholders) PathInput can
    # never supply, instead of leaving it as an empty iterable that would
    # zero out the Cartesian product. Its role otherwise depends on what the
    # caller supplied:
    #
    #   * No metadata_iterables at all → adopt every discovered key/value.
    #   * All template keys passed as [] → fill from disk and use the
    #     discovered combos directly. This avoids Cartesian-product
    #     "invention" of combos that have no file on disk (regression
    #     covered by tests/matlab/scidb/TestForEachSchemaFiltering).
    #   * Any template key has explicit user values → user intent is
    #     authoritative. Empty-list keys still get filled from disk, but
    #     no filtering happens; the Cartesian product of metadata_iterables
    #     drives base_combos. Combos with missing files fail at runtime
    #     and are surfaced as "missing" by check_node_state. ---
    _discovered_combos = None
    if _has_pathinput(inputs):
        Log.debug("PathInput detected, running filesystem discovery")
        pi = find_pathinput(inputs)
        if pi is not None:
            # Keys with provably NO source: the database could not fill them
            # (Step 2) and this template has no {key} placeholder, so discovery
            # cannot either. Step 3b drops them from the iteration a few lines
            # below; tell discovery now so a key on its way out does not force
            # the Cartesian-product fallback and invent combos with no file
            # behind them. Computed from Step 2's result plus the template's own
            # placeholders — no extra disk access.
            _unsupplyable = {
                k for k in _unresolved_from_db if k not in set(pi.placeholder_keys())
            }
            if _unsupplyable:
                Log.debug(
                    f"keys with no possible source (DB empty, not a placeholder of "
                    f"{pi.path_template!r}): {sorted(_unsupplyable)} — excluded from "
                    f"the discovery-combos decision"
                )
            # "Explicit" keys are those the user passed with non-empty
            # values — a value filled from DB (Step 2) or disk is an
            # auto-fill, not intent.
            metadata_iterables, _discovered_combos = _scifor_resolve_pathinput_discovery(
                pi,
                metadata_iterables,
                user_explicit_keys,
                log=Log.debug,
                unsupplyable_keys=_unsupplyable,
            )
            # O(1) per run and it decides what actually executes, so INFO:
            # 419 disk combos vs a 952-cell Cartesian product is the difference
            # between a clean run and 533 phantom "file not found" failures.
            if _discovered_combos is not None:
                Log.info(
                    f"PathInput discovery drives iteration directly: "
                    f"{len(_discovered_combos)} combo(s) found on disk "
                    f"(no Cartesian product of iterables)"
                )
            else:
                Log.info(
                    "PathInput discovery did not drive iteration; the Cartesian "
                    "product of the metadata iterables will — combos with no file "
                    "behind them will fail at runtime"
                )
    else:
        Log.debug("no PathInput detected, skipping filesystem discovery")

    # Step 3b: Now that discovery has had its turn, report on the keys the
    # database could not fill (Step 2). Three outcomes, three different lines:
    # discovery filled it (INFO, says where the values came from), discovery
    # dropped the key as unsupplyable by this template (DEBUG, expected), or
    # nothing could fill it (WARN — this one really is 0 iterations).
    if _unresolved_from_db:
        _filled_by_discovery: dict[str, int] = {}
        _still_empty: list[str] = []
        for key in _unresolved_from_db:
            if key not in metadata_iterables:
                Log.debug(
                    f"'{key}' was dropped by PathInput discovery — the template "
                    f"has no {{{key}}} placeholder and can never supply it"
                )
                continue
            n_values = len(metadata_iterables.get(key) or [])
            if n_values:
                _filled_by_discovery[key] = n_values
            else:
                _still_empty.append(key)
        if _filled_by_discovery:
            Log.info(
                "PathInput discovery filled "
                + ", ".join(
                    f"'{k}'={n} value(s)"
                    for k, n in sorted(_filled_by_discovery.items())
                )
                + " from disk (the database had none)"
            )
        # A key nothing could fill is DROPPED as long as some other key still
        # has values, rather than left as an empty list that zeroes the
        # Cartesian product in Step 12 and silently turns the whole run into
        # "0 combos". This is the same decision PathInput discovery already
        # makes for a key a static template can never supply (Step 3's
        # comment) — applied to the database source, which had no equivalent.
        #
        # It matters because a caller iterating a schema level asks for EVERY
        # key with `key=[]` ("all levels"): a two-key schema [pass, cycle]
        # whose records are all saved at pass level has no cycle values at
        # all, so `pass=[], cycle=[]` meant zero iterations even though `pass`
        # resolved to three. Not iterating a key you have no values for is a
        # coarser iteration, not an empty one.
        #
        # When NOTHING resolved, the empty lists stay and the run really is 0
        # iterations — an empty database should not silently become a single
        # data-less call.
        def _has_values(v) -> bool:
            try:
                return len(v) > 0
            except TypeError:
                return v is not None  # a bare scalar is one value

        _any_resolved = any(
            _has_values(v)
            for k, v in metadata_iterables.items()
            if k not in _still_empty
        )
        for key in _still_empty:
            if _any_resolved:
                del metadata_iterables[key]
                Log.info(
                    f"no values found for '{key}': the database has none and "
                    f"PathInput discovery could not supply any — dropping it "
                    f"from the iteration (other keys still have values, so "
                    f"this is a coarser iteration, not zero iterations)"
                )
            else:
                Log.warn(
                    f"no values found for '{key}': the database has none and "
                    f"PathInput discovery could not supply any, 0 iterations"
                )

    # Step 4: Propagate schema keys to scifor so distribute and DataFrame detection work
    Log.debug("propagating schema keys to scifor")
    _propagate_schema(db, distribute)
    if db and hasattr(db, "dataset_schema_keys"):
        Log.debug(f"schema keys propagated: {db.dataset_schema_keys}")

    # Step 5: Stringify metadata_iterables values for schema keys.
    # load_all_as_df (spread layout) stringifies schema columns in loaded DataFrames
    # (DB returns typed values like np.int64); combo metadata must match to filter correctly.
    Log.debug("stringifying metadata iterable values for schema keys")
    _resolved_db_for_str = db
    if _resolved_db_for_str is None:
        _resolved_db_for_str = database_or_none()
    if _resolved_db_for_str is not None and hasattr(
        _resolved_db_for_str, "dataset_schema_keys"
    ):

        _sk_set = set(_resolved_db_for_str.dataset_schema_keys)
        _sk_types = getattr(_resolved_db_for_str, "dataset_schema_key_types", {}) or {}
        stringify_count = 0
        for key in list(metadata_iterables.keys()):
            if key in _sk_set:
                # Declared-numeric keys canonicalize unconditionally — every
                # spelling of the same number ("001", 1, 1.0) collapses to one
                # identity before stringification (scifor owns the rule and
                # the de-duplication).
                with scidb_key_type_errors():
                    values = _scifor_schema_types.canonicalize_values(
                        key, metadata_iterables[key], _sk_types.get(key)
                    )
                metadata_iterables[key] = [schema_str(v) for v in values]
                stringify_count += 1
        Log.debug(f"stringified {stringify_count} schema key iterable(s)")

        # Filesystem-discovered combos carry disk spellings ("001"); apply
        # the same declared-numeric canonicalization so discovery-driven and
        # explicit-iterable runs agree on schema-key identity. Combos that
        # collapse (both "6MWT-1.mat" and "6MWT-001.mat" on disk) are
        # de-duplicated so one identity is not iterated twice.
        canon_keys = [k for k, t in _sk_types.items() if t == "numeric"]
        if _discovered_combos is not None and canon_keys:
            with scidb_key_type_errors():
                canonical = _scifor_schema_types.canonicalize_combos(
                    _discovered_combos, _sk_types
                )
            for combo in canonical:
                for k in canon_keys:
                    if k in combo:
                        combo[k] = schema_str(combo[k])
            _discovered_combos[:] = canonical
            Log.debug(
                f"canonicalized numeric schema key(s) {canon_keys} in "
                f"{len(_discovered_combos)} discovered combo(s)"
            )
    else:
        Log.debug("no database available for schema stringification, skipping")
    return metadata_iterables, _discovered_combos, needs_resolve, resolved_db


def _existing_combos(
    metadata_iterables: dict, inputs: dict, needs_resolve: list, resolved_db, db
) -> "list[dict] | None":
    """Prepare stage: the combos to run, restricted to schema combinations that
    exist in the database and not excluded — or None when the iterables were
    explicit (or a PathInput drives discovery), leaving the Cartesian product
    to scifor.

    THE one spelling of this filter: the dry-run preview calls it too, so the
    printed iteration count is the real run's (it used to carry its own copy,
    which skipped the configured-database fallback for exclusions).
    """
    # Step 9: Pre-filter to only schema combinations that actually exist in the database.
    all_combos = None
    if needs_resolve and not _has_pathinput(inputs):
        Log.debug("pre-filtering combos to only existing schema combinations")

        filter_db = resolved_db
        schema_keys_set = set(filter_db.dataset_schema_keys)
        keys = list(metadata_iterables.keys())
        schema_indices = [i for i, k in enumerate(keys) if k in schema_keys_set]
        filter_keys = [keys[i] for i in schema_indices]

        if filter_keys:
            value_lists = [metadata_iterables[k] for k in keys]
            raw_combos = list(product(*value_lists))

            existing = filter_db.distinct_schema_combinations(filter_keys)
            existing_set = set(existing)

            filtered = [
                dict(zip(keys, combo, strict=False))
                for combo in raw_combos
                if tuple(schema_str(combo[i]) for i in schema_indices) in existing_set
            ]
            removed = len(raw_combos) - len(filtered)
            if removed > 0:
                # O(1) per run and changes what will execute — INFO.
                Log.info(
                    f"filtered {removed} non-existent schema combinations "
                    f"(from {len(raw_combos)} to {len(filtered)})"
                )
            else:
                Log.debug(f"all {len(raw_combos)} combos exist in database")
            all_combos = filtered
    else:
        Log.debug(
            "skipping combo pre-filtering (no empty list resolution or PathInput detected)"
        )

    # Step 9.5: Schema exclusions — filter out excluded combos.
    # (No override-hash cache key needed: the bipartite graph invalidates
    # precisely — an aggregation's invocation_id includes its input record_ids,
    # so changing which combos are excluded changes the input set and forces a
    # recompute; unaffected outputs correctly skip.)
    _exclusion_db = db or resolved_db
    if _exclusion_db is None:
        _exclusion_db = database_or_none()

    if _exclusion_db is not None:
        if all_combos is not None:
            _before = len(all_combos)
            all_combos = filter_excluded_combos(
                all_combos,
                _exclusion_db.dataset_schema_keys,
                _exclusion_db,
            )
            _after = len(all_combos)
            if _before != _after:
                Log.info(
                    f"schema exclusions removed {_before - _after} combo(s) "
                    f"(from {_before} to {_after})"
                )
        else:
            Log.debug(
                "all_combos is None (explicit iterables); "
                "exclusion filtering will be skipped at combo level"
            )
    else:
        Log.debug("no database available, skipping schema exclusion filtering")
    return all_combos


def _dry_run_preview(
    fn,
    inputs: dict,
    metadata_iterables: dict,
    *,
    needs_resolve: list,
    resolved_db,
    db,
    as_table,
    distribute: bool,
    output_names: list,
    locations,
    cancel_check,
    project_root: "_Path | None" = None,
) -> None:
    """Prepare stage (dry run only): convert inputs for display and let scifor
    print the iteration, over the SAME combos the real run would process
    (:func:`_existing_combos`)."""
    Log.debug("dry_run=True, converting inputs for display and delegating to scifor")
    display_inputs = _convert_inputs_for_display(inputs)
    all_combos = _existing_combos(
        metadata_iterables, inputs, needs_resolve, resolved_db, db
    )
    _scifor_for_each(
        fn,
        display_inputs,
        dry_run=True,
        as_table=as_table,
        distribute=distribute,
        outputs=output_names,
        locations=locations,
        project_root=project_root,
        hooks=LoopHooks(all_combos=all_combos, cancel_check=cancel_check),
        **metadata_iterables,
    )


def _iteration_level_of(metadata_iterables: dict, db) -> list[str]:
    """The schema keys a call iterates, in dataset order (`[]` = one call).

    THE one definition of a call's iteration level: the dataset schema keys
    present in the resolved metadata iterables. A non-schema iterable is a
    sweep over something else and does not count; the VALUES do not matter
    (`subject=["SS01"]` iterates the subject level as much as
    `subject=[]` does).
    """

    keys = dataset_schema_keys_of(db)
    present = set(metadata_iterables or {})
    return [k for k in keys if k in present]


def _build_call_identity(
    fn,
    inputs: dict,
    where,
    distribute: bool,
    as_table,
    glue_chains,
    level=None,
    constant_glue: "dict | None" = None,
) -> "tuple[dict, str]":
    """Prepare stage: the call's ``ForEachConfig`` config keys and call_id.

    ``inputs`` holds GLUED constants (what the function receives), but a
    glued constant is recorded by its RAW value — the value the Parameter was
    declared with, which is what the canvas shows and what names the call
    site. The glue's identity goes under ``glue.CONSTANT_GLUE_KEY`` (a version
    key, not a call-site key), from which ``provenance_save.constant_bindings``
    derives the virtual rid the consumer binds. Glue names are part of the
    call site."""
    Log.debug("building ForEachConfig config keys")
    constant_glue = constant_glue or {}
    identity_inputs = {
        **inputs,
        **{p: cg.raw for p, cg in constant_glue.items()},
    }
    config = ForEachConfig(
        fn=fn,
        inputs=identity_inputs,
        where=where,
        distribute=distribute,
        as_table=as_table,
        glue=glue_chains,
        level=level,
    )
    config_keys = config.to_config_keys()
    if constant_glue:
        config_keys[_glue.CONSTANT_GLUE_KEY] = _glue.constant_glue_config_keys(
            constant_glue
        )
    call_id = config.to_call_id()
    Log.debug(
        f"ForEachConfig: call_id={call_id}, config_keys={list(config_keys.keys())}"
    )
    return config_keys, call_id


def _load_all_inputs(
    fn_name: str,
    inputs: dict,
    db,
    where,
    deferred_glue_chains,
    glue_language,
    locations=None,
    level=(),
    schema_keys=(),
):
    """Prepare stage: bulk-load every input into a DataFrame (with
    ``__record_id`` / ``__branch_params``), narrow them to ``locations``, resolve
    mapping inputs, and fuse deferred glue onto the loaded tables.

    Returns ``(loaded_inputs, mapping_inputs, glue_fusion, selection_axes)``.
    """
    # Step 10: Load all inputs into DataFrames (with __record_id and __branch_params)
    Log.debug(f"loading {len(inputs)} input(s) into DataFrames")
    unnarrowed = _convert_inputs(inputs, db, where)
    # Before anything reads the record ids: every call's Selection, its lineage
    # and the skip gate all derive from these rows, so the three cannot disagree
    # about what a selection left out. schema_str because a date key loads with
    # its original type while a selection names it by its stored text.
    loaded_inputs = _scifor.narrow_inputs(
        unnarrowed, locations, context=f"for_each({fn_name})", spell=schema_str
    )
    selection_axes = selection_axes_of(
        unnarrowed, loaded_inputs, level, schema_keys, context=f"for_each({fn_name})"
    )
    df_count = sum(
        1
        for v in loaded_inputs.values()
        if isinstance(v, __import__("pandas").DataFrame)
    )
    Log.debug(
        f"loaded {df_count} DataFrame input(s), {len(loaded_inputs) - df_count} other(s)"
    )
    mapping_inputs = _resolve_mapping_inputs(inputs, loaded_inputs, db)

    # --- Glue fusion: reshape the bulk loaded table in memory, before Step 11
    #     renames __record_id → __rid_{param}. Placed here on purpose: the
    #     table already carries schema-key columns (so per-combo slicing is
    #     untouched) and provenance bookkeeping has not started yet. MATLAB
    #     runs delegate all of this to for_each_prepare too, so Python glue
    #     would apply there identically — which is exactly why glue is
    #     required to match the run's language. ---
    glue_fusion = _glue.GlueFusion()
    if deferred_glue_chains:
        with Log.step(f"apply_glue({fn_name})"):
            # MATLAB glue bodies run in +scidb/for_each.m, after prepare
            # returns and before the loop — but the *identity* half (the
            # virtual glue records) is computed here either way, so the two
            # languages share one provenance recipe.
            if glue_language != "python":
                Log.info(
                    f"[glue] {len(deferred_glue_chains)} chain(s) deferred to "
                    f"the {glue_language} side; identity recorded here"
                )
            glue_fusion = _glue.fuse_glue(
                loaded_inputs,
                deferred_glue_chains,
                schema_keys=dataset_schema_keys_of(db),
                apply_bulk=(glue_language == "python"),
            )
    return loaded_inputs, mapping_inputs, glue_fusion, selection_axes


def _track_variants(loaded_inputs: dict):
    """Prepare stage: map every loaded record_id to its branch_params and sort
    the inputs by kind (plain = rid-tracked, Fixed = one pinned rid,
    ColumnSelection = pruning only). ``__branch_params`` is then stripped
    from the frames — it lives on in ``rid_to_bp``.

    Returns ``(loaded_inputs, rid_to_bp, tracked_params, fixed_rid_values,
    colsel_params)``; ``loaded_inputs`` is a new dict, the argument is not
    mutated.
    """
    loaded_inputs = dict(loaded_inputs)
    # --- Step 11: Variant tracking: build the rid→bp mapping and sort inputs by kind ---
    #
    # A loaded frame keeps its `__record_id` column exactly as the loader
    # wrote it. Which of its records a combination reads is decided at combo
    # expansion (a `Selection` per combo, `scidb.run.bindings`) and applied by
    # scifor through the `LoopHooks.select_rows` hook — NOT by renaming the column per
    # input and pushing those names into scifor's schema, which is what this
    # step did until 2026-09-20 (and then stripped back out in four places).
    import pandas as pd

    Log.debug("building variant tracking (rid->branch_params mapping)")
    rid_to_bp: dict = {}  # {record_id: branch_params_dict}
    tracked_params: list = []  # plain variable inputs: rid is an iteration axis / a pool
    fixed_rid_values: dict = {}  # {param_name: record_id} for Fixed inputs
    # ColumnSelection inputs participate in non-existent-combo PRUNING but NOT in
    # rid expansion. Coupling them in (as plain DataFrame inputs are) would
    # change variant semantics — it perturbs Variant branch_param pinning and
    # for_columns aggregation. So we track them separately and use only their
    # schema-location coverage to prune combos.
    colsel_params: list = []  # param_names whose input is a ColumnSelection wrapper

    for param_name, data in list(loaded_inputs.items()):
        # Extract DataFrame from a wrapper if needed. Both _scifor.Fixed and
        # _scifor.ColumnSelection expose a `.data` DataFrame, so they MUST be
        # distinguished by TYPE — a bare ``hasattr(data, 'data')`` check
        # misclassifies a ColumnSelection as Fixed.  Three distinct treatments:
        #   - plain DataFrame  -> full rid tracking (expansion + schema + pruning)
        #   - Fixed            -> single fixed record_id, no iteration rid key
        #   - ColumnSelection  -> pruning only (tracked in colsel_params), NOT
        #                         rid expansion/schema (preserves Variant /
        #                         for_columns semantics)
        df = None
        is_fixed = False
        is_colsel = False
        if isinstance(data, pd.DataFrame):
            df = data
        elif isinstance(data, _scifor.Fixed) and isinstance(data.data, pd.DataFrame):
            df = data.data
            is_fixed = True
        elif isinstance(data, _scifor.ColumnSelection) and isinstance(
            data.data, pd.DataFrame
        ):
            df = data.data
            is_colsel = True

        if df is None or RECORD_ID_COLUMN not in df.columns:
            if df is not None:
                Log.debug(
                    f"input '{param_name}' "
                    f"({type(data).__name__}) has no {RECORD_ID_COLUMN} column — "
                    f"rid tracking disabled for this input"
                )
            continue

        # Build rid→bp from this input's DataFrame (vectorized for performance)
        bp_col = "__branch_params" if "__branch_params" in df.columns else None
        # Filter out None record_ids using vectorized operation
        valid_mask = df[RECORD_ID_COLUMN].notna()
        if valid_mask.any():
            valid_rids = df.loc[valid_mask, RECORD_ID_COLUMN]
            if bp_col:
                valid_bps = df.loc[valid_mask, bp_col]
                # Use zip over columns instead of iterrows() - 10-100x faster
                for rid, bp_raw in zip(valid_rids, valid_bps, strict=False):
                    rid_to_bp[rid] = (
                        json.loads(bp_raw or "{}") if isinstance(bp_raw, str) else {}
                    )
            else:
                for rid in valid_rids:
                    rid_to_bp[rid] = {}

        if is_fixed:
            # The one record a Fixed input pins. The loaded frame is the WHOLE
            # variable (scifor applies the pin per call), so the pin's metadata
            # is applied here to find it: a pin on a subject-level variable
            # by `subject="01"` matches one of N rows. Before 2026-09-20 only
            # a frame that was ALREADY one row counted, so a partial pin
            # recorded no rid, no edge, and no `__rid_<param>` on the combo —
            # the Fixed input was invisible to provenance and to the skip gate.
            _pinned = df
            for _k, _v in (getattr(data, "fixed_metadata", None) or {}).items():
                if _k in _pinned.columns:
                    _pinned = _pinned[_pinned[_k].astype(str) == schema_str(_v)]
            # Count RECORDS, not rows: a table-valued record (DataFrame mode,
            # or a custom to_db table) is loaded as one row per stored row,
            # all carrying its record id. Counting rows called one 2-row
            # record "2 records" and dropped its edge (2026-10-18).
            _pinned_rids = list(
                dict.fromkeys(_pinned[RECORD_ID_COLUMN].dropna().astype(str))
            )
            if len(_pinned_rids) == 1:
                fixed_rid_values[param_name] = _pinned_rids[0]
            elif len(_pinned_rids) > 1:
                Log.warn(
                    f"input '{param_name}' (Fixed): the pin "
                    f"{getattr(data, 'fixed_metadata', {})} matches "
                    f"{len(_pinned_rids)} records, not one — no input edge can be "
                    f"recorded for it. Pin every schema key of that variable or "
                    f"narrow with where=."
                )
            Log.debug(
                f"input '{param_name}' (Fixed): rid tracked "
                f"via fixed_rid_values (no iteration axis)"
            )
        elif is_colsel:
            # ColumnSelection: mark the param for existence-based combo
            # pruning. Deliberately NOT a tracked param: no rid expansion.
            #
            # Not expanding does NOT mean absent from lineage. The rids are
            # collected separately into `colsel_rid_per_combo` and land in
            # each combo's `Selection`, which is what the save path binds
            # `_invocation_input` edges from. Before 2026-09-15 they were not,
            # and every record built from a column-selected input was saved
            # with no record of what it consumed.
            colsel_params.append(param_name)
            Log.debug(
                f"input '{param_name}' (ColumnSelection): "
                f"pruning-only (no rid expansion)"
            )
        else:
            # Plain DataFrame: full rid tracking (expansion + pruning).
            tracked_params.append(param_name)
            Log.debug(
                f"input '{param_name}' "
                f"({type(data).__name__}): rid-tracked"
            )

    Log.debug(
        f"variant tracking: {len(rid_to_bp)} record_id(s) mapped, "
        f"{len(tracked_params)} tracked input(s): {tracked_params}, "
        f"{len(fixed_rid_values)} fixed input rid(s): {list(fixed_rid_values.keys())}"
    )

    # Strip __branch_params from all DataFrames (now tracked via rid_to_bp)
    for param_name, data in list(loaded_inputs.items()):
        if isinstance(data, pd.DataFrame) and "__branch_params" in data.columns:
            loaded_inputs[param_name] = data.drop(columns=["__branch_params"])
    return loaded_inputs, rid_to_bp, tracked_params, fixed_rid_values, colsel_params


@dataclass
class _RidIndex:
    """Where each rid-bearing input's records sit, over one run's lookup keys.

    The ONE owner of the combo-location geometry both expansion modes read:
    the rid mappings (plain and ColumnSelection inputs, kept apart — see the
    comment in :func:`_build_rid_index`), which lookup positions each input
    populates, and ColumnSelection existence coverage. The two probes are the
    comparisons a combo location is matched with; they used to be closures
    inside ``_for_each_prepare``.
    """

    lookup_keys: list
    rid_per_combo: dict
    rid_populated_idx: dict
    colsel_rid_per_combo: dict
    colsel_existence: dict
    colsel_coverage: list

    def probe_key(self, param: str, schema_vals: tuple) -> tuple:
        """The key to look up *param*'s mapping with for a combo location.

        ``schema_vals`` carries a real value in every position; a coarse input's
        mapping keys carry "" wherever that input has no axis. Blank the same
        positions before probing, so a subject-level input matches at every
        session/trial below it instead of matching nothing — which pruned the
        ENTIRE grid, since a combo with no rids for any rid key is skipped.

        An input that populates every lookup key probes with ``schema_vals``
        unchanged: the overwhelmingly common case, byte-identical to the exact
        match this replaced. Same shape as ``colsel_present``, which has
        always compared this way — that asymmetry is why a coarse
        ``ColumnSelection`` input worked while a coarse plain one did not.
        """
        populated = self.rid_populated_idx.get(param)
        if populated is None or len(populated) == len(self.lookup_keys):
            return schema_vals
        return tuple(
            schema_vals[i] if i in populated else "" for i in range(len(self.lookup_keys))
        )

    def colsel_present(self, schema_vals: tuple) -> bool:
        """True if every ColumnSelection input has data at this combo location.

        ``schema_vals`` is the combo key over ``self.lookup_keys``. Compared only on
        the keys each input actually populates, so a ColumnSelection stored at a
        coarser level (finer keys absent from its frame) still matches.
        """
        n = len(self.lookup_keys)
        for present, populated_idx in self.colsel_coverage:
            probe = tuple(
                schema_vals[i] if i in populated_idx else "" for i in range(n)
            )
            if probe not in present:
                return False
        return True


def _build_rid_index(
    loaded_inputs: dict, tracked_params: list, colsel_params: list, _lookup_keys: list
) -> _RidIndex:
    """Combo expansion: index every rid-tracked and ColumnSelection input by
    schema location (see :class:`_RidIndex`)."""
    import pandas as pd

    # For each tracked input, map combo_tuple → [rid_values at that combo].
    # Keyed by PARAM (2026-09-20; was the `__rid_{param}` column name).
    rid_per_combo: dict = {}
    # param -> set of _lookup_keys positions that input actually populates
    rid_populated_idx: dict = {}
    # The SAME mapping, built for ColumnSelection inputs, kept in its own dict.
    #
    # ColumnSelection is deliberately not a tracked param: coupling it into
    # rid expansion changes Variant pinning and for_columns semantics. But
    # the tracked set was also the only thing feeding the SAVE path's input
    # binding, so switching off iteration switched off LINEAGE too — records
    # saved from a column-selected input got no `_invocation_input` edges at
    # all (2026-09-15: GAITRiteSymmetry, which then could not be told apart
    # from its own superseded generation and plotted as replicates of it).
    # Two unrelated jobs on one wire. This dict is the second wire: it feeds
    # each combo's `Selection`, which the save path reads, and nothing that
    # decides how many times the function is called.
    colsel_rid_per_combo: dict = {}
    for param_name in list(tracked_params) + list(colsel_params):
        _is_colsel_input = param_name in colsel_params
        data = loaded_inputs.get(param_name)

        # Extract DataFrame from Fixed wrapper if needed
        df = None
        if isinstance(data, pd.DataFrame):
            df = data
        elif hasattr(data, "data") and isinstance(data.data, pd.DataFrame):
            df = data.data

        if df is None or RECORD_ID_COLUMN not in df.columns:
            continue
        # Group only by the schema keys this input actually POPULATES. A variable
        # stored at a coarser level than the dataset schema (e.g. a subject/session/
        # speed/trial variable in a schema that also has 'cycle') carries the finer
        # keys as all-NaN columns. Including an all-NaN column here is catastrophic:
        # pandas groupby drops NaN-key groups by default, so EVERY row is dropped →
        # empty mapping → no rids tracked → no __upstream → no _invocation_input
        # edges (severed input provenance, the precondition for the orphan cascade).
        # The mapping key is still built over the full _lookup_keys, with the
        # keys this input does not populate filled with "". Those "" positions
        # are why rid_populated_idx exists: a combo key has REAL values in every
        # position, so the full-iteration lookup below has to blank the same
        # positions before probing or it would never match (see _RidIndex.probe_key).
        schema_cols_in_df = [
            k for k in _lookup_keys if k in df.columns and not df[k].isna().all()
        ]
        # Populated for BOTH kinds. It is read only by `_RidIndex.probe_key`, and
        # the full-iteration expansion loop probes `rid_per_combo` alone — so a
        # ColumnSelection entry here changes no combo, and lets the lineage
        # lookup below blank the same positions a coarse input leaves empty.
        rid_populated_idx[param_name] = {_lookup_keys.index(k) for k in schema_cols_in_df}
        mapping: dict = {}
        # Dedupe rids per group so DataFrame-mode inputs (one DuckDB row
        # per inner-table row, all sharing a single record_id) don't
        # produce N duplicate combos. We preserve insertion order via
        # dict.fromkeys.
        if schema_cols_in_df:
            for combo_vals, group in df.groupby(schema_cols_in_df, sort=False):
                raw_key = combo_vals if isinstance(combo_vals, tuple) else (combo_vals,)
                # Expand to ALL _lookup_keys, filling missing cols with ""
                col_val = {
                    sk: ("" if v is None else schema_str(v))
                    for sk, v in zip(schema_cols_in_df, raw_key, strict=False)
                }
                key = tuple(col_val.get(sk, "") for sk in _lookup_keys)
                mapping[key] = list(dict.fromkeys(group[RECORD_ID_COLUMN].tolist()))
        else:
            # No lookup cols in df — use all-empty key
            mapping[tuple("" for _ in _lookup_keys)] = list(
                dict.fromkeys(df[RECORD_ID_COLUMN].tolist())
            )
        if _is_colsel_input:
            colsel_rid_per_combo[param_name] = mapping
        else:
            rid_per_combo[param_name] = mapping

    # Existence coverage for ColumnSelection inputs: the set of schema-location
    # keys (over _lookup_keys) each one actually has data for. Used purely to
    # PRUNE non-existent Cartesian combos — no rid expansion, no schema change.
    # Keyed identically to rid_per_combo so the same combo_key tuple compares.
    colsel_existence: dict = {}  # param_name -> set[tuple]
    for param_name in colsel_params:
        data = loaded_inputs.get(param_name)
        df = (
            data.data
            if (hasattr(data, "data") and isinstance(data.data, pd.DataFrame))
            else None
        )
        if df is None:
            continue
        # Same all-NaN exclusion as rid_per_combo above: a coarser-level
        # ColumnSelection input carries finer schema keys as all-NaN columns;
        # grouping by them would drop every row (pandas dropna), yielding EMPTY
        # coverage → the prune step would then drop ALL combos and nothing runs.
        schema_cols_in_df = [
            k for k in _lookup_keys if k in df.columns and not df[k].isna().all()
        ]
        present: set = set()
        if schema_cols_in_df:
            for combo_vals, _group in df.groupby(schema_cols_in_df, sort=False):
                raw_key = combo_vals if isinstance(combo_vals, tuple) else (combo_vals,)
                col_val = {
                    sk: ("" if v is None else schema_str(v))
                    for sk, v in zip(schema_cols_in_df, raw_key, strict=False)
                }
                present.add(tuple(col_val.get(sk, "") for sk in _lookup_keys))
        else:
            present.add(tuple("" for _ in _lookup_keys))
        colsel_existence[param_name] = present
        Log.debug(
            f"ColumnSelection '{param_name}' covers "
            f"{len(present)} schema location(s) for combo pruning"
        )

    # Precompute, per ColumnSelection input, the index positions it actually
    # populates (a coarser input's finer keys are "" everywhere). Computed once,
    # not per combo, since it depends only on the coverage set.
    _colsel_coverage = []  # list[(present_set, populated_idx_tuple)]
    for _param, present in colsel_existence.items():
        if not present:
            continue
        populated_idx = tuple(
            i for i in range(len(_lookup_keys)) if any(key[i] != "" for key in present)
        )
        _colsel_coverage.append((present, populated_idx))

    return _RidIndex(
        lookup_keys=_lookup_keys,
        rid_per_combo=rid_per_combo,
        rid_populated_idx=rid_populated_idx,
        colsel_rid_per_combo=colsel_rid_per_combo,
        colsel_existence=colsel_existence,
        colsel_coverage=_colsel_coverage,
    )


def _log_input_multiplicity(loaded_inputs: dict, _lookup_keys: list, rid_to_bp: dict) -> None:
    """Combo expansion diagnostic: say, per input, whether extra rows at one
    schema location come from several records (variants) or one multi-row
    record."""
    import pandas as pd

    # --- Step 12 diagnostic: record/row multiplicity per schema location ---
    # When a per-combo call receives MORE rows than expected, the cause is almost
    # always >1 record_id at one schema location (distinct branch_param/version
    # VARIANTS, both kept by "latest version PER PARAMETER SET"), or one record
    # whose stored table has multiple rows. This pass makes the distinction
    # explicit so it's obvious which is happening and what the variants are.
    _diag_inputs = []
    for _pn, _data in loaded_inputs.items():
        _d = (
            _data
            if isinstance(_data, pd.DataFrame)
            else (
                _data.data
                if (
                    hasattr(_data, "data")
                    and isinstance(getattr(_data, "data", None), pd.DataFrame)
                )
                else None
            )
        )
        if _d is not None:
            _diag_inputs.append((_pn, _d))
    for _pn, _d in _diag_inputs:
        if RECORD_ID_COLUMN not in _d.columns:
            continue
        _rc = RECORD_ID_COLUMN
        _scols = [k for k in _lookup_keys if k in _d.columns]
        if not _scols:
            continue
        _multi = 0
        _samples = []
        for _vals, _grp in _d.groupby(_scols, sort=False):
            _rids = list(dict.fromkeys(_grp[_rc].tolist()))
            if len(_rids) > 1 or len(_grp) > 1:
                _loc = dict(
                    zip(
                        _scols,
                        _vals if isinstance(_vals, tuple) else (_vals,),
                        strict=False,
                    )
                )
                if len(_rids) > 1:
                    _multi += 1
                if len(_samples) < 5:
                    _bps = [rid_to_bp.get(r, {}) for r in _rids]
                    _samples.append(
                        f"{_loc}: {len(_grp)} row(s), {len(_rids)} record_id(s) "
                        f"branch_params={_bps}"
                    )
        if _multi:
            Log.warn(
                f"diagnostic: input '{_pn}' has {_multi} schema "
                f"location(s) with MORE THAN ONE record_id (distinct "
                f"branch_param/version variants). Full iteration expands these "
                f"into separate combos; aggregation auto-splits into one call "
                f"per branch_param signature (pooled only if the input is "
                f"wrapped in AcrossVariants). Examples: " + "; ".join(_samples)
            )
        elif _samples:
            Log.debug(
                f"diagnostic: input '{_pn}' — single record_id but "
                f"multi-row stored table at some locations. Examples: "
                + "; ".join(_samples)
            )


def _expand_aggregation(
    *,
    inputs: dict,
    loaded_inputs: dict,
    base_combos: list,
    current_schema_keys: list,
    _iterated_schema_keys: set,
    rid_index: _RidIndex,
    rid_to_bp: dict,
    tracked_params: list,
    fixed_rid_values: dict,
    colsel_params: list,
    _path_placeholder_names,
    _path_missing_placeholders: set,
    selection_axes=None,
):
    """Combo expansion, aggregation mode: one call per iterated location per
    variant group (auto-split), pooled for AcrossVariants / ColumnSelection
    inputs. Drops all-NULL schema columns below the iterated level from
    ``loaded_inputs`` IN PLACE.

    Returns ``(full_combos, run_bindings)``.
    """
    import pandas as pd

    _lookup_keys = rid_index.lookup_keys
    rid_per_combo = rid_index.rid_per_combo
    rid_populated_idx = rid_index.rid_populated_idx
    colsel_rid_per_combo = rid_index.colsel_rid_per_combo

    # Aggregation mode: skip per-record rid expansion, but AUTO-SPLIT by
    # upstream branch_param signature (decision D1,
    # endpoints-viz-and-stats-design.md): one call per distinct variant
    # group, as if the user had written EachOf(Variant(...), Variant(...)) —
    # implemented here at signature granularity so nothing is reloaded.
    # Pooling distinct variants into one table double-counts aggregates and
    # destroys variant identity; AcrossVariants inputs opt out explicitly
    # (multiverse analysis) and pool with branch_params attached as columns.
    # __rid_* columns are stripped so the user's function doesn't see
    # internal tracking columns.
    #
    # Also drop schema columns BELOW the lowest iterated level when
    # they are entirely NULL.  Loaded DataFrames carry one column per
    # dataset_schema_key; for a variable stored at a higher schema
    # level, columns for finer-grained keys come back all-NULL.  In
    # aggregation mode those un-iterated schema keys carry no per-row
    # meaning — leaving them in clutters the user-facing table and,
    # on the MATLAB bridge, surfaces as an empty cell column the
    # caller has to special-case.
    iterated_indices = [
        i for i, k in enumerate(current_schema_keys) if k in _iterated_schema_keys
    ]
    if iterated_indices:
        below_iterated_keys = set(current_schema_keys[max(iterated_indices) + 1 :])
    else:
        # No schema keys iterated — every schema key is "below"
        # (this is the no-iteration case; aggregate across everything).
        below_iterated_keys = set(current_schema_keys)

    # --- D1 auto-split bookkeeping (before __rid_* columns are stripped) ---
    # Every rid-tracked input becomes a `RecordPool`: its records per
    # iterated location, per variant group (`scidb.run.bindings`). A plain
    # input SPLITS — one call per group, the group named on the combo as
    # `__vsig_{param}`; an AcrossVariants or ColumnSelection input POOLS
    # every group into the one call. The pools are the only thing the
    # save path, the skip gate and the draft stamp read to answer "which
    # records did this call consume".
    _across_params = {
        name for name, spec in inputs.items() if isinstance(spec, AcrossVariants)
    }
    _sig_cache: dict = {}

    def _sig_of(rid) -> str:
        """Canonical signature of a record's upstream branch_params group."""
        try:
            if rid in _sig_cache:
                return _sig_cache[rid]
        except TypeError:
            rid = str(rid)
            if rid in _sig_cache:
                return _sig_cache[rid]
        sig = variant_signature(rid_to_bp.get(rid, {}))
        _sig_cache[rid] = sig
        return sig

    _iterated_keys_ordered = [k for k in _lookup_keys if k in _iterated_schema_keys]
    _iter_idx = [_lookup_keys.index(k) for k in _iterated_keys_ordered]

    def _pool_from_mapping(param: str, mapping: dict, *, split: bool) -> RecordPool:
        """A `RecordPool` from `rid_per_combo`'s ``{full_key: [rids]}``,
        keyed by the iterated keys THIS input populates ("" positions in
        its keys are axes it does not have — a coarse input is found at
        every location beneath it, the aggregation twin of
        `_RidIndex.probe_key`)."""
        populated = rid_populated_idx.get(param, set(range(len(_lookup_keys))))
        loc_idx = [i for i in _iter_idx if i in populated]
        loc_keys = tuple(_lookup_keys[i] for i in loc_idx)
        grouped: dict = {}
        for full_key, rids in mapping.items():
            loc = tuple(full_key[i] for i in loc_idx)
            for rid in rids:
                grouped.setdefault(loc, {}).setdefault(_sig_of(rid), []).append(rid)
        return RecordPool(
            loc_keys,
            {
                loc: {sig: VariantGroup(sig, tuple(rids)) for sig, rids in groups.items()}
                for loc, groups in grouped.items()
            },
            split=split,
        )

    pools: dict = {}  # param -> RecordPool
    for param_name, mapping in colsel_rid_per_combo.items():
        pools[param_name] = _pool_from_mapping(param_name, mapping, split=False)

    for param_name in tracked_params:
        data = loaded_inputs.get(param_name)
        _df = data if isinstance(data, pd.DataFrame) else None
        if _df is None or RECORD_ID_COLUMN not in _df.columns:
            continue

        if param_name in _across_params:
            # Opt-out: pool all variants, attaching each namespaced
            # branch_param key as an ordinary column so the function can
            # group by specification (variant identity is preserved).
            pools[param_name] = _pool_from_mapping(
                param_name, rid_per_combo.get(param_name, {}), split=False
            )
            bp_keys = sorted(
                {
                    k
                    for rid in _df[RECORD_ID_COLUMN].dropna().unique()
                    for k in rid_to_bp.get(rid, {})
                }
            )
            _attached, _collided = [], []
            for k in bp_keys:
                if k in _df.columns:
                    _collided.append(k)
                    continue
                _df[k] = _df[RECORD_ID_COLUMN].map(
                    lambda r, _k=k: rid_to_bp.get(r, {}).get(_k)
                )
                _attached.append(k)
            if _collided:
                _msg = (
                    f"AcrossVariants('{param_name}'): branch_param "
                    f"column(s) {_collided} collide with existing data "
                    f"columns — not attached."
                )
                warnings.warn(_msg, UserWarning, stacklevel=2)
                Log.warn(_msg)
            Log.debug(
                f"AcrossVariants('{param_name}'): pooling "
                f"{len(pools[param_name].signatures())} variant group(s); "
                f"attached branch_param column(s): {_attached}"
            )
            continue

        # Split input: one call per variant group. The group a call reads
        # is named in its `Selection` (built below) and applied by scifor
        # through `LoopHooks.select_rows`, by record id — no discriminator column
        # on the frame, no key in scifor's schema.
        pools[param_name] = _pool_from_mapping(
            param_name, rid_per_combo.get(param_name, {}), split=True
        )

        # Ragged variant groups: a group missing schema locations that other
        # groups cover aggregates a PARTIAL set of rows. Decided policy
        # (D1): warn and proceed.
        per_combo_locs: dict = {}
        for full_key, rids in rid_per_combo.get(param_name, {}).items():
            ck = tuple(full_key[i] for i in _iter_idx)
            for rid in rids:
                per_combo_locs.setdefault(ck, {}).setdefault(_sig_of(rid), set()).add(
                    full_key
                )
        _ragged_examples: list = []
        for ck, sig_map in per_combo_locs.items():
            if len(sig_map) <= 1:
                continue
            union_locs = set().union(*sig_map.values())
            for sig, locs in sig_map.items():
                missing = union_locs - locs
                if missing and len(_ragged_examples) < 5:
                    combo_disp = (
                        dict(zip(_iterated_keys_ordered, ck, strict=False))
                        or "(grand aggregation)"
                    )
                    miss_disp = [
                        {
                            k: v
                            for k, v in zip(_lookup_keys, m, strict=False)
                            if v != ""
                        }
                        for m in sorted(missing)[:3]
                    ]
                    _ragged_examples.append(
                        f"combo {combo_disp}: group {json.loads(sig)} missing "
                        f"{len(missing)} location(s), e.g. {miss_disp}"
                    )
        if _ragged_examples:
            _msg = (
                f"aggregation auto-split: input '{param_name}' has "
                f"RAGGED variant groups — some branch_param groups cover "
                f"fewer schema locations than others; each group aggregates "
                f"only the rows it has. Pin one group with Variant(...) or "
                f"pool explicitly with AcrossVariants(...) if this is not "
                f"intended. " + " | ".join(_ragged_examples)
            )
            warnings.warn(_msg, UserWarning, stacklevel=2)
            Log.warn(_msg)

    for param_name, data in list(loaded_inputs.items()):
        # Drop schema columns BELOW the lowest iterated level when they are
        # entirely NULL (see above). `__record_id` stays: scifor's
        # `LoopHooks.select_rows` hook selects the call's rows by it and drops it.
        if isinstance(data, pd.DataFrame):
            _df = data
        elif isinstance(data, _scifor.ColumnSelection) and isinstance(
            data.data, pd.DataFrame
        ):
            _df = data.data
        else:
            continue
        empty_schema_cols = [
            c
            for c in _df.columns
            if c in below_iterated_keys and _df[c].isna().all()
        ]
        if empty_schema_cols:
            _stripped = _df.drop(columns=empty_schema_cols)
            if isinstance(data, pd.DataFrame):
                loaded_inputs[param_name] = _stripped
            else:
                data.data = _stripped
            Log.debug(
                f"aggregation: dropped all-null schema "
                f"column(s) {empty_schema_cols} from loaded input "
                f"'{param_name}' (below iterated schema level)"
            )

    # Step 13: every input's binding, typed — decided once from what Step
    # 11 sorted the inputs into (`RunBindings`). Built BEFORE expansion so
    # each combination's `Selection` can be registered on it.
    run_bindings = _build_run_bindings(
        inputs,
        tracked_params=tracked_params,
        fixed_rid_values=fixed_rid_values,
        colsel_params=colsel_params,
        aggregation_mode=True,
        pools=pools,
        iterated_keys=_iterated_keys_ordered,
        rid_to_bp=rid_to_bp,
    )

    # Expand combos over the observed signature combinations per combo —
    # one call per variant group (auto-split), Cartesian across split
    # inputs (mirroring full-iteration rid expansion at signature
    # granularity). A split input with NO data at a combo expands with the
    # empty signature so the combo still flows through and skips
    # gracefully, matching pre-split behavior for empty locations.
    #
    # Each call gets ONE `Selection` — per input, the records it reads —
    # and the combo carries only its handle (`COMBO_KEY`). scifor's frame
    # filter and the save path's edges both read the selection, so each
    # output saves with only ITS group's contributing rids and the
    # branch_params merge is conflict-free by construction.
    split_params = [p for p, pool in pools.items() if pool.split]

    full_combos = []
    for combo in base_combos:
        sig_options = []
        for p in split_params:
            sigs = sorted(pools[p].groups_at(combo).keys()) or [EMPTY_SIGNATURE]
            aligned = [s for s in sigs if not signature_conflicts_with(s, combo)]
            if len(aligned) < len(sigs):
                Log.debug(
                    f"aggregation auto-split: input '{p}' at combo {combo}: "
                    f"aligned __save__.* signature(s) to iterated value(s) — "
                    f"kept {len(aligned)}/{len(sigs)} group(s)"
                )
            # All groups conflicting = no matching data at this combo:
            # flow through with the empty signature and skip gracefully,
            # same as a combo with no data at all.
            sig_options.append(aligned or [EMPTY_SIGNATURE])
        for sig_combo in product(*sig_options) if sig_options else [()]:
            groups = dict(zip(split_params, sig_combo, strict=False))
            rids: dict = {}
            for p, pool in pools.items():
                chosen = pool.rids_at(combo, groups.get(p))
                if chosen:
                    rids[p] = tuple(chosen)
            for p, rid in fixed_rid_values.items():
                if rid:
                    rids[p] = (str(rid),)
            fc = dict(combo)
            axis = selection_axes.for_combo(combo) if selection_axes and rids else None
            fc[COMBO_KEY] = run_bindings.add_selection(Selection(rids, groups, axis))
            if _path_placeholder_names:
                # Group bp = the parsed split-input signatures + any Fixed
                # inputs' bp; {variant} digests the merged coordinate.
                _bp_dicts = [json.loads(s) for s in sig_combo]
                _bp_dicts += [
                    rid_to_bp.get(r, {}) for r in fixed_rid_values.values() if r
                ]
                _merged, _confl = merge_branch_params(_bp_dicts)
                _inject_path_placeholders(
                    fc,
                    _path_placeholder_names,
                    _merged,
                    _confl,
                    _path_missing_placeholders,
                )
            full_combos.append(fc)

    total_rids = sum(
        len(g.rids)
        for pool in pools.values()
        for groups in pool.groups_by_location.values()
        for g in groups.values()
    )
    Log.debug(
        f"aggregation mode (auto-split by branch_param signature): "
        f"iterating {sorted(_iterated_schema_keys) or '(none)'} "
        f"of schema {current_schema_keys}, "
        f"{len(base_combos)} base combo(s) -> {len(full_combos)} call(s), "
        f"variant groups per split input: "
        f"{ {p: len(pools[p].signatures()) for p in split_params} }, "
        f"pooled (AcrossVariants) inputs: {sorted(_across_params) or 'none'}, "
        f"{total_rids} contributing rids"
    )
    return full_combos, run_bindings


def _expand_full_iteration(
    *,
    inputs: dict,
    loaded_inputs: dict,
    base_combos: list,
    rid_index: _RidIndex,
    rid_to_bp: dict,
    tracked_params: list,
    fixed_rid_values: dict,
    colsel_params: list,
    _path_placeholder_names,
    _path_missing_placeholders: set,
):
    """Combo expansion, full iteration: every plain input's record is an
    iteration axis; Fixed pins and ColumnSelection records ride along in each
    combination's ``Selection``.

    Returns ``(full_combos, run_bindings)``.
    """
    import pandas as pd

    _lookup_keys = rid_index.lookup_keys
    rid_per_combo = rid_index.rid_per_combo
    colsel_rid_per_combo = rid_index.colsel_rid_per_combo
    colsel_existence = rid_index.colsel_existence
    _rid_probe_key = rid_index.probe_key
    _colsel_combo_present = rid_index.colsel_present

    # Full iteration mode: expand combos with rid variants.
    #
    # Every plain input's record is an iteration AXIS: one combination per
    # record at a location. A Fixed input's pin and a ColumnSelection's
    # (lineage-only) record are added to every combination's `Selection`
    # without expanding it. No pools in this mode.
    pools = {}
    _iterated_keys_ordered = []

    # AcrossVariants only changes aggregation-mode behavior: in full
    # iteration every combo sees exactly one variant row (rid expansion),
    # so pooling is a no-op and the input behaves as if unwrapped.
    _across_noop = sorted(
        name for name, spec in inputs.items() if isinstance(spec, AcrossVariants)
    )
    if _across_noop:
        _msg = (
            f"AcrossVariants input(s) {_across_noop} in FULL "
            f"iteration mode: every combo sees exactly one variant, so "
            f"pooling is a no-op — the input(s) behave as if unwrapped "
            f"(variants expand into separate combos). AcrossVariants only "
            f"affects aggregation-mode for_each calls."
        )
        warnings.warn(_msg, UserWarning, stacklevel=2)
        Log.warn(_msg)

    # Step 13: the typed bindings, BEFORE expansion so each combination's
    # `Selection` can be registered on them.
    run_bindings = _build_run_bindings(
        inputs,
        tracked_params=tracked_params,
        fixed_rid_values=fixed_rid_values,
        colsel_params=colsel_params,
        aggregation_mode=False,
        pools=None,
        iterated_keys=None,
        rid_to_bp=rid_to_bp,
    )

    # Expand each base combo with all valid rid-combos for that schema location
    Log.debug(
        f"expanding combos: {len(base_combos)} base combos, "
        f"{len(rid_per_combo)} rid dimensions"
    )
    full_combos: list = []
    _pruned_colsel = 0
    for combo in base_combos:
        schema_vals = tuple(str(combo.get(k, "")) for k in _lookup_keys)

        # Prune Cartesian combos that no ColumnSelection input has data for.
        # (Plain DataFrame inputs prune via the rid-validity check below;
        # ColumnSelection inputs deliberately don't expand, so they prune
        # here instead — without this, non-existent grid points leak through
        # as empty per-combo calls.)
        if colsel_existence and not _colsel_combo_present(schema_vals):
            _pruned_colsel += 1
            continue

        # The records every expansion of this location binds without
        # iterating over: a Fixed input's pin, and a ColumnSelection's
        # record(s) at this location (lineage only). Computed once per
        # location rather than per expanded combo.
        _fixed_rids: dict = {p: (str(r),) for p, r in fixed_rid_values.items() if r}
        for _cs_param, _cs_map in colsel_rid_per_combo.items():
            _rids = _cs_map.get(_rid_probe_key(_cs_param, schema_vals), [])
            if not _rids:
                continue
            if len(_rids) > 1:
                # ColumnSelection pools variants rather than expanding them
                # (that is the whole point of keeping it out of the tracked
                # set); it used to bind only the first because a `__rid_*`
                # combo key held one id. A Selection holds them all.
                Log.debug(
                    f"ColumnSelection '{_cs_param}' has {len(_rids)} "
                    f"record(s) at this location; binding all for lineage"
                )
            _fixed_rids[_cs_param] = tuple(str(r) for r in _rids)

        rid_lists: list = []
        rid_params: list = []
        valid = True
        for param, mapping in rid_per_combo.items():
            rids = mapping.get(_rid_probe_key(param, schema_vals), [])
            if not rids:
                valid = False
                break
            rid_lists.append(rids)
            rid_params.append(param)

        if not valid:
            continue

        def _register(full_combo: dict, chosen: dict) -> None:
            rids = {**_fixed_rids, **{p: (str(r),) for p, r in chosen.items()}}
            full_combo[COMBO_KEY] = run_bindings.add_selection(Selection(rids))
            if _path_placeholder_names:
                _bp_dicts = [rid_to_bp.get(r, {}) for rs in rids.values() for r in rs]
                _merged, _confl = merge_branch_params(_bp_dicts)
                _inject_path_placeholders(
                    full_combo,
                    _path_placeholder_names,
                    _merged,
                    _confl,
                    _path_missing_placeholders,
                )
            full_combos.append(full_combo)

        if rid_lists:
            for rid_combo in product(*rid_lists):
                _register(dict(combo), dict(zip(rid_params, rid_combo, strict=False)))
        else:
            _register(dict(combo), {})

    if _pruned_colsel:
        Log.debug(
            f"pruned {_pruned_colsel} non-existent combo(s) "
            f"via ColumnSelection coverage"
        )
    if len(full_combos) != len(base_combos):
        Log.debug(
            f"expanded {len(base_combos)} base combos -> "
            f"{len(full_combos)} full combos (rid variants / pruning)"
        )
    else:
        Log.debug(f"{len(full_combos)} combos (no rid expansion needed)")

    # Existence-pruning health check. In full iteration mode the rid-validity
    # skip and the ColumnSelection coverage prune are the ONLY things that
    # drop Cartesian combos with no backing data. If there are DataFrame-backed
    # inputs but NEITHER pruning mechanism is active, the entire Cartesian
    # product leaks through — every non-existent location becomes an empty
    # per-combo call. This is exactly the failure mode when an input wrapper
    # fails to register either a rid key or ColumnSelection coverage.
    _has_df_inputs = any(
        isinstance(v, pd.DataFrame)
        or (
            isinstance(v, (_scifor.ColumnSelection, _scifor.Fixed))
            and isinstance(getattr(v, "data", None), pd.DataFrame)
        )
        for v in loaded_inputs.values()
    )
    if (
        _has_df_inputs
        and not rid_per_combo
        and not colsel_existence
        and len(full_combos) == len(base_combos)
    ):
        Log.warn(
            "expand_combos: full iteration over all schema keys kept the "
            f"ENTIRE Cartesian product ({len(full_combos)} combos) with no "
            "pruning — DataFrame-backed inputs registered neither a rid key nor "
            "ColumnSelection coverage, so non-existent schema locations will be "
            "passed to the function as EMPTY tables. This usually means an "
            "input's __record_id was lost before variant tracking. "
            "Check the per-input logs above."
        )
    return full_combos, run_bindings


def _check_path_outputs(
    inputs: dict,
    _path_outputs,
    full_combos: list,
    run_bindings,
    _path_placeholder_names,
    _path_missing_placeholders: set,
) -> None:
    """Combo expansion: warn about unresolved PathOutput placeholders and
    refuse variant-group path collisions before anything renders."""
    # PathOutput placeholders: warn once per unresolved name (the literal
    # ``{name}`` stays in the path), then guard against variant-group path
    # collisions BEFORE anything renders — a shared path means each group's
    # file silently overwrites the previous one.
    if _path_missing_placeholders:
        _msg = (
            f"PathOutput placeholder(s) "
            f"{sorted('{' + n + '}' for n in _path_missing_placeholders)} did not "
            f"match any branch_param of this call's variant group(s) — the "
            f"literal placeholder text stays in the resolved path. Available "
            f"branch_param keys come from upstream constants (e.g. "
            f"'bandpass.low_hz', bare-name '{{low_hz}}')."
        )
        warnings.warn(_msg, UserWarning, stacklevel=2)
        Log.warn(_msg)
    if _path_outputs and full_combos:
        _colname_columns = None
        for _spec in inputs.values():
            _cs = _iterate_column_selection(_spec)
            if _cs is not None and _cs.groups is not None:
                # {ColName} is the group's name (a dict key, or the paired
                # output in split mode). The preview only needs one distinct
                # label per group, so an unnamed group's position will do.
                _colname_columns = [
                    name or f"group{i + 1}" for i, (name, _cols) in enumerate(_cs.groups)
                ]
                break
            if _cs is not None and _cs.columns:
                _colname_columns = list(_cs.columns)
                break
        _guard_pathoutput_collisions(
            _path_outputs,
            full_combos,
            _colname_columns,
            run_bindings,
            _path_placeholder_names,
        )


def _apply_pre_combo_hook(
    full_combos: list, _pre_combo_hook, run_bindings, _aggregation_mode: bool
) -> "tuple[list, int]":
    """Prepare stage: drop the combos the pre-combo hook (skip_computed)
    claims. Returns ``(full_combos, skipped_count)``."""
    # Step 14: Apply the pre-combo hook (e.g. skip_computed): filter out any
    # combos where the hook returns True.
    _skip_computed_count = 0
    if _pre_combo_hook is not None:
        # Hand the skip hook the typed bindings (via the mutable holder
        # _build_skip_hook exposes): each combo names its `Selection` and the
        # gate compares that — its exact consumed-rid set — against the
        # stored edges. Without it a new variant group (or a grown record
        # set) would cross-skip against another group's output. Works for
        # both the Python and MATLAB-bridge call sites, which both build the
        # hook before calling prepare.
        _hook_agg_ref = getattr(_pre_combo_hook, "_agg_binding_ref", None)
        if _hook_agg_ref is not None:
            _hook_agg_ref["bindings"] = run_bindings
            _hook_agg_ref["aggregation"] = _aggregation_mode
        Log.debug("applying pre-combo hook (skip_computed)")
        pre_hook_count = len(full_combos)
        full_combos = [c for c in full_combos if not _pre_combo_hook(c)]
        skipped = pre_hook_count - len(full_combos)
        _skip_computed_count = skipped
        if skipped > 0:
            # O(1) per run and part of the "what ran / what was skipped" story.
            Log.info(f"skip_computed: {skipped}/{pre_hook_count} combos skipped")
        else:
            Log.debug(
                f"skip_computed: 0/{pre_hook_count} combos skipped (all will be computed)"
            )
    else:
        Log.debug("no pre-combo hook provided, skipping")
    return full_combos, _skip_computed_count


def _expand_combos(
    *,
    inputs: dict,
    loaded_inputs: dict,
    metadata_iterables: dict,
    all_combos,
    _discovered_combos,
    db,
    rid_to_bp: dict,
    tracked_params: list,
    fixed_rid_values: dict,
    colsel_params: list,
    selection_axes=None,
):
    """Prepare stage: every call this run makes — base combos (existing,
    discovered, or the Cartesian product) expanded by record / variant group,
    each carrying its ``Selection`` handle on ``run_bindings``.

    Returns ``(full_combos, run_bindings, aggregation_mode,
    current_schema_keys, path_placeholder_names)``.
    """
    # --- Step 12: Build full combos: base_combos × valid rid-combos per schema location ---
    Log.debug("expanding combos with record-ID variants")
    # The database is the one holder of the dataset's schema keys; scifor's
    # set_schema copy is for scifor, never read back here (cleanup-audit F14).
    current_schema_keys = dataset_schema_keys_of(db)

    base_combos = all_combos
    Log.debug(
        f"all_combos={'None' if all_combos is None else len(all_combos)}, "
        f"_discovered_combos={'None' if _discovered_combos is None else len(_discovered_combos)}"
    )
    if base_combos is None and _discovered_combos is not None:
        # Use filesystem-discovered combos directly (avoids non-existent Cartesian combos)
        base_combos = _discovered_combos
        Log.debug(f"using {len(base_combos)} filesystem-discovered combos")
    if base_combos is None:
        keys = list(metadata_iterables.keys())
        value_lists = [metadata_iterables[k] for k in keys]
        base_combos = [
            dict(zip(keys, combo, strict=False)) for combo in product(*value_lists)
        ]
        Log.debug(f"built {len(base_combos)} base combos from metadata iterables")

    # Detect aggregation mode: not all schema keys are being iterated, so
    # lower-level records should be aggregated into multi-row DataFrames
    # rather than being separated into individual combos via rid expansion.
    _iterated_schema_keys = set(metadata_iterables.keys()) & set(current_schema_keys)
    _aggregation_mode = len(current_schema_keys) > 0 and len(
        _iterated_schema_keys
    ) < len(current_schema_keys)
    if _aggregation_mode:
        Log.debug(
            f"aggregation mode detected: iterating {len(_iterated_schema_keys)}/{len(current_schema_keys)} schema keys"
        )
    else:
        Log.debug("full iteration mode: all schema keys being iterated")

    # Lookup keys for rid disambiguation: schema keys + any non-schema metadata
    # iterable keys.  Using only schema keys misses non-schema iterables (e.g.
    # "session") that ARE present in the loaded DataFrame and should distinguish
    # which record belongs to which combo.
    _lookup_keys = list(
        dict.fromkeys(
            current_schema_keys
            + [k for k in metadata_iterables if k not in set(current_schema_keys)]
        )
    )

    # PathOutput variant placeholders: names referenced by templates that are
    # NOT combo-supplied — these resolve from each expanded combo's variant
    # group branch_params and are injected below (then stripped before save).
    _path_placeholder_names, _path_outputs = _pathoutput_placeholders(
        inputs, set(current_schema_keys) | set(metadata_iterables)
    )
    _path_missing_placeholders: set = set()
    if _path_placeholder_names:
        Log.debug(
            f"PathOutput branch_param placeholder(s) detected: "
            f"{sorted(_path_placeholder_names)}"
        )

    rid_index = _build_rid_index(
        loaded_inputs, tracked_params, colsel_params, _lookup_keys
    )
    _log_input_multiplicity(loaded_inputs, _lookup_keys, rid_to_bp)

    _expand = dict(
        inputs=inputs,
        loaded_inputs=loaded_inputs,
        base_combos=base_combos,
        rid_index=rid_index,
        rid_to_bp=rid_to_bp,
        tracked_params=tracked_params,
        fixed_rid_values=fixed_rid_values,
        colsel_params=colsel_params,
        _path_placeholder_names=_path_placeholder_names,
        _path_missing_placeholders=_path_missing_placeholders,
    )
    if _aggregation_mode:
        # Full iteration cannot narrow within a call: a call is one location.
        full_combos, run_bindings = _expand_aggregation(
            current_schema_keys=current_schema_keys,
            _iterated_schema_keys=_iterated_schema_keys,
            selection_axes=selection_axes,
            **_expand,
        )
    else:
        full_combos, run_bindings = _expand_full_iteration(**_expand)

    _check_path_outputs(
        inputs,
        _path_outputs,
        full_combos,
        run_bindings,
        _path_placeholder_names,
        _path_missing_placeholders,
    )
    return (
        full_combos,
        run_bindings,
        _aggregation_mode,
        current_schema_keys,
        _path_placeholder_names,
    )


def _for_each_prepare(
    *,
    fn: Callable,
    fn_name: str,
    inputs: dict,
    outputs: list,
    dry_run: bool,
    db,
    options: RunOptions,
    selection: RunSelection,
    context: RunContext,
    glue: "dict[str, Any] | None" = None,
    glue_language: str = "python",
    generates_file: bool = False,
    endpoint_kind: "str | None" = None,
    project_root: "str | _Path | None" = None,
) -> "_ForEachState | None":
    """Run scidb.for_each's pre-loop work, as a sequence of named stages:
    :func:`_normalize_glue`, :func:`_resolve_iterables`,
    :func:`_build_call_identity`, :func:`_existing_combos`,
    :func:`_load_all_inputs`, :func:`_track_variants`, :func:`_expand_combos`,
    :func:`_apply_pre_combo_hook`.

    On ``dry_run=True`` runs :func:`_dry_run_preview` and returns ``None`` to
    signal the caller to stop. Otherwise returns the prepared state object
    the loop and save phases consume.

    ``glue_language`` names the language the *run* executes in. Chains
    authored in another language are refused (a glue node executes in the
    language of the run); in a MATLAB run the whole chain is carried through
    on the returned state for ``+scidb/for_each.m`` to apply, because a ``.m``
    function cannot execute inside this prepare step.

    The call arrives bundled (``scidb.run.foreach_call``); ``selection`` has
    ``schema_filter`` / ``schema_keys`` already resolved by ``for_each``.
    """
    as_table, distribute = options.as_table, options.distribute
    where, metadata_iterables = selection.where, dict(selection.metadata_iterables)
    pre_combo_hook, parameter_names = context.pre_combo_hook, context.parameter_names

    # A Parameter value is recorded as DECLARED, not as the transport spelled
    # it (MATLAB passes a declared 6 as 6.0). Before anything below derives
    # identity, version keys or metadata from the inputs.
    inputs = declared_spelling(inputs, parameter_names)

    # Where a rootless PathInput resolves, decided once for the whole call:
    # the caller's root, else the process-wide one (the MATLAB bridge entry
    # and a plain script reach here with None). Bound into the inputs, so
    # discovery and per-combo resolution read it without a global.
    root = _Path(project_root).resolve() if project_root is not None else current_project_root()
    inputs = bind_path_inputs(inputs, root)

    # Glue chains: normalized and validated up front, constant-fed ones applied.
    glue_chains, deferred_glue_chains, inputs, constant_glue = _normalize_glue(
        glue, inputs, glue_language
    )
    # The skip hook compares a glued constant by the virtual glue rid its edge
    # binds. Derived HERE, once, by the save path's own rule
    # (provenance_save.constant_bindings) from the same raw value the version
    # keys record — after declared_spelling, so a MATLAB 6.0 declared as 6 is
    # one value on both sides. The hook derives nothing.
    glue_ref = getattr(pre_combo_hook, "_constant_glue_ref", None)
    if glue_ref is not None:
        glue_ref["rids"] = {
            b.param: b.rid
            for b in constant_bindings(
                {p: cg.raw for p, cg in constant_glue.items()},
                _glue.constant_glue_config_keys(constant_glue),
            )
        }
        glue_ref["set"] = True
        for p, vrid in sorted(glue_ref["rids"].items()):
            Log.info(
                f"[skip] {fn_name}: '{p}' is a Parameter-fed glued constant; "
                f"skip_computed compares its virtual glue record {vrid} "
                f"(raw value + glue chain {constant_glue[p].chain_hash} + glued "
                f"value {constant_glue[p].glued_hash}), not the constant"
            )

    # The metadata iterables: [] filled from the database / disk, schema
    # propagated, schema-key values stringified.
    metadata_iterables, _discovered_combos, needs_resolve, resolved_db = (
        _resolve_iterables(metadata_iterables, inputs, db, distribute)
    )

    output_names = [_output_column_name(o) for o in outputs] if outputs else ["result"]
    Log.debug(f"resolved {len(output_names)} output name(s): {output_names}")

    # Dry run: preview the iteration through scifor and stop.
    if dry_run:
        _dry_run_preview(
            fn,
            inputs,
            metadata_iterables,
            needs_resolve=needs_resolve,
            resolved_db=resolved_db,
            db=db,
            as_table=as_table,
            distribute=distribute,
            output_names=output_names,
            locations=selection.locations,
            cancel_check=context.cancel_check,
            project_root=root,
        )
        return None

    # The iteration level: the schema keys this call iterates, final (after
    # PathInput discovery), in dataset order. Part of invocation identity
    # since 2026-10-01 (docs/claude/iteration-level-identity.md).
    level = _iteration_level_of(metadata_iterables, resolved_db or db)
    Log.info(
        f"[level] {fn_name}: iterates "
        + (", ".join(level) if level else "nothing (one call over the whole dataset)")
    )
    level_ref = getattr(pre_combo_hook, "_iteration_level_ref", None)
    if level_ref is not None:
        level_ref["level"] = list(level)
        level_ref["set"] = True

    # The call's identity: config keys and call_id (glue names included).
    config_keys, call_id = _build_call_identity(
        fn, inputs, where, distribute, as_table, glue_chains, level, constant_glue
    )

    # Only the schema combinations that exist (and are not excluded).
    all_combos = _existing_combos(
        metadata_iterables, inputs, needs_resolve, resolved_db, db
    )

    # Bulk-load every input (+ glue fusion on the loaded tables).
    loaded_inputs, mapping_inputs, glue_fusion, selection_axes = _load_all_inputs(
        fn_name,
        inputs,
        db,
        where,
        deferred_glue_chains,
        glue_language,
        selection.locations,
        level=level,
        schema_keys=dataset_schema_keys_of(resolved_db or db),
    )
    per_combo_glue = glue_fusion.per_combo

    # Variant tracking: rid -> branch_params, and each input's kind.
    loaded_inputs, rid_to_bp, tracked_params, fixed_rid_values, colsel_params = (
        _track_variants(loaded_inputs)
    )

    # Every call this run makes, each with its Selection on run_bindings.
    (
        full_combos,
        run_bindings,
        _aggregation_mode,
        current_schema_keys,
        _path_placeholder_names,
    ) = _expand_combos(
        inputs=inputs,
        loaded_inputs=loaded_inputs,
        metadata_iterables=metadata_iterables,
        all_combos=all_combos,
        _discovered_combos=_discovered_combos,
        db=db,
        rid_to_bp=rid_to_bp,
        tracked_params=tracked_params,
        fixed_rid_values=fixed_rid_values,
        colsel_params=colsel_params,
        selection_axes=selection_axes,
    )

    # skip_computed: drop the combos whose outputs are already current.
    full_combos, _skip_computed_count = _apply_pre_combo_hook(
        full_combos, pre_combo_hook, run_bindings, _aggregation_mode
    )

    return _ForEachState(
        fn=fn,
        fn_name=fn_name,
        config_keys=config_keys,
        call_id=call_id,
        output_names=output_names,
        output_types=[_output_type_label(o) for o in outputs] if outputs else [],
        loaded_inputs=loaded_inputs,
        full_combos=full_combos,
        extended_metadata_iterables=dict(metadata_iterables),
        aggregation_mode=_aggregation_mode,
        current_schema_keys=current_schema_keys,
        generates_file=generates_file,
        endpoint_kind=endpoint_kind,
        path_extra_keys=_path_placeholder_names or None,
        skip_computed_count=_skip_computed_count,
        mapping_inputs=mapping_inputs or None,
        inputs=inputs,
        # The DEFERRED set: a Parameter-fed chain has already run (before
        # fan-out), so it must not be handed to MATLAB's loop to run again.
        glue_chains=deferred_glue_chains or None,
        per_combo_glue=per_combo_glue or None,
        glue_virtual=glue_fusion.virtual or None,
        bindings=run_bindings,
        parameter_names=parameter_names or None,
        path_input_specs=path_input_specs_of(inputs) or None,
        project_root=root,
        options=options,
    )


def _for_each_save_resolved(
    *,
    state: "_ForEachState",
    result_tbl,
    inputs: dict,
    outputs: list,
    save: bool,
    db,
):
    """Run scidb.for_each's Step 18 (schema restore) and Step 19 (save).

    Returns ``result_tbl`` unchanged after performing the save side effect.
    For endpoint runs (``endpoint_kind`` in {"plot", "stat"}), artifacts get a
    provenance stamp (D4): record mode stamps inside ``_save_results`` (where
    the record_id is known); draft mode stamps here with the same blob minus
    the record_id.
    """
    # (Step 18 — restoring scifor's schema after Step 15 extended it — is gone
    # with Step 15, 2026-09-20. The schema is never touched by a run.)
    if result_tbl is None:
        return None

    # Strip PathOutput placeholder columns injected at combo expansion: they
    # exist only so scifor's path resolution can substitute them. Left in,
    # they would surface in the user-facing table and — worse — be picked up
    # as dynamic-discriminator branch_params on save (the group's REAL
    # namespaced bp already inherits through its edges).
    if state.path_extra_keys:
        _extra_cols = [c for c in result_tbl.columns if c in state.path_extra_keys]
        if _extra_cols:
            result_tbl = result_tbl.drop(columns=_extra_cols)
            Log.debug(
                f"stripped {len(_extra_cols)} PathOutput placeholder "
                f"column(s) from results: {_extra_cols}"
            )

    # Step 19: Save results
    if save and outputs and not result_tbl.empty:
        Log.debug(
            f"saving {len(result_tbl)} result row(s) for {len(outputs)} output(s)"
        )
        # The pinned record of every Fixed input is an edge in every mode.
        # Step 12 resolved it from the loaded frame (`RunBindings.pinned_rids`);
        # a pin Step 12 could not settle (several rows matched) is looked up
        # in the database last. A rid from the database bypasses the
        # __record_id rewrite fuse_glue performed on the frame, so it is
        # routed through the same virtual map — or the graph edge would point
        # at the raw record while the combo carries the virtual one, a
        # binding mismatch that makes skip_computed recompute on every run.
        bindings = state.bindings
        unpinned = [
            b.param for b in bindings.of_kind(InputKind.PINNED) if not b.pinned_rid
        ]
        if unpinned:
            looked_up = _compute_fixed_input_rids(
                {p: inputs[p] for p in unpinned if p in inputs},
                db if db is not None else _active_database(),
            )
            if looked_up and state.glue_virtual:
                looked_up = _remap_fixed_rids_through_glue(looked_up, state.glue_virtual)
            for param, rid in looked_up.items():
                bindings.pin(param, rid)
            Log.debug(
                f"Fixed input rid(s) resolved from the database for the graph: "
                f"{list(looked_up)}"
            )

        save_t0 = time.perf_counter()
        _save_results(result_tbl, outputs, state, db)
        save_elapsed = time.perf_counter() - save_t0
        Log.debug(
            f"save_results complete: saved {len(result_tbl)} result(s) in {save_elapsed:.3f}s"
        )
    elif not save:
        Log.debug("skipping save (save=False)")
    elif not outputs:
        Log.debug("skipping save (no outputs specified)")
    elif result_tbl.empty:
        Log.debug("skipping save (result table is empty)")

    # Endpoint DRAFT stamping (D4): save=False, but draft artifacts get the same
    # provenance blob a recorded run would embed — draft:true in place of a
    # record_id.
    if state.endpoint_kind and not (save and outputs) and not result_tbl.empty:
        _stamp_draft_endpoint_artifacts(state.endpoint_kind, result_tbl, state, db)

    return result_tbl


def _remap_fixed_rids_through_glue(fixed_rids: dict, glue_virtual: dict) -> dict:
    """Rewrite Fixed-input rids to their virtual glue rids where a chain applies.

    ``fixed_rids`` is ``{param: rid}``.
    """
    out = dict(fixed_rids)
    for param, rid in fixed_rids.items():
        entry = glue_virtual.get(param)
        if not entry or rid is None:
            continue
        _chain_hash, _sig, mapping = entry
        mapped = mapping.get(str(rid))
        if mapped:
            out[param] = mapped
            Log.debug(
                f"[glue] '{param}': Fixed input rid {rid} -> virtual {mapped}"
            )
    return out


def _normalize_variable_inputs(
    resolved: dict,
    current_combo: dict,
    inputs: dict,
    loaded_inputs: "dict | None" = None,
    as_table_params: "set[str] | None" = None,
    *,
    bindings: "RunBindings | None" = None,
    aggregation_mode: bool = False,
) -> dict:
    """Normalize variable inputs to the raw data the function expects.

    scifor extracts data from the spread DataFrames; this normalizes each
    variable param to its clean raw value — pulling the data column out of a
    1-row DataFrame and restoring the dict structure for multi-column
    (dict-of-arrays) variables (scifor's _extract_data strips the dict wrapper
    when there is exactly one data column). Non-variable params pass through.

    This is the value the function receives: the raw data, not a
    ``BaseVariable``.

    ``as_table_params`` is what the caller asked to receive as whole DataFrames,
    and those are passed through untouched. Unwrapping them was a real bug with a
    narrow trigger: this normalization only runs in FULL iteration (every schema
    key iterated — one record per input per call), so an ``as_table`` input
    silently arrived as a bare scalar in exactly that case and nowhere else. A
    ``plot_``/``stat_`` endpoint iterating down to the leaf level then received
    a float where its body expects a frame.

    Args:
        resolved: Dict of param_name → raw_data from scifor
        current_combo: The combo scifor is running (schema keys + ``COMBO_KEY``)
        inputs: Original inputs dict with param_name → variable_class or Fixed()
        loaded_inputs: The spread DataFrames from _for_each_prepare (state.loaded_inputs).
        as_table_params: Param names the caller declared ``as_table``.
        bindings: The run's ``RunBindings`` — which records this combo reads
            (``selection_of``) is what says a param is a variable input here.
        aggregation_mode: An aggregating call hands the function the pooled
            frame as is; nothing is normalised.

    Returns:
        Dict with normalized raw data for variable inputs, pass-through for others
    """
    import pandas as pd

    reconstructed = {}
    as_table_params = as_table_params or set()
    selection = bindings.selection_of(current_combo) if bindings is not None else None
    bound = {} if (aggregation_mode or selection is None) else dict(selection.rids)

    for param_name, raw_value in resolved.items():
        # A variable input in a full-iteration combo reads exactly one record;
        # anything else (a constant, a PathInput, an as_table param, any param
        # of an aggregating call) passes through untouched.
        if param_name not in bound or param_name in as_table_params:
            reconstructed[param_name] = raw_value
            continue

        # Get variable class from inputs
        input_spec = inputs.get(param_name)
        variable_class = None

        if isinstance(input_spec, type):
            # Simple variable type
            variable_class = input_spec
        elif isinstance(input_spec, Fixed) and isinstance(input_spec.data, type):
            # Fixed wrapper - extract the variable class
            variable_class = input_spec.data

        if variable_class is None:
            # Not a simple variable type - pass through
            reconstructed[param_name] = raw_value
            continue

        # Extract raw data from DataFrame if needed
        data_value = raw_value
        if isinstance(raw_value, pd.DataFrame):
            # Extract the data column (variable name column)
            var_name = variable_class.__name__
            if var_name in raw_value.columns:
                # Get the single value from the data column
                data_value = raw_value[var_name].iloc[0]
            else:
                # Fallback: pass through as-is
                data_value = raw_value
        elif loaded_inputs is not None and not isinstance(raw_value, dict):
            # Check whether the original variable stored a dict (multi-column mode).
            # scifor's _extract_data strips the dict wrapper when there is a single
            # data column (e.g. {"vals": array} → just array).  Detect this by
            # comparing the spread DataFrame's data column name(s) against the
            # variable's view_name: single-column variables rename their column to
            # view_name, so if any data column differs from view_name the original
            # data was a dict.
            df_input = loaded_inputs.get(param_name)
            if isinstance(df_input, pd.DataFrame):
                # Columns that are NOT data: internal __ columns + schema/combo keys
                schema_keys_in_combo = {
                    k for k in current_combo if not k.startswith("__")
                }
                data_cols = [
                    c
                    for c in df_input.columns
                    if not c.startswith("__") and c not in schema_keys_in_combo
                ]
                view_name = (
                    variable_class.view_name()
                    if hasattr(variable_class, "view_name")
                    else variable_class.__name__
                )
                # Multi-column dict: data columns differ from the view_name
                is_dict_type = len(data_cols) > 1 or (
                    len(data_cols) == 1 and data_cols[0] != view_name
                )
                if is_dict_type and RECORD_ID_COLUMN in df_input.columns:
                    rid_val = str(bound[param_name][0])
                    mask = df_input[RECORD_ID_COLUMN].astype(str) == rid_val
                    if mask.any():
                        row = df_input[mask].iloc[0]
                        data_value = {col: row[col] for col in data_cols}

        # Pass the normalized raw data (DataFrame → array, dict structure
        # restored for multi-column variables) — the raw value, not a BaseVariable.
        reconstructed[param_name] = data_value

    return reconstructed
