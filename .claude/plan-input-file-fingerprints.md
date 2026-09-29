# Plan: input-file fingerprints (the skip gate notices an edited file)

Status: PLAN ONLY. Nothing is built yet; waiting for the user's decisions (§6).

## 1. Problem

A PathInput is identified by its `name=` only. Since 2026-09-25, `to_key()` is
what gets hashed into record ids, the invocation id, the call id and the skip
gate. That was deliberate: moving data or switching machines keeps every id.
The file's contents are never read into identity.

As a result, the skip gate (`scidb.foreach._build_skip_hook` /
`_find_skip_gate_record`) compares only three things: the function hash, the
variable-input record ids and the constant hashes. After someone edits
`Subject Demographics Aim 2.csv`, every one of those still matches, so a
`skip_computed=True` run skips the call as already up to date.

Which routes are affected:

- **The GUI node Run button** passes `skip_computed=False`
  (`scistack-gui/scistack_gui/api/run.py:610`), so it always re-runs. The
  newer record then wins "latest" through
  `provenance_query._supersede_same_invocation` (built 2026-09-29).
- **Pipeline "Run all" / `run_until`** passes `skip_computed=True`
  (`execution_service.py:2293`), so it skips.
- **Scripts** that pass `skip_computed=True` also skip.

Marker test: `scidb/tests/test_latest_same_invocation.py::test_skip_computed_reruns_after_the_file_is_edited`
is marked `xfail(strict=False)`. It should turn into a pass once this plan is
built; switch it to `strict=True` then.

## 2. Hard constraints

- **Never part of identity.** A fingerprint must not reach `to_key`, the
  record id or the invocation id. Otherwise moved data would re-key everything
  and fork every node on the canvas.
- **No migrations and no backfill** (beta rule). Runs saved before this is
  built have no fingerprint. For them the gate says "no fingerprint on record"
  and behaves exactly as it does today.
- **Warn first, opt in later.** This follows the user's note deferring content
  staleness (memory: feedback_defer_content_staleness): design it as its own
  surface, never as a forced stale state.
- **One owner** for "has this input file changed". It lives in scidb, not in
  the GUI.

## 3. What a fingerprint is

Record `(size, mtime_ns, sha256)` for each file that each call actually read,
and compare them in two tiers:

1. **Fast path.** If size and modification time are both unchanged, treat the
   file as unchanged. This costs one `stat`, with no read.
2. **Otherwise, hash it.** If the sha256 is unchanged (the file was only
   copied or touched), treat it as unchanged and refresh the stored
   modification time so the next check takes the fast path. If the sha256
   differs, the file changed.

Two cases to watch:

- **Moved or copied data** (a different machine, a new folder): every
  modification time changes, so the first run after the move hashes every
  file once and then settles. A large tree (GAITRite, Delsys) would be slow
  that one time, so log progress and timing (per NOTE 2).
- **Directories used as inputs**, for example a folder the function lists: a
  fingerprint of a directory is not defined. Record `(kind='dir')` and never
  call it changed. Open question Q3.

## 4. Storage (the open design point)

Fingerprints are per **call**, because a templated PathInput reads a
different file for each combo. The stored PathInput spec is one per name, so
it can't hold them.

- **A. New table** `_input_fingerprint(invocation_id, param_name, resolved_path,
  size, mtime_ns, sha256, run_id)`. Additive: created with CREATE IF NOT
  EXISTS, so old databases work without a migration. Readable per invocation.
- **B. Ride on the PathInput value record.** Each combo's resolved file
  already flows through the save as the argument value. Store the fingerprint
  JSON beside it, the way `path_input_specs` rides beside the metadata.
  Problem: that record is per name, not per call.
- **C. A `_record_save` sidecar** keyed by output record id. This works
  because the gate already starts from the output record.

Recommendation: **A**. The user prefers existing machinery over new tables
(memory: feedback_prefer_existing_machinery), but B does not fit the per-call
shape, and C couples an input fact to an output save. The table is purely
additive and has one writer: `record_run`, which already receives
`path_input_specs`.

## 5. Stages

1. **scifor.** The PathInput resolver hands back `(path, stat)` per combo, so
   the file is stat-ed once. No hashing happens here.
2. **scidb, save.** `record_run` writes a fingerprint row per
   `(invocation, argument, resolved_path)`. Hashing is lazy: store sha256
   only when the file is at most N MB, or when asked to (Q2).
3. **scidb, gate.** `_find_skip_gate_record` compares fingerprints. It adds
   the rejection reason `input file changed: <path>` to the existing
   `[skip-gate]` diagnostics.
   - In **warn mode** it logs one WARN line per run naming the changed files
     and the count of combos affected, then still skips.
   - In **re-run mode** it recomputes those combos.
4. **GUI.** Surface the warning on the node (for example a yellow "input
   changed" badge). The run-state owner (`scidb.check_multiple_nodes_state`)
   reads the same comparison, so the canvas and the gate never disagree.
5. **Tests.**
   - An edited file is detected.
   - A copied file whose modification time changed but content didn't is not
     changed, and the stored time is refreshed.
   - A moved root folder takes one hash pass, then the fast path.
   - Directory inputs are ignored.
   - Old history with no fingerprint behaves as today.
   - The xfail marker above turns into a pass.

## 6. Decisions for the user

- **Q1.** Should warn mode be the only mode at first, or ship both with warn
  as the default?
- **Q2.** Hashing budget: always hash, hash only up to N MB (and above that
  use size and modification time alone), or never hash (size and modification
  time only)?
- **Q3.** Directory inputs: ignore them, or fingerprint the listing (names,
  sizes and modification times)?
- **Q4.** Storage: option A (recommended), or something else?
