# Plan: Parameter-fed glue gets a history hop (option B) — 2026-10-21

## The bug (from scidb.log, run uw6ncxs0, 14:00:22)

`config` (Parameter, value = path to config.json) → `glue_xsens_config` →
`load_xsens_one_file.col_names`.

1. `glue.apply_constant_glue` replaces `inputs['col_names']` with the glued
   dict **before** version keys are built, so `__constants = {col_names: {dict}}`.
2. `provenance_save.record_run` writes ONE constant edge `col_names → crid(dict)`
   and stamps `declared_name = 'config'` on it (`provenance_save.py:712-716`).
3. History now says "Parameter `config` = {dict}", and the edge
   `param__config → fn.col_names` skips the glue node (the history has no glue hop
   for a constant head).

Two concepts are conflated on one edge: **the Parameter's value** (raw path) and
**what the function received** (the glued dict). Variable-fed glue already
separates them with a virtual `__glue__` record (docs/claude/free-code-glue-nodes.md
§2). Constant-fed glue skipped that "because identity was right for free".

## Target shape

```
_record CONSTANT  crid(raw path)        ← declared_name 'config' on THIS edge
      │  (glue_inv, 'input', crid(raw))
_invocation  glue_xsens_config  fn_hash = "{chain_hash}:{glued_hash}"
      │  output
_record __glue__  vrid = compute_glue_record_id(chain_hash, crid(raw), glued_hash)
      │  (fn_inv, 'col_names', vrid)
_invocation  load_xsens_one_file
```

The function's invocation edge points at the virtual rid. That is the same
"one extra hop" mechanism as variable glue, reusing `_write_glue_nodes`,
`glue_source(_batch)` and `glue_invocation_ids`.

`glued_hash = canonical_hash(glued value)` goes into the third slot of
`compute_glue_record_id` (the `input_set_signature` slot; a constant has no
input set). This keeps the one property the current design has and a pure
chain-hash design would lose: if the content of **config.json** changes but
the path and glue body don't, the glued value changes, so the downstream
result goes stale.

## Decisions to confirm

- **Q1 — what `__constants` holds for a glued param.** Recommended: the **raw**
  value (the path). Effects:
  - call_id and variant coordinates show `config = <path>`, the value the
    user declared, not a dict. `__glue` names already fork the call site.
  - the glued content enters identity only through the virtual rid, the same
    split as `__fn` vs `__fn_hash`.
  - Alternative: keep the glued value in `__constants` and only add the hop.
    That's smaller, but the variant label still shows the dict.
- **Q2 — glued_hash in the virtual rid.** Recommended yes (see above).
  Cost: an edit to the glue body that doesn't change its output doesn't
  invalidate anything, which is a correct outcome.
- **No migration (beta rule).** Existing records keep their old shape. A
  re-run produces the new shape, and skip_computed sees a different
  invocation id, so the first run after this change recomputes once.

## Stages

### Stage 0 — diagnostics + failing regression tests (scidb) — WRITTEN 2026-10-21, pytest unrun
Q1 = raw value in `__constants`, Q2 = glued hash in the virtual rid: both YES (user, 2026-10-21).
File: `scidb/tests/test_constant_glue_provenance.py`. 6 target tests are `xfail(strict=True)`;
each stage removes the markers it satisfies. 3 recompute guards pass today. The record_run
INFO log line moved to Stage 2 (it names the hop that Stage 2 writes), with a caplog test there.

**Found by Stage 0 (user ran pytest 2026-10-21):** `test_an_unchanged_rerun_skips` FAILED —
a PRE-EXISTING bug: Parameter-fed glue never skips. `_build_skip_hook` (foreach.py:645) is built
before prepare's `apply_constant_glue` (foreach.py:1671), so `target_const_hashes` hashes the RAW
value while history stores the GLUED value → `const_hashes mismatch` (DEBUG only, foreach.py:1302)
on every combo, forever. Test now xfail(strict) with that reason. The existing
`test_glue.py::test_editing_a_constant_glue_body_recomputes` passed only because nothing ever skipped.

Stage 1 addition: the skip hook becomes a consumer of `constant_edges` — for a glued param its
target is the virtual rid. The glued hash exists only after prepare runs the glue, so prepare
fills a `constant_glue_ref` on the hook (same pattern as `level_ref` / `_agg_binding_ref`).
Log ONE INFO line per glued param in the hook ("[skip] cfg: glued constant, target virtual rid
…"), not per combo (a const mismatch is normal for multi-valued Parameters, so the per-combo
rejection stays DEBUG).
- `scidb/tests/test_constant_glue_provenance.py`, running a Python `for_each`
  with a Parameter → glue → fn wiring (glue reads a JSON file at the path):
  1. the declared-name constant edge for `config` carries the **raw path**,
     not the dict (this is the reported bug);
  2. history has a glue invocation, found by `glue_invocation_ids`, and the
     consumer edge's rid is `__glue__`;
  3. re-run with nothing changed → skipped;
  4. edit the glue body → recomputes;
  5. edit the **JSON file content** (same path) → recomputes;
  6. `invocation_identity(meta, bindings)` == the id `record_run` writes (the
     identity-drift guard stays the referee).
- Logging: `apply_constant_glue` already logs at INFO. Add an INFO line in
  `record_run` naming the constant glue hop:
  `[provenance] fn=X: param 'col_names' ← glue [g] ← Parameter 'config' (raw crid …, virtual …)`.

### STATUS 2026-10-21 — Stages 1-3 + the one-owner audit implemented, pytest NOT run
User approved the audit fixes #1-#5 and option (b) for #6:
- #1 `provenance_save.constant_edge_rid` is THE rule (constant_bindings + record_run).
- #2 `glue.constant_glue_rid` beside `glue.virtual_rid_map`.
- #3 `provenance_save._add_glue_hop` writes the hop rows for both heads.
- #4 `parameter.single_value` (ForEachConfig + apply_constant_glue).
- #5 prepare computes the skip targets once (after declared_spelling) into `_constant_glue_ref["rids"]`;
  the hook derives nothing. Gate compares `stored_invocation_signature(...)["constant_glue"]`.
- #6(b) new record type `CONSTANT_GLUE_TYPE = "__constant_glue__"` + sets `GLUE_RECORD_TYPES`,
  `NON_VARIABLE_INPUT_TYPES`, `INTERNAL_RECORD_TYPES` in scidb.provenance; `_types_sql` helper; all
  readers switched (provenance_query, variant_pins, variant_delete, inspect/api, inspect/graph).
  `variant_keys_batch` now reads through `invocation_edges_batch` (it had its own SQL copy).
  BEHAVIOUR CHANGE: `is_inputless_function` now counts a VARIABLE-head glue record as a variable input
  (it previously excluded all glue — disagreeing with `realized_inputless_invocations`).
- Tests: xfail markers removed; TestOneOwner added (branch params parity, variant key parity,
  inputless via type, INFO hop summary once, AST guard against type-string literals outside provenance).
- Docs: free-code-glue-nodes.md §1 + §4a table rewritten.
Stages 0-3 + audit: user ran the tests 2026-10-21 and they PASS.

### STATUS — Stage 4 (GUI) implemented 2026-10-21, pytest NOT run
- scidb `aggregate_pipeline_variants` carries `functions[fkey]["glue_chains"]` (both heads).
- GUI `AggregatedData.fn_glue_chains` (filled in aggregate_from_scidb, carried through the split + grouping).
- `graph_builder.history_glue_on_canvas` + `build_edges(fn_glue_chains=, manual_nodes=)`: the direct history
  edge is omitted when the drawn chain matches the recorded one; INFO logs both outcomes.
- Tests: scistack-gui/tests/test_history_glue_edges.py; scidb TestAggregate.
- GUI manual test §0zzzzz; doc §1 "On the canvas".
Also fixes the same direct-edge problem for VARIABLE-head glue (pre-existing).

### Stage 1 — one owner for "the edge of a constant input" (scidb)
- `apply_constant_glue` returns
  `{param: ConstantGlue(raw, chain_hash, glued_hash)}` and still swaps the
  value passed to the function. `foreach` keeps the **raw** value for the
  version keys (Q1). It carries `__constant_glue = {param: {chain_hash,
  glued_hash}}` into the save metadata. That key is excluded from call_id,
  like `__glue_hashes`.
- New single helper `provenance_save.constant_edges(meta)` → `[Binding]`,
  which returns a virtual rid for a glued param and a crid for every other
  param. Both `invocation_identity` and `record_run`'s inline copy
  (`provenance_save.py:634-649`) call it, so the two can't diverge
  (NOTE 4).
- MATLAB path: `scimatlab.bridge` calls prepare directly (EachOf
  unexpanded, `is_constant_axis` branch). Return the per-alternative
  ConstantGlue too, and test it via the bridge's Python entry point.

### Stage 2 — write the hop (scidb)
- `_write_glue_nodes` accepts constant heads. The virtual `_record` gets
  `schema_id = NULL` (a constant has no location). The glue invocation's
  input edge is `crid(raw)`. `declared_name` moves from the consumer edge
  to that input edge.
- `variant_delete` and `variant_pins` already skip `GLUE_TYPE`. Add tests
  to confirm.

### Stage 3 — read paths undo the hop (scidb)
- `glue_source(_batch)`: a constant source reports `variable_type =
  CONSTANT_TYPE` and the raw value. `invocation_inputs` /
  `invocation_edges_batch` (`provenance_query.py:283-305, 1692-1727`) put
  the **raw** value into `constants[param]`, with `glue_chain` beside it.
- Prediction (`_predict_config_invocations`) replays recorded constant
  edges. A glued one replays its recorded virtual rid as-is, so there is
  no re-hash and no need to run the glue at prediction time. A never-run
  Parameter value is red anyway.
- Audit every `CONSTANT_TYPE` branch in provenance_query (≈8 sites listed
  by grep) for "edge rid is a constant record" assumptions.

### Stage 4 — GUI consumes, owns nothing new
- graph_builder draws `param__config → glue → fn` from history: the
  declared Parameter name now arrives via the glue hop, so the history
  edge no longer skips the glue. The manual edges stop being "superseded
  by DB-derived".
- The Parameter's values list no longer receives the dict. Test in
  `scistack-gui/tests`.
- The edge you hid (`e__param:config__fn:…__col_names`) and the hidden
  dict value stay hidden (they're intent rows). Note it in the manual
  test doc.

### Stage 5 — docs
- `docs/claude/free-code-glue-nodes.md` §1 "constant-fed parameter" and the
  §4a table: identity is now the virtual glue record (with glued hash), not
  the glued value in `__constants`.
- gui-manual-testing-todo entry.

## Risks
- **Identity drift** between `invocation_identity` and `record_run`: removed
  by the shared helper, and the existing hard RuntimeError guards it.
- **Multi-valued Parameter** (EachOf): Step 1 recursion means each call
  records its own hop. Test with 2 values.
- **MATLAB run**: constant glue is applied in Python prepare on both paths,
  so the hop is written by the same `record_run`. That needs one real
  MATLAB run (standing gap).
