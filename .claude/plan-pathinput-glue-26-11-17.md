# Plan: PathInput → glue → function (issue #21)

**Status: BUILT 2026-11-17 (docs first, then code). Uncommitted; pytest and MATLAB tests NOT yet run.**

## The problem

`PathInput → glue → filterDelsys` fails in prepare with
`GlueUnsupportedInputError` from `scidb.run.glue.refuse_pathinput_glue`
(called in `foreach_prepare._normalize_glue`). The refusal was a scope cut
(`docs/claude/free-code-glue-nodes.md` §4a, §7). Its reason was "a PathInput
resolves per combo, so there is no loaded table at the fusion point". That is
true, but no table is needed. A PathInput-fed glue gets the **resolved path**
at each combo, the same value the function would have received, and returns
what the function should get instead.

Chesterton's fence: the refusal existed because the bulk fusion point has
nothing to reshape and `_route_provenance` has no `__record_id` to rewrite.
Both still hold. The fix is a different application site (per combo) and a
different identity route (a glue hop from the `__pathinput__` record), not
bending the bulk site.

## Decisions

| # | Decision | Choice |
|---|---|---|
| P1 | What the glue receives | The resolved path for that combo: `pathlib.Path` in Python, `string` in MATLAB. Same type the function gets today. |
| P2 | Where it runs | **Per combo**, always: the whole chain, whatever `per_schema_key` says, because no table exists before the loop. INFO says so. |
| P3 | Language | The ordinary run-language rule (`check_run_language`): Python glue in a Python run, MATLAB glue in a MATLAB run. A PathInput chain is a *deferred* chain, not a constant one. **Open question for the user below.** |
| P4 | Identity | A glue hop, the PathInput twin of the Parameter case: `__pathinput__ → glue_inv → __pathinput_glue__ → consumer`. The consumer's edge binds the virtual rid, so an edited glue body changes `invocation_id` and `skip_computed` recomputes. |
| P5 | File-content staleness | Not tracked. That matches plain PathInputs today ([[feedback_defer_content_staleness]]). The virtual rid is `compute_glue_record_id(chain_hash, pathinput_rid, "pathinput")`, so the third slot is a fixed token. |
| P6 | Row contract | Unchanged. Glue #1 gets a path, so the non-table branch applies with no row check. Any later glue in the chain that gets a DataFrame gets the normal frame contract. |

## Single owners (NOTE 4)

| Concept | Owner (new or extended) |
|---|---|
| "Is this param PathInput-fed?" (bare, `Fixed(PathInput)`, all-PathInput `EachOf`) | `glue.is_pathinput_input(spec)`: the predicate lifted out of `refuse_pathinput_glue` |
| PathInput-glue virtual rid | `glue.pathinput_glue_rid(pi_rid, entry)`, next to `constant_glue_rid` and `virtual_rid_map` |
| Version key `__pathinput_glue = {param: {"chain": h}}` | `glue.PATHINPUT_GLUE_KEY` and `glue.pathinput_glue_config_keys(...)`, next to `CONSTANT_GLUE_KEY`. Not a call-site key, the same as `__glue_hashes`. |
| The record a PathInput edge binds | `provenance.save.pathinput_edge_rid(pi_rid, entry)`, next to `constant_edge_rid`. Used by `_pathinput_bindings` (identity and `record_run`), the skip gate in `foreach.py` (~L1396, `path_input_rids`) and `_predict_config_invocations`. |
| The hop's rows | `save._add_glue_hop` (existing), through a new `_write_pathinput_glue_hop`, the twin of `_write_constant_glue_hop` |
| The record type | `provenance.graph.PATHINPUT_GLUE_TYPE = "__pathinput_glue__"`, added to `GLUE_RECORD_TYPES` and `NON_VARIABLE_INPUT_TYPES` |

## Stages

### Stage: scidb Python run
- `glue.py`: delete `refuse_pathinput_glue` and `GlueUnsupportedInputError`. Remove it from `scidb/__init__.py` as well (beta, no deprecation). Add `is_pathinput_input`.
- `glue.fuse_glue`: a PathInput-fed param (bare PathInput passes through `_convert_inputs` as a constant; `Fixed(PathInput)` arrives as a `PerComboLoader`) puts the whole chain in `per_combo` and skips `_route_provenance`, because its identity comes from the hop. Today a bare PathInput falls into the "not a table, glue applied to the value as-is" branch, which would call the glue **on the PathInput object**. INFO: `[glue] '<p>': fed by PathInput '<name>' — chain applied per combo to the resolved path`.
- `_for_each_prepare`: compute the `__pathinput_glue` entries (chain hash) once, put them in the version keys, and hand them to the skip hook through the same ref pattern as `_constant_glue_ref` (generalize that ref so it carries both heads).
- The per-combo site already exists: in `foreach.py` the `fn` wrapper's `_per_combo_glue` loop runs on `resolved[param]`. scifor has already resolved the PathInput to its path at that point (`scifor/foreach.py` ~L1625). No change there.
- Confirm that `Fixed(PathInput)`'s `to_key()` lands in `__inputs` as PathInput JSON, so `_pathinput_specs` finds it. If it doesn't, settle that first.

### Stage: provenance save and readers
- `save._pathinput_bindings` → `pathinput_edge_rid`; `record_run` writes the hop (`_write_pathinput_glue_hop`) plus an INFO summary like `constant_glue_hops`. The existing identity-drift `RuntimeError` acts as the agreement guard.
- `query.py` readers that switch on `CONSTANT_GLUE_TYPE` also need a `PATHINPUT_GLUE_TYPE` branch, which reports the param as a **PathInput** (its spec value) plus a `pathinput_glue` detail (`glue_chain`, `glue_chain_hash`, `glue_record_id`, `pathinput_record_id`):
  `glue_source_batch` (also return the source spec), `invocation_edges_batch`, `stored_invocation_signature`, `_build_upstream_closure`, the variant-config builder (~L343), `realized_inputless_schema_ids`, `_predict_config_invocations`.
  To keep one owner, add one helper, `_glue_head(rtype, src)` → `("constant" | "pathinput" | "variable", ...)`, and have every site dispatch through it rather than repeating type checks.
- `pipeline_variants` / `aggregate_pipeline_variants` carry the chain into `fn_glue_chains`, so `graph_builder.history_glue_on_canvas` hides the direct history `PathInput → fn` edge when a matching glue is drawn.

### Stage: MATLAB
- Prepare is shared Python, so the stages above already cover identity and the language check.
- `scimatlab/bridge/entry.py`: the prepare result gains `glue_per_combo: [param, ...]` (from `state.per_combo_glue`).
- `+scidb/for_each.m`: `apply_matlab_glue` skips per-combo params (today it would call `h(PathInputObj)`). It then builds `input_transforms` (param → `@(v) apply_glue_chain(v, handles.(param), param, schema_keys)`, reusing `apply_one_glue`) and passes `'_input_transforms'` to `scifor.for_each`.
- `+scifor/for_each.m`: a new private option `_input_transforms` (struct name → handle). In `run_combos`, right after `prepare_value`, apply it when present. It sits inside the existing try/catch, so a glue error fails **that combo only**, like a missing file. scifor stays policy-free: it is a generic per-combo hook, like `_pathinput_loader`. Check whether the kwarg-parity test needs an exemption for an underscore option ([[project_for_each_kwarg_parity]]).
- GUI `matlab_command_format._format_glue_struct`: no change expected (it already emits a handle for MATLAB glue). Verify with a generator test.

### Stage: GUI
- `edge_resolver.py`: comment only (the head is already bound honestly).
- `tests/test_glue_nodes.py` ~L179: change the refusal assertion into a run assertion.

### Stage: docs
- `free-code-glue-nodes.md`: update the §4a table row (PathInput: per combo, `__pathinput_glue__` hop), the §7 bullet, and the §6 diagnostics list. Update the `glue.py` module and `is_constant_input` docstrings.

## Logging (NOTE 2)
- INFO in prepare: PathInput-fed chain, per-combo placement, `per_schema_key` ignored when it is false.
- DEBUG per combo: the existing `apply_glue_chain` non-table line (`Path -> DataFrame`).
- DEBUG per hop and INFO summary in `record_run`.
- On skip: the existing `[skip] binding '<p>' changed (old → new)` line proves that a glue edit propagated.

## Tests
- `scidb/tests/test_glue.py` (replaces the refusal test at ~L600):
  - The glue receives a `Path` per combo, and the function receives the glued value.
  - A second run with the same glue skips. This proves the skip target equals the saved binding.
  - An edited glue body recomputes, and the binding moves to a new `__pathinput_glue__` rid.
  - History: the consumer binds `__pathinput_glue__`, and the source of that record is the `__pathinput__` record. `invocation_edges_batch` reports the param under `path_inputs` with `pathinput_glue`.
  - `Fixed(PathInput)` plus glue.
  - Two PathInputs, only one of them glued.
  - A Python run with MATLAB glue on a PathInput is refused (`GlueLanguageMismatchError`).
  - A glue error on one combo fails only that combo.
- MATLAB (`scimatlab/tests/matlab`):
  - The scifor `_input_transforms` option is applied per combo, and its error is per combo.
  - scidb: PathInput → MATLAB glue → fn runs, skips on rerun, and recomputes after an edit.
- GUI: a graph_builder test where a drawn `PathInput → glue → fn` matches history and no direct edge is drawn.

## Revision 2026-11-17: user feedback

### Glue language is independent of the run's language (user decision)
Glue should not care which language feeds it, or which language the function it feeds is written in. This replaces P3 and the "language must match" rule in `free-code-glue-nodes.md` §1.

| Glue \ Run | Python run | MATLAB run |
|---|---|---|
| Python glue | Works today | Cheap. The variable head already runs in Python prepare, so dropping `check_run_language` is enough. The constant head works today. The PathInput head is called per path from MATLAB through a bridge entry; the result converts with `from_python`. |
| MATLAB glue | **Hard.** scidb (a Python process) has no way to execute `.m`. It needs `matlab.engine` (an optional dependency, a slow first start, and no table type crossing) or the GUI's MATLAB sidecar, which would be a layering violation. | The variable head works today. The PathInput head uses `_input_transforms`. The constant head is currently refused; it needs MATLAB to apply it before prepare and hand Python the glued value to hash. |

### Glue runs at the PathInput's level, not the function's
- As first drafted, the glue ran once per **function** combo. A `{subject}` PathInput feeding a `[subject, session, trial]` function would re-glue the same file once per trial.
- New rule: within one run, the glue runs once per **distinct resolved path**, which is the PathInput's own level. With no keys that means once per dataset; with `{subject}` keys, once per subject.
- One rule, owned by `glue.py`: the cache key is `(param, resolved path)`, and the cache is scoped to one run.
- Python hands each combo a `copy.deepcopy` of the cached result, so a function that mutates its input cannot leak into the next combo.
- MATLAB's transform uses a `containers.Map` with the same key, which is fine under value semantics. A parity test pins both.
- Identity is unaffected: the PathInput record is one per name, so it is already level-free.
- Logging: DEBUG on a cache hit or miss, plus an INFO line at the end of the run: `glue '<p>': N distinct path(s) glued for M combo(s)`.

### Decided 2026-11-17
- **MATLAB glue in a Python run: refused for now.** It is the only refused cell in the matrix. The error names the glue and the param, and says to rewrite the glue in Python or run the function in MATLAB.
- **MATLAB glue on a Parameter in a MATLAB run: included.**
  - After Step 0 expands each EachOf, `+scidb/for_each.m` applies the MATLAB constant chain to the concrete value, before calling `for_each_prepare`.
  - It passes both the raw value and the glued value. Python records the raw value in `__constants`, hashes the glued value into `glued`, and writes the `__constant_glue__` hop exactly as `apply_constant_glue` does today.
  - One owner for the `ConstantGlue` record is kept: Python builds it from `(raw, chain, glued)` whichever language ran the body.
- **One language gate.** `check_run_language` and `check_constant_glue_language` are replaced by `glue.check_glue_executable(chains, run_language)`. Its only refusal is MATLAB glue in a Python run.
- **Still refused:** a mixed-language chain on one param (`_validate_chain`). On a variable head the Python part runs in prepare and the MATLAB part runs after it, so a mixed chain would run out of order. That would need every element applied MATLAB-side, with tables crossing the bridge once per element. Revisit only if someone asks for it.

### Python glue in a MATLAB run, per head
- **Variable head:** applied at the fusion point in Python prepare (it already works there). MATLAB rebuilds the reshaped table from `for_each_describe_loaded_input`. `glue_chains` in the prepare result lists only the params whose chain MATLAB still has to apply.
- **Constant head:** unchanged; it already works.
- **PathInput head:**
  - The `_input_transforms` handle calls a bridge entry, `scimatlab.bridge.apply_glue(call_token, param, value)`. That entry holds the chain reconstructed in prepare, and it owns the per-path cache from the section above.
  - The result returns through `scidb.internal.from_python`, so this direction has one cache owner (Python).
  - MATLAB glue on a PathInput head uses the MATLAB-side cache.

### Extra tests
- A Python-glue × MATLAB-run test for each head.
- A MATLAB constant glue in a MATLAB run that skips on rerun and recomputes after the body is edited.
- MATLAB glue in a Python run is refused, and the message names the glue.
- A cached PathInput glue at `{subject}` under a `[subject, session, trial]` function runs once per subject; its call count is asserted.
- A Python function that mutates its glued input does not affect the next combo.

## Superseded: P3 open question
**P3: Python glue on a PathInput in a MATLAB run.** The recommendation is to refuse it under the normal run-language rule. Allowing it would mean applying Python glue per combo inside `matlab_loop._resolved_paths` and converting each glued value (which may be a DataFrame or dict) across the bridge per combo. That is a second application site and a second conversion path. If the user's existing `config` glue is Python (it was Parameter-fed before), they would rewrite it as a `.m` glue.
