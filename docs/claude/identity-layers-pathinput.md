# Three identity layers, and how a PathInput-fed step moves through them

Written 2026-09-25 after two `pandas.read_csv` canvas nodes, reading two
different files, had their wiring rewritten between runs.

## The layers

| Layer | Owner | Recipe | Answers |
|---|---|---|---|
| `invocation_id` | `scidb.provenance.compute_invocation_id`, fed by `provenance_save.invocation_identity` / `record_run` (save), `_predict_config_invocations` (predict) | fn_hash + run options + **every input edge** (variable rids + selectors, constant rids, PathInput spec rids) | "Which exact call produced this record?" One `_invocation` row; outputs hang on it via `_invocation_output`. |
| `call_id` | `scidb.foreach_config.CallSite` (forward `ForEachConfig.to_call_id`, backward `provenance_query.config_call_id` / `pipeline_variants`) | fn name + input TYPES (a PathInput contributes its `to_key()`) + constants + run options + glue names | "Which for_each call site is this?" `pipeline_variants` rows group by it; the GUI's `fkey = (fn, call_id)`. |
| `wiring_id` | `scidb.provenance.compute_wiring_id` (GUI `graph_builder.wiring_id` delegates) | fn name + loadable-input shape + output types + `{param: declared PathInput name}` — no constants | "Which canvas node?" Constant sweeps share one node. Dispatch records a claim (`node_wiring`) so the next build attributes the run to the node that ran it. |

Each coarser layer is built FROM what the finer one recorded: `pipeline_variants`
reads invocations and their edges; `aggregate_pipeline_variants` groups them by
`(fn, call_id)`; `group_call_sites_by_wiring` maps each call site's wiring to a
node token.

## Why the PathInput has to be in all three

Before 2026-09-25 the PathInput spec was in `call_id` and `wiring_id` but NOT
in `invocation_id` (WON'T DO 2026-06-21). For a function whose ONLY input is a
PathInput, the invocation then had no edge left to hash, so every file it read
produced the same `invocation_id`:

1. Run 1 `read_csv(Symmetry) -> SymmetryTable` creates invocation X.
2. Run 2 `read_csv(Unmatched) -> UnmatchedTable` writes into X as well. X
   now has two PathInput edges on one param and outputs of both types.
3. `pipeline_variants` saw `1 invocation(s) -> 2 variant(s)` and kept ONE spec
   per param (dict, last wins). The result was one call site with two outputs
   and the Unmatched input.
4. That call site's wiring matched neither dispatch claim, so a fresh node was
   allocated. Both manual nodes graduated onto it ("graduation collision"),
   their edges were rewritten, and the Symmetry edge vanished.

A finer layer that merges what a coarser layer keeps apart cannot be
repaired downstream: the invocation row no longer says which output came
from which file.

## Rule now: the NAME is the identity

`scifor.PathInput(template, ..., *, name)` — `name=` is REQUIRED, in scripts,
declarations and MATLAB alike (`+scifor/PathInput.m`). Two serializations:

| | owner | holds | used for |
|---|---|---|---|
| `to_key()` | `scifor.pathinput.path_input_key(name)` | `{"__type": "PathInput", "name": ...}` | EVERY identity: `__inputs` version key (so record ids), `call_id`, `invocation_id`, the skip gate |
| `to_spec()` | `PathInput.to_spec` | name + template + root_folder + regex/aliases/key_regex | stored as the `__pathinput__` record's `value_repr`, for display and re-discovery — never hashed |

One PathInput record per name (`compute_pathinput_record_id` reduces a key OR
a stored spec to the name via `provenance.path_input_key_of`), bound as an
identity edge like a constant:

- save: `provenance_save._pathinput_bindings` (`invocation_identity`,
  `record_run`); the spec rides beside the metadata as
  `record_run(path_input_specs=)` (`parameter.path_input_specs_of`) because
  anything in the metadata reaches the record id;
- the stored spec is refreshed on every run (`_commit_graph`
  `3b_pathinput_spec`); a change logs INFO `PathInput 'X': location changed …
  identity is the name, so nothing re-runs because of it`;
- predict: `_predict_config_invocations` binds `cfg["path_inputs"]`; the
  never-run fallback `config_from_inputs` carries them too;
- call site: `CallSite.version_keys` reduces any PathInput value to its key;
- skip: `stored_invocation_signature()["path_inputs"]` vs the live key.

Consequences:

- moving the data or running on another machine changes nothing: same
  invocations, same record ids, `skip_computed` skips;
- two names are two PathInputs even over the same files;
- pointing a name at genuinely different files does NOT fork — re-run it
  yourself, as for edited file contents;
- a RENAME forks: every run is recorded under the old name. The GUI's rename
  record (`record_path_input_rename`) re-attributes old runs to the renamed
  node for display, but their identity is not rewritten (no migrations).

Uniqueness:

- declarations: `parameter.path_input_declaration` — every arm of an `EachOf`
  must carry one name; a binding whose object carries another name is a
  re-export if that name is also bound, else a load error. A name declared in
  two files is a load error (`registry._register_path_input`, first wins);
- one `for_each` call: `parameter.check_path_input_names` refuses two
  arguments with one name but different specs (EachOf alternatives exempt);
- canvas: one node per PathInput per scope (`layout_service.put_layout`,
  refusal `reason: duplicate_path_input`; the frontend pre-checks).

Tests: `scidb/tests/test_pathinput_invocation_identity.py`,
`scidb/tests/test_step_grouping_f38.py`,
`scistack-gui/tests/test_path_input_uniqueness.py`.

## Diagnosing a recurrence

- `[timing] pipeline_variants: N invocation(s) -> M variant(s)` with M > N for
  a single-output function means one invocation spans several call sites.
- `[graph_builder] call site fn/cid records K output types from ONE set of
  inputs`: the same symptom from the GUI side.
- `[node_wiring] … now runs as wiring W (run_id=None)` right after a run, with W
  different from the `claims … ['…']` id logged at dispatch, means the build
  saw a wiring nobody ran.
