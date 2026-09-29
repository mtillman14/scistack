# Plan: hiding a PathInput must not fork a function node

Doc: `docs/claude/hidden-path-input-identity.md`. Found in scidb.log 2026-09-28
(`pandas.read_csv`, runs qocir8eg / 5excolla / kqd9p8gw).

## Stage 0: failing tests first (written, NOT yet run)

`scistack-gui/tests/test_hidden_path_input_identity.py`, 4 tests. All four are
expected to FAIL on an assertion (not an error) before any code changes:

| test | expected failure today |
|---|---|
| `test_no_second_node_is_minted` | a 2nd `fn__load_table__…` id is allocated |
| `test_the_node_stays_on_the_canvas` | the canvas shows the phantom id, not the original |
| `test_the_drawn_path_input_is_the_binding` | binding stays `RAW` (PathInput overrides are skipped) |
| `test_the_drawn_output_is_the_output` | output stays `LoadedTable` |

Gate: the user runs them and confirms they fail for those reasons. If one
errors in the fixture, fix the fixture first.

### Stage 0b: the general rule, for EVERY node type (added after review)

The user's rule: hiding ANY node (PathInput, Parameter, Variable or Function)
must leave every OTHER node's identity alone, whatever its type.
`TestHidingAnyNodeKeepsEveryOtherNodesIdentity` is parametrised over 7 hide
targets: the `RAW` PathInput, `low_hz`, `RawSignal`, `FilteredSignal`,
`LoadedTable`, `load_table` and `bandpass_filter`. It runs 3 tests on each:

| test | asserts |
|---|---|
| `test_hide_mints_and_rewires_nothing` | `_node_wiring`: no function id allocated or dropped, no current wiring changed |
| `test_every_other_node_keeps_its_canvas_id` | every node of every type on the canvas keeps its id, and no new id appears |
| `test_every_other_node_stays_on_the_canvas` | nothing but the hidden node leaves the canvas (a display cascade, reported separately from identity) |

Run 1 (the earlier function-only version): only the two `RAW` cases failed.
That confirmed PathInput is the only leak among function identities, and that
a hidden function node leaves the canvas cleanly. The all-type version still
needs a run.

## Stage 1: no filter before identity (root cause), IMPLEMENTED 2026-09-28, pytest unrun

**The structural fault.** Every step in `_build_graph` before grouping
(identity, `hidden_wirings`, `wiring_disconnected_fkeys`, run states,
`input_params_with_manual_edges`, `group_call_sites_by_wiring`) hashes
`wiring_id` from the aggregate. A pre-identity `filter_hidden` pass mutated
that aggregate, so each kind of hidden node was one flag away from leaking.
Hidden output variables leaked first (fixed with `strip_var_type_values=False`),
then hidden PathInputs.

**Chosen fix: remove the pre-identity pass, instead of the `agg.fn_wiring`
design drafted earlier.** Before grouping, the aggregate is exactly what
`build_aggregate` produced, the same thing `ensure_node_identities` already
used. That makes it true for every node type by construction, changes no
function signatures (38 test call sites stay as they are), and removes a
flag instead of adding a field. `fn_wiring` stays as a possible follow-up
only if a filter ever has to run before grouping.

- `api/pipeline._build_graph`: the pre-grouping `filter_hidden(...,
  strip_var_type_values=False)` is deleted, with a comment saying why. The
  post-grouping call is now THE filter.
- `graph_builder.filter_hidden`: the `strip_var_type_values` flag is removed
  (a clean break), and it always strips. The docstring says it is display-only,
  runs after grouping, and takes grouped `(fn, token)` keys. It now logs how
  many hidden function ids matched a node.
- `domain/node_identity._warn_minted_beside_orphans`: WARN when a function
  mints a node in the same pass that one of its existing nodes matches
  nothing in history. That is the signature of this bug, whatever kind of
  node was hidden.
- Tests: `TestFilterRunsAfterIdentity` is an AST guard that `_build_graph`
  calls `filter_hidden` exactly once, after `_resolve_node_identity` and
  `group_call_sites_by_wiring`. The four `strip_var_type_values=False` unit
  tests in `test_graph_builder.py` were removed with the flag, and
  `test_removes_hidden_fn` kept the one behaviour worth keeping.
- Side effects, intended but unverified:
  - A hidden Parameter's pending values can now auto-clean. Before, the
    filter emptied `const_fns`, so they never could.
  - Run states are computed for hidden variables. They are dropped for
    display afterwards.
- Hidden function nodes: the old pre-grouping `hidden_fkeys` branch was dead,
  because call_id-keyed data never matches the node token. The live branch
  was always the post-grouping one.
- A hidden PathInput still resolves to the RECORDED binding at run time,
  although the canvas shows the handle unbound. That breaks
  "visible edges are ground truth", and Stage 2 covers it.

Expected after Stage 1: `TestHidingAPathInputKeepsTheNode` (2 tests), the
`RAW` identity cases, and `TestFilterRunsAfterIdentity` pass.
`TestRewiringAfterTheHideRuns` (2 tests) still fails, for Stages 2 and 3.

## Stage 2: drawn PathInput edges override on history nodes, IMPLEMENTED 2026-09-28, pytest unrun

- The one owner stays `graph_builder.manual_input_overrides`. Return PathInput
  sources as a typed override (`{"kind": "pathinput", "ref": name}`), not
  skipped. Resolve the source with the `edge_resolver` rule the manual-node
  path already uses, so there is still one rule.
- Consumers: `reconcile_manual_inputs` substitutes a `BINDING_PATHINPUT`
  binding and recomputes the wiring with the new PathInput term. The display
  overlay shows it. `input_params_with_manual_edges` (colour) folds it in.
- The existing INFO `manual edge(s) override …` line then covers PathInputs too.

## Stage 3: drawn output edges on the per-node run path, IMPLEMENTED 2026-09-28, pytest unrun

- `derive_target_for_node`: apply the same output override `derive_fn_targets`
  uses (`infer_manual_fn_output_types`), scoped to `{node_id}`. Don't copy the
  logic; move it into one helper that both call.
- The message `no targets — graduated node … no manual edges to infer from`:
  list the edges actually drawn on the node, and say whether they were
  considered.

## Stage 4: docs + manual GUI check, docs DONE; GUI §0zzm unchecked

- Update `docs/claude/hidden-path-input-identity.md` "Two more gaps" to done.
  Add a row to the `manual-edges-on-history-nodes.md` table.
- `docs/gui-manual-testing-todo.md`: hide a PathInput, draw a new one plus a
  new output, then Run. Expect ONE node, the run uses the new file and saves
  the new variable.

## The existing Stroke-R01-Aim-2 database (no migration, by policy)

After Stage 1 the call site maps back to `rlw90w`, which returns to the canvas.
The phantom `cdb48…` keeps an association to a wiring no history holds, so it
drops off the canvas, and the edges drawn on it are orphaned. Redraw
`DemographicsPath2 → rlw90w` and `rlw90w → DemographicsTable2`, and hide the
orphans. Separately, resolve the duplicate `Demographics` /
`DemographicsTable` declarations (`src/vars/*.m` vs `scistack_entities.toml`,
plus the duplicate at toml line 13).

## As built (Stages 2-4), 2026-09-28

- Stage 2: `graph_builder.manual_path_input_overrides` is a sibling of
  `manual_input_overrides`, not a mixed-kind return value. It's used by
  `reconcile_manual_inputs` (the Run) and `_claim_stated_wirings` (so a
  script run through the drawn PathInput keeps one node). The display
  overlay and colour are unchanged, because PathInputs aren't part of
  `input_params`.
- Stage 3: `execution_service._apply_manual_output_types` was extracted from
  `derive_fn_targets`, and `derive_target_for_node` now calls it too. The
  stuck-node log now lists the edges drawn on the node.
- Tests added: `TestManualPathInputOverrides` (pure),
  `TestAScriptRunThroughTheDrawnPathInput`,
  `TestStuckNodeLogNamesItsDrawnEdges` and `TestMintedBesideOrphansWarns`.
- Unrelated fix: `test_plot_service::test_schema_keys_no_longer_write_row_filters`
  matched the literal `<Section title="Schema keys">`, which 4036eaf1 had
  wrapped across lines. It now matches a regex.
- Left open, in the doc's "Known limitations": a hidden input node with
  nothing drawn still runs its recorded binding; drawn outputs aren't part of
  script-run claims; edges from a still-manual PathInput node's suffixed id
  are unverified.

## Follow-up 2026-09-29: re-key a run hand-dragged node (uncommitted, pytest unrun)

Found in scidb.log after Stages 1-4 shipped. `rlw90w` is a hand-dragged
node's 6-char id. It became the history node's id through its dispatch
record, but `ids.parse_fn_node_id` rejects it, so Run returned nothing
without logging and its drawn edges weren't indexed.
- `node_identity.resolve_identities`: a claimant outside the id grammar is
  re-keyed to a minted id (`IdentityPlan.rekeys`). All its wirings go to one
  new id, and it keeps its current shape.
- `api/pipeline._resolve_node_identity`: applies each re-key with
  `pipeline_store.rebase_node` and `layout.rebase_node_positions` before
  `to_record`. `_build_graph` re-reads hidden and manual rows after a re-key.
- `execution_service.derive_target_for_node`: every early `return []` now
  logs its reason.
- Tests: `TestRunManualNodeIsReKeyedPure` and `TestRunManualNodeEndToEnd`.
  The `test_node_identity.py` placeholder ids were padded to 16 hex, with
  their order preserved.
