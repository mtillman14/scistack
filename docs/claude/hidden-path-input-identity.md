# A node's recorded shape vs the edges you draw

Written 2026-09-28 after a real session (`pandas.read_csv`,
`DemographicsPath → Demographics`) where every Run said *"No pipeline history
or output connections found"* on a node with edges plainly drawn on it.

Read with `node-identity.md` (why node ids are allocated) and
`manual-edges-on-history-nodes.md` (how drawn edges override history).

## Three things that all look like "the node's wiring"

| | what it is | where it lives | who writes it |
|---|---|---|---|
| **recorded wiring** | the shape a call site actually ran as: `wiring_id(fn, variable inputs, outputs, PathInput bindings)` | provenance, recomputed each build from `get_aggregated_variants` | a run |
| **current wiring** | which recorded wiring the node *is* right now | `_node_wiring`, latest `last_seen` (`node_wiring.current_wiring`) | a dispatch (`record_dispatch_wirings`) or a graph build (`_resolve_node_identity` → `plan.to_record`) |
| **drawn edges** | what the user wants the next run to use | `_pipeline_edges` (manual edges), hidden-edge / hidden-node statements in `_intent` | the canvas |

The id is allocated once and looked up by current wiring. **Hiding and
drawing never change the id or the current wiring.** What you draw is folded
in *on top*:

- **display:** `graph_builder.manual_input_overrides` → `overlay_manual_inputs`
- **run:** `derive_target_for_node` picks the history targets whose recorded
  wiring equals the node's current wiring, then
  `variant_resolver.reconcile_manual_inputs` swaps in the drawn bindings
- **after the run:** the new recorded wiring is claimed by this same node
  (dispatch record, or `stated_wiring_claims` for a script run) and becomes
  its current wiring

## The invariant

> The identity pass (history wirings → node ids) must compute every wiring
> from what history **recorded**, never from the filtered view.

`api/pipeline._build_graph` calls `filter_hidden` twice. The first call,
before `_resolve_node_identity`, passes `strip_var_type_values=False` so
hidden **variable** types stay in `fn_input_params` / `fn_outputs` (see the
`plan-scope-hidden-nodes-edges` postmortem in the `filter_hidden` docstring).
The second call, after identity is fixed, strips them for display.

**Hidden PathInputs broke this.** The first call still popped hidden
PathInputs out of `agg.path_inputs`. `wiring_id` has a PathInput term
(`path_input_bindings_by_fkey(agg.path_inputs)`), so hiding a PathInput node
changed the recorded wiring the identity pass saw.

**The rule is general, not about PathInputs.** Hiding any node (PathInput,
Parameter, Variable or Function) is a view decision and must not mint,
re-key or rewire any other node. The underlying fault was not PathInput-specific.
Every step in `_build_graph` before grouping hashes `wiring_id` from the
aggregate, and a pre-identity `filter_hidden` pass mutated that aggregate, so
each kind of hidden node was one flag away from leaking.

**The fix (2026-09-28): there is no filter before identity.** `filter_hidden`
runs once, after `_resolve_node_identity` and `group_call_sites_by_wiring`, and
it is display-only. The `strip_var_type_values` flag is gone. Everything
before grouping sees exactly what `build_aggregate` produced, the same data
`ensure_node_identities` reads. Two things keep it that way:
- an AST guard, `TestFilterRunsAfterIdentity`, pins the call order;
- `node_identity._warn_minted_beside_orphans` WARNs if a mint ever again
  coincides with an existing node of that function dropping out of history.

## What the failure looks like in scidb.log

In order (2026-09-28, 09:41–09:57):

1. `[node_wiring] fn__pandas.read_csv__rlw90w now runs as wiring 15506ac7…
   (run_id=0dgc9md2)`: the real run.
2. `[intent_store] wrote … ('pathInput__DemographicsPath', 'hidden', 'node')`:
   the user deletes the PathInput node.
3. `[node_identity] allocated 1 node id(s): [('fn__pandas.read_csv__cdb48…',
   'a7aef19e…')]`. **This is the bug.** The same call site, hashed without its
   PathInput, looks like a never-seen wiring. It gets a new node, and `rlw90w`
   drops off the canvas.
4. The user draws `DemographicsPath2 → cdb48…` and `cdb48… → DemographicsTable2`.
   The edges are real, but they land on the phantom node.
5. Run: `wiring a7aef19e… matches none of the 1 candidate variant(s) — computed
   [{'wiring_id': '15506ac7…', …}]`. Execution computes candidates through
   `_db_path_input_params`, which reads unfiltered history, so it gets the
   real `15506ac7`. The two computations disagree, which breaks NOTE 4 (one
   owner per concept).
6. `no targets — graduated node with no matching history and no manual edges
   to infer from`: misleading, because the edges exist. A node that already
   has a recorded wiring (a "graduated" node) never infers from its edges.
   Only a never-run manual node does.

**What to grep for:** an `allocated … node id(s)` line for a function that
already has a node, right after a `hidden` statement. That means a view
decision leaked into identity.

## Two more gaps behind the first one, fixed 2026-09-28

With identity fixed, the user's rewire would still not have run as drawn:

- **Drawn PathInput edges were skipped** by `manual_input_overrides`, which
  covers variables only. Now `graph_builder.manual_path_input_overrides(fn,
  token, manual_index, manual_nodes)` → `{param: declared name}` is the owner
  of that rule for PathInputs. A drawn PathInput **replaces** the recorded one
  on its handle, because a history node binds one PathInput per handle. If
  two are drawn, the last one wins and a WARN is logged. The source is
  resolved by `edge_resolver.resolve_source_binding`. It has two consumers:
  - `variant_resolver.reconcile_manual_inputs` (the Run), which substitutes a
    `BINDING_PATHINPUT` binding and recomputes the call_id;
  - `_claim_stated_wirings` (identity), which folds the drawn PathInput into
    the stated wiring, so a script run through it doesn't fork a node.
- **Drawn output edges were ignored** by `derive_target_for_node`.
  `execution_service._apply_manual_output_types` is now the one owner, and
  both Run paths call it: `derive_fn_targets` (name-scoped) and
  `derive_target_for_node`, with only the edges drawn on that node.

**Diagnostics.** A node with a recorded wiring that matches no history now
logs the edges drawn on it, and says that they aren't consulted
(`… does not infer from its drawn edges (N drawn: …)`). It used to say
"no manual edges to infer from".

### Known limitations (open)

- **A hidden input node with nothing drawn in its place.** The run still uses
  the recorded binding, although the canvas shows the handle unconnected.
  This applies to a hidden PathInput and equally to a hidden input Variable.
  Hiding the node doesn't hide its edge ids, so `hidden_wirings` never counts
  the node as disconnected. The "visible edges are ground truth" rule says it
  should be. That's a behaviour change for every hidden input node, so it's
  left for a decision.
- **A script run through a drawn OUTPUT edge.** `_claim_stated_wirings` folds
  in drawn inputs and PathInputs but not drawn outputs, so a for_each run
  outside the GUI into the new output could still fork a node. A GUI run is
  unaffected, because it records the node at dispatch.
- **Unverified: an edge from a still-manual PathInput node**
  (`pathInput__X__abc123`, before it becomes `pathInput__X::main`).
  `resolve_source_binding` strips only the prefix and the placement suffix,
  so it would read the name as `X__abc123`. In the 2026-09-28 log the edge
  was drawn from `pathInput__DemographicsPath2::main`, so the case didn't
  arise. Check it if a drawn PathInput is ignored on a node that was just
  dragged in.
## Tests

`scistack-gui/tests/test_hidden_path_input_identity.py`: hide a PathInput →
same node id, same current wiring, still on canvas; a drawn PathInput edge
and a drawn output edge on the history node become the run's binding and
output; a script run through the drawn PathInput keeps one node. `TestHidingAnyNodeKeepsEveryOtherNodesIdentity` is the general rule,
parametrised over every node type.

## See also

- `node-identity.md` §10: current vs every wiring, `resolve_identities`
- `manual-edges-on-history-nodes.md`: the override rule and its four consumers
- `identity-layers-pathinput.md`, `project_pathinput_invocation_identity`
  memory: a PathInput's identity is its declared name
