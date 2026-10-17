# How a function node's colour is decided

Written 2026-10-17 after a scidb.log triage: `pandas.read_csv`, rewired from
a hidden `RatingsData` to a new, empty `Rating`, stayed green. Read with
`manual-edges-on-history-nodes.md` (the rule "visible edges are the ground
truth") and `node-identity.md` (tokens, `token_for`).

## Vocabulary

| State | Meaning | Who says so |
|---|---|---|
| green | the node has done its work for every expected combination | scidb `check_multiple_nodes_state` (pass 1), then the cascade |
| red | something is missing, stale, disconnected, or upstream is red | same, plus the GUI overrides below |
| pending | an unrun staged constant value (a synthesized variant row) | display only: `graph_builder.group_call_sites_by_wiring` |

scidb's own state is binary (green/red). "pending" never downgrades a real
call site.

## Two kinds of function node, two code paths

**Manual (never-run) node** — exists because the user dragged it in; it has
no call sites in history. Colour is decided in `api/pipeline.py` (the
`manual fn node ... state=` INFO line):

- no output edge wired → red;
- otherwise `_own_state_for_function(db, fn, outputs)` — "has this FUNCTION
  NAME ever produced these outputs" — unless every recorded call site's
  wiring conflicts with the drawn one, which forces red.

**History (DB-derived) node** — exists because provenance records a call
site. Colour comes from `api/pipeline._compute_run_states`, in two passes,
run BEFORE hidden nodes are filtered out:

1. **Own state per call site** `(fn, call_id)`: scidb
   `check_multiple_nodes_state` over the call site's **recorded** outputs.
   Sub-sites split by Run claims are asked once, over their union.
2. **GUI overrides** on that own state, each forcing red:
   - `disconnected_fkeys` — a required INBOUND edge is hidden
     (`graph_builder.hidden_wirings` / `wiring_disconnected_fkeys`);
   - `unproduced_outputs` — a drawn OUTBOUND edge goes to a variable no
     call site of the node has recorded
     (`graph_builder.unproduced_output_fkeys`, added 2026-10-17).
3. **Cascade** (`domain/run_state.propagate_run_states`): a variable takes
   its worst producer's state; a function takes the worst of its own state
   and its inputs'. Inputs come from
   `graph_builder.input_params_with_manual_edges`: recorded inputs with drawn
   edges folded in, so red travels along edges the user drew.
   `hidden_fkeys` (hidden function nodes) take no part.

Then `group_call_sites_by_wiring` merges call sites into canvas nodes: a
node's own state is its WORST member (including synthesized pending rows),
and the cascade runs again on the grouped wiring with the same manual-edge
overlay.

## The symmetry, and why each half exists

| Edge the user changed | Without the rule | Owner |
|---|---|---|
| drew an input edge | red stopped at the last history edge (2026-09-22) | `input_params_with_manual_edges` |
| hid a required input edge | node stayed green with no input | `wiring_disconnected_fkeys` |
| drew an output edge to a never-written variable | node kept the colour of the output it used to write (2026-10-17) | `unproduced_output_fkeys` |
| hid a function node | its producer state still coloured its outputs | `hidden_fn_fkeys` |

`unproduced_output_fkeys` works per NODE: the drawn output counts as produced
when ANY call site grouped into the node recorded it, because a run of the
node writes every drawn output. After the run, the new output is recorded
(split by Run claims) and the node turns green again.

## None of this touches identity

Every overlay above is a COPY passed to run state only. `wiring_id` hashes the
recorded `input_params`/outputs; folding a drawn edge into them would rename
the node (the placement-id lookup trap).

## Known gaps (by decision)

- **Input-file content changes do not colour anything.** Lineage records the
  file path, not its bytes; content staleness is deferred (memory
  `feedback_defer_content_staleness`). Renaming a CSV column leaves the loader
  green.
- **A failed run does not colour the node.** Colour reads what the database
  holds, and a failed `OutputSaveError` run leaves the previous records in
  place, so the node stays green. The failure is reported by the run panel
  and in scidb.log (`Thread finished (success=False ...)`).

## Diagnosing "why is this node this colour" in scidb.log

- `manual fn node <id> (<fn>): state=...` — manual nodes, with the reason.
- `[timing] check_multiple_nodes_state: ...` — pass 1 ran, over how many combos.
- `state(<fn> call_id=...): green -> red — drawn output(s) [...] never recorded` and
  `[graph_builder] <node>: drawn output(s) [...] never recorded` — the
  output override fired.
- `[pipeline] N wiring(s) disconnected: ...` — the input override fired.
- `run-state propagation follows N manual edge binding(s)` — the drawn-input overlay.
- `propagate_run_states complete: N total nodes (g green, p pending, r red)` — the cascade.
