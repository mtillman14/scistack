# Plan: a hand-placed node merges into its DB twin even when the twin is already placed

*2026-10-01. Status: BUILT (user chose: the hand-placed position wins); pytest not yet run.*

## The bug (confirmed with the user's database)

Before running `grSides`, the user hand-placed `var__GaitRiteLoaded_UA__k7huza` and
wired `grSides → it → calculateSymmetryOneVector`. The run produced the DB-derived
node `var__GaitRiteLoaded_UA`, and both nodes stayed on the canvas.

- `_pipeline_nodes` holds one manual row: `var__GaitRiteLoaded_UA__k7huza`.
- `layout.json` already held `positions/main/var__GaitRiteLoaded_UA::main` (x 1520,
  y 147). That is a leftover from the 09-30 hand-placed node
  `var__GaitRiteLoaded_UA__5zy1v9` (placed at x 1501, y 134), which is gone from
  `_pipeline_nodes`.
- `graph_builder.merge_manual_nodes` graduates a manual node only when exactly one DB
  node shares its (type, label) AND that node's placement has no saved position. The
  second condition failed, so the build logged "1 to add, 0 to graduate", and both
  nodes were drawn.

The rule is deliberate: `test_graduation_skipped_if_own_placement_already_exists`.
But its effect is a permanent duplicate. The manual node can never graduate as long as
the twin's position exists, and a stale position from an earlier placement is enough
to block it.

General logic, triggered by this database's layout history: any project where a
variable's canonical node has ever been positioned in a scope hits it the next time a
twin is hand-placed in that scope.

## Fix

1. `merge_manual_nodes`: when exactly one DB node shares the label and the target
   placement already has a saved position, graduate anyway. A variable node (and a
   PathInput or Parameter node) names one entity, so two nodes for it on one canvas is
   never meaningful. Function nodes keep their stricter wiring-matched rule
   (`api/pipeline` refinement), unchanged.
2. `layout.graduate_manual_node` already handles a target that has a position. It
   keeps or replaces it per the Question below, moves the edges
   (`pipeline_store.rename_edge_endpoints`), and drops the manual row. No new
   mechanism.
3. Different scopes stay independent: placement is per scope, unchanged
   (`test_same_label_different_scopes_graduate_independently`).
4. `test_graduation_skipped_if_own_placement_already_exists` is rewritten to the new
   rule (graduates; position per the Question), plus a regression test for the
   DemographicsTable/GaitRiteLoaded_UA shape (a twin with a stale position).
5. Logging: the INFO line that already names the reason becomes
   "graduating onto an already-placed node (position kept from …)".

## Question for the user

When both nodes have a position, which one survives?
- **The hand-placed node's** (recommended): it is the one the user just placed and
  wired; the twin's position is usually stale.
- **The twin's**: the existing `graduate_manual_node` behaviour (it never overwrites).

## For the current database (either way)

Once the fix is in, opening the canvas graduates `…__k7huza` into
`var__GaitRiteLoaded_UA` automatically, and its edge to `calculateSymmetryOneVector`
moves with it. Nothing has to be deleted by hand.
