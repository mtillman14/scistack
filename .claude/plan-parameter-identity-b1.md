# Plan: Parameter identity survives a run (B1) + all-columns compare (B2)

STATUS 2026-09-23: Option A + B2 BUILT (user chose A, same batch). Tests written, unrun. See docs/claude/cleanup-audit.md §3.

Diagnosis: docs/claude/cleanup-audit.md §3.

## B1 — duplicate Parameter node after a run

Root: history records a constant under the function's ARGUMENT name; the canvas
keys a Parameter by its DECLARED name. Forward path has the mapping
(`ResolvedWiring.parameter_params`); nothing records it, so the backward path
(`graph_builder.build_parameter_nodes` / `build_edges`) invents
`param__{argument}`.

### Option A (recommended) — scidb records the declared name at write time
1. Logging first (NOTE 2): `build_edges` logs each history constant whose name
   is not a declared Parameter (fn, call_id, argument name, declared names bound
   to that handle), and the graph-built line lists Parameter node ids at DEBUG.
2. scidb: a Parameter binding carries its declared name into `for_each`
   (entities loader and discovery set `Parameter.name`; the GUI/MATLAB generated
   call passes the declared Parameter, not a bare value). Provenance records it
   on the constant consumption edge (additive column; old rows = NULL).
3. Backward path: `get_aggregated_variants` returns `{argument: declared}` per
   call site; graph_builder builds `param__{declared}` and the edge into
   `param_handle(argument)` (the PathInput edge shape). NULL (legacy/script
   runs with a bare value) falls back to the argument name.
4. MATLAB: generated command passes the declared name (option or wrapper);
   `+scidb/for_each.m` forwards it through the bridge.
5. Tests: scidb round-trip (declared name survives save → aggregated variants);
   GUI graph build with declared≠argument yields ONE node + edge; legacy NULL
   row falls back; MATLAB parity test.

### Option B — GUI-only mapping from the node's stated wiring
Map a history constant (fn, call_id, argument) to the declared Parameter the
fn node's current wiring binds to that handle. Smaller, but reconstructs on
read (the pattern §2.5 says fails), and does nothing for script runs.

## B2 — false "unused intent" for all-columns for_columns
`same_columns`: a stated `{columns: [], iterate: true}` matches any recorded
`iterate: true` selection (the stated form is symbolic, resolved at for_each).
Owner stays `scidb.intent`; test in scidb for the symbolic-vs-resolved pair.
