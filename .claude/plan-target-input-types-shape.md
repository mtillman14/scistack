# Plan: one shape for a target's `input_types` (grSides "unhashable type: 'list'")

## Symptom

Clicking Run on the never-run MATLAB node `grSides` (wired, with a `side`
column selection) fails in `generate_matlab_command`:

```
api/matlab_command.py:1059 _collect_var_types
    all_var_types.add(type_val)
TypeError: unhashable type: 'list'
```

`scidb.log` 2026-09-23 11:59:48 and 12:00:08, both preceded by
`scoped to node fn__grSides__8bofmr — 1 of 0 variant row(s) belong to this
node's wiring`.

## Root cause

The system has **one** stated rule for how a variable input's type is
spelled, owned by `domain/edge_resolver.variable_types_view(bindings)`:

> a bare string when one type is bound, a list only for a genuine multi-type
> (EachOf) input. That shape is load-bearing … `"RawEMG"` and `["RawEMG"]`
> produce DIFFERENT wiring ids.

But `ResolvedFunctionEdges.input_types` (the property on the same module) is a
**second** view with a different shape: always a list, even for one type.
Whoever reads the property instead of `variable_types_view` gets the other
shape.

`execution_service._inferred_targets` (the never-run fallback, shared by
`derive_fn_targets` and `derive_target_for_node`) stores
`"input_types": resolved.input_types`, so **never-run targets are list-shaped
while history targets and reconciled targets** (`variant_resolver.py:630,680`,
which use `variable_types_view`) **are flat**.

Consumers then compensate on their own, each differently:

| Site | Compensation |
|---|---|
| `matlab_command_service._normalize_input_types` (multi-step MATLAB route) | 1-item list → item; >1 → skip target + warning |
| `matlab_command_service.scope_variants_to_node` (single-node MATLAB route) | **none — this is the crash** |
| `execution_service.py:791` (manual-node wiring id) | `ts[0]` — first candidate only |
| `api/pipeline.py:1174, 1391` (graduation / manual-node display) | `ts[0]` |
| `api/pipeline.py:1379` (glue node display) | `ts[0]` |
| `execution_service.build_run_inputs` / `_call_site_for_target` | read `bindings` directly, handle both |

The crash became reachable when the single-node route was scoped to the
node's targets (`scope_variants_to_node`, fix for grSides running twice):
a never-run *wired* node now has one inferred target, so the generator skips
its first-run template branch and reads that target as if it were history.

### Latent second bug found on the way

`execution_service.py:791` hashes a manual node's wiring with `ts[0]`, but
`record_dispatch_wirings` hashes the same node with `variable_types_view`.
For a multi-type (EachOf) manual node those differ: `"A"` vs `["A", "B"]`, so
the wiring recorded at dispatch never matches the one derivation looks up.
Harmless for single-type nodes (`ts[0]` == the flattened value) — which is
every node today, hence unnoticed.

## Decision (NOTE 4: one owner)

`variable_types_view` is the owner of the shape. Every target's `input_types`
and every wiring-id input is produced by it. The always-list view stops being
called `input_types` so nobody can read the wrong one by name.

## Stages

### Stage 1 — one view on `ResolvedFunctionEdges` (scistack-gui/domain)

- `ResolvedFunctionEdges.input_types` returns `variable_types_view(self.bindings)`.
- Add `ResolvedFunctionEdges.input_type_candidates` for the explicit
  always-list view (`bindings_of_kind(..., BINDING_VARIABLE)`), for the one
  consumer that genuinely wants lists: `matlab_command_service._collect_variable_inputs`
  (its callers render `{param: [types]}`). Clean break, no alias
  (beta-no-deprecation).
- Update every consumer (list from `findReferences` on the property, confirmed
  set above):
  - `_inferred_targets` — unchanged code, now flat. **Fixes the crash.**
  - `execution_service.py:791`, `api/pipeline.py:1174, 1391` — drop the
    `ts[0]` comprehension, use `resolved.input_types` directly. For
    wiring ids this fixes the latent multi-type mismatch. For display
    (`1391`), a multi-type param would now show a list instead of its first
    type: check the frontend consumer of `resolved_input_params` handles a
    list; if not, keep display-only first-of-list there with a comment
    saying it is display, not identity.
  - `api/pipeline.py:1379` (glue) — same treatment as 1391.
  - `_collect_variable_inputs` — switch to `input_type_candidates`.

### Stage 1b — the recipe owns the equivalence (scidb)

Stage 1 makes every caller spell a single type bare; this makes it not
matter if one forgets. `scidb.provenance.compute_wiring_id` collapses a
one-item list/tuple/set to its item before hashing (multi-item lists stay
sorted lists, as today), so `"X"` and `["X"]` hash identically by
construction (NOTE 3: the recipe lives in scidb).

No stored id changes: every id recorded today (history rows,
`record_dispatch_wirings`, the `ts[0]` derivations) was computed from the
bare form. Tests in `scidb/tests`:
- `compute_wiring_id(fn, {"x": ["A"]}, ...) == compute_wiring_id(fn, {"x": "A"}, ...)`.
- A pinned literal id for a fixed single-type input, so a byte change to the
  recipe is caught.
- Multi-type still order-insensitive and distinct from either single type.

### Stage 2 — MATLAB route: one runnability filter (scistack-gui/services)

With producers flat, `_normalize_input_types`'s 1-item case disappears; what
is left is "MATLAB cannot express a multi-type input". Make that one helper,
used by both routes:

- `_matlab_runnable_targets(targets, fn_label) -> (runnable, warnings)`:
  drops targets whose `input_types` contain a list, one warning naming the
  params. Replaces `_normalize_input_types` in the multi-step loop.
- `scope_variants_to_node` calls it. If a node's only targets are
  multi-type, return an error to the GUI ("'grSides': input 'x' is wired to
  2 types — MATLAB runs need one") instead of silently falling back to
  name-scoped history (which would run a different node's wiring).

### Stage 3 — logging (NOTE 2)

- `scope_variants_to_node`: log the source of the rows — `history` vs
  `inferred from edges (never run)` — and replace the misleading
  `1 of 0 variant row(s)` with `N target(s), M history row(s)`.
- `_collect_var_types`: if a value is still not a `str`, raise a `TypeError`
  naming the function, param and value instead of the bare `unhashable type`
  (a guard for the invariant; should be unreachable after Stage 1).

### Stage 4 — tests

In `scistack-gui/tests/`:

1. `derive_target_for_node` on a never-run manual node with one incoming
   edge → `input_types == {"grTableIn": "GAITRiteLoaded"}` (flat).
2. Same node with two edges into one param → `input_types` value is a list
   of both, and the wiring id computed during derivation equals the one
   `record_dispatch_wirings` records (latent-bug regression).
3. `generate_matlab_command` end-to-end for a never-run wired node with a
   column selection on a second input (the grSides shape) → no exception,
   script contains `scidb.register_variable(GAITRiteLoaded());` and
   `Demographics("PareticSide")`.
4. Single-node route, multi-type input → clear error message, no crash, no
   fallback to name-scoped history.
5. Update the existing `_normalize_input_types` tests (`test_matlab.py:2145-2180`)
   to target `_matlab_runnable_targets`.

Commands (user runs):
```
cd scistack-gui && python -m pytest tests/test_matlab.py -q
cd scistack-gui && python -m pytest tests -q -k "target or wiring or execution"
```

### Stage 5 — docs

- `docs/gui-manual-testing-todo.md`: "Run a never-run MATLAB node wired with a
  column selection from its own Run button."
- Optional `docs/claude/` note: target shapes (history vs inferred) and the
  `variable_types_view` rule.

## Out of scope

- Teaching the MATLAB generator EachOf-style multi-type inputs.

## Status — 2026-09-23

All stages implemented, uncommitted, pytest unrun (user runs tests).
Deviation from Stage 2: `record_dispatch_wirings` on the single-node route
moved AFTER `scope_variants_to_node` and records only the runnable targets,
so a refused multi-type run doesn't claim a wiring. Display sites
`api/pipeline.py` glue/manual-fn branches read `input_type_candidates` with
first-of-list (frontend `input_params` is `Record<string, string>`).
