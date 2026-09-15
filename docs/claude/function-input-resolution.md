# How a Function Node's Inputs Get Built

## Overview

When you press Run on a function node, something has to turn "this box on a
canvas, with lines drawn into it" into the `inputs={...}` dict handed to
`scidb.for_each`. That path crosses four modules and two entirely different
sources of truth, and it is not obvious from any one of them. This is the
map.

**The governing rule, since 2026-08-25: inputs are built from EDGES.** Not
from a name coincidence between a declaration and a function parameter, not
from argument position. If no edge says a parameter is fed by something,
that parameter is left unbound and the function's own default applies. See
`.claude/plan-edge-based-inputs-26-08-25.md` for why the previous
name-matching design was removed rather than kept as a fallback.

The pipeline, end to end:

```
canvas edges ─┐
              ├─→ resolve_function_edges ─→ ResolvedEdges ─┐
node metadata ┘                                            ├─→ target dict ─→ build_run_inputs ─→ for_each(inputs=)
                                                           │
scidb history ─→ get_aggregated_variants ─→ _attach_db_path_inputs
```

---

## 1. The two sources, and why both are needed

A function node's wiring can come from either of two places, and the split
is invisible on screen — both render as ordinary lines.

| | Never-run wiring | Already-run wiring |
|---|---|---|
| Where the edge lives | `_pipeline_edges` (a real row, drawn by the user) | Nowhere — **synthesized** on every graph build from DB history |
| Who produces it | `layout_service.put_edge` | `graph_builder.build_edges` |
| Who reads it for execution | `resolve_function_edges` | `_attach_db_path_inputs` |

This is the single most important thing to know about this subsystem. A
source-declared pipeline that has already run **has no manual edge rows at
all**; its edges are re-derived from `db.get_aggregated_variants()` each
time the canvas is drawn. Any change to input resolution that only reads
manual edges will appear to work perfectly on a fresh canvas and silently
break every project with history.

---

## 2. `resolve_function_edges` — the never-run path

`scistack_gui/domain/edge_resolver.py`. Pure; no I/O.

Scans every edge, keeps the ones touching this function's node ids, and
classifies each **incoming** edge by its SOURCE node's id prefix:

| Source prefix | Goes to | Keyed by | Value |
|---|---|---|---|
| `var__` / manual `variableNode` | `input_types` | param name | `[variable class names]` |
| `pathInput__` (`PATH_INPUT_ID_PREFIX`) | `path_input_params` | param name | declared PathInput name |
| `param__` (`PARAM_ID_PREFIX`) / manual `parameterNode` | `parameter_params` | param name | declared Parameter name |

Outgoing edges (function → variable) become `output_types`, and are the one
case that needs no handle: the target variable node *is* the binding.

### The param name and the declared name are different things

`path_input_params` and `parameter_params` are `{param_name:
declared_name}` — two names, deliberately kept apart:

- the **parameter name** comes from the edge's `targetHandle`
  (`in__{param}`) and is what `for_each` is ultimately called with;
- the **declared name** comes from the source node's id and is what
  `registry.get_path_inputs_registry()` / `get_parameters_registry()` and
  the hidden-value store are keyed by.

A PathInput declared `test_pi` can perfectly well feed `read_csv`'s
`filepath_or_buffer`. `graph_builder.build_edges` has encoded both names in
its PathInput edge ids all along (`e__{pi}__{param}__{fn}__{wid}`) for
exactly this reason. Collapsing them is what made the original bug
unfixable.

### Handles are mandatory on input edges

An incoming edge whose `targetHandle` doesn't name a parameter is **dropped
with a WARNING** naming the edge id, source, target and handle. Two guesses
used to live here, and both produced silently-wrong wiring:

- **positional matching** — a handle-less edge was assigned to the first
  leftover signature parameter, so on a function with many parameters it
  bound whichever one happened to be free;
- **the label fallback** — a Parameter edge with an unrecognized handle used
  the source node's *label* as the parameter name, correct only when the two
  names coincide.

Dropping visibly is better than binding wrongly: a dropped edge produces a
log line and an unbound parameter, both of which are diagnosable.

---

## 3. `_attach_db_path_inputs` — the already-run path

`scistack_gui/services/execution_service.py`.

A PathInput is never a citizen of `input_types` or `constants` — it resolves
*files*, not a versioned DB record — so a target derived from history
carries no trace of one. The mapping does survive, though, in
`get_aggregated_variants()["path_inputs"]`:

```python
{param_name: {"template": str, "root_folder": str|None,
              "functions": [(fn_name, call_id), ...]}}
```

Keyed by **param name**, carrying the recorded spec.
`graph_builder.convert_scidb_path_inputs` resolves each spec back to its
source-declared name — via the registry, falling back to the D7
`_pipeline_path_input_history` table for a template that has since been
edited. `_db_path_input_params` inverts that into `{call_id: {param_name:
declared_name}}`, and `_attach_db_path_inputs` stamps each history target
with its own call site's bindings.

`parameter_params` is left empty for a history target on purpose: its
`constants` already hold concrete recorded values, so there is nothing to
look up in the Parameter registry.

---

## 4. The target dict — the contract between the two halves

Both derivation functions (`derive_fn_targets`, name-scoped;
`derive_target_for_node`, node-scoped) return a list of targets with the
same shape regardless of which source produced them:

```python
{
    "input_types":       {param: [var class names]},   # or a bare str from history
    "output_type":       "VariableClassName",
    "constants":         {param: concrete value},
    "path_input_params": {param: declared name},
    "parameter_params":  {param: declared name},
}
```

Uniformity is the point — `build_run_inputs` must not need to know which
branch it came from.

Two things that consume this dict and are easy to forget when widening it:
`variant_resolver.deduplicate_variants` and
`filter_hidden_constant_value_targets`. `wiring_id` is computed from
`input_types`/`output_type` only, so the binding keys don't perturb node
identity.

A **variable** binding may additionally carry `"columns"` and `"iterate"` —
the GUI's column selection (`MyVar["col"]` / `MyVar.for_columns([...])`),
stamped on by `execution_service._attach_column_selections` from the node's
saved config, since DB history carries no trace of it. The two keys are
absent entirely when there is no selection, and they are deliberately NOT
part of `wiring_id` or `compute_call_id`. See
`docs/claude/column-selection.md` §From the GUI for why that asymmetry with
scidb's forward `to_call_id` is correct rather than an oversight.

---

## 5. `build_run_inputs` — the single construction point

This is the **only** place in scistack-gui where a live `scifor.PathInput`
or a fanned-out `Parameter` is ever constructed for execution. Both the
per-node Run path (`api/run.py`) and the compiled-pipeline path
(`build_backend_pipeline`) call it, after a long period in which they
carried two independently-drifting copies.

Order of operations:

1. `input_types` → variable classes (a multi-candidate list becomes
   `EachOf(...)`);
2. `constants` → merged in as concrete scalars;
3. `path_input_params` → registry lookup **by declared name**, bound under
   the param name;
4. `parameter_params` → same, then filtered through `_apply_hidden_values`.

A declared name missing from the registry logs a WARNING and leaves the
parameter unbound — the declaration can legitimately have been deleted from
source after the edge was drawn. It must never raise mid-run.

### Hidden values use the declared name — in BOTH filters

The per-value checkbox on a Parameter node writes the **declared** name into
the hidden-value store, while a target's `constants` are keyed by the
**parameter** name. Two separate places have to translate between them, and
both got this wrong originally:

- `filter_hidden_constant_value_targets` (`domain/variant_resolver.py`) —
  drops whole targets whose constants carry a hidden value. It translates via
  the target's own `parameter_params`, defaulting to the param name.
- `_apply_hidden_values` (`execution_service.py`) — trims values off a
  Parameter that is being handed to `for_each` whole. It is called with the
  declared name.

Both are needed because a Parameter reaches execution by **two different
routes**, and which one applies is easy to get wrong when reading the code:

| | Fanned out at derivation | Handed over whole |
|---|---|---|
| When | The wiring materializes it — `_infer_wired_constants` expands every declared value into its own target | The param is bound in `build_run_inputs` from `parameter_params` and never entered `constants` |
| Shape | N targets, each with a **scalar** under the param name | 1 target, one `Parameter` object fanned out **inside scidb** |
| Filtered by | `filter_hidden_constant_value_targets` | `_apply_hidden_values` |

Same runs either way. But in the first route the GUI's hidden state is
checked against target dicts; in the second it is invisible to the GUI
entirely once `for_each` has the object, which is why the trimming must
happen before handover. Unchecking *every* value raises rather than running
the full set — both silent alternatives produce exactly the records the user
asked not to produce.

Both filters share `variant_resolver.is_hidden_value` for the **value**
comparison, which matches a number against both its int and float spelling.
`ParameterCreate.values` is `list[float | int | str | bool]` and preserves
the distinction deliberately, so `5` and `5.0` both occur in practice; when
the two filters had their own comparisons, the checkbox worked on one route
and silently did nothing on the other depending on how the value was
written.

**The recurring trap in this subsystem:** one concept represented two ways —
a Parameter's *name* (declared vs. parameter) and a Parameter's *value*
(int vs. float spelling) — with the translation present at one of the two
places that need it and missing at the other. Both known bugs of this shape
are now fixed and tested; expect a third if another execution route is
added.

---

## 6. Handle ids are a cross-language contract

The handle ids `FunctionNode.tsx` renders and the ones `edge_resolver`
matches on must agree, or a hand-drawn edge binds nothing:

| Handle | Rendered by | Matched by | Written by |
|---|---|---|---|
| `in__{param}` | `FunctionNode.tsx` | `edge_resolver` | `build_edges` (PathInput edges) |
| `param__{name}` | `FunctionNode.tsx` | `edge_resolver` | `build_edges` (Parameter edges) |
| `out__{param}` | `FunctionNode.tsx` | `infer_manual_fn_param_to_class` | — |

**This has drifted once already.** `FunctionNode.tsx` rendered
`const__{name}` while `PARAM_ID_PREFIX` was `param__{name}` — a leftover
from the D6 Constant+Sweep→Parameter merge. It went unnoticed for as long as
it did precisely *because* of the label fallback described in §2, which
absorbed the unrecognized prefix by guessing from the node label.

`tests/test_edge_resolver.py::TestHandleIdsMatchTheFrontend` reads the .tsx
source and asserts it uses `PARAM_ID_PREFIX`, because this class of drift is
unobservable from either side alone: the frontend compiles, the backend's
tests pass, and only a real GUI session shows the wiring doing nothing.

---

## 7. Diagnosing "I pressed Run and nothing happened"

The failure mode this subsystem produces is a run that **succeeds while
doing no work** — `for_each` iterates zero times, writes no records, and
every node stays red. Read the log in this order:

1. **`[run_thread] ... completed without running a single combination`** —
   the WARNING that says this happened at all.
2. **`[execution] '<fn>': built inputs for N param(s): ...`** — what was
   actually bound. `(none)` here is the whole story.
3. **`[execution] '<fn>': N signature param(s) left unbound by the wiring`**
   — INFO, not an error (optional params legitimately go unbound), but the
   place to look for the parameter you expected to see.
4. **`[edge_resolver] ignoring edge ...`** — an edge was drawn but carries
   no usable handle.
5. **`no values found for '<schema_key>' in database, 0 iterations`** — with
   an empty `inputs`, `for_each` falls back to schema-key iteration; on a
   fresh database that is zero combinations. This line is a *symptom* of an
   empty binding, not an independent problem.

If the binding looks right and the run still does nothing, the next suspect
is PathInput discovery: a `root_folder` that doesn't exist, or a template
whose keys aren't the database's schema keys, both yield zero files with no
error.

---

## Related

- `docs/claude/code-discovery-categories.md` — how PathInputs/Parameters get
  into the registry in the first place, and the declared-name identity they
  carry.
- `docs/claude/entity-editability-model.md` — D6 (Constant + Sweep →
  Parameter), the source of the `param__` prefix.
- `docs/claude/each-of-variant-expansion.md` — what happens to a
  multi-valued Parameter *after* `build_run_inputs` hands it over.
- `docs/claude/bipartite-provenance.md` — why a PathInput's identity is
  content-addressed from its spec rather than its name, which is what makes
  `convert_scidb_path_inputs`' spec→name resolution necessary.
- `.claude/plan-edge-based-inputs-26-08-25.md` — the change that established
  the edges-only rule, with the full inventory of what was removed.
