# Plan: ColumnSelection for individual function inputs in the GUI

*Drafted 2026-09-15. Decisions locked with the user up front: settings-panel
picker + canvas chip; multi-column **and** `iterate` (for_columns) exposed;
Python **and** MATLAB in this pass; the column list comes live from the
database, as a multi-select with Select all / Deselect all.*

---

## 0. What already exists (nothing below re-implements it)

| Layer | Already there |
|---|---|
| `scifor.ColumnSelection` | container: `data`, `columns`, `iterate`, `excl_columns`, `to_key()` |
| `scidb.ColumnSelection` | subclass with `.load()`, comparison ops; built by `MyVar["c"]` / `MyVar.for_columns([...])` |
| `scifor.foreach` | per-combo extraction; `_extract_data` collapses **1 row × 1 data column → the scalar itself** — this is the "automatic unwrapping" the user described; nothing to add |
| `scidb.foreach` | `_is_loadable` recognises `ColumnSelection`, bulk-loads and re-wraps |
| `foreach_config._serialize_inputs` | puts `to_key()` in `__inputs`, so a column change forks the version-key group (re-runs rather than silently reusing) |
| MATLAB | `BaseVariable.selected_columns` / `.iterate`, `MyVar("c")`, `MyVar().for_columns([...])`, bridge spec `{"kind": "column_selection", ...}` |
| GUI | `glue_service.input_columns(variable_type)` already reads a variable's real columns from `_variables.dtype` |

**The gap is only:** no GUI surface, and no path from a GUI wiring to a
`ColumnSelection` in `build_run_inputs` / generated MATLAB / exported code.

---

## 1. Data shape and where it is stored

### Stored state — `_node_config`

```jsonc
// _node_config[node_id].columnSelections
{ "table_in": { "columns": ["filename"], "iterate": false } }
```

Same table, same upsert, same rehydration pass as `runOptions` /
`whereFilters` / `schemaSelection` (see `docs/claude/gui-run-options-flow.md`).
Chosen over an edge-keyed store for one decisive reason: **a function that has
already run has no manual edge rows** — a source-declared pipeline's edges are
re-synthesized from DB history on every graph build. A per-edge selection would
work perfectly on a fresh canvas and vanish the first time the node ran.
`_node_config` is keyed by node id in all three of its shapes and already has a
working rehydration story for the placement-qualified one.

`graph_builder._SAVED_CONFIG_KEYS` gains `"columnSelections"` (one-line change;
without it the key is written and never read back — the exact 2026-09-14
snap-back bug).

### In-flight state — the variable binding

`edge_resolver.variable_binding()` gains two optional fields:

```python
{"kind": "variable", "ref": ["Trials"], "columns": ["filename"], "iterate": False}
```

**Deliberately not part of `input_types`.** `variable_types_view` keeps
returning bare type names, so:

- `graph_builder.wiring_id` is unchanged → the node does not split when a
  column is picked;
- `variant_resolver.compute_call_id` is unchanged → the predicted call_id still
  matches the one the canvas will see.

That second point is load-bearing and non-obvious. scidb's **forward**
`to_call_id` *does* include the `ColumnSelection.to_key()` (via `__inputs`),
but the GUI never sees the forward id: provenance stores an input edge as
`(param → record → variable_type)` with no trace of the columns, so
`provenance_query.pipeline_variants` reconstructs `__inputs = {"table_in":
"Trials"}` and the canvas node id is built from *that*. Feeding columns into
the GUI's `compute_call_id` would predict an id no record ever carries and
silently break combo hiding on every column-selected node. A test pins this.

---

## 2. Stages

### Stage 1 — binding carries the selection (pure domain)

* `domain/edge_resolver.py`: `variable_binding(type_names, columns=None, iterate=False)`;
  views (`input_types`, `variable_types_view`, `loadable_bindings`) unchanged.
* New pure module `domain/column_selection.py`:
  * `normalize(raw) -> {"columns": [...], "iterate": bool} | None` — tolerates a
    bare string, a list, a legacy/partial dict, and drops an empty non-iterate
    selection (empty + `iterate=False` means "whole variable", so it must not
    survive as a `ColumnSelection` at all);
  * `apply_to_bindings(bindings, selections)` — stamps onto variable bindings
    only, WARNs when a selection names a param that is not variable-bound
    (a rename left it dangling);
  * `describe(sel)` — the one spelling used in logs and in the canvas chip.
* Tests: `scistack-gui/tests/test_column_selection_binding.py`.

### Stage 2 — derivation stamps it onto every target

* `execution_service._attach_column_selections(db, node_ids, targets)`, modelled
  directly on `_attach_db_path_inputs` (the existing precedent for "GUI state
  that DB history cannot carry").
* Called from **both** derivation paths — `derive_fn_targets` (name-scoped:
  union of the fn's node ids, bare + `::scope`, conflict WARNed) and
  `derive_target_for_node` (node-scoped: that node's own ids).
* INFO log per target: `[execution] 'fn': 'table_in' restricted to column(s)
  ['filename']` — absence of this line is the diagnostic when a selection
  appears in the panel but not in the run.
* Tests cover the never-run path *and* the has-history path (the latter is the
  one that regresses).

### Stage 3 — `build_run_inputs` builds the object

* `BINDING_VARIABLE` branch: `cls[cols]` (non-iterate) / `cls.for_columns(cols)`
  (iterate); for a multi-type (`EachOf`) binding, wrap **each** alternative —
  `EachOf` documents `ColumnSelection` as a legal alternative.
* One INFO line naming param, type(s) and the resolved selection.
* Test: the returned object is a `scidb.ColumnSelection` with the right
  `columns`/`iterate`, and `to_key()` differs from the bare class's.

### Stage 4 — the column list, live from the database

* Move the dtype→columns logic out of `glue_service.input_columns` into
  `variable_service.input_columns(variable_type)` (variables own it; glue
  delegates — `feedback_avoid_scifor_scidb_duplication`, and the behaviour for
  glue is unchanged).
* Surface it on both protocols: `GET /api/variables/{name}/columns` and JSON-RPC
  `get_variable_columns`, returning `{data_columns, schema_keys, mode, note}`.
* A scalar/array-stored variable reports its single class-named column and the
  note explaining why — the picker shows that rather than an empty list.

### Stage 5 — the UI

**`FunctionSettingsPanel` — new "Inputs" section** (above Data Filters):

```
Inputs
  table_in   Trials    [ filename ×          ▾ ]
  raw_in     RawEMG    [ (whole variable)    ▾ ]
```

* One row per variable-bound parameter (from `data.input_params`).
* The dropdown is a **multi-select checkbox list** populated from Stage 4, with
  **Select all** / **Deselect all**; nothing checked = whole variable.
* A checkbox under the row: **"Run once per column (for_columns)"** — enabled
  only when the panel can resolve columns; the tooltip states the reassembly
  semantics (one output variable, one-row table, source column names).
* Columns are fetched once per distinct variable type when the panel opens
  (mirrors the glue panel's live read; no scaffolding, no stale list).
* Saves exactly like `runOptions`: `updateNodeData({columnSelections})` +
  `update_node_config`. **The backend reads only the stored config** — unlike
  run options there is no second, live-canvas route, which is the disagreement
  `gui-run-options-flow.md` warns about.

**`FunctionNode` — read-only chip** beside the input handle:
`⟨"filename"⟩`, `⟨3 columns⟩`, or `⟨per column⟩` for iterate. Display only;
editing stays in the panel.

### Stage 6 — MATLAB generation

* `variable_inputs[param]` accepts **either** today's `str | list[str]` **or**
  `{"types": [...], "columns": [...], "iterate": bool}`; one parser
  (`_variable_binding_parts`) feeds both `_variable_input_items` and
  `_variable_input_type_names`, so the map stays single (no parallel
  `column_selections` dict — that is the "one concept, two representations"
  trap this subsystem keeps falling into).
* `_format_variable_input` emits `Trials("filename")`,
  `Trials(["a","b"])`, `Trials().for_columns(["a","b"])`,
  `Trials().for_columns()`, and `scifor.EachOf(A("c"), B("c"))`.
* Feed the selection in at `matlab_command_service._collect_variable_inputs`
  (edge path) **and** the DB-variant path, so all **three** emit sites
  (template/first-run, single-fn `_for_each_call_lines`, per-step pipeline) are
  covered through the one helper.
* Test asserts the rendered text for each of the four shapes.

### Stage 7 — code export (`scidb report` / export to plain Python)

* `_py_literal`: `Trials["filename"]` / `Trials[["a","b"]]` /
  `Trials.for_columns([...])`.
* `_matlab_literal`: the Stage 6 spellings.
* Without this the exported script renders `<ColumnSelection object at 0x…>` —
  a file that looks fine until it is run.

### Stage 8 — tests and docs

* New: `tests/test_column_selection_binding.py`,
  `tests/test_column_selection_run_inputs.py`; additions to the MATLAB-command
  and code-export test modules.
* Regression pins:
  * `wiring_id` and `compute_call_id` unchanged by a column selection;
  * a config saved under a placement-qualified id is rehydrated
    (`_SAVED_CONFIG_KEYS` drift test);
  * a selection naming a param that is no longer variable-bound WARNs and does
    not raise mid-run.
* Docs: extend `docs/claude/column-selection.md` with a "From the GUI" section
  (storage, binding field, the call_id asymmetry) and cross-link from
  `docs/claude/function-input-resolution.md` §4 (the target dict gains a field)
  and `docs/claude/gui-run-options-flow.md` (a second consumer of node config,
  single-route on purpose).

---

## 3. Known limitations, stated rather than discovered later

1. **`excl_columns` is not exposed.** Reachable from Python only. Noted in the
   panel's tooltip text? No — left silent; it is a scifor-level escape hatch.
2. **One selection per parameter, not per edge.** A parameter fed by two
   variable types (EachOf) applies the same column set to both. A column
   present in one type and absent in the other fails at load with scifor's
   existing `KeyError` naming the available columns.
3. **History cannot tell you a run used a selection.** Provenance records the
   variable type, not the columns; the GUI's own config is the only record.
   Making that durable means recording the selection in `_invocation_input`,
   which is a scidb-layer change and out of scope here.

---

## Implementation status — 2026-09-15

All 8 stages built; **scistack-gui tests pass** (user-run 2026-09-15).
Frontend typechecks clean and BOTH vite bundles were rebuilt. Uncommitted.

| Stage | Landed in |
|---|---|
| 1 | `domain/column_selection.py` (new); `edge_resolver.variable_binding(columns=, iterate=)` |
| 2 | `execution_service.column_selections_for_nodes` / `_attach_column_selections`; `graph_builder._SAVED_CONFIG_KEYS` |
| 3 | `execution_service._apply_column_selection` + the `BINDING_VARIABLE` branch of `build_run_inputs` |
| 4 | `variable_service.input_columns` (moved from `glue_service`, which now delegates); `GET /api/variables/{name}/columns`; RPC `get_variable_columns`; `frontend/src/api.ts` route |
| 5 | `FunctionSettingsPanel` Inputs section + `ColumnSelectRow`; `FunctionNode` chip + handle tooltip; `Sidebar` props |
| 6 | `api/matlab_command._variable_binding_parts` / `_format_variable_class`; `matlab_command_service._apply_column_selections` / `_db_input_types`, wired into the single-fn and per-step paths |
| 7 | `code_export_service._column_selection_parts`, `_py_literal`, `_matlab_literal` (+ an explicit `EachOf` branch in `_py_literal`) |
| 8 | `tests/test_column_selection_binding.py`, `tests/test_column_selection_run_inputs.py`, additions to `test_matlab.py` / `test_code_export.py`; docs in `column-selection.md`, `function-input-resolution.md`, `gui-run-options-flow.md` |

### Decisions taken during implementation (not in the plan above)

* **Name- vs node-scoped config lookup.** `column_selections_for_nodes` takes
  an optional `function_name`. `derive_fn_targets` passes it (any
  `fn__{name}__*` config id counts — required for a source-declared pipeline
  whose canvas id is in no edge row); `derive_target_for_node` and the MATLAB
  per-step path do not, so two call sites of one name stay independent.
* **One INFO line per parameter per derivation**, not per target. Its absence
  is the diagnostic; N identical copies make that harder to see, not easier.
* **The node chip is a labelled row under the function name**, not an element
  absolutely positioned beside the handle — the node container is not a
  positioning context and making it one would move the existing badges. The
  same text is on the input handle's own `title`, so the information is at
  the handle too.
* **A parameter with no variable type** (unwired, or PathInput-/Parameter-fed,
  which `aggregate_variants` partitions out of `input_params` and the fill-in
  pass restores as `""`) renders an inert `n/a` row rather than spinning on a
  fetch it cannot make.
