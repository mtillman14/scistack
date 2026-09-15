# Column Selection for BaseVariable

## Purpose

When a `BaseVariable` stores a wide table (e.g. 50+ columns loaded from
Excel), loading the entire table to extract one column is wasteful. Column
selection lets users request specific columns at the point where the
variable is used as a `for_each()` input, without changing how data is
stored or loaded from the database.

## Python Syntax

```python
# Single column → function receives numpy array
for_each(fn, inputs={"x": MyVar["col_a"]}, outputs=[Result], subject=[1, 2, 3])

# Multiple columns → function receives DataFrame subset
for_each(fn, inputs={"x": MyVar[["col_a", "col_b"]]}, outputs=[Result], subject=[1])

# Works inside Fixed too
for_each(fn, inputs={"x": Fixed(MyVar["col_a"], session="BL")}, outputs=[Result], subject=[1])
```

`MyVar["col"]` uses `BaseVariable.__class_getitem__` (`scidb/src/scidb/variable.py`)
to construct a `ColumnSelection`.

**As of the scifor/scidb modifier-class unification**
(`docs/claude/scifor-scidb-modifier-unification.md`), `ColumnSelection`'s
container (`.data`, `.columns`, `.iterate`, `.excl_columns`, `to_key()`,
`__hash__`, `__name__`) is defined once in
`scifor/src/scifor/column_selection.py` and wraps either a DataFrame
(standalone scifor use) or a variable type (scidb use) — the same class
either way. `scidb/src/scidb/column_selection.py` is a thin **subclass**
adding only the DB-only surface with no scifor equivalent: comparison
operators (`MyVar["col"] == value` → `scidb.filters.ColumnFilter`),
`.load()`, `.to_csv()`. `MyVar["col"]` always constructs scidb's subclass;
scidb's internal isinstance checks use the scifor **base** class so a bare
`scifor.ColumnSelection(some_dataframe, [...])` passed directly into
`scidb.for_each()` is recognized too (new capability — previously
impossible to even construct, since scidb's old `ColumnSelection` only ever
wrapped a variable type).

## MATLAB Syntax

```matlab
% Single column → function receives array
scidb.for_each(@fn, struct('x', MyVar("col_a")), {Result()}, subject=[1 2 3]);

% Multiple columns → function receives subtable
scidb.for_each(@fn, struct('x', MyVar(["col_a", "col_b"])), {Result()}, subject=[1]);
```

Unlike Python, MATLAB has no separate `ColumnSelection` wrapper class — the
column names are passed to the `BaseVariable` constructor and stored
directly on the instance in the `selected_columns` property
(`+scidb/BaseVariable.m`), alongside an `iterate` flag for `for_columns()`.

## Return Behavior

| Selection | Python return type | MATLAB return type |
|-----------|-------------------|-------------------|
| Single column | `numpy.ndarray` (`.values` of the column) | numeric/cell array (table column) |
| Multiple columns | `pandas.DataFrame` (subset of columns) | MATLAB `table` (subtable) |

## How It Works (Python)

1. `MyVar["col"]` creates `ColumnSelection(MyVar, ["col"])` (scidb's subclass).
2. `_is_loadable()` in `scidb/src/scidb/foreach.py` recognizes `ColumnSelection`
   (checked against the scifor base class) as loadable.
3. `_load_input` dispatches on what's inside: a plain DataFrame passes
   through unchanged (as-is, no loading needed); something with `.load()`
   goes through `_load_var_type_as_spread` (bulk) and gets re-wrapped as a
   `scifor.ColumnSelection` around the loaded DataFrame; anything else
   (can't bulk-load) is wrapped in `PerComboLoader`, resolved per-combo by
   `_resolve_per_combo_loader` calling `spec.data.load(**load_kw)`.
4. Either way, the per-combo *extraction* (dropping to a numpy array for one
   column, or a DataFrame subset for several) is scifor's job —
   `scifor/src/scifor/foreach.py`'s `prepare_input`/column-selection
   handling, unchanged by which package constructed the wrapper.

## How It Works (MATLAB)

1. `MyVar("col_a")` stores `"col_a"` in the `selected_columns` property on
   the `BaseVariable` instance itself (no separate wrapper object).
2. For `for_each()`, MATLAB's bridge (`scimatlab/src/scimatlab/bridge.py`,
   `describe_input_for_python` on the MATLAB side in `+scidb/for_each.m`)
   serializes a non-empty `selected_columns` (or `iterate`) into a
   `{"kind": "column_selection", "type_name": ..., "columns": ..., "iterate":
   ...}` spec, which Python's `_reconstruct_input_for_keys` rebuilds into a
   real `scidb.column_selection.ColumnSelection` — from there it's the same
   Python machinery described above.
3. For direct calls that don't go through `for_each` (e.g. `.to_csv()`),
   MATLAB constructs `py.scidb.column_selection.ColumnSelection(py_class,
   py.list(cols))` directly via the bridge and calls its instance methods —
   MATLAB never re-implements the extraction itself.

## From the GUI

Everything above is reachable from the canvas: a function node's settings
panel has an **Inputs** section with one row per variable-bound parameter, a
multi-select column list (Select all / Deselect all) and a **Run once per
column** checkbox for `for_columns`. Nothing ticked means the whole variable.
The node itself shows a read-only chip — `⟨"filename"⟩`, `⟨3 columns⟩`,
`⟨per column⟩` — and the same text is on the input handle's tooltip.

`excl_columns` is **not** exposed; it stays a Python-level escape hatch.

### Where the selection is stored

In `_node_config`, beside `runOptions` / `whereFilters` / `schemaSelection`
(`docs/claude/gui-run-options-flow.md`):

```jsonc
// _node_config[node_id].config
{ "columnSelections": { "table_in": { "columns": ["filename"], "iterate": false } } }
```

Keyed by NODE, not by edge. That is not a convenience: a function that has
already run has no manual edge rows at all — a source-declared pipeline's
edges are re-synthesised from DB history on every graph build — so a
per-edge selection would work perfectly on a fresh canvas and vanish the
first time the node ran.

An empty `columns` with `iterate: false` is stored as no entry at all. It
means "the whole variable", which is what binding the bare class already
does; surviving as a `ColumnSelection` would fork the version key for no
change in what the function receives. An empty `columns` **with** `iterate`
is kept — that is `for_columns()`, every data column, one call each.

### How it reaches a run

```
Inputs panel ──put_node_config──▶ _node_config.columnSelections
                                         │
              execution_service._attach_column_selections  (BOTH derivation paths)
                                         │
                            target["bindings"][param]["columns"/"iterate"]
                    ┌────────────────────┼────────────────────┐
                    ▼                    ▼                    ▼
        build_run_inputs         matlab_command_service   code_export_service
        MyVar[cols] /            _apply_column_selections  _py_literal /
        MyVar.for_columns(cols)  → variable_inputs dict     _matlab_literal
```

`derive_fn_targets` is name-scoped (any `fn__{name}__*` config id counts);
`derive_target_for_node` is node-scoped, so two call sites of one function
name keep their own selections.

The MATLAB side folds the selection into the **same** `variable_inputs` map
rather than a parallel `column_selections` dict — one map, one parser
(`_variable_binding_parts`), so all three `for_each` emit sites
(template/first-run, single-function, per-pipeline-step) are covered at once.

The diagnostic when a selection shows in the panel but the run loads whole
tables is the **absence** of this INFO line in `scidb.log`:

```
[execution] 'summarise': 'table_in' restricted to "filename" (['filename']) on 1 target(s)
```

### The call_id asymmetry (non-obvious, load-bearing)

scidb's **forward** `to_call_id` *does* include `ColumnSelection.to_key()`
— `ForEachConfig._serialize_inputs` puts it in `__inputs`, which is exactly
what makes a column change re-run instead of silently reusing a cached
result.

The GUI never sees that forward id. Provenance stores an input edge as
`(param → record → variable_type)` with no trace of the columns, so
`provenance_query.pipeline_variants` reconstructs `__inputs = {"table_in":
"Trials"}` and the canvas node id is built from *that*. So the selection is
kept **out** of `graph_builder.wiring_id` and
`variant_resolver.compute_call_id`: feeding it in would predict an id no
record ever carries, and silently break combo hiding on every
column-selected node. `tests/test_column_selection_binding.py` pins both.

### Known limitations

1. **One selection per parameter, not per edge.** A parameter fed by two
   variable types (`EachOf`) applies the same column set to both. A column
   present in one type and absent in the other fails at load with scifor's
   existing `KeyError` naming the available columns.
2. **History cannot tell you a run used a selection.** Provenance records the
   variable type, not the columns; the GUI's own config is the only record.
   Making that durable means recording the selection in `_invocation_input`,
   a scidb-layer change.

## Key Files

| File | Role |
|------|------|
| `scifor/src/scifor/column_selection.py` | `ColumnSelection` base (container, `to_key()`, `__hash__`, `__name__`) |
| `scidb/src/scidb/column_selection.py` | scidb subclass: comparison operators, `.load()`, `.to_csv()` |
| `scidb/src/scidb/variable.py` | `BaseVariable.__class_getitem__`, `.for_columns()` |
| `scidb/src/scidb/foreach.py` | `_is_loadable`, `_load_input`, `_resolve_per_combo_loader` |
| `scifor/src/scifor/foreach.py` | Per-combo filter/extract logic (shared, package-agnostic) |
| `scimatlab/src/scimatlab/matlab/+scidb/BaseVariable.m` | `selected_columns` / `iterate` properties |
| `scimatlab/src/scimatlab/matlab/+scidb/for_each.m` | `describe_input_for_python` — bridges `selected_columns` to Python |
| `scimatlab/src/scimatlab/bridge.py` | `_reconstruct_input_for_keys` — rebuilds the Python `ColumnSelection` |
| `scistack-gui/scistack_gui/domain/column_selection.py` | GUI: normalise a stored selection; stamp it onto bindings |
| `scistack-gui/scistack_gui/services/execution_service.py` | GUI: `_attach_column_selections`, `build_run_inputs` |
| `scistack-gui/scistack_gui/services/variable_service.py` | GUI: `input_columns` — the live column list the picker shows |
| `scistack-gui/frontend/src/components/Sidebar/FunctionSettingsPanel.tsx` | GUI: the Inputs section |

## See Also

- `docs/claude/scifor-scidb-modifier-unification.md` — why `ColumnSelection`
  is the one unified class that still needs a scidb-side subclass, and how
  that interacts with `_is_loadable`/isinstance checks throughout scidb.
