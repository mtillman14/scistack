# The input binding, and its round trip

Written 2026-09-19, alongside `.claude/plan-input-binding-seam.md`, after the
integration suites found two column-selection bugs whose common cause is that
*how a function's inputs were chosen* is a fact nobody owns end to end.

This document answers two questions: **what is an input binding**, and **what
happens to one between the moment a run is described and the moment the same
run is described again from history.**

---

## 1. Definition

> **An input binding is the answer to "where does this one function parameter
> get its value?" — one parameter, one source, stated explicitly.**

A function has parameters. A pipeline step has to say, for each of them, what
fills it. That statement is the binding. It is:

* **per parameter** — one binding per named parameter of the function
  signature, never a positional list, never a bundle;
* **explicit** — it comes from a wired edge, from what a previous run
  recorded, or from Python source. It is **never inferred from a name
  matching**, and never from argument position (see §6);
* **a source, not a value** — it names *what to load* (a variable type, a
  declared PathInput, a declared Parameter), not the loaded data. The value
  appears later, per combination, when the run actually loads it;
* **complete on its own** — a parameter with no binding is unbound, which is
  a state the system must show and refuse, not paper over.

### The three kinds

A parameter is fed by exactly one of these
(`scistack_gui/domain/edge_resolver.py`, the `BINDING_*` constants):

| kind | `ref` is | fills the parameter with |
|---|---|---|
| `variable` | a list of variable TYPE names | records loaded from the DB — one per iteration combo |
| `pathinput` | the DECLARED PathInput name | a file path resolved per combo |
| `parameter` | the DECLARED Parameter name | a declared sweep value (fans out into combos) |

More than one type name in a `variable` ref means `EachOf` — "run it with
each of these", an axis, not an ambiguity.

### The shape

In the GUI layer a binding is a plain dict, built only by the constructors in
`edge_resolver` (`variable_binding`, `pathinput_binding`,
`parameter_binding`):

```python
{"kind": "variable",  "ref": ["TrialMeanSymmetry"], "columns": ["ankle"], "iterate": False}
{"kind": "pathinput", "ref": "gait_csv"}
{"kind": "parameter", "ref": "cutoff_hz"}
```

`columns`/`iterate` are **omitted entirely** when there is no column
selection, so a whole-variable binding keeps exactly the two keys it always
had — `resolve_function_edges._bind` compares bindings with `==` to detect
two edges fighting over one handle.

A step's bindings are one dict keyed by parameter: `{param_name: binding}`.

### What a binding is NOT

* **Not the data.** Binding `TrialMeanSymmetry` to `value` does not say which
  records; the schema keys and `where`/variant filters do that, per combo.
* **Not part of the call's GUI identity.** `graph_builder.wiring_id` and
  `variant_resolver.compute_call_id` read `ref` alone — a column pick does
  not change the node id. That asymmetry is load-bearing: scidb's forward
  `to_call_id` *does* fold `ColumnSelection.to_key()` in, but provenance
  stores an input edge as `(param → record → variable_type)` with no trace of
  the columns, so the reconstructed backward id would never match a forward
  one. See `column_selection.py`'s module docstring and
  `docs/claude/column-selection.md` §From the GUI.
* **Not a name match.** A PathInput declared `test_pi` can feed a parameter
  called `filepath_or_buffer`; the edge says so and the declared name never
  has to agree with the parameter. Resolving by elimination-plus-name is
  exactly the bug that once made a `read_csv` node run with `inputs={}`,
  iterate zero times, write nothing, and report success
  (`build_run_inputs`'s docstring, "the binding is the edge, never the name").
* **Not a glue node.** A glue node interposes on a binding; the parameter
  still binds to whatever is at the HEAD of the chain, and the chain rides
  alongside in `ResolvedEdges.glue_chains`
  (`docs/claude/free-code-glue-nodes.md` §5).

### The same idea in Python

`for_each(inputs={...})` is the binding dict written by hand: the key is the
parameter, the value is the source — a variable class, `MyVar["col"]`,
`MyVar.for_columns([...])`, `Fixed(...)`, `Variant(...)`, `Merge(...)`,
`EachOf(...)`, a `PathInput`, or a bare DataFrame. The GUI's binding dicts
exist because a canvas cannot hand around live Python objects across a
restart; `build_run_inputs` turns them back into exactly these objects.

---

## 2. The selector — the part that travels worst

Most of a binding survives trivially: the parameter name and the source name
are strings, and both ends already agree on them. One piece does not.

> **The selector is the part of a binding that says *which columns* of the
> source the function gets, and whether it gets them one at a time.**

`{"columns": ["ankle", "knee"], "iterate": false}` — with two meanings that
are not interchangeable:

* `iterate: false` → `MyVar[cols]`: the function is called ONCE and receives
  the column(s) as one argument (a numpy array for one column, a DataFrame
  subset for several).
* `iterate: true` → `MyVar.for_columns(cols)`: the function is called ONCE
  PER COLUMN and the results are reassembled into a one-row table carrying
  the source column names (`docs/claude/for-columns-iteration.md`).

Two rules that every layer must apply the same way:

1. **`None` means the whole variable.** An empty, non-iterate selection
   normalizes to `None` and must not survive as a `ColumnSelection`: no
   columns and no iteration is what binding the bare class already does, and
   wrapping it anyway forks the version key for no change in what the
   function receives.
2. **Empty WITH `iterate` is real.** `for_columns()` means "every data
   column, one call each", resolved at for_each time.

It travels worst because it is the only part of the binding that is optional,
that has two spellings, and that is written by one layer (`ColumnSelection`
objects) and read by another (JSON in a DB column).

---

## 3. The round trip

A pipeline that survives a restart has to write things down, so a binding
makes this trip. The hops are not the problem; each hop re-spelling the
binding is.

```
  Python source            GUI canvas                 node config
  for_each(inputs=…)       edge → in__{param}         _node_config.columnSelections
        │                        │                           │
        └───────────┬────────────┴───────────────────────────┘
                    ▼
        inputs={param: <source object>}          ← build_run_inputs rebuilds this
                    │
                    ▼  compute_input_selectors(inputs)        [provenance_save.py]
        selectors = {param: '{"columns": [...], "iterate": true}' | None}
                    │
                    ▼  per output row, at save                [foreach.py _save_results]
        edges = state.bindings.for_combo(row)     ← the row's __combo names its Selection
        (every mode: full iteration, aggregation, for_columns — one assembly)
                    │
                    ▼  record_run                             [provenance_save.py]
        _invocation_input(invocation_id, param_name, input_record_id, selector)
                    │
                    ▼  read back                              [provenance_query.py]
        function_variant_configs(…)["selectors"]
        pipeline_variants[].selectors
                    │
                    ▼  target derivation                      [execution_service.py]
        _attach_db_path_inputs   → bindings from history (+ recorded selectors)
        _attach_column_selections → node config OVERRIDES (latest user intent)
                    │
                    ▼  build_run_inputs                       [execution_service.py]
        inputs={param: MyVar["ankle"]}  → back to the top
```

**Precedence, stated once:** node config beats history, because it is the
user's most recent statement of intent; history beats nothing at all. A run
whose binding carries a selection that matches neither is a bug, and that is
what the Stage 1 guards look for.

---

## 4. Where it breaks

Three known breaks, all of the same family — a re-spelling that loses the
optional part:

1. **(Fixed 2026-09-20.) The `__upstream` fallback dropped every selector.**
   Aggregation and `for_columns` reassembly rows carried no `__rid_*`
   columns, so the edge list was never set and the save fell back to
   `__upstream`, `{__rid_<param>: record_id}`, with nowhere to put a
   selector. Both spellings are gone: every row names its `Selection`
   (`__combo`), and `RunBindings.edges_for` is the one assembly in every
   mode, selector included.
2. **A node-config selection that never reaches the run** — an id-matching
   question, because `_node_config` is keyed by the placement-qualified
   canvas node id (`docs/claude/placement-qualified-ids.md`).
3. **Resolution by name/elimination** (historical, fixed): the binding must
   be the edge.

What makes all three expensive is not the loss, it is the **silence**: a
function handed a whole table instead of one column either raises somewhere
far away, or — worse — computes a number.

---

## 5. The guards

Because the trip has two ends, one check cannot cover it. A read-side check
("what history recorded vs what this run binds") is blind to a selection that
was **never recorded**, since there is nothing to disagree with — and that is
break #1, the live bug. A write-side check ("what the call passed vs what got
recorded") is blind to a selection lost after storage. So:

* **write side**, at save: `compute_input_selectors(inputs)` vs the edges
  actually written → WARN naming both and the save path taken.
* **read side**, at run entry: the recorded selectors for this function vs
  the ones this run is binding → WARN when recorded-had-one and current has
  none; INFO when both exist and differ (a user changing their mind).

Both live in **scidb**, under every caller — the GUI, MATLAB, the CLI and
plain Python lose a selection identically, and only scidb sees them all. The
GUI contributes the one fact scidb cannot see: whether a node config
overrode history. Plus one INFO line per run naming every binding, so
`scidb.log` always answers "what did this run actually feed the function?".

The model for all of this is the `[coarse-input]` line
(`foreach.py:3623`): a silent, legitimate-looking narrowing of what a
function receives, made loud, with both facts on the line.

---

## 6. Invariants

1. One binding per parameter; the parameter name comes from the edge's
   `targetHandle` (`in__{param}`), never from position or from the source's
   declared name.
2. A binding names a SOURCE; which records it yields is decided per combo.
3. `columns`/`iterate` are absent when there is no selection; `None` means
   the whole variable; empty-with-`iterate` means every column, one call each.
4. The selection is deliberately NOT part of `wiring_id` / the GUI's
   `call_id`, and deliberately IS part of scidb's forward `invocation_id`.
5. Node config beats history beats nothing.
6. A selection that goes in comes back out unchanged — and when it does not,
   something says so.

## 7. Ground truth

| what | where |
|---|---|
| binding kinds + constructors | `scistack-gui/scistack_gui/domain/edge_resolver.py` |
| selection normalizer, one spelling for logs/chips | `scistack-gui/scistack_gui/domain/column_selection.py` |
| bindings → live objects for a run | `scistack_gui/services/execution_service.py::build_run_inputs` |
| history → bindings | `execution_service.py::_attach_db_path_inputs`, `_attach_column_selections` |
| inputs → selectors | `scidb/src/scidb/provenance_save.py::compute_input_selectors` |
| edges written per row | `scidb/src/scidb/foreach.py::_save_results` (`__graph_var_bindings`) |
| edges read back | `provenance_save.py::_variable_bindings`, `provenance_query.function_variant_configs` |
| the caching comparison of the same facts | `foreach.py::_build_skip_hook` ("selector for {param} changed") |
| related docs | `column-selection.md`, `for-columns-iteration.md`, `function-input-resolution.md`, `free-code-glue-nodes.md`, `placement-qualified-ids.md`, `coarse-level-inputs.md` |
