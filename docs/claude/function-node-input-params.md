# `input_params` — how a function node's input handles are assembled

*Written 2026-09-15, reconstructed from source while building the column
selection UI (`.claude/plan-column-selection-ui.md` Stage 5). Concerns
`scistack-gui` only.*

`FunctionNodeData.input_params` is `{param_name: variable_type_name}`. It
decides the node's left-hand **handle set**, the labels on those handles, and
— since the column selection UI — which rows the settings panel's **Inputs**
section offers a column picker for.

It looks like "the function's variable inputs". It is not quite that, and the
gap is the reason this note exists: **a parameter whose value is the empty
string is not one thing.** It is at least four, and nothing downstream can
tell them apart.

---

## 1. Two producers, never both

A function node is built by one of two paths depending on whether it has DB
history. They agree on the shape and disagree on almost everything else.

### DB-derived nodes — `graph_builder.build_function_nodes`

Source: `AggregatedData.fn_input_params`, keyed `(fn_name, call_id)` before
grouping and `(fn_name, wiring_id)` after it.

`aggregate_variants` (`graph_builder.py`) walks each variant's recorded
`input_types` and **partitions** it:

```python
for param_name, type_val in inputs.items():
    pi = parse_path_input(type_val)
    if pi is not None:
        agg.path_inputs[pi_name]["functions"].add((fkey, param_name))   # ← leaves
    else:
        agg.fn_input_params[fkey][param_name] = type_val                # ← stays
```

So a PathInput-fed parameter is recorded in history *inside* `input_types`,
right beside the real variable inputs, and is deliberately removed here. It
survives only in `agg.path_inputs`, keyed by declared PathInput name with a
`functions` set of the `(fkey, param)` call sites that used it.

Constants never appear at all — they are in `agg.fn_constants` and render as
`param__{name}` handles, a separate list.

`group_call_sites_by_wiring` then merges the per-call-site dicts into one per
wiring group (`grouped.fn_input_params[gkey].update(...)`). Members of a group
agree by construction, because `wiring_id` hashes the input shape.

### Manual (never-run) nodes — `api/pipeline.py`

Source: `resolve_function_edges` on that node's own edges, intersected with
the registry signature:

```python
sig_params      = _fn_params_from_registry(fn_label)
inferred_inputs = {p: ts[0] for p, ts in resolved.input_types.items() if ts}
resolved_input_params = {p: inferred_inputs.get(p, "") for p in sig_params}
```

Every signature parameter is present; the ones no edge feeds are `""`. A
parameter fed by more than one variable type (EachOf) keeps only `ts[0]` here
— the node shows one type, the settings panel's Variants table shows the axis.

---

## 2. The fill-in pass, and the four meanings of `""`

`build_function_nodes` ends its per-node assembly with:

```python
known = set(input_params) | set(constant_params)
for name in fn_params_map.get(fn, []):
    if name not in known:
        input_params[name] = ""
```

The intent is good: a function node must render a handle for every signature
parameter, or there is nowhere to drop an edge for a parameter the DB has
never seen filled.

The consequence is that `""` now means **any** of:

| Why it is `""` | Reachable by |
|---|---|
| Genuinely unwired — nothing feeds this parameter | a node placed but not fully connected |
| Fed by a **PathInput** | partitioned out by `aggregate_variants`, restored here |
| Fed by a **Parameter**… | …only if it is *also* not in `constant_params`; normally it gets a `param__` handle instead |
| Fed by a variable the user **hid** | `filter_hidden(strip_var_type_values=True)` deletes the entry (§3) |

Nothing in the node data distinguishes them. The settings panel therefore
cannot say "this parameter is fed by a file path" — it can only say "no
variable type", which is why the Inputs section renders those rows as an
inert `n/a` with a tooltip rather than a disabled picker or a spinner.

**The fill-in is display-only.** `input_params` is rebound to a fresh sorted
dict at the top of the loop, so the `""` entries live on that copy and never
reach `agg.fn_input_params`. Neither `build_edges` nor `wiring_id` ever sees
them. That is load-bearing: an entry mapping a parameter to `""` would
otherwise become an edge from a node named `var__` and would change the node's
identity hash.

---

## 3. Hiding a variable type mutates `input_params`

`filter_hidden(agg, hidden_ids, strip_var_type_values=True)` removes every
`{param: type}` entry whose *value* is a hidden variable type:

```python
agg.fn_input_params[fkey] = {
    p: t for p, t in agg.fn_input_params[fkey].items() if t not in hidden_var_types
}
```

The parameter then has no entry, so the fill-in pass restores it as `""`.
Hiding an input variable and never wiring the parameter at all are, from the
node's point of view, the same state.

`api/pipeline.py` calls `filter_hidden` **twice**, and the flag differs:

1. **pre-grouping, `strip_var_type_values=False`** — identity has not been
   fixed yet. Scrubbing values here would move `wiring_id`, so the node would
   lose its saved placement and vanish from non-root scopes the moment one of
   its outputs was hidden.
2. **post-grouping, default `True`** — identity is already fixed, so stripping
   is safe and removes phantom handles for hidden types.

Getting those two the wrong way round is a whole class of bug; the ordering is
documented at `filter_hidden`'s docstring and repeated at both call sites.

---

## 4. Ordering

**One owner: `graph_builder.input_port_order(signature, names)`.** The order is
registry signature order, then any name outside the signature (stale DB data)
alphabetically. **What feeds an argument never moves it.**

`graph_builder.assign_input_ports(nodes, signatures)` runs once, as the last
node pass in `api/pipeline._build_graph`, after the manual-edge overlay and
the manual and glue nodes. It writes:

- `data.input_ports`: every argument (signature ∪ `input_params` ∪
  `constant_params`) in that order. The frontend draws it as is through
  `frontend/src/components/DAG/inputPorts.ts`, which both the canvas node and
  the variant-graph node use, and never re-orders it.
- `input_params`: re-ordered by the same rule, so the sidebar Inputs list
  matches the node.

`build_function_nodes` also sorts with `input_port_order`, and it fills in
unrecorded arguments *before* sorting.

Until 2026-10-21 there was no single owner, and three things each broke the
order:
1. The frontend drew `[...input_params, ...constant_params]`, so a
   Parameter-fed argument was drawn after every variable-fed or unwired one.
   `load_xsens_one_file(xsens_file_path, col_names, sheet_name)` drew
   `col_names` third.
2. The fill-in for unrecorded arguments ran after the sort and appended them
   at the end.
3. The manual-edge overlay appended keys after `build_function_nodes` had
   sorted.

`assign_input_ports` logs one INFO line for each node whose port order differs
from the old variable-first drawing.

`fn_params_map` comes from `_fn_params_from_registry`, which reads
`registry.function_arguments` (MATLAB declared params first, else the Python
function, its `.fcn` if it is a wrapper) and drops `_`-prefixed names. Since
2026-10-24 every signature read goes through ONE owner,
`scifor.signature.function_arguments` (see §4b).

This matters because MATLAB's `for_each` binds struct fields to arguments
**positionally** — see `matlab_command._order_inputs_by_signature`, which
re-sorts the emitted struct for the same reason.

---

## 4b. Shown vs optional ports (2026-10-24)

*Plan: `.claude/plan-optional-argument-ports.md`. Trigger: `pandas.read_csv`
drew ~50 handles on one node edge, so `sep` could not be wired.*

**One owner: `graph_builder.port_visibility(arguments, params, constants, fed)`.**
A port is SHOWN when its argument has no default (and isn't `*args`/`**kwargs`),
or is fed: a variable type in `input_params`, a Parameter in
`constant_params`, or a name in `fed`. A name outside the signature is always
shown (§4). If the signature is unknown, or its defaults are (MATLAB:
`registry.function_arguments` reports declared params as required), every
argument is shown, as before. Every other non-private argument is OPTIONAL:
`data.optional_ports = [{"name", "default"}]` (`Argument.default_repr`),
signature order.

`fed` per node, built by `assign_input_ports` from:
- the edges the canvas DRAWS (`_build_graph`'s final `edges`, by
  `handle_name(targetHandle)` on that node id). A removed (hidden, kept) edge
  is not drawn, so it doesn't keep a port; a past run alone doesn't either
  (user decision 2026-10-24: "shown only while fed");
- `data.columnSelections` keys;
- `data.shownArguments`: revealed with "+ argument". Stored in `_node_config`
  (`_SAVED_CONFIG_KEYS`, so every rehydration path copies it), written by
  `layout_service.set_shown_argument` (merges into the blob, since
  `update_node_config` replaces it; `undoable=True`; broadcasts dag_updated).

`input_params` keeps exactly the drawn ports (the hidden ones' `""` entries
are dropped). That is safe because the fill-in is display-only (§2).

Consumers of the rule, never a second copy of it:
- `assign_input_ports` (every canvas build);
- `pipeline_service.get_function_full_info` (`get_function_params`): the
  optimistic node a drop draws gets `input_ports` / `optional_ports` from the
  same function with nothing fed, so the refetch doesn't redraw it.

Signature reads have ONE owner too: `scifor.signature.function_arguments`
(`Argument`: `name, kind, default`; `.optional`, `.named`, `.private`,
`.default_repr()`). Readers: scifor `_accepted_param_names`, scidb
`pipeline._default_input_names` (step values), and in the GUI
`registry.function_arguments` → `_fn_params_from_registry`,
`port_visibility`, `library_service.unwired_step_values` (tunables). So
"has a default" means the same thing to the ⚙ Tunables panel and to the
canvas.

Frontend: `FunctionNode` draws `input_ports` and, when `optional_ports` is
non-empty, a **＋ argument** control (`OptionalArgumentPicker.tsx`; pure
filter/label logic in `inputPorts.ts`, `node --test`). Clicking it reveals the
port (`set_shown_argument`). Dropping a connection on it (the control carries
`data-optional-args-node`) is caught by `PipelineDAG.onConnectEnd`, which
opens the same list and calls `put_edge` to `in__{arg}` without an optimistic
edge (the port doesn't exist until the refetch).

Tests: `scifor/tests/test_signature.py`,
`scistack-gui/tests/test_graph_builder.py::TestOptionalPorts`,
`scistack-gui/tests/test_optional_ports.py` (HTTP: drop, wire, reveal, undo).

Logging: `[signature]` DEBUG per read (layer scifor); `[graph_builder] ...: N
port(s) shown, M optional hidden` INFO per node that hides any;
`[layout_service] set_shown_argument` INFO; frontend `console.debug`
`[FunctionNode …] reveal argument` and `[PipelineDAG] connection dropped on +
argument`.

---

## 5. What `input_params` is *not*

* **Not the binding map.** `ResolvedEdges.bindings` is the source of truth for
  execution: one entry per parameter, tagged `variable` / `pathinput` /
  `parameter`, each with a declared-name `ref`. `input_params` is a display
  projection that keeps only the variable kind and flattens a multi-type
  binding to its first type. Anything that needs to *run* something reads
  bindings (`docs/claude/function-input-resolution.md`).
* **Not partitioned the same way as a raw variant.** `wiring_id` accepts both
  views and normalises with `strip_path_input_params`, precisely because the
  canvas passes the partitioned one and the run path passes a raw
  `input_types`. When those disagreed, a graduated PathInput-fed node hashed
  one way on the canvas and another in `derive_target_for_node`, and reported
  "No pipeline history or output connections found" for a green, fully wired,
  already-run node.
* **Not a record of column selection.** Columns picked in the Inputs section
  live in `_node_config.columnSelections`, keyed by parameter, and reach a run
  via the target's bindings — never via `input_params`. See
  `docs/claude/column-selection.md` §From the GUI.

---

## 6. If you are adding a consumer

Ask which of these you actually want:

* **"Which parameters get a left handle?"** → `input_params` keys. `""` is
  fine; the handle still has to exist.
* **"Which parameters are fed by a variable, and which one?"** →
  `input_params` entries with a non-empty value. Treat `""` as "no variable",
  never as "unwired" — §2 says you cannot tell.
* **"What will actually be passed at run time?"** → not this dict. Derive
  targets and read `bindings`.

The column selection UI wanted the second, and the inert `n/a` row is what
honesty about `""` looks like in the UI.

## Related

- `docs/claude/function-input-resolution.md` — bindings, the target dict, and
  `build_run_inputs`.
- `docs/claude/column-selection.md` §From the GUI — the first consumer of
  `input_params` that had to care about the `""` case.
- `docs/claude/archive/graph-database-state.md` §Identity — what `wiring_id` and
  `call_id` are built from.
- `docs/claude/placement-qualified-ids.md` — the other id-shape trap in this
  area.
- `docs/claude/manual-edges-on-history-nodes.md` — a manual variable edge
  onto a DB-derived node's `in__` handle is overlaid onto `input_params`
  after the fill-in pass (`graph_builder.overlay_manual_inputs`), so since
  2026-09-15 "there is a manual edge here" is no longer one of the meanings
  of `""`; `data.manual_inputs` names the overlaid params.
