# Plan: optional-argument ports (wire one argument of a many-argument function)

*Drafted 2026-10-24. Trigger: the Best Popcorn walkthrough needs a `SEP`
Parameter wired into `pandas.read_csv`'s `sep`. Today `assign_input_ports`
gives the node a port for EVERY signature argument (~50 for `read_csv`), drawn
on one short edge of the node, so no single argument can be targeted, and
there's no picker.*

## Goal

A function node draws only the arguments that matter; the rest are one
gesture away. Then the walkthrough: `SEP = ","` wired to `read_csv.sep`;
the colleague changes it to `";"` after import.

## Rule (one owner: `graph_builder.assign_input_ports`)

A port is **shown** when ANY of:
1. the argument has no default (required), or the function's defaults are
   unknown (MATLAB, no signature) → all shown, as today;
2. something feeds it: a drawn edge (variable, Parameter, PathInput, glue),
   a constant/Parameter in `constant_params`, or a recorded input in history;
3. it has a column selection;
4. the user revealed it on this node (persisted, see Stage 3).

Every other signature argument is **optional-hidden**. `assign_input_ports`
writes both lists: `data.input_ports` (shown, in `input_port_order`) and
`data.optional_ports` (`[{name, default_repr}]`, signature order). The
frontend draws `input_ports` as is (unchanged contract) and offers
`optional_ports` in a picker. No second place decides visibility.

## Signature owner (NOTE 4)

Today four readers call `inspect.signature` independently:
`api/pipeline._fn_params_from_registry` (names only),
`services/library_service.unwired_step_values` (defaults),
`scidb/pipeline.py:319` (defaulted args for `"fn.param"`),
`scifor/foreach.py:1544` (accepted names). Add ONE owner at the lowest
layer, `scifor.signature.function_arguments(fn) -> list[Argument]`
(`name, kind, has_default, default`, private `_` names dropped, the
`__scidb_params__` fallback for proxies), and make all four consumers.
`unwired_step_values` and the new hidden rule then agree by construction
on "has a default".

## Stages

**1. scifor owner.** `scifor/signature.py` + tests (plain fn, keyword-only,
`*args/**kwargs` excluded, builtin with no signature, `__scidb_params__`
proxy, `pandas.read_csv` has `sep` with default). Switch the four readers.
DEBUG log per resolved function: `n args, n defaulted`.

**2. Backend visibility.** `assign_input_ports(nodes, arguments)` takes the
`Argument` lists; computes shown/optional per the rule. INFO log per node
that hides anything: `"%s: %d port(s) shown, %d optional hidden"`. Tests in
`tests/test_input_port_order.py` (or new `test_optional_ports.py`):
read_csv manual node shows only `filepath_or_buffer`; wiring a Parameter to
`sep` shows `sep`; a history node keeps every recorded input shown; a
column-selected argument stays shown; MATLAB node shows all; a REMOVED
(hidden, kept) edge no longer keeps a port shown (`edge_view` owner, as
d2107318).

**3. Reveal state.** Revealing an optional port without wiring it must
survive refetch, so it's node config: `_node_config` key
`shown_arguments` (pipeline_store, already in `UNDOABLE_TABLES` → undo for
free). Handler `set_shown_arguments` (`undoable=True`, label "show
argument"). Hiding again = remove from the list (only when unwired).

**4. Frontend.** `FunctionNode.tsx`: when `optional_ports` is non-empty, a
small **＋ argument** control below the last input handle opens a
searchable list (name + default, e.g. `sep  = ','`). Two gestures:
(a) click a name → `set_shown_arguments` → port appears, drag onto it;
(b) drop a connection on **＋ argument** → same list → choosing creates the
edge to `in__{name}` directly (one undo step via `taskChange`). Pure logic
(filtering, labels) in `inputPorts.ts` with `node --test` cases. Rebuild
both bundles (frontend bundle trap).

**5. Docs.** `docs/gui/process.ui.md` §5 (connect) explains ＋ argument;
`docs/claude/function-node-input-params.md` gains a "shown vs optional"
section; GUI manual checklist item. Then the Best Popcorn changes:
Step 4 adds a `SEP` Parameter (value `,`) wired via ＋ argument → `sep`;
Step 14 has the colleague set `SEP` to `;` for `store2_ratings.csv`.
New 📷 placeholders for the picker.

## Open questions

- Should `**kwargs` functions offer a free-text "other argument" entry?
  Proposed: no (out of scope).
- Default `shown` rule for history nodes: every recorded input stays shown
  even if its edge is now removed? Proposed: shown only while fed (rule 2
  via drawn edges), so history doesn't pin clutter.

## Tests run by the user (never by Claude)

`pytest scifor/tests/test_signature.py`, then scistack-gui
`tests/test_optional_ports.py tests/test_history.py`, then
`cd scistack-gui/frontend && npm test`. One package at a time.
