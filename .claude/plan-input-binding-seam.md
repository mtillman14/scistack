# Plan — the input-binding seam: make it loud, then make it simple

**Superseded 2026-09-19 by `.claude/plan-intent-and-fact.md`**, which folds
these five stages into the intent/fact model (`docs/claude/intent-and-fact.md`).
This file stays as the detailed write-up of the column-selection case.

Drafted 2026-09-19 after the integration suites found two column-selection
bugs (`.claude/plan-gui-pane-combinations.md`, the two `xfail(strict=True)`
tests in `tests/integration/test_dag_runs.py`).

## The one sentence

**How a function's inputs were chosen is a fact that has to survive from the
call, into history, back out into a GUI-derived call — and today nothing owns
that fact end to end, and nothing complains when it is lost.**

Everything below follows from that. The hops themselves are not the problem
(a pipeline that survives a restart has to write things down); the problem is
that each hop re-invents the shape, and a loss is silent.

## Stage 1 — Guard the round trip in scidb, at BOTH ends (do this first)

The bug that matters is not "the selection was dropped", it is "the function
ran on everything and nobody said so". A `for_columns` step silently became a
whole-table step; the example only failed loudly because the function could
not accept a table. A quieter function writes wrong numbers.

**The invariant: a selection that goes into a run comes back out of history
unchanged.** It needs a guard at each end of the round trip, and both belong
in **scidb**, under every caller — Python, MATLAB, the GUI and the CLI all
lose a selection the same way. (The first draft of this stage put one
read-side check in `execution_service.build_run_inputs`. That is GUI-layer,
and — see 1b — it cannot see the loss that actually happened.) See
`docs/claude/input-binding-round-trip.md` for what a binding is and every hop
it makes.

### 1a — Write side: what was PASSED vs what got RECORDED

`compute_input_selectors(inputs)` (`foreach.py:3094`) knows what the call
asked for. `_variable_bindings` (`provenance_save.py:211`) decides what the
edges carry — and its `__upstream` fallback writes `selector=None` for every
edge, because aggregation and `for_columns` reassembly rows carry no
`__rid_*` columns, so `__graph_var_bindings` is never set. That fallback is
the entire Stage 2 bug, and today it is silent.

* One owner in `scidb.provenance_save`:
  `check_selector_round_trip(fn_name, asked, recorded, *, context)` — WARN per
  param whose asked-for selector is missing from the recorded edges, naming
  both values and which save path was taken (`__graph_var_bindings` vs
  `__upstream`). Same shape as the `[coarse-input]` line
  (`foreach.py:3623`).
* Call it in `_save_results`' row assembly (`foreach.py:5249-5266`), where
  `input_selectors` and `_row_bindings` are both in hand — **once per run**,
  deduped on the param set, not once per row.

This is the guard that would have caught `for_columns` on the day it broke.

### 1b — Read side: what was RECORDED vs what this run BINDS

Once per `for_each` call, right after `input_selectors =
compute_input_selectors(inputs)`: ask
`provenance_query.function_variant_configs(duck, fn_name)` for the configs
already recorded for this function, and compare per param.

* recorded has a selector, current has none → **WARN** (the loss).
* both present and different → **INFO** ("selection changed"): a user
  changing their mind, which `_recompute`'s `selector for {param} changed`
  (`foreach.py:1376`) already treats as ordinary.
* current has one, history does not → silent (a new selection).

**Not in `_build_skip_hook`.** It already does this comparison at
`foreach.py:1376`, which is tempting and wrong: the hook is built only when
`skip_computed and not dry_run and outputs and active_db is not None`
(`foreach.py:580`), and a lost selection has nothing to do with caching. One
query at run entry, log-only, never changes what runs.

### 1c — The GUI contributes CONTEXT, not logic

`build_run_inputs` knows the one fact the guards cannot see: whether
`_node_config.columnSelections` overrode history (`_attach_column_selections`
after `_attach_db_selectors`, `execution_service.py:265-283`). It passes that
as the `context=` string so the WARN reads "overridden by node config" rather
than leaving the reader to guess. Plus the run's own summary:

* `api/run._run_in_thread`: one INFO line per run naming EVERY input binding,
  spelled with `column_selection.describe` (the one spelling shared with the
  canvas chip) — `value: TrialMeanSymmetry["ankle", "knee"] · cycles:
  CycleSymmetry (whole variable)`. So `scidb.log` always answers "what did
  this run actually feed the function?" — the question that took four round
  trips this session.

### Tests

* **scidb, write side:** call `check_selector_round_trip` directly with a
  mismatched pair and assert the WARN — constructed, not reached through the
  `for_columns` bug, so Stage 2 does not silently remove the test's trigger.
  Then a second test asserting the real `for_columns` reassembly path is
  SILENT: red today, green at Stage 2, and the regression guard thereafter.
* **scidb, read side:** history with a recorded selector, re-run binding the
  whole variable → WARN; identical selector → silent; different selector →
  INFO, not WARN.
* **GUI integration** (`tests/integration/test_dag_runs.py`): the bindings
  line names every signature param — completeness pulled from the registry
  signature, not a spot check.

Independent of every other stage, and it is what turns the next failure of
this class into a one-line diagnosis.

## Stage 2 — Fix the two open bugs

With Stage 1 in place these are ordinary bugs, and the log says which layer.

1. **`for_columns` records no selector** on its reassembly path. Bisect from
   the shape that DOES record one
   (`scidb/tests/test_reload_supersedes_changed_file.py::
   test_a_for_columns_call_records_its_selector`) to the example's
   `scale_joint`, one difference at a time. Fix in the writer that omits it.
   **Guard:** any change to what is recorded changes `invocation_id` — run
   `scidb/tests/test_aggregation_with_variants.py` and `test_stat_leaves.py`
   before anything else (folding at save time broke `skip_computed` for every
   aggregation earlier today).
2. **A node-config selection never reaches the run.** Ask
   `column_selections_for_nodes` directly, with the id the panel really saves
   under, and with the node ids the run derivation passes. Fix the matcher or
   the test, whichever the answer indicts.

Then flip both `xfail(strict=True)` tests to ordinary tests.

## Stage 3 — One owner for the binding shape

Today two modules normalize the same thing by convention:
`scidb.provenance_save.compute_input_selectors` and
`scistack_gui.domain.column_selection.normalize`. They agree because someone
remembered to make them agree.

* Move the shape (`{"columns": [...], "iterate": bool}`), its normalizer and
  its `None`-means-whole-variable rule into **one** module — scidb, since it
  is below the GUI and already defines the storage — and have the GUI import
  it.
* State the precedence rule in that module's docstring and implement it
  there: **node config beats history** (the user's most recent statement of
  intent), with the WARN from Stage 1 whenever they differ.
* No new abstraction. One module, two importers.

## Stage 4 — `for_columns` belongs beside `distribute` / `as_table`

`iterate` is an execution MODE recorded inside a column list. Its siblings —
`distribute`, `as_table` — are first class: columns on `_invocation`, folded
into `invocation_id`, reported by `pipeline_variants`, shown in the GUI's run
options, and carried through `run_options`. That is why they never get lost
and `iterate` did.

* Record `iterate` as part of the invocation's run options rather than (only)
  the selector.
* Report it in `pipeline_variants[].run_options` and let the GUI's Run
  options section show "run once per column", which is what it means.
* **Migration:** this changes `invocation_id` for existing `for_columns`
  calls, so their first re-run writes superseding records. Same class of
  change as the 2026-09-14 run-option work; do it deliberately, in its own
  commit, with a note in `docs/claude/for-columns-iteration.md`.

Stage 2 can land without this; this is what stops the bug returning.

## Stage 5 — Key node config by something that does not move

`_node_config` is keyed by the canvas node id, which is placement-qualified
and changes on graduation and scope moves — hence `migrate_node_config`,
`strip_placement`, and the placement-id trap already in the notes. Every
lookup is "compare ids that are deliberately not comparable".

* Key by `wiring_id` (already stable across graph builds: name + input shape
  + outputs) plus the scope, with the node id as a display attribute.
* Keep `migrate_node_config` for one release to read old rows.
* This retires a class of bug rather than an instance, but it touches the
  canvas, so it wants its own session and a GUI pass afterwards.

## Order, and why

1. **Stage 1** — cheapest, and it changes every future diagnosis of this class.
2. **Stage 2** — the two live bugs; users hit these today.
3. **Stage 3** — removes the "agree by convention" between two layers.
4. **Stage 4** — removes the place the bug lived.
5. **Stage 5** — the recurring id trap; biggest blast radius, least urgency.

## Not doing

A general framework for "GUI state history cannot carry". There is exactly
one such seam — input bindings — and one module with two importers is the
whole fix. PathInputs already travel the same road
(`_attach_db_path_inputs`); if a third case appears, generalize then.
