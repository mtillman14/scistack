# Plan — intent and fact: one resolver, one store, five rules

Drafted 2026-09-19. Conceptual reference: `docs/claude/intent-and-fact.md`
(read that first — the rules, the nouns and the three decisions live there).
This plan SUPERSEDES `.claude/plan-input-binding-seam.md`, whose five stages
are folded in below; that file stays as the detailed write-up of the
column-selection case that started it.

## The one sentence

**A statement about a run that should happen and a record of a run that did
happen are different kinds of thing, and today they are mixed at five
different sites by five different pieces of code — so make one resolver that
mixes them once, and one store that holds the statements.**

## Scope discipline

Every stage is independently shippable and independently useful. Stage 5 is
the only one with a migration; Stages 1-4 are what the current bugs need. If
the model turns out wrong, the damage is one module and one table, not a
rewrite — which is why the vocabulary comes before the store.

---

## Stage 1 — The vocabulary, in scidb, with no behaviour change

*(absorbs `plan-input-binding-seam` Stage 3 — one owner for the binding shape)*

New `scidb/src/scidb/intent.py`:

* `Statement`, `Origin`, `Scope`, `Aspect` — plain dataclasses/enums, no I/O.
* `RunPlan` — resolved bindings, constants, run options, schema filter.
  Exactly what `for_each` needs, nothing more.
* `Decision` — per resolved field: winning value, its surface and scope, and
  what it beat.
* `resolve(statements, fact, *, origin) -> (RunPlan, Decision)` — rules 3 and
  4 of the doc, in one function.

Move in, as the first aspect normalizer, the column-selection shape
(`{"columns": [...], "iterate": bool}`, its `None`-means-whole-variable rule,
its comparison), merging `provenance_save.compute_input_selectors` and
`scistack_gui.domain.column_selection.normalize`. The GUI imports it; the
GUI keeps only `describe()` (the canvas spelling) if that reads better there.

**Nothing changes behaviourally.** The GUI still calls its existing
derivation; it just calls the shared normalizer inside it.

Tests: pure, no DB — precedence per origin, scope shadowing, the `None` rule,
and a table-driven case per aspect as aspects arrive.

---

## Stage 2 — The round-trip guards, as reports on a `Decision`

*(absorbs `plan-input-binding-seam` Stage 1 — same two guards, same layer,
now sourced from the resolver instead of bolted on)*

* **Write side**, at save: `compute_input_selectors(inputs)` vs the edges
  actually written. `_variable_bindings`' `__upstream` fallback
  (`provenance_save.py:211`) records `selector=None` for every edge, because
  aggregation and `for_columns` reassembly rows carry no `__rid_*` columns.
  One owner — `provenance_save.check_selector_round_trip(fn_name, asked,
  recorded, *, context)` — called once per run from `_save_results`' row
  assembly (`foreach.py:5249-5266`), deduped on the param set. WARN naming
  both values and the save path taken. Same shape as `[coarse-input]`
  (`foreach.py:3623`).
* **Read side**, at run entry: the `Decision` already holds "history recorded
  X, this run binds Y, node config said Z". WARN when history had a selector
  and the plan carries none; INFO when both exist and differ (a user changing
  their mind, which `_recompute`'s `selector for {param} changed`
  (`foreach.py:1376`) already treats as ordinary). **Not** in
  `_build_skip_hook` — that hook exists only when `skip_computed and not
  dry_run and outputs and active_db is not None` (`foreach.py:580`), and a
  lost selection has nothing to do with caching.
* **One INFO line per run** naming every input binding, rendered from the
  `RunPlan` with the single spelling shared by the canvas chip:
  `value: TrialMeanSymmetry["ankle", "knee"] · cycles: CycleSymmetry (whole
  variable)`.

Tests:
* write side — call the checker directly with a mismatched pair (constructed,
  so Stage 3 does not remove the test's own trigger), plus a test that the
  real `for_columns` reassembly path is SILENT: red today, green at Stage 3,
  regression guard thereafter;
* read side — recorded-and-dropped → WARN; identical → silent; different →
  INFO, not WARN;
* GUI integration (`tests/integration/test_dag_runs.py`) — the bindings line
  names every signature param, completeness pulled from the registry.

---

## Stage 3 — The two live bugs

*(`plan-input-binding-seam` Stage 2, unchanged — now with the `Decision`
trace saying which layer)*

1. **`for_columns` records no selector** on its reassembly path. Bisect from
   the shape that DOES record one
   (`scidb/tests/test_reload_supersedes_changed_file.py::
   test_a_for_columns_call_records_its_selector`) to the example's
   `scale_joint`, one difference at a time. Fix in the writer that omits it.
   **Guard:** any change to what is recorded changes `invocation_id` — run
   `scidb/tests/test_aggregation_with_variants.py` and `test_stat_leaves.py`
   first.
2. **A node-config selection never reaches the run.** Ask
   `column_selections_for_nodes` directly, with the id the panel really saves
   under and the node ids the run derivation passes. Fix the matcher or the
   test, whichever the answer indicts.

Then flip both `xfail(strict=True)` tests in
`tests/integration/test_dag_runs.py` to ordinary tests.

---

## Stage 4 — `iterate` becomes a run option

*(`plan-input-binding-seam` Stage 4, now expressed as the `run_options`
aspect)*

`iterate` is an execution MODE recorded inside a column list. Its siblings
`distribute` and `as_table` are first class — `_invocation` columns, folded
into `invocation_id`, reported by `pipeline_variants`, shown in the GUI's Run
options — which is why they never get lost and `iterate` did.

* Record `iterate` in the invocation's run options rather than only the
  selector; report it in `pipeline_variants[].run_options`; the GUI's Run
  options section shows "run once per column".
* **Migration:** this changes `invocation_id` for existing `for_columns`
  calls, so their first re-run writes superseding records. Same class as the
  2026-09-14 run-option work — its own commit, with a note in
  `docs/claude/for-columns-iteration.md`.

---

## Stage 5 — One intent store

*(absorbs `plan-input-binding-seam` Stage 5 — key by something that does not
move)*

Replace the seven execution-intent tables (`docs/claude/intent-and-fact.md`
§4) with one:

```
_intent(subject_kind, subject_ref, scope, aspect, value_json, origin, stated_at)
```

* `subject_ref` is stable — `wiring_id` + declared names, never a
  placement-qualified canvas id. This retires `migrate_node_config`,
  `strip_placement` lookups and the placement-id trap as a CLASS.
* `scope` on every row; resolution walks `scope → global`; duplication copies
  the scope's rows (generalising `scope_service._clone_nodes`, which already
  does exactly this for node config at `scope_service.py:425`).
* **Aspect by aspect, not big-bang:** `columns` first (it has tests by then),
  then `run_options`, then `hidden`, then edges/constants. Each aspect is its
  own commit with its own migration.
* **Migration policy:** every row is carried over and verified before the old
  table is dropped — no row is lost, no "remove" means delete. No dual-read
  shim: this is a beta, so each aspect is a clean break once its rows have
  moved.

This stage touches the canvas. It wants its own session and a GUI pass after,
with `docs/gui-manual-testing-todo.md` updated per aspect.

---

## Stage 6 — Origin, and making divergence visible

* Plumb `origin` through every run path: GUI run, MATLAB run, script run,
  replay. Each run records which surfaces it read.
* A script run reads source only (decision A). The canvas can therefore hold
  a statement the last run never used — so the node shows **"stated here, not
  used by the last run — it ran from source."** One line of UI; the
  `Decision` already carries the fact.
* `docs/gui-manual-testing-todo.md` gets the steps for that marker.

---

## Stage 7 — The remaining consumers (later, optional)

* `api/matlab_command.py:1415` drops its copy of the binding logic and
  renders the `RunPlan`.
* `scidb trace --intent` shows intent, fact and the `Decision`.
* Plot Studio's `VariantSet` pin becomes a statement with
  `aspect="variant_selection"` — same store, same precedence.

---

## Order, and why

1. **Stage 1** — vocabulary first, so the bugs get fixed against the shape
   they will keep.
2. **Stage 2** — the guards; cheapest change to every future diagnosis.
3. **Stage 3** — the two live bugs users hit today.
4. **Stage 4** — removes the place the bug lived.
5. **Stage 5** — the store; biggest blast radius, own session.
6. **Stage 6** — origin + divergence marker.
7. **Stage 7** — consolidation of the remaining consumers.

## The constraint that can invalidate the design

Resolution sits on the hot path of every run, every canvas render and every
export. It must be **batched** — resolved once per graph build, not once per
node — or it repeats the N+1 provenance trap. If a correct resolver cannot be
made batch-shaped, the model is wrong and Stage 5 should not start.

## Not doing

* Display intent (layout, value groups, hidden ports, hypothesis prose) stays
  exactly where it is.
* No write-back into `.py`/`.m`. Code export remains the only bridge between
  surfaces.
* No `derived_from` inheritance between hypotheses.
* No general framework for "state a layer cannot carry" beyond the six
  aspects named in the doc.

---

## Status — 2026-09-19, branch `refactor/intent-and-fact`

Built and committed, **no Python test has been run** (the user runs tests):

| stage | commit | what | verification needed |
|---|---|---|---|
| 1 | 27c4c109 | `scidb.intent` vocabulary + resolver; columns normalizer one owner | `scidb/tests/test_intent.py` |
| 2 | 14d83771 | write-side + read-side selector guards; bindings INFO line | `scidb/tests/test_selector_round_trip.py`, `tests/integration/test_dag_runs.py` |
| 3 | a60445d7 | aggregation rows now carry `__graph_var_bindings` (with selector) — identity changes for aggregation+selector calls only; name-scoped config match admits non-16-hex suffixes | the two former xfails + `scidb/tests/test_aggregation_with_variants.py`, `test_stat_leaves.py` |
| 4 | 1061612b | `_invocation.for_columns` + `run_options_label` + GUI read-only line. **Deviation:** NOT an identity term (selector already is) | `scidb/tests/test_for_columns_run_option.py` |
| 5 | 637386b1 | `_intent` table; `columns` graduated; rekey/copy; one-time import | `scistack-gui/tests/test_intent_store.py` |
| 6 | 1c9716d4 | `_run.origin`; ambient origin; MATLAB command labels; `mark_unused_intent` + amber chip/note | `scidb/tests/test_run_origin.py`, `scistack-gui/tests/test_unused_intent.py`, manual item 0 |
| 7 | 97af5ded | partial: `scidb runs` origin column | `scidb/tests/test_inspect_phase3.py` |

Stage 7 remaining (deliberately after a green run): MATLAB route's copy of
`build_run_inputs` (`api/matlab_command.py:~1415`) rendered from a
`RunPlan`; Plot Studio `VariantSet` pins as `aspect="variant_selection"`.

Stage 5 remaining aspects (one commit each): `run_options`, `hidden`,
edges/constants, `schema_location`. Stored scope is still `global` until
execution is scope-aware.
