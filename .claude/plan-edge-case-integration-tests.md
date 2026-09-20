# Plan — edge-case integration tests (get ahead of what breaks the GUI)

Drafted 2026-09-19. Status: **built (all sections), see the status block at the end.**

## Intent (user, 2026-09-19)

> "I do encounter edge cases that break my GUI. The intent of this suite is
> to get ahead of those, and confirm that odd/rare combinations of schema
> keys, including no schema keys, potentially redundant/impossible settings
> combinations, or a unique order of steps, will function correctly."

`tests/integration/` today is the happy path. This adds the space around it.
Same database fixture (the example pipeline over a subset), plus small
purpose-built additions where an edge needs data the example lacks.

## The contract every test asserts

The GUI never sees an uncaught exception. For any input the user can produce
through the panel, each layer must do exactly one of:

* return a result, or
* raise the *typed* error the layer above translates into a message
  (`RoleError` / `ValueError` for a spec, `NotFoundError` / `no_data` for a
  run, `SchemaKeyTypeError` for a key), **with a reason a person can act on**.

Anything else — `KeyError`, `IndexError`, `AttributeError`, a pandas error —
is a bug, and the test names the combination that produced it.

## A. Schema-key edge cases (scifor / scidb)

| # | case | what must hold |
|---|---|---|
| A1 | **No schema keys**: a step iterating nothing (`for_each(fn, as_table=True)` over every cycle) → one dataset-level record with every key NULL | it saves; it loads by no keys; a later step at cycle level reads it (broadcast to everything); it plots as one mark; the CSV has no key columns |
| A2 | **Non-contiguous keys**: a record at `subject + speed` (session skipped) — a calibration file per subject per speed | saves with session/trial/cycle NULL; a cycle-level step reads it broadcast on `(subject, speed)` only; `distinct_schema_combinations` reports it; the plot layer gives it depth 3, not 2 |
| A3 | **Deep key iterated alone** (`cycle=[]`, nothing else) | either the ancestors are discovered from the data or a clear error — never a Cartesian product over unrelated subjects |
| A4 | **Input finer than the iteration, no `as_table`** (cycle-level input, trial-level loop) | a typed error naming the input and the two levels, not a silent first-row pick |
| A5 | **Ragged data**: remove one trial's files for one subject | `for_each` reports `no_data` for that combo and completes the rest; the coarse-broadcast step still runs for the other combos; the plot draws the gap; the CSV omits the rows |
| A6 | **A key value that matches nothing** (`session=["week99"]`) | zero iterations, a summary line, no exception |
| A7 | **Redundant keys in the call** (`subject=[...]` given AND discovered by the PathInput) | discovery and the explicit list agree; a conflicting explicit value is reported |
| A8 | **Parameter changed between runs** (threshold 50 → 75) | a third variant; both older ones intact; the Variant axis lists three |
| A9 | **Function body changed between runs** | a code-version variant at the same location; `CodeIsLatest` flips; provenance shows both hashes |
| A10 | **Same variable from two producers** (a loader and a computed step both writing `TrialInfo`) | the ambiguity is reported at load / in the picker, not resolved silently |
| A11 | **Direct `.save()` beside `for_each` records** for the same variable | both load; provenance says "direct save" for one |
| A12 | **Mixed key spellings**: `"01"` and `1` for the same subject in one database | one clear error or one consistent key — never two subjects |

## B. Plot spec edge cases (scistackplot / scistackplotdb)

| # | case | what must hold |
|---|---|---|
| B1 | **Role-assignment sweep**: every one of the 4^5 = 1024 assignments of GROUP/FACET/ITERATE/COLLAPSE to the five keys, × {bar, box, line-on-1-D} on a tiny subset | `capabilities` always returns; if it says the kind is available, `resolve` succeeds and both renderers draw; if not, `validate` raises `RoleError` with a reason; `plot_data` returns or raises `ValueError`; **nothing else** |
| B2 | Everything ITERATE (nothing grouped) | one figure per record, one mark each |
| B3 | Everything COLLAPSE | a single mark; the CSV is one row per joint |
| B4 | Colour naming a non-grouping factor | `RoleError` with the factor named |
| B5 | COLLAPSE on the variant factor / on `Variable` | refused with the reason the panel shows |
| B6 | Four grouping layers (over `MAX_X_LAYERS`) | refused, with the cap in the message |
| B7 | A filter that empties the data | an empty figure (`row_count == 0`), not an exception; `plot_data` is an empty frame with the right header |
| B8 | A filter on a column the table does not have (stale spec) | ignored with a WARN, figure drawn |
| B9 | A spec whose roles name a key this table lacks (saved against a deeper variable) | the stale role is dropped, not fatal |
| B10 | `show_sample` naming a key that is not collapsed | ignored and logged |
| B11 | `plot_data(depth=...)` for a key that is not collapsed / not a key | `ValueError` naming the choices |
| B12 | Kind switches on one cached plan: bar → box → line → band → bar, and cell collapse on/off | every step draws; y limits recomputed where they must be |
| B13 | Pooled + show_sample; pooled + depth | consistent (one depth, points averaged the pooled way) |
| B14 | A variable with ONE record; a variable whose measure is all NULL | one mark / an honest "nothing to draw", never a division by zero |
| B15 | Nested x over ragged combinations (A5's data) | no hole reads as data; brackets still correct |
| B16 | Struct with a field a record lacks (A5 variant) | the panel is skipped for that record; the wide CSV has an empty cell |

## C. GUI service edge cases (scistack-gui)

| # | case | what must hold |
|---|---|---|
| C1 | `describe` of a string-valued / unplottable variable | `eligible: False` with a reason |
| C2 | `resolve_figures` with an invalid spec | `{"error": ...}`, no 500 |
| C3 | `figure_index` past the fan-out | clamped |
| C4 | save to an unwritable / nonexistent-parent path; an unsupported format | refused **before** the load |
| C5 | the DAG built from the example source: every step a node, every edge a real input | matches `pipeline.py`'s calls; a node's config round-trips |
| C6 | running a node from the GUI's run service with inputs missing (order of steps) | a failure message naming the missing variable, then success after the loader runs |

## D. Order of steps

| # | case | what must hold |
|---|---|---|
| D1 | A processing step run **before** its input's loader | `no_data` / typed error; the same call succeeds once the loader has run; no stale "computed" record left behind |
| D2 | A loader re-run after processing steps (files unchanged) | zero new records; downstream stays "up to date" |
| D3 | A loader re-run after ONE file changed | that record versioned; only its dependants go stale (node state), not the whole graph |
| D4 | A coarse step re-run after a fine step that reads it | the fine step's inputs are now stale — reported, not silently reused |

## Build order

1. **B1 first** — the sweep is one parametrised test and covers the most
   ground; it is where "odd combinations" live.
2. A1, A2, A5 (they need small extra data, built inside the test).
3. B2–B16, C1–C4 (spec-level, cheap).
4. A3–A12, D1–D4, C5–C6 (each is a pipeline scenario; some may expose
   behaviour that needs a decision from the user rather than a fix).

## Questions for the user

* Which of these have actually broken the GUI for you? Those go first, and
  I'd add the exact combination you hit as its own test.
* A4 and A10: is the desired behaviour an error, or a defined rule?
* Anything missing from the tables (MATLAB-side parity is out of scope here)?

## Built 2026-09-19 (all sections) — status after the first two runs

* `test_edge_specs.py` (B), `test_edge_schema.py` (A + D), `test_edge_gui.py`
  (C), `test_edge_features.py` (the gotchas and the leftover features:
  NaN round trip, schema-key data columns, dict-valued Parameter, empty
  Parameter, coarse-input prune, stacking, LocationFilter, `where=`,
  exclusions, Merge, show sample, zero-padded PathInput, violin/strip/
  spaghetti).
* The B1 sweep runs a deterministic eighth of the grid plus every uniform
  assignment and every one at/over the layer cap (142 of 1024), full grid
  under `SCISTACK_INTEGRATION_FULL=1`; one `capabilities` per assignment.
  The full grid took ~20 min.
* Findings fixed so far: `UnknownMeasureError`; stale roles dropped (neither
  table); wide CSV header under an emptying filter; `output_num` out of the
  latest-collapse key (one changed file = one version); the layer cap shared
  by `validate` and the capability report (`roles.layer_cap_reason`).
* Decisions pinned rather than changed: explicit key values are authoritative
  and missing files fail per combination (foreach Step 3).
* A3 / A4 / A10 / Merge-across-levels assert the contract only (xfail for
  Merge if the run reports failures) — behaviour still undecided.
