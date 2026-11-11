# Plan: Stage 2 — `stat_` leaf nodes + `finalized` draft/record mode

Implements **D5** (stats leaves, csv-stats vocabulary) and **D3** (draft vs.
record via `finalized`) of `docs/claude/endpoints-viz-and-stats-design.md`.
Builds directly on the shipped `plot_` machinery (`foreach.py` Step 1.55,
`_make_plot_wrapper`) and on stage 1's variant auto-split.

## Semantics

### `stat_` detection and wrapping

- **Detection:** `fn.__name__.startswith("stat_")`, new Step 1.56 mirroring
  the `plot_` check at Step 1.55 (`foreach.py:356`). Prefixes are mutually
  exclusive by construction.
- **Wrapper** (`_make_stat_wrapper`, mirroring `_make_plot_wrapper` at
  `foreach.py:2661`, `functools.wraps` so hashing/metadata-injection resolve
  to the user's fn): the fn must return a **dict** (csv-stats' native return)
  or a ready JSON **str**; anything else → `TypeError` with a helpful message.
  The wrapper normalizes and returns a **JSON string** (D5: the stored record
  data), flowing through the normal lineage + save path exactly like `plot_`
  path strings — queryable VARCHAR record, full provenance, `skip_computed`.
- **Normalization** (own ~15-line helper; **scidb gains NO csv-stats
  dependency** — the integration is convention-level, any dict works):
  numpy scalars → native via `.item()` (same recipe as csv-stats'
  `convert_types`), then `json.dumps(..., sort_keys=True)`.
- **Strip the top-level `"date"` key** before serializing: csv-stats stamps a
  wall-clock timestamp inside every result, which would make identical reruns
  produce different stored bytes. The DB's own save timestamp is the time
  authority (reproducibility > redundancy).
- Combo-metadata injection enabled (like `plot_`), so `stat_` fns may accept
  schema keys as kwargs.

### `finalized` — draft/record mode (D3)

New `for_each(..., finalized: bool = False)` parameter. **Endpoint functions
only** (`plot_`/`stat_`); passing it with a non-endpoint fn logs a warning
and is ignored.

- **`finalized=False` (default — draft):**
  - No DB writes at all: internally forces the save phase off (the existing
    `save` gate in `_for_each_save_resolved` skips records, lineage, and
    graph in one place). The in-memory result table is still returned.
  - `stat_`: pretty-prints each result (header naming fn + combo, then
    `json.dumps(indent=2)`) — the interactive exploration mode. Any
    `PathOutput` input is resolved to **`None`** by the wrapper before
    calling the fn, so `ttest_dep(df, ..., filename=filename)` naturally
    disables csv-stats' PDF side effect (its `filename=None` contract).
  - `plot_`: figure IS still rendered to its `PathOutput` path (the user
    needs to look at it) — only the record is suppressed.
  - `skip_computed` has nothing to gate against in draft (no records) —
    drafts always re-run. Document, don't fight it.
- **`finalized=True` (record):** current shipped `plot_` behavior; for
  `stat_`, the JSON string is saved as the output record. A `PathOutput`
  input (optional for `stat_`, unlike `plot_` where it is required) is
  resolved normally — the user hands it to csv-stats for the PDF report —
  and the wrapper appends `"report_path": <resolved path>` into the stored
  JSON so the artifact is discoverable from the record.

**BREAKING (intentional, per D3):** `plot_` calls without `finalized=True`
become drafts — existing pipelines that relied on plot records being saved
must add the flag. `scidb/tests/test_plotting.py` gets `finalized=True` added
to its record/skip tests plus a new draft test.

### What stage 2 deliberately does NOT do

- No per-test variable-type machinery: users declare
  `class TTestResult(BaseVariable)` per test type themselves (documented
  convention). Cross-test summary tables project common fields
  opportunistically.
- No family-wise correction machinery: expressible today
  (`correct_pvalues` consuming `TTestResult.load(as_df=True)`); documented
  pattern only.
- No artifact metadata stamping (D4 — stage 3), no MATLAB (D7 — stage 4).
- csv-stats' `data_column="_"` all-columns loop: noted as a `for_columns`
  analog, not integrated.

## The paved-road usage this enables (doc example + integration test)

```python
class StepLengthTTest(BaseVariable):
    pass

def stat_step_length(df, filename):
    from csvstats.ttest import ttest_dep
    return ttest_dep(df, "session", "StepLength",
                     repeated_measures_column="subject", filename=filename)

for_each(stat_step_length,
         inputs={"df": StepLength,
                 "filename": PathOutput("reports/step_length_ttest.pdf")},
         outputs=[StepLengthTTest],
         finalized=True)        # grand aggregation: subjects+sessions fan in
```

Aggregation mode delivers the long-format DataFrame with schema columns —
exactly csv-stats' input contract (`group_column="session"`,
`repeated_measures_column="subject"`) — and stage 1's auto-split runs one
t-test per upstream variant group, each with its own provenance.

## Files

| File | Change |
|---|---|
| `scidb/src/scidb/foreach.py` | `finalized` param + non-endpoint warning; Step 1.56 `stat_` detection; `_make_stat_wrapper` (normalize/strip-date/serialize; draft print; PathOutput→None in draft; `report_path` in record); draft save-suppression for both endpoint kinds |
| `scidb/tests/test_stat_leaves.py` | new (see below) |
| `scidb/tests/test_plotting.py` | add `finalized=True` to record/skip tests; new draft test (file written, no record) |
| `docs/claude/plotting-leaf-nodes.md` | extend to cover both leaf kinds + `finalized` (or split out an endpoints doc section); drop "stat_ out of scope" |
| `docs/claude/endpoints-viz-and-stats-design.md` | mark D5 implemented, D3 implemented (stat_ + plot_) |

## Tests (`test_stat_leaves.py`)

1. **Record path:** `stat_` fn returning a dict, `finalized=True` → JSON
   string record saved with provenance; `json.loads` round-trips; numpy
   scalars converted; `"date"` absent from stored JSON.
2. **Draft default:** no versions/records exist after the run; result table
   still returned; `capsys` sees the pretty-printed JSON.
3. **PathOutput handling:** draft → fn receives `None` for the path param;
   record → resolved path received, `report_path` present in stored JSON.
4. **Return-type contract:** non-dict/str return → `TypeError`; plain str
   passes through.
5. **D1 integration:** `stat_` over an aggregated input with two upstream
   variants → two stat records, each with its group's branch_params (one
   t-test per pipeline decision — the stage 1 + stage 2 payoff).
6. **skip_computed:** `finalized=True` run twice → second run skips (call
   counter); draft runs never skip.
7. **`finalized` on a non-endpoint fn** → warning, normal behavior.
8. **csv-stats end-to-end (`pytest.importorskip("csvstats")`):** `ttest_dep`
   on a constructed subject×session DataFrame through the full
   for_each-aggregation path; stored JSON has `p_value`/`t_statistic` keys;
   PDF written when `finalized=True` with a PathOutput. Skips cleanly where
   csv-stats isn't installed.

Run: `pytest scidb/tests/test_stat_leaves.py scidb/tests/test_plotting.py -q`,
then the full `pytest scidb/tests -q` sweep (user runs; no Python here).

## Risks / watch items

1. **The plot_ draft default flips existing behavior** — biggest blast
   radius; the full-suite sweep should catch any other test relying on plot
   records implicitly.
2. **Draft PathOutput→None only applies to `stat_`** — a `plot_` draft still
   renders. Asymmetry is intentional (a stat's artifact is the *record*'s
   sidecar; a plot's artifact IS the deliverable) but must be documented.
3. csv-stats `dict_to_json` upstream bug (`json.dump` args swapped) means
   `.json` filenames crash inside csv-stats — the integration test uses
   `.pdf` or `filename=None` until fixed upstream.
4. Result dicts containing non-JSON-serializable values beyond numpy scalars
   (e.g. DataFrames in `post_hoc`?) — normalization falls back to `str()`
   via `json.dumps(default=str)`; verify against a real `anova1way` result
   in the importorskip test.
