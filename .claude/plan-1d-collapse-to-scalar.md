# Plot Studio: scalar plot kinds for 1-D variables (collapse to a centre)

**Status:** approved and BUILT 2026-09-14 (see the build log at the end). Python
tests written but NOT run — hand them to the user.

## The ask

Scatter, Strip, Box, Violin and Bar+error are offered only for scalar measures.
They should work for 1-D vector-valued measures too, by first reducing each
vector to one number (a measure of centre). A per-trial vector of step lengths
becomes one mean step length per trial, and the violin is then drawn across
trials exactly as it would be for a scalar variable.

So: **collapse 1-D to 0-D, then treat the measure as an ordinary scalar.**
Nothing about the scalar path changes.

## Decisions taken with the user (2026-09-14) — do not re-litigate

1. **The plot kind implies the collapse.** There is no toggle. A scalar-only
   kind selected on a 1-D measure means "collapse each vector first". This is
   what makes the feature unable to enter a contradictory state (`collapse=on`
   with `kind=line` cannot be expressed).
2. **Mean and median only.** Reuses the existing `Statistic` enum. No new
   vocabulary, and min/max/sum can be added later without a migration.
3. **Choosing a scalar kind re-defaults the roles** when — and only when — the
   roles are still the untouched 1-D defaults (every schema key on ITERATE,
   from the 2026-09-13 opening-cost decision). Otherwise box/violin would be
   permanently greyed out on a freshly opened 1-D variable ("needs replicates")
   and the first click would appear to do nothing.

## Design

### Where the collapse happens

`collapse.apply_collapse(spec, table) -> LongTable`: a **derived table**, the
third of the same family as `variants.apply_variant_sets` and
`groups.apply_level_groups` (`docs/claude/synthetic-factors.md`). It maps the y
measure's column from one array per row to one float per row and flips its
`MeasureInfo.shape` to `SCALAR`.

That is the whole feature. Everything downstream — `validate`, `complete_roles`,
`reduce`, `ylimits`, both renderers, the y-limit scope, the range filter, nested
x grouping — already handles a scalar measure and needs no knowledge that the
column was ever 1-D. Concretely this is why the derived-table form was chosen
over threading an "effective shape" through fifteen call sites: the alternative
adds a parameter to every function that asks `table.shape_of(...)` and leaves
each of them free to disagree.

Applied in `reduce._build_plan_timed` right after `apply_level_groups` and
**before** `validate` / `complete_roles` / `apply_filters`, so:

- `validate` sees a scalar measure and therefore permits a factor on X and
  nested x layers — the checks that currently refuse them for a 1-D measure are
  correct and stay untouched;
- a range filter on the measure filters the **collapsed** value, which is the
  only thing a user could mean;
- `y_extents` runs over 419 floats instead of 174 M samples.

### The reduction itself

`series_stats.collapse_cells(arrays, statistic) -> np.ndarray` — `np.nanmean` /
`np.nanmedian` per cell over `cell_array`, honouring the package's pinned pandas
semantics (NaN-skipping; an empty or all-NaN cell yields NaN).

Deliberately **not** a new `Reducer` protocol method. The protocol exists for
reductions that have two genuinely different implementations (a grouped
per-position statistic over an exploded frame vs. over padded blocks); a
per-cell mean has one implementation and both reducers would call the same
numpy. Adding a seam here would create a second definition to keep in sync for
no gain. It stays numpy-over-loaded-cells, per `project_plot_at_scale`.

Rows whose collapse is NaN are **kept as NaN**, not dropped — dropping would
silently change level counts the factor summary reports — and counted in a WARN.

The pre-exploded case (`MeasureInfo.exploded`, a source that supplied a real
index column) collapses by `groupby(every other column).agg(statistic)` instead.
No shipped source produces one today; it is covered by a test rather than left
to raise.

### Capability reporting

`capabilities()` needs **both** tables:

- the kind list is computed from the **raw** shape with `collapsible=True`, so
  `available_plots(SERIES_1D, roles, collapsible=True)` returns
  `[LINE, BAND?] + [SCATTER, STRIP, BOX?, VIOLIN?, BAR?]`. This is the part that
  matters: computing it from the collapsed table would make LINE and BAND
  vanish the moment a violin was selected, stranding the user on the scalar
  kinds with no way back;
- roles, factors, grouping and the reported `shape` come from the **collapsed**
  table, so the panel's wording, its range filter and its nested-x offer all
  describe the figure that will actually be drawn.

New fields in the report: `raw_shape`, and a `collapse` block
(`{applies, active, statistic}`) for the badge and the dropdown.
`why_unavailable` learns the same `collapsible` flag so a scalar kind on a 1-D
measure never reports "Not available for a 1d measure"; the replicate rule still
applies unchanged and is still the reason box/violin can be refused.

### Re-defaulting roles on a kind change (decision 3)

`roles.roles_for_kind(spec, table, kind) -> dict | None`: returns the roles that
kind should open with, or None to keep the current ones. The rule is "only if
you have not touched them": it fires when `spec.roles` equals `default_roles`
under the *current* effective shape, and then returns `default_roles` under the
*new* one.

Delivered to the GUI as an optional `roles` on each entry of
`capabilities.kinds`, so `setKind` stays **synchronous** — it applies a value
that is already in hand rather than making a round trip that would race the
resolve queue and could stomp a role the user set in the meantime. The GUI
applies it only when the capability report belongs to the current spec key
(`capsSpecRef`, already tracked). Policy stays in scistackplot (NOTE 3); the
panel does a lookup.

### Export

`codegen` emits the collapse into the generated seaborn function, before the
filter lines and instead of the explode block:

```python
# 1-D measure: one value per record (mean of each vector)
df['StepLength'] = df['StepLength'].map(
    lambda v: float(np.nanmean(v)) if v is not None and len(v) else float('nan')
)
```

with `import numpy as np` added to the generated imports. `generate_plot_function`
passes the **effective** shape to `_preamble` / `_plot_call` / `_x_expression` /
`_nested_x_layers` / `_x_is_categorical`, so the exported figure is the previewed
figure — the failure this whole package is arranged to prevent. Covered by an
execute-the-generated-function parity test, not by asserting on the text alone.

### GUI

- Plot type list unlocks on its own (it renders `capabilities.kinds`).
- A `Collapse 1-D  [Mean|Median]` dropdown directly under the Plot type list,
  shown only when `capabilities.collapse.applies`.
- The shape badge reads `1d → scalar (mean)` while a collapse is active, so the
  figure never silently claims to be showing the vectors.
- `setKind` applies `kinds[i].roles` when present (see above).
- Both vite bundles rebuilt (`project_frontend_bundle_rebuild`).

## Logging and diagnostics (CLAUDE.md NOTE 2)

- `apply_collapse` logs INFO once per resolve: measure, statistic, rows, total
  samples consumed, elapsed, and the count of empty/all-NaN cells at WARN.
- A `collapse_1d` phase in the existing `_build_plan_timed` timer, so the cost
  appears in the phase breakdown already printed for every resolve.
- The INFO line is what distinguishes "the violin is empty because the vectors
  are empty" from "because the filter removed everything" without a debugger.

## Staging

1. **Spec + collapse.** `PlotSpec.collapse_statistic`, `SCALAR_KINDS`,
   `collapse.py` (`collapses`, `effective_shape`, `apply_collapse`),
   `series_stats.collapse_cells`, logging. Tests: values, ragged cells,
   empty/all-NaN, pre-exploded path, no-op on a scalar measure, a stacked
   two-variable frame (variant rows over two 1-D variables collapse together).
2. **Capability + roles.** `collapsible` through `available_plots` /
   `why_unavailable` / `capabilities`, `roles_for_kind`. Tests: scalar kinds
   offered for 1-D, LINE still offered while a violin is selected, replicate
   rule intact, X-on-a-factor accepted once collapsed, re-roll fires only on
   untouched roles.
3. **Reduce.** Wire into `_build_plan_timed` + timer phase. Test the user's
   example end to end: a 1-D per-trial measure, `trial=FREE`, violin — drawn
   values equal the per-cell means, y limits computed from the collapsed
   scalars.
4. **Codegen.** Emit the collapse, skip the explode, effective shape through the
   helpers; execute-the-generated-function parity test.
5. **GUI.** Report fields through `plot_service`, dropdown + badge + `setKind`
   in `PlotStudio.tsx`, rebuild both bundles. `scistack-gui/tests/test_plot_service.py`
   covers the new report fields.

## Deliberately out of scope (v1)

- **Relational plots.** When `x_measure` is set the kind list is already
  `[SCATTER, LINE]` and the pairing is per-sample; collapsing both measures to
  one point per record is a coherent thing to want but a different feature, so
  the collapse does not fire while `x_measure` is set.
- **2-D measures.** A matrix collapses to a scalar only through a choice of two
  axes; nothing here applies.
- **Pushdown.** The collapse runs in numpy over the loaded frame. DuckDB
  `list_avg` in the SELECT would avoid transporting the samples at all — a real
  win on RawEMG-sized data and the obvious follow-up — but it is an optimization
  of a measured cost, and the cost has not been measured yet. The INFO line
  above is what will measure it.

## Test commands (user runs these)

```
pytest scistackplot/tests
pytest scistackplotdb/tests
pytest scistack-gui/tests/test_plot_service.py
```

(one package per invocation — `project_pytest_one_package_at_a_time`)

---

## Build log (2026-09-14)

All 5 stages built. Tests written, **not run** (no Python in this environment —
the venv binaries are the host's architecture). Both vite bundles rebuilt and
the frontend typechecks clean.

Deviations from the plan above, all discovered while building:

1. **The plan cache had to learn about the collapse.** `kind` is in
   `reduce._PLAN_IRRELEVANT_FIELDS` — presentation, so box and violin share one
   plan — and that is exactly wrong once the kind decides whether the measure is
   collapsed. The fix keeps the intent: the DECISION (`collapses(spec, table)`)
   goes into the cache key rather than the kind, so box/violin still share a
   plan and line/violin of a 1-D measure can never share one. Without this a
   violin would have been drawn from a line's plan. `_plan_cache_miss_reason`
   names this case, or "identical spec, cache miss" reads as a cache bug.

2. **`codegen._y_limit_plan` collapses too.** Baked-in y limits are computed at
   generation time from the table; for a collapsed measure that has to be the
   range of the per-record means, not of every sample — otherwise the exported
   figure's axis is off by the whole within-record spread.

3. **The figure says so.** `collapse_note` is taken before the collapse and
   carried in `ResolvedPlot.fanout_notes`, and the generated docstring says the
   same thing. A violin of trial means and a violin of raw samples look
   identical.

4. **No new RPC** (as designed) — the re-rolled roles ride on
   `capabilities.kinds[i].roles`, applied by `setKind` only when
   `capsSpecRef.current === specKey`, so a stale suggestion cannot overwrite a
   role the user set in the meantime.

Known cost, deliberately not optimized: the collapse runs twice per spec change
(capabilities + resolve). See the doc's scope section.

### Follow-up after the first test run (2026-09-14)

`test_the_kind_list_carries_the_roles_a_scalar_kind_would_open_with` failed:
`assert 'free' in dict_values(['x', 'color'])`. The GUI fixture has exactly two
factors, so the scalar default is `subject=X, session=COLOR` — one point per
combination. The assertion was wrong AND so was the behaviour: the re-roll
handed back roles that still could not draw a violin.

Fixing it surfaced the other half of the same loop: from the opening state
box/violin are refused for want of replicates, so the kind whose selection would
have supplied them could not be clicked in the first place.

Both fixed, both documented in `docs/claude/measure-shape-and-collapse.md`:

- `roles._with_replicates_for` frees the **innermost** plain factor when the
  chosen kind is a distribution kind and nothing is FREE. Innermost because its
  levels are replicates of each other; variant factors are never eligible
  (a FREE variant is refused by `validate`).
- `capability.capabilities` judges each kind against the roles IT would bring,
  and derives `available` from the per-kind entries so the two cannot disagree.
