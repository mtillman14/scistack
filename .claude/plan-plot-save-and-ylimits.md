# Plan — save timeouts, save-time observability, legend parity, and y-limit scoping

Drafted 2026-09-11 from `/workspace/scidb.log` (sessions 12:24–12:54 and 15:26–15:29).

---

## What the log actually shows

| Time        | Line                                                      | Reading                                                                                    |
| ----------- | --------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| 12:54:11    | `[timing] resolve: line, TOTAL=1543.714s`                 | A **full-resolution** resolve of 2 figures / 17.1 M samples took **25.7 minutes**.         |
| 12:54:16    | `[plot] saved 2 figure(s)`                                | That save was the old **synchronous** path; it eventually succeeded.                       |
| 12:26–12:28 | `variant sets kept …: {'FilteredEMG': 24, 'latest': 24}`  | Old build: row 2 labelled `latest`. The GUI _and_ the save both said `latest`.             |
| 15:27:36    | `variant sets kept …: {'FilteredEMG': 24, 'RawEMG': 24}`  | New build: the `auto_label` variable fix is in and the label is right.                     |
| 15:28:04    | `plot_save_figure: held the DuckDB connection for 0.031s` | "Save current figure" — still a **request/response** RPC. 30 s client timeout in `api.ts`. |
| 15:28:56    | `[plot] starting save job 903b685e`                       | "Save all" — background job, correct shape.                                                |
| 15:29:00    | `exploded 1-D measure 'FilteredEMG': … 8906400 sample(s)` | Last line in the file: that job was **still resolving** when the log was captured.         |

Three conclusions:

1. **The single-figure save times out because one full-resolution figure takes
   ~12 minutes, not 30 seconds.** Stage 3 of the previous plan assumed one
   figure was answerable in one round trip. The 1543 s resolve says it is not.
2. **Nothing is logged while a save runs.** `Log.timer` emits _only on exit_
   (`scistacklog/__init__.py:357-371`), so a 25-minute save is 25 minutes of
   silence followed by one line. `resolve` is likewise one opaque block.
3. **The `latest` legend is already fixed.** The PNGs on disk
   (`FilteredEMG_pass_1.png`, `FilteredEMG_pass_2.png`) were written at
   **12:54** by the pre-fix build; the GUI showing `RawEMG` is the **15:27**
   build. The save-all launched at 15:28:56 never finished, so no post-fix PNG
   exists yet to compare. Both paths call the same `apply_variant_sets`, so
   there is no second bug here — but nothing _tests_ that the two renderers
   agree on legend text, which is why the drift was invisible. Stage 4 adds
   that test.

---

## Stage 1 — Say what is happening _while_ it happens

Problem: every timing line in the log is a post-mortem. A save that outlives the
user's patience produces no evidence at all.

- `plot_service.save_figure`: log on **entry** to each phase, not only on exit —
  `[plot] save <job>: loading table`, `… resolving figure 2/2 at full
resolution (24 rows, no downsampling)`, `… rendering figure 2/2`.
- `reduce._build_figure`: wrap in `Log.timer("build_figure")` with named phases
  — `explode`, `collapse_aggregates`, `facet_groups`, `panel_frames`,
  `summarize`, `order` — so each figure's breakdown lands as it completes
  instead of one `resolve` total after all of them.
- `reduce.resolve`: emit a start line (`resolving %d figure(s) …`) before the
  first `_build_figure`, and a per-figure completion line.
- Bump the per-phase `Log.debug` table in `Log.timer` to INFO when the total
  exceeds a threshold (a 25-minute operation should not need DEBUG to be
  explicable). Alternative if that is too broad: `Log.timer(..., loud=True)` on
  the two call sites that matter.
- Push `plot_save_progress` messages for the **resolve** steps too, not just
  after each file is written — the GUI currently says nothing until file 1 of 2
  exists, which is 12 minutes in.

Test: `test_plot_service.py` asserts the progress callback fires before the
first file is written, and `test_resolve.py` asserts the per-figure timer phase
names exist (guards the names against silent renaming).

### BUILT 2026-09-11 (pytest not yet run by the user)

Implemented, with one deviation: the "bump DEBUG phases to INFO above a
threshold" idea was dropped in favour of the stated alternative — an explicit
`Log.timer(..., live=True)`. A threshold cannot help the case that motivated
this, because a save that is *still running* has no total to compare against.

- `scistacklog`: `Log.timer(..., live=True)` narrates each phase on entry
  (`[timing] save_figure: resolve started — 2 figure(s)`) and on exit
  (`… resolve done in 771.402s`), plus `t.note(...)` for progress inside a long
  phase. `TOTAL=` still appears only on the summary line, so the MATLAB timing
  archives' grep target is unchanged. Quiet timers behave exactly as before.
- `reduce._build_figure`: `Log.timer("build_figure")` with phases `explode`,
  `collapse_aggregates`, `downsample`, `facet_groups`, `panel_frames`,
  `grid_layout`, `y_limits` — one summary per figure in every path, narrated
  per phase when asked. `panel_frames` also emits a per-panel heartbeat.
- `reduce.resolve` / `resolve_one`: a `plan` phase, a start line naming the
  figure count and whether it is full resolution, and `narrate=` /
  `on_figure(position, total, label)` — **fired as each figure starts**.
- `plot_service.save_figure`: `live=True`, `narrate=True`, a
  `[plot] rendering figure i/n` line before each render, and a stage-aware
  `on_progress(stage, done, total, detail)`.
- `plot_save_progress` gained `stage` (`"resolving"` / `"writing"`);
  `PlotStudio.tsx` reads it — "Resolving figure 1 of 2 at full resolution…".
  Both vite bundles rebuilt and verified to contain the string.
- Narration is **opt-in**, not implied by `max_points=None`: a pipeline `plot_`
  endpoint also resolves at full resolution, once per iteration.

New tests: `scistacklog/tests/test_log.py` (live vs quiet timer),
`scistackplot/tests/test_narration.py` (7 tests: phase names, ordering,
per-panel heartbeat, `on_figure`, and that the quiet paths stay quiet),
`scistack-gui/tests/test_plot_service.py` (resolve reported before any file
exists; indexed save reports its one figure; job messages carry `stage`).

## Stage 2 — No save runs on the RPC clock

- `start_save_job` takes `figure_index: int | None`; the worker forwards it to
  `save_figure`.
- `server.py`: one RPC, `plot_save_start` (`figure_index` present = one figure,
  `null` = the fan-out). `plot_save_figure` and `plot_save_all` both go away —
  beta, clean break, no aliases.
- `PlotStudio.tsx`: `saveFigures('current')` uses the same job path as `'all'`,
  including the "adopt the job id before awaiting" ordering. The notice becomes
  `Rendering figure 1 at full resolution…` → progress → done.
- `save_figure` stays a plain synchronous function for tests, codegen, and
  `for_each`; only the transport changes.

Test: `plot_save_start` with a `figure_index` returns a `job_id` immediately
(no blocking), and the completion message names exactly one file.

### BUILT 2026-09-11 (pytest not yet run by the user)

As planned, plus one addition the plan did not anticipate:

- `start_save_job` takes `figure_index`; `save_figure` is untouched and still
  synchronous for tests, codegen and `for_each`.
- One RPC `plot_save_start` and one route `POST /api/plot/save`, both carrying
  `figure_index` (absent/null = the fan-out). `plot_save_figure`,
  `plot_save_all` and `/api/plot/save-all` are **gone** — clean break.
  `SELF_MANAGED_DB_METHODS` follows.
- `PlotStudio.tsx`: both buttons call `plot_save_start`; both are disabled
  while a job runs; the notice reads "Saving this figure at full resolution…"
  then the Stage 1 stage-aware progress.
- **Added: the client names the job.** `job_id` is now an accepted parameter on
  both transports, and the panel generates the id and adopts it *before* the
  request leaves. The old code set `saveJob.current` from the response, so a
  save that finished while the response was in flight had its completion
  dropped — the panel would sit on "Saving…" with the file already on disk.
  That race was latent before and gets likelier as saves get faster (Stage 3).
  The failure path now releases the id too.

New/updated tests in `test_plot_service.py`: a single-figure save is a job and
reports `(resolving 1/1, writing 1/1)`; a client-supplied `job_id` is honoured
end to end; `figure_index` survives both transports; and the synchronous
methods are asserted **absent** (`plot_save_figure`/`plot_save_all` not in
`METHODS`, `/api/plot/save-all` → 404).

Both vite bundles rebuilt; `plot_save_start` present in each, `plot_save_all`
gone from each.

## Stage 3 — Why a full-resolution resolve costs 25 minutes

Stage 1 supplies the numbers; this stage fixes what they point at. The standing
hypothesis, from reading `reduce._panel_frame`:

```python
out[SERIES] = group[series_cols].astype(str).agg(" | ".join, axis=1).values
```

`.agg(..., axis=1)` is a **row-wise Python call over 8.9 M exploded rows**, and
the interactive path never pays it because `_downsample` runs _before_ the
panels are built (20 015 rows instead of 8 906 400). That matches the shape of
the evidence exactly — 3.8 s downsampled vs ~770 s per figure at full
resolution — but it is a hypothesis until the phase timers say so.

The architectural fix, not just a faster join: **series identity, and every
factor column, is constant within an original row.** Compute the series key
_before_ `_explode_1d` (24 rows, not 8.9 M) and let the explode carry it. Same
for the nested-x `leaf_key` list comprehension.

Also in scope once measured: `pd.to_numeric` over object columns post-explode,
and `_summarize`'s groupby on string keys (categoricals instead).

Test: a resolve-cost regression test — a synthetic 1-D table, assert the
per-row Python path is not taken (e.g. panel construction over N samples stays
within a wall-clock or call-count budget). Prefer call counting over wall clock
so it does not flake on CI.

### BUILT 2026-09-11 (pytest not yet run by the user)

**Built on the old evidence, not on a new measurement.** `scidb.log` has not
been rewritten since Stage 1 landed, so the `build_figure` phase breakdown does
not exist yet. What is known from the 12:28 run still points one way: the
explode is ~4s for 17.1 M samples (12:28:27 → 12:28:31), which leaves ~770s per
figure downstream of it, and the only per-row Python call downstream is the
series join. The next real save prints the breakdown and either confirms this or
names the phase that actually holds the time.

- New `reduce._composed_key(frame, columns, separator)`: factorize each column
  (one C-level hash pass), fold the codes into a dense combination id
  (re-factorizing after each fold, so the id cannot overflow int64 on a wide
  frame), stringify **once per distinct combination**, and take. The Python
  loop runs over combinations — 24 — instead of 8.9 million rows.
- Text still comes from pandas' own `astype(str)`, applied to the levels. That
  is what makes the strings provably identical to the join it replaces for
  every dtype pandas renders differently from Python.
  `use_na_sentinel=False` keeps NaN a level rather than a -1 sentinel that
  would index the label list backwards.
- **One deliberate difference, found by the tests:** pandas 3's `astype(str)`
  *preserves* missing values instead of writing `"nan"`, so the join this
  replaces raised `TypeError: sequence item: expected str instance, NAType
  found` on any factor column with a gap — a figure that died on real data
  rather than drawing it. A missing level is now a level, spelled
  `MISSING_LEVEL_TEXT`.
- Not fixed, and now pinned by a test: a factor level whose text *contains* the
  separator can compose to the same key as a different combination, drawing two
  traces as one line. The old join collided identically; no separator choice
  rules it out.
- Used for `__series` (post-explode, the hot one) **and** the nested-x leaf key,
  which composed the same way through `xaxis.leaf_key`.
- `codegen`: the generated `_series` line is now `str.cat`, not
  `.agg(' | '.join, axis=1)` — generated endpoints run on exactly the exploded
  data this is about. The nested-x join in generated code is left alone: it is
  emitted BEFORE the explode, so it runs on the small frame.

Deliberately NOT touched, for want of a measurement: `_explode_1d`'s
`map(lambda v: list(range(len(v))))` and its two `to_numeric` passes. They are
~4s of a 770s figure, and the Stage 1 `explode` phase now reports them.

New tests (`scistackplot/tests/test_composed_keys.py`): equivalence with the
old join across mixed dtypes, single columns, repeated levels, missing values,
an empty frame and a level containing the separator; agreement with
`xaxis.leaf_key`; and the call-count guard — a factor level that counts its own
`__str__` calls, asserting the composition stays under rows/10 (the old form
lands at one per row). The level type is deliberately not a `str` subclass:
pandas 3 would infer its own string dtype and the budget would stop measuring
anything.

## Stage 4 — Legend parity, locked down

No production change expected (the fix landed at 15:2x). Add the missing guard:
one test resolves a two-variant spec and asserts the **matplotlib legend
entries** and the **plotly legend entries** are both exactly
`["FilteredEMG", "RawEMG"]` — i.e. `variants.set_name`'s output, reached through
both renderers. The drift the user saw was only visible by opening a PNG; this
makes it visible in `pytest`.

### BUILT 2026-09-11 (pytest not yet run by the user)

No production change — as expected, both renderers already label colour groups
from `str(level)` off the shared `color_groups`, so the only thing missing was
the assertion. `scistackplot/tests/test_legend_parity.py`, three tests on a
two-variable stacked table (one variant row per variable — the user's figure):

- the matplotlib legend, the plotly legend and `variants.set_name` all read
  `["FilteredEMG", "RawEMG"]`;
- a row the user typed a name into reaches both legends verbatim, with neither
  renderer falling back to the column's own level text;
- both renderers agree with `render.base.legend_levels` — pinned to the shared
  definition rather than to each other, so a third backend (MATLAB) stays a new
  leaf instead of becoming a third opinion.

## Stage 5 — Y-axis limits at any scope

### Today

`reduce._build_figure` computes **one** `y_limits` per figure, over that
figure's panels, only when `spec.facet.share_y` (default `True`, no GUI control
at all). So: all facets of a figure share limits, figures differ only by their
own data, and nothing else is adjustable.

### The model

Y limits are **split by a chosen set of factors**. Call it `scope`:

| Scope                   | Meaning                                                                 |
| ----------------------- | ----------------------------------------------------------------------- |
| `[]` (nothing selected) | one limit for the **whole dataset** — every panel of every figure       |
| `[subject]`             | one limit per subject; all that subject's facets share it               |
| `[subject, ColName]`    | per subject **and** per facet — true autoscale to what is on that panel |

Plus a manual override: explicit `min`/`max` win over any computed scope.

**Invariant:** `scope ⊆ (ITERATE factors ∪ FACET factors)`. Those are the only
factors that separate panels; splitting on a COLOR or FREE factor would ask one
panel to have two y ranges. The GUI offers exactly that list, and the backend
drops anything else with a warning rather than silently ignoring it.

### How the limits are computed (exactly, without a 25-minute pass)

A new `scistackplot/ylimits.py`, one entry point:

```python
limits_by_scope(table, spec, scope) -> dict[tuple, tuple[float, float]]
```

- **Non-aggregating kinds** (line/scatter/strip/box/violin): the drawn extent is
  the raw extent. Compute per-cell `np.min`/`np.max` over the **unexploded**
  arrays and group by `scope` — 48 rows, effectively free. No explode.
- **Aggregating kinds** (band/bar with an error band): the drawn extent is
  `centre ± spread`, so the reduction is needed — but only its extremes. One
  explode plus one groupby on `(scope…, index, colour)`; no panel frames, no
  series strings, no sorting. One pass for the entire fan-out.
- Cached on the plot service beside the loaded table, keyed by the spec fields
  that affect reduction (`measures`, `roles`, `kind`, `aggregate`,
  `variant_sets`, `filters`) plus `scope` — so paging through a 30-subject
  fan-out computes it once.

### Plumbing

- `PlotSpec.y_axis: YAxis(scope: list[str], min: float|None, max: float|None)`.
  `FacetOptions.share_y` **goes away** — it is `scope` with the facet factors
  excluded, and keeping both would be two controls for one decision.
- `Panel.y_limits` becomes the authority; `ResolvedPlot.y_limits` stays as the
  figure-level value **only when every panel agrees** (renderers and the GUI
  already read it).
- `render/mpl.py`: `sharey=` becomes _derived_ — `True` only when all panels in
  the figure share one limit, else `False` with per-axes `set_ylim`. (Sharing
  also hides inner tick labels, which is right in exactly that case.)
- `render/plotly_.py`: per-panel `layout[y_key]["range"]`; `matches` only under
  the same derived condition.
- `resolved.to_dict`: per-panel `y_limits` in the payload.
- `codegen.py`: emit the limits. Generated code currently ignores `share_y`
  entirely (no `ylim`/`sharey` anywhere in it), so exported figures already
  disagree with the GUI on this — fixed here.

### GUI

A **Y axis** section in the Plot Studio sidebar:

- `Separate limits by:` — a checkbox per eligible factor (ITERATE + FACET),
  with the two ends named: nothing checked = "same limits everywhere", all
  checked = "autoscale each panel".
- `Min` / `Max` boxes for the manual override, blank = computed.
- One line of feedback under it, from the backend: `y limits: -0.42 … 0.61
(per subject)` — so the number on the axis is traceable to a rule.

### Tests

`scistackplot/tests/test_ylimits.py`:

- scope `[]` → every panel of every figure gets identical limits;
- scope = iterate factors → limits differ across figures, identical within;
- scope = iterate + facet → each panel's limits bracket exactly its own data;
- manual min/max beats every scope;
- a band plot's limits include the error band, not just the centre line;
- an ineligible factor (COLOR/FREE) in `scope` warns and is dropped;
- mpl and plotly apply the same numbers (renderer parity, same shape as Stage 4).

### BUILT 2026-09-11 (pytest not yet run by the user)

Built as designed. Deviations and findings, all deliberate:

- **`YAxis(scope, minimum, maximum)`**, not `min`/`max` — those shadow builtins
  in every comprehension that touches them. `FacetOptions.share_y` is gone; an
  older saved spec still loads (the field is simply ignored) and gets the new
  default, which is what `share_y=True` meant for a single figure anyway.
- **The limits are computed in `_plan`, not in `_build_figure`** — once for the
  whole fan-out. That is forced by the empty scope: "one range everywhere" spans
  figures `resolve_one` deliberately never builds, so it cannot come off the
  figures. `scistackplot/ylimits.py` computes it off the **table** instead: a
  min/max over unexploded arrays for the non-aggregating kinds (24 numpy
  reductions, no explode), and one groupby for BAND/BAR where the drawn extent
  is `centre ± spread`.
- `y_axis` is deliberately **in** the plan cache key, so changing a scope
  rebuilds the plan. It is a checkbox and two boxes, not a dragged slider.
- **`sharey` and plotly's `matches` are now DERIVED**, from whether the panels
  ended up agreeing. This was the half most likely to go wrong silently: both
  tie axes together, so a per-panel range would be overwritten by whichever
  panel drew last. `ResolvedPlot.y_limits is None` is the signal, and
  `render.base.shares_y_axis` is the one rule both renderers read.
- **Found while wiring it:** `shows_y_labels` hid inner tick labels
  unconditionally. On independently-scaled panels that is a lie — a grid at
  different scales, numbered down the left column only, reads as one scale. Now
  gated on `shares_y_axis`.
- **codegen bakes a literal `g.set(ylim=...)`** for cross-figure scopes, and
  emits `facet_kws={"sharey": False}` for per-panel ones (seaborn shares y by
  default, so silence there would have meant a different figure). Deliberately
  NOT scifor's `share_limits`, which maps onto `scope` exactly — the rationale
  is recorded in `_y_limit_plan`'s docstring: a literal reproduces the approved
  figure, `share_limits` recomputes at run time and would silently rescale a
  recorded artifact when data is added. Worth revisiting if a living endpoint
  is ever wanted over a reproducible one.
- GUI: a **Y axis** section — a checkbox per eligible factor (iterate + facet
  only), Min/Max boxes whose blank means "compute it", and a line naming the
  rule *and* the numbers it produced. `LimitInput` holds partial text so a
  half-typed `-` or `1e` does not snap the axis, and an empty box reads as
  `None` rather than `Number('') === 0`.

New `scistackplot/tests/test_ylimits.py` (17 tests) covers all seven planned
cases plus: a global scope holding for a figure built alone via `resolve_one`
(the whole reason limits come off the table), an inverted manual range being
ordered rather than flipping the axis, one end pinned with the other computed,
`""` reading as "compute it", and a pre-`YAxis` spec still loading.

---

## Order and dependencies

1. **Stage 1** (observability) first — it is what makes Stage 3 provable.
2. **Stage 2** (no RPC clock) immediately after; it makes the tool usable today
   even while the resolve is still slow.
3. **Stage 3** (performance) once the phase timings name the culprit.
4. **Stage 4** (legend parity test) — independent, small.
5. **Stage 5** (y limits) — the largest, and the only one that changes the spec
   format. `share_y` removal touches saved specs; a spec written before this
   stage loses nothing (`share_y=True` ≡ empty scope restricted to facets).

## Decisions (user, 2026-09-11)

- **Empty scope = global across the whole dataset** — one y range for every
  panel of every figure. This _changes_ today's behaviour (per-figure limits);
  "per figure" is now spelled by checking the ITERATE factors.
- **Performance before the feature**: Stages 1-3 land first, then 4, then 5.

### Other (from user)

1. When saving all figures, select a folder not a file.
2. After saving all finishes, update the UI. Right now permanently says "Saving..."
3. Support saving to png/svg/eps/fig?

#### BUILT 2026-09-11 — items 1-3 (`.fig` excluded per user)

**1. Folder for a fan-out.** `path` may now name a file or a folder, decided by
`plot_service._destination`: an existing directory, or a path with no suffix, is
a folder. The rule is **additive** — every caller that passed a filename keeps
its exact behaviour, so no existing test changed. Files inside a folder are
`<y measure>_<figure label>.<format>`, which is what the filename-plus-suffix
scheme already produced. The existence check comes first and is load-bearing: a
real folder can carry a dot (`~/analysis.v2`), and reading `.v2` as a format
would turn a chosen destination into a guess. VS Code gets a new
`pick_save_folder` (`showOpenDialog`, `canSelectFolders`); the browser prompt
asks for a folder path.

**2. "Permanently says Saving…" — already fixed by Stage 2**, and confirmed by
reading rather than assumed. The cause was the job-id race: the panel set
`saveJob.current` from the *response*, so a job that finished while the response
was still in flight had its `plot_save_complete` rejected by the id filter and
`saving` never cleared. Stage 2 made the client mint and adopt the id *before*
the request leaves. The worker already pushes exactly one terminal message on
every path (success, refusal, unhandled exception — `test_a_thread_that_dies…`
covers the last). What *was* still poor is the completion text: 30 full paths
sharing one folder pushed the one fact that matters off the end. Now one file
names itself and many name the count plus the folder, from a new `directory`
field on the result and on `plot_save_complete`.

**3. Formats.** `image_format` threaded through both transports and the job,
validated against `FigureCanvasBase.get_supported_filetypes()` — not a
hard-coded list.

The validation sits at the **literal top** of `save_figure`, before the database
is touched. It was first written after the resolve, which the test caught: a bad
format still cost the full twelve minutes of reduction before refusing, and the
docstring claiming "before any rendering" was true but beside the point. Nothing
about a format depends on the spec, so nothing about the spec is loaded to check
it. The test now asserts the ORDER — `resolve`, `resolve_one` and
`render_matplotlib` are all monkeypatched to record, and must record nothing. `describe` returns `image_formats`
so the GUI dropdown offers exactly what the save accepts. The dropdown is the
single source of truth: it builds the default filename and is the only filter
the VS Code dialog offers, so the two cannot disagree. `.fig` is excluded by
construction — matplotlib has no MATLAB writer, and a test asserts it never
appears in the offered list.

Tests added to `test_plot_service.py`: folder naming, a suffix-less path that
does not exist yet, a dotted folder name, a filename still meaning a file, all
four formats round-tripping to disk, format-beats-suffix, an unwritable format
refused before any rendering, the offered list matching matplotlib, and the job
reporting its folder. All three build artifacts rebuilt (both vite targets plus
`dist/extension.js`, which `plotPanel.ts` compiles into).
