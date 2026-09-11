# Plot Studio fixes — 2026-09-11

Six items, in the order the user asked for them. Evidence throughout is
`/workspace/scidb.log` (session 12:24:14 – 12:28:31).

Decisions locked with the user before planning:

* **1-D roles** — gate the role menu by what the backend says is legal for this
  shape/kind, but keep `AGGREGATE` and `FREE` reachable on 1-D measures. They are
  what unlock the Band plot (mean line + error region) and "average the passes
  into one waveform"; removing them would delete both features. What changes is
  that illegal roles stop being offered and the labels become shape-accurate.
* **Pooling** — `variant_policy`, the enum and the checkbox are deleted, but
  pooling stays *reachable* as an explicit role in the Factors section. Revised
  by the user after the first pass: with 5 variants of a 1-D variable they want
  both the average and the mean + error band. The property to protect is not
  "never pool variants", it is "never pool them **silently**". See Stage 8.

---

## Stage 1 — Stop the duplicate compute (item 1)

**The problem, measured.** Log 12:27:39–12:28:10: four full resolves in flight at
once, `resolve_one` climbing 3.3s → 7.6s → 15.2s → 27.7s as they compete. Each
one re-melts, re-stacks and re-explodes the same 17.1M samples. The generation
counter at `PlotStudio.tsx:447` only drops stale *replies* — its own comment says
it cannot cancel the work, and defers to "the frame cache below". That cache
(`plot_service._sources`) is at the **variable-load** level only: `load_variable`
ran once (12:24:23, 1.0s) while `melted`/`stacked`/`exploded` repeat on every
single request.

Four changes, smallest first. Each is independently useful.

**1a. Memoize `get_table` on the source object.**
`plot_service._table_for` (`plot_service.py:694`) calls `source.get_table(...)`
fresh every time, and `plot_resolve` + `plot_capabilities` each call it per user
action — that is the doubled `melted 'FilteredEMG'` / `stacked [...]` pairs
visible on every action in the log. Add a small memo (LRU, 4 entries) keyed on
`(tuple(measures), x_measure, tuple(factor_variables))`, held on the `DataSource`
instance in `scistackplot/sources/base.py` so both `ScidbSource` and `CsvSource`
inherit it. Invalidation needs no new code: `plot_service.invalidate()` already
drops the whole source object, and the memo goes with it.

**1b. Memoize `_plan`.**
`reduce._plan` (`reduce.py:131`) is where the cost actually is — filters, variant
folding, `_explode_1d`, `_collapse_aggregates`, grouping. It depends on
`measures`, `x_measure`, `filters`, `variant_sets`, `level_groups`,
`factor_variables`, `roles`, `index_column` — and **not** on `kind`, `style`,
`facet` or `max_points`. Cache it (LRU, 2 entries — these frames are large) keyed
on that subset plus the table's identity. Changing plot kind, the facet grid or a
colour then costs nothing. 1a is a prerequisite: the memo can only key on table
identity safely once the same table object is being handed back.

**1c. Explode per figure, not per fan-out.**
`_plan` explodes the **whole** frame before grouping, then `resolve_one` builds
one figure from one group. The log shows it exactly: `exploded ... 17119200
sample(s)` followed by `downsampled 8906400 row(s)` — half the explode was thrown
away. With 30 subjects it would be 30x waste. Move `_explode_1d` and
`_collapse_aggregates` to after `_ordered_groups`, inside `_build_figure`.
The fan-out keys are schema columns that exist pre-explode, so grouping does not
need them. `scistackplotdb/tests/test_fanout_parity.py` is the guard rail here
and must still pass unchanged.

**1d. Client-side single-flight.**
`PlotStudio.tsx:447` fires a resolve per 180ms debounce tick regardless of
whether one is already running. Hold at most one in-flight resolve plus one
pending "latest" spec; when the in-flight one settles, fire the pending one if
the spec still differs. A drag across role dropdowns then costs two resolves (the
one already running, and the final state) instead of six. Pure frontend, no
protocol change, and it composes with the existing generation counter rather than
replacing it.

*Optional, only if 1a–1d prove insufficient:* a server-side supersede — client
sends a monotonic `request_seq`, `_h_plot_resolve` returns `{"superseded": true}`
without working if a higher seq has already arrived. Cheap, but it is a protocol
change and the four above should make it unnecessary.

**Logging (NOTE 2).** `Log.timer` around `_plan` stating cache hit/miss and row
count, so "why was this resolve slow" is answerable from the log alone. A cache
hit should log at DEBUG, a miss at INFO with the reason it missed.

**Tests.** `scistackplot/tests/test_resolve.py`: a plan cache hit returns an
equal plan and does not re-explode (assert via a spy on `_explode_1d`); changing
only `kind` hits the cache; changing `filters` misses it. `test_fanout_parity.py`
unchanged and passing proves 1c preserved the figure set.

---

## Stage 2 — Narrow the DuckDB lock around plot work (prereq for item 2)

`_handle_request` (`server.py:1274-1279`) acquires the DB connection around the
**whole** RPC, so a 27s resolve holds the DuckDB file lock for 27s — visible in
the log as `acquire_db_connection` at 12:27:39 and `release` at 12:28:10. This
already blocks MATLAB today, and it is exactly the thing the user does not want a
background save doing.

Plot work needs the database only while loading variable frames; everything after
that is pandas and matplotlib. Change:

* Add a `SELF_MANAGED_DB_METHODS` set in `server.py` (the `start_run` handler at
  `server.py:708` is the existing precedent for a handler owning its own
  connection) containing the `plot_*` methods.
* In `plot_service`, wrap **only** `get_source(...)` + `_table_for(...)` in
  `acquire_db_connection()` / `release_db_connection()`.
* With Stage 1a in place, a warm table memo means most resolves never touch the
  database at all.

Do the same for the FastAPI path (`api/plot.py` uses `Depends(get_db)`), or
document why it differs — standalone mode has no MATLAB contending for the file.

**Logging.** One INFO line per plot RPC stating how long the DB was actually
held, so the improvement is measurable rather than asserted.

---

## Stage 3 — Save: instrumentation + "Save current figure" (item 2)

**Why it failed.** `save_figure` (`plot_service.py:571`) calls `resolve(spec,
table)` — every figure in the fan-out (2 here), at full resolution
(`max_points=None`, deliberate). The interactive path builds **one** figure
downsampled to 20k and still took 25–27s. The client gives up at 30s
(`api.ts:133`, VS Code transport only; the standalone `fetch` path has no
timeout, which is why this only bites in the extension). Log lines 507–511 are the
save: one table build, `exploded ... 17119200`, **no** `downsampled` line, and
`[plot] saved` never appears.

**3a. Instrument first.** `save_figure` currently logs nothing until it succeeds,
which is why a slow save and a genuine hang are indistinguishable. Add:
`Log.timer` over the whole call; an INFO on entry naming the spec, figure count
and target path; per-figure INFO with row count and elapsed; per-figure elapsed
for `render_matplotlib` and for `savefig` separately (we do not currently know
which of the two dominates).

**3b. Split the operation in two.**

* `plot_save_figure` gains `figure_index` (same parameter `plot_resolve` already
  takes) and uses `resolve_one`. Saves the figure on screen. Still full
  resolution — that part of the docstring's promise is correct and stays.
* GUI: the existing "Save image" button becomes **"Save current figure"**, plus a
  **"Save all figures"** button when `figure_count > 1`.

Expected: one figure at full resolution is roughly half the failing call's work
and should land inside 30s for this dataset. It is not *guaranteed* to — which is
what Stage 4 is for, and why "Save all" is not the only thing moving to the
background.

**Tests.** `scistack-gui/tests`: `save_figure` with `figure_index` writes exactly
one file and names it after that figure's label; without it, writes all of them.

---

## Stage 4 — "Save all figures" as a background job (item 2)

Follow the existing run pattern exactly (`server.py:711-733`): the handler spawns
a daemon thread, returns `{"job_id": ...}` immediately, and progress arrives as
JSON-RPC notifications via `notify.notify` (`notify.py:28`), which the frontend
already consumes through `addNotificationHandler` (`api.ts:55`).

**The database is held for the load only.** With Stage 2 done this falls out
naturally: the job acquires, builds the table, releases, and *then* renders
figure-by-figure with no connection held. That is the user's stated requirement —
a long save must not lock the `.duckdb` file.

* Notifications: `plot_save_progress` (`job_id`, `done`, `total`, `path`) per
  figure, `plot_save_complete` (`job_id`, `files`, `elapsed`) at the end,
  `plot_save_failed` (`job_id`, `error`) on failure.
* GUI: a progress readout in the Plot Studio footer ("Saving 3 of 12…"), and the
  completed file list on finish.
* Cancellation: out of scope for this stage unless asked — the run service's
  cancel machinery is heavier than this needs and a save is bounded.

**Tests.** A job writes every file and emits one progress notification per
figure; a job that fails mid-way emits `plot_save_failed` and does not leave the
DB refcount elevated (that last one matters — a leaked refcount holds the lock
forever).

---

## Stage 5 — Role availability comes from the backend (item 3)

**Today** `ROLE_OPTIONS` (`PlotStudio.tsx:28-35`) is a static array: all six roles
are offered for every factor regardless of shape or kind. `roles.validate`
already refuses several of them — X on a 1-D measure (`roles.py:253`), X on a 2-D
measure (`roles.py:260`), X when `x_measure` supplies the axis (`roles.py:247`) —
so the user can pick an option and get an error where the option should simply
not have been there. That is the whole of item 3 and half of item 4.

`capability.py` already does this job for plot *kinds*: `available_plots` +
`why_unavailable`, rendered by the GUI as enabled/disabled options with tooltips.
Mirror it for roles (NOTE 3 — policy in the library, not in TypeScript):

* `available_roles(spec, table, factor) -> list[Role]` — the roles legal for this
  factor given the measure's shape, whether `x_measure` is set, and the
  single-assignment rule. Derived from the same conditions `validate` enforces,
  so the two cannot drift.
* `why_role_unavailable(role, spec, table, factor) -> str | None` — the tooltip,
  reusing `validate`'s existing message text.
* `role_label(role, shape) -> str` — shape-accurate labels. For `SERIES_1D`:
  "Average over" → **"Average into one line"**, "Replicates" → **"One line
  each"**. For `SCALAR` they keep today's wording. This is where the user's
  "Replicates is jargon" objection gets fixed.
* Report all three per factor in `factor_summary` (`capability.py:160`), which
  the GUI already renders from.

GUI: `ROLE_OPTIONS` stops being a constant and comes from `factor.roles`, with
unavailable entries disabled and carrying `reason` as the tooltip — the same
treatment the kind picker already gives.

**Per the locked decision:** on a 1-D measure `AGGREGATE` and `FREE` stay
available. They are legal, they are what `available_plots` keys BAND off
(`capability.py:53-57`), and hiding them would delete the Band plot.

**Variant factors specifically** must report `FREE` and `AGGREGATE` as
*available*, not filtered out — that is the control Stage 8 moves here from the
deleted checkbox. They carry a cautionary tooltip ("levels are different
pipeline variants, not replicates — combining them mixes results") rather than a
refusal. `available_roles` and `validate` must agree on this, which the
round-trip test below already enforces.

**The `Variable` factor is the exception** and reports `FREE`/`AGGREGATE` as
*unavailable* — averaging across variables is refused outright. See Stage 8's
"Aggregation must never cross variables".

**Tests.** `scistackplot/tests/test_core.py`: for each shape, every role reported
available is accepted by `validate`, and every role reported unavailable is
rejected by it. That round-trip is the property worth owning — it is what stops
the menu and the validator drifting.

---

## Stage 6 — "Grouping" section, scalar only (item 4)

The user's reading is right: a factor on `X` **is** a categorical grouping.
`xaxis.py` composes the levels into leaf positions with spacer categories,
explicitly "so box, violin, bar and strip all position themselves exactly as they
already do". A continuous x never comes from a factor — it comes from `x_measure`
or, for 1-D, the within-observation index, which is why `default_roles`
(`roles.py:60`) gives no factor the X role on 1-D data.

### Groups vs Grouping — merge them

The first pass proposed a new "Grouping" section alongside the existing "Groups"
section. The user asked how they differ. They are **adjacent, not identical**:

* Existing **Groups** makes factors *exist*. Two controls:
  `factor_variables` (`PlotStudio.tsx:1092`) joins another variable in as a
  factor — a subject-level `Condition` becomes a stim/sham column every row
  carries; `level_groups` (`:1103`) buckets an existing factor's levels into a
  derived one (`session` → `Phase`).
* Proposed **Grouping** *assigns* the x-axis role to factors that already exist.

So one creates and the other assigns, and a bucket defined in the first is what
you select in the second. But that is a distinction about implementation stages,
not about the question the user is asking, and two sections with near-identical
names is the wrong UI. **Merge them into one "Grouping" section**, reading top to
bottom as "what groups exist" → "which ones group the x axis":

1. Join a variable in as a factor (`factor_variables`, unchanged)
2. Bucket a factor's levels (`level_groups` + `BucketAdder`, unchanged)
3. Which factors group the x axis, and their nesting order

**Shape gating applies to part 3 only.** Parts 1 and 2 are useful for every shape
— a bucket can be a colour on a 1-D line plot — so the section is always visible
and part 3 renders only when `shape == 'scalar'` and `x_measure` is unset. (The
first pass had the whole section scalar-gated, which would have hidden bucketing
from 1-D users.)

Part 3 absorbs the existing "X grouping" section (`PlotStudio.tsx:1140`), which
today appears only once two factors already hold X — unreachable until the user
has found the role dropdown. `MAX_X_LAYERS` (3) is enforced by the backend
already.

`X` also comes out of the Factors role dropdown entirely. After Stage 5 it is
already hidden on 1-D and 2-D; removing it for scalars too leaves exactly one
place the x grouping is decided.

**Tests.** GUI-level: parts 1 and 2 render for a 1-D measure while part 3 does
not; part 3 renders for a scalar measure; checking two factors produces `roles`
with two X holders and an `x_layers` order matching the UI order.

---

## Stage 7 — Variant-row defaults (item 5)

**7a. Always seed one row.** `default_spec` (`roles.py:337`) seeds a variant row
only when `default_selection(table)` returns something. This project has no
variant axes (log 12:26:05: `variant_graph(RawEMG): 0 axis/axes`; `default
selection none`), so `variant_sets` is `[]` and the Variants pane opens with zero
rows. Seed unconditionally: `VariantSet(name=None, selection=selection,
variable=<primary measure>)`, with `selection` possibly empty.

This also removes a NOTE 3 violation — `PlotStudio.tsx:728` currently does this
same seeding in the GUI, lazily, but only when the user adds their *second* row.
That branch gets deleted once the backend always supplies row 0.

**7b. Name rows after their variable.** `set_name` (`variants.py:327`) returns the
variable name **only when the row has no selection**; with both, the variable
disappears and the label is the selection string. The log shows it: line 12:26:18
`{'FilteredEMG': 24, 'latest': 24}` — the second row plots RawEMG and its name
never says so. New rule:

* base name is the row's variable (or the primary measure when the row does not
  name one);
* a non-empty selection appends as a qualifier — `RawEMG · low_hz=20`;
* an explicit user name still overrides everything.

Drop the `CURRENT_VARIANT_NAME = "current"` constant (`variants.py:65`) — the
comment at `roles.py:332` already concedes it under-describes the row it names.
Per the no-deprecation rule, this is a clean break.

**Tests.** `scistackplot/tests/test_default_variant_pin.py`: a table with no
variant axes still opens with exactly one row, named for its variable; a row with
a variable *and* a selection names both; an explicit name wins.

---

## Stage 8 — Delete `variant_policy`, move pooling into Factors (item 6)

**Revised after user review.** The first pass deleted the flag *and* refused
pooling outright. That breaks a figure the user actually wants: with 5 variants
of a 1-D variable, "mean + error band across the variants" needs the `Variant`
factor left **FREE** to supply the replicates `available_plots` keys BAND off
(`capability.py:53-57`), and "average of the 5" needs **AGGREGATE**. Refusing
both would delete the feature, not just the checkbox.

The property worth keeping is **"never pool variants silently"**, not "never pool
variants". `validate` can already tell the two apart: a role the user picked is
in `spec.roles`; a role nobody picked is filled in by `complete_roles`.

* `spec.py`: delete `VariantPolicy` (`spec.py:100`), the `variant_policy` field
  (`spec.py:420`) and its `to_dict`/`from_dict` handling (`spec.py:503,543`).
* `reduce.py`: delete `_apply_variant_policy` (`reduce.py:333`) and its call
  (`reduce.py:149`). Its warning (`reduce.py:363`) does **not** disappear — it
  moves to the explicit-pooling path below, which is now the only way to get
  there.
* `roles.py:267`: the guard loses its `variant_policy` condition and gains a
  narrower one. A multi-level variant factor whose **completed** role is
  FREE/AGGREGATE:
  * **absent from `spec.roles`** (defaulted — nobody chose it) → refuse, with
    today's message minus the deleted-flag sentence (`roles.py:285`).
  * **present in `spec.roles`** (the user set it in Factors) → allow, and log the
    warning that different pipeline variants are being combined.

  Note `complete_roles` already defaults the synthetic `Variant` factor to COLOR
  (`roles.py:107-115`), so the defaulted-to-FREE case arises for real `Code:` and
  branch-param axes — which is exactly where silent pooling was the hazard.
* `capability.py:316,348`: drop the `"policy"` key.
* `__init__.py:73,115`: drop the export.
* GUI: delete the checkbox (`PlotStudio.tsx:1035-1043`), `setPooling`
  (`:700`) and the `variant_policy` field on the `Spec` interface (`:258`).
* Tests: `test_resolve.py:264` and `test_core.py:172` construct specs with
  `VariantPolicy.POOL` — rewrite them to set the role explicitly instead, which
  is the new spelling of the same intent; `test_variant_pin.py:707,732,816` pass
  `VariantPolicy.FACET` explicitly and just drop the argument. Add the pair that
  matters: a **defaulted** FREE variant factor raises, an **explicitly assigned**
  one resolves and warns.
* Docs: `scistackplotdb/README.md:62-63` and
  `docs/claude/plotting-library-design.md:179` describe the opt-in and need
  updating to point at the role rather than the flag. Older `.claude/plan-*.md`
  files are historical records — leave them.

**The user-visible result.** The checkbox is gone; setting the `Variant` factor
to "Average over" or "Replicates" in the Factors section is how you pool, which
is where the user asked for it to live. One switch instead of two — the same
reasoning that removed `VariantPolicy.PIN`.

### Aggregation must never cross variables

**Raised by the user, and it is a real latent bug — verified in the code, not
hypothetical.** Variants of `Var1` must never be averaged together with variants
of `Var2` just because both are levels of a factor called `Variant`.

What happens today with 3 variants of FilteredEMG and 3 of RawEMG:

* `apply_variant_sets` folds all six rows into **one** `Variant` factor with six
  levels (`variants.py:560`).
* `VARIABLE_COLUMN = "Variable"` (`variants.py:54`) is in the frame, but
  `_answered` (`variants.py:397-401`) deliberately strips it from the factor
  list — "which variable is what the row's NAME says".
* So `Variable` never reaches `roles`. `_collapse_aggregates` builds its groupby
  key from `roles.items()` (`reduce.py:438-442`), and a column with no role is
  not in `keep` — so `Variant = AGGREGATE` averages **EMG together with whatever
  the second variable is**. `FREE` + BAND is the same failure: the series
  grouping never sees `Variable`, so one mean ± band is computed across both.

It is unreachable today only because Stage 8 is what first makes FREE/AGGREGATE
legal on a variant factor. The two must therefore land together.

**The fix, as the user proposed it — a `Variable` row in Factors:**

* `_answered` adds `VARIABLE_COLUMN` to the answered set **only when the rows
  span exactly one variable**. With one variable the column is constant and
  offering it is noise; with 2+ it is a genuinely separate question from "which
  variant", and it becomes an ordinary factor with its own dropdown. The "only
  appears when 2+ variables are plotted" behaviour the user asked for falls out
  of this one condition, with no GUI change at all — Factors already renders
  whatever `factor_summary` returns (NOTE 3).
* `complete_roles` must **not** let `Variable` default to FREE — that is the
  silent pooling above. Default it to `FACET`, matching the user's case of "two
  mean + error band 1-D plots", one panel per variable. Note this needs its own
  branch: `VARIABLE_COLUMN` is appended to `factors` but **not** to
  `variant_factors` (`source.py:506-510`), so it is not a variant factor and the
  existing guard at `roles.py:273` does not cover it.
* `validate` gains a rule: when 2+ variables are plotted, `Variable` must hold a
  separating role — `X`, `COLOR`, `FACET` or `ITERATE`. `FREE` and `AGGREGATE`
  are refused, with a message saying why.
* `available_roles` (Stage 5) reports the same four as available for `Variable`
  and gives the reason on the other two.

Once `Variable` holds a channel role the rest is automatic: `_collapse_aggregates`
groups by every non-AGGREGATE factor, so averaging `Variant` collapses **within**
each variable and never across. No change needed there.

**Flagged for the user, implemented as instructed.** The rule as stated is
absolute — `Variable` may never be pooled. There is a case where pooling across
variables is meaningful (`LeftKneeAngle` and `RightKneeAngle` averaged into one
mean), which this refuses along with the meaningless ones. Implemented per the
instruction; say the word and it becomes the same explicit-opt-in treatment
`Variant` gets above.

**Tests.** `scistackplot/tests`: a two-variable spec with `Variant = AGGREGATE`
produces one mean **per variable**, not one overall (the regression this whole
section exists for); `Variable` unassigned raises rather than pooling; a
one-variable spec still reports no `Variable` factor at all.

---

## Running it

Per CLAUDE.md the user runs tests; one package per invocation (never span two
`tests/` dirs — bare `from conftest import` collides on collection):

```
cd /workspace && python -m pytest scistackplot/tests -x -q
cd /workspace && python -m pytest scistackplotdb/tests -x -q
cd /workspace && python -m pytest scistack-gui/tests -x -q
```

Frontend — **both** vite targets, or the committed `.tsx` fix is dead in whichever
one was skipped:

```
cd /workspace/scistack-gui/frontend && npm run build
cd /workspace/scistack-gui/extension && npm run build:all
```
