# What a variant row selects

*Written 2026-10-15, before building `.claude/plan-saved-plot-version-pins.md`.
Read alongside `plot-variant-rows.md` (what a row is) and `saved-plots.md`
(what a saved plot stores).*

## The question this answers

Plot Studio has one or more variant rows (`scistackplot.VariantSet`). Every
part of the stack that touches rows has to know **which data rows a given
variant row draws**:

- the figure (`variants.apply_variant_sets`);
- the row counts, span report and banner (`capability.variant_summary`);
- the factor list ("which columns has the row already answered?",
  `variants._answered`);
- the export (`codegen.variant_expression`).

Until 2026-10-15 these answered differently. For an empty row 0, the figure
used the source default ("current"), but the banner used *everything*. The
result was a banner saying "CosmedData mixes v1 | v2" above a figure that
drew v2 only. **One owner now answers the question:
`scistackplot.variants.row_mask`**, built on `effective_selection` and
`frozen_mask`. Everything else consumes it.

## The rule, in order

For one row of a spec over one table:

1. **Frozen versions win.** `VariantSet.frozen` is `{"Code:fn": ["v1", …]}`.
   It is filled in by `scistackplotdb.saved` when a saved plot is reopened,
   by translating the function hashes stored at save time into today's
   ordinals. For each frozen column, the row keeps only those ordinals and,
   **within each schema location**, only the highest of them. That is
   "latest as of the save", reproduced exactly even when the save spanned
   v1/v2 across locations. While a row has frozen columns, its own latest flag
   and its selection keys for those columns are ignored, because they would
   contradict the freeze: "current" AND "v1" selects nothing.
2. **The selection is applied** via `variant_set_mask`, so `"latest"`
   resolves as described in `plot-variant-rows.md` §2.
3. **Unanswered code defaults to current.** A row whose selection names no
   function axis (`Code:`/`Run:`) and not the latest flag has the source's
   `default_pin` (`{CodeIsLatest: True}`) added. This is what keeps "no
   pooling unless the user asked" true. A row that only says `low_hz=20`, or
   says nothing (an empty row 0), draws the current code, never every
   version at once. **Several versions are pooled only when the row's
   selection explicitly names them** (a list on a `Code:` axis, picked in the
   Variants popup).
4. **The row's own variable** narrows a stacked multi-variable frame
   (`VARIABLE_COLUMN`).

When **no row is concrete** (`defined_sets` is empty), the figure applies
step 3 to an empty row. It is the same function, not a second rule. The code
axes that the default answered then leave the factor list, so a leftover
`Code:fn` no longer picks up ITERATE and the title no longer reads
`Code:fn=v2`.

### Multi-variable (stacked) plots: the same default (2026-10-15)

`ScidbSource._stacked_table` gives the stacked frame ONE `CodeIsLatest`
column and `default_pin={CodeIsLatest: True}`, just like a single variable.
Each variable's flag is its own per-location `is_latest`. A variable that
attached no flag has nothing superseded, so its rows are filled `True`. A bare
concat would leave `NaN` there, which the mask reads as `False`, dropping the
variable. Until this date the stacked table had `default_pin=None`, so a row
naming no code pooled every version of its variable. Each row still pins its
own variable to a specific version by naming that `Code:` axis, because a
stated selection gets no default.

## What counts as "saying something"

| Predicate | True when | Used by |
|---|---|---|
| `is_stated(row, i)` | row 0, or a selection, a variable or a frozen pin | the panel's "not set" tag; which rows `variant_summary` measures |
| `defined_sets(rows)` | any row has a selection, a variable or a frozen pin; then every stated row | whether a `Variant` factor is folded at all |

A frozen pin is a statement. A reopened saved plot whose row 0 is
`selection={}` with `frozen={"Code:load_cbrace": ["v1"]}` must not fall back
to "current", which is the whole point of freezing.

## Freezing and following (saved plots)

- **Save** (`scistackplotdb.saved.save_plot`): for every row with
  `follow_latest=False`, the function hashes of the rows actually drawn go
  into the envelope as `frozen_code[i] = {fn: [hash, …]}`, read from
  `scidb.provenance_query.code_versions_batch`. It uses hashes, not ordinals,
  because ordinals renumber after a Variants-popup Delete. This works even
  when only one version exists, which is the usual case at save time.
- **Open** (`load_saved_plot` -> `thaw_code`): hashes become ordinals through
  `function_versions`. Only functions that are currently an axis (a `Code:`
  column, meaning two or more versions) get a `frozen` entry. A
  single-version function needs none. The open result carries `newer`: per
  row and function, which versions present in the data are newer than the
  frozen ones. A frozen hash that no longer exists (its version was deleted
  in the Variants popup) gets a restore note; the row keeps its remaining
  frozen versions or, with none left, draws current code, and `newer` offers
  to make that permanent. It is never replaced silently.
- **Follow latest** (`VariantSet.follow_latest=True`): set by answering Yes to
  the GUI prompt (RPC `plot_saved_follow_latest` ->
  `scistackplotdb.follow_latest_saved_plot`, which appends a saved version and
  also drops the row's own `Code:`/`Run:`/latest-flag keys, since a row still
  saying `Code:f = v1` would keep drawing v1).
  Such a row is never frozen and never prompts.
- **An older envelope with no `frozen_code`** opens on current, with no
  prompt. Nothing is back-filled.

## Export

The exported endpoint loads each row through `scistackplotdb.endpoint.row_expression`,
which applies `variants.export_selection`. That is the same `_effective` core
as above, fed the TABLE's `default_pin` and `latest_column` (the endpoint
generator has the table). So a row that names no code exports
`code_version='latest'` exactly when the preview adds the flag, a table with
no default (a single-generation variable: nothing superseded) exports its rows
as written, and a
frozen row exports its ordinal. Grouping
variables keep their raw selection (empty means unfiltered).

**Known limit:** a freeze of several ordinals means "highest per location" in
the preview but "any of" in a `Variant(...)` list. The export logs a WARN
when that happens. Re-saving the plot on one version gives an exact export.

## Stored Variants pins never carry a freeze

The intent store's `variant_selection` pins seed every *new* plot of a
variable. `intent_store.set_variant_selections` therefore drops `frozen` and
`follow_latest`. Opening a saved plot also re-baselines the panel's pin
effect (`savedPlots.variantSetsKey`), so the opened rows are never written as
pins. Only an edit of the Variants section writes pins. Editing a row's
selection clears its freeze, because the user's choice then rules.

## Titles

A variant factor (`Variant` or `Code:*`) with exactly one level never appears
in the figure title (`DisplayText.figure_title`). One series needs no name in
the title. `figure_key` keeps it, because save-all file names and endpoint
`for_each` keys consume it.

## Diagnostics

Every resolve logs, per row, its effective selection and source:
`stated`, `source default` or `frozen`. That comes from
`variants.log_effective_selections`. If a banner ever disagrees with a
figure again, the log line shows the two row counts side by side.
