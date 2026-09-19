# Synthetic factors in scistackplot

> **Roles changed 2026-09-17.** `X / COLOR / AGGREGATE / FREE` are gone; the model is now a Grouping list (innermost first, one coloured layer) plus `FACET / ITERATE / COLLAPSE`, with a nested collapse chain. Read `docs/claude/grouping-and-collapse.md` first; role names below are historical.

*Written 2026-09-09, before the Plot Studio work in
`.claude/plan-plot-studio-todos-26-09-09.md`. Concerns `scistackplot` and
`scistackplotdb`.*

## The idea

`scistackplot`'s whole control surface is one rule: **every categorical column
carries exactly one role** (`Role.GROUP/FACET/ITERATE/COLLAPSE`, since 2026-09-17). The
GUI is a `<select>` per factor and nothing else.

That rule only pays off if *everything the user might want to split a figure by*
is a factor — including things that are not columns of any table. The fields of
a struct variable, the pipeline variant a record came from, which variable a
series is: none of these are data columns, and each one is a question a user
answers by pointing at a plot channel.

A **synthetic factor** is such a thing, reshaped into an ordinary
`FactorInfo` before anything downstream looks at the table. The alternative —
teaching the renderer that structs get subplots, that variants get colours, that
two variables get two line styles — is three special cases in two renderers plus
codegen, i.e. six places to keep in agreement.

Existing ones:

| Factor | Levels are | Built in | Constant |
|---|---|---|---|
| `ColName` | the keys of a dict/struct variable | `ScidbSource._melt_fields` | `scistackplotdb.source.FIELD_FACTOR` |
| `Variant` | the user's named variant rows | `variants.apply_variant_sets` | `variants.VARIANT_FACTOR` |
| `Code:<fn>` | version ordinals of one function | `load.attach_variants` | `table.CODE_FACTOR_PREFIX` |

`Code:<fn>` is a half-case worth noticing: it is a real column that scidb
attaches, but the **prefix convention lives in `scistackplot`**, because naming
is presentation. `FactorInfo.origin` then carries `{"kind": "code", "function":
…}` so that no consumer ever reconstructs the producing function by splitting the
name on `":"` — that would re-implement scidb's namespacing one layer away from
where it is defined.

## Two ways to build one, and how to choose

**At the source, by melting** (`ColName`). The reshape is a property of *how the
storage layer laid the data out* — scidb keeps a dict-valued variable in
`multi_column` mode, one DuckDB column per key. The user never asked for 13
columns, so the source hands back the long form and the factor comes with it.

**As a derived table, by a pure function** (`Variant`). The reshape is a property
of *what the spec says* — `apply_variant_sets(spec, table) -> LongTable` returns
the table unchanged when the spec names no variants, so a project that never
edited a function pays nothing.

The test: **does the spec decide?** If yes, it is a derived table, because
`capabilities`, `resolve` and `codegen` each need to recompute it whenever the
spec changes. If the shape of the storage decides, melt it once in the source and
cache it with the frame.

## Six invariants

### 1. Downstream must not know it is synthetic

`is_variant` and `is_field` exist so the **GUI can label a row** ("variant",
"fields") and so `default_roles` can open sensibly — a struct's fields default to
FACET because 13 muscles overplotted on one axis is not a figure anyone wanted.
Neither flag is read by `reduce` when it groups, facets or colours, and neither is
read by a renderer at all. Adding `if factor.is_<mine>` to `render/` or to
`_build_figure` means the abstraction has failed; fix the derived table instead.

### 2. It must exist before `validate` and `complete_roles`

There are exactly **three** call sites, and they must stay in agreement:

```python
reduce.resolve()          # line ~75
capability.capabilities() # line ~133
codegen.generate_plot_function()  # line ~62
```

All three do the same two steps in the same order:

```python
derived = apply_variant_sets(spec, table)
spec    = strip_answered_roles(spec, table, derived)
```

Miss one and the symptom is not a crash but a disagreement: `capabilities`
offering a role selector for a factor `resolve` is about to reject, or generated
code drawing a figure the preview never showed.

### 3. Consumed columns must leave the factor list

Once the variant row "baseline" *means* `Code:bandpass == v1`, keeping
`Code:bandpass` as its own factor states the same thing twice — and with two rows
it is an unassigned two-level variant factor, which `roles.validate` refuses,
rejecting the exact comparison the user just asked for. `variants._answered`
computes the consumed set; `strip_answered_roles` drops roles that named them
(stale, not a typo — a role naming something that was *never* a factor still
raises).

Any new synthetic factor that consumes columns owes the same pair of functions.

### 4. Reshaping on the interactive path must be mirrored in codegen

This is the invariant that silently produces wrong figures. `ScidbSource`
melts a struct's fields; the generated endpoint receives the **unmelted** frame
from `as_table`, so `codegen._preamble` emits a matching `df.melt(...)`. The
`Variant` factor is built by masking one frame; the endpoint gets one input per
row, so `codegen._variant_preamble` emits the `pd.concat([... .assign(Variant=…)])`
that reconstitutes it.

Every reshape therefore exists **twice**, and the two copies are only trustworthy
because a test runs both and compares figure sets
(`scistackplotdb/tests/test_fanout_parity.py`). A new synthetic factor needs its
own entry in that family of tests, not just a unit test of the melt.

Where the two paths *cannot* be identical, prefer emitting the **resolved
result** over re-deriving it in generated code: `codegen` emits
`col_order=[...]` computed by `reduce.plan_layout` rather than the layout rules,
because replaying rules in seaborn would be a second implementation that can
drift.

### 5. Levels are declared order, and emptiness is never silent

Level order comes from what the user (or the schema) declared, never from what
the data happens to contain — a legend that reorders itself when a variant loses
its last record is disorienting, and zero-padded keys sort `1, 10, 2` under
pandas' default. `LongTable.level_order` carries the real order and
`reduce._level_rank` consults it.

A level that matched **no rows** must say so. `apply_variant_sets` logs a warning
naming the empty variants and `capability.variant_summary` reports `row_count`
per row, because a variant selecting a combination nobody ran is
indistinguishable from a working one until its series quietly fails to appear.

### 6. A synthetic factor needs MORE THAN ONE level to exist

A factor with one level is not a condition, it is a constant. Offering it asks
the user to choose between one thing, and — carrying a `variant` tag and a role
selector — it is indistinguishable from a genuinely swept parameter until you
count its levels.

Code axes had this guard from the start: `scidb.code_version_ordinals` omits
single-version functions, so an unedited project gets no `Code:<fn>` columns at
all. **Branch params did not**, because `branch_params_batch` returns every
upstream *constant* whether it varies or not — the name promises a branch, the
query does not check for one. Measured 2026-09-11: `filterDelsys.config` and
`filterDelsys.Fs` each held one level across both records of `FilteredEMG` and
both demanded a role. Now dropped in `scistackplotdb.load.attach_variants`.

One subtlety in the test: **absence counts as a value.** A key present on some
records and missing on others genuinely does tell them apart, so the check is
over the raw value list, not over the non-null values. Dropping such a column
would reintroduce exactly the overplotting the mechanism exists to prevent.

A code axis reporting one level is a *different* problem — scidb promised not to
emit it — so that is logged as a WARN rather than filtered, on purpose:
duplicating scidb's rule here would hide the regression instead of surfacing it.

## Traps already paid for

- **Two variables' data columns are both named `value`.** Rename *before*
  joining or melting; a rename after the merge cannot separate them and one
  measure vanishes.
- **`bool` is an `int` subclass.** `shape.classify_value` checks bool first, or a
  True/False column classifies as a scalar measure instead of a factor.
- **A selection key naming a missing column is dropped, not treated as
  no-match** (`variants.resolve_selection`). Right for a *stale* spec — it must
  never silently empty a figure. But once several variables share one frame,
  "stale" and "not applicable to this variable" become indistinguishable, and a
  variant pinning `Code:filterEMG=v1` would claim every `Force` row too. The fix
  is to **qualify the mask by the row's own variable**, not to change the drop
  rule.
- **A CSV column of bare numeric IDs classifies as a measure, not a factor**
  (documented in `sources/csv.py`). scidb sources never hit it because the schema
  declares its keys.

## Checklist for adding one

1. Decide source-melt vs derived-table by "does the spec decide?".
2. Name the constant in `scistackplot`, even if the data comes from scidb —
   naming is presentation.
3. Emit an ordinary `FactorInfo`; add an `is_*` flag only if the GUI must label
   it or `default_roles` must treat it specially.
4. Wire the derived table into **all three** call sites.
5. If it consumes columns, extend `_answered` and `strip_answered_roles`.
6. Mirror the reshape in `codegen`, and add a parity test that renders both paths
   and compares.
7. Drop it entirely if it holds fewer than two levels — a constant is not a
   factor (invariant 6).
8. Warn on levels that matched nothing; report per-level row counts in
   `capabilities` so the GUI can show them.

## A derived table that is not a factor

Everything above is about reshaping something into a **factor**. The same
derived-table mechanism has since grown a member that reshapes the **measure**:
`cell.apply_cell_collapse` rewrites a 1-D measure's cells to one value each and
flips its shape to `SCALAR`, which is what lets scatter/box/violin/bar be offered
for a vector-valued variable (added 2026-09-14).

It passes the same "does the spec decide?" test — the plot kind decides — and it
obeys invariants 2 and 4 here unchanged (it must exist before `validate` and
`complete_roles`; the reshape is mirrored in `codegen` and parity-tested).
Invariants 1, 3, 5 and 6 are about factors and do not apply to it.

The one thing it adds to this note's model: a derived table may now change what
`table.shape_of(measure)` answers, so **the capability report has to hold two
tables at once** — the kind list is computed from the raw shape and everything
else from the derived one. See `docs/claude/measure-shape-and-collapse.md`.

## See also

- `docs/claude/measure-shape-and-collapse.md` — shape as the second axis of the
  control surface, and the derived table that changes the measure
- `docs/claude/plotting-library-design.md` — the `PlotSpec`-is-the-product idea
- `docs/claude/plot-variant-rows.md` — the `Variant` factor in detail
- `docs/claude/variant-axis-node-binding.md` — which canvas node supplies an
  axis, and the three names for a parameter that made that hard
- `docs/claude/facet-layout-grid.md` — `plan_layout`, the other pure
  reduce-then-replay function
- `.claude/plan-plot-studio-todos-26-09-09.md` — the work this note precedes
