# Plan: stratify by a column of a variable (`Demographics["InterventionGroup"]`)

*Drafted 2026-09-15. Scope decided with the user: **Plot Studio only**,
**categorical columns only**, **attach-then-row-filter** (no load-time `where=`),
**y-limit scoping falls out** (no column-as-measure).*

## What already exists

`PlotSpec.factor_variables: list[str]` already joins a **variable** in as an
ordinary factor:

| Piece | Where |
|---|---|
| offer list | `ScidbSource.groupable_with` — `scistackplotdb/source.py:885` |
| the join | `ScidbSource._attach_factor_variables` — `source.py:489` (prefix-merge, broadcast down the hierarchy) |
| role/filter/scope | free — it becomes an ordinary `FactorInfo`, so Filters (`otherFilterable`), Factors and Y-axis scope pick it up with no new code |
| export | `codegen.group_param` / `_preamble` merge (`codegen.py:208,353`) and `endpoint.py:224` (one `for_each` input, listed in `as_table`) |
| GUI | Grouping section checkbox list — `PlotStudio.tsx:1557` |

## The gap

Both the offer list and the join **refuse any variable with more than one data
column**:

- `source.py:897` — `if len(data_columns_for(self._db, candidate)) > 1: continue`
- `source.py:511` — `"{name!r} stores one column per dict/struct field, so it has
  no single value to group by."`

A Demographics table loaded from `.xlsx` is stored multi-column (one DuckDB
column per field — `data_columns_for`, `load.py:127`), so `InterventionGroup` is
unreachable today. There is no way to say *variable + column*.

So the work is: make a factor reference a **variable and (optionally) a column**,
and thread that one change through offer → spec → attach → codegen → GUI.

## Verified facts the plan rests on

- A single-column variable's data column is renamed to the variable's view name
  on load (`scidb/foreach.py:3134`), which is why today's codegen merge needs no
  rename. A **multi-column** variable keeps its field names, so a column factor's
  merged column is named `InterventionGroup` on both paths — no rename either.
- `as_table` + `ColumnSelection` already yields *schema keys + the selected
  column* as a DataFrame (`scifor/foreach.py:1645-1658`). So the endpoint can
  pass `Demographics["InterventionGroup"]` and the generated merge works
  unchanged. **This is what makes invariant 4 (codegen mirrors the interactive
  reshape) cheap here.**
- `sources/base.py:158` memoizes `get_table` on `tuple(factor_variables or ())`,
  so the new element type must be hashable → frozen dataclass.

## Stage 1 — `FactorVariable` in the spec (`scistackplot`)

`spec.py`:

```python
@dataclass(frozen=True)
class FactorVariable:
    """A variable joined in as a factor, optionally one column of it."""
    variable: str
    column: str | None = None

    @property
    def factor_name(self) -> str:
        return self.column or self.variable
```

- `PlotSpec.factor_variables: list[FactorVariable]`, with `to_dict`/`from_dict`
  updated (`spec.py:660,787`).
- `MISSING_LEVEL = "(missing)"` — named in `scistackplot` (naming is
  presentation, per `docs/claude/synthetic-factors.md`).
- The factor is named after the **column** (`InterventionGroup`), not qualified
  `Demographics.InterventionGroup`, precisely so the interactive column name and
  the `as_table` column name are the same string and codegen needs no rename.
  A name collision (two group columns, or a column colliding with a schema key
  or the measure) is a hard `ValueError` naming both — never a silent overwrite.

Tests: round-trip, `factor_name`, collision message.

## Stage 2 — attach a column (`scistackplotdb`)

`_attach_factor_variables(frame, levels, factor_variables)`:

- whole-variable entry → today's behaviour, unchanged;
- column entry → validate the column against `data_columns_for`, take
  `[*variable.levels, column]`, `drop_duplicates`, left-merge on the shared
  levels; drop the variable's variant columns as today;
- **level check unchanged** — the factor variable must sit at a prefix of the
  measure's levels, else the existing "no unambiguous way to attach" error;
- **missing coverage is never silent**: after the left join, NaN → `MISSING_LEVEL`
  with a `Log.warn` naming the count. (A subject absent from the sheet is a real
  answer, not a reason to drop their trials.)
- **one level warns but still attaches.** Invariant 6 drops *auto-derived*
  single-level factors; this one the user ticked, so silence would read as a
  broken checkbox.
- `MISSING_LEVEL` sorts last in `_ordered`.

`groupable_report(measure) -> {"offered": [...], "rejected": {label: reason}}`
(mirroring `stackable_report`, `source.py:822` — an absent candidate is
indistinguishable from a broken panel):

- whole-variable candidates as today;
- for each multi-column candidate at/above the measure's level, one entry per
  **categorical** column;
- numeric columns → `rejected` with *"numeric column — bucketing not offered
  yet"* (the agreed future extension, visible rather than missing);
- more than `MAX_GROUP_LEVELS = 50` distinct values → `rejected` with the count
  (*"looks like an identifier, not a group"*);
- levels come from one `SELECT DISTINCT "col" … LIMIT 51` per candidate column,
  under a `Log.timer`, so a wide sheet's cost is attributable.

## Stage 3 — GUI (`scistack-gui`)

- `plot_service.describe` → `groupable_with: [{variable, column, label,
  level_count}]` plus `groupable_refused`; `_table_for` (`plot_service.py:1178`)
  builds `FactorVariable`s.
- `PlotStudio.tsx`: `GroupableInfo` type; the Grouping list renders one row per
  offer, labelled `Demographics · InterventionGroup` with its level count, and
  refusals in the same muted style the variant picker uses.
  `toggleFactorVariable` matches on `(variable, column)` and strips the role by
  `factor_name`.
- **No other section changes.** Filters, Factors and Y-axis scope read the
  resolved factor list and get the new factor for free — that is the payoff of
  the one-role-per-factor model.

## Stage 4 — export parity (invariant 4)

- `codegen.group_param(fv)` → `group_demographics_interventiongroup`; the
  `_preamble` merge keeps its shape and gains the `fillna(MISSING_LEVEL)` line so
  the exported figure is the previewed figure.
- `endpoint.py` emits `Demographics["InterventionGroup"]` for a column entry and
  plain `Demographics` otherwise; the param joins the `as_table` list as today.
- A case in the `test_fanout_parity` family: a categorical column factor on
  COLOR, both paths rendered and the figure sets compared.

## Stage 5 — tests

`scistackplotdb/tests`: broadcast subject→trial merge; missing subject →
`(missing)` + warning; unknown column; collision; finer-level refusal;
`groupable_report` offers and all three refusal reasons.
`scistackplot/tests`: spec round-trip, codegen golden.

## Two decisions to confirm

1. **Old saved specs.** Generated `plot_` endpoints carry their spec as embedded
   JSON. A clean break (project rule: no deprecation shims) makes an existing
   endpoint with `factor_variables: ["Condition"]` fail to parse. Proposal:
   `FactorVariable.from_dict` reads a bare string as `variable` — one line, no
   alias, no rename.
2. **Numeric columns.** Offered as refusals with a reason now; binning (`Age` →
   ranges) is a later extension of `level_groups`, not in this work.

## Not in scope (decided)

- Processing / `for_each` `where=` UI — `where=Demographics["InterventionGroup"]
  == "Onward"` already works in hand-written pipeline code.
- Stratified runs (one `for_each` per group).
- Column as x or y measure.
