# Expected locations

*Written 2026-11-07. Plan: `.claude/plan-variable-expected-locations.md`.
Owner: `scidb/provenance/expectation.py`. Tests: `scidb/tests/test_expectation.py`.*

## The question

For one variable: which schema-key combinations should it have, and does
each one match? It answers what the [data inventory](data-inventory.md)
(database-wide, no per-variable mode) and the
[location picker](schema-location-status.md) (expected = what a function
should have run, so a downstream variable inherits its loader's blind spot)
cannot.

## Expectation × fact

| | present | absent |
|---|---|---|
| **expected** | `present` | `missing_unexpected` |
| **not expected** | `present_unexpected` | `missing_expected` |

`excluded` sits outside the table and wins: an analysis exclusion governs the
combination (`scidb.store.exclusions`), or every record there is marked
excluded.

**Two levels, full detail underneath (user, 2026-11-07).** The first look is
the tone: `ok` (green) when the fact matches the expectation, `problem` (red)
when it does not, grey for excluded. The exact status and one sentence of
*why* are always there too: `STATUS_LABELS` and `ExpectationRow.why`, worded
only in `expectation.py`. Every surface shows the sentence as a column as
well as a hover, and an icon per status, so the two reds and two greens are
told apart without colour.

## The variable's keys

The key set most of its locations use (ties: the deeper set). A variable
with no records falls back to `_variables.schema_level`. Keys below that are
never crossed. A record at another key set is `present_unexpected`
("saved at subject, while this variable is saved at subject/session/trial")
unless a mark accepts it.

## The universe

Built key by key in schema order. The question per key is: **does every
parent share the same values?**

| Setting | Values it contributes |
|---|---|
| `shared` | its declared values, else every value it has anywhere in the database (declared level order) |
| `per_parent` | only the values found under each parent, anywhere in the database |

A parent with nothing below it becomes ONE row with `None` (`(all)`) in the
deeper keys: "nothing in the database has any session under subject=S03".

**Automatic sharing** (no stated setting): the top key is shared; a key to
the left of the last key stated `shared` is shared; every other key varies
per parent. Example: state `trial = shared` and `subject`, `session` become
shared; `cycle` stays per parent, so a trial with 12 cycles is not reported
missing cycles 13–15.

**Declared values** are allowed only on a shared key. They catch a value
missing from every subject, which values read from the database never can.
A value outside them is `present_unexpected`. Spellings are stored through
`schema_str`, so `"03"` stays `"03"`.

Without declared values a stray value (`S7` beside `S07`) is not unexpected
by itself: it joins the universe and shows up as `missing_unexpected` rows
for every other variable. Those rows say so: "subject S7 appears only in
RawEMG", for a value of an undeclared shared key found in fewer than half the
variables that use the key.

## Marks

**Expected** / **Not expected**, with a required reason, on one variable at
some schema keys (the rest are wildcards). Never "Include/Exclude": those
words mean *leave out of / put back in the analysis*, and change what runs.
Marks never change what runs.

* **Which mark governs** a combination: `exclusions.most_specific_match`,
  THE rule shared with exclusions (most keys wins, then newest). A mark naming
  a key the variable lacks does not apply to it.
* **Own beats inherited.** Marks are inherited from every variable upstream
  (`provenance.query.related_variable_types`, one query over the edge
  tables, walking through glue hops). So "the EMG file is missing" is marked
  once, on RawEMG, and FilteredEMG and everything after it agree. A coarser
  downstream variable (a subject mean) does not inherit a trial-level mark;
  how many trials fed its mean is the pooling check, not this.
* An Expected mark adds a combination the rule did not generate (a
  recruited subject with no files yet), or accepts one it flagged (a pilot).
* An unmark appends `active = false`; nothing is deleted.

## Storage

Both tables are dataset intent (`BUNDLE_INTENT`): they travel with a bundle
and go live on import. Append-only; the newest row is current.

* `_schema_key_expectation(key, sharing, declared_values, declared_at, source)`;
  `sharing` NULL = automatic.
* `_expectation_mark(variable_name, <schema keys>, expected, active, reason, changed_at)`.

## Not yet

* Variant: presence is "any record of the variable", every variant together.
* A mark that is more specific than a wildcard row does not split the row.

## Surfaces (one read API: `Inspector`)

| Question | Inspector | CLI |
|---|---|---|
| Who makes / reads X, and its neighbours | `variable(X)`: `producers`, `consumers`, `fed_by`, `feeds`, `saved_directly` | `scidb vars X` |
| How each key is expected | `key_expectations(history=)` | `scidb keys [--history]`, `scidb set-key KEY shared\|per-parent\|auto [--values 1..5]` |
| What X should have, and does it | `expectation(X)` | `scidb expected X [--problems] [--status …] [--by KEY] [--csv PATH]` |
| Marks | `expectation_marks(X, history=)`; `Mutator.mark_expectation` / `unmark_expectation` | `scidb marks [X] [--history]`, `scidb mark X k=v --expected\|--not-expected --reason …`, `scidb unmark X k=v` |
| Coverage matrix and funnel | `coverage(by, variables)` | `scidb coverage --by subject [--funnel]` |
| One location through the pipeline | `follow(variables, **location)` | `scidb follow subject=S07 trial=2` |
| Exclusion history | `exclusions(history=True)` | `scidb exclusions --history` |

`coverage` and `follow` (`scidb/provenance/coverage.py`) only summarise
`expectation()` per variable, in `query.variable_types_in_pipeline_order`,
so a cell can never disagree with the variable's own view. `follow` reports
`lost_at`: variables without data at the location although a variable
feeding them has it.

Declared values are typed as `parse_values_text` reads them (the one parser
for the CLI and the GUI): `1..5`, `01..12` (width kept), `BL, POST`.

## Why a location is missing: run outcomes (Stage 7)

Owner `scidb/provenance/outcomes.py`, table `_run_outcome` (history bundle,
emptied by a data clear). One row per location where a Python run **failed**
or found **no data**: function, its output types, the location's schema keys,
blame category, reason, time. Successes are not repeated (the graph has them),
and the rows are not tied to `_run`, because a run that saved nothing writes
no `_run` row.

* **Writer:** `scidb.run.foreach._for_each_execute` collects scifor's
  `combo_skip` events (`OutcomeCollector`) and calls `record_outcomes` in the
  same `finally` as the run-summary line, so a stopped or cancelled run still
  records what it saw. A write failure is a WARN, never a failed run.
  scifor's small per-location failure now carries `blame` and `probable`.
* **Reader:** `expectation()` reads `latest_outcomes(variable)` once (runs
  that listed the variable among their outputs, newest per function and
  location) and appends `Outcome.describe()` to a *missing* row's why:
  "Last run: filter_emg failed here when run on 2026-11-03 14:02: Error in
  your function filter_emg: IndexError: …". Matching is
  `most_specific_match`: a per-subject run that failed at S02 explains S02's
  trials; a trial-level outcome never explains a whole-subject row.
* A present row is never explained, so a later successful run supersedes an
  old failure without deleting it.
* **Not yet:** MATLAB runs (their loop runs in MATLAB; failures reach the GUI
  through the `.done` marker only), locations excluded at run time, and
  "never run since the input arrived".

## What is inside cells (Stage 4)

Owner `scidb/inspect/cells.py`. A vector of per-minute values with NaNs is
ONE present, non-blank cell to every location check, so this looks inside:
per cell `blank` (scalar NaN/None), `empty`, `all_nan`, `partial_nan`,
`constant` (two or more equal non-NaN values), with element and NaN counts.
`CellStats.describe()` is the one wording. Consumers: `scidb nans X
[k=v] [--by KEY] [--kind …]`, `Inspector.cell_profile`, and the data preview
(`ColumnPreview.cells`, shown as `show --types`' and the GUI's **Inside cells**
column).

Still open (plot layer, `docs/claude/not-plotted.md` "Known limits"): a 1-D
measure collapsed to one value per record (`nanmean`) does not list its
partial NaNs under "Not plotted".

## GUI (Stage 5)

All through `scistack_gui/services/expectation_service.py`, which only calls
`Inspector` / `Mutator` and shapes replies. Handlers in `api/schema.py`:
`variable_expectation`, `expectation_badges`, `expectation_coverage`,
`follow_location`, `set_key_expectation`, `mark_expectation`,
`unmark_expectation` (writes broadcast `dag_updated`).

* Right-click a Variable → **✅ Expected locations…**
  (`components/Expectation/ExpectedLocationsPopup.tsx`): key strip, status
  chips, table (icon, keys, status, why; the row tooltip repeats them), Mark…
  (untick keys to widen, Expected / Not expected, reason), Unmark, Follow.
* Variable node badges: red **✕ N** (problems) and **🏷 N** (own marks), from
  `context/ExpectationBadgesContext.tsx`; refetched on `dag_updated` and
  `run_done`. The service caches each badge against a fingerprint of the
  tables that can change it, so a badge is never stale and an unchanged
  database costs one query.
* Header **✅ Coverage** (`CoveragePopup.tsx`): the matrix and funnel; a cell
  opens the variable's popup filtered to that value.
* Icons and colours are the frontend's (`expectationTypes.ts`); every word is
  the backend's.

## The location picker reads this owner (Stage 6)

`locations.location_states` adds `_expectation_locations`: missing-unexpected
locations join the picker's universe (red when absent for the variant),
missing-as-expected ones are grey like exclusions ("not counted", in the GUI
and the CLI), a parent with nothing below it is one red node, and data present
although not expected keeps its colour and is counted in a note. The picker's
own function-derived expected set still applies (union). Excluded locations
that absent data would otherwise put in the universe stay in it, grey.

Function nodes (`intersect_location_states`) join their inputs' universes, so a
location an input should have but lacks shows red there (the function cannot
run), not hidden. Tests: `TestPickerParity` in `scidb/tests/test_expectation.py`,
`TestIntersectedLocations` in `scidb/tests/test_locations.py`.
