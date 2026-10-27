# SciDB

Database operations layer for SciStack. Provides abstractions for defining typed variables, configuring the database, and saving/loading data by metadata.

## Named Filters (value-based, reusable)

`Filter` objects are first-class values — assign them to a variable and reuse
them in any `where=` clause, or compose them with `&` / `|` / `~`:

```python
# Define once (this IS a ColumnFilter — no special wrapper needed)
clean_gr = GAITRiteLoadedCycle["StepLengths_GR"] != 0

# Use anywhere
for_each(mean_change_from_reference,
         inputs={"baseline": Fixed(GAITRiteLoadedCycle["StepLengths_GR"], session="BL"),
                 "value":    GAITRiteLoadedCycle["StepLengths_GR"]},
         outputs=[DeltaStepLength],
         where=clean_gr & (UAStartFoot() == "A"))

# Compose named filters
clean_and_unilateral = clean_gr & (UAStartFoot() == "U")
for_each(..., where=clean_and_unilateral)
```

MATLAB equivalent:

```matlab
clean_gr = GAITRiteLoadedCycle("StepLengths_GR") ~= 0;
scidb.for_each(@meanChangeFromReference, ..., where=clean_gr & (UAStartFoot() == "A"));
```

## Permanent Schema-Level Exclusions

For data that should be excluded from **every** analysis (e.g., a failed
recording session), use the exclusion registry instead of per-call filters.

```python
# Mark a specific trial as excluded (persisted in the database)
scidb.exclude_schema(subject=1, trial=2,
                     reason="equipment malfunction during recording")

# Exclude an entire subject (trial omitted = wildcard)
scidb.exclude_schema(subject=3, reason="participant withdrew")

# Inspect currently-excluded combinations
exclusions_df = scidb.list_exclusions()

# Re-include (logged; the original exclusion row is preserved)
scidb.include_schema(subject=1, trial=2,
                     reason="re-reviewed video, recording was valid")
```

MATLAB equivalent:

```matlab
scidb.exclude_schema("equipment malfunction", 'subject', 1, 'trial', 2)
scidb.include_schema("re-reviewed, recording was valid", 'subject', 1, 'trial', 2)
tbl = scidb.list_exclusions()
```

Exclusions are applied automatically by `for_each` before the iteration loop.
The full exclusion table is hashed and stored in `version_keys`
(`__schema_overrides_hash`) so that adding or removing an exclusion
invalidates cached results.

```python
from scidb import configure_database, BaseVariable
import numpy as np

db = configure_database("experiment.duckdb", ["subject", "session"])

class RawSignal(BaseVariable):
    schema_version = 1

RawSignal.save(np.array([1, 2, 3]), subject=1, session="A")
raw = RawSignal.load(subject=1, session="A")  # one match -> a BaseVariable; several -> a list
all_versions = list(RawSignal.load(subject=1, session="A", version="all"))
```

## Data Inventory (missing schema-key combinations)

"Which combinations of these schema keys have no data?" The inventory crosses
every value each chosen key has anywhere in `_schema` (in the declared
`[schema_keys]` order) and gives each combination a status: `present`,
`missing`, or `excluded` (an active `exclude_schema` matches it; excluded wins
over present). A deeper row counts for its projection onto the chosen keys,
and only the database is consulted, never a Variable.

```python
from scidb.inventory import schema_inventory, write_inventory_csv

inv = schema_inventory(db, ["subject", "session"])   # None = every key
inv.counts            # {"present": 10, "missing": 2, "excluded": 0}
write_inventory_csv(inv, "missing.csv", missing_only=True)
```

```bash
scidb inventory --keys subject,session          # counts + missing table
scidb inventory --all --csv inventory.csv       # every combination + status
scidb inventory --within group                  # one inventory per study arm
```

`within=["group"]` (CLI `--within group`) runs the inventory separately for
each `group` value and crosses only the values found in that arm, so a subject
of one arm is never reported missing at the other arm's sessions. Use it for
design keys (arm, site, protocol).

Above 1,000,000 combinations the call raises before building the product. The
GUI's 📋 Inventory button shows the same table. Details:
`docs/claude/data-inventory.md`.

## Logging

Every scidb operation logs through the shared [`scistacklog`](../scistacklog/README.md) facade. `configure_database()` points the file sink at `scistack.log` next to the database file and writes a run-context header (package versions, Python version, pid), so each log file is self-describing.

Two sinks with independent levels, both defaulting to INFO:

- **console** (stderr): the pipeline narrative — for_each banner, periodic progress, run summary with failure reasons, `[timing]` summaries.
- **file** (`scistack.log`): the same narrative, with date + millisecond timestamps and the originating layer (`[scidb]`, `[scifor]`, `[matlab]`, …) on every line; one record per line.

For debugging, raise a sink to DEBUG to capture the full execution trace (named internal steps with durations, per-iteration `[run]`/`[skip]` lines, per-phase `[timing]` tables, column/dtype dumps):

```python
from scidb import Log

Log.set_level("DEBUG", sink="file")   # full detail in scistack.log only
Log.set_level("DEBUG")                # both sinks
```

or set the `SCIDB_LOG_LEVEL` environment variable, or pass `-v` to the `scidb` CLI.
