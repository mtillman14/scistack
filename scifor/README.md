# scifor

## Stop Writing Loops, Start Writing Analysis

`scifor` is the batch execution engine behind SciDB. It takes a function you've already written and runs it across every combination of experimental conditions — subjects, sessions, trials, whatever your data is organized by — automatically slicing your data to match each combination and collecting the results into a clean table.

It works in both **Python** and **MATLAB**, and it works standalone — no database required, just plain tables.

## The Problem

Scientific data is almost always organized by conditions: subjects, sessions, trials, limbs, speeds. Processing it means writing nested loops that slice, call a function, and collect results:

```matlab
for i = 1:numel(subjects)
    for j = 1:numel(sessions)
        rows = tbl(tbl.subject == subjects(i) & tbl.session == sessions(j), :);
        result = my_analysis(rows.emg);
        % ...now figure out where to put the result
    end
end
```

This gets worse as the number of conditions grows, and the loop logic buries the actual analysis. Every scientist writes some version of this, and it's never the interesting part.

## How scifor Works

You tell scifor two things:

1. **Schema** — which columns in your tables represent experimental conditions (the things you iterate over)
2. **Inputs** — which tables to slice, and which values are just constants

Then scifor handles the rest: it loops over every combination, filters each table to the matching rows, calls your function, and collects the results.

**MATLAB**

```matlab
scifor.set_schema(["subject", "session"]);

results = scifor.for_each(@my_analysis, ...
    struct('emg', data_table, 'cutoff_hz', 20), ...
    subject=[1, 2, 3], session=["pre", "post"]);
```

**Python**

```python
from scifor import set_schema, for_each

set_schema(["subject", "session"])

results = for_each(
    my_analysis,
    inputs={"emg": data_table, "cutoff_hz": 20},
    subject=[1, 2, 3],
    session=["pre", "post"],
)
```

For each of the 6 combinations (3 subjects x 2 sessions), scifor filters `data_table` to the matching rows, passes the `emg` column to `my_analysis` along with the constant `cutoff_hz=20`, and collects the return value. The result is a table with `subject`, `session`, and `output` columns — one row per combination.

Your function doesn't need to know about looping, filtering, or metadata. It just receives data and returns a result.

## What You Get Back

`for_each` returns a MATLAB table (or pandas DataFrame in Python) with one row per combination. Metadata columns come first, then the output:

| subject | session | output |
|---------|---------|--------|
| 1       | pre     | 0.82   |
| 1       | post    | 1.47   |
| 2       | pre     | 0.91   |
| 2       | post    | 1.38   |
| 3       | pre     | 0.76   |
| 3       | post    | 1.22   |

If your function returns a table, its columns are flattened into the result alongside the metadata. If your function returns multiple outputs, you get multiple result tables.

## Beyond the Basics

The examples above cover the most common case, but real pipelines have real-world complications. scifor has tools for each of them:

- **Fixed inputs** — Pin one input to a specific condition while the others iterate. The classic case: comparing every session against a fixed baseline.

- **Merging tables** — When your function needs columns from two separate tables (say, kinematics and force data), combine them into a single input per combination.

- **Column selection** — Extract just the columns you need from a multi-column table before your function sees it.

- **File paths from metadata** — When your data lives in files organized by condition (e.g., `data/subject_1/trial_3.mat`), generate the right file path for each combination automatically.

- **Row filtering** — Apply column-based filters (like "only right-side trials" or "speed > 1.5") on top of the metadata filtering.

- **Location selection** — `locations=` names which schema locations to run: ragged prefixes (`all of subject 01, plus trials 1-3 of subject 02`) plus standing per-key rules (`session BL is out, everywhere, including in data collected next month`). Filters *combinations*, where `where=` filters *rows within* a combination. See [location filter semantics](../docs/claude/location-filter-semantics.md).

- **Dry run** — Preview which combinations would be processed and what data would be passed, without actually running anything.

- **Distribute** — When a function returns a vector of values that should each become their own row at a deeper schema level (e.g., splitting a trial into individual gait cycles), scifor can expand the output automatically.

See the [API reference](../docs/api/for-each.md) and [batch processing guide](../docs/guide/for_each.md) for details on all of these.

## Relationship to SciDB

scifor is the engine that powers `scidb.for_each()`. When used through SciDB, inputs are loaded from the database and outputs are saved back automatically. When used standalone (as `scifor.for_each()`), it works with plain MATLAB tables or pandas DataFrames — no database needed.

If you're already using SciDB, you're already using scifor under the hood. If you just want the loop orchestration without the database, use the `scifor` namespace directly.

## Logging

scifor logs through the shared [`scistacklog`](../scistacklog/README.md) facade with `layer="scifor"`. At the default (INFO) level a run produces a concise narrative: a banner with truncated value previews (`subject=12 values [s01, …, s12]`), a progress line each time the outermost iterated key advances (rate-limited so fast runs stay silent), and an end-of-run summary that aggregates failures by reason:

```
for_each(compute_psd) done in 74.0s: completed=114, failed=3, total=120
failed: 3 × "ValueError: bad channel count" — subject=s03, session=2; … (+N more)
```

The first occurrence of each distinct failure reason is logged at WARN with the full traceback. Per-iteration `[run]`/`[skip]`/`[done]` lines are DEBUG — enable them with `Log.set_level("DEBUG")` or `SCIDB_LOG_LEVEL=DEBUG`. Dry-run output goes to stdout (it is the requested result, not logging).

Standalone (no scidb) usage gets the same console output automatically; when used through SciDB, the records also land in `scidb.log`.
