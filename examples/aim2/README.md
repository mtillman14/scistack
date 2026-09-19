# Stroke R01 Aim 2
Aim 2 data processing pipeline

Test with MT/AK
## Example dataset: per-cycle gait symmetry (`data/`, `src/cycles/`)

A synthetic dataset on the full `[subject, session, speed, trial, cycle]`
schema — **one file per cycle**, 720 files:

| key | levels |
|---|---|
| subject | subject01, subject02, subject03 |
| session | baseline, week04, week12, week24 |
| speed | slow, fast |
| trial | 01, 02, 03 |
| cycle | 01 … 10 |

```
data/subject01/baseline/t01/subject01_baseline_slow_t01_c01.csv
```

Each file holds a header and **one data row** — left-vs-right symmetry for
three joints:

```
ankle,knee,hip
168.74,78.15,18.23
```

Values are made up, in the range 0–200, where **0 is perfectly symmetric**
and larger is more asymmetric.

Run `src/cycles/main_cycles.m` to load it: it configures the database with
the five-key schema and runs one `for_each` over a `PathInput` that
discovers every key from the file names. The loader returns a struct with
one field per joint, so the joints arrive as one variable with a column
each (a `ColName` factor in the Plot Studio).

`[schema_keys]` in `scistack.toml` declares the session and speed order, so
axes read baseline → week04 → week12 → week24 and slow → fast rather than
falling back to a natural sort.
