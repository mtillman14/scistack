# Stroke R01 Aim 2
Aim 2 data processing pipeline

Test with MT/AK
## Example dataset: gait symmetry (`data/`, `src/cycles/`)

A synthetic dataset on the full `[subject, session, speed, trial, cycle]`
schema, with files at **four levels** — the realistic case, where a
per-cycle measure is processed alongside per-trial, per-session and
per-subject values:

| key | levels |
|---|---|
| subject | subject01, subject02, subject03 |
| session | baseline, week04, week12, week24 |
| speed | slow, fast |
| trial | 01, 02, 03 |
| cycle | 01 … 10 |

| level | keys | files | file | columns |
|---|---|---|---|---|
| cycle | all five | 720 | `subject01/baseline/t01/subject01_baseline_slow_t01_c01.csv` | `ankle,knee,hip` |
| trial | subject, session, speed, trial | 72 | `subject01/baseline/t01/subject01_baseline_slow_t01_trial.csv` | `duration_s,walking_speed_mps` |
| session | subject, session | 12 | `subject01/baseline/subject01_baseline_session.csv` | `comfortable_speed_mps,perceived_effort` |
| subject | subject | 3 | `subject01/subject01_demographics.csv` | `age_years,height_cm,mass_kg` |

Every file holds a header and **one data row**:

```
ankle,knee,hip
168.74,78.15,18.23
```

Symmetry values are made up, in the range 0–200, where **0 is perfectly
symmetric** and larger is more asymmetric. A file's level follows from the
keys its name carries — nothing declares it separately.

Both loaders store the three joints as one variable with a column each, so
the joints arrive as a `ColName` factor in the Plot Studio and "Save data
(CSV)" writes one column per joint.

### Running it

Two pipelines read the same files, each configuring the database with the
five-key schema and discovering every key from the file names.

**Python** — `src/cycles/pipeline.py` (standard library and pandas only) plus
`src/cycles/run_pipeline.py`:

```
cd examples/aim2
python src/cycles/run_pipeline.py
```

Four load steps — one per level, each a `PathInput` whose template names
only its own keys — then the processing steps, one `for_each` feature each:

| step | feature |
|---|---|
| `load_cycle_symmetry` / `load_trial_info` / `load_session_info` / `load_demographics` | `PathInput` at the cycle, trial, session and subject levels |
| `ankle_over_threshold` | `CycleSymmetry["ankle"]` (one column) + a two-value `Parameter` (two variants) |
| `trial_mean_symmetry` | `as_table` — a trial's ten cycles arrive as one DataFrame |
| `cycle_deviation` | `as_table` + `distribute=True` — one row back out per cycle, addressed by the returned `cycle` column |
| `scale_joint` | `for_columns()` — runs once per joint column, reassembled into one table |

And the steps that read **more than one level at once**:

| step | levels it combines |
|---|---|
| `normalized_knee` | a cycle's knee value ÷ (its **trial's** walking speed x its **subject's** height) |
| `trial_cadence` | every **cycle** of a trial as a table, over that **trial's** duration |
| `speed_change_from_baseline` | a **session** value against the same subject's baseline session (`Fixed`) |
| `subject_profile` | every **trial** mean of a subject, plus that **subject's** age |

A value recorded above the combination being computed is broadcast down to
it, so every cycle of a subject sees that subject's height. Nothing has to
be joined by hand.

**MATLAB** — `src/cycles/main_cycles.m` runs the load step only
(`loadGaitSymmetryOneCycle.m` + the `GaitSymmetryLoaded` variable).

`[schema_keys]` in `scistack.toml` declares the session and speed order, so
axes read baseline → week04 → week12 → week24 and slow → fast rather than
falling back to a natural sort.
