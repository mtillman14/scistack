# Did It Work? Checking Your Pipeline

After a run, three questions come up again and again: did that step work, where is data missing, and why. SciStack answers each from what is in the database, so nothing here re-runs anything.

<!-- Ground truth (verified 2026-11-21 against main b695de40):
     scidb/tests/test_pipeline_status.py (states, verdict, chips, between keys,
     findings, try_step, the `scidb status FN` CLI), scidb/tests/test_inspect_cli.py,
     scidb/tests/test_coverage.py, scidb/tests/test_expectation.py,
     scidb/tests/test_filter_report.py. Engineering note: docs/claude/pipeline-status.md. -->

## One step: the verdict

Ask about a step with the function itself and the variables it saves:

```python
from scidb import get_database
from scidb.provenance.status import step_status

v = step_status(get_database(), filter_emg, [FilteredEMG])
v.status_line()   # "✅ Succeeded · 40 of 40 present · ＋4 new to process"
v.chip()          # the top reason, e.g. "✕ 3 failed" ("" when there is none)
v.needs_running   # True if running the step again would change something
```

From the terminal, `scidb status filter_emg` prints the same line. (`scidb status` with no function name prints the database overview.)

The verdict is one of three words:

| Verdict | Means |
|---|---|
| ✅ **Succeeded** | Every location the step should cover has a result from the current code. |
| ⚠ **Succeeded with warnings** | Every location has a result, but some look odd (see the ⚠ state below). |
| ✕ **Not complete** | Something is missing. The reasons follow, worst first: ⏸ cancelled, ⏸ stopped, ✕ failed, ↓ missing input, ○ not run, ✎ code changed, ↑ inputs changed. |

New data never makes a finished step look broken. If new subjects arrive after a step succeeded, it still reads ✅, with **＋4 new to process** at the end of the line. `needs_running` is then `True`: run the step to include them.

### Which locations a step should cover

A step's **planned locations** are every combination of the levels of the keys it runs once per. With schema keys `subject, trial` and a step that runs once per trial, two subjects with trials `t1, t2` plan four locations.

That assumes every subject has every trial. When they don't, because each subject has their own trials, or a site has its own subjects, declare that key as a **between key**:

```python
from scidb.store.between_keys import set_between_keys

set_between_keys(get_database(), ["subject"])
```

Levels are then combined within each subject: if S01 has `t1, t2` and S02 has `t3`, the step plans three locations, not six. At least one key must stay crossed. In the GUI, between keys are set in the **Schema keys** dialog.

## Each location's state

Behind the verdict, every planned location of every output gets one state. The first rule that matches wins:

| | State | Means |
|---|---|---|
| ⊘ | excluded | Excluded on purpose, for every variable or for this one (or for one of the step's inputs). |
| ✕ | failed | The step raised an error here on its last run. |
| ↻ | outdated | Saved by older code, or from inputs saved again since. Run again to update. |
| ⚠ | warning | Saved, but something looks odd: empty, all NaN, NaN added, an unusual shape, or an outlier. |
| ✓ | present | Saved by this step's current code. |
| ↓ | missing input | An input this step needs is not there. |
| ＋ | new | Inputs arrived since the step last ran. Run to include. |
| ○ | not run | Inputs are there, but the step has not run here yet. |

The states are in `v.outputs`, one entry per output variable, each with its `locations`.

The ⚠ checks happen during the run, while the values are in memory, and are stored only when something is wrong. A value you saved yourself with `.save()` was never checked, so it can't be ⚠.

"Code changed" needs the function itself. If you pass only the function's name, an edit to its source can't be detected, and only re-saved inputs make a location ↻.

## Try a step without saving

Before a long run, try the function on a few locations. `try_step` takes the same arguments as `for_each` and saves nothing: no records, and no entry in the run history.

```python
from scidb.run.try_step import try_step

res = try_step(filter_emg, {"signal": RawEMG}, [FilteredEMG], subject=[], trial=[])
res.rows       # one per location: the location and a preview of each output
res.failures   # the errors it raised, grouped
res.findings   # what would read ⚠ (empty, all NaN, ...), computed but not stored
res.printed    # everything the function printed
```

`locations=` narrows it to a few locations. `try_step` is Python only.

## Where data goes missing: the `scidb` CLI

These commands open the database read-only, so they're safe to run while the GUI or MATLAB has it open. Every command takes `--json`. See [Inspecting a Database](../guide/inspect.md) for the full command list and how the database is found.

| Command | Question it answers |
|---|---|
| `scidb status FN` | Did that step work? The verdict and its reasons. `--run ID` for one stored run. |
| `scidb run-report FN` | What did the last run do: counts, settings, exclusions, each input and output, NaNs. |
| `scidb coverage --by subject` | Which variables have which subjects, in pipeline order, and how many drop out at each step (`--funnel` for only the counts). |
| `scidb follow subject=S07` | One location through every variable in pipeline order: its state at each, and where it is first missing. |
| `scidb expected RawEMG --problems` | Which locations a variable should have and doesn't, or has and shouldn't. `--by KEY` for counts per value. |
| `scidb nans RawEMG` | What's inside the cells: all-NaN, partly NaN, blank, empty and constant cells, per column and location. |

## Leaving data out on purpose

A location you know is bad (a subject who withdrew, a sensor that fell off) should be **excluded**, not left to fail. It then reads ⊘ instead of ✕ or ↓, and every step skips it. An exclusion can cover every variable, or only one:

```python
from scidb import exclude_schema, mark_missing_expected

exclude_schema(subject=12, reason="withdrew")                          # every variable
exclude_schema(subject=3, trial=2, variable=RawEMG, reason="lead fell off")  # RawEMG only
mark_missing_expected(RawEMG, subject=4)    # the same, with a fixed reason
```

When only one variable is excluded, the steps that read it skip that location, and their outputs read ⊘ "inherited from RawEMG". Every other step still runs there. The full API, including MATLAB and re-including, is in [Database API: Exclusions](../api/database.md#exclusions).

## In the GUI

The same verdicts and states appear on the canvas: a chip on each function node, the **▦ Data** view, and a run report after every run. See the GUI version of this page.
