# Node States: Green, Red, Pending

<!-- Ground truth (tests win over prose). Verified against:
     scidb/src/scidb/provenance/state.py (binary NodeState; check_node_state compares
       expected vs present invocations; check_combo_state deep walk; _discovery_gate);
     scidb/tests/test_hist_state.py, test_hist_state_workflows.py (lazy one-level
       propagation; shallow code-change detection; fork/join; constant variants),
       test_hist_state_realworld.py, test_hist_state_call_id.py;
     scistack-gui/scistack_gui/domain/run_state.py and scistack-gui/tests/test_run_state.py
       (GUI cascade; pending; disconnected; hidden; cycles);
     scidb/tests/test_rename_functions.py (results carried across a rename);
     docs/gui/process.md (the same three colours for GUI users).
     The grey state, _for_each_expected and _lineage are gone. -->

Every function and variable node in the GUI has a state that says whether its results
are current. `scidb` decides between green and red, and the GUI adds pending.

| State | Meaning | Comes from |
|---|---|---|
| **Green** | This step has results for everything it should cover, from the current code, inputs, and settings | `scidb` |
| **Red** | Anything else: never run, partly run, an input changed, or the function was edited | `scidb`, or the GUI's cascade |
| **Pending** (yellow) | A parameter value you added in the GUI but haven't run | GUI only |

There's no in-between state for a partial run. Any missing result makes the node red.

## How a function's state is decided

You can ask for a function's state from code or from a terminal:

```python
from scidb import check_node_state

result = check_node_state(bandpass, outputs=[Filtered])
result["state"]   # "green" or "red"
result["counts"]  # {"up_to_date": 3, "stale": 0, "missing": 1}
result["combos"]  # the same answer per location
```

```bash
scidb state bandpass --missing
```

The check compares two sets of [invocations](lineage.md):

- **Expected** is worked out now from the current input data, for each configuration
  the function has run with.
- **Present** is the part of that set the database actually holds.

The node is green when there's at least one expected invocation and all of them are
present. Nothing is stored ahead of time, so the answer always reflects the current
inputs.

A changed input or an edited function changes the expected invocation ids. The old
results stop counting and the new ones are absent, so these changes show up as
`missing` in `counts`.

### Functions that only read files

A function whose only inputs are `PathInput`s and constants has no database input to
predict from, so its expected set is what it has already produced. To find what it
hasn't, SciStack also looks for the input files on disk, and any file that has never
been run turns the node red. This has three limits:

- If the search finds nothing while results exist, SciStack assumes the path is broken,
  such as an unmounted drive. It logs a warning rather than reporting every location as
  missing.
- A location whose file doesn't exist isn't expected, so a run that failed because the
  file was missing stays green.
- To leave a file out on purpose, [exclude that location](../guide/inspect.md) with a
  reason.

/// warning | Editing a data file doesn't turn anything red
SciStack records which file was read, not what was inside it. If you replace a file but
keep its name, re-run the loader yourself.
///

### One location at a time

`check_combo_state` answers for a single location, with three possible answers:

| Answer | Meaning |
|---|---|
| `up_to_date` | The output exists, and the function and everything upstream are unchanged |
| `stale` | The output exists, but the function's code changed or any record upstream of it was replaced, however far back |
| `missing` | No output exists for this location |

A value saved by hand has no function behind it, so it's `up_to_date` as long as it
exists.

## How changes spread

### A function's own state changes one level at a time

`check_node_state` looks only at a function and its direct inputs:

- **An input is re-saved with different data.** Only the function that reads it directly
  turns red. Later functions stay green, because their own inputs haven't changed yet.
- **A function is edited.** Only that function turns red.
- **The function is re-run.** Its new records change the inputs of the next function,
  which then turns red.

For example, with `raw → step1 → step2 → step3`, re-saving `raw` makes only `step1` red.

### The GUI spreads red down the pipeline

The GUI shows each node's **effective** state, the worst of its own state and the states
of its inputs, ordered red, then pending, then green. So on the canvas, a red `step1`
also shows `step2` and `step3` as red. Run them in order and the chain turns green.

- A variable takes the state of the function that produces it. If several functions
  produce it, it takes the worst.
- A variable that no function produces counts as green for the functions that read it.
- A function with a required input left unconnected is forced red, whatever its results.
- A hidden node takes no part, so it neither passes on nor receives a state.
- Nodes caught in a cycle are red.

## Constants

"Constant" covers two different things.

**A constant you've run** identifies a variant. `scale=2.0` and `scale=3.0` are separate
variants of the same function, and changing the value doesn't make the old variant
out of date. Each variant has its own expected set, so a function with `scale=2.0` fully
run and `scale=3.0` partly run is red. Editing the function's body changes the expected
set for all of them.

**A pending constant** is a value you've added on the GUI canvas but haven't run. Nothing
is in the results yet, so it's shown as its own pending row beside the real ones: the old
variant stays green and the new one is yellow. It doesn't change what `check_node_state`
returns. Once you run it, it becomes an ordinary variant.

## Common scenarios

| What happened | Own state of that function | Shown in the GUI |
|---|---|---|
| Every expected location ran | Green | Green, if everything upstream is too |
| Some locations failed or haven't run | Red | Red, and downstream too |
| New subjects arrived after the last run | Red | Red, and downstream too |
| An input was re-saved with different data | Red | Red, and downstream too |
| An input was re-saved with identical data | Unchanged | Unchanged |
| The function's code changed | Red | Red, and downstream too |
| Only comments or formatting changed in a Python function | Unchanged | Unchanged |
| An upstream step is red, and this step ran for everything available to it | Green | Red |
| Never run, with no input data | Red | Red |

## Renaming a function with a library action

⇪ Share as library, ⇪ publish, and ✎ Make my own copy rename a function, such as
`filter_emg` to `emg_tools.filter_emg`.

- **The code is unchanged.** Each result is recorded again under the new name without
  running, so the new node stays green. The old name's records become history and the
  retired node is hidden.
- **The code changed, or the step never ran.** Nothing is carried over, and the node is
  red until it runs.

The action's report lists how many results were carried for each renamed step, or why
none were.

**Next:** [Skipping Computed Work](caching.md) · [Lineage & Provenance](lineage.md)
