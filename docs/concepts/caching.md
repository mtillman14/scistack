# Skipping Computed Work

<!-- Ground truth (tests win over prose). Verified against:
     scidb/tests/test_hist_skip_computed.py (first run computes; unchanged skips; input,
       scalar and dict-of-arrays changes recompute only that combination; identical
       re-save skips; another function runs; plain .save() output runs; two-step and
       three-step propagation; constants are independent variants);
     scidb/src/scidb/run/foreach.py::_build_skip_hook (the comparison, in order; compares
       direct inputs only) and is_endpoint_draft (a draft ignores skip_computed);
     scidb/src/scidb/run/pipeline.py (run_all defaults to skip_computed=True);
     scistack-gui/scistack_gui/api/scopes.py (run requests default to True);
     scidb/src/scidb/run/foreach_config.py::CallSite (call_id).
     SciStack has no result cache any more: lineage_fcn cache hits were removed. -->

`skip_computed=True` makes `scidb.for_each` skip every combination whose result
already exists and was produced from the same code, inputs, and settings. Your
function isn't called for those combinations, so re-running a pipeline only costs what
changed.

This is the only reuse SciStack does. There's no cache of function calls: calling your
function directly always runs it.

## Turning it on

`for_each` runs every combination by default. Ask for skipping per call:

```python
for_each(double, inputs={"x": RawSignal}, outputs=[Filtered],
         subject=[1], trial=[1], skip_computed=True)   # first run: computes
for_each(double, inputs={"x": RawSignal}, outputs=[Filtered],
         subject=[1], trial=[1], skip_computed=True)   # second run: skipped
```

`Pipeline.run_all` and GUI run requests already default to `skip_computed=True`.

Each decision is logged per combination as a `[skip]` or `[recompute]` line with the
reason. These are DEBUG lines, so look in `scistack-debug.log`.

## What makes a combination run again

For each combination, SciStack finds the existing output and compares the invocation
that produced it against the current call. See
[Lineage & Provenance](lineage.md) for what an invocation contains.

| What's different | What happens |
|---|---|
| No output yet for this function and constants | Runs |
| The output was written by a plain `.save()`, not by this function | Runs |
| The function's hash changed | Runs |
| An input's record changed, such as a re-save with different data | Runs for the combinations that use it; the others still skip |
| An input's column selection changed | Runs |
| The `PathInput` that feeds it changed | Runs |
| `distribute` or `as_table` changed | Runs |
| Nothing | Skips |

Sameness is judged by content. Re-saving an input with identical data produces the same
record, so its consumers still skip.

/// warning | Edits to a data file aren't detected
A `PathInput` records which file was read, not what was inside it. If you replace a
file but keep its name, nothing re-runs. Pass `skip_computed=False` for that loader.
///

An endpoint run with `save=False` is a draft and ignores `skip_computed`, so it always
draws or prints its result.

## Changes reach later steps as you run them

Only a step's **direct** inputs are compared. A change moves down the pipeline when
you run the steps in order: a changed raw input makes step 1 run, which saves a new
record, which then makes step 2 run.

If you run only step 2 after changing the raw input, step 2 skips, because step 1
hasn't produced anything new yet.

## Constants create variants

Changing a constant doesn't invalidate the earlier result. It adds a new variant
beside it:

```python
for_each(scale, inputs={"signal": Raw, "factor": 2.0}, outputs=[Out], skip_computed=True, ...)
for_each(scale, inputs={"signal": Raw, "factor": 3.0}, outputs=[Out], skip_computed=True, ...)
# factor=2.0 and factor=3.0 are independent, and re-running either one skips.
# A new factor=7.0 has no output yet, so it runs.
```

Sweeping a parameter never overwrites earlier runs. If an input changes, every variant
that reads it runs again.

## Skipping and node states

These are two different questions, answered from the same provenance:

- **Should this combination run now?** `skip_computed` answers this during a run.
- **Is this step up to date?** [Node states](node-states.md) answer this without
  running anything.

## The same function in several places

One function can be used by several `for_each` calls with different inputs. Each
call is identified by a **`call_id`**, built from what makes that call distinct: the
function's name, its input types, its constants, its run options, and any glue. An edit
to the function's body doesn't change it. The GUI tracks each call's state separately,
and `check_node_state(..., call_id=...)` does the same, so one call's partial run doesn't
affect another's.

**Next:** [Node States](node-states.md) · [Lineage & Provenance](lineage.md) ·
[Variables & Storage](variables.md)
