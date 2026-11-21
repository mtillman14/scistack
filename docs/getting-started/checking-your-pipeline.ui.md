# Did It Work? Checking Your Pipeline

After a run, three questions come up again and again: did that step work, where is data missing, and why. The canvas answers each from what is in the database, so looking never re-runs anything.

<!-- Ground truth (verified 2026-11-21 against main b695de40):
     scidb/tests/test_pipeline_status.py; scistack-gui/tests/test_step_status_service.py;
     frontend/src/components/DAG/stepVerdict.test.ts and DataView/dataView.test.ts.
     Labels from DataView/DataViewPopup.tsx, HeaderMoreMenu.tsx, App.tsx,
     DAG/PipelineDAG.tsx. Engineering note: docs/claude/pipeline-status.md. -->

## The chip on each step

Every function node carries a short **chip** under its name: the most important thing to know about it, such as **✕ 3 failed**, **↓ 2 missing input** or **○ not run**. A step with nothing to report has no chip. Click the chip to open the [data view](#the-data-view) on exactly those locations.

A second chip, **＋4 new · run to include**, means new data arrived after the step last finished. The step still succeeded (new data never makes a finished step look broken), but the node is red because running it again would add those four locations.

The chip sits on top of the node's colour (see [What the colours mean](../gui/process.md#what-the-colours-mean)): the colour says *whether* the step needs running, the chip says *why*.

/// info | 📷 Screenshot
**Show:** three function nodes: one with no chip (green), one with **✕ 3 failed** (red), and one green-to-red with **＋4 new · run to include**.
///

## After a run: the run report

When a run finishes, its **Run report** opens, with one tab per step that ran. It stays closed when nothing ran or everything failed: the Runs dock already says why. Untick **Open after every run** to stop it opening by itself. Right-click a function node and choose **📋 Run report…** to see a step's latest report at any time. Its sections:

| Section | What it says |
|---|---|
| **Did it work?** | The verdict: ✅ **Succeeded**, ⚠ **Succeeded with warnings**, or ✕ **Not complete** with its reasons, worst first. |
| **What's new?** | Which locations this run added, written compactly (ten trials read `trial=11–20`). |
| **What looks odd?** | Results that were saved but look wrong: empty, all NaN, NaN the function added, an unusual shape, or an outlier. |
| **What was it run with?** | The settings, code version and exclusions in force. |
| **Inputs** / **Outputs** | Each variable read and written, with **Read the records** for shapes, sizes and NaN cells. |

## The data view

Click **▦ Data** in the header to answer "what data do I have, and where did it go?". You can also right-click a function or variable and choose **▦ Data view** to open it on just that step.

There's one row per location (subject, trial, …) and one narrow column per variable, in pipeline order. Each cell is a symbol, so the view reads without colour:

| | State | Means |
|---|---|---|
| ✓ | present | Saved by this step's current code. |
| ⚠ | warning | Saved, but something looks odd. |
| ↻ | outdated | Saved by older code, or from inputs saved again since. Run again to update. |
| ✕ | failed | The step raised an error here on its last run. |
| ↓ | missing input | An input this step needs is not there. |
| ＋ | new | Inputs arrived since the step last ran. Run to include. |
| ○ | not run | Inputs are there, but the step has not run here yet. |
| ⊘ | excluded | Excluded on purpose, for every variable or for this one. |

The row of symbols along the top is both the legend and the filter: hover one for its meaning, click it to show only those locations. The footer counts each column and shows where data drops out from step to step, such as `40 → 38 → 31`.

Click a cell to see that location in the right pane: what made the value, why it's in that state, and what you can do about it.

- **Preview…** shows the value, column by column, as a function receives it.
- **Exclude here…** leaves this location out of every step, for example a subject who withdrew. **Exclude RawEMG only…** leaves out just that variable: steps that read it skip the location, and every other step still runs there. On an excluded cell, the button is **Include again…**.

/// info | 📷 Screenshot
**Show:** the data view for a small pipeline, a few ↓ and ⊘ cells, the drop-off line in the footer, and the right pane open on one ✕ cell.
///

### Which locations a step should cover

A step should cover every combination of the levels of the keys it runs once per. That assumes every subject has every trial. If each subject has their own trials, or each site its own subjects, open **Schema keys…** in the header and tick that key under **Between keys**. Levels are then combined within each subject, so a subject with no trial 3 isn't counted as missing it.

## Data from old code

When you edit a function, results made by the old code are kept, as older variants. A variable holding some shows a chip **↻ N outdated variants**. Click it, or use **Clean up outdated…** in the data view, to see each one with what deleting it would remove, and delete the ones you tick. A deletion is one step, so **Ctrl+Z** brings it all back.

## Try a step without saving

Right-click a function node and choose **🧪 Try on…** to run it on a few locations without saving anything. You see each location's result, anything the function printed, any errors, and anything that would read ⚠. It's the quickest way to check a function you're still writing. **Try** isn't available for MATLAB functions yet.
