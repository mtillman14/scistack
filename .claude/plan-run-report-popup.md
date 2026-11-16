# Plan: Run report popup (GitHub issue #17)

*Drafted 2026-11-15. Approved the same day (A-D: yes). **Status: all stages
built 2026-11-15; pytest and MATLAB suites not yet run, GUI unchecked.** What
was built, and where it differs from this plan, is in
`docs/claude/run-report.md` ("Limits"): up-to-date combinations are counted
not listed; excluded / not-selected combinations are not rows; the off switch
is per VS Code install; the MATLAB stage needed no `.m` change.*

## The ask

After a function node runs, the researcher must be able to verify **what the
run did**: every input and output described (size, shape, value, status) at
each schema-key combination, the settings used, inclusions and exclusions,
NaNs and other missing data, and any other metadata worth knowing.

The popup (1) opens by itself when a run finishes and (2) is reachable at any
time by right-clicking the function node.

## Decisions taken (user, 2026-11-15)

| Question | Decision |
|---|---|
| Where do the descriptions come from | **Read from the database afterwards.** The point is verifying a "successful" run, not explaining a failed one. |
| Pipeline / "Run until here" runs | **One window, one tab per function**, in run order, each tab marked clean / needs attention. |
| History | **Stored in the database.** Opens on the node's latest run; earlier runs selectable. |
| MATLAB | **Python first**, MATLAB as the next stage. |
| Failures | Raised by the user: maybe a separate popup, maybe none. Proposal below (open question A). |

## What the database already answers, and what it cannot

A run that saves writes `_run` (id, time, function, `where=`, origin),
`_run_step`, `_run_outputs`, and `_run_invocation` → `_invocation` (function
hash, `as_table`, `distribute`, `for_columns`, `across_variants`, iteration
level) → `_invocation_input` / `_invocation_output` (the exact input and
output records of every combination). Exclusions are append-only with
timestamps, so "exclusions in force when the run started" is derivable.
Cell contents (blank, empty, all NaN, partly NaN, constant) have one owner:
`scicanonicalhash._cells` + `scidb.inspect.cells`.

So for every combination a run **computed**, the database already holds its
inputs, outputs and settings. MATLAB runs save through the same path, so
those sections work for MATLAB with no MATLAB work.

Four things only the run knows, and nothing stores today:

1. **A run that saved nothing writes no `_run` row** (every combination up to
   date, failed, or without data). The popup would have nothing to open.
2. **Counts and timing**: total, computed, already up to date, failed, no
   data, removed by exclusions, outside the `locations=` selection; duration;
   cancelled / stopped.
3. **The `locations=` selection**, `on_error`, `skip_computed`: not identity,
   so not on the invocation.
4. **Which run a `_run_outcome` row belongs to** (failed / no-data locations
   are stored per function, not per run), plus the summary's small notes
   (outputs skipped at a location, empty-table returns).

## Design

### One owner: `scidb.provenance.run_report`

The report is a scidb concept (NOTE 3): the GUI popup, `Inspector` and the
`scidb` CLI all read one module and none of them recomputes a status, a count
or a sentence. The frontend owns icons and colours only (the rule the
Expected locations popup already follows).

Read API (through `Inspector`, per the observability rule that the facade
shapes what primitives hold):

| Call | Returns | Cost |
|---|---|---|
| `run_reports(fn, outputs=None, limit=)` | the runs of a function, newest first: id, time, duration, counts, verdict | one query |
| `run_report(run_id)` | header, settings, inclusions/exclusions, per-input and per-output summaries, attention list | queries only, no data loaded |
| `run_report_locations(run_id, variable=None)` | one row per combination: status, and per input/output the record's shape, size and cell findings | loads data, so fetched when a section is opened |

CLI: `scidb run-report <fn> [--run ID] [--locations] [--csv PATH]`.

### What the report shows

**Header.** Function, time, duration, origin (GUI / script), step name, and
one status line: `120 combinations: 96 computed, 20 already up to date,
2 failed, 2 no data. 4 more left out by exclusions.`

**Needs attention.** A short list, worded by the owner, of what a researcher
should look at before trusting the result. First set:
- an output has blank / all-NaN / partly-NaN / empty / constant cells (counts, locations);
- an output's shape or column set differs between locations;
- an input had NaNs at locations where the output does too (or does not);
- fewer output records than computed combinations (outputs skipped, empty tables);
- locations the output is expected to have and still lacks after this run (`expectation()`);
- an input at a location had more than one current variant pooled.

Empty list = "Nothing flagged", said out loud.

**Settings used.** Function version (hash; "show the code as it ran" from the
stored source), each constant / Parameter with its value, `where=`,
`locations=`, run options (`as_table`, `distribute`, `for_columns`,
`across_variants`, iteration level, `on_error`, `skip_computed`), variant of
each input that was read, variant pins that applied.

**Inclusions and exclusions.** Exclusions in force at run time (keys, reason,
date) and how many combinations each removed; the `locations=` selection;
Expected / Not expected marks on the outputs. Wording kept apart as today:
exclusions change what runs, marks do not.

**Inputs** and **Outputs**, one card per argument / output variable: variable,
iteration level, record count, columns with dtype, shape summary ("all
1200×1" or "3 shapes: …"), total size, cell findings. Outputs also say new
vs identical to what was already stored.

**Locations.** The per-combination table (lazy): status (computed / already up
to date / failed / no data / excluded / not selected), then for each input
and output its shape, size, value (scalars inline) and cell findings. Filter
by status and by schema key. A row opens the existing Data preview on that
record; a failed row has **Show failure** (existing FailuresPanel).

### New storage (the smallest set that closes the four gaps)

- `_run_summary` (history bundle, emptied by Clear data), keyed by `run_id`
  like `_run_step` / `_run_outputs`: function, output types, started at,
  duration, the counts, cancelled, stopped, `locations` text, `on_error`,
  `skip_computed`, notes (JSON: output skips, empty returns). Written **once
  per run, including a run that saved nothing**.
- `run_id` column on `_run_outcome`.

Writer: one call in the `finally` of `scidb.run.foreach._for_each_execute`
that already calls `record_outcomes` and logs the run summary. A write
failure is a WARN, never a failed run.

`scidb/run/foreach.py` is a feature-pause hot spot. The addition there is
that one call; the logic lives in the new module. Flagging it because the
pause says new asks go to the backlog first: **approve or defer** (open
question B).

No size or shape is stored: profiling reads the run's records when a section
opens, through a record-scoped entry added to `scidb.inspect.cells` (the
existing owner), batched per variable, never per record.

### GUI

- `services/run_report_service.py`: calls `Inspector` only, shapes replies.
- One RPC handler, `run_report`, with a `part` field (`list` / `summary` /
  `locations`), instead of three endpoints.
- `run_done` gains `reports: {node_id: [run_id, …]}` (same shape rule as
  `failures.by_node`), so the popup knows what to open without guessing
  "latest run of this function". Pipeline runs fill it from the step report
  (`StepSpec.tag`).
- `components/RunReport/RunReportPopup.tsx`: tabs per function, run picker,
  the sections above. Non-blocking window (canvas stays usable).
- Right-click a function node → **📋 Run report…** (also for a node that has
  never run: "No runs recorded").
- A node covered by an EachOf run writes several `_run` rows; the tab groups
  them under the one click.

### MATLAB stage (after Python ships)

Settings, inputs, outputs and the locations table work as soon as Python
does, since they come from the save path. Missing for MATLAB: `_run_summary`
and `_run_outcome`, because the loop runs in MATLAB. The bridge already
receives the tally (`matlab_loop.assemble`, `for_each_save`); this stage
writes the same two tables from there. Until then a MATLAB run's popup shows
"counts not recorded for MATLAB runs yet" in the header and nothing else
changes.

## Stages (one commit each; the user runs the suites between)

**Record the run.** `_run_summary` table, `run_id` on `_run_outcome`, writer
+ the one call in `foreach.py`. Tests: `scidb/tests/test_run_summary.py`
(run that saves, run fully up to date, run that fails everywhere, cancelled,
stopped). Log: `[run_summary] run=<id> fn=… total=… computed=… wrote in N ms`.

**Report owner + CLI.** `scidb.provenance.run_report`, record-scoped cell
profile, `Inspector` methods, `scidb run-report`. Tests:
`scidb/tests/test_run_report.py` on real DuckDB: two-input function with a
NaN input, a multi-output function, exclusions, `locations=`, `where=`, a
re-run (history), `distribute`, an as_table input. Each phase logs its
elapsed time (`[run_report] phase … done in N ms`).

**GUI service + wire.** Service, `run_report` handler, `run_done.reports`.
Tests in `scistack-gui/tests/test_run_report_service.py`.

**Popup.** Component, auto-open on `run_done`, right-click entry, tabs for
pipeline runs. Pure rules (which tab is flagged, when to auto-open) in a
`.ts` file with unit tests. Entry in `docs/gui-manual-testing-todo.md`.

**MATLAB capture.** As above; MATLAB + pytest suites.

**Docs.** `docs/claude/run-report.md` (owner, tables, what is derived vs
stored), README index line, user guide page.

## Open questions

A. **Failures.** Proposal: failures keep the surfaces they have (node badge,
   FailuresPanel, Problems entries, RemedyDialog); no second failure popup.
   The run report lists failed and no-data locations as rows with a **Show
   failure** link, and auto-opens whenever at least one combination was
   computed or already up to date. A run where everything failed does not
   auto-open it (the existing error notification does the talking); right
   click still shows it.
B. **Hot-spot pause.** OK to add the one writer call to `foreach.py` now?
C. **Auto-open off switch.** A "Don't open after runs" tick in the popup
   (per project)? Proposed: yes, default on.
D. **Runs started from a script.** They are in the database too, so the
   right-click history would list them (marked "script"). Proposed: yes.
E. **Anything missing** from "What the report shows"?
