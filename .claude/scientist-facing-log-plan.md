# Plan: a scientist-facing log (run journal) + a complete debug log

*Drafted 2026-10-26. Status: IMPLEMENTED 2026-10-26 (uncommitted; Python tests not yet run by the user). Deviations: stage 4 (demoting GUI INFO lines) not done; the GUI "Copy debug details" action not done; open decisions taken as recommended (scistack.log = journal, scistack-debug.log = debug, data notes deduped into the journal, 10 MB × 5, marker = scifor.failures.ActionableError). See docs/claude/logging-architecture.md.*

## Problem (measured on `/workspace/scidb.log`, 2026-10-22)

- 7,294 lines. 5,113 are `INFO [scistack_gui]` internals (`[db] acquire/release` ×233 each,
  `[run_state]` ×109, `[pipeline] Building…` ×55 per refresh, RPC counts).
- The one real analysis run (`pandas.read_csv`) is ~10 lines near line 1111.
- A scientist cannot tell whose fault a failure is:
  - `AttributeError: 'StringArray' object has no attribute 'split'` (×11 columns × N subjects).
    This was raised in user code, but nothing in the log says so.
  - `Error handling put_edge` + 30-line traceback. This is a SciStack-internal (GUI) failure,
    and nothing says that either.
  - 171 `[scistackplotdb]` "holds a number in only 0 of 34 records" WARNs. These are real
    findings about the data, repeated once per column per refresh.
- The current level policy mixes two audiences on one axis. An INFO line has to serve both
  "what happened to my analysis" and "what did the framework do", so it serves neither.

## Core idea: audience is a separate axis from level

One emit stream, two files, linked by a run id:

| File | Audience | Content | Level |
|---|---|---|---|
| `scistack.log` (**journal**) | scientist | one block per run: what ran, counts, duration, failures grouped by reason **with blame**, data notes | curated events, not free-text log calls |
| `scistack-debug.log` | developer / bug report | everything, as today's file format + `run=<id>` token | **always DEBUG**, rotating (10 MB × 5) |

**The debug log is a strict superset.** Every journal event is also written to the debug file,
so the debug file still contains the full story. The journal says *"run a3f2"*, and grepping the
debug file for `run=a3f2` gives the whole trace. That is how one log can serve both audiences.
DEBUG being always on means bugs no longer need to be reproduced with `-v`.

## Blame: one classifier, three categories (+ no data)

Owner: `scifor/failures.py` (it already owns `frame_origin`). A new `ComboFailure.blame` field is
computed once and consumed by the journal, the GUI badge/FailuresPanel, Problems, and MATLAB
(NOTE 4).

| `blame` | Rule (evaluated in order) | Journal headline |
|---|---|---|
| `no_data` | `NoDataError` | "– no data for N combos" (informational, not an error) |
| `setup` | exception is an intentional SciStack error (marker base class, see below) | "Problem with your setup or data" + the actionable message |
| `user_code` | the innermost *non-library* frame is a user frame (a library call made from user code counts as user code) | "Error in your code" + `file:line in fn` + source line |
| `internal` | otherwise (innermost non-library frame is framework, exception is not a marker) | "SciStack internal error, not caused by your code. Please report: run a3f2" |

**Marker base class.** Today `scidb.SciStackError` exists, but scifor (which is below scidb)
cannot see it. Proposal: `scifor.failures.ActionableError` (name TBD) is the base, and
`scidb.SciStackError` + `scifor.SchemaKeyTypeError`/`ForColumnsError` subclass it. Stage 1
checks whether sciduckdb/scilineage raise user-actionable errors that would need it too.

**Honest limit:** blame describes *where* an error was raised, not its root cause. Example: if
SciStack hands user code a `StringArray` where the user expected a `list`, the blame says
`user_code`. The journal therefore says "raised in your code" rather than "your fault", and
always shows the user frame so the scientist can judge. Errors outside `for_each` (GUI RPCs such
as `put_edge`) use the same rule: a marker exception → `setup`, anything else → `internal`.

## Errors raised inside SciStack but caused by a user's values (`blame = user_value`)

Example: a function returns a DataFrame for some combos and an ndarray for others, so
`pd.concat` inside scifor's result collection (`foreach.py` ~346 / ~2597) fails. The raising
frame is framework code, so the rules above would wrongly say `internal`. This needs a fifth
category, `user_value`: "raised inside SciStack, caused by a value your function produced or
received, fixable by you". It is checked before `internal`. There are two layers:

1. **Pre-flight checks for known cases (precise).** At every point where SciStack processes a
   user's values (combining outputs, splitting rows by schema key, saving, column typing,
   for_columns re-assembly), check the property *before* the operation and raise an
   `ActionableError` subclass, `UserValueError(fn, output, stage, offenders)`. The message names
   the function, the output and the disagreeing combos, grouped:
   `clean_asia returned a DataFrame for 31 subjects but an ndarray for 3 (2204674, 2204690, …).
   SciStack can only combine one type per output.`
   This is the preferred fix, and it is what turns an unexpected exception into the
   `setup`/`user_value` path.
2. **Seam fallback for cases nobody anticipated (honest).** That same code runs inside
   `with user_value_seam(stage="combine_outputs", fn=..., output=...)` (owner: `scifor/failures.py`).
   An exception without a marker that escapes a seam is re-raised as `UserValueError(...)`
   `from` the original, so the original traceback stays in the debug log. The journal wording is
   hedged because SciStack has not confirmed the cause:
   `SciStack couldn't combine the outputs of your function clean_asia (defined at clean_asia.py:12).
   This usually means the function returns different types or shapes for different subjects.
   If its outputs look consistent, this may be a SciStack bug: report run a3f2.`
   The fn definition location comes from `inspect` (MATLAB: `which`). The combo that first
   disagreed is recorded where the seam can see it.
   - Every fallback hit logs `[seam_fallback] stage=… fn=… <ExcType>` to the debug file. Each hit
     is a missing pre-flight check, which becomes a ticket to add one (layer 1) plus a regression test.
   - MATLAB: the same seams in `+scifor/for_each.m` are wrapped in try/catch, rethrown with
     identifier `scistack:userValue` and the original as `cause`. `failure_from_mexception.m`
     maps it to `blame=user_value`.

`user_value` is kept separate from `setup` because the fix is in a different place: `setup` =
config, schema or database state, while `user_value` = the function's inputs/outputs. The
journal headline reads "Fixable in your function: <fn>".

Tests: a mixed-type output (pre-flight check message lists the offending combos), a seam fallback
with a synthetic exception inside `combine_outputs` (blame `user_value`, `__cause__` kept,
`[seam_fallback]` logged), an exception outside any seam (`internal`), and an AST guard that the
framework's `pd.concat`/`np.concatenate` over user values only appears inside a seam.

## Journal shape (golden-tested)

```
── 2026-10-22 17:55:18  clean_asia_columns  (run a3f2) ──────────────
   inputs: LoadedData · 34 subjects
   ✓ 0 completed   ✗ 34 failed   – 0 no data   2m 14s

   ✗ Error in your code — 34 subjects (11 columns each)
     AttributeError: 'StringArray' object has no attribute 'split'
     at clean_asia.py:42 in parse_level
         levels = col.split(",")
     subjects: 2204674, 2204675, 2204676, … (+31 more)

   Nothing was saved for failed subjects. Completed subjects are skipped on re-run.
```

```
── 2026-10-22 17:51:49  connect LoadedData → clean_asia ─────────────
   ⚠ SciStack internal error, not caused by your code
     KeyError: 'node_7'
     Please report this; details: scistack-debug.log, search "run=b91c"
```

Data notes are deduplicated per run or session, e.g.
`note: LoadedData — 12 columns hold text, not numbers (IP_SPINAL_STIMULATION, …); they can't be plotted as numbers.`

## Ownership (NOTE 3/4)

- **scistacklog**: the journal sink, the debug sink (rotation), the `run_id` contextvar that
  stamps debug lines, and `Log.journal(event: dict)`, the only way to write the journal (AST
  guard). It also renders the event dicts. scistacklog stays dependency-free, so events are plain
  dicts.
- **scifor**: `blame` classification, and run start/summary events (it already builds the
  summary event; the journal consumes `FailureGroup.to_dict()`).
- **scidb**: run id (it reuses the provenance `run_id`, so there is no second id), the saved-count
  line, and the `OutputSaveError` mapping.
- **MATLAB**: `+scifor` computes `blame` in `failure_from_mexception.m` (parity test extends
  `test_matlab_failure_record_shape.py`) and emits the summary via `py.scistacklog` → the same renderer.
- **GUI**: emits only `internal`/`setup` one-liners for failed user actions. Everything else
  becomes DEBUG. "Open log" opens the journal. FailuresPanel shows a blame chip and adds a
  "Copy debug details" action, which slices the debug log by run id.

## Stages

1. **Blame.** Add the marker base + `ComboFailure.blame` + MATLAB parity. Tests cover each
   category: user raise, library-called-from-user, user code calling `scidb.load` →
   `NotFoundError` (`setup`), framework `KeyError` (`internal`), `NoDataError`, for_columns
   `ColumnFunctionError` chain. DEBUG log line: `blame=<x> via <rule> (innermost non-library
   frame <file:line>)`, so misclassifications can be diagnosed.
2. **Debug sink.** Rename to `scistack-debug.log`, always DEBUG, `RotatingFileHandler`, add a
   `run=<id>` token via contextvar (`-` outside a run). Tests: the format regex, rotation, the
   token present inside `for_each` and absent outside it.
3. **Journal sink + renderer.** Add `Log.journal`, the event schema (`run_start`, `run_end`,
   `action_failed`, `data_note`), golden-file render tests (Python + MATLAB-originated events),
   an assertion that the debug file is a superset, and an AST guard that no other code writes
   `scistack.log`.
4. **Demotion sweep.** Move `scistack_gui` / `[db]` / `[pipeline]` / `[run_state]` / RPC-count
   INFO lines to DEBUG. Console policy: the Python-script console shows the journal; the GUI
   Output channel keeps DEBUG. Budget test: a refresh/get_* RPC cycle emits zero journal
   events. Delete the INFO inventory from logging-architecture.md, because the journal replaces it.
5. **GUI surfacing.** Blame chip in FailuresPanel and Problems, "Open log" → journal, and
   "Copy debug details for this run". Update GUI manual testing checklist.
6. **Docs.** Rewrite `docs/claude/logging-architecture.md` (two audiences), update
   `error-surfacing.md` (blame), the scistacklog README, and add a user guide page, "Reading your
   SciStack log".

## Diagnostics

- Stage 0 (before coding): a throwaway script reports line counts by layer×level on the
  user's existing logs, so the sweep in stage 4 can be checked against the same numbers.
- Blame decisions log their rule at DEBUG (see stage 1).
- Journal render failures must never break a run: they are caught, logged at ERROR to the debug
  file, and the journal gets a plain fallback line.

## Open decisions

1. File names: `scistack.log` = journal, `scistack-debug.log` = debug (beta, no migration)?
2. Do data-quality notes (the plotdb WARNs) belong in the journal, deduplicated?
3. Is the debug size budget OK (10 MB × 5 per project)?
4. Marker base class name and home (`scifor.failures.ActionableError`?).
