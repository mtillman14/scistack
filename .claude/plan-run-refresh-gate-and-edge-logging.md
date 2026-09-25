# Plan: run refresh gate + edge/state logging (2026-09-25)

Source: scidb.log triage of the Stroke-R01-Aim1 session (11:11–11:12).

## Findings
1. One `pandas.read_csv` history node with two outputs: call site `a7e9cc53`
   (fn + PathInput template matchedCycles_withCGAM.csv) recorded both
   GaitSpeedTable (run 09:54 or 10:04) and SymmetryTable. The call site doesn't
   include the output type, so both render on one node. The problem is the data:
   GaitSpeedTable came from the Symmetry file.
2. Spontaneous edge: the one unhidden DB-derived edge regenerates every build.
   The log could not say which edge: ids were logged only at DEBUG.
3. The UnmatchedTable run succeeded (15613 records). The canvas rebuilt mid-save
   (26.5s) because the extension's DuckDB watcher took the GUI's own Python run
   writes for an external change. It only defers during MATLAB runs.

## Changes
- Extension: `MatlabRunTracker` gains Python runs (`beginPythonRun` on
  `run_start`, cleared by `end` on `run_done`, cleared on process replace).
  `noteDbChange` drops watcher refreshes while a Python run is in flight. The
  backend's own post-run `dag_updated` covers it.
- GUI logging (INFO):
  - `build_edges`: list DB-derived (capped), hidden, superseded edges by id.
  - `aggregate_from_scidb`: a call site with >1 output type names them.
  - manual fn node state: node id, outputs, history trust, resulting state.
- Tests: tracker node tests; pytest for the three log lines.
