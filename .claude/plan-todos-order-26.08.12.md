# Ordering spec/to-dos-26.08.12.md

Context: user wants to start actually running real data-processing pipelines
in the scistack GUI. Two explicit constraints: (1) good observability while
running real work, (2) don't want to keep "restarting progress" building the
pipeline. Existing docs read: `scistack-gui-frontend-architecture.md`,
`scistack-gui-project-setup-guide.md`, `scistack-gui-pending-constants.md`,
`gui-export-to-plain-python.md`.

Key finding: GUI pipeline state (edges, hidden state, nodes, layout) is
already persisted continuously to the `.duckdb` + `.layout.json` on every
edit — there's no "unsaved draft" to lose. So "restarting progress" risk is
less about missing autosave and more about (a) wiring/semantics bugs forcing
rework of already-built graphs, and (b) not having an escape hatch to plain
code if the GUI state gets confusing.

## Recommended order

**Phase A — canvas chrome / observability surface (low risk, do first)**
1. #1 Runs area, always-visible + expandable
2. #2 Hypothesis/Research Question area, always-visible (do alongside #1 —
   both are canvas-chrome relocations, same sidebar-tab refactor)
3. #3 Paths popup (rename "Project" → "Paths tab" or .env). Note:
   `pyproject.toml [tool.scistack]` already covers Python module/package
   discovery — this to-do is mostly a UI relocation + MATLAB path support,
   not new plumbing.

**Phase B — wiring/graph semantics (fix before building the real pipeline —
changing these later means rewiring graphs you already built)**
4. #10 multiple nodes on same input → EachOf. **DONE 2026-08-13** — see
   `.claude/plan-eachof-multi-input.md`. Turned out variable-type EachOf
   already worked end to end with no code changes needed; the real gap
   was PathInput (which needed the fresh-run execution fix first — see
   `.claude/plan-pathinput-fresh-run-fix.md` and
   `.claude/plan-pathinput-file-vs-directory.md`, both discovered as
   prerequisites along the way). PathInput now supports multiple
   alternate templates per name, resolving to EachOf(...) at execution
   time.
5. #8 Sweep node. **DONE 2026-08-13** — see `.claude/plan-sweep-node.md`.
   Backend is sugar over #10's EachOf resolution in `build_run_inputs`
   (Sweep checked right alongside PathInput, same "resolve missing param
   by name, last" pattern); the real work was the frontend UI —
   `SweepSettingsPanel.tsx` with List and Range (start/end + step-size-or-
   count toggle) modes and a live preview.
6. #9 sub-pipeline input/output marking cleanup — a live toggle (right-click
   a variable node inside the subpipeline's canvas → show/hide outside
   pipeline), not a one-time decision. **DONE 2026-08-13** — see
   `.claude/plan-subpipeline-port-visibility.md` (type-level port
   granularity, `_pipeline_hidden_ports` table + `document_interface`
   filter + right-click context menu on variable nodes). Backend +
   frontend + regression tests (`TestHiddenPorts`,
   `TestHiddenPortsFiltering` in `tests/test_pipeline_scopes.py`) all
   written; tests not yet run by the user.

**Phase C — productivity + data observability (once wiring is stable)**
7. #5 copy/paste nodes — do after Phase B so you're not copy-pasting nodes
   whose input/EachOf semantics are about to change underneath you.
   **DONE 2026-08-13** — see `.claude/plan-copy-paste-nodes.md`.
   `duplicate_pipeline`'s node-cloning logic generalized into a shared
   `_clone_nodes` helper reused by a new `paste_nodes` at selection
   granularity; Cmd/Ctrl+C / Cmd/Ctrl+V plus toolbar Copy/Paste buttons,
   working within one scope and between different hypothesis pipelines.
   Regression tests (`TestPasteNodes`) written; not yet run by the user.
8. #4 default plotting by schema level — this is the other half of
   "observability": seeing your data, not just run status. Depends on
   nodes/wiring being stable since plots are pipeline endpoints.
   **DONE 2026-08-13** — see `.claude/plan-default-plotting-by-schema-level.md`.
   New `GET /api/variables/{name}/plot-data` (scalar/1D-numeric
   eligibility judged from an actual sampled value's Python type, not a
   SQL type-string guess) + a new sidebar `VariablePlot.tsx` using
   Plotly.js (new npm dependency) with a checkbox per schema key
   controlling which dimensions stay distinct vs. get averaged over.
   Tests written (`tests/test_variable_plot_data.py`); not yet run by the
   user. This closes out Phase C.

**Phase D — portability (biggest lifts, not blocking daily use)**
9. #7 import/export pipelines between users — do before #6; exporting/
   importing the GUI's own pipeline spec is a smaller, more self-contained
   problem than code translation, and doubles as a pipeline backup/escape
   hatch (directly serves "don't lose progress"). **DONE 2026-08-13** —
   see `.claude/plan-pipeline-import-export.md`. Export bundles a
   pipeline + everything it (recursively) uses + referenced Constants/
   PathInputs/Sweeps into one portable JSON; import recreates it with
   fresh ids, reusing local Constant/PathInput/Sweep definitions by name
   when they already exist (confirmed design decision). New Export/Import
   buttons in `HypothesisTabs.tsx` + `PipelineNode.tsx`. Tests written
   (`tests/test_portability.py`); not yet run by the user.
10. #6 translate pipelines to Python/MATLAB/bash — biggest lift. A design
    doc already exists (`docs/claude/archive/gui-export-to-plain-python.md`)
    covering how disconnected wirings should be handled at export time
    (skip vs. warn-comment vs. refuse — doc recommends warn-comment). Do
    last; not needed to start real data processing today.
    **DONE 2026-08-13** — see `.claude/plan-pipeline-to-code-export.md`.
    Language auto-detected per pipeline (all-Python -> `.py`, all-MATLAB
    -> native `.m`, mixed -> explicit "not supported yet" error, per
    user decision). No bash generation (not meaningful — bash has no
    scidb SDK to call into; a bash "wrapper" would just be `python
    script.py`, not worth generating). Python path reuses
    `build_backend_pipeline`'s compiled Pipeline directly; MATLAB path
    reuses `derive_target_for_node`/`build_run_inputs` (found to already
    be language-agnostic) plus a new topological sort and MATLAB-syntax
    serializer. Tests written (`tests/test_code_export.py`, including an
    all-MATLAB case via a faked registration — no real MATLAB environment
    available to verify against, flagged as residual risk); not yet run
    by the user. **This closes out Phase D and the full to-do list.**

## Possibly missing from the list

- **Undo/redo (or at least a confirm step) for destructive canvas edits**
  (deleting a node/edge, hiding a pipeline). Not on the list; directly
  relevant to "not restarting progress" if a misclick forces a rebuild.
- **Surface the existing Observability CLI (Inspector) inside the GUI** —
  memory notes a `scidb` CLI Inspector+Mutator was already built
  (all 6 phases, 2026-07-05). Right now that's terminal-only; folding a
  read-only Inspector view into the Runs panel (Phase A) would directly
  serve the "good observability" ask without new backend work — reuse, not
  rebuild.
- **Search/jump-to-node** on the canvas — not urgent today, but once Sweep
  (#8) and multi-input EachOf (#10) make pipelines wider, finding a specific
  node gets harder.
- Not proposing pipeline versioning/snapshots (`.scistack/snapshots/` is
  already flagged "future" in the project-setup doc) — overlaps with #7,
  revisit after #7 ships.

## Open question for user

Do you want #1–#3 (Phase A) done as one combined sidebar-layout refactor
since they touch the same chrome, or sequenced independently?

**Answered 2026-08-12: done together.** Progress below.

## Progress (Phase A: #1–#3, done together, 2026-08-12)

**Status: DONE — user-verified 2026-08-12** ("Things are showing as
expected"). Includes the same-day follow-up below (sidebar full-height,
tab bar removed).

- **#1 Runs.** Moved to bottom left of canvas with a button for a popout
  window. Concretely: `components/RunsDock.tsx` is a new component
  mounted as a React Flow `Panel position="bottom-left"` inside
  `PipelineDAG.tsx` (stacks above the existing hidden-edges panel the same
  way `Controls` already coexists there). Collapsed state is a small pill
  showing total run count, a "N running" badge while anything's in
  flight, and the latest run's status icon/color — all visible without
  any click. Clicking the pill expands an inline 320×360 scrollable panel
  reusing the existing `RunsTab` cards; a ⤢ button in that panel's header
  pops the same content out into a larger centered modal (560px, up to
  80vh) for reading long histories. Removed 'Runs' from the sidebar tab
  bar (`Sidebar.tsx`) entirely — it no longer competes for a tab slot.

- **#2 Hypothesis / Research Question.** `HypothesisTabs.tsx` (already
  the always-visible strip above the canvas, one tab per hypothesis) got
  a second row directly beneath the tab strip: an inline-editable
  "Research Question" field for whichever hypothesis is currently active,
  saved via the existing `update_hypothesis` backend call on blur — no
  new backend work needed, the field already existed. Removed the
  Research Question field and the Evidence For/Against lists entirely
  from the sidebar's `HypothesisPanel.tsx` (dropped per the to-do's
  "maybe remove" suggestion, now that the question itself no longer
  needs a tab switch to see); the sidebar Hypothesis tab now holds only
  the hypothesis statement. Evidence data still exists on the backend
  (`HypothesisInfo.evidence_for/evidence_against`) — only the editing UI
  was removed, consistent with "never delete, mark hidden."

- **#3 Paths.** Removed 'Project' from the sidebar tab bar. Added a small
  "📁 Paths" button to the app header (`App.tsx`) that opens
  `components/PathsPopup.tsx`, a centered modal (same chrome pattern as
  the existing startup-error dialog). The popup has two sections: (1) a
  new **Configured Paths** block — Python modules/packages, MATLAB
  functions/variables/addpath, all read directly from the project's
  `[tool.scistack]` config via a new read-only backend endpoint
  (`GET /api/project/paths` → `api/project.py`, wired through
  `services/project_service.py`, `server.py`'s JSON-RPC handler table,
  and the frontend `api.ts` route table, per the three-places rule in
  `scistack-gui-frontend-architecture.md`); (2) the pre-existing
  discovered-code browser (`ProjectConfigPanel`, retitled internally from
  "Project" to "Discovered Code" to avoid duplicating the popup's own
  "Paths" title).
  **Scope cut:** the to-do's "allow the user to specify paths" /
  ".env file" alternative was NOT built — paths are still edited by hand
  in `pyproject.toml`/`scistack.toml` (the popup is read-only, with a
  hint pointing at the config file). Python and MATLAB paths were
  already fully expressible there (`[tool.scistack]` /
  `[tool.scistack.matlab]`, MATLAB support confirmed present in
  `config.py`); building a GUI write-path (or a competing `.env`
  mechanism) is a separate, heavier feature — revisit as its own to-do
  if hand-editing the TOML turns out to be friction in practice.

**Files touched:**
Backend — `scistack_gui/api/project.py`, `scistack_gui/services/project_service.py`,
`scistack_gui/server.py`.
Frontend — `components/RunsDock.tsx` (new), `components/PathsPopup.tsx` (new),
`components/DAG/PipelineDAG.tsx`, `components/HypothesisTabs.tsx`,
`components/Sidebar/Sidebar.tsx`, `components/Sidebar/HypothesisPanel.tsx`,
`components/Sidebar/ProjectConfigPanel.tsx`, `App.tsx`, `api.ts`.

## Follow-up (same day): sidebar goes full-height, tab bar removed

Once Research Question lived permanently above the canvas, the sidebar's
Hypothesis tab (by then just a bare "hypothesis statement" textarea) and
the sidebar's own tab bar both became pointless — with Runs/Project
already moved out earlier the same day, "Edit" was the only base tab left,
and "Node" already auto-appeared/disappeared on selection with nothing
left for a human to click between. So:

- **Deleted** `components/Sidebar/HypothesisPanel.tsx` outright (git rm) —
  fully unused once its one remaining field had nowhere left to be shown.
  Backend fields (`hypothesis_statement`, `evidence_for/against`) are
  untouched; only the dead GUI editing surface is gone.
- **Sidebar.tsx**: removed the tab-bar UI and all `activeTab` state —
  the component now branches directly on `selectedNode`'s type (`EditTab`
  when nothing trackable is selected, the matching settings panel
  otherwise). No manual switching left to model.
- **App.tsx**: moved the header (title/db name/Restart/Report/📁 Paths/
  schema) and `<HypothesisTabs/>` from spanning the full window width
  down into the `dagArea` column, so `sidebar` is now a direct sibling of
  `dagArea` inside the full-height body row — it runs from the very top
  of the screen to the bottom. The header's buttons end up visually
  shifted left as a pure consequence of their container shrinking to the
  canvas column's width; no extra positioning CSS was needed for that.

**To verify (copy/paste):**
```
cd /workspace/scistack-gui/frontend
npm run build
```
Then launch the GUI against a real project/db and check: the Runs pill
appears bottom-left with live status as a function runs, expand + popout
both work; the Research Question row under the hypothesis tabs saves on
blur and switches content when you change hypothesis tabs; the header's
📁 Paths button opens the popup, shows the resolved pyproject.toml paths
(and MATLAB paths if configured), and the discovered-code browser still
works with its Refresh button.
