# GUI manual testing — running to-do list

Features that are built and pass their automated tests but have **not yet been
checked by eye in the running GUI**. Claude keeps this list updated. When you
have checked an item, tell Claude (or move it to **Done** yourself) and note
anything that looked wrong.

Newest first. Each item says what changed, the backend steps (anything outside
the webview: terminal, config files, restarts, `scidb.log`), the frontend
steps (clicks in the GUI), and what you should see.

---

## Before any session: common setup

**Backend**
1. Make sure the committed bundles are current. Every item below is in commit
   `72a7b938` or earlier, and those bundles were rebuilt before committing. If
   you have pulled frontend changes since then, rebuild both targets:
   ```
   cd scistack-gui/frontend
   npm run build
   VITE_BUILD_TARGET=webview npm run build
   ```
2. Open the real project folder in VS Code (for example Stroke-R01-Aim-2) and
   open the database in the SciStack extension.
3. After pulling Python changes, click **Restart** in the GUI toolbar (or reload
   the VS Code window) so the Python server loads the new code.
4. Keep `scidb.log` open (`tail -f scidb.log` in the project folder). Most items
   below name a log line that confirms the backend did the right thing.

**Frontend**
- Plot Studio opens from a Variable node on the DAG canvas (**📈 Plot**).
- If a panel goes blank, the render error is reported to `scidb.log` by
  `ClientErrorBoundary`. Search for `report_client_error`.

---

## 0o. Saved figures are numbered, with a manifest — added 2026-09-22

**What changed:** saving a fanned-out plot used to name each file after its
figure label. A Variant label spells out the constants that define it, so one
`gaitRiteConfig` dict produced a 630-character path — over Windows' 260 — and
three saves failed with `FileNotFoundError`, which reads like a missing
folder. Files are now `<name>_v1.svg`, `<name>_v2.svg`, … and a sidecar
`<name>.figures.json` records what each number holds. Plan: Problem 7 in
`.claude/plan-run-state-and-duplicate-nodes.md`.

**Backend:** `scistack_gui/services/plot_service.py`. **No frontend change.**

**Steps:**

1. Open a plot that fans out over more than one figure (a Variant or an
   ITERATE role). **Save all figures** into a folder. You should get
   `<name>_v1`, `<name>_v2`, … — short names — plus `<name>.figures.json`.
2. Open that JSON. Each entry should name its file, the figure it holds, and
   the full plot settings. *This is the thing that has to make sense to you in
   six months — tell me if it does not.*
3. **Save again without changing anything.** The same files should be
   overwritten; no `_v3` should appear.
4. **Change a setting** (y-limits, grouping, kind) and save again. This time
   new numbers *should* appear, and the old files must be untouched.
5. Save a **single** figure to an explicit filename. It should keep exactly
   the name you gave it, with no number and no manifest.
6. Try saving into a deeply nested folder with a long path. You should get a
   clear "path is N characters, over Windows' 260-character limit" message
   rather than a file-not-found error.

**Judgement call to confirm:** `emg_v1.png` is less self-describing than the
old `emg_subject_1.png`. Short was chosen because the label is unbounded, and
the manifest carries more than a filename could. If the numbering feels worse
in practice for short labels, say so — a hybrid is possible but makes the
numbers unstable, which is why I did not do it.

---

## 0n. Node colours: correct, cascading, and refreshed — added 2026-09-22

**What changed:** three separate defects that together made the canvas
colours untrustworthy. Found by reading `scidb.log` from the 2026-09-22
session. Plan: `.claude/plan-run-state-and-duplicate-nodes.md` (Problems 1, 2
and 4). Docs: `run-option-variants.md` §"The third consumer",
`manual-edges-on-history-nodes.md` §Colour, `matlab-run-completion.md` §5.

1. **A finished step could be red forever.** If a function had ever been run
   under different `distribute`/`as_table` settings, the colour check kept
   counting work against records no run would ever load. `grSides` sat at
   `red — 130 expected invocation(s) not present` through four clean re-runs.
2. **Red did not spread across an edge you drew.** Colours were computed from
   the recorded wiring only, so a step fed by a hand-drawn edge had no
   upstream to inherit from. `loadGaitRiteOneFile` red, everything downstream
   of it green.
3. **Terminal MATLAB runs never asked the canvas to repaint.** 4 refresh
   messages in a 55-minute session against 9 runs, none after a run.

**Backend:** `scidb/provenance_query.py`, `scidb/database.py`,
`scistack_gui/domain/graph_builder.py`, `domain/run_state.py`,
`api/pipeline.py`, `api/run.py`, `matlab_run_watch.py`. Pull and reload the
VS Code window. **No frontend change — no rebuild needed.**

**Steps:**

1. Open the Stroke-R01-Aim-2 database. Look at `grSides`. It should now be
   **green** if its work is complete. In `scidb.log`, the line
   `node grSides: red — 130 expected invocation(s) not present` should be
   gone. *(If it is red with a different, smaller number, that is a real
   shortfall — tell me the number.)*
2. Run a MATLAB node from the canvas and **do not touch anything**. When
   MATLAB finishes, the canvas should repaint on its own — the node's colour
   should update without you clicking Refresh or switching tabs. In
   `scidb.log` look for `[notify] Emitting dag_updated` immediately after the
   run's verdict.
3. Open a plot of a variable the run just wrote. It should show the **new**
   data without you reopening the tab (the plot cache is dropped by the same
   announcement).
4. Make something upstream go red on purpose — the easiest is to add a new
   subject folder on disk that a loader has not loaded yet. The loader should
   go red, **and so should every step downstream of it**, including ones
   connected by edges you drew by hand. Before this change red stopped at the
   first drawn edge.
5. Check the location picker on a loader node still lists what you expect. It
   now uses the same "which records count" rule as loading, so it may show
   slightly fewer entries than before if your database has mixed-run records.
   *(This one is a behaviour change with no automated test — worth a look.)*

**What is NOT fixed yet:** the duplicated `grSides` node is still there
(Problem 3), so grSides still runs twice per click and still blocks the
database while it does. Expect that to look wrong; it is next.

---

## 0m. MATLAB runs report when they actually end — added 2026-09-22

**What changed:** the big one. A MATLAB run dispatched to the MathWorks
terminal used to report **success the instant the script was sent** — before
MATLAB had run a line, and whether or not it then failed. It now reports
what actually happened, via two signals: markers the script writes
(`<db stem>.runs/`), and who holds the DuckDB file. Doc:
`docs/claude/matlab-run-completion.md`; plan:
`.claude/plan-matlab-run-completion.md`.

**Expect this to feel slower, and that is the fix:** the node stays on
"⏳ Running in MATLAB…" for the real duration instead of flicking green.

**Backend:** new `scidb/run_markers.py`, new
`scimatlab/.../+scidb/run_marker.m`, new `scistack_gui/matlab_run_watch.py`,
plus `api/matlab_command.py`, `api/run.py`, `db.py`, `server.py`. Pull,
**reinstall/refresh so MATLAB sees the new `+scidb/run_marker.m`** (it is on
the addpath the generated script sets, so a normal pull is enough), and
reload the VS Code window.

**Frontend:** both bundles rebuilt — new `unknown` run status, and a
✕ stop-waiting control on running MATLAB nodes.

**Steps — the happy path:**

1. Run a MATLAB node. While it runs, check `<your db stem>.runs/` next to
   the `.duckdb`: a `<run_id>.started` file should appear almost
   immediately, with MATLAB's PID in it.
2. The node should stay "⏳ Running in MATLAB…" for the whole run, then go
   green **when MATLAB finishes** — not before. The `.runs` folder should be
   empty again afterwards (reported markers are cleaned up).
3. While it is running, click around the GUI. Requests that need the
   database should now report "MATLAB has the database" in well under a
   second instead of hanging for five. *(This is the papercut; tell me if it
   still feels slow.)*
4. Also while it runs: the canvas should NOT keep refreshing. One refresh
   should land after the run finishes.

**Steps — the paths that used to lie:**

5. **A failing run.** Break a MATLAB function (a typo is fine) and run it.
   The node must go **red**, and the run's row should carry MATLAB's own
   error — e.g. `MATLAB:undefinedFunction: Unrecognized function...`.
   Previously this showed green.
6. **Ctrl-C mid-run** in the MATLAB Command Window. Within ~20 s the node
   should resolve to **`?` unknown** (amber), saying MATLAB stopped without
   reporting and that whatever it wrote is still in the database. It must
   NOT say success, and should not say a plain failure either.
7. **Close MATLAB entirely** mid-run. Same as 6, and the message should add
   "the MATLAB process is gone".
8. **A pyenv failure.** If you can, point `scistack.pythonPath` at a broken
   interpreter and run. After ~2 minutes the node should say **"MATLAB never
   started this run"** — distinguishable from 6/7, which is the reason
   `.started` is written before the preamble.
9. **Stop waiting.** Start a run, then close MATLAB, then click the ✕ on the
   node before the grace expires. The node should resolve immediately as
   cancelled. Confirm the tooltip is accurate: it stops the GUI waiting, it
   does not stop MATLAB.
10. **Clipboard tier.** If you have no MathWorks extension, run a node so
    the script goes to the clipboard, then paste it into MATLAB yourself.
    The node should still report properly when it finishes — previously this
    was a black hole.

**Diagnostics if anything sticks:** Command Palette ▸ **SciStack: Show
MATLAB Run State**. It prints the marker directory, whether each marker
exists, MATLAB's PID and the last lock probe for every run being watched.
Please paste that output if a node hangs.

---

## 0l. Every plot opens its own tab + MATLAB across databases — added 2026-09-22

**What changed:** two things, both from the same multi-session work
(`.claude/plan-multi-session-tabs.md` stages 4 and 5).

1. **Plot tabs no longer reuse one tab.** Plotting a second variable used to
   retarget the open Plot Studio, destroying the figure you were looking at.
   Every plot now opens its own tab, titled with its database.
2. **MATLAB is treated as the one shared resource it is.** Each database
   writes its own generated script file, and a Run is refused while another
   database's MATLAB run is being dispatched.

**Backend:** `extension/src/{plotPanel,dagPanel,matlabTerminal,
matlabConnectionGate,sessionCore,session}.ts`; bundles rebuilt. Pull and
**reload the VS Code window**.

**Frontend:** `PlotRoot.tsx` simplified (no retarget path);
`PlotStudio.tsx` badges its database beside the title.

**Steps — plot tabs:**

1. On one database, right-click a Variable ▸ **Plot**. Then plot a *second*
   variable. You should now have **two** plot tabs, both open, the first
   figure untouched. (Before: one tab, first figure gone.)
2. Each tab's title reads `Plot — <Variable> · <db>.duckdb`, and the
   studio's own header shows the database in grey after the shape badge.
3. With two databases open, plot from each. Start a **Save** in one — only
   that tab shows "Saving…", and only that tab returns to normal when it
   finishes. (The notification must not cross databases.)
4. Close a canvas tab — its plot tabs close with it; the other database's
   plot tabs stay.

**Steps — MATLAB (only if you use the MATLAB path):**

5. Run a MATLAB node. In the SciStack Output Channel, the dispatch line
   should name a per-database script:
   `wrote N-char script to …/scistack_run_<8 hex chars>.m`. Two databases
   must show two different filenames.
6. **Only the MathWorks terminal is shared.** If your MATLAB runs go through
   the *sidecar* (no MathWorks extension, or no MATLAB terminal open), each
   database has its OWN MATLAB process already — start a long run in A, then
   run in B: both should proceed in parallel, neither blocked. This is the
   case that matters most; an earlier version of the gate wrongly blocked it.
7. **If you do use the MathWorks terminal:** with a MATLAB run just
   dispatched from database A, immediately click Run on a MATLAB node in
   database B. You should get: *"MATLAB is running A.duckdb right now… wait
   for that run to finish"*, and B's node must return to idle rather than
   sticking on "running".
8. **Known limit, please confirm it behaves as described rather than
   worse:** that refusal covers the dispatch window only. Clicking Run in B
   well into a long MATLAB *terminal* run in A is NOT refused — nothing
   tells VS Code when a terminal run ends. If you hit this in practice, say
   so; the fix is run markers written by MATLAB itself
   (`.claude/plan-matlab-terminal-run-tracking.md` Stage 2).

---

## 0k. Several databases open at once — added 2026-09-22

**What changed:** the extension now opens **one tab per database**. Each open
`.duckdb` gets its own Python server, its own canvas tab, its own plot tabs
and its own file watcher. Before, opening a second database killed the first
one's server and reused its canvas — the graph changed but the header kept
the **old** filename (this is the bug you reported). Doc:
`docs/claude/gui-multi-session.md`; plan:
`.claude/plan-multi-session-tabs.md` (Stages 0-2).

**Backend:** `extension/src/{session,sessionCore,extension,dagPanel,plotPanel,
pythonProcess,serverArgs,panelRegistry}.ts`; both vite bundles and
`dist/extension.js` rebuilt. Pull, then **reload the VS Code window** (not
just Restart Python — the extension host itself changed).

**Frontend:** the canvas header reads the database name injected into the
webview instead of fetching it once on mount.

**Steps:**

1. **Open Pipeline** on database A. Note the tab title: it should now read
   `SciStack — A.duckdb`, not `SciStack Pipeline`. The header inside the
   canvas should name A with no "loading…" flash.
2. **Open Pipeline** again, on a *different* database B. You should get a
   **second tab**, `SciStack — B.duckdb`, with A's tab still open and still
   showing A's graph and A's name. *(This is the reported bug: before, there
   was one tab and it showed B's graph under A's name.)*
3. Switch back to A's tab. Its graph, its Runs dock and its header must all
   still be A's. Run something small in A, then in B — neither run should
   appear in the other's console.
4. **Open Pipeline** on A a third time. No new tab and no new server: A's
   existing tab is revealed. The Output Channel says
   `[session] A.duckdb is already open — revealing its tab`.
5. Bottom-left status bar: one `$(database) SciStack: <name> (+1)` item that
   follows whichever tab you are looking at. Click it — you get a database
   picker. (Before, every open left a stale item behind.)
6. Command Palette ▸ **SciStack: Show Open Sessions**. The Output Channel
   lists both databases with their project roots, debug ports, and plot-tab
   counts, the focused one marked `*`.
7. Check the Output Channel generally: every line should be prefixed
   `[A.duckdb]` or `[B.duckdb]` so the two servers can be told apart.
8. Plot a variable from A's canvas and one from B's. Each plot tab's title
   ends with its database (`Plot — StepLength · A.duckdb`) and each talks to
   its own server. Save a figure from A's tab — only that tab leaves
   "Saving…".
9. Close A's canvas tab. A's server and A's plot tabs close with it; B is
   untouched. The status bar drops to B alone.
10. **If you use a multi-root workspace:** open a database from each folder
    and confirm in **Show Open Sessions** that each one's `project=` is its
    *own* folder. Previously both got the first folder, so the second
    discovered the wrong code.

**What to look for:** any place that still says the wrong database name —
that is the class of bug this change is about.

---

## 0j. Plot a CSV with no database open — added 2026-09-22

**What changed:** right-click ▸ **Plot CSV** no longer needs a pipeline open.
It starts a database-less `--plot-only` server (no project init, no code
discovery, no `configure_database`) shared by every CSV tab. Plan Stage 6.

**Backend:** `scistack_gui/server.py` (`--plot-only`, `--log-file`, `--db`
now optional), `api/handlers.py` (`Handler.db_optional`), `api/plot.py`.
Pull and reload the window.

**Frontend:** "Add to pipeline" is hidden on a CSV tab.

**Steps:**

1. With **no** pipeline open, right-click a `.csv` in the Explorer ▸
   **Plot CSV**. A plot tab opens and draws. (Before: "Open a pipeline
   first".) The Output Channel shows `[plot-only] Spawning: … --plot-only`
   and `No database — CSV plotting only`.
2. In that tab: picking axes, changing the plot kind, **Export code** and
   **Save** all work. **Add to pipeline** is absent — a CSV has no project to
   write an endpoint into.
3. Right-click a second `.csv`. It reuses the same plot-only server (no
   second `Spawning` line).
4. Now open a real pipeline as well, and check **Show Open Sessions**: the
   plot-only server is listed with `db=(none — plot only)` and is *not*
   offered by **SciStack: Switch Database**.
5. Command Palette ▸ **SciStack: Plot Variable…** with only CSV tabs open
   should say a database is needed, not fail obscurely.
6. The plot-only server's log goes to the extension's own storage folder, not
   next to your CSV. Check no stray `scidb.log` appears beside the data.

---

## 0i. Lines span the innermost grouping layer only — added 2026-09-21

**What changed:** a joined line — a "Show sample" line and a spaghetti's own
polyline alike — now runs along the **innermost** grouping layer only and
never crosses a bracket (the layers above it). Before, a subject's line ran
across every x position of the panel: bars grouped `[ColName, session]`
joined `pre·A → pre·B → post·A → post·B`. The auto-join rule now asks
"does the shown key recur across the innermost layer?" (`roles.
line_recurrence`), so a synthetic innermost layer (`ColName`, `Variant`)
joins where it used to refuse ("no grouping layer is a schema key").
Plan: `.claude/plan-sample-line-span.md`.

**Backend:** `scistackplot.roles` / `reduce` / `render.*` / `codegen` /
`capability`; pull and restart the GUI server.

**Frontend:** both bundles rebuilt (the Join tooltip names the span and the
brackets); pull, rebuild if you build locally.

**Steps (a table-valued variable, Plot Studio):**

1. Bar +/- Error, 2+ columns selected, Grouping `ColName` (first / innermost)
   then `session`; `subject` + `trial` collapsed. Tick `subject` under Show
   sample, Join = Auto. The Auto option reads "(lines)"; each subject's
   line joins its columns **inside one session bracket** and stops there —
   no line runs from the last column of `pre` to the first of `post`.
   Hover the Join points label: the tooltip names `ColName` as the layer a
   line spans and `session` as what it never crosses.
2. Move `session` above `ColName` in the Grouping list (session innermost).
   Lines now run `pre → post` inside each column bracket. The granularity
   sentence under the checkboxes reads "Lines join the points across
   session within each ColName."
3. Grouping `ColName` only: lines across the columns (this used to be
   points with "no grouping layer is a schema key").
4. Set **Colour by** on the OUTER layer (the bracket): lines unchanged, bars
   painted. Colour by the INNER layer: the runs split per colour, so with
   two colour levels each run is one point (known, unchanged; set "Colour
   points by" `subject` to get lines across the colours).
5. Save PNG: the export draws the same runs (one per subject per bracket).
6. Spaghetti kind, Grouping `subject` (lines), `session`, then a third
   layer every subject has at every level (a speed, a variant, `ColName`):
   each subject's polyline is drawn once per bracket, never across it.
   Save PNG: same (`units=_run` in the generated code).
7. scidb.log at DEBUG: `overlay join: shown=[…] span='ColName'
   brackets=['session'] … -> True (…)` and `sample overlay: N point(s) in
   panel …; R run(s) over S identity(ies), span=… brackets=…` with
   R = S × number of brackets.

---
## 0h. "Show sample" survives colouring by the ONLY grouping layer — added 2026-09-21

**What changed:** with one grouping layer (e.g. `ColName`) that is also the
**Colour by** layer, there is no tick layer left, so the marks sit at one
unlabelled x position. The figure carried no `x_order` for that position,
the axis read as numeric, and both renderers dropped the Show-sample overlay
silently (plotly still drew the bars off the empty-string category; the
matplotlib export placed them at NaN). `reduce` now lists the single
unlabelled level as `x_order`, and a shared renderer guard
(`render.base.sample_dropped_reason`) WARNs in scidb.log whenever an
overlay is built but not drawn.

**Backend:** `scistackplot.reduce` / `render.base` / `render.mpl` /
`render.plotly_`; pull and restart the GUI server.

**Frontend:** no rebuild needed.

**Steps (a table-valued variable, Plot Studio):**

1. Bar +/- Error, 2 columns selected, Grouping `ColName` only, Variant and
   one schema key on Separate figures, `subject` + `trial` collapsed. Tick
   `subject` under Show sample, Join = Lines. Points + lines appear.
2. Set **Colour by** to `ColName`. The two bars take two colours at one
   tick, and the points/lines are **still drawn** inside their bars (this
   used to make them vanish). Set "Colour points by" `subject`: same, with
   the overlay in the subject palette and a subject legend.
3. Set Colour by back to none: overlay unchanged.
4. Save PNG: the export shows both bars AND the overlay at the single tick.
5. scidb.log: no `sample overlay panel … NOT drawn` WARN line during any
   of the above.

---

## 0g. No on-mark labels under a "Show sample" overlay — added 2026-09-21

**What changed:** ticking any Show-sample key on a **Bar +/- Error** plot used
to label every bar with its level name (`pre`, `post`, `stim · pre`, …).
That text was the hover payload riding in the plotly trace's `text` field,
which plotly paints onto bars. Hover content now travels in `customdata`
(hover-only for every trace type), in the scistackplot renderer.

**Backend:** none — the fix is in `scistackplot.render.plotly_`; pull and
restart the GUI server.

**Frontend:** no rebuild needed.

**Steps (aim2 example, Plot Studio):**

1. Bar +/- Error, Grouping `session`, nothing ticked under Show sample. The
   bars carry no text. Hover a bar: the tooltip names the level and value.
2. Tick `subject` under Show sample. Points appear beside the bars and the
   bars still carry **no text**. Hover a bar: same tooltip as before. Hover a
   point: level, `subject=…`, value.
3. Switch the kind to Box, Violin, Strip, Spaghetti with `subject` still
   ticked: no level names drawn on or beside any mark in any kind; hover
   still names the level.
4. Save PNG (matplotlib path): the export shows no labels on the marks either.

---

## 0f. "Show sample" coloured by its own key — added 2026-09-21

**What changed:** the overlay points (and their lines) can be coloured by one
of the shown keys, independently of the Grouping colour: bars coloured by
intervention group, one colour per subject on top. With it, a joined line runs
ACROSS the marks' colours (pre → post inside one group) — which is what used
to make Lines / Auto (lines) draw nothing when `session` was the coloured
layer. Doc: `docs/claude/show-sample-overlay.md` ("The overlay's own colour").

**Backend:** none beyond the common setup. `scidb.log` gets
`sample overlay coloured by 'subject': N level(s) — joined across colour levels`
at INFO on every resolve.

**Frontend** (the assignment this was built for)
1. Plot Studio → a scalar variable → kind **Bar ± error**. Grouping:
   `session` and `Demographics.InterventionGroup`, `session` ticked as the
   colour. Factors: `subject` and `trial` **Collapse**, `speed` **Separate
   figures**, `ColName` **Separate panels**.
2. Show sample: tick `subject`. Join points: **Auto (lines)**. Before this
   change nothing was joined; now still nothing — each subject's pre and post
   points sit in different-coloured bars, and a line has no colour to be.
3. **Colour points by** → `subject`. Every subject now gets its own colour,
   the same in every panel, and a line joins its pre bar to its post bar
   inside each intervention-group tick. The bars keep the session colours.
4. The legend lists the sessions, then the subjects, titled
   `session / subject`. Click a subject in the legend: that subject's points
   and lines vanish in every panel; the bars stay.
5. Untick the colour on `session` and tick it on `InterventionGroup` instead.
   The lines now run across the session ticks inside one colour slot; the
   subjects keep their colours.
6. **Colour points by** → **Mark's colour**. Back to step 2's figure.
7. Tick `trial` in Show sample too. The dropdown now offers `trial` as well;
   pick it — one colour per trial.
8. **Export code**, save, compare with the preview: same colours per subject,
   same lines, same two-block legend.

---

## 0e. Every GUI method through the handler tables — added 2026-09-21

All ~95 GUI methods are now declared once (`scistack_gui/api/*.py` tables)
and both transports are derived from the rows. Behaviour is meant to be
identical, except where the two copies had drifted and the table settles
it the same way for both:

* the extension's single-node **Run** now passes the clicked node id to the
  run thread (it derived targets by NAME before) and refuses a glue node;
* the extension's canvas refreshes after **create / save / delete glue**
  (only the browser did before);
* the browser can now **cancel / force-cancel a run**, **restore a hidden
  hypothesis**, open the **Provenance panel** and the node **location
  tree** (all were "Unknown method" in the browser);
* the browser now sends the hypothesis scope with **delete Parameter /
  PathInput** and **hide / unhide a Parameter value** (it hid in root
  regardless before).

Backend: nothing to configure. Frontend: **rebuild both vite targets**
(`api.ts` changed; done in the commit, but rebuild if you pull source).

- [ ] VS Code extension: on a hypothesis canvas that has two placements of
      the same function wiring with different constants, click Run on ONE —
      only that node's variants run (the log names its node id).
- [ ] VS Code extension: create a glue node — the canvas shows it without a
      manual refresh; edit + save its body — the consumer turns red.
- [ ] Browser build: start a long run and Cancel it; Force cancel a stuck
      one — both buttons work (they threw "Unknown method" before).
- [ ] Browser build: delete a hypothesis tab, then restore it from the
      hidden list.
- [ ] Browser build: open 🔍 Provenance on a variable node; open View
      Schema Locations on a function node.
- [ ] Either: a bad request (e.g. Run with no function) is refused with a
      message, not "database locked".

## 0d. Hypothesis-scoped settings and hides — added 2026-09-20

A node's run options / schema level / column selections and a Parameter's
unchecked values are now statements made ON a canvas: root's apply on root,
a hypothesis's on that hypothesis; a duplicate copies what it was made from.
Before, every node-config write was global (two placements of one wiring
shared their run options) and a run saw every hypothesis's hidden values at
once. Settings saved before this build were global and still apply on every
canvas until that canvas changes them.

Backend: nothing to configure. Frontend: unchanged (the scope is read off the
node id the canvas already sends).

- [ ] Root canvas: set Distribute on a node, then Duplicate the hypothesis
      (root). The copy shows Distribute ticked (copied).
- [ ] In the copy, untick Distribute. Root still shows it ticked; the copy
      shows it unticked after a reload (its own statement).
- [ ] Run the node from the hypothesis — the log's run options say
      `distribute=False`; run from root — `distribute=True`.
- [ ] Duplicate the copy. The second copy shows the unticked state; ticking
      it back in the first copy leaves the second unticked.
- [ ] Hypothesis: uncheck one value of a Parameter. Run the consumer from
      root — the value still runs; from the hypothesis — it is excluded.
- [ ] Settings saved BEFORE this build (legacy `global` rows) still show on
      every canvas until you change them on one.

## 0c. Plot Studio over both transports (handler table) — added 2026-09-20

The plot family's JSON-RPC methods and HTTP routes are now built from ONE
table (`scistack_gui/api/plot.py`, `PLOT_HANDLERS`), and RPC params are
validated through the same pydantic model as the HTTP body. Behaviour is
meant to be identical; this is a regression check, plus one real fix: the
browser build had no route for **saving named variant sets**
(`plot_variant_sets_save` was missing from `frontend/src/api.ts`).

Backend: nothing to configure. Frontend: **rebuild both vite targets**
(`api.ts` changed).

- [ ] VS Code extension: open Plot Studio on any variable — the panel
      describes, resolves, and the location tree opens (four different
      RPC methods, all through the table).
- [ ] VS Code extension: Save figure (one) and Save data (CSV) — the job
      id comes back at once and progress notifications arrive.
- [ ] VS Code extension: with MATLAB attached and holding the DB, resolve a
      plot — it still renders (the self-managed hold policy came through
      the table).
- [ ] Browser build (`scistack-gui` CLI): name a variant set in the DAG
      popup and save it — before this it threw `Unknown method:
      plot_variant_sets_save` in the console; now it persists and survives
      a reload.
- [ ] Either transport: force a webview render error (or POST
      `/api/client-error` with `{"where":"x","message":"y"}`) — the line
      reaches `scidb.log` at ERROR, with no database needed.

## 0b. Run options reach the compiled pipeline and both code exports — added 2026-09-20

A step's saved **run options** (`distribute`, `as_table`) were honoured by the
single-node **Run** button and by the MATLAB command, but the **compiled
pipeline** (Run Pipeline / Run Scope) and **both code exports** hardcoded
`distribute=False, as_table=None`. So a node set to distribute ran
non-distributed from the pipeline button, and an exported script re-ran the
pipeline as a *different* call — writing a second record at every location.
All three now read the node's config (`scidb.foreach_config.RunOptions`).

**Backend**
1. Nothing special. Keep `scidb.log` open.

**Frontend**
1. Pick a function node whose output makes sense distributed (a loader that
   returns one row per trial). Open its settings panel and tick
   **distribute**.
2. Click **Run Pipeline** (not the node's own Run button).
3. In `scidb.log`, find the `resolve_distribute_target: '<key>'` line for that
   function — that is scifor confirming the option took effect. Before this
   fix the line was absent on this path.
4. Now export the pipeline to Python (**Export code**). The generated
   `for_each(...)` for that step must carry `distribute=True`.
5. Export to MATLAB. The generated `scidb.for_each(...)` must carry
   `'distribute', true`.
6. If the step instead uses `as_table`, the same two exports must carry
   `as_table=[...]` / `'as_table', ["..."]`.

**What you should see:** the same run options in all four places — the node
panel, the pipeline run's log, and both exported scripts. A step with no
options set emits neither argument (unchanged).

---

## 0a. Intent vs fact: run origin, "not reflected" marker, per-column run option — added 2026-09-19

Branch `refactor/intent-and-fact` (Stages 4–6 of `.claude/plan-intent-and-fact.md`;
model in `docs/claude/intent-and-fact.md`). Python + both bundles changed.

**Backend**
1. Open a project whose database has an already-run function with a
   column selection saved on its node (or make one: item 11 below).
2. In `scidb.log`, on the first GUI start after pulling, confirm the one-time
   import ran: `[intent_store] imported column selections from N node config(s)`.
   The selections still show on their nodes (Inputs section) — the storage moved
   from `_node_config` to `_intent`, the panel should look identical.
3. Run that node from the GUI. Confirm in `scidb.log`:
   - `[execution] '<fn>': bindings — value: <Type> ("col") · ...` — one line
     naming EVERY signature parameter (unbound ones say `(unbound)`).
   - no `[selector-lost]` and no `[selector-dropped]` line.
4. Run the SAME function from a Python script or the MATLAB prompt with a
   different (or no) column selection. Then click Restart / refresh the DAG.
   - `scidb.log`: `[graph_builder] fn__...: saved column selection not reflected
     by its last run — <param>: stated ..., last run (script) bound ... [script_run]`.
   - `[selector-dropped]` WARN at the start of that script run if it bound the
     whole variable where the GUI's last run had a selection.

**Frontend**
- [ ] After step 4, the node's column chip on the canvas turns amber with a `!`
      and its tooltip says the last run came from a script and did not use it.
- [ ] The Inputs section shows an amber **NOT REFLECTED** note under that
      parameter, wording: "Not used by the last run: it ran from a script, which
      reads source only, and bound ⟨…⟩. This selection (…) applies when <param>
      is run from here."
- [ ] Run the node from the GUI again → refresh → the amber marker is gone.
- [ ] Change a selection on a node that HAS run from the GUI, don't run →
      refresh → note reads "Changed since the last run … Run again to apply …".
- [ ] A node whose function has never run shows "Not run yet — … will apply on
      the first run from here."
- [ ] Run options section: for a node with a `for_columns` (per-column)
      selection, a read-only line **Run once per column** appears, naming the
      parameter(s) and pointing at the Inputs section. It is absent otherwise.
- [ ] Provenance / `scidb variants`: a per-column run's `run_options` reads
      `distribute=false, for_columns=[<param>]`; a whole-table run of the same
      function reads `distribute=false` — the two are distinguishable rows.
- [ ] Duplicate a hypothesis containing a node with a column selection: the copy
      shows the same selection; changing it on the copy leaves the original.
- [ ] MATLAB: generate a run command from the GUI. The script contains
      `py.scidb.intent.set_ambient_origin('gui');` after the pyenv preamble and
      `...('script');` at the end on both the success and the catch path. After
      running it, the node's last run shows as a GUI run (no amber marker).
- [ ] **Pins survive the panel.** In Plot Studio, add/rename a variant row,
      close the panel, reopen the same variable: the rows are back
      (`scidb.log`: `[plot] <Var>: N stored variant pin(s) replace the default`).
      Open a DIFFERENT variable: its own default, not the other's pins.
- [ ] **MATLAB parity.** For a node whose selection came from a Python
      `Var["col"]` run (no node config), generate the MATLAB command: it loads
      `Var("col")`, not `Var()`.
- [ ] **CLI.** `scidb --db <path> intent <fn>` prints the intent-vs-fact table;
      `--origin script` lists the GUI statements under "not read". `scidb trace
      <Var> <key=val> --intent` appends one block per function in the chain.
- [ ] **Hidden state and edges still work** after the storage move: hide a
      value on a Parameter node, hide/unhide an edge, draw a manual edge,
      stage a pending constant — each behaves as before, and `scidb.log`
      shows `[intent_store] import <name>: N row(s) carried over` once on
      the first start.

---

## 0. Colour is paint + "Show sample" on spaghetti — added 2026-09-21

Backend: `roles.grouping_layers` keeps the coloured layer in the ticks;
renderers no longer dodge (`render.base.MARK_SPAN`, plotly `offsetgroup`);
codegen emits `dodge=False`; `OVERLAY_KINDS` includes spaghetti and the
overlay is placed on each point's line (`SAMPLE_LINE`). Frontend: Grouping
hint / colour-radio wording only. Bundles rebuilt 2026-09-21.

- [ ] **Colour never moves a bar.** Bar plot, grouping `[ColName, session,
      subject]` (innermost first), trial collapsed, one figure per speed.
      Note the bar layout. Tick the colour on `session`, then on `subject`,
      then off. The bars, brackets and tick labels must not move at all —
      only the bar colours and the legend change. `scidb.log` shows the new
      `grouping: ticks=[...] ... colour=X paints only` line with the SAME
      `ticks=` on every one of those resolves.
- [ ] **Coloured innermost layer.** Colour the FIRST grouping entry: same
      layout as uncoloured, bars painted by that layer, legend added.
- [ ] **Save figure** (matplotlib export) and **Generate code** for the
      coloured figure: same layout as the preview (one bar per tick, full
      width). The generated call carries `dodge=False`.
- [ ] **Show sample on a spaghetti.** Spaghetti, lines = subject, ticks =
      session, trial + cycle collapsed. The "Show sample" section is now
      enabled. Tick `trial`: small points appear ON each subject's line
      (at that subject's sideways shift), not on the tick centre, and not
      joined (trial belongs to one session). Tick `cycle`: many more points,
      still on their subject's line.
- [ ] **Show sample on a spaghetti with subject collapsed** (the "every point
      is one subject" shape: lines = speed or group across sessions, subject
      collapsed and drawn one line each): trials land on their subject's own
      line inside the group.
- [ ] **Colour points by** still works on a spaghetti overlay (own colour per
      shown key, second legend block).
- [ ] **Four grouping layers with one coloured** now says "At most 3 labelled
      tick layers" (colouring no longer frees a layer) — expected.

---

## 1. Save data (CSV) — the rows a plot is drawn from — added 2026-09-19

**What changed:** a **Save data (CSV)** button in the Plot Studio writes the
long table the current plot is drawn from. Every figure of a
separate-figures fan-out goes into one file, with the figure key (for
example `speed`) as a column. By default it holds the **plotted sample**, so
the file is exactly what the bars / boxes / points are computed from. A depth
chooser can keep lower collapsed levels (for example trials, cycles)
unaveraged instead. Scalar plots only: a raw 1-D (line/band) or 2-D plot
greys the button out with the reason. Uncommitted. Doc:
`docs/claude/plot-data-export.md`.

**Backend**
1. Restart the GUI (**Restart**) so the new Python code is loaded. The
   bundles and `extension/dist/extension.js` were rebuilt 2026-09-19. If
   you pulled instead, rebuild all three (see the common setup, plus
   `npm run build` in `scistack-gui/extension`).
2. After a save, run `grep "plot-data\|saved data of" scidb.log`. Expect
   `[plot-data] <measure>: N figure(s), chain … -> subject (sample),
   depth=subject -> R row(s) x C column(s) [...]` and
   `[plot] saved data of <measure> to <path>: R row(s) …`.

**Frontend**
1. Open a scalar variable in the Plot Studio. Group `session`, set
   `speed` (or any key) to Separate figures, and collapse `subject`,
   `trial` and `cycle`. Choose Bar.
2. Click **Save data (CSV)**. A chooser should open, listing
   "subject — the plotted sample (cycle, trial averaged) (default)",
   "down to trial (cycle averaged)" and "down to cycle (raw — nothing
   averaged)". Each option shows the column header underneath.
3. Keep the default and click **Save CSV…**. The file dialog should filter on
   CSV. Save, and the notice should say
   `Saved <path> — N row(s) in …s`.
4. Open the file. Expect columns `subject, session, speed, <measure>`, one
   row per subject × session × speed, and subject IDs like `01` written as
   `01`. The mean of each session's rows should equal the bar height.
5. Save again with "down to trial". Expect a `trial` column and more rows.
6. With nothing collapsed, the button saves straight away with no chooser.
   With **Weight by N** ticked, there's also no chooser, and the file has
   every collapsed level.
7. Switch a 1-D variable to a line or band. The button should be greyed out,
   and hovering it should explain why (scalar plots only).
8. **Struct / table variable** (for example a per-muscle peak table). Open it
   with the fields as panels, then click **Save data (CSV)**. The chooser
   should show a **One column per field (ColName)** checkbox, checked by
   default, and the header preview should list the field names as columns.
   Save and open the file. Expect one column per field (`subject, session,
   RTA, RMG, …`). Uncheck the box and save again: expect a `ColName`
   column and one row per field instead. Set `ColName` to Collapse: the
   checkbox should disappear at the default depth and come back at the
   deepest ("raw") depth.
   In `scidb.log`, expect `[plot-data] one column per ColName: … field
   column(s) [...]`.

## 2. Every plot kind draws the sample (schema-level parity) — added 2026-09-19

**What changed:** scatter and strip no longer average the sample (for
example subjects) into one point. They draw one point per subject, the same
rows a bar summarises and a box draws. Line plots draw one line per subject.
Spaghetti plots draw one line per subject inside each line group when the
subject recurs across the x ticks. When it doesn't (for example trials
under a session tick), each line is the mean, and `scidb.log` says so.
Uncommitted. Doc: `docs/claude/grouping-and-collapse.md`.

**Backend**
1. Restart the GUI.
2. For the spaghetti fallback, run `grep "spaghetti draws the mean" scidb.log`.

**Frontend**
1. Scalar variable: group `session`, collapse `subject` (and `trial`),
   choose Scatter. Expect one point per subject at each session, not one
   point. Switch to Box: the box should be built from those same points.
2. 1-D variable: colour a group layer, collapse `subject` and `trial`,
   choose Line. Expect one thin line per subject in its group's colour.
   Subjects should not get dash styles, and the legend should not list them.
3. Spaghetti with an intervention group as the first grouping layer and
   session second, subject collapsed: expect one line per subject, inside the
   group's colour.

## 3. Declared schema level order (`[schema_keys]`) — added 2026-09-19

**What changed:** levels follow `[schema_keys]` in `scistack.toml` everywhere:
- loaded tables
- `for_each` iteration
- GUI level lists
- plot axes, legends and facets
- exported seaborn code

Edits to the file now apply **without a restart**. Commit `72a7b938`. Doc:
`docs/claude/config-file-formats.md` (`[schema_keys]` section).

**Backend**
1. Add or confirm a declaration that differs from alphabetical order, for
   example:
   ```toml
   [schema_keys]
   session = ["BL", "POST", "FU"]   # use your real levels
   ```
2. Restart once (**Restart** button) so the new code is loaded.
3. Run `grep schema_order scidb.log`. Expect
   `[schema_order] using <path>/scistack.toml (found from the working directory …)`
   and `declares level order for session (3)`. If you see
   `no project config found from …` instead, the server's working directory is
   not the project folder. Note the paths it lists.

**Frontend**
1. Open Plot Studio on a variable. Put `session` on the x axis (Grouping), then
   try it as the colour, then as **Separate panels** (Factors). Each time, the
   ticks, legend and panel order should read `BL, POST, FU`.
2. Open **Schema keys** (location picker) and check that the tree lists the
   levels in the declared order.
3. **Live edit:** without restarting, swap two levels in `scistack.toml`, save,
   and change any control in Plot Studio (or reopen it). The order should flip.
   The log should show
   `table cache: declared [schema_keys] level order changed — dropping N built table(s)`.
4. **Export:** click **Export code**. The generated function should contain
   `order=_x_order` / `hue_order=_hue_order` / `col_order=_col_order` lines,
   and the saved figure should match the preview's order.
5. **for_each:** run a step whose `session` input is "all levels". The run log
   and output rows should come out in the declared order.

---

## 4. "Show sample" overlay — added 2026-09-19

**What changed:** on bar, box, violin, scatter and strip plots, collapsed keys
can be drawn as points inside each mark. Points join into lines automatically
when they are repeated measures. Doc: `docs/claude/show-sample-overlay.md`.

**Backend:** none beyond the common setup.

**Frontend**
1. Plot Studio → a scalar variable → kind **Bar**. Put `session` in Grouping and
   set `subject` and `trial` to **Collapse** in Factors.
2. Open the **Show sample** section and tick `subject`. You should see one point
   per subject inside each bar. The points should be spread deterministically,
   not jittered randomly.
3. Tick `trial` as well. You should see one point per trial, and the subjects
   should still be implied.
4. Check the joining. With `subject` shown and `session` on x, lines should
   join each subject's points across sessions (repeated measures). Use the
   **Join points** control to switch lines off and on.
5. Add a colour group. The points should dodge with their bars, and each
   point's colour should match its bar.
6. Switch the kind to Box, then Violin, then Scatter. The overlay should follow.
   Kinds that can't show it should say why instead of drawing nothing.
7. Click **Export code**, save, and compare the saved figure with the preview.

---

## 5. Grouping + Collapse roles (new plot role model) — added 2026-09-19

**What changed:**
- The old X / COLOR / AGGREGATE / FREE roles are replaced. The **Grouping** list
  is ordered innermost first, and one entry can be the colour.
- In the **Factors** pane each key is **Separate figures**, **Separate panels**
  or **Collapse**.
- Collapse keys form a nested chain; the deepest collapsed key is the sample.

Doc: `docs/claude/grouping-and-collapse.md`.

**Backend:** none beyond the common setup. Old saved `plot_` endpoints that use
the old role strings now raise `LegacySpecError`. Re-open and re-save them.

**Frontend**
1. Open a scalar variable. Check the defaults:
   - the deepest schema key is in Grouping;
   - the other keys are **Separate figures**;
   - the kind is Scatter;
   - the ← / → buttons step through the figures.
2. Add a second key to Grouping and use **Move inward** / **Move outward**. The
   nested x brackets should reorder to match.
3. Tick **Label this layer by legend colour** on one layer. That layer should
   leave the ticks and become the colour.
4. Set a key to **Collapse**. With **Mean** / **Median** and **Spread** you get
   bars with error bars. Tick **Weight by N** and check that the values change
   the way pooled averaging would.
5. Box, violin and band need a sample key. Remove every collapsed key and check
   that the GUI explains why.
6. Pick **Spaghetti** with fewer than 2 groups. It should be refused with a
   reason.
7. Check the uncoloured series (line kinds). They should get dash styles, and a
   warning should appear in `scidb.log` when there are more than the dash
   cycle holds.

---

## 6. Spaghetti plot kind — added 2026-09-16

**What changed:** `PlotKind.SPAGHETTI` draws markers plus one line per subject
across a categorical (possibly nested) x. Each line is offset by a fixed amount
so it ends on its own markers. Doc: `docs/claude/spaghetti-plot.md`.

**Frontend**
1. Plot Studio → Grouping: Intervention (outer) > `session` (inner) on x, then
   kind **Spaghetti**. You should get one line per subject, and each line
   should stay inside its Intervention bracket.
2. Colour by Intervention. Each line should take its group's colour.
3. Zoom in. Every line should pass exactly through its own markers, with no
   random jitter.
4. **Export code**. The saved figure should have the same tick labels and line
   positions as the preview.

---

## 7. Variable-column factors (group by one column of a wide table) — added 2026-09-15

**What changed:** you can group, filter or scope by one column of a wide
variable, for example `Demographics.InterventionGroup`. Subjects missing from
the sheet show as `(missing)`, sorted last, never dropped. Plan:
`.claude/plan-variable-column-factors.md`.

**Backend:** re-tick any old generated `plot_` endpoint that grouped by a
variable. The old bare-string spelling now raises a `ValueError`.

**Frontend**
1. Plot Studio → Grouping → add a grouping variable. A wide variable should
   offer its **categorical** columns only. Numeric columns should appear as
   refused, with a reason, and array columns (EMG muscles) should not be listed
   at all.
2. Pick `InterventionGroup`. The factor should be named `InterventionGroup` (not
   `Demographics.InterventionGroup`), and the bars should split by group.
3. Take a subject that is not in the sheet. It should appear as `(missing)`
   last, and `scidb.log` should warn about it.
4. **Export code**. The generated code should group by the same column and give
   an identical figure.
5. In `scidb.log`, `groupable_columns(RawEMG)` should now be fast. It used to
   take 4–5 s on every panel open.

---

## 8. Grouping picker (DAG popup), step two — added 2026-09-15

**What changed:** you can pin grouping variables to a variant (default: latest)
through a DAG picker. Step one was checked on 2026-09-15; it blanked the tab,
and that was fixed. **Step two has never been checked.**

**Frontend**
1. Plot Studio → Grouping → add a grouping variable through the DAG popup.
2. Step one: the canvas should draw, not blank.
3. Step two: choose a specific variant of that grouping variable. The groups
   should change to match that variant. Switch back to latest and check that
   they revert.
4. If the tab blanks, search `scidb.log` for `report_client_error`.

---

## 9. Variant provenance panel (🔍 Provenance) — added 2026-09-15

**What changed:** a toolbar panel shows which functions, versions and runs
produced a variable at a chosen variant. It gives the same answer as
`scidb trace --variant … --runs`. Doc:
`docs/claude/variant-provenance-introspection.md`.

**Backend:** for comparison, run in a terminal from the project folder:
```
scidb trace <Variable> --variant <label> --runs
```

**Frontend**
1. Click **🔍 Provenance** in the toolbar, then pick a variable and a variant.
2. You should see the chain of functions with their code versions and run
   records, and it should match the CLI output above.
3. Try a variable that was re-run with no data change. It should show a second
   run under the same invocation, not a separate producer.
4. Try a code-version pin (a function with two body versions). It should
   resolve, not come back empty.

---

## 10. Manual edges on already-run (history) nodes — added 2026-09-15

**What changed:** edges visible in the DAG are the ground truth for execution.
- A new edge drawn onto an already-run function input is used on the next run.
- A manual edge next to a still-visible history edge becomes `EachOf`: both
  run.
- A fresh node's config carries over into the history node when it graduates
  (fresh wins).

Doc: `docs/claude/manual-edges-on-history-nodes.md`.

**Backend:** have a function that has already run (for example `grSides`). Add
a new parameter (for example `side`) to its signature, then click
**🔄 Refresh Code**.

**Frontend**
1. Draw an edge from `Demographics` to the new `side` input. The panel should
   not show `n/a`, and the node's id should not change.
2. Run the function. `scidb.log` should show the edge being used, and the new
   records should reflect it.
3. Draw a second source onto an input that still shows its history edge. Both
   sources should run (EachOf), and the panel should list both.
4. Hide the history edge. From then on only the manual edge should run.
5. After the run, the manual edge and any column selections should move onto
   the new node.

---

## 11. Column selection in the GUI — added 2026-09-15

**What changed:** per-input column picking on a function node: one column,
several columns, or iterate over columns (`for_columns`). It is stored per node.
Doc: `docs/claude/column-selection.md` (§From the GUI).

**Frontend**
1. Select a function node whose input is a wide variable. In the Node tab's
   settings, pick one column for that input. The node should show the column
   next to the input.
2. Run it. `scidb.log` should contain
   `[execution] '<fn>': '<param>' restricted to ...`. If that line is missing,
   the whole table was loaded.
3. Pick several columns, then switch to iterate (`for_columns`) and run again.
   You should get one run per column.
4. Check that the node's id and combo hiding are unchanged after selecting
   columns.
5. For a MATLAB function, repeat step 1 and check the generated MATLAB command
   (it should quote column names correctly, including ones with `'`).

---

## 12. Schema location picker (🗂 View Schema Locations) — added 2026-09-13

**What changed:** a nested, status-coloured tree of every schema location for
one variable and one variant:
- **green**: current;
- **amber**: an input was re-saved since;
- **red**: missing;
- **grey**: excluded.

It opens from the canvas and replaces Plot Studio's old per-key level pickers.
The canvas badge also turns red when PathInput discovery finds files that
haven't been processed. Doc: `docs/claude/schema-location-status.md`.
**Nothing here has been checked by eye.**

**Backend:** compare against the CLI:
```
scidb locations <Variable> --problems
```

**Frontend**
1. Right-click a Variable node and choose **🗂 View Schema Locations**. The tree
   should draw with counts, and the colours should match the CLI.
2. Click a row. The Plot panel should open already showing that location.
3. In Plot Studio, the **Schema keys** section should be one button. Use the
   per-row checkboxes (**Include this location in the figure**,
   **Include every level of this key**) to select a ragged set. The figure
   should show exactly those locations, and **Export code** should mask the
   same set.
4. Re-save an upstream input for one subject. Only that subject's rows should
   turn amber.
5. Add a raw file that PathInput would discover but that hasn't been processed.
   Within about 5 s the function's canvas badge should turn red.

---

## 13. Hypothesis tabs: duplicate an already-run hypothesis — added 2026-08-08

**What changed:** placement-qualified node ids let a pipeline that has already
been run be duplicated into a new hypothesis tab. Each copy can be edited on
its own, and skip-computed still checks globally. Plan:
`.claude/plan-placement-qualified-node-ids.md`.

**Frontend**
1. On a hypothesis tab whose pipeline has already run, click **Duplicate** (the
   hypothesis tab's duplicate action).
2. The new tab should contain the full graph. The original tab should keep all
   its nodes where they were (nothing moved or stolen).
3. Run the duplicate. Unchanged steps should be skipped as already computed,
   and both tabs should show independent green states.
4. Edit a node's config in the duplicate. The original should be unaffected.
5. Select a few nodes, then **Extract** them to a submodule. The boundary edges
   should still render and the submodule's ports should be correct.

---

## Done

(Move items here once checked, with the date and anything noticed.)
