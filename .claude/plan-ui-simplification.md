# Plan: UI simplification after GitHub #28 (2026-11-21)

Six items from the UI review. Words stay in scidb (one owner); the GUI only
lays them out.

1. **Node: the two signals.** `StepVerdict.new_chip()` says what to do:
   `＋4 new · run to include` (scidb.provenance.status).
2. **Legend.** scidb owns a one-line meaning per state (`status.MEANINGS`);
   `data_view` sends `states` (order) and `meanings`. The Data view's filter
   row becomes the legend: every state incl. ✓ present, tooltip = meaning,
   built from the reply (the hard-coded `FILTERS` labels were a second owner).
   *Changed from the review:* ＋ new is NOT merged into ○ not run. The node's
   `＋N new` chip and its click-through filter use `new`; merging the cell
   state only in the Data view would give one location two names.
3. **One word: exclude / include.** The cell pane's 5 actions become
   Exclude here… / Exclude <V> only… (+ Include again… / Include <V> again…
   on an excluded cell). The "Mark missing / present as expected" buttons
   duplicated the scoped exclude/include, and their reason strings were a
   frontend copy of `MARKED_*_REASON`. The Python/MATLAB preset APIs stay.
4. **One data preview.** Variable menu loses "Preview data…" (the Data view
   is where you look at a variable's data). The cell pane's raw JSON snippet
   becomes a "Preview…" button opening `DataPreviewPopup` at that location
   (new optional `DataPreviewTarget.location`), mounted inside the Data view
   so it stacks on top. The port preview and run report entry stay.
5. **Header.** `🔄 Reload code` stays; Restart Python, Paths, Clear data,
   Bug report move to a `⋯` menu. The dialogs are lifted so closing the menu
   does not close them (ClearDataButton/BugReportButton held their own open
   state).
6. **Run vs Try.** Function menu: `▶ Run until here` then `🧪 Try on… (saves
   nothing)` together, a divider, then the read-only entries.

Tests: scidb (new_chip, meanings in the reply), frontend unit test for the
legend/filter builder, e2e selectors unchanged (`menu-data-view`,
`data-view`). Rebuild and commit `scistack_gui/static/` (release step 1).
