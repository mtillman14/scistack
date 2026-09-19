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

## 1. Declared schema level order (`[schema_keys]`) — added 2026-09-19

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

## 2. "Show sample" overlay — added 2026-09-19

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

## 3. Grouping + Collapse roles (new plot role model) — added 2026-09-19

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

## 4. Spaghetti plot kind — added 2026-09-16

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

## 5. Variable-column factors (group by one column of a wide table) — added 2026-09-15

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

## 6. Grouping picker (DAG popup), step two — added 2026-09-15

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

## 7. Variant provenance panel (🔍 Provenance) — added 2026-09-15

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

## 8. Manual edges on already-run (history) nodes — added 2026-09-15

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

## 9. Column selection in the GUI — added 2026-09-15

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

## 10. Schema location picker (🗂 View Schema Locations) — added 2026-09-13

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

## 11. Hypothesis tabs: duplicate an already-run hypothesis — added 2026-08-08

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
