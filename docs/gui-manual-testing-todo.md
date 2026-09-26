# GUI manual testing — running to-do list

Features that are built and pass their automated tests but have **not yet been
checked by eye in the running GUI**. Claude keeps this list updated. When you
have checked an item, tell Claude (or move it to **Done** yourself) and note
anything that looked wrong.

Newest first. Each item says what changed, the backend steps (anything outside
the webview: terminal, config files, restarts, `scidb.log`), the frontend
steps (clicks in the GUI), and what you should see.

---

## 0zx. Mark colours and fill opacity match the exported figure — added 2026-09-26

**What changed:** one owner for the marks' palette
(`render.base.mark_palette`) and fill opacity (`render.base.fill_alpha`).
The preview now honours a spec's named `palette` (it always drew the
default before). Plotly bars are now drawn at the style's alpha (0.85)
instead of opaque, and boxes/violins at 0.6/0.55 instead of plotly's own
half-transparent fill. The exported code now uses the preview's palette,
does not fade fills (`saturation=1`), and states the same opacity.

**Backend steps:** restart the GUI (Python changed; no frontend change).
Commit and push, then pull in the GUI runtime clone. With a bad palette
name in a spec, `scidb.log` shows `palette '<name>' is not a
seaborn/matplotlib palette … drawing the default palette`.

**Frontend steps:**
1. Plot Studio, a bar plot coloured by a grouping layer; then a box plot
   and a violin plot of the same variable.
2. Save / export each and open the saved figure next to the preview.

**What you should see:** bars slightly see-through (the gridless paper
background shows through a little); the exported figure's bars, boxes
and violins are the same colours and the same opacity as the preview —
not paler, not seaborn's blue/orange.

---

## 0zw. Show sample lines when the points take the marks' colour — added 2026-09-26

**What changed:** with "Colour points by" = *Mark's colour* and Join points
= *Auto (lines)* or *Lines*, the lines were missing whenever the coloured
grouping layer was the innermost tick (e.g. ticks `[session, timepoint]`,
colour = `timepoint`): the overlay was split per mark colour, so every run
was one point. Now each subject's line joins its points across the
coloured ticks; the line is neutral grey (`#808080`) and each point keeps
its own mark's colour. When the coloured layer is a bracket instead (the
line stays inside one colour), line and points are in that colour as before.
The exported figure draws the same.

**Backend steps:** restart the GUI (Python changed; no frontend change).
Commit and push, then pull in the GUI runtime clone. In `scidb.log`, look
for `sample overlay in the marks' colour ('timepoint'): each point its mark's
colour; a line crossing 'timepoint' levels is drawn in #808080` (INFO), and
at DEBUG `sample overlay: N of M run(s) cross the marks' colour`.

**Frontend steps:**
1. Plot Studio, bar plot grouped by `session` then `timepoint`, colour =
   `timepoint`, Show sample = `subject`.
2. Colour points by = *Mark's colour*, Join points = *Auto (lines)*; then
   *Lines*.
3. Switch Colour points by to `subject` and back.
4. Export / save the figure and open the generated code's output.

**What you should see:** in 2, one grey line per subject per session, from
its first timepoint to the last, each point in its bar's colour. In 3, the
lines take the subject colours; back on *Mark's colour* they are grey
again. In 4, the export matches the preview.

---

## 0zv. Group a figure by a column of the variable being plotted — added 2026-09-26

**What changed:** the Grouping picker's DAG canvas now lets you click the
PLOTTED variable itself, when it has more than one data column, and tick one
of its own non-schema columns (e.g. `Side` on a wide gait record). The label
comes from each row, so it is not joined on schema keys and there is no
variant step: the picker skips the variant graph for it. The exported
`plot_` endpoint takes no extra input for it, because the column is already
in `df`.

**Backend steps:** restart the GUI (Python changed). The frontend was rebuilt
(both bundles). Commit and push, then pull in the GUI runtime clone. In
`scidb.log`, look for `grouping <Var> by its own column(s) [...] — carried on
each row, not joined` and `carried 'Var.Side' as factor 'Side' on the
measure's own rows (N level(s))`.

**Frontend steps:**
1. Open Plot Studio on a wide variable that has a text column (e.g. `Side`).
2. Grouping → Group by… The plotted variable's node should say "N column(s)
   of the plotted data — click to choose". Click it.
3. The sidebar should list `Side` with its level count. The numeric fields
   should be listed as refused with the reason "a measured field of the
   plotted variable, not a label". The subtitle should say there is no version
   to choose.
4. Tick `Side` and Apply.

**What you should see:** `Side` appears as a grouping with its levels (L/R),
and the bars/boxes split by side within each field panel. Export the plot and
check that the exported figure matches the preview.

---

## 0zu. One function reading several files keeps its wiring; wide variables open on one field — added 2026-09-25

**What changed:** a PathInput is identified by its NAME, which is now a
required `name=` argument (Python `scidb.PathInput(tmpl, name="X")`, MATLAB
`scidb.PathInput(tmpl, 'name', 'X')`). The name is part of the invocation id,
so `pandas.read_csv` run on two differently named PathInputs makes two
invocations and two canvas nodes. Before, the two runs merged and the edges
were rewritten after the second run. The template and root folder are NOT
identity: moving the data or opening the project on another machine re-runs
nothing. A second node for a PathInput already on the canvas is refused.
Separately, Plot Studio opens a variable with more than 24 fields on its first
field only, and y limits are computed with one groupby.

**Backend steps:** restart the GUI (Python changed). Delete and recreate the
database: ids written under the old recipe will not match. Any `.py`/`.m`
file that declares a PathInput needs `name=` equal to its binding
(`GaitSpeed = scidb.PathInput(..., name="GaitSpeed")`); TOML entries need
nothing (the key is the name). The frontend was rebuilt (both bundles).
Commit and push, then pull in the GUI runtime clone.

**Frontend steps:**
1. Add PathInputs Symmetry, Unmatched and GaitSpeed. Add three
   `pandas.read_csv` nodes, one per PathInput, each wired to its own output
   variable.
2. Run them one at a time, in order.
3. Drag GaitSpeed from the sidebar onto the canvas a second time.
4. Move the Aim1 CSV folder, point the three PathInputs at the new folder in
   the sidebar, and run the three nodes again.
5. Open SymmetryTable in Plot Studio.

**Expect:**
- Step 3: an alert "PathInput 'GaitSpeed' is already on this canvas" and no
  second node; scidb.log has `Refusing to place PathInput 'GaitSpeed'`.
- Step 4: every node stays green and nothing new is saved (`0 new rows`);
  scidb.log has `PathInput 'GaitSpeed': location changed … nothing re-runs
  because of it`.
- After each run, every node keeps exactly its own PathInput edge and its own
  output edge. No node feeds two tables, and there is no "graduation
  collision" WARN in scidb.log.
- scidb.log shows `pipeline_variants: 3 invocation(s) -> 3 variant(s)` after
  the third run.
- The plot opens within a few seconds, showing ONE field (the first). The
  field filter shows only that field ticked. The log has `default_spec(...):
  80 field(s) in 'ColName' — opening on the first`.
- Ticking every field still works, and `build_plan ... y_limits=` is well
  under the old 18.7 s.

---

## 0zt. No canvas rebuild mid-save during a Python run; edge/state logging — added 2026-09-25

**What changed:** the extension's `.duckdb` file watcher no longer refreshes the
canvas while this session's own Python run is writing (it still waits for
MATLAB as before). The run's own `dag_updated` after `run_done` does the one
refresh. `scidb.log` now names DB-derived / hidden / superseded edges, call
sites that record several output types, and why each manual function node is
red or green. A first run of a new wiring logs INFO, not a "cannot run" WARN.

**Backend steps:** reload the VS Code window (the extension bundle
`dist/extension.js` was rebuilt). Use a large run, e.g. the
`pandas.read_csv` → UnmatchedTable node (15k records).

**Frontend steps:** run the node; watch the canvas during the save.

**You should see:**
- During the save: the Output channel says `DuckDB file changed during this
  session's own Python run — skipping the watcher refresh`, and `scidb.log`
  has **no** `Starting graph build orchestration` between `run_start` and
  `run_done`.
- After `run_done`: exactly one graph build, and the UnmatchedTable node shows
  its records.
- In `scidb.log`: a `build_edges detail:` line listing edges as
  `source->target`; a `call site pandas.read_csv/... records 2 output types`
  line; a `manual fn node ... state=` line per manual function node.

## 0zs. Rename a PathInput from its panel; the hint names the entities file — added 2026-09-25

**What changed:** the PathInput panel's name at the top is now an editable
field. A rename rewrites the key in the entities file and moves every canvas
placement, edge, hidden edge and note to the new name. Runs recorded under the
old name stay attached to the renamed node. The hint under Path Template now
names the file it writes (e.g. `scistack_entities.toml (not scistack.toml)`)
where it used to say `scidb.PathInput(...)`. Plan:
`.claude/plan-pathinput-rename.md`.

**Backend**
1. Reload the GUI (both bundles were rebuilt).
2. After the rename, `src/scistack_entities.toml` should show the new key with
   the same value and comment. `scidb.log` should have lines for
   `rename_declaration`, `recorded PathInput rename`, `rebase_node ... {counts}`
   and `rename_path_input ... done`.

**Frontend**
1. Select a PathInput that has already been run and is wired into a function.
   The hint should name `scistack_entities.toml`.
2. Click the name, type a new one, and press Enter. The panel should close, and
   the node should reappear in the same position under the new name, with its
   edges still attached.
3. Check that no second node with the old name appears. Open the downstream
   function. Its PathInput input should show the new name, and it should
   still be green.
4. If the PathInput was placed in two hypothesis tabs, check both.
5. Try renaming to an existing Parameter's name. You should get a red "already
   declared" error, and nothing should change.
6. Try a PathInput declared in a `.py`/`.m` file. The name field should be
   greyed out.
7. Type a new name and press Escape. The old name should come back and nothing
   should be written.

---

## 0zr. Startup names each file it imports; analysis scripts are refused — added 2026-09-25

**What changed:** during startup discovery, the server sends a progress line
for every file ("Importing 16/25: csv-stats-change-score.py"), so the 60 s
startup timeout applies to each file instead of all of them. Scripts that do
their work inside a top-level `for`/`with`/`if` block are now refused instead
of run. A new database's folder is only added to `scistack.toml` if it has
`.py`/`.m` files in it. Plan: `.claude/plan-discovery-import-timeout.md`.

**Backend**
1. Open `Stroke-R01-Aim1.duckdb` again (with the stats scripts unchanged).
2. The SciStack output channel should show `Importing i/25: ...` lines and the
   server should reach ready, not time out.
3. `src/stats/create_change_score_df.py` and `create_cohensd_df.py` should
   show `Refusing to import ... inside a top-level for loop at line N` (or
   `with`/`if` block), and no new "Saved ANOVA df ..." CSVs should appear.
4. In `scidb.log`, each "Loaded module file" line ends with `import X.XXs`,
   and there is one "Imported 25 module files in ..." line. Any file taking
   over 5 s logs a `Slow import` WARN naming it.
5. With a brand-new database in a folder with no code, the new
   `scistack.toml`'s `modules` and `[matlab] sources` should list only the
   project root, and there should be no "directory contains no .py/.m files"
   WARNs on the next start. (For the existing Stroke project, you can delete
   the `/Users/.../Documents/Aim1` entries from `scistack.toml` by hand.)

**Frontend**
1. Paths → Discovered Code lists the refused stats scripts with the new
   "inside a top-level ... at line N" reason.

---

## 0zq. Plot Studio light mode = the saved figure, exactly — added 2026-09-24

**What changed:** there is now a ☀ / ☾ button at the top right of the Plot
Studio's controls header. It switches the studio (controls, popups, saved-plots
rail) between dark and light. The choice is remembered and shared by every plot
tab. Studio colours come from a single palette,
`frontend/src/components/PlotStudio/plotTheme.ts`.

The figure itself now draws the export's "paper" (`scistackplot/paper.py`):
a white background, black text, a black frame round every panel, outward
ticks, no grid, no zero line, and black error bars with caps. **Light mode
shows the figure untouched, so it should be the saved PNG exactly.** Dark
mode recolours the text, frame, ticks and error bars to light grey on a
transparent background, for the screen only. Doc:
`docs/claude/preview-paper-parity.md`.

**Extension:** the "SciStack" output channel logs `plot theme: light (stored;
N other webview(s) told)` on each toggle.

**Backend:** `scidb.log`'s `figure size … in (…)` line (written on save) now
ends with `paper #ffffff bg, #000000 frame 0.8pt, ticks out 3.5pt, grid off`.

**Frontend:**
1. Open a plot tab (dark). The figure should now have a light-grey frame
   and ticks, and no grid lines.
2. Click **☀**. The whole studio turns light: the rail, inputs, dropdowns,
   checkboxes, scrollbars and the Saved plots rail. Look for any dark patch
   or any text you can't read (for example the amber notes, the yellow "no
   records" warning, or the cyan/violet tags).
2a. **The parity check.** In light mode, with the preview at the export size
   (not "Fit pane"), open a bar plot with error bars, faceted by one
   factor. Save it as PNG and open the file beside the tab. Background,
   frame, ticks, the absence of a grid, text colour and error-bar caps
   should match. Note any difference you see (legend, bracket lines,
   spacing) rather than fixing it here.
2b. Repeat 2a with a box plot, a line/band plot and a log-y axis. On the
   log axis, matplotlib's extra minor ticks are a known difference.
3. Open the variant, grouping and location popups. Their frames and
   sidebars should be light too. (The pipeline nodes drawn inside the
   variant/grouping canvas keep the canvas's own dark style, as expected.)
4. With a second plot tab already open, toggle in the first. The second
   should switch as well.
5. Close every plot tab and open a new one. It should open in light mode.
   Reload the window and check it again.
6. On the pipeline canvas, the 🗂 location picker and the Provenance panel's
   variant popup should still look as they did before (dark).
7. Browser build (`scistack-gui` CLI): the toggle works and survives a page
   reload.

---

## 0zp. No bracket lines when the bracket labels are hidden — added 2026-09-24

**What changed:** when **Hide labels the legend repeats** blanks a bracket
row's labels (that layer is also the colour), the horizontal lines under the
tick labels are no longer drawn either. Brackets that still show their labels
keep their lines. Applies to the preview and the saved figure.

**Backend:** `scidb.log` has `x bracket row 'InterventionGroup': no label
shown, rules omitted` for the export, and `preview: N bracket rule(s) dropped
with their blank labels` for the preview.

**Frontend:**
1. Open a bar plot with x grouped by `session` then `InterventionGroup`, and
   colour set to `InterventionGroup`. Bracket labels and lines appear under
   the session ticks.
2. Tick **Hide labels the legend repeats**. The group labels and the lines
   under them should both disappear, in the preview and in a saved figure.
3. Set the colour to `session` instead. The ticks are hidden, but the
   `InterventionGroup` brackets keep their labels **and** their lines.

## 0zo. Exported plot code has the preview's y range — added 2026-09-24

**What changed:** the exported `plot_` code used to work out its fixed y range
from the data *before* filters, so a filtered-out subject could move the
bottom of the axis. It now uses the same numbers the preview uses. An exported
bar chart that autoscales now also leaves the same small gap below zero as the
preview, instead of starting at exactly 0.

**Backend:** after exporting, `scidb.log` has one `export y limits: …` line
saying whether the range was baked in (and its numbers) or autoscaled.

**Frontend:**
1. Open a bar plot of GAITRiteSymmetry with `speed` iterating and a location
   filter that removes some subjects. Note the y-axis min and max in the
   preview.
2. Export the plot code and run it for the same figure. The saved figure's
   y-axis min and max should match the preview's.
3. Tick every iterate factor in the y-limit scope so that each figure scales
   to its own data, then export again. The exported bar axis should start a
   little below 0, as the preview does, not at exactly 0.
4. Now tick only `ColName` (the facet) in the y-limit scope, with `speed`
   still iterating. In the preview each GAITRite field has its own range,
   and that range is the same across speeds. Export and run it for both
   speeds: each field's panel should have the same y min/max as the preview.
   Small fields should no longer be flattened onto one axis shared with the
   large ones. The log line should say `export y limits: per panel over
   ['ColName']`.

## 0zn. Weight of the "Show sample" points and of spaghetti lines — added 2026-09-24

**What changed:** two new boxes scale point size and line thickness
together (a multiplier, empty = 1x = the old look). **Weight** in the Show
sample section sizes the overlay's points and joining lines; **Line weight**
under Plot type (only on a spaghetti) sizes the spaghetti's own points and
lines. The exported code also now draws the spaghetti at the preview's line
width, marker size and opacity (it used seaborn's defaults before).

**Backend**
1. Pull, then **Restart** the GUI (both bundles were rebuilt).

**Frontend**
1. Open a bar plot with a collapsed `subject`; in **Show sample** tick
   `subject`. Type `2` in **Weight**, then `0.5`.
2. Switch the kind to **Spaghetti**. A **Line weight** box appears under the
   kinds; type `2`. With Show sample still ticked, set Weight to `0.5`.
3. Clear both boxes. Save the plot, reopen it.
4. Export the plot code and run it (or save the figure).

**What you should see**
- Step 1: the overlay points and lines get visibly bigger/thicker, then
  thinner; the bars do not change. Hovering the box shows the resolved
  sizes in pt.
- Step 2: the spaghetti lines and points thicken; the overlay stays thin —
  the two are independent. Line weight is absent on any non-spaghetti kind.
- Step 3: cleared boxes show the `1×` placeholder; a reopened saved plot is
  not "● modified".
- Step 4: the saved/exported figure has the same weights as the preview.
  `scidb.log` INFO `figure size … marks lines 2x (…); sample 0.5x (…)`.

---

## 0zm. A Parameter named differently from its argument: staged and unchecked values work — added 2026-09-24

**What changed:** staged (pending) and unchecked values on a Parameter now
reach the function argument it feeds even when the names differ — e.g. the
`gaitrite_config` Parameter wired into `loadGaitRiteOneFile.gaitRiteConfig`
(cleanup-audit F20). Before, a staged value never ran and an unchecked value
ran anyway, for such a Parameter only. Backend only.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. On `gaitrite_config`, stage a new value and click **Run** on
   `loadGaitRiteOneFile`.
2. Untick one of `gaitrite_config`'s values and run again.

**What you should see**
- Step 1 runs the staged value: `scidb.log` shows `[execution] pending
  override on … target: {'gaitRiteConfig': …}`.
- Step 2 runs without the unticked value's combination.

---

## 0zl. PathInput nodes follow the recorded name — added 2026-09-24

**What changed:** a run now records WHICH declared PathInput fed it, and the
canvas uses that name before matching templates (cleanup-audit F38). Normally
nothing looks different. It matters when two PathInputs share a template, or
after a template edit. Backend only.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Run a PathInput-fed function from the canvas (Python or MATLAB).
2. Edit that PathInput's template in the sidebar and refresh.

**What you should see**
- The function node and its PathInput edge stay where they were; no second
  function node appears.
- `scidb.log`: `[provenance] fn=…: … PathInput edge(s) named by declared PathInput`.
- `scidb graph` in a terminal shows the same number of steps as the canvas.

---

## 0zk. Labels: titles and display aliases, for one plot or the whole project — added 2026-09-24

**What changed:** Plot Studio has a **Labels** section below Figure size:
- **Title, X label and Y label** boxes. Empty means automatic.
- One block per thing the figure draws as text: the measure, then each
  factor on the x axis, the colour, the panels, the figures and the dashes.
  Each block has a name box and a "▸ N levels" list with one box per level.
- **Two layers.** A box holds THIS plot's alias; it is saved with the plot
  and overrides the project. An empty box shows, greyed, what the figure
  draws without it: the project's alias (marked "(project)") or the raw
  text.
- **"↑ project"** writes a plot alias into `scistack.toml`'s `[aliases]` so
  that every plot reads it, and clears the plot's own copy.
- **"✕ project"** removes a project alias.
- A packaged (pyproject.toml) project refuses the project buttons with a
  message, because the GUI does not edit pyproject.toml. A CSV plot has no
  project buttons.

Aliases change TEXT only: filters, the location picker, facet layout rules
and the plot-data CSV still use raw levels. Two levels of one factor that
would read the same are refused with a message naming where each alias came
from. Exported code carries the aliases as a fixed copy, so re-export a step
after you change a project alias. Plan:
`.claude/plan-plot-text-sizes-and-aliases.md` (stages 3-6).

**Backend**
1. Pull, **reload the VS Code window** (webview bundle changed), then
   **Restart** the GUI.

**Frontend**
1. Plot a variable grouped by a schema key such as `session`, and coloured
   by it. The Labels section lists the measure, then `session` as "x axis"
   (the colour is the same factor, so it appears once).
2. Type `Visit` in session's name box. The x title and the legend title read
   "Visit" after the redraw.
3. Open "▸ N levels" and type `Baseline` for `BL`. The tick label and the
   legend entry both change. The block header says "· 1 aliased".
4. Click **↑ project** on that row. The box empties and now shows
   "Baseline (project)" greyed, and the figure still reads Baseline. Open
   `scistack.toml`: it has an `[aliases.session.levels]` table with
   `"BL" = "Baseline"`, and everything else in the file is unchanged.
5. Open ANOTHER variable's plot that also shows `session`. It reads
   Baseline with no edit.
6. Back in the first plot, type `Pre` in the BL box. This plot reads Pre and
   the other still reads Baseline. Clear the box, and it goes back to
   Baseline.
7. Click **✕ project** on the BL row. The figure reads `BL` again, and the
   entry is gone from `scistack.toml`.
8. Give two levels the same alias. The panel shows an error naming both
   levels and where each alias came from, and the figure is not drawn.
   Clear one of them and the figure returns.
9. With a faceted factor, alias a level. The panel's y title changes. A
   facet layout rule you wrote against the raw name still places the panel.
10. Save the plot in the Saved plots rail, reopen it, and check the aliases
    and titles come back and it is not "● modified".
11. Click **Export code**. The code has an `_aliases = {…}` literal, and the
    exported figure reads like the preview.
12. `scidb.log` has an `aliases: levels session 1/N (project 1, plot 0)…`
    line on each resolve, and `[config] set_project_alias: wrote …` for each
    project edit.

---

## 0zj. Set each text size separately — added 2026-09-24

**What changed:** The Figure size section has a **Text sizes (pt)** block,
with one box each for Title, X label, Y label, X ticks, Y ticks, Groups (the
bracket rows under a nested x axis), Legend and Legend title. An empty box
follows **Font (pt)**, and its grey placeholder shows the size it will be
drawn at (`auto · 14`). A typed size is fixed: automatic fitting may still
rotate, wrap or move that text, but never shrinks it. **Reset** clears them
all. The old "Tick font (pt)" box is now "X ticks". The preview's bracket
labels are now the same size as in the saved figure (previously slightly
smaller). A plot saved before today opens with default fonts and a yellow
note naming `font_size` / `tick_font_size`. Plan:
`.claude/plan-plot-text-sizes-and-aliases.md` (stages 1-2).

**Backend**
1. Pull, **reload the VS Code window** (webview bundle changed), then
   **Restart** the GUI.

**Frontend**
1. Plot a variable with a nested x axis (two grouping layers) and a colour
   legend. The eight boxes are empty, with placeholders like `auto · 14`,
   `auto · 16.8` (Title) and `auto · 11.7` (Groups). Legend title says
   `= legend`.
2. Set **Font (pt)** to 20. Every placeholder updates after the redraw
   (Title 24, Groups 16.7), and all preview text grows.
3. Type 30 in **Title**, then 9 in **Y ticks**. Only those change. A
   **Reset** button appears beside the heading.
4. Type 9 in **Groups** with X ticks empty. The bracket rows shrink and the
   tick labels do not. Clear Groups, then type 10 in **X ticks**. The
   bracket placeholder follows (`auto · 8.3`).
5. Narrow the figure (Width 4). Type 18 in **Legend**. The legend moves below
   the plot but stays at 18. If the x labels overlap, the yellow notice ends
   with "The x tick font is fixed at 10 pt."
6. Save the plot (Saved plots rail) and reopen it. Every size comes back, and
   it is not marked "● modified". Clear a box and set it again to the same
   value; the ● goes away when the value matches the saved one.
7. Save the figure. In `scidb.log` the `figure size … text …` line lists every
   size, and the fixed ones carry `*`.
8. Click **Reset**. Every box empties, and Font stays at 20.
9. Open a saved plot from before today. The yellow note names
   `style.font_size` (and `style.tick_font_size` if it was set), and the
   fonts are 14.

---

## 0zi. Saved plots: name a plot, reopen it exactly as it was — added 2026-09-24

**What changed:** Plot Studio has a **Saved plots** rail on the right. Save
the current plot under a name, and reopen it later from the list with every
setting restored: spec, preview mode, figure-size dropdown, and which figure
of a Separate-figures set was showing. Saving an existing name adds a new
version and keeps the old ones. Remove hides a plot and never deletes it.
A plot saved by an older build still opens; settings that no longer exist
are listed in a yellow note instead of failing. Not available when plotting
a CSV. Plan: `.claude/plan-saved-plots.md`.

**Backend**
1. Pull, **reload the VS Code window** (webview bundle changed), then
   **Restart** the GUI.

**Frontend**
1. Right-click a variable → Plot. The rail on the right says "Unsaved plot"
   and "No saved plots for this variable yet".
2. Change a few settings (kind, a role, the figure-size dropdown, Preview
   mode "Fit pane"). Click **Save plot…**, type `Fig A`, press Enter.
3. Change something. The name gets a yellow ● and "· modified".
4. Click **Save…**. The box is prefilled with `Fig A`; press Enter. The
   ● goes away and the row reads `v2`.
5. Save another plot as `Fig B`. Then click **Save…** while `Fig B` is open,
   type `Fig A`, press Enter. It asks: *"Fig A" is another saved plot. Save
   this as its next version?* Try Cancel (back to the name box), then accept.
6. Close the tab. Right-click the same variable → Plot again. Click `Fig A`
   in the list.
7. With unsaved changes, click another plot. It asks before discarding them.
8. ✎ renames a plot. Renaming onto another plot's name shows the reason in
   red. ✕ asks, then removes it from the list.
9. Collapse the rail with ❯. It shrinks to a vertical "Saved plots (N)" strip,
   still showing ● when there are unsaved changes.
10. A Separate-figures plot: step to figure 3, save, change figures, reopen.
    It returns on figure 3.

**What you should see**
- Reopening gives the same figure you saved: kind, roles, colours, size,
  preview mode, figure number.
- **Please check:** opening a fresh plot or a saved plot and touching
  nothing must NOT show ● modified. If it does, some setting is being
  adjusted automatically after opening. Send the `scidb.log` lines below and
  describe what you changed.
- `scidb.log`: `[saved_plot] saved <Var> / 'Fig A' as version N (… bytes,
  envelope format 1)`, `[saved_plot] opened <Var> / 'Fig A' version N …: 0
  note(s)`, `[restore] <Var>: K stored setting(s), 0 note(s)`.
- A CSV plot has no Saved plots rail.

---

## 0zh. Canvas and Plot Studio pickers load faster — added 2026-09-24

**What changed:** the graph build reads every function call's inputs and
outputs in two batched queries, not four queries per call (about 5 s of an
11 s build on your AIM 2 database). The Variant and Grouping pickers now load
their canvas WITHOUT run states, skipping the ~6 s state check. Their
variable nodes therefore have no green/red border any more. That is
intended: the picker is about selection, not freshness.

**Backend**
1. Pull, **reload the VS Code window** (webview bundle changed), then
   **Restart** the GUI.

**Frontend**
1. Open the main canvas. Note how long it takes to appear.
2. In Plot Studio, open the Variant selection popup, then the Grouping popup.

**What you should see**
- The canvas colours are unchanged, and it appears noticeably faster.
- Both popups open in about 1-2 s, and their nodes have no green/red border.
- In `scidb.log`: `[timing] pipeline_variants: … (invocation_edges=…, outputs=…, group=…, annotate=…)`,
  and `[timing] check_multiple_nodes_state` with `expected.variant_configs`,
  `expected.realized_inputless` and `expected.predict` — please send those lines.
- For each popup: `[pipeline] run states not requested — skipping the state check`.

---

## 0zg. Fix the tick settings yourself; exported code matches — added 2026-09-23

**What changed:** the Figure size section has new controls:
- **Tick rotation** (Auto / 0° / 45° / 90°)
- **Show tick labels** (Auto / every label / every 2nd…10th)
- **Tick font (pt)** (blank = auto)
- **Hide labels the legend repeats**

A fixed value is kept even where it overlaps; then a note says "The x labels
still overlap at this size (…)". **Export code** now replays the fitted labels
(prefix, wrapping, every k-th, rotation, font), and the exported script saves
at exactly the figure size. Both bundles rebuilt.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. On graph1, set Tick rotation = 45°, then 0°.
2. Set Show tick labels = Every 2nd on a plot with named ticks.
3. Tick **Hide labels the legend repeats** on graph1 (session is the colour).
4. On the 40-subject plot, click **Export code** and run the script.

**What you should see**
- 1: the preview (and a saved PNG) use exactly that rotation. At 0° on
  graph1 the overlap note appears.
- 2: every other name is blank, the first and last are shown.
- 3: the session tick labels disappear, the brackets and legend stay.
- 4: the script's `figure.png` has `01, 02, …` labels (no `SS`) and is
  exactly the figure size; the code has a
  `# x tick labels as scistackplot fitted them at …` block.

---

## 0zf. The preview shows the export's labels and legend — added 2026-09-23

**What changed:** the Plot Studio preview now draws the SAME label and legend
decisions Save makes (stripped IDs, wrapping, smaller font, rotation, every
k-th label, legend wrapped or below). The Figure size section has **Preview
at: Export size | Fit pane**:
- **Export size** (default): the preview is drawn at the saved size
  (Width x Height at 72 px per inch), so it can be smaller than the pane, or
  scroll if larger.
- **Fit pane**: fills the pane, with the decisions made at the pane's size.
  Below the setting: "This view would save as W × H in. [Use this size]".
Resizing the pane in Fit pane mode re-renders after ~0.3 s without
re-resolving. Both bundles rebuilt.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Open graph1 and graph2. Compare each preview with the PNG from 0ze.
2. Switch to **Fit pane**, then drag the pane wider and narrower.
3. Click **Use this size**.

**What you should see**
- Step 1: the preview matches the PNG — same tick text/rotation, same
  legend place and title wrapping.
- Step 2: the labels re-fit within about a second after you stop dragging,
  without the long "resolving" wait; the readout's inches change with the
  pane.
- Step 3: Width/Height take the readout's numbers, the mode goes back to
  Export size, and the preview looks the same as it did in Fit pane.
- `scidb.log`: `[plot] preview (export|pane) decided at W x H in: ticks …;
  legend …` and, on a resize, `re-rendered from the last resolve`.

---

## 0ze. Saved files are exactly the size you set — added 2026-09-23

**What changed:** Save no longer trims or grows the file around its content
(`bbox_inches="tight"` is gone). The file is exactly Width x Height at the
save dpi. The readout under the Figure size inputs now says "exactly". Both
bundles rebuilt.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Set Figure size to 7.2 in wide at 16:9 and note the readout (e.g.
   `1440 × 810 px at 200 dpi … exactly`).
2. Save graph1 and graph2 as PNG.
3. Open each PNG's properties / Get Info.

**What you should see**
- Both PNGs are exactly the readout's pixel size, whatever their labels
  and legend.
- Nothing is cut off at the edges. If something is, `scidb.log` has a
  `figure content reaches …in past the … canvas` WARN; send it to Claude.
- `scidb.log`: `[plot] saved figure …: … 7.20 x 4.05 in = 1440 x 810 px`.

---

## 0zd. Saved figures fit their legend — added 2026-09-23

**What changed:** in the SAVED figure, a legend at the right may take at most
30% of the width. Past that its title wraps at " / " ("InterventionGroup /" on
one line, "subject" on the next), the line samples shorten, and the text
shrinks (never below the tick-label minimum). If it's still too wide, or if
beside it the x labels can't fit, the legend moves **below** the panels, in as
many columns as fit. The preview is not changed yet (stage 4).

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Save the graph1 plot at its usual size.
2. Save the graph2 plot at 8 in wide, with Show in legend ticked.
3. Save graph2 again with Show in legend unticked.

**What you should see**
- graph1: the legend stays at the right, title on two lines ("session /",
  "subject"), and the panel is wider than before.
- graph2 (ticked): the legend sits below the panels in several columns; the
  panels use the full width, and the session labels are readable (or
  `scidb.log` WARNs that they still overlap).
- graph2 (unticked): a short legend (Digitimer / Onward / Sham), and still
  no overlap with the panels.
- `scidb.log`: `legend at the right: …` or `legend moved below the panels:
  <reason> …` per saved figure.

---

## 0zc. "Show in legend" checkbox for Show sample — added 2026-09-23

**What changed:** the Show sample pane has a **Show in legend** checkbox
(`PlotSpec.sample_in_legend`, default on). Unticked, the legend reads as
though nothing in the Show sample pane were ticked: no subject entries and no
"/ subject" in the legend title. The points keep their colours. The checkbox
is greyed out while the points take their mark's colour, because then nothing
of theirs is listed anyway. Both bundles rebuilt.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Open the graph2 plot (bars coloured by InterventionGroup, Show sample =
   subject, Colour points by = subject).
2. Untick **Show in legend**.
3. Set Colour points by = Mark's colour, and hover the checkbox.
4. Save the figure with the box unticked.

**What you should see**
- After step 2 the preview legend lists only Digitimer / Onward / Sham, titled
  "InterventionGroup"; the subject points and lines are still drawn, still in
  their own colours. Ticking it again brings SS01… back.
- In step 3 the checkbox is greyed out, and its tooltip says the points take
  their mark's colour.
- The saved PNG has the same short legend, and the panels are visibly wider.
- Reopening the saved figure keeps the box unticked.

---

## 0zb. Saved figures fit their x labels; nested axes lose the "a / b" title — added 2026-09-23

**What changed:** the SAVED figure (matplotlib) now measures its x tick
labels and, if they collide, drops a shared ID prefix (`SS01` → `01`, numbered
labels only), wraps, shrinks the font (never below 8pt or 70% of the font
size), rotates 45°/90°, and as a last resort shows every k-th label (numbered
labels only). Bracket rows sit a fixed distance below the tick labels. A
nested axis no longer has an x title ("session / InterventionGroup"). With
several facet columns, a single-layer axis title is drawn once under the
figure. The preview is NOT changed yet (stage 4).

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Open the plot from spec/images/graph1.png (bars: session ticks inside
   InterventionGroup brackets, Show sample = subject) and **Save**.
2. Open the plot from spec/images/graph2.png (same, two speed facets) and **Save**.
3. Plot a scalar by `subject` alone (many subjects) at a narrow width and **Save**.

**What you should see**
- Graph 1 / 2 PNGs: session labels readable, not overlapping; "Digitimer /
  Onward / Sham" on their own row below them, not on the tick labels; no
  "session / InterventionGroup" text anywhere.
- Subject plot: labels `01, 02, …` (no `SS`), rotated and/or every k-th, first
  and last always shown.
- `scidb.log`: an INFO line `x tick labels: … -> font …` (and `x bracket
  labels: …`) per saved figure; a WARN only if labels still overlap.
- Not yet: the preview may still overlap, and the saved file size may still
  differ from the requested size (stage 3).

---

## 0za. Edges and pending values read/write the session's own database — added 2026-09-23

**What changed:** drawing/deleting a manual edge, adding/removing a pending
Parameter value, the canvas build and both MATLAB command generators now use
the database of the request's session, instead of a global lookup beside it
(cleanup-audit F10). Backend only; nothing should look different.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Open two databases in two tabs.
2. In tab A: draw an edge, add a pending value to a Parameter, delete the edge.
3. In tab B: refresh.

**What you should see**
- Tab A's edge and pending value appear (and the edge disappears) in tab A only.
- Tab B shows none of them.
- "Copy MATLAB command" in tab A includes tab A's manual wiring.

---

## 0z. Saving a figure never times out in the save dialog — added 2026-09-23

The webview used to fail any request not answered within 30 s. That included
the Save dialog itself, so taking longer than 30 s to choose where to save gave
"Could not save: Request pick_save_path timed out". The webview now has no
timer. The extension's `scistack.rpcTimeoutMs` (default 300 s) is the only one,
and it applies only to requests that go to Python.

**Backend**
1. Pull, then **reload the VS Code window**. This change is in the extension
   bundle (`extension/dist/extension.js`) as well as the webview bundle, and
   a GUI Restart does not reload the extension.
2. Open the extension's SciStack output channel.

**Frontend**
1. Open a plot tab, click **Save figure**, and leave the Save dialog open for
   over a minute. Then pick a file.
   - Expect: the save starts ("Saving this figure at full resolution…") and
     completes. No "timed out" message.
2. Do the same with **Save all** (folder picker) and **Save data** (CSV).
3. Optional: set `scistack.rpcTimeoutMs` to `5000`, reload, and trigger a slow
   plot resolve.
   - Expect: the panel shows "no response from the Python server for
     'plot_resolve' after 5s". When the server does answer, the output channel
     shows `RPC late response: plot_resolve (id=…) answered after …ms, …ms
     after the timeout gave up on it`. Reset the setting afterwards.

## Before any session: common setup

**Backend**
1. Make sure the committed bundles are current. Items 0q and 0p (2026-09-22)
   are the newest; both vite targets were rebuilt for them before committing.
   If you have pulled frontend changes since, rebuild both targets:
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

## 0w. Schema location picker: every indeterminate box responds — added 2026-09-23

**What changed:** a checkbox in either pane of the location picker ignored
clicks when its gap was caused by the OTHER pane (e.g. untick `trial=1` on the
left → every subject on the right is indeterminate and could not be re-ticked;
or a subject on the left could not be re-ticked). One tick rule
(`withNodes` in `locationSelection.ts`) now repairs both halves. Frontend only —
both bundles rebuilt.

**Frontend** (Plot Studio → location picker)
- [ ] Right: untick `subject=1 / trial=1`. Left: `subject=1` goes indeterminate. Click it → every `subject=1` location on the right is ticked again.
- [ ] Left: untick `trial=1`. Right: each subject goes indeterminate. Click `subject=1` on the right → its whole subtree ticks, including `trial=1`; other subjects' `trial=1` stays unticked, and left `trial=1` turns indeterminate.
- [ ] Left: untick `subject=1`. Right: tick `subject=1` → it comes back fully; the footer shows no "−1 subject".
- [ ] Right: untick every location one by one. The LAST untick leaves every box unticked (it used to re-tick them all); the footer reads "All locations (−N subject)". Then tick one trial → only that trial is ticked.

## 0y. Schema Selection honoured by MATLAB runs and pipeline runs — added 2026-09-23

**What changed:** a node's Schema Selection (the location picker) now applies
exactly on MATLAB runs (it used to be squashed into per-key lists and could run
more than selected) and on pipeline runs, Python and MATLAB (they ignored it).
Backend + MATLAB only.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. On a MATLAB node, pick a RAGGED selection (all of one subject, plus one
   session of another) and click **Run**.
2. Run the whole pipeline containing that node (Python or MATLAB).

**What you should see**
- The generated command has a `'locations', '{"exclude_levels": …, "include": …}'`
  line, and MATLAB's `… N iterations` line counts only the selected locations.
- The pipeline run iterates the same locations as the node's own Run.

---

## 0x. MATLAB addpath breakdown in scidb.log — added 2026-09-23

**What changed:** the generated MATLAB script now times each `addpath`
directory (addpath was ~4.4 s of every run's ~4.7 s preamble). Backend only.

**Backend**
1. Pull, then **Restart** the GUI.

**Frontend**
1. Run any MATLAB function node.

**What you should see**
- In `scidb.log`, right after `[timing] matlab_preamble …`: one line
  `[timing] matlab_addpath: TOTAL=…s (dirs=N, already_on_path=K,
  path_entries=A->B, slowest=…s <dir>)`. Send it to Claude: it decides whether
  the fix is fewer folders, skipping folders already on the path, or one slow
  network folder.
- Set the log level to DEBUG to see one `addpath … already_on_path=… <dir>`
  line per folder.
- Also after that run (F35): `[provenance] captured 1 source unit(s) for
  fn=<name> @ <hash>` and NO `source NOT captured … recipes have drifted`
  warning. MATLAB functions now keep their code per version.

---

## 0w. Schema Level: automatic by default, same on every route — added 2026-09-23

**What changed:** an unset Schema Level is now "automatic" everywhere. Every
route (Python Run, Python pipeline, MATLAB Run, MATLAB pipeline script) and the
settings panel ask one owner: stated on the node, else where THIS node last
ran under its current wiring, else its inputs' level, else every key. MATLAB
used to run every populated key (`loadDemographics` ran 714 times on one file).
Unticking every box (`[]`) now means one call on every route (a Python Run used
to iterate EVERY key). Frontend rebuilt (both targets).

**Backend**
1. Pull, then **Restart** the GUI.
2. Keep `scidb.log` open; every run logs one `[schema-level] … -> iterating …
   (<rule>) via <route>` line.

**Frontend**
1. Select `loadDemographics`. If you set its level by hand earlier, click
   **Use automatic**.
2. Select a node that has never run, wired to a trial-level variable and a
   subject-level one.
3. On any node, untick one box, then click **Use automatic**.
4. Run `loadDemographics` (MATLAB).

**What you should see**
- Step 1: the Schema Level boxes are faded, the hint reads "Automatic — the
  finest level its inputs carry · one call over the whole dataset", no box ticked.
- Step 2: boxes subject…trial ticked (faded), hint "Automatic — the finest level
  its inputs carry".
- Step 3: after the untick the hint reads "Set on this node" and the rest of the
  boxes stay ticked (they start from the automatic level, not from "all").
  **Use automatic** returns to the faded state, and it survives a canvas refresh.
- `as_table` no longer implies one call (F32, 2026-09-23): a never-run Python
  node with `as_table` on and no level set now runs at its automatic level,
  one table per location. To aggregate, untick the keys to pool over (untick
  all for one call). Nodes that already ran as one call keep doing so.
- Step 4: the MATLAB log shows `1 iteration: no metadata`, and `scidb.log`
  shows `[schema-level] loadDemographics … -> iterating nothing: one call over
  the whole dataset (the finest level its inputs carry) via matlab run`.

---

## 0v. One Parameter node after a run; no false "not reflected" chip — added 2026-09-23

**What changed:** (B1) a Parameter whose declared name differs from the
argument it feeds (`gaitrite_config` → `gaitRiteConfig`) got a second node
after the function's first run, with only one of the two wired. Runs now
record the declared Parameter name and the canvas builds one node.
(B2) a node with "every column, one call each" selected showed "saved column
selection not reflected by its last run" after running. Backend + MATLAB only —
no frontend rebuild.

**Backend**
1. Pull, then **Restart** the GUI (the database gains a `declared_name`
   column on first open; the log says `added declared_name column`).
2. Keep `scidb.log` open.

**Frontend**
1. Unhide the two extra Parameter nodes you hid earlier today, if you want to
   see them go: they are history from the runs before this fix and will stay
   until those functions are re-run.
2. Re-run `loadGaitRiteOneFile` (MATLAB) from its node's **Run** button.
3. Create a new Parameter whose name differs from the argument it will feed,
   wire it into a never-run function, and **Run** it (MATLAB and, if you have
   one, a Python function).
4. Look at `calculateSymmetryOneVector` (input `v` set to every column).

**What you should see**
- The generated MATLAB command ends its `scidb.for_each` call with
  `'parameter_names', struct('gaitRiteConfig', 'gaitrite_config')`.
- `scidb.log`: `[provenance] fn=…: N constant edge(s) named by declared
  Parameter; argument->Parameter {'gaitRiteConfig': 'gaitrite_config'}`, then
  on the rebuild `[graph_builder] 1 Parameter edge(s) feed an argument of
  another name …`.
- Steps 2–3: after the run there is ONE Parameter node, still wired to the
  function. No new `param__gaitRiteConfig` node. If a history-only node does
  appear, the log names it: `build_parameter_nodes: … come from run history
  alone, not a declaration: […]`.
- Step 4: no "saved column selection not reflected by its last run" chip.

---

## 0u. Run a never-run MATLAB node from its own Run button — added 2026-09-23

**What changed:** a MATLAB node that has never run but is wired on the canvas
(grSides: `GAITRiteLoaded` → `grTableIn`, `Demographics["PareticSide"]` →
`side`) failed on Run with `unhashable type: 'list'`. Never-run targets now
use the same input-type shape as history. Backend only — no frontend rebuild.

**Backend**
1. Pull, then **Restart** the GUI.
2. Keep `scidb.log` open.

**Frontend**
1. On a function node that has never run (grSides, or a fresh node wired to
   one input and one output), click **Run**.
2. Optional: wire TWO variable nodes into the SAME input of a never-run MATLAB
   node, click **Run**.

**What you should see**
- Step 1: the MATLAB command is generated and runs; no `unhashable type`
  error. The log shows `scoped to node fn__grSides__… — 1 target(s)
  (inferred from edges, never run), 0 name-scoped history row(s)` (the old
  misleading `1 of 0 variant row(s)` line is gone).
- Step 2: the run is refused with `… have more than one candidate producer
  type … Wire exactly one variable type into each input.` — it must NOT run
  some other node's history.

## 0t. Source-declared entities are read-only in the sidebar — added 2026-09-23

Backend (`target_file_service.entity_editability`, RPC `get_entity_editability`) + frontend (`PathInputSettingsPanel`, `ParameterSettingsPanel`, `ReadOnlyDeclarationBanner`). **Rebuild both bundles.**

- [ ] Select a PathInput declared in a MATLAB script (e.g. `grPathTemplate`, `main_entrypoint_aim2.m`) → a grey "Read-only — declared in source" banner names `main_entrypoint_aim2.m:32` (file name only, not the full `Y:\…` path); Template, Root Folder, alternate input, Add and × are all greyed out and can't be typed into.
- [ ] Same for a Parameter declared in a `.m`/`.py` outside the entities file → banner shown; Add value, ×, and the Generate inputs/Replace values button are all disabled.
- [ ] Select a PathInput/Parameter declared in `scistack_entities.toml` → no banner, everything editable exactly as before.
- [ ] Edit the declaration in the `.m` file, hit 🔄 Refresh Code, reselect the node → the new values show.
- [ ] `scidb.log` shows `get_entity_editability: path_input '<name>' -> editable=False reason=read_only file=… line=…` on each selection.

---

## 0s. One project root; one load-errors list — added 2026-09-23

Backend only (`scifor.project_root`, `registry.all_load_errors`); no frontend rebuild needed.

- [ ] Open a project, then run a function fed by a PathInput with no `root_folder` → it resolves under the opened folder. `scidb.log` shows `[pathinput] project root override set to <that folder>` at load.
- [ ] A project whose `scistack.toml` has `[schema_keys]` → tables and plots still follow the declared level order.
- [ ] Break one Python entity and one MATLAB `.m` file → both errors still appear wherever load errors are shown (the registry panel and the project scan).
- [ ] MATLAB terminal run via the generated command → rootless PathInputs still resolve under the project.

---

## 0r. A new database opens on a blank canvas — added 2026-09-23

Backend only (`scope_filter.resolve_scope_view` + `declared_only` flag); no frontend rebuild needed.

- [ ] Create a new database in a project whose source declares Parameters and PathInputs → the root canvas is EMPTY; they are all listed in the sidebar.
- [ ] Drag a Parameter from the sidebar onto the canvas → it appears, showing its declared values; reload the window → it is still there.
- [ ] Same for a PathInput.
- [ ] Delete a placed, never-run Parameter/PathInput from the canvas → it leaves the canvas and does not come back on refresh (still in the sidebar).
- [ ] Open an existing database that has runs → every node with history is still on the canvas as before.
- [ ] An existing database where a never-run Parameter was wired to a function but never dragged → the Parameter and its edge are still shown.

---

## 0q. Variants panel — "why does this variable have more variants than I expected?" — added 2026-09-22

**What changed:** `Inspector.topologies` (already in the Python API and behind
`scidb variants <name>`) now has a GUI surface. It is **bottom-up**, where the
existing Provenance panel is top-down: you cannot pin a variant you do not know
exists. The `load:` line is the reason it exists — it is the only place in the
GUI where "this variant is not what a run will read" is visible. Plan:
`.claude/plan-topologies-panel.md`.

**Backend:** `scistack_gui/services/variants_service.py`,
`api/provenance.py` (`variable_topologies`), plus `variant_verdict` /
`location_sample` moved into `scidb/inspect/api.py` so the terminal and the
panel share one owner. **Frontend:** `components/Variants/TopologiesPanel.tsx`
+ `topologies.ts`. Both bundles rebuilt.

**Steps:**

1. Open the Stroke-R01-Aim-2 database. Click **🧬 Variants** in the toolbar and
   pick `GAITRiteLoaded`.
2. You should see **one topology** — `loadGaitRiteOneFile(...) → GAITRiteLoaded`
   — with **two variants** under it, differing in their `run` chip
   (`distribute=false` / `distribute=true`).
3. **The check that matters:** the older one should read
   `load: SUPERSEDED (an older run-option set)`, greyed and struck through, and
   the newer one `load: CURRENT`. Two variants that look equally alive is the
   bug this panel exists for — *tell me if they read the same*.
4. Compare with the terminal: run the `scidb variants GAITRiteLoaded` line the
   panel prints. The two must agree, variant for variant.
5. Tick **Show every location** and confirm the location lines grow from a
   sample (`subject/session/speed — SS01/BL/SSV, … (+417)`) to the full set,
   and that the panel stays responsive with 450 of them.
6. Right-click a **variable node** on the canvas → **🧬 Variants…**. The panel
   should open already on that variable. Confirm this is a *separate* menu
   entry from Provenance — they answer opposite questions.
7. Pick a variable with no producing steps (a raw-saved one). It should say so
   in words, not show an error.
8. `scidb.log` should carry `[variants] <name>: N topology/ies, M variant(s),
   K not current`.

---

## 0p. A node that is rewired and run stays ONE node — added 2026-09-22, revised 2026-09-23

**What changed:** a canvas function node's id is no longer
`fn__{fn}__{wiring_id}` — a hash of its *recorded* input bindings. It is
allocated once (`fn__{fn}__{random}`) and remembered in a new `_node_wiring`
table, with `wiring_id` demoted from identity to attribute. Plan:
`.claude/plan-node-identity.md`; argument: `docs/claude/node-identity.md`.

This is the fix for the 2026-09-22 session where drawing
`Demographics → grSides.side` and running produced **two** `grSides` nodes, and
where `schemaLevel` silently stopped applying and had to be re-entered.

**Backend:** `scistack_gui/node_wiring.py`, `domain/node_identity.py`,
`api/pipeline.py`, `domain/graph_builder.py`, `services/execution_service.py`,
`services/matlab_command_service.py`.
**Frontend:** the ambiguity dialog in `components/DAG/PipelineDAG.tsx`.

**Read this first — this one is a CLEAN BREAK.** There is no migration. On the
first build of an existing database every function node gets a brand-new id,
so **saved positions, node settings, hides and hypothesis membership keyed by
the old ids stop resolving**. Expect the canvas to re-lay-out and expect to
re-set node settings, once. That is intended (D-2026-09-22-3/-4).

Do this on a **copy** of the real database first, or on a scratch project, so
you can see what the churn actually costs before paying it on Stroke-R01-Aim-2.

**Steps:**

1. **The first open.** Open a copy of the real project. `scidb.log` should show
   `[node_identity] allocated N node id(s)` once, then
   `[node_wiring] <node> now runs as wiring <W>` per wiring — and **nothing**
   on later refreshes. Nodes will be re-laid-out; note anything else that
   looks lost so we can decide whether it should have been.
2. **Ids do not churn.** Refresh, switch hypothesis tabs, come back. The node
   ids must be identical every time. *Any churn here is a bug, not the break.*
3. **The grSides shape.** Draw an edge onto an unbound parameter of a node that
   has already run (`Demographics → grSides.side`). Set something distinctive on
   that node first — a **schema level** and a **column selection**.
4. Run the node from the canvas.
5. **The check that matters:** afterwards there should be **ONE** node, in the
   same place, still carrying the schema level and the column selection you set.
   Not two. *Tell me if a second one appears.*
6. **Its handles show the NEW shape only.** The node should show `side` bound,
   and should NOT have grown an extra handle or edge for the shape it had
   before. `scidb.log` says `[pipeline] N wiring(s) are history rather than a
   node's current shape` when that has happened.
7. **Run it again.** It must run the shape it has now — not also re-run the
   records it produced before you drew the edge. Check the run's target count.
8. `scidb.log` should carry `[execution] run <id> on node <n> ('grSides') claims
   N wiring(s) … recorded at dispatch` before the run, and NOT
   `[pipeline] manual edge … moved onto …` (that repair path is deleted).
9. **The same, from the terminal.** Repeat on a MATLAB node run through the
   MathWorks terminal, and again by running the pipeline (not the single node).
   Same answer: one node.
10. **A script run.** Draw an edge, then run the equivalent `for_each` from a
    Python script instead of the GUI. On the next canvas refresh it must still
    be one node — this is the inference path rather than the dispatch record,
    and `[node_identity] wiring <W> of 'grSides' attributed to <node> — that
    node STATES it` is the line that proves it fired.
11. **A genuinely new node is still new.** Drag a fresh function node in, wire
    it to *different* inputs, run it. It must be its own node, not merged into
    anything.
12. **The ambiguity popup.** Drag a second copy of a node and wire it identically
    to the first. On the next build a dialog should say they are wired the same,
    name both, and say runs go to one of them until they differ — and that
    nothing was merged. Dismiss it; it must not come back on every refresh.
    *Tell me if it nags.*
13. **A previously-duplicated node.** If the real database still has the two
    `grSides` nodes from 2026-09-22, they stay two nodes — there is no absorb.
    Hide or rewire one. It must not come back.
14. **Failures are loud now.** If the identity machinery ever cannot answer,
    the build fails rather than drawing something unverified. If you see the
    canvas refuse to load with a `_node_wiring` traceback, that is the design
    working — send me the traceback.

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
