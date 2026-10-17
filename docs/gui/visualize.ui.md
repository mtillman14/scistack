# Visualize: Plot Studio

Plot Studio turns any variable into a figure without any plotting code. You decide what the figure should show, such as conditions side by side or one panel per group, and Plot Studio does the drawing.

Three things make it different from a plotting script:

- **The preview is the saved file.** What you see in the panel is exactly what Save writes, down to the label sizes and legend placement.
- **Figures follow your data.** A figure is built from the database, so after you re-run a step, reopening the figure shows the new results. A saved figure remembers which version of the code it was drawn from, and asks before switching to newer data.
- **Nothing is lost.** Name a figure and it's saved with every setting. Reopen it next month exactly as you left it.

/// info | 📷 Screenshot
**Show:** Plot Studio with a finished bar chart: the left rail, the figure with its toolbar, and the Saved plots rail on the right.
///

## Opening Plot Studio

| From | How |
|---|---|
| A variable on the canvas | Click it, then **Open Plot Studio →** in the sidebar. Or right-click it and choose **📈 Plot**. |
| One location | Right-click a variable, choose **🗂 View Schema Locations**, and click the location you want. |
| A `plot_` function | Right-click the node and choose **📈 Open in Plot Studio**, to adjust a figure your own function draws. |
| The Command Palette | **SciStack: Plot Variable…**, or **SciStack: Plot CSV File** to plot a CSV file with no database at all. |

Each figure opens in its own tab, so the canvas stays visible beside it.

## How the window is laid out

| Area | What's there |
|---|---|
| **Left rail** | Every setting for the figure, in five groups that follow the order you build a figure in: **Data**, **Chart**, **Structure**, **Statistics** and **Appearance**. Each group shows a one-line summary when it's closed. |
| **Figure** (centre) | The live preview, with the **toolbar** above it for size and saving. |
| **Right rail** | **Saved plots** and **Presets**. |

Click **ⓘ** beside any setting for a short explanation.

## The key idea: every schema key has a role

Plot Studio starts from your schema. Suppose each of 100 participants rated three brands of popcorn, so the schema is `participant, brand`. Each schema key gets one of four roles, and the roles decide the figure:

| Role | What it does | With popcorn |
|---|---|---|
| **Grouping** | One mark (bar, box, point) per level, side by side along the x axis | `brand` in Grouping: one bar per brand |
| **Separate panels** | One small panel per level, in a grid | `brand` as panels: three panels |
| **Separate figures** | One whole figure per level. Step through them with **←** and **→**. | `participant` as figures: 100 figures, one per person |
| **Collapse** | Averaged away. The values are combined into a summary. | `participant` collapsed: each bar is the mean of 100 people |

When Plot Studio first opens a variable, any key that isn't grouped is set to **Separate figures**, which shows the raw data one piece at a time. Change a key to **Collapse** when you want a summary across it.

/// tip | Why roles instead of chart settings?
The same data can answer different questions. "How did *this person* rate each brand?" and "How did people rate each brand *on average*?" use the same three brands and 100 people. The only difference is whether `participant` is a separate figure or collapsed. Changing one role switches between the two figures.
///

**The sample.** The last key you collapse is the **sample**: the error bars, box plots and statistics are calculated over its levels. With popcorn, collapsing `participant` makes each bar's error bar describe the spread across the 100 people. If you also had trials inside each participant, collapse both. Trials are averaged within each person first, and then people are summarised. This means a person with more trials doesn't count more than anyone else.

/// info | 📷 Screenshot
**Show:** the Structure group with `brand` in Grouping and `participant` set to Collapse in Factors, next to the resulting bar chart.
///

## Building a figure, group by group

### Data: what's plotted

- **Variants.** The variable being plotted. Click **+ Add** to plot another variable alongside it, or another version of the same one (for example, two filter settings side by side). Each row can be picked on a small copy of your pipeline, so you can see exactly where the data comes from.
- **Schema keys.** Choose which locations to include, such as all participants except two.
- **Filters.** Keep only some levels, or a range of values.

### Chart: what kind of figure

| Plot type | Good for |
|---|---|
| **Bar + error** | Comparing averages, with SD, SEM, 95% CI or IQR error bars. |
| **Box** / **Violin** | Comparing whole distributions. |
| **Strip (jittered)** | Showing every individual value. |
| **Scatter** | Every value as a point, or one variable plotted against another. |
| **Spaghetti (paired lines)** | Repeated measures: one line per person, joined across conditions. |
| **Lines** / **Mean + error band** | Curves and time series, such as a gait cycle or a signal over time. |
| **Heatmap** | Data with two dimensions, such as a matrix or an image. |

A plot type that doesn't fit the data is greyed out. Hover over it to see why.

### Structure: how the figure is organised

- **Grouping.** Click **Group by…** to choose the keys that become marks along the x axis. With two or three keys, the axis is nested, for example sessions inside groups. Use the arrows to change which key is on the inside. Tick **colour** on one key to colour the marks by its levels. Colour never moves a mark: it only paints marks that are already in place.
- **Combine.** Merge levels into new ones, such as combining `pre1` and `pre2` into `pre`. Levels you leave out are dropped from the figure.
- **Compare.** Plot each value as a **difference** or **% change** from a reference level, such as the change from baseline.
- **Factors.** Set the role of every key that isn't grouped: **Separate figures**, **Separate panels** or **Collapse**.
- **Layout.** When a key makes panels, choose how many rows and columns the grid has, and where each panel goes.

You can also group by a column inside your data rather than a schema key. For example, if each record has a `side` column holding left and right, you can group by `side`.

### Statistics: how collapsed data is summarised

This group appears once something is collapsed.

- **Summary.** **Centre** is the mean or median. **Spread** is SD, SEM, 95% CI, IQR or none. **Weight by N** gives every row the same weight instead of averaging level by level, so a participant with more trials counts more.
- **Show sample.** Draws the individual sample values on top of the bars or boxes. **Join points** connects each participant's values across conditions, which is the clearest way to show a repeated-measures design. **Colour points by** paints the points by a key of their own.
- **Difference bars.** Draws a bracket between two marks to show they differ. Click **+ Add difference bar**, then click the two marks in the preview, and type a label such as `*` or `p < 0.001`. If you'd rather have brackets calculated from a statistical test, [Stat Studio](summarize.md) can draw them for you.

/// info | 📷 Screenshot
**Show:** a bar chart with Show sample on and Join points set to Lines (one grey line per participant), with a difference bar between two bars.
///

### Appearance: how it looks

This group starts closed, because the defaults are usually good. Open it to adjust:

- **Size.** The figure's width and height, and its aspect ratio.
- **Y axis.** Fixed minimum and maximum values, and whether panels share a y axis.
- **Panels.** A different y range or y title for one panel.
- **X tick labels.** Rotation, which labels to show, and whether to show empty slots for levels this figure has no data for.
- **Marks.** Line and point thickness.
- **Text.** Text sizes. By default, every piece of text is as large as it can be without overlapping. Choose **Print** or **Slide** for the right range of sizes, or set any size yourself.
- **Labels.** The title, axis titles and **aliases**: display names such as "Butterburst" for `BB`. Click **↑** to use a label for every figure in the project.
- **Colours.** Pick the colour for any level. Click **↑** to use that colour everywhere in the project, so `pre` is the same blue in every figure.

## The toolbar

| Control | What it does |
|---|---|
| **Preview at** | **Fit pane** fills the panel. **Export size** shows the figure at exactly the size it will be saved. |
| **W**, **H**, units | The saved size, in inches, millimetres or pixels. Tick **Lock aspect** to keep the shape while you resize. |
| **Format**, **Save image** | Save as PNG, SVG, PDF or EPS. **Save all** saves every figure when a key is set to Separate figures. Files are named after their levels, such as `Rating_participant=P001.png`. |
| **Copy PNG** | Copies the figure to the clipboard, ready to paste into slides or a document. |
| **Save data (CSV)** | The exact numbers the figure is drawn from. |
| **Export code** | A stand-alone Python script that draws the same figure, for when you need to go beyond Plot Studio. |
| **Add to pipeline** | Turns the figure into a `plot_` step on the canvas, so it's re-drawn whenever its data changes. |
| **StatStudio** | Opens [Stat Studio](summarize.md), set up with the same design as this figure. |

## Saving and reusing figures

- **Saved plots** (right rail). Give the figure a name and save it. It's kept with every setting, and each later save adds a new version. When the data has been re-run since you saved, Plot Studio tells you and asks whether to switch to the newer results.
- **Presets** (right rail). A preset saves the *look* of a figure (plot type, roles, colours, sizes and text) without the data. Apply it to a different variable to give a whole set of figures the same style.
- **Undo.** **Ctrl+Z** (**Cmd+Z**) undoes any change in Plot Studio.

/// tip | Light and dark
The **☀ / ☾** button switches Plot Studio between light and dark. Dark mode recolours the figure to be easier on the eyes on screen. Light mode shows it exactly as it will be saved, so switch to light for a final check.
///

## Next steps

- [Summarize](summarize.md): add descriptive statistics and tests that use the same design as your figure.
- [Best Popcorn](../examples/best-popcorn.md): build a figure step by step on a real dataset.
