# Process: The Canvas

The canvas is where you build and run your pipeline. Each processing step is a **function**, each kind of data is a **variable**, and arrows show what feeds into what. The whole analysis fits on one screen, and every step is coloured by whether its results are up to date.

The canvas shows the steps but doesn't hold your science. Your functions are ordinary Python or MATLAB files in your project, and your data lives in the project's database. The canvas connects the two: it says which data goes into which function and where the results are saved.

/// info | 📷 Screenshot
**Show:** a small finished pipeline (a path input → a loader function → a variable → a processing function → a variable), all nodes green, sidebar showing the palette.
///

## A tour of the window

| Area | What it's for |
|---|---|
| **Header** (top) | The database name, and buttons for the whole project: **🔄 Refresh Code**, **Restart**, **📄 Report**, **📁 Paths** and **📋 Inventory**. Your schema keys are listed on the right. |
| **Hypothesis tabs** (under the header) | One tab per research question. Each tab is its own canvas, with a **Research Question** line so you never lose track of what you're trying to answer. |
| **Canvas** (centre) | Your pipeline. Drag to move around, scroll to zoom. |
| **Sidebar** (right) | When nothing is selected, the **palette**: everything you can add to the canvas. When you click a node, that node's **settings**. Drag its left edge to make it wider. |
| **Runs dock** (bottom-left) | The status of every run. Click it to see progress, failures and messages. |

/// info | 📷 Screenshot
**Show:** the full window with each of the five areas labelled.
///

**🔄 Refresh Code** re-reads your Python and MATLAB files. Click it after you edit a function or add a new file, so the canvas sees the change. **Restart** restarts SciStack's Python process; you rarely need it, but it's there if something gets stuck.

## The building blocks

The palette has a tab for each kind of node:

| Node | What it is | Example |
|---|---|---|
| **Function** (`f(x)`) | One processing step. Either a function you wrote, or one from a library. | `filter_signal`, `pandas.read_csv`, `numpy.mean` |
| **Variable** (`x`) | A named kind of data, saved in the database at each of your schema locations. | `RawForce`, `PeakForce` |
| **Parameter** (`P`) | A setting passed to a function. Give it several values and the function runs once for each. | `cutoff_hz = 10, 15, 20` |
| **Path Input** (📁) | A pattern for finding files on disk, one file per schema location. | `data/{subject}/{session}.csv` |
| **Submodule** (⧉) | A group of steps packed into a single node, to keep a big pipeline readable. | `preprocessing` |

There's also **glue**: a small piece of free-form code placed between a variable and a function, for quick reshaping (renaming a column, picking a row) that doesn't deserve its own function.

## Building a pipeline

### 1. Add your functions

Functions you write in your project's `src` folder (`.py` or `.m` files) appear automatically in the **Functions** tab. Drag one onto the canvas to use it. Double-click a function node to open its source code in the editor.

To use a function you didn't write, click **+** in the Functions tab and type its name, such as `pandas.read_csv` or `numpy.mean`. You don't need to write a wrapper around it.

The **Process / Plots / Stats / Glue** filter at the top of the list helps in a large project. Functions whose names start with `plot_` or `stat_` are listed as plots and statistics. They're the **endpoints** of a pipeline: the figures and results the processing builds up to.

/// tip | Why your code stays in files
Keeping functions in normal files means you can use them with or without SciStack, test them, put them under version control and share them. The canvas only records how they're wired together.
///

### 2. Add variables

Click **+** in the **Variables** tab and give the new variable a name, such as `PeakForce`. Then drag it onto the canvas. You don't need to describe what it holds. SciStack works that out from the first value saved to it.

### 3. Point at your files with a path input

Raw data usually starts in files. A path input is a template that tells SciStack where each file lives, using your schema keys in curly braces:

```text
data/{subject}/{session}/force.csv
```

SciStack fills in the braces for every file it finds, so `data/S01/pre/force.csv` is read as the file for `subject = S01, session = pre`. If your whole dataset is in one file, the template is just its path, such as `data/ratings.csv`.

Click **+** in the **Path Inputs** tab, name it, and set the template in its settings. If the same data is stored under two different folder layouts, add an **alternate template**.

### 4. Add parameters (optional)

A parameter is a setting that you want SciStack to keep track of, such as a filter cutoff. Click **+** in the **Parameters** tab, then add one or more values. If you give it three values, the function it feeds runs three times, and the three results are kept side by side as **variants**. You don't need a separate copy of the pipeline for each setting.

### 5. Connect them

Drag from the small handle on one node to the handle on another:

- **path input → function input**: the function receives the file path.
- **function output → variable**: the function's result is saved as that variable.
- **variable → function input**: the function receives that variable's data.
- **parameter → function input**: the function receives that setting.

A function only receives what you connect to it. SciStack never guesses based on names. If an input isn't connected, the node turns red and tells you before you run it.

/// info | 📷 Screenshot
**Show:** dragging an edge from a variable's handle to a function's input handle, with the input names visible on the function node.
///

## Running

There are three ways to run:

| Control | Where | What it runs |
|---|---|---|
| **▶ Run** | On a function node | That one step, for every schema location it covers. |
| **▶ Run until here** | Right-click a function node | That step and every step before it that isn't up to date. A preview lists the steps first, so you can check before anything runs. |
| **▶ Run endpoints** | Panel on the canvas | Every `plot_` and `stat_` endpoint, plus whatever they need. Tick **finalized** to save the figures and results as records. Leave it unticked for a quick draft that saves nothing. |

Endpoint nodes also have a **👁 Show** button. It draws the figure or result as a draft, without saving anything, so you can adjust it as often as you like.

While a run is going, the Runs dock shows its progress. When it ends, the dock summarises what finished, what failed and why, and what was skipped. The full story is also written to `scidb.log`, next to your database.

**Running again is safe.** If a function produces the same result at the same location, SciStack keeps the existing record rather than adding a duplicate. You can re-run after any change without cleaning up first.

### What the colours mean

| Colour | Meaning |
|---|---|
| **Green** | Up to date: this step has results for every location it should cover, from the current code and settings. |
| **Red** | Something needs attention: results are missing or out of date, an input isn't connected, or a step before it is red. |
| **Yellow** | Pending: a parameter value you've added but haven't run yet. |

Red flows downstream. If you edit a function, it turns red, and so does everything that depends on it, because those results were built from the old version. Run it again and the whole chain turns green.

/// warning | Changing an input file doesn't turn anything red
SciStack records *which* file was read, not what was inside it. If you edit or replace a data file but keep its name, nothing turns red. Re-run the loader yourself.
///

## Node settings

Click a node to see its settings in the sidebar.

### Functions

| Section | What it controls |
|---|---|
| **Variants** | Every combination of parameter values this function runs with. Click **hide** to stop running one combination without deleting its results. |
| **Inputs** | For an input that's a table, which of its columns the function receives. |
| **Data Filters** | Conditions a record must meet to be used, such as `Rating > 0`. |
| **Schema Selection** | Which schema locations the function runs at. Everything runs unless you leave some out. |
| **Schema Level** | Which schema keys the function runs **once per**. See below. |
| **Run Options** | Less common options: a dry run that only previews, or passing data to the function as a table that keeps its schema columns. |

**Schema level** is the setting that matters most. Suppose your schema is `participant, brand`:

- Tick **participant and brand**: the function runs once per rating and sees one number at a time.
- Tick **brand** only: it runs once per brand and receives every participant's rating for that brand. This is how you compute a per-brand summary.
- Tick **nothing**: it runs once, on the whole dataset. This is how you run a test that compares the brands.

You rarely need to set it by hand. SciStack picks a sensible level from the step's inputs, and you change it only when you want something different.

### Variables

Clicking a variable shows a **quick plot** of its data, a table of its **variants** with how many records each one holds, and an **Open Plot Studio →** button for building a proper figure (see [Visualize](visualize.md)).

Right-click a variable for more:

- **📈 Plot** opens it in Plot Studio.
- **🗂 View Schema Locations** shows every location in your schema and whether this variable has data there. Click a location to plot it.
- **🧬 Variants…** lists each version of this variable as a card: which settings and code produced it, and how many locations it covers. **Make current** picks which version plots and later steps use by default. **Delete** removes a version you're sure you don't need.

/// info | 📷 Screenshot
**Show:** the Variants popup for a variable with two variants (two parameter values), one marked current.
///

## Finding missing data

Real datasets have gaps: a participant who skipped a session, a file that never got copied. Click **📋 Inventory** in the header to see every combination of your schema keys, each marked **present**, **missing** or **excluded**. Choose which keys to check, and whether any key (a study arm, a site) should be checked separately for each of its values. Long runs of complete data are folded into one "(all)" row, so the gaps stand out.

/// info | 📷 Screenshot
**Show:** the Inventory popup with a few missing rows.
///

## Undo, copy and paste

Everything you do on the canvas can be undone with **Ctrl+Z** (**Cmd+Z** on a Mac) or the undo buttons, including adding, moving, connecting and removing nodes. Select nodes and press **Ctrl+C** / **Ctrl+V** to copy part of a pipeline.

Removing a node or edge from the canvas never deletes data. A step you remove is hidden, its results stay in the database, and you can bring it back.

## Larger projects

- **Submodules** pack a group of steps into one node. Double-click it to go inside. The breadcrumb above the canvas shows where you are.
- **Hypothesis tabs** keep separate research questions on separate canvases while they share the same data and functions.

## Sharing and reporting

- **📄 Report** collects every finalized figure and statistical result into one web page, each labelled with where it came from.
- **Export project** packs the project into a single bundle file. A colleague opens it with **SciStack: Import Project Bundle…** from the Command Palette.
- **Verify reproduction** re-runs an imported project and compares the results with the ones the original author got.

## Next steps

- [Visualize](visualize.md): turn any variable into a figure with Plot Studio.
- [Summarize](summarize.md): tables of statistics and test results with Stat Studio.
- [Best Popcorn](../examples/best-popcorn.md): a complete analysis, built entirely in the GUI.
