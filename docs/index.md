---
hide:
  - navigation
---

# SciStack

**Spend more time on your science, and less time on the code around it.**

SciStack is made for scientists by scientists. It takes the analysis functions you already write in Python or MATLAB and handles everything around them: looping over every subject, session, or trial; saving and loading results; keeping a record of how each result was produced; and turning those results into figures and statistics tables.

```bash
pip install scistack   # one install for Python, MATLAB, the CLI, and the GUI backend
```

[Install SciStack](getting-started/installation.md){ .md-button .md-button--primary }
[Create your first project](getting-started/create-new-project.md){ .md-button }

/// info | 📷 Screenshot
**Show:** VS Code with the SciStack canvas open: a small pipeline from a path input to a `plot_` endpoint, all nodes green, and Plot Studio open in a second tab showing a bar chart with error bars.
///

SciStack does not do your analysis for you. The science stays in your own functions, or in functions someone else wrote. SciStack is the layer underneath that makes them easier to run, trust, and share.

---

## From raw data to results

An analysis usually has three stages. SciStack covers all three, and they all read from the same database, so a figure or a table can always be traced back to the files it came from.

<div class="grid cards" markdown>

-   :material-sitemap-outline:{ .lg .middle } **Process**

    ---

    Write each step as a plain function. SciStack runs it for every subject, session, and trial, saves each result at its place in your study, and records the function, inputs, and settings that made it.

    [:octicons-arrow-right-24: The canvas (GUI)](https://scistack.readthedocs.io/en/latest/ui/gui/process/) ·
    [`for_each` (code)](api/scidb.md)

-   :material-chart-bar:{ .lg .middle } **Visualize**

    ---

    Plot Studio turns any variable into a publication-ready figure without plotting code. Choose what's grouped, what's split into panels, and what's averaged, then save as PNG, SVG, PDF, or EPS, or export a stand-alone Python script.

    [:octicons-arrow-right-24: Plot Studio](https://scistack.readthedocs.io/en/latest/ui/gui/visualize/)

-   :material-table-large:{ .lg .middle } **Summarize**

    ---

    Stat Studio builds the tables that go in a paper: descriptives per condition, and comparisons with effect sizes and confidence intervals. It reads its design from your figure, so the table and the figure always describe the same data.

    [:octicons-arrow-right-24: Stat Studio](https://scistack.readthedocs.io/en/latest/ui/gui/summarize/)

</div>

## A small example

Most analysis code starts out like this:

/// tab | Python

```python
for subject in ["S01", "S02", "S03"]:
    for session in ["pre", "post"]:
        data = scipy.io.loadmat(f"data/{subject}/{session}/raw.mat")
        filtered = lowpass_filter(data["force"], cutoff_hz=10)
        peak = np.max(filtered)
        np.save(f"results/{subject}_{session}_peak_v2_final.npy", peak)
```
///

/// tab | MATLAB

```matlab
subjects = {'S01', 'S02', 'S03'};
sessions = {'pre', 'post'};

for i = 1:numel(subjects)
    for j = 1:numel(sessions)
        data = load(fullfile('data', subjects{i}, sessions{j}, 'raw.mat'));
        filtered = lowpass_filter(data.force, 10);
        peak = max(filtered);
        save(fullfile('results', [subjects{i} '_' sessions{j} '_peak_v2_final.mat']), 'peak');
    end
end
```
///

It works for a while. Then the loops, file paths, and naming conventions get tangled up with the science. Change the cutoff and the old results are overwritten, or you end up with another `_v3` file. Six months later, nobody can say which settings produced the figure in the paper.

The same analysis with SciStack:

/// tab | Python

```python
def peak_force(force, cutoff_hz):
    return np.max(lowpass_filter(force, cutoff_hz))

scidb.for_each(peak_force,
               inputs={"force": RawForce, "cutoff_hz": 10},
               outputs=[PeakForce],
               subject=[], session=[])   # [] means "every subject and session"
```
///

/// tab | MATLAB

```matlab
function peak = peak_force(force, cutoff_hz)
    peak = max(lowpass_filter(force, cutoff_hz));
end
```

```matlab
scidb.for_each(@peak_force, ...
    struct("force", RawForce(), "cutoff_hz", 10), ...
    {PeakForce()}, ...
    subject=[], session=[]);   % [] means "every subject and session"
```
///

Your function only contains the science. SciStack:

- **Runs it once for every subject and session**, handing it one piece of data at a time. Add a subject and it's included on the next run.
- **Saves each result** in a database file, at its subject and session. No file names, no folders.
- **Records how each result was made:** which function, which inputs, and which settings.
- **Keeps both versions** when you change `cutoff_hz` to 15, instead of overwriting the first.
- **Lets you load any result later** by asking for it: `PeakForce.load(subject="S01", session="pre")`.

## Questions you can answer

Sooner or later, a co-author, a reviewer, or you in six months will ask about your results. With SciStack, the answers are already recorded.

| The question | Where the answer comes from |
| --- | --- |
| *"Which settings made Figure 3?"* | Every result records the function, inputs, and settings that produced it. `scidb trace` (or the GUI) follows any number back to the raw file it came from. |
| *"What if you'd used a different cutoff?"* | `EachOf(10, 15, 20)` runs every setting in one go. All the results are kept, labelled with their settings, and can be plotted side by side. |
| *"Why is subject 3 missing?"* | `scidb.exclude_schema(subject=3, reason="withdrew")` skips a subject in every step, including steps written later. Nothing is deleted, and the reason is saved. |
| *"Is any data missing?"* | The **Inventory** lists which combinations of subject, session, and trial have data and which don't. |
| *"Did you average the trials before testing?"* | Every Stat Studio table has a caption that states the unit of analysis and *n*, the tests, the correction, and the effect-size definition. |
| *"Can I re-run your analysis?"* | The whole project exports as one bundle a colleague can import and re-run. Your functions are plain functions, and your data is in an open file format. |

## What's different about SciStack

<div class="grid cards" markdown>

-   :material-file-tree-outline:{ .lg .middle } **Data addressed by your study, not by file names**

    ---

    You describe how your data is organised once, as **schema keys** such as `subject, session, trial`. From then on, every result is saved *at* a place in that structure, and you load it by asking for that place. [What's a schema?](getting-started/whats-a-schema.md)

-   :material-history:{ .lg .middle } **A record of every result, for free**

    ---

    Provenance is recorded as a side effect of running your code, not as an extra chore. Each result knows its function, its inputs, and its settings, all the way back to the raw files.

-   :material-source-branch:{ .lg .middle } **Analysis choices are kept, not overwritten**

    ---

    Changing a filter cutoff or editing a function doesn't destroy the earlier results. Every version is kept as a **variant**, so you can compare analysis choices side by side instead of committing to one in advance.

-   :material-scale-balance:{ .lg .middle } **Figures and statistics share one design**

    ---

    The roles you give your schema keys in Plot Studio (grouped, split, averaged) describe your study design. Stat Studio reads that same design, works out which factors are within- or between-participants from the data, and warns you when no unit of analysis has been chosen, a common cause of pseudoreplication.

-   :material-lock-open-variant-outline:{ .lg .middle } **Your data isn't locked away**

    ---

    Everything lives in one [DuckDB](https://duckdb.org/) file next to your code, with a readable table for each variable. Open it in [DBeaver](https://dbeaver.io/) or any SQL tool. Python and MATLAB read and write the same database.

-   :material-stairs-up:{ .lg .middle } **Adopt it one step at a time**

    ---

    Start by replacing a few loops, and move up when you want more. Each tool builds on the one before it, so nothing you've written has to be thrown away.

</div>

## Three ways to use it

SciStack is a _stack_ of tools. Start with the lightest one that does the job.

| Tool | What it does | Good for |
| --- | --- | --- |
| **`scifor`** | Replaces nested `for` loops. Works on tables in memory, with no database. | Quick analyses, or adding structure to an existing script |
| **`scidb`** | Everything in `scifor`, plus a database that saves every result along with how it was made. | Real projects you'll come back to, change, and share |
| **SciStack GUI** | A VS Code extension that draws your `scidb` pipeline as a diagram you can run and inspect, with Plot Studio and Stat Studio built in. | Seeing the whole pipeline at once, and making figures and tables without writing code |

All three work from **Python and MATLAB**. Use the **Code / With GUI** switch at the top of any page to see these docs for writing code or for using the GUI.

## Why SciStack exists

### The current state of scientific computing

The current state of scientific computing is a fractured one. Different disciplines use tools that are unbeknownst to one another; labs within the same field waste time rediscovering the metaphorical wheel, all because journal articles' data and code [are not often published](https://pmc.ncbi.nlm.nih.gov/articles/PMC10334349/).

Thanks to recent [US federal funding mandates](https://grants.nih.gov/policy-and-compliance/policy-topics/sharing-policies/dms/policy-overview#introduction-to-the-2023-nih-data-management-&-sharing-policy), since 2023 [data sharing rates have increased to about 14.6%](https://pmc.ncbi.nlm.nih.gov/articles/PMC11135734/)! While this is progress, it is still very poor performance. Worse still, no such mandates have been given for code sharing, another crucial part of reproducible science.

### Better research tools, better research outcomes

To advance science by leveraging existing research, better tools are sorely needed. While there are many pieces of brilliant domain-specific analytical software written by and for researchers, a lack of interoperability between tools greatly hampers their efficacy to advance science.

The problem, as I see it, is that domain-specific software sits, unsupported, too high up on the [software stack](https://en.wikipedia.org/wiki/Solution_stack), missing an abstraction that should sit between the software's programming language (Python, MATLAB, etc.) and the domain-specific software. This is the niche that SciStack fills.

When every lab's results are addressed the same way, carry their own history, and live in an open file format, sharing data and code stops being an extra project at the end of a paper. It's just the project. [More on the state of the art](getting-started/state-of-the-art.md).

## Get started

<div class="grid cards" markdown>

-   :material-download:{ .lg .middle } **Install**

    ---

    One `pip install` for Python, MATLAB, the CLI, and the GUI, plus the VS Code extension.

    [:octicons-arrow-right-24: Installation](getting-started/installation.md)

-   :material-file-tree-outline:{ .lg .middle } **Choose a schema**

    ---

    The one design decision to make before you start: the levels your data is organised by.

    [:octicons-arrow-right-24: What's a schema?](getting-started/whats-a-schema.md)

-   :material-flask-outline:{ .lg .middle } **Work through an example**

    ---

    Which of three popcorn brands is best? A complete analysis from CSV to figure to statistics.

    [:octicons-arrow-right-24: In code](examples/best-popcorn-scidb.md) ·
    [In the GUI](https://scistack.readthedocs.io/en/latest/ui/examples/best-popcorn/)

-   :material-book-open-variant:{ .lg .middle } **Look something up**

    ---

    Every function and option in `scifor` and `scidb`.

    [:octicons-arrow-right-24: API reference](api/scifor.md)

</div>

/// note | SciStack is in beta
SciStack is under active development, and the statistics in Stat Studio are newest of all. Bug reports and feature requests are welcome on the [issue tracker](https://github.com/mtillman14/scistack/issues).
///
