# SciStack

**Spend more time on your science, and less time on the code around it.**

SciStack is made for scientists by scientists. It takes the analysis functions you already write in Python or MATLAB and handles everything around them: looping over every subject, session, or trial; saving and loading results; and keeping a record of how each result was produced.

```bash
pip install scistack   # one install for Python, MATLAB, the CLI, and the GUI backend
```

SciStack does not do any analysis itself. It helps you run code-based analyses more effectively, whether you use your own code or build on someone else's.

📖 **Documentation:** [Code](https://scistack.readthedocs.io/en/latest/) · [GUI](https://scistack.readthedocs.io/en/latest/ui/)

---

## Who is SciStack for?

SciStack is for researchers who use code to get to their results but don't want to become software engineers. It suits you if:

- Your data is **nested**:
  - participants have sessions, sessions have trials
  - players have seasons, seasons have games.
- You run a **series of steps on every piece** of that data: load, filter, compute a metric, summarize.
- You work in **Python, MATLAB, or both**.
- You've ever found a file called `results_v2_final_FINAL.csv` and weren't sure what made it.

No database or software background is needed. If you can write a function, you can use SciStack.

## The problem

Most analysis code starts out like this:

```python
for subject in ["S01", "S02", "S03"]:
    for session in ["pre", "post"]:
        data = scipy.io.loadmat(f"data/{subject}/{session}/raw.mat")
        filtered = lowpass_filter(data["force"], cutoff_hz=10)
        peak = np.max(filtered)
        np.save(f"results/{subject}_{session}_peak_v2_final.npy", peak)
```

That works for a while. Then the script grows to hundreds of lines. The analysis gets tangled up with loops, file paths, and naming conventions. Change the cutoff and the old results are overwritten, or you end up with another `_v3` file. Six months later, nobody can say which settings produced the figure in the paper. Sharing the code means sharing hard-coded paths that only work on your computer.

## What SciStack gives you

The same analysis with SciStack:

```python
def peak_force(force, cutoff_hz):
    return np.max(lowpass_filter(force, cutoff_hz))

scidb.for_each(peak_force,
               inputs={"force": RawForce, "cutoff_hz": 10},
               outputs=[PeakForce],
               subject=[], session=[])   # [] means "every subject and session"
```

Your function only contains the science. SciStack:

- **Runs it once for every subject and session**, handing it one piece of data at a time.
- **Saves each result** in a database file, at its subject and session.
- **Records how each result was made:** which function, which inputs, and which settings.
- **Keeps both versions** when you change `cutoff_hz` to 15, instead of overwriting the first.
- **Lets you load any result later** by asking for it: `PeakForce.load(subject="S01", session="pre")`.

## Three ways to use it

SciStack is a _stack_ of tools. Start with the lightest one that does the job, and move up when you want more.

| Tool             | What it does                                                                                           | Good for                                                                 |
| ---------------- | ------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| **`scifor`**     | Replaces nested `for` loops. Works on tables in memory, with no database.                              | Quick analyses, or adding structure to an existing script                |
| **`scidb`**      | Everything in `scifor`, plus a database that saves every result along with how it was made.            | Real projects you'll come back to, change, and share                     |
| **SciStack GUI** | A VS Code extension that draws your `scidb` pipeline as a diagram you can run, inspect, and plot from. | Seeing the whole pipeline at once, and working without writing loop code |

All three work from **Python and MATLAB**. Data saved from one language can be loaded in the other.

## Quick start: the GUI

1. Install the [SciStack Pipeline GUI](https://marketplace.visualstudio.com/items?itemName=scistack.scistack-gui) extension in VS Code.
2. Pick a Python environment (`Python: Select Interpreter` in the Command Palette) and run `pip install scistack` in it.
3. Run **`SciStack: Open Pipeline`** from the Command Palette.
4. Create a new database: give it a name and list your **schema keys**, the levels your data is organized by, such as `subject, session, trial`.

That's it. You'll see a blank canvas, ready for your first function. The [GUI docs](https://scistack.readthedocs.io/en/latest/ui/) walk through the rest.

## Quick start: code

### Step 1: Replace your loops with `scifor`

No database and no setup beyond naming your schema keys:

```python
import scifor

scifor.set_schema(["subject", "session"])

results = scifor.for_each(
    peak_force,
    inputs={"force": force_table, "cutoff_hz": 10},
    subject=[1, 2, 3],
    session=["pre", "post"],
)
```

For each of the 6 combinations, `scifor` takes the matching rows of `force_table`, passes the `force` column to `peak_force`, and collects the answers into one table with `subject`, `session`, and result columns.

```matlab
scifor.set_schema(["subject", "session"]);

results = scifor.for_each(@peak_force, ...
    struct("force", force_table, "cutoff_hz", 10), ...
    subject=[1, 2, 3], session=["pre", "post"]);
```

### Step 2: Keep your results with `scidb`

One hundred shoppers each rated three brands of popcorn from 1 to 10. Which brand is best?

```python
import numpy as np
import pandas as pd
import scidb
from scidb import BaseVariable

# A variable is a named kind of data. Declaring one takes one line.
class Rating(BaseVariable):
    """One participant's rating of one brand."""

class BrandSummary(BaseVariable):
    """The mean and SD of one brand's ratings."""

def load_ratings(csv_path):
    return pd.read_csv(csv_path)

def summarize(ratings):
    r = np.ravel(ratings)
    return pd.DataFrame({"mean": [r.mean()], "sd": [r.std(ddof=1)]})

# Create (or open) the database. Each rating lives at a participant and a brand.
scidb.configure_database("popcorn.duckdb", ["participant", "brand"])

# Import the CSV: each row becomes its own Rating record
scidb.for_each(load_ratings, inputs={"csv_path": "popcorn_ratings.csv"}, outputs=[Rating])

# One summary per brand
scidb.for_each(summarize, inputs={"ratings": Rating["rating"]},
               outputs=[BrandSummary], brand=[])
```

Close Python, come back next week, and everything is still there:

```python
db = scidb.configure_database("popcorn.duckdb", ["participant", "brand"])

BrandSummary.load(brand="KettleKing").data         # the saved result
db.get_provenance(BrandSummary, brand="KettleKing")  # how it was made
```

Running the script again doesn't create duplicates, so you can re-run after any change without cleaning up first.

The [Best Popcorn](https://scistack.readthedocs.io/en/latest/examples/best-popcorn-scidb/) and [Best Player](https://scistack.readthedocs.io/en/latest/examples/best-player-scidb/) examples go through complete analyses, in both Python and MATLAB.

## Big wins, small effort

These features take a line or two of code (or a click) and save the most time:

| You write...                                         | You get...                                                                                                                               |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `subject=[], session=[]`                             | Your function runs on every combination, with no loops to maintain. Add a subject and it's included on the next run.                     |
| `outputs=[MyResult]`                                 | Every result is saved at its subject, session, or trial. No file names, no folders, no overwriting.                                      |
| _Nothing extra_                                      | Every result records the function, inputs, and settings that produced it, so you can trace any number back to the raw file it came from. |
| `EachOf(10, 15, 20)`                                 | One run tries every setting. All the results are kept and labelled with their settings, so you can compare them side by side.            |
| `scidb.exclude_schema(subject=3, reason="withdrew")` | Subject 3 is skipped by every step, including ones you write later. Nothing is deleted, the reason is saved, and you can undo it.        |
| `scidb inventory --keys subject,session`             | A list of which subject and session combinations have data and which are missing.                                                        |
| `scidb trace PeakForce subject=S01 session=pre`      | The full history of one result, from the terminal.                                                                                       |

## Your data isn't locked away

Everything (your data and its history) lives in **one [DuckDB](https://duckdb.org/) file** next to your code. Each variable gets a readable table you can open in [DBeaver](https://dbeaver.io/), the DuckDB command line, or any SQL tool:

```sql
SELECT participant, brand, rating FROM Rating;
```

You don't need Python or MATLAB to look at your results, and you're not tied to SciStack to get them back out. Your functions are plain functions: they take normal arrays and tables and return normal values, so they work with or without SciStack.

## The GUI at a glance

The SciStack GUI runs inside VS Code, next to your code:

- **See the whole pipeline.** Functions and variables are drawn as a diagram, connected by what feeds into what.
- **Know what's up to date.** Each step is coloured by whether its results are current, so you can see what needs re-running after you edit a function.
- **Run from the canvas.** Run one step or the whole pipeline, and watch the progress.
- **Plot without writing plotting code.** Plot Studio turns any variable into a publication-ready figure: group by condition, split into panels, choose colours, and save.
- **Compare versions.** When a change in settings or code produces several versions of a result, the Variants view lists them side by side.
- **Find missing data.** The Inventory shows which combinations of your schema keys have no data.
- **Share a project.** Export the project as a bundle that a colleague can import and re-run.

## Learn more

- **[Installation](https://scistack.readthedocs.io/en/latest/getting-started/installation/)**, including one-time MATLAB setup
- **[What's a schema?](https://scistack.readthedocs.io/en/latest/getting-started/whats-a-schema/)**: the one design decision to make before you start
- **[Create a new project](https://scistack.readthedocs.io/en/latest/getting-started/create-new-project/)** (or [with the GUI](https://scistack.readthedocs.io/en/latest/ui/getting-started/create-new-project/))
- **[Example projects](https://scistack.readthedocs.io/en/latest/examples/best-popcorn-scifor/)**: complete analyses, start to finish
- **[API reference](https://scistack.readthedocs.io/en/latest/api/scifor/)**

## Contributing

SciStack is in beta and under active development. To work on it locally:

```bash
git clone https://github.com/mtillman14/scistack
cd scistack
./dev-install.sh
```

See the [contributing guide](https://github.com/mtillman14/scistack/blob/main/docs/project/contributing.md) for details. Bug reports and feature requests are welcome on the [issue tracker](https://github.com/mtillman14/scistack/issues).
