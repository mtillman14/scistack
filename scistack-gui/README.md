# SciStack GUI

The Python backend for the [SciStack Pipeline GUI](https://marketplace.visualstudio.com/items?itemName=scistack.scistack-gui) VS Code extension. It draws your [SciStack](https://github.com/mtillman14/scistack) pipeline as a diagram you can run, inspect, and plot from.

![A SciStack pipeline on the canvas: a CSV path input, read by pandas.read_csv, saved as 300 Rating records](https://raw.githubusercontent.com/mtillman14/scistack/main/docs/images/example_best-popcorn/after_first_run.png)

📖 **Documentation:** [GUI docs](https://scistack.readthedocs.io/en/latest/ui/)

## Install

Most people don't install this package directly. It comes with `scistack`:

```bash
pip install scistack
```

Install it into the Python environment that VS Code uses (`Python: Select Interpreter` in the Command Palette), **at the same version as the extension**. The extension warns on startup if the two versions differ, and shows the exact `pip install` command that fixes it.

## What it does

- **Canvas.** Functions, variables, parameters and path inputs are drawn as nodes, connected by what feeds into what. Each step is coloured by whether its results are up to date.
- **Runs.** Run one step or the whole pipeline from the canvas, for Python and MATLAB functions alike, and watch the progress.
- **Plot Studio.** Turn any variable into a publication-ready figure without writing plotting code: group by condition, split into panels, show each participant, and save.
- **Stat Studio.** Answer a few questions about your study, and it recommends a statistical test, runs it, and draws the result on the linked figure.
- **Variants and Inventory.** See every version of a result side by side, and find which combinations of your schema keys have no data.
- **Project bundles.** Export a project that a colleague can import and re-run.

![Plot Studio: mean rating per brand with SD error bars and significance brackets from Stat Studio](https://raw.githubusercontent.com/mtillman14/scistack/main/docs/images/example_best-popcorn/stats_step12.png)

![Stat Studio's "Which test?" screen with its recommendation](https://raw.githubusercontent.com/mtillman14/scistack/main/docs/images/example_best-popcorn/stats_step7.png)

The [Best Popcorn walkthrough](https://scistack.readthedocs.io/en/latest/ui/examples/best-popcorn/) goes from a CSV to a figure with statistics, without writing any code.

## How the extension uses it

The extension starts one backend process per open database:

```bash
python -m scistack_gui.server --db experiment.duckdb
```

It talks to that process with JSON-RPC over stdin and stdout, and forwards the process's log output to the **SciStack** Output Channel. The extension chooses the Python interpreter in this order: the `scistack.pythonPath` setting, then the interpreter selected in the VS Code Python extension, then `python3` on `PATH`.

## Running it in a browser

The same GUI can also run outside VS Code, in your browser:

```bash
scistack-gui experiment.duckdb
```

This serves the GUI on `localhost:8765` and opens a browser tab. Run it with no database to open the project-creation screen instead. Options:

| Option | What it does |
| --- | --- |
| `--port N` | Serve on port `N` instead of 8765 |
| `--schema-keys subject,session` | Create the database with these schema keys if it doesn't exist yet |
| `--module pipeline.py` | Load pipeline functions from a single `.py` file |
| `--project PATH` | Use this `scistack.toml`, or the folder that contains one |
| `--project-root PATH` | Where to write a new `scistack.toml` and entities file when the project has none yet (default: the current folder) |
| `--no-browser` | Don't open a browser tab |

## License

MIT
