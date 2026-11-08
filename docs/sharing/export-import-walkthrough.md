# Export & Import Walkthrough

<!-- Code variant of sharing/export-import-walkthrough.ui.md. Ground truth:
     scistack-gui/scistack_gui/bundle_cli.py (export flags from EXPORT_FLAGS /
     ExportOptions: history on, data off; import --into/--schema/--map/
     --path-root/--history/--value-rules/--trust/--no-install/--check-code;
     bundle-info; install), scidb/project/bundle.py (_place_history: history +
     data + kept schema + value rules -> live; otherwise archived under
     .scistack/archive/<export time>/, dataset intent live only with the schema
     kept; value_rules=False leaves out exclusions, warned; _intent_not_applied
     warning when history is declined), scidb/inspect/cli.py _cmd_verify
     (--against, exit code 2 on differences), design docs/claude/portability.md.
     The example project is docs/examples/best-player-scidb.md. -->

This page follows one finished project through three hand-offs, from a terminal. It shows which export options to choose for each one, what the person importing types, and what they get.

The project is the one you build in [Best Player - scidb](../examples/best-player-scidb.md): nine players' box scores, Morgan excluded with a recorded reason, and four `BestPlayer` variants from a parameter sweep. Work through that first, or follow along with a project of your own. [Export & Import Projects](export-import.md) lists every option; this page shows them in use.

| Hand-off | Who | What they need | Export with |
| --- | --- | --- | --- |
| [1. Open your results](#hand-off-1-the-assistant-coach-opens-your-results) | The assistant coach, on a laptop | Every result, ready to load. No re-running. | `--data` |
| [2. Check they reproduce](#hand-off-2-the-league-statistician-checks-your-results) | The league statistician | Your code and history, to re-run on your CSV and compare | the defaults |
| [3. Same analysis, new data](#hand-off-3-next-season-on-new-data) | You, next season | The pipeline, without this season's results or decisions | the defaults |

## The project you start from

After Best Player - scidb, the folder looks like this:

```text
best-player/
├── game_stats.csv
├── player_variables.py     # GameStats, SeasonStats, BestPlayer
├── best_player.py          # the analysis
└── best_player.duckdb      # every result, its history, and Morgan's exclusion
```

The database holds more than results. Morgan's exclusion lives there too, with its reason, not in your code: `best_player.py` only adds it when the database has none yet (`if scidb.list_exclusions().empty`). That matters below, because what's in the database travels differently from what's in the code.

### Before exporting: dependencies

For an import to install what the project needs, the packages must be listed in a `pyproject.toml` (see [List your dependencies](export-import.md#list-your-dependencies)). This project imports only `pandas`, `scidb`, `scistackplot`, `scistackstats` and `scistackstatsdb`, which all come with SciStack, so it needs none.

In a project of your own that imports, say, SciPy, run `scistack init` in the project folder once and add it:

```toml
[project]
dependencies = ["scidb", "scipy"]
```

## Hand-off 1: the assistant coach opens your results

The assistant coach wants to look at the award results on their own laptop: load `BestPlayer`, read `SeasonStats`, maybe plot something. They don't want to re-run anything.

### What you do

Close anything that's still running on the database, then, in `best-player/` with your environment active:

```bash
scistack export --data
```

```text
exported /home/you/best-player/best_player.duckdb (single *.duckdb in cwd)
      to /home/you/best-player/best_player.scistack (… bytes)
 options include_history=True, include_data=True, include_wheelhouse=False
```

`--data` adds every saved result. History is on by default, and data can't travel without it, because a result whose history is missing couldn't say where it came from. `game_stats.csv` is **not** in the file: raw data never travels. The coach doesn't need it, because they won't re-run the import step.

Send them `best_player.scistack`.

### What the assistant coach does

They create and activate a new environment and install SciStack into it ([New to Coding?](../getting-started/new-to-coding.md) steps 4 and 7). Then they look inside the file, which runs nothing:

```bash
scistack bundle-info best_player.scistack
```

It lists the package, the schema (`player, game`), which sections are inside (`code`, `config`, `env`, `history`, `data`…), and what would need installing.

Then they import it into a **new** folder:

```bash
scistack import best_player.scistack --into ~/best-player --trust
```

- `--into` names the new folder. An import never writes into an existing project.
- `--trust` says they trust your code. It installs whatever the project lists that they're missing, at your versions. Here nothing is listed, so nothing is installed.
- History is imported by default. Because the bundle carries the data too and they kept your schema, both go into their new `best_player.duckdb` exactly as you saved them, Morgan's exclusion included.

The report ends with `Open …/best_player.duckdb in the SciStack GUI to continue.` They can, or they can stay in Python. In `~/best-player`:

```python
import scidb
from player_variables import BestPlayer

scidb.configure_database("best_player.duckdb", ["player", "game"])
best = BestPlayer.load(metric="points_per_game", min_games=10).data
print(best)   # Avery, 21.5 points per game, 6 eligible
print(scidb.list_exclusions())   # Morgan, with your reason
```

Nothing ran: those are your results, loaded from the imported database. `scidb.get_provenance` traces them back to `game_stats.csv` as on your computer.

## Hand-off 2: the league statistician checks your results

The league wants an independent check before the awards are announced. The statistician will re-run your analysis from the raw box scores on their own computer and compare.

### What you do

```bash
scistack export
```

The defaults: code, settings and history, no data. Send them `best_player.scistack` **and** `game_stats.csv`. The CSV has to be sent separately, because raw data never travels in the bundle.

### What the statistician does

In a fresh environment with SciStack installed:

```bash
scistack import best_player.scistack --into ~/best-player-check --trust
cp ~/Downloads/game_stats.csv ~/best-player-check/
cd ~/best-player-check
```

This time the bundle has history but no data, so the history is **archived**, read-only, under `.scistack/archive/<export time>/`, not loaded into the new database. The database starts empty, because history that says "this result exists" without the result would be wrong. Two things from the history do go live, because they govern a re-run: the schema key types, and Morgan's exclusion.

Then they run your script unchanged:

/// tab | Python
```bash
python best_player.py
```
///

/// tab | MATLAB
```matlab
best_player
```
///

The exclusion is already in the new database, so the script's `if scidb.list_exclusions().empty` guard skips adding it again, and every step leaves Morgan out exactly as yours did.

Then they compare:

```bash
scistack verify
```

`verify` runs no code. It matches each of their results to yours by what produced it (the function, its inputs, the settings, the location) and compares the two:

```text
verified against /home/them/best-player-check/.scistack/archive/…
  exact comparison (the exporter's data is not available: export WITH data and verify --against the bundle for a tolerance)
  reproduced           …
OK: everything reproduced
```

Without your data in the bundle, the comparison is exact. If you export with `--data` instead, they can run `scistack verify --against best_player.scistack` to compare the values with a tolerance (`--rtol`, `--atol`), which is the better choice when results come from floating-point arithmetic on different computers.

When something differs, `verify` names the first step where it does (`first divergence: season_totals -> SeasonStats at player=Avery`), and marks the steps after it as differing *downstream*, so they know where to look. It exits with code 2, so it can be used in a script.

## Hand-off 3: next season, on new data

A year later, you want the same analysis on the new season's box scores. Morgan isn't on the team any more, and last season's results shouldn't mix with the new ones. Import your own bundle into a new folder:

```bash
scistack import best_player.scistack --into ~/best-player-2027 --no-history
cp ~/new-season/game_stats.csv ~/best-player-2027/
```

`--no-history` leaves out last season's history, and with it Morgan's exclusion. The import says so instead of dropping it silently:

```text
warning: dataset rules NOT applied: 1 schema exclusion(s), because the run history was declined at import. To keep them, …
```

That's what you want here: Morgan's exclusion was a decision about last season's roster.

To keep the history for reference but still leave out the rules that name last season's players, import with `--no-value-rules` instead. The history is archived as in Hand-off 2, the schema key types still apply, and the exclusion stays in the archive, unapplied, with a warning that says so.

**Check the code, too.** The import left the exclusion out of the database, but `best_player.py` still contains:

```python
if scidb.list_exclusions().empty:
    scidb.exclude_schema(player="Morgan", reason="Transferred to another team after game 4")
```

A decision written in code travels with the code, and an import can't tell that it's about one season's data. Delete those lines, or replace them with this season's exclusions, before running the script.

### A different schema

If next season's file names its columns differently, say `athlete` instead of `player`, give the import your schema and say which key is which:

```bash
scistack import best_player.scistack --into ~/best-player-2027 \
    --schema athlete game --map player=athlete
```

`--map` renames the key in everything SciStack stores, such as saved plots and saved tables. It can't rename it in your code, and `best_player.py` names `player` (in `exclude_schema`, and as the column `pick_best` reads). In a code project, renaming the CSV's column header to `player` is usually simpler. With a changed schema, history and data are never loaded, because they describe a dataset of a different shape. See [Best Popcorn - scidb, Step 10](../examples/best-popcorn-scidb.md#step-10-optional-share-the-project-with-a-colleague) for an example.

## Which hand-off is yours?

| You want them to… | Export | Import |
| --- | --- | --- |
| Open your results without re-running | `scistack export --data` | `scistack import … --trust` |
| Re-run your analysis on your data and check it | `scistack export`, and send the raw data | `scistack import … --trust`, run, `scistack verify` |
| Run your analysis on **their** data | `scistack export` (or `--no-history`) | `scistack import … --no-history` (or `--no-value-rules`), then check the code for decisions about your data |

Whichever you choose, the imported project is a copy: nothing they change reaches your project, and nothing you change later reaches theirs. To share an update, export again.

## Where to go next

- [Export & Import Projects](export-import.md): every option, and what each part of a project does when it travels.
- [Libraries](libraries.md): share functions that several projects keep using, instead of copying a whole project.
