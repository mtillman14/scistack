# Export & Import Walkthrough

<!-- GUI variant of sharing/export-import-walkthrough.md. Ground truth:
     scistack-gui ProjectExportButton.tsx (⇩ export project; History / Data /
     Wheelhouse from bundle_cli.export_choices, defaults from ExportOptions),
     extension/src/bundleImport.ts (folder, schema keys, "Which of your keys
     is …", PathInput Choose my folder / Keep the exporter's folder / Decide
     later, History Import / Skip, "Will this project run on the same data as
     the exporter's?" only when asksAboutValueRules, report document, "Trust,
     Install and Open" / "Trust and Open"), VerifyButton.tsx (✓ verify),
     scidb/project/bundle.py (_place_history), design docs/claude/portability.md.
     The example project is docs/examples/best-player.ui.md. -->

This page follows one finished project through three hand-offs. It shows which boxes to tick when you export, what the person importing answers, and what they see when the project opens.

The project is the one you build in [Best Player](../examples/best-player.md): `game_stats_csv` → `read_csv` → `GameStats` on the canvas, the saved figure `Points per game` (with Morgan left out), and the saved table `Scoring statistics`. Work through that first, or follow along with a project of your own. [Export & Import Projects](export-import.md) lists every option; this page shows them in use.

| Hand-off | Who | What they need | Tick |
| --- | --- | --- | --- |
| [1. Open your results](#hand-off-1-the-assistant-coach-opens-your-results) | The assistant coach, on a laptop | The project exactly as you left it, every step done | **History** and **Data** |
| [2. Check they reproduce](#hand-off-2-the-league-statistician-checks-your-results) | The league statistician | Your project and history, to re-run on your CSV and compare | **History** |
| [3. Same analysis, new data](#hand-off-3-next-season-on-new-data) | You, next season | The pipeline, figure and table, without this season's results | **History** (or nothing) |

## Before exporting: dependencies

For an import to install what the project needs, every package your code imports must be listed in the project's `pyproject.toml` (see [Before you export](export-import.md#before-you-export-list-your-dependencies)). Best Player's only step is `pandas.read_csv`, and pandas comes with SciStack, so there's nothing to add.

Let any run finish before exporting, so the export doesn't catch it halfway through.

## Hand-off 1: the assistant coach opens your results

The assistant coach wants to look through the results on their own laptop: the figure, the statistics, the box scores. They don't want to re-run anything.

### What you do

1. Click **⇩ export project** at the right end of the hypothesis tab strip.
2. Tick **History** and **Data**. Leave **Wheelhouse** off: it's only for [libraries](libraries.md).
3. Click **Export…** and save `best_player.scistack`. Send it to the coach.

`data/game_stats.csv` is **not** in the file: raw data never travels. The coach doesn't need it, because they won't re-run `read_csv`.

/// info | 📷 Screenshot
**Show:** the **Export project** popover with **History** and **Data** ticked and **Wheelhouse** unticked.
///

### What the assistant coach does

They need VS Code, the SciStack extension and a virtual environment with SciStack installed ([New to Coding?](../getting-started/new-to-coding.md) steps 4 and 7). Then:

1. Open the Command Palette (**View → Command Palette…**), run **SciStack: Import Project Bundle…** and pick `best_player.scistack`.
2. **Where should the new project go?** Pick a parent folder and accept the folder name. An import always creates a new folder.
3. **Your schema keys.** Keep the suggested `player, game`.
4. **PathInput "game_stats_csv": data/game_stats.csv.** Choose **Decide later**. They won't re-run the import step, so it doesn't matter where the CSV is.
5. **History (with data).** Choose **Import history**.
6. If SciStack asks **Will this project run on the same data as the exporter's?**, choose **Same data**. (It asks only when the project has rules about particular players or games, such as an exclusion.)
7. The import report opens. Under **History** it says *Loaded live, with its data.*
8. Click **Trust, Install and Open**. Nothing needs installing, so the project opens.

The canvas looks exactly like yours, every step green. Click `GameStats`, then **Open Plot Studio →**, and pick `Points per game` from **Saved plots**: the figure is drawn from your data, Morgan still left out. **StatStudio** opens `Scoring statistics` with your results. Nothing has run on their laptop.

## Hand-off 2: the league statistician checks your results

The league wants an independent check before the awards are announced. The statistician will re-run the pipeline from the raw box scores and compare.

### What you do

1. Click **⇩ export project**, tick **History** only, and click **Export…**.
2. Send them `best_player.scistack` **and** `data/game_stats.csv`. The CSV has to be sent separately.

### What the statistician does

1. Put the CSV in a folder of their own, with the same layout: `league-check/data/game_stats.csv`.
2. Run **SciStack: Import Project Bundle…** and pick the file.
3. **Your schema keys.** Keep `player, game`, so the results can be compared.
4. **PathInput "game_stats_csv"**: **Choose my folder…** and pick `league-check/`, the folder the template `data/game_stats.csv` is read from.
5. **History (without data).** Choose **Import history**. Without your data, it can't be loaded into their database (it would say results exist that aren't there), so it's archived, read-only, for checking. If SciStack asks about the same data, choose **Same data**.
6. Click **Trust and Open**.

The canvas looks like yours, but every step is red: nothing has run in this project yet. They click **Run all**, and every step turns green.

Then they click **✓ verify** in the hypothesis tab strip. It runs nothing: it matches each of their results to yours by what produced it (the step, its inputs, its settings, the player and game) and compares them. It should report every step as **reproduced**.

/// info | 📷 Screenshot
**Show:** the **Verify reproduction** popover after the check, with every step reproduced.
///

When something differs, **✓ verify** names the first step where it does, and counts the steps after it as differing *downstream*, so they know where to look. Steps whose code was changed are listed separately.

## Hand-off 3: next season, on new data

A year later, you want the same pipeline, figure and statistics for the new season's box scores. Last season's results shouldn't mix with the new ones.

1. Export with **History** ticked, or nothing ticked. History costs little and keeps a record of last season's run.
2. Put the new season's CSV in a new folder: `best-player-2027/data/game_stats.csv`.
3. Run **SciStack: Import Project Bundle…**:
    - **Your schema keys:** keep `player, game`.
    - **PathInput "game_stats_csv":** **Choose my folder…** and pick `best-player-2027/`.
    - **History:** **Skip history**. The new database holds only the new season's results.
4. Click **Trust and Open**, then **Run all**.

**Check the figure's settings.** Open `Points per game` in Plot Studio and look at **Data > Schema keys**: it still leaves out `Morgan`. That was a decision about last season's roster, and it travels with the figure's design, as does every other setting of a saved figure or table. Change it for this season's players, then step through `Scoring statistics` in Stat Studio once to confirm its answers (who's compared, nothing missing) still hold for the new data.

### If the exporter's project had an exclusion

[Best Player - scidb](../examples/best-player-scidb.md) records Morgan's exclusion in the database, for every analysis, instead of in one figure. An exclusion like that travels in the history. When you import with history, SciStack asks **Will this project run on the same data as the exporter's?**:

- **Same data** applies it, as in Hand-offs 1 and 2.
- **Other data** leaves it out, keeps it in the archive, and the report lists it as a warning. Choose this for next season.

**Skip history** leaves it out too, and the report says so. Nothing is dropped without a warning.

### If the new file names its columns differently

If the new CSV calls the players `athlete`, enter `athlete, game` as your schema keys, and answer **Which of your keys is "player"?** with `athlete`. SciStack renames `player` to `athlete` in the saved figure (group by `athlete`, Morgan left out under `athlete`) and the saved table, and lists both under **Review** in the report. With a changed schema, history is never loaded. [Best Popcorn, Step 14](../examples/best-popcorn.md#step-14-optional-share-the-project-with-a-colleague) walks through this case.

## Which hand-off is yours?

| You want them to… | Tick | They answer |
| --- | --- | --- |
| Open your results without re-running | **History**, **Data** | your schema, **Import history**, **Same data** |
| Re-run your analysis on your data and check it | **History**, and send the raw data | your schema, their folder, **Import history**, then **Run all** and **✓ verify** |
| Run your analysis on **their** data | **History** (or nothing) | their folder, **Skip history** (or **Other data**), then check saved figures and tables for settings about your data |

Whichever you choose, the imported project is a copy: nothing they change reaches your project, and nothing you change later reaches theirs. To share an update, export again.

## Where to go next

- [Export & Import Projects](export-import.md): every option, and what each part of a project does when it travels.
- [Share a Hypothesis or Submodule](pipelines.md): share part of a project instead of all of it.
- [Libraries](libraries.md): share functions that several projects keep using.
