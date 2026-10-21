# Best Player - scidb

A youth basketball team played 20 games this season, and the coach recorded a box score line for every player in every game: minutes played, shots made, shots attempted, and points. The coach wants to hand out two awards, **Best Shooter** and **Best Scorer**. Who should get them?

The answer depends on choices, such as how many games a player must have played to qualify. A good analysis tries several of these choices and shows how the answer changes. This walkthrough does that with **scidb**, and along the way shows what a database gives you beyond saving results:

- **Asking the database a question** in the middle of an analysis ("who missed games?").
- **Excluding** data with a recorded reason, without deleting it.
- **Parameter sweeps**: running one step under several settings at once.
- **Variant tracking**: every result remembers the settings that produced it, so you can load exactly the answer you want and list every answer you've computed.
- **Statistics on the stored records**: an exclusion applies to the tests too.

It assumes you've worked through [Best Popcorn - scidb](best-popcorn-scidb.md), which introduces variables, `configure_database`, and saving results with `for_each`. [Best Player - scifor](best-player-scifor.md) is the same analysis without a database, for comparison.

## The data

123 rows: one per player per game played. Players who sat out a game have no row for it.

| player | game | minutes | shots_made | shots_attempted | points |
|--------|------|---------|------------|-----------------|--------|
| Avery  | G01  | 32      | 9          | 18              | 23     |
| Blake  | G01  | 28      | 5          | 7               | 10     |
| Casey  | G01  | 34      | 8          | 13              | 17     |
| …      | …    | …       | …          | …               | …      |

**Download:** [game_stats.csv](data/game_stats.csv){ download="game_stats.csv" }

If the download doesn't work for you, expand the box below, copy the contents, and save them as `game_stats.csv`.

/// details | game_stats.csv (click to expand)
    type: example

```text
player,game,minutes,shots_made,shots_attempted,points
Avery,G01,32,9,18,23
Blake,G01,28,5,7,10
Casey,G01,34,8,13,17
Devon,G01,22,2,7,4
Emerson,G01,25,6,15,14
Finley,G01,7,3,6,8
Jordan,G01,34,11,20,27
Morgan,G01,30,6,11,13
Avery,G02,36,6,16,15
Blake,G02,31,7,13,14
Casey,G02,35,8,18,20
Devon,G02,24,2,5,6
Emerson,G02,29,6,8,13
Finley,G02,23,3,6,6
Jordan,G02,31,10,19,25
Morgan,G02,35,10,19,22
Avery,G03,31,6,19,16
Blake,G03,28,5,12,10
Casey,G03,31,8,17,20
Devon,G03,28,3,10,6
Emerson,G03,22,4,9,9
Finley,G03,12,1,2,3
Morgan,G03,33,7,14,22
Avery,G04,36,9,19,22
Blake,G04,28,5,9,14
Casey,G04,35,3,9,9
Devon,G04,29,3,5,7
Emerson,G04,19,4,10,12
Finley,G04,20,7,11,14
Morgan,G04,31,8,17,19
Avery,G05,25,5,19,13
Blake,G05,29,8,16,18
Casey,G05,27,6,13,14
Devon,G05,19,0,3,2
Emerson,G05,14,4,7,9
Finley,G05,9,6,14,12
Avery,G06,33,13,20,30
Blake,G06,28,8,12,18
Casey,G06,26,1,8,7
Devon,G06,26,2,10,5
Emerson,G06,24,4,9,9
Finley,G06,18,4,7,9
Avery,G07,27,9,20,23
Blake,G07,28,5,12,10
Devon,G07,22,5,10,10
Emerson,G07,18,8,12,17
Finley,G07,13,1,9,2
Avery,G08,35,13,22,30
Blake,G08,17,5,8,13
Devon,G08,19,4,8,8
Emerson,G08,31,5,12,10
Finley,G08,17,1,3,2
Avery,G09,28,6,17,17
Blake,G09,28,9,12,19
Casey,G09,30,2,14,5
Devon,G09,23,4,8,12
Emerson,G09,18,3,4,9
Finley,G09,24,2,4,4
Avery,G10,31,11,20,26
Blake,G10,29,5,10,12
Casey,G10,34,2,13,5
Devon,G10,22,0,1,0
Emerson,G10,13,5,11,10
Finley,G10,7,1,4,4
Avery,G11,34,6,15,15
Blake,G11,25,5,8,11
Casey,G11,26,7,11,17
Devon,G11,25,2,5,6
Finley,G11,15,0,1,1
Avery,G12,37,9,18,23
Blake,G12,23,2,6,5
Casey,G12,31,8,15,17
Devon,G12,27,6,13,15
Finley,G12,15,2,5,6
Avery,G13,32,13,21,32
Blake,G13,23,4,8,10
Casey,G13,36,7,12,17
Devon,G13,23,2,7,6
Finley,G13,14,2,8,6
Avery,G14,30,7,17,18
Blake,G14,26,3,8,8
Casey,G14,34,4,13,12
Devon,G14,29,7,10,15
Finley,G14,15,3,9,8
Avery,G15,30,12,22,27
Blake,G15,31,6,11,16
Casey,G15,21,8,15,17
Devon,G15,28,6,11,14
Emerson,G15,18,5,11,13
Finley,G15,16,1,3,4
Avery,G16,31,7,18,17
Blake,G16,34,7,9,16
Casey,G16,35,5,11,15
Devon,G16,17,5,12,13
Emerson,G16,20,2,7,4
Finley,G16,15,1,6,4
Avery,G17,32,11,23,26
Blake,G17,31,4,12,11
Casey,G17,29,7,11,22
Devon,G17,21,1,5,4
Emerson,G17,27,6,9,14
Finley,G17,16,3,5,9
Avery,G18,34,7,19,18
Blake,G18,31,5,6,12
Casey,G18,37,10,20,23
Devon,G18,32,5,7,10
Emerson,G18,18,2,6,7
Finley,G18,24,1,2,2
Harper,G18,6,2,2,4
Avery,G19,28,8,21,19
Blake,G19,24,2,7,8
Casey,G19,29,10,18,20
Devon,G19,22,2,7,4
Emerson,G19,30,4,6,10
Finley,G19,22,2,5,4
Harper,G19,4,1,1,2
Avery,G20,31,8,17,20
Blake,G20,31,4,9,13
Casey,G20,28,2,7,10
Devon,G20,21,2,10,7
Emerson,G20,26,7,10,17
Finley,G20,10,2,4,6
Harper,G20,7,2,2,5
```
///

/// note | The data is made up
The team, the players, and their stats were generated for this tutorial.
///

## Step 1: Set up a folder

/// tab | Python
```text
best-player/
├── game_stats.csv
├── player_variables.py     # the variable definitions (Step 2)
└── best_player.py          # the analysis
```
///

/// tab | MATLAB
```text
best-player/
├── game_stats.csv
├── GameStats.m  SeasonStats.m  BestPlayer.m          % variables (Step 2)
├── load_game_stats.m  season_totals.m  pick_best.m   % one function per file
└── best_player.m                                    % the analysis script
```
///

## Step 2: Configure the database and declare variables

Each row is identified by **who** played and **which game**, so the schema is `["player", "game"]`. The analysis needs three variables: the raw box scores, each player's season totals, and the award winner.

/// tab | Python
```python
# player_variables.py
from scidb import BaseVariable


class GameStats(BaseVariable):
    """One player's box score line for one game."""


class SeasonStats(BaseVariable):
    """One player's season totals."""


class BestPlayer(BaseVariable):
    """Who wins an award, under one definition of 'best'."""
```

```python
# best_player.py
import pandas as pd
import scidb
from scidb import EachOf
from scidb.inventory import schema_inventory, write_inventory_csv

from player_variables import GameStats, SeasonStats, BestPlayer

db = scidb.configure_database("best_player.duckdb", ["player", "game"])
```
///

/// tab | MATLAB
```matlab
% GameStats.m
classdef GameStats < scidb.BaseVariable
    % One player's box score line for one game.
end
```

```matlab
% SeasonStats.m
classdef SeasonStats < scidb.BaseVariable
    % One player's season totals.
end
```

```matlab
% BestPlayer.m
classdef BestPlayer < scidb.BaseVariable
    % Who wins an award, under one definition of 'best'.
end
```

```matlab
% best_player.m
scidb.configure_database("best_player.duckdb", ["player", "game"]);
```
///

## Step 3: Import the box scores

As in Best Popcorn, a loader function reads the whole CSV in a single `for_each` run with no keys named. Each row has its own `player` and `game`, so each one is saved as a separate `GameStats` record at that location: 123 records.

/// tab | Python
```python
def load_game_stats(csv_path):
    """Read every box score line: one row per (player, game)."""
    return pd.read_csv(csv_path)


scidb.for_each(load_game_stats, inputs={"csv_path": "game_stats.csv"}, outputs=[GameStats])
```
///

/// tab | MATLAB
```matlab
% load_game_stats.m
function stats = load_game_stats(csv_path)
    % Read every box score line: one row per (player, game).
    stats = readtable(csv_path, TextType="string");
end
```

```matlab
scidb.for_each(@load_game_stats, struct("csv_path", "game_stats.csv"), {GameStats()});
```
///

## Step 4: Ask the database who missed games

Before computing anything, it's worth knowing how complete the data is. 9 players × 20 games could be 180 box score lines, but there are only 123. Who's missing, and how many games did each player miss?

You don't need to work this out yourself. scidb can produce an **inventory**: every combination of the schema keys you name, each marked present, missing, or excluded.

/// tab | Python
```python
inv = schema_inventory(db, ["player", "game"])
print(inv.counts)                       # how many combinations are present / missing / excluded

write_inventory_csv(inv, "missing_games.csv", missing_only=True)
missing = pd.read_csv("missing_games.csv")
print(missing.groupby("player").size())  # games missed, per player
```
///

/// tab | MATLAB
The inventory is a Python function, so MATLAB calls it through `py.`:

```matlab
inv = py.scidb.inventory.schema_inventory(py.scidb.get_database(), py.list({'player', 'game'}));
disp(inv.counts)                        % how many combinations are present / missing / excluded

py.scidb.inventory.write_inventory_csv(inv, "missing_games.csv", true);
missing = readtable("missing_games.csv", TextType="string");
disp(groupcounts(missing, "player"))     % games missed, per player
```
///

The counts show 123 present and 57 missing. The per-player breakdown:

| player  | games missed |
|---------|--------------|
| Casey   | 2            |
| Emerson | 4            |
| Harper  | 17           |
| Jordan  | 18           |
| Morgan  | 16           |

The inventory turns this into specific questions to put to the coach, who explains:

- **Casey and Emerson** were sick for a few games. Nothing to do.
- **Harper** joined the team for the last three games.
- **Jordan** was injured after the second game.
- **Morgan** transferred to another team after game 4, and the awards are for the current roster.

Harper and Jordan matter for Step 7. Morgan needs handling now.

/// tip | The same question from a terminal
The `scidb` command-line tool answers it too: open a terminal in the project folder and run `scidb inventory --keys player,game`. Run it while no Python or MATLAB session has the database open, because the database file can only be opened by one program at a time.
///

## Step 5: Exclude Morgan (without deleting anything)

In the scifor version, Morgan's rows were dropped from the table with a line of code. Here, the decision is **recorded in the database** instead:

/// tab | Python
```python
if scidb.list_exclusions().empty:   # exclusions are stored in the database: add once
    scidb.exclude_schema(player="Morgan", reason="Transferred to another team after game 4")
print(scidb.list_exclusions())
```
///

/// tab | MATLAB
```matlab
if height(scidb.list_exclusions()) == 0   % exclusions are stored in the database: add once
    scidb.exclude_schema("Transferred to another team after game 4", player="Morgan");
end
disp(scidb.list_exclusions())
```
///

What this does, and what it doesn't:

- **Every later `for_each` skips Morgan.** You don't need a filter in each step. The exclusion applies everywhere, including to steps you write next month.
- **Nothing is deleted.** Morgan's 4 box score lines are still in the database, and the inventory now lists them as *excluded* instead of present.
- **The reason is saved** next to the exclusion, so anyone who opens this database later can see why Morgan isn't in the results.
- **It can be undone.** `scidb.include_schema(player="Morgan", reason=...)` brings Morgan back, and that decision is recorded too.

The `if` guard is there because the exclusion is stored in the database, not in the script. When you run the script a second time it already exists, and `exclude_schema` refuses to add a duplicate.

## Step 6: Season totals for each player

Turn each player's box score lines into one season line. `player=[]` means every player in the database, minus excluded ones:

/// tab | Python
```python
def season_totals(games):
    """All of one player's box score lines in, one season line out."""
    made = games["shots_made"].sum()
    attempted = games["shots_attempted"].sum()
    return pd.DataFrame({
        "games":           [len(games)],
        "minutes":         [games["minutes"].sum()],
        "fg_pct":          [made / attempted],         # field goal (shooting) percentage
        "points_per_game": [games["points"].mean()],
    })


season = scidb.for_each(season_totals, inputs={"games": GameStats},
                        outputs=[SeasonStats], player=[])
print(season[["player", "games", "minutes", "fg_pct", "points_per_game"]].round(3))
```
///

/// tab | MATLAB
```matlab
% season_totals.m
function out = season_totals(games)
    % All of one player's box score lines in, one season line out.
    made = sum(games.shots_made);
    attempted = sum(games.shots_attempted);
    out = table(height(games), sum(games.minutes), made / attempted, mean(games.points), ...
        VariableNames=["games", "minutes", "fg_pct", "points_per_game"]);
end
```

```matlab
season = scidb.for_each(@season_totals, struct("games", GameStats()), ...
    {SeasonStats()}, player=[]);
disp(season(:, ["player", "games", "minutes", "fg_pct", "points_per_game"]))
```
///

`GameStats` has four measurement columns, so each call receives one player's games as a **table** (without the `player` and `game` columns), and the function looks columns up by name. Each result is saved as a `SeasonStats` at the player's location. There are 8 rows, not 9: Morgan was skipped.

| player  | games | minutes | fg_pct | points_per_game |
|---------|-------|---------|--------|-----------------|
| Avery   | 20    | 633     | 0.459  | 21.50           |
| Blake   | 20    | 553     | 0.533  | 12.40           |
| Casey   | 18    | 558     | 0.445  | 14.83           |
| Devon   | 20    | 479     | 0.409  | 7.70            |
| Emerson | 16    | 352     | 0.514  | 11.06           |
| Finley  | 20    | 312     | 0.404  | 5.70            |
| Harper  | 3     | 17      | 1.000  | 3.67            |
| Jordan  | 2     | 65      | 0.538  | 26.00           |

As Step 4 predicted, Harper (5 for 5 shooting in 3 games) and Jordan (26 points per game, but only 2 games) have very small samples.

## Step 7: A parameter sweep — four definitions of "best"

Choosing a winner takes two decisions:

- **Which statistic?** `fg_pct` for Best Shooter, `points_per_game` for Best Scorer.
- **How many games to qualify?** `min_games` of 1 (everyone counts) or 10 (half the season).

**`EachOf`** runs every combination of the settings, 2 × 2 = 4 runs, in a single call:

/// tab | Python
```python
def pick_best(season, metric, min_games):
    """The top player by `metric`, among players with at least `min_games` games."""
    eligible = season[season["games"] >= min_games]
    top = eligible.sort_values(metric, ascending=False).iloc[0]
    return pd.DataFrame({
        "best_player": [top["player"]],
        "value":       [top[metric]],
        "n_eligible":  [len(eligible)],
    })


scidb.for_each(
    pick_best,
    inputs={
        "season":    SeasonStats,
        "metric":    EachOf("fg_pct", "points_per_game"),
        "min_games": EachOf(1, 10),
    },
    outputs=[BestPlayer],
    as_table=True,   # keep the player column; pick_best needs it
)
```
///

/// tab | MATLAB
```matlab
% pick_best.m
function out = pick_best(season, metric, min_games)
    % The top player by METRIC, among players with at least MIN_GAMES games.
    eligible = season(season.games >= min_games, :);
    eligible = sortrows(eligible, metric, "descend");
    out = table(eligible.player(1), eligible.(metric)(1), height(eligible), ...
        VariableNames=["best_player", "value", "n_eligible"]);
end
```

```matlab
scidb.for_each(@pick_best, ...
    struct("season",    SeasonStats(), ...
           "metric",    scifor.EachOf("fg_pct", "points_per_game"), ...
           "min_games", scifor.EachOf(1, 10)), ...
    {BestPlayer()}, ...
    as_table=true);   % keep the player column; pick_best needs it
```
///

No schema keys are named, so each of the four runs gets **every** player's `SeasonStats` and saves one `BestPlayer` at the dataset level.

Compare `pick_best` with the scifor version. It **doesn't return its own `metric` and `min_games`**. scidb saves the settings with each result automatically, so the database now holds four `BestPlayer` records, each tagged with the settings that produced it. These are four **variants** of the same variable.

/// note | The winner isn't returned as `player`
The output column is named `best_player`, not `player`. A returned column named after a schema key is read as a location, so scidb would file the record under that player instead of at the dataset level.
///

## Step 8: Load the answer you want

With four variants in the database, a plain load is ambiguous:

/// tab | Python
```python
BestPlayer.load()   # raises AmbiguousVersionError: there are 4 variants here
```
///

/// tab | MATLAB
```matlab
BestPlayer().load()   % errors: there are 4 variants here
```
///

scidb raises an error instead of picking one, because quietly returning the wrong definition of "best" would be worse. To pick a variant, pass the settings you want, just like schema keys:

/// tab | Python
```python
for metric in ["fg_pct", "points_per_game"]:
    for min_games in [1, 10]:
        best = BestPlayer.load(metric=metric, min_games=min_games).data
        print(f"{metric:>15}, min_games={min_games:>2}: "
              f"{best['best_player'].iloc[0]} ({best['value'].iloc[0]:.3g}), "
              f"{best['n_eligible'].iloc[0]} eligible")
```
///

/// tab | MATLAB
```matlab
for metric = ["fg_pct", "points_per_game"]
    for min_games = [1, 10]
        best = BestPlayer().load(metric=metric, min_games=min_games).data;
        fprintf("%15s, min_games=%2d: %s (%.3g), %d eligible\n", ...
            metric, min_games, best.best_player, best.value, best.n_eligible);
    end
end
```
///

```text
         fg_pct, min_games= 1: Harper (1), 8 eligible
         fg_pct, min_games=10: Blake (0.533), 6 eligible
points_per_game, min_games= 1: Jordan (26), 8 eligible
points_per_game, min_games=10: Avery (21.5), 6 eligible
```

The definition decides the winner. If everyone counts, Harper and Jordan win on a handful of games. If a player needs 10 games, the awards go to **Blake (Best Shooter)** and **Avery (Best Scorer)**, who earned them over a full season. That's the coach's call, and all four answers are saved, so the coach can see what the choice changes.

## Step 9: What's in the database?

Weeks later, someone asks how you decided the awards. You don't have to rerun anything or remember which settings you tried. Ask the database which `BestPlayer` variants exist:

/// tab | Python
```python
for v in db.list_versions(BestPlayer):
    print(v["branch_params"])
```
///

/// tab | MATLAB
```matlab
versions = BestPlayer().list_versions();
for i = 1:numel(versions)
    disp(versions(i).branch_params)
end
```
///

Each variant lists **every setting that went into it**, including ones from earlier steps. You'll see four entries along these lines:

```text
{'load_game_stats.csv_path': 'game_stats.csv', 'pick_best.metric': 'fg_pct', 'pick_best.min_games': 1}
{'load_game_stats.csv_path': 'game_stats.csv', 'pick_best.metric': 'fg_pct', 'pick_best.min_games': 10}
...
```

Each setting is prefixed with the function it was passed to. `csv_path` was given to `load_game_stats` in Step 3, two steps before `pick_best`, and scidb carried it forward. A variant's settings describe how the whole result was produced, not just its last step. (When you load, the short name such as `min_games=10` is enough, as long as only one function has a setting by that name.)

## Step 10: Is Jordan's scoring more than two good games?

`min_games=10` is a rule of thumb. Statistics can back it with evidence: how sure can you be of each player's scoring average, and is the gap between two players bigger than game-to-game luck? `scistackstats`, which comes with SciStack and runs [Stat Studio](https://scistack.readthedocs.io/en/latest/ui/gui/summarize/) in the GUI, answers both questions. ([Best Player - scifor](best-player-scifor.md) explains this step in more detail.)

You describe the analysis the way you would describe a bar chart of points per game: **group** by `player`, and **collapse** `game` so that each bar averages that player's games. `GameStats` has four columns, so `DV(..., columns=["points"])` picks the one to analyse. `load_stats_tables` reads the records from the database:

/// tab | Python
```python
from scistackplot import PlotKind, PlotSpec, Role
from scistackstats import DV, Analysis, DescriptiveSpec, StatsSpec, describe_table, run
from scistackstats import export_comparisons, export_descriptives
from scistackstatsdb import load_stats_tables

design = PlotSpec(
    measures=["GameStats"],
    roles={"player": Role.GROUP, "game": Role.COLLAPSE},
    groups=["player"],
    kind=PlotKind.BAR,
)
spec = StatsSpec(
    design=design,
    dvs=[DV("GameStats", columns=["points"])],      # one column of a multi-column variable
    descriptives=DescriptiveSpec(cell="mean_ci95"),  # mean [95% CI] per player
    comparisons=[Analysis()],                        # tests chosen from the data
)

tables = load_stats_tables(db, spec)
print(export_descriptives(describe_table(spec, tables), spec, "md", tables=tables))
report = run(spec, tables)
print(export_comparisons(report, spec, "md", tables=tables))
```
///

/// tab | MATLAB
`scistackstats` is a Python package, and there's no MATLAB version of it yet. The database is the same file whichever language wrote it, so run the Python code on `best_player.duckdb` after the MATLAB steps.
///

**Morgan isn't in the tables**, and there's no filter in this code. The exclusion from Step 5 applies to the statistics as well, just as it did to every `for_each`. That's one decision, recorded once, applied everywhere.

| | Avery | Blake | Casey | Devon | Emerson | Finley | Harper | Jordan |
|---|---|---|---|---|---|---|---|---|
| Mean [95% CI] | 21.5 [18.9, 24.1] | 12.4 [10.7, 14.1] | 14.8 [12.0, 17.6] | 7.7 [5.7, 9.7] | 11.1 [9.2, 12.9] | 5.7 [4.1, 7.3] | 3.7 [−0.1, 7.5] | 26.0 [13.3, 38.7] |
| n | 20 | 20 | 18 | 20 | 16 | 20 | 3 | 2 |

Over 20 games, Avery's average is known to within about ±2.6 points. Jordan's two games leave anything from 13 to 39 plausible.

The 28 pairs of players are compared with Welch's t-tests, and the p-values are Holm-adjusted for running 28 tests. The rows that decide Best Scorer:

| Comparison | Δ [95% CI] | Statistic | p | p (adj.) | n |
|---|---|---|---|---|---|
| Avery − Jordan | −4.5 [−8.4, −0.6] | t(5.8) = −2.81 | .032 | .159 | 22 |
| Avery − Casey | 6.7 [3.0, 10.4] | t(35.5) = 3.65 | < .001 | .011 | 38 |
| Avery − Blake | 9.1 [6.1, 12.1] | t(32.9) = 6.08 | < .001 | < .001 | 40 |

Avery outscored everyone who played most of the season. Jordan's 4.5-point lead isn't convincing once the 28 comparisons are accounted for (adjusted p = .159), and the table's notes flag every comparison with Jordan as resting on *n=2*. The statistics agree with the `min_games=10` variant: **Avery is the best scorer**.

These tables are printed, not saved. To make the statistics a recorded step, open `GameStats` in Stat Studio with the same design and click **Add to pipeline**. That creates a `stat_` step whose results are saved and tracked.

## Summary

| Capability | What it did here |
|---|---|
| **Inventory** (`schema_inventory`, `scidb inventory`) | Found who missed games, which turned up the small samples and the transfer |
| **Exclusions** (`exclude_schema`) | Removed Morgan from every analysis, with a reason, without deleting data |
| **Parameter sweep** (`EachOf`) | Ran four definitions of "best" in one call |
| **Variant tracking** | Saved each result with its settings, without the function labelling anything |
| **Loading a variant** (`load(metric=..., min_games=...)`) | Picked the exact answer wanted, and refused to guess between variants |
| **Listing variants** (`list_versions`) | Listed every answer computed, with its full set of settings |
| **Statistics** (`scistackstats`, `load_stats_tables`) | Confidence intervals and tests on `GameStats[points]`, with Morgan still excluded |

## The complete code

//// details | Full code (click to expand)
    type: example

/// tab | Python
```python
# player_variables.py
from scidb import BaseVariable


class GameStats(BaseVariable):
    """One player's box score line for one game."""


class SeasonStats(BaseVariable):
    """One player's season totals."""


class BestPlayer(BaseVariable):
    """Who wins an award, under one definition of 'best'."""
```

```python
# best_player.py
import pandas as pd
import scidb
from scidb import EachOf
from scistackplot import PlotKind, PlotSpec, Role
from scistackstats import DV, Analysis, DescriptiveSpec, StatsSpec, describe_table, run
from scistackstats import export_comparisons, export_descriptives
from scistackstatsdb import load_stats_tables
from scidb.inventory import schema_inventory, write_inventory_csv

from player_variables import GameStats, SeasonStats, BestPlayer


def load_game_stats(csv_path):
    """Read every box score line: one row per (player, game)."""
    return pd.read_csv(csv_path)


def season_totals(games):
    """All of one player's box score lines in, one season line out."""
    made = games["shots_made"].sum()
    attempted = games["shots_attempted"].sum()
    return pd.DataFrame({
        "games":           [len(games)],
        "minutes":         [games["minutes"].sum()],
        "fg_pct":          [made / attempted],
        "points_per_game": [games["points"].mean()],
    })


def pick_best(season, metric, min_games):
    """The top player by `metric`, among players with at least `min_games` games."""
    eligible = season[season["games"] >= min_games]
    top = eligible.sort_values(metric, ascending=False).iloc[0]
    return pd.DataFrame({
        "best_player": [top["player"]],
        "value":       [top[metric]],
        "n_eligible":  [len(eligible)],
    })


db = scidb.configure_database("best_player.duckdb", ["player", "game"])

# Import: one GameStats record per (player, game) row
scidb.for_each(load_game_stats, inputs={"csv_path": "game_stats.csv"}, outputs=[GameStats])

# Who missed games?
inv = schema_inventory(db, ["player", "game"])
print(inv.counts)
write_inventory_csv(inv, "missing_games.csv", missing_only=True)
print(pd.read_csv("missing_games.csv").groupby("player").size())

# Morgan transferred: exclude (recorded once, applies to every later step)
if scidb.list_exclusions().empty:
    scidb.exclude_schema(player="Morgan", reason="Transferred to another team after game 4")

# Season totals per player
season = scidb.for_each(season_totals, inputs={"games": GameStats},
                        outputs=[SeasonStats], player=[])
print(season[["player", "games", "minutes", "fg_pct", "points_per_game"]].round(3))

# Parameter sweep: 2 metrics x 2 eligibility rules = 4 variants of BestPlayer
scidb.for_each(
    pick_best,
    inputs={
        "season":    SeasonStats,
        "metric":    EachOf("fg_pct", "points_per_game"),
        "min_games": EachOf(1, 10),
    },
    outputs=[BestPlayer],
    as_table=True,
)

# Load each variant by its settings
for metric in ["fg_pct", "points_per_game"]:
    for min_games in [1, 10]:
        best = BestPlayer.load(metric=metric, min_games=min_games).data
        print(f"{metric:>15}, min_games={min_games:>2}: "
              f"{best['best_player'].iloc[0]} ({best['value'].iloc[0]:.3g}), "
              f"{best['n_eligible'].iloc[0]} eligible")

# Every variant, with its full set of settings
for v in db.list_versions(BestPlayer):
    print(v["branch_params"])

# Is Jordan's scoring more than two good games? (scistackstats; Morgan stays excluded)
design = PlotSpec(
    measures=["GameStats"],
    roles={"player": Role.GROUP, "game": Role.COLLAPSE},
    groups=["player"],
    kind=PlotKind.BAR,
)
spec = StatsSpec(design=design, dvs=[DV("GameStats", columns=["points"])],
                 descriptives=DescriptiveSpec(cell="mean_ci95"), comparisons=[Analysis()])
tables = load_stats_tables(db, spec)
print(export_descriptives(describe_table(spec, tables), spec, "md", tables=tables))
report = run(spec, tables)
print(export_comparisons(report, spec, "md", tables=tables))
```
///

/// tab | MATLAB
```matlab
% GameStats.m
classdef GameStats < scidb.BaseVariable
    % One player's box score line for one game.
end
```

```matlab
% SeasonStats.m
classdef SeasonStats < scidb.BaseVariable
    % One player's season totals.
end
```

```matlab
% BestPlayer.m
classdef BestPlayer < scidb.BaseVariable
    % Who wins an award, under one definition of 'best'.
end
```

```matlab
% load_game_stats.m
function stats = load_game_stats(csv_path)
    % Read every box score line: one row per (player, game).
    stats = readtable(csv_path, TextType="string");
end
```

```matlab
% season_totals.m
function out = season_totals(games)
    % All of one player's box score lines in, one season line out.
    made = sum(games.shots_made);
    attempted = sum(games.shots_attempted);
    out = table(height(games), sum(games.minutes), made / attempted, mean(games.points), ...
        VariableNames=["games", "minutes", "fg_pct", "points_per_game"]);
end
```

```matlab
% pick_best.m
function out = pick_best(season, metric, min_games)
    % The top player by METRIC, among players with at least MIN_GAMES games.
    eligible = season(season.games >= min_games, :);
    eligible = sortrows(eligible, metric, "descend");
    out = table(eligible.player(1), eligible.(metric)(1), height(eligible), ...
        VariableNames=["best_player", "value", "n_eligible"]);
end
```

```matlab
% best_player.m
scidb.configure_database("best_player.duckdb", ["player", "game"]);

% Import: one GameStats record per (player, game) row
scidb.for_each(@load_game_stats, struct("csv_path", "game_stats.csv"), {GameStats()});

% Who missed games?
inv = py.scidb.inventory.schema_inventory(py.scidb.get_database(), py.list({'player', 'game'}));
disp(inv.counts)
py.scidb.inventory.write_inventory_csv(inv, "missing_games.csv", true);
disp(groupcounts(readtable("missing_games.csv", TextType="string"), "player"))

% Morgan transferred: exclude (recorded once, applies to every later step)
if height(scidb.list_exclusions()) == 0
    scidb.exclude_schema("Transferred to another team after game 4", player="Morgan");
end

% Season totals per player
season = scidb.for_each(@season_totals, struct("games", GameStats()), ...
    {SeasonStats()}, player=[]);
disp(season(:, ["player", "games", "minutes", "fg_pct", "points_per_game"]))

% Parameter sweep: 2 metrics x 2 eligibility rules = 4 variants of BestPlayer
scidb.for_each(@pick_best, ...
    struct("season",    SeasonStats(), ...
           "metric",    scifor.EachOf("fg_pct", "points_per_game"), ...
           "min_games", scifor.EachOf(1, 10)), ...
    {BestPlayer()}, as_table=true);

% Load each variant by its settings
for metric = ["fg_pct", "points_per_game"]
    for min_games = [1, 10]
        best = BestPlayer().load(metric=metric, min_games=min_games).data;
        fprintf("%15s, min_games=%2d: %s (%.3g), %d eligible\n", ...
            metric, min_games, best.best_player, best.value, best.n_eligible);
    end
end

% Every variant, with its full set of settings
versions = BestPlayer().list_versions();
for i = 1:numel(versions)
    disp(versions(i).branch_params)
end
```
///
////
