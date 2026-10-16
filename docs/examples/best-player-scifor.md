# Best Player - scifor

A youth basketball team played 20 games this season, and the coach recorded a box score line for every player in every game: minutes played, shots made, shots attempted, and points. At the end-of-season party the coach wants to hand out two awards, **Best Shooter** and **Best Scorer**. Who should get them?

That sounds like a lookup, but it isn't. A player who made all 5 of their shots this season shoots 100%, which beats everyone. Does that make them the best shooter? Most people would say a player needs to have played a reasonable number of games first. "Best" depends on choices you make, and a good analysis shows how the answer changes with those choices.

This walkthrough uses `scifor` alone, with no database. It assumes you've worked through [Best Popcorn - scifor](best-popcorn-scifor.md), and focuses on what's new.

**What you'll learn**

- How a function can receive **several columns at once** (a whole box score line).
- How to run one analysis **under several settings at once** with **`EachOf`**, a *parameter sweep*.
- Why each result has to be **labelled with the settings that produced it**, and why that becomes a chore.

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

Make a new folder, put `game_stats.csv` in it, and create a script file (`best_player.py` or `best_player.m`) next to it. No extra libraries are needed beyond SciStack.

## Step 2: Load the data and declare the schema

Each row is identified by **who** played and **which game** it was, so the schema is `["player", "game"]`. The other four columns are measurements.

/// tab | Python
```python
import pandas as pd
from scifor import set_schema, for_each, EachOf

stats = pd.read_csv("game_stats.csv")
set_schema(["player", "game"])
```
///

/// tab | MATLAB
```matlab
stats = readtable("game_stats.csv", TextType="string");
scifor.set_schema(["player", "game"]);
```
///

One more thing before the analysis: **Morgan transferred to another team after four games**, and the awards are for the current roster. Remove Morgan's rows:

/// tab | Python
```python
stats = stats[stats["player"] != "Morgan"]
```
///

/// tab | MATLAB
```matlab
stats = stats(stats.player ~= "Morgan", :);
```
///

This works, but the reason Morgan was removed now exists only as this line of code. The scidb version records the reason in the database instead, and leaves Morgan's data in place.

## Step 3: Season totals for each player

The first step is to turn 20 box score lines into one season line per player. The function receives **all of one player's games** and returns one row:

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


season = for_each(
    season_totals,
    inputs={"games": stats},
    player=[],               # every player in the data; games are pooled
)
print(season.round(3))
```
///

/// tab | MATLAB
Add this function at the bottom of `best_player.m`:

```matlab
function out = season_totals(games)
    % All of one player's box score lines in, one season line out.
    made = sum(games.shots_made);
    attempted = sum(games.shots_attempted);
    out = table(height(games), sum(games.minutes), made / attempted, mean(games.points), ...
        VariableNames=["games", "minutes", "fg_pct", "points_per_game"]);
end
```

```matlab
season = scifor.for_each(@season_totals, ...
    struct("games", stats), ...
    player=[]);              % every player in the data; games are pooled
disp(season)
```
///

In Best Popcorn the function got a single column of numbers. This time the table has **four** measurement columns, so `for_each` passes them as a table: one player's rows, with the `player` and `game` columns removed. The function looks columns up by name.

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

Two lines stand out. **Harper** joined the team for the last three games and made all 5 shots. **Jordan** scored 27 and 25 points in the first two games, then got injured for the rest of the season.

## Step 4: Who's the best? Try several definitions at once

The awards need two decisions:

- **Which statistic?** `fg_pct` for Best Shooter, `points_per_game` for Best Scorer.
- **How many games must a player have played to qualify?** The question is whether to count Harper's and Jordan's small samples. Try a `min_games` of 1 (everyone counts) and 10 (half the season).

That's 2 × 2 = 4 combinations. Instead of calling the function four times, wrap each setting in **`EachOf`**, and `for_each` runs every combination. This is called a **parameter sweep**.

/// tab | Python
```python
def pick_best(season, metric, min_games):
    """The top player by `metric`, among players with at least `min_games` games."""
    eligible = season[season["games"] >= min_games]
    top = eligible.sort_values(metric, ascending=False).iloc[0]
    return pd.DataFrame({
        "metric":      [metric],       # label the result with its settings...
        "min_games":   [min_games],    # ...or you can't tell the rows apart
        "best_player": [top["player"]],
        "value":       [top[metric]],
        "n_eligible":  [len(eligible)],
    })


awards = for_each(
    pick_best,
    inputs={
        "season":    season,
        "metric":    EachOf("fg_pct", "points_per_game"),
        "min_games": EachOf(1, 10),
    },
    as_table=True,   # keep the player column; pick_best needs it
)
print(awards)
```
///

/// tab | MATLAB
Add this function at the bottom of the script:

```matlab
function out = pick_best(season, metric, min_games)
    % The top player by METRIC, among players with at least MIN_GAMES games.
    eligible = season(season.games >= min_games, :);
    eligible = sortrows(eligible, metric, "descend");
    out = table(string(metric), min_games, ...    % label the result with its settings...
        eligible.player(1), eligible.(metric)(1), height(eligible), ...
        VariableNames=["metric", "min_games", "best_player", "value", "n_eligible"]);
end
```

```matlab
awards = scifor.for_each(@pick_best, ...
    struct("season",    season, ...
           "metric",    scifor.EachOf("fg_pct", "points_per_game"), ...
           "min_games", scifor.EachOf(1, 10)), ...
    as_table=true);   % keep the player column; pick_best needs it
disp(awards)
```
///

No schema keys are named in this call, so there's nothing to iterate over: each of the four runs gets the **whole** season table (as in the Friedman step of Best Popcorn). `as_table=True` keeps its `player` column, which the function needs to report who won.

| metric          | min_games | best_player | value | n_eligible |
|-----------------|-----------|-------------|-------|------------|
| fg_pct          | 1         | Harper      | 1.000 | 8          |
| fg_pct          | 10        | Blake       | 0.533 | 6          |
| points_per_game | 1         | Jordan      | 26.00 | 8          |
| points_per_game | 10        | Avery       | 21.50 | 6          |

The definition decides the winner. If everyone counts, Harper (5 shots all season) wins Best Shooter and Jordan (2 games) wins Best Scorer. If a player needs 10 games to qualify, the awards go to **Blake** and **Avery**, who earned them over a full season. That's the call most coaches would make, and the sweep shows exactly what is at stake in that choice.

/// warning | The labelling chore
`for_each` runs all four combinations and stacks the results, but **it doesn't add columns saying which settings produced which row**. That's why `pick_best` returns its own `metric` and `min_games`. Without them, you'd have four unlabelled rows and no way to tell which is which.

This is manageable here. In a larger project with several steps, each with its own settings, keeping track by hand quickly becomes error-prone. [Best Player - scidb](best-player-scidb.md) runs the same analysis and lets scidb keep track of which settings produced each result.
///

## The complete script

//// details | Full script (click to expand)
    type: example

/// tab | Python
```python
# best_player.py
import pandas as pd
from scifor import set_schema, for_each, EachOf


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
        "metric":      [metric],
        "min_games":   [min_games],
        "best_player": [top["player"]],
        "value":       [top[metric]],
        "n_eligible":  [len(eligible)],
    })


stats = pd.read_csv("game_stats.csv")
set_schema(["player", "game"])
stats = stats[stats["player"] != "Morgan"]   # transferred after game 4

season = for_each(season_totals, inputs={"games": stats}, player=[])
print(season.round(3))

awards = for_each(
    pick_best,
    inputs={
        "season":    season,
        "metric":    EachOf("fg_pct", "points_per_game"),
        "min_games": EachOf(1, 10),
    },
    as_table=True,
)
print(awards)
```
///

/// tab | MATLAB
```matlab
% best_player.m
stats = readtable("game_stats.csv", TextType="string");
scifor.set_schema(["player", "game"]);
stats = stats(stats.player ~= "Morgan", :);   % transferred after game 4

season = scifor.for_each(@season_totals, struct("games", stats), player=[]);
disp(season)

awards = scifor.for_each(@pick_best, ...
    struct("season",    season, ...
           "metric",    scifor.EachOf("fg_pct", "points_per_game"), ...
           "min_games", scifor.EachOf(1, 10)), ...
    as_table=true);
disp(awards)

% --- Local functions (must be at the end of a script) ---

function out = season_totals(games)
    % All of one player's box score lines in, one season line out.
    made = sum(games.shots_made);
    attempted = sum(games.shots_attempted);
    out = table(height(games), sum(games.minutes), made / attempted, mean(games.points), ...
        VariableNames=["games", "minutes", "fg_pct", "points_per_game"]);
end

function out = pick_best(season, metric, min_games)
    % The top player by METRIC, among players with at least MIN_GAMES games.
    eligible = season(season.games >= min_games, :);
    eligible = sortrows(eligible, metric, "descend");
    out = table(string(metric), min_games, ...
        eligible.player(1), eligible.(metric)(1), height(eligible), ...
        VariableNames=["metric", "min_games", "best_player", "value", "n_eligible"]);
end
```
///
////
