# Best Player

A youth basketball team played 20 games this season, and the coach recorded a box score line for every player in every game: minutes played, shots made, shots attempted, and points. At the end-of-season party the coach wants to hand out a **Best Scorer** award. Who should get it?

That sounds like a lookup, but it isn't. One player averaged 26 points a game, more than anyone else, but played only two games before an injury. Does that make them the best scorer? This walkthrough answers the question entirely in the SciStack GUI, with no code. Each step explains *why* as well as *what*. It assumes you've worked through [Best Popcorn](best-popcorn.md), and focuses on what's new.

**What you'll learn**

- How to find **missing data** with the Inventory before you analyse anything.
- How to **leave a player out** of a figure and its statistics, without deleting their data.
- How to plot **one column** of a variable that holds several.
- How a **confidence interval** shows how much a small sample can tell you.
- What Stat Studio does when the groups are **different people** rather than the same people measured repeatedly.

Before you start, [install](../getting-started/installation.md) the VS Code extension and the `scistack` package.

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

Make a new folder called `best-player`, with a `data` folder inside it, and put `game_stats.csv` in `data`:

```text
best-player/
└── data/
    └── game_stats.csv
```

Open the `best-player` folder in VS Code (**File > Open Folder…**).

## Step 2: Create the database and choose the schema

Run **SciStack: Open Pipeline** from the Command Palette, choose to create a new database, and name it `best_player`. For the schema keys, enter:

```text
player, game
```

Each box score line is identified by **who** played and **which game** it was. The other four columns (`minutes`, `shots_made`, `shots_attempted`, `points`) are measurements, not part of how the data is organised. As in Best Popcorn, the keys must match the CSV's column names exactly.

## Step 3: Import the box scores

This works exactly like the Best Popcorn import:

1. In the **Path Inputs** tab (📁), click **+**, name the path input `game_stats_csv`, and set its **Path Template** to `data/game_stats.csv`. Drag it onto the canvas.
2. In the **Functions** tab (`f(x)`), click **+**, enter `pandas.read_csv`, and drag it onto the canvas. Connect `game_stats_csv` to its `filepath_or_buffer` input.
3. In the **Variables** tab (`x`), click **+**, name it `GameStats`, drag it onto the canvas, and connect `read_csv`'s output to it.
4. Click **▶ Run** on the `read_csv` node.

Each row has its own `player` and `game`, so each one is saved as a separate `GameStats` record: **123 records**. Each record is a whole box score line, with all four measurement columns.

## Step 4: Find out who missed games

Before computing anything, check how complete the data is. 9 players × 20 games could be 180 box score lines, but there are only 123.

Click **📋 Inventory** in the header and check `player` and `game`. The Inventory lists every combination of the two keys, each marked **present**, **missing** or **excluded**: 123 present and 57 missing. Players who played every game are folded into one row, so the gaps stand out:

| player  | games missed |
|---------|--------------|
| Casey   | 2            |
| Emerson | 4            |
| Harper  | 17           |
| Jordan  | 18           |
| Morgan  | 16           |

/// info | 📷 Screenshot
**Show:** the Inventory popup for `player` and `game`, with the missing games of Harper, Jordan and Morgan.
///

Each gap is a question to put to the coach, who explains:

- **Casey and Emerson** were sick for a few games. Nothing to do.
- **Harper** joined the team for the last three games.
- **Jordan** was injured after the second game.
- **Morgan** transferred to another team after game 4, and the awards are for the current roster.

Harper and Jordan matter for the statistics later. Morgan needs handling in the next step.

## Step 5: Plot points per game, without Morgan

Click `GameStats`, then **Open Plot Studio →** in the sidebar.

`GameStats` holds four columns, so Plot Studio adds a factor called `ColName` whose levels are the column names. Set up the figure:

1. **Data > Filters:** under `ColName`, click **Deselect all**, then tick only `points`.
2. **Data > Schema keys:** click the button and leave out `Morgan` for the `player` key.
3. **Structure:** group by `player`, and set `game` to **Collapse** under **Factors**. Each bar now averages one player's games.
4. **Chart:** choose **Bar + error**. In **Statistics > Summary**, set **Spread** to **95% CI**.
5. **Statistics > Show sample:** tick `game`, so each game appears as a point on its player's bar.

/// info | 📷 Screenshot
**Show:** the bar chart of points per game for 8 players with 95% CI error bars and one point per game. Jordan's bar is the tallest, with the widest error bar.
///

Leaving Morgan out here is a setting of this figure, not a change to the data: Morgan's four games are still in the database. It also travels with the figure's design, so the statistics in the next steps leave Morgan out too. ([Best Player - scidb](best-player-scidb.md) shows how to record an exclusion, with its reason, for every analysis in the project.)

Jordan's bar is the tallest, at 26 points per game, ahead of Avery's 21.5. But look at the error bars. Avery's covers about ±2.6 points, while Jordan's stretches from about 13 to 39 points. The error bar is a **95% confidence interval**: the range the player's true scoring average probably lies in, given the games you have. Two games can't pin down a player's average, and the figure shows it.

Save the figure as `Points per game` in the **Saved plots** rail.

## Step 6: Describe the analysis to Stat Studio

Click **StatStudio** in Plot Studio's toolbar. Stat Studio opens in the **Step by step** view, set up from the figure, so `points` is the measure and Morgan is left out. Because the figure's error bars are 95% CIs, the descriptive table shows each player's mean [95% CI].

- **What did you measure?** 119 values: the 123 box score lines minus Morgan's 4.
- **What is one independent thing you measured?** *Each game counts once: 119 games in total.* Stat Studio adds: *Each game is counted within its player: the same game name (such as G01) under two different players counts as two different games.* In the schema, a game belongs to a player (`player` comes first), so Avery's G01 and Blake's G01 are two different box score lines. Each line counts once.
- **What are you comparing?** `player` is set to **Compare**, and `game` to **Average over**.
- **Was each game measured at every level?** *Each game appears at only one player level.* Each box score line belongs to one player, so the players are **different groups**, like a treatment group and a control group. That's the opposite of Best Popcorn, where every participant rated every brand.
- **Is anything missing?** Nothing: each line belongs to exactly one player, so no line can be missing a player. The games players missed don't count as missing data here. They just mean that some players have fewer values.

/// info | 📷 Screenshot
**Show:** the "Was each game measured at every level?" screen, with **Different games in each level** recommended.
///

## Step 7: Choose the tests

- **What is your main question?** Every pair of players: 8 players make **28 comparisons**. Keep **Yes, an overall test and then the pairs**.
- **Which test?** *The values are roughly bell-shaped* and *the groups vary by similar amounts*, so Stat Studio recommends **Welch's t-test** for the pairs and an **ANOVA** overall. Keep both. Points are real counts, so unlike Best Popcorn's ratings, there's no reason to switch to ranks. Welch's t-test compares two separate groups without assuming they vary by the same amount, which is the safer choice when one group has 20 games and the other has 2.
- **How should false alarms be kept rare?** With 28 comparisons, Stat Studio points out that *about 1.4 of them would still look significant by chance without a correction*. Keep **Holm correction**.
- **How should the size of each difference be reported?** Keep **Hedges' g_s**, the standard size for Welch's t-test, and the **95% CI of the difference**.

## Step 8: Read the results

The overall ANOVA gives F(7, 111) = 27.27, p < .001: the players don't all score the same. *20 of the 28 comparisons are significant after the correction.* These are the rows that decide Best Scorer:

| Comparison | Δ [95% CI] | Statistic | p | p (adj.) |
|---|---|---|---|---|
| Avery − Jordan | −4.5 [−8.4, −0.6] | t(5.8) = −2.81 | .032 | .159 |
| Avery − Casey | 6.7 [3.0, 10.4] | t(35.5) = 3.65 | < .001 | .011 |
| Avery − Blake | 9.1 [6.1, 12.1] | t(32.9) = 6.08 | < .001 | < .001 |

- **Avery outscored everyone who played most of the season.** Casey is the closest, and even that gap (6.7 points per game) has an adjusted p of .011.
- **Jordan's lead over Avery isn't convincing.** On its own, p = .032 looks significant. But it's one of 28 comparisons, and after the Holm correction it's .159. Two games can't separate Jordan from Avery.
- **The table says when a comparison rests on very little.** Every row with Jordan carries the note *cell has n=2*. The test ran, but it's based on two games.

/// info | 📷 Screenshot
**Show:** the Results screen with the comparisons table and the notes beneath it.
///

Use **Copy sentences** under the table for a results section. Then name the table `Scoring statistics` and save it. To draw brackets on `Points per game`, switch to **Advanced** and use **Brackets on the figure**, as in Best Popcorn. With 28 pairs, keep **Pairs** on the significant ones, or the figure fills with brackets.

## Summary

| Question | Where it was answered |
|---|---|
| Who missed games? | **📋 Inventory**: 57 missing combinations, mostly Harper, Jordan and Morgan |
| How to leave out Morgan? | Plot Studio **Schema keys**, a setting of the figure, carried into Stat Studio |
| Which column to plot? | Plot Studio **Filters** on `ColName`: only `points` |
| How sure is each average? | 95% CI error bars, and the mean [95% CI] table |
| Are the players different people? | Stat Studio: each box score line belongs to one player, so `player` is between |
| Which tests? | Welch's t-tests and a one-way ANOVA, Holm over 28 comparisons |

## Conclusion

> Points per game differed between players (one-way ANOVA, F(7, 111) = 27.3, p < .001). Avery (21.5 points per game, 95% CI [18.9, 24.1], 20 games) outscored every player who played most of the season (Welch's t-tests, Holm-adjusted p ≤ .011). Jordan's higher average (26.0, 95% CI [13.3, 38.7]) came from two games and was not reliably different from Avery's (adjusted p = .16).

**Avery is the Best Scorer.**

## Where to go next

- The Code versions, [Best Player - scifor](best-player-scifor.md) and [Best Player - scidb](best-player-scidb.md), also pick a **Best Shooter** and run the award under four definitions of "best" at once, a parameter sweep. That takes a few short functions, because shooting percentage has to be calculated from each player's season totals.
- [Process](../gui/process.md), [Visualize](../gui/visualize.md) and [Summarize](../gui/summarize.md) describe each part of the GUI in more detail.
