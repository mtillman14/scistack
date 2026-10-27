# Best Popcorn - scidb

One hundred shoppers in a grocery store each tasted three brands of popcorn and rated each one from 1 (awful) to 10 (amazing). Which brand is best, and is the difference big enough to be more than chance?

[Best Popcorn - scifor](best-popcorn-scifor.md) answered this question with tables held in memory. This page runs the same analysis with **`scidb`**, which stores every input and result in a database file. Each saved value also records **how it was produced**: the function that made it, the data it read, and any settings passed in. Close Python or MATLAB, come back next week, and the results are still there with their history.

The statistics are the same as in the scifor version, so this page explains them only briefly. It focuses on what changes when a database is involved. If a `for_each` step is unclear, the scifor version explains the slicing in more detail.

**What you'll learn**

- How to declare **variables**, the named kinds of data that scidb stores.
- How to **import a CSV** so that each row becomes its own record at the right location.
- How `scidb.for_each` **loads inputs from the database and saves outputs** for you.
- How a run with **no iterated keys** produces a single dataset-wide result.
- How **`scistackstats`** runs the statistics on records **read straight from the database**.
- How to **read results back** and ask what produced them.

## The data

The same dataset as the scifor version: 300 rows (100 participants × 3 brands), one row per rating, with columns `participant`, `brand`, and `rating`.

**Download:** [popcorn_ratings.csv](data/popcorn_ratings.csv){ download="popcorn_ratings.csv" }

If the download doesn't work for you, expand the box below, copy the contents, and save them as `popcorn_ratings.csv`.

/// details | popcorn_ratings.csv (click to expand)
    type: example

```text
participant,brand,rating
P001,Butterburst,5
P001,KettleKing,5
P001,SeaSaltCo,4
P002,Butterburst,7
P002,KettleKing,7
P002,SeaSaltCo,6
P003,Butterburst,8
P003,KettleKing,6
P003,SeaSaltCo,6
P004,Butterburst,7
P004,KettleKing,8
P004,SeaSaltCo,8
P005,Butterburst,6
P005,KettleKing,6
P005,SeaSaltCo,6
P006,Butterburst,5
P006,KettleKing,7
P006,SeaSaltCo,4
P007,Butterburst,9
P007,KettleKing,9
P007,SeaSaltCo,8
P008,Butterburst,9
P008,KettleKing,6
P008,SeaSaltCo,4
P009,Butterburst,8
P009,KettleKing,8
P009,SeaSaltCo,3
P010,Butterburst,10
P010,KettleKing,8
P010,SeaSaltCo,8
P011,Butterburst,8
P011,KettleKing,5
P011,SeaSaltCo,3
P012,Butterburst,9
P012,KettleKing,10
P012,SeaSaltCo,8
P013,Butterburst,10
P013,KettleKing,7
P013,SeaSaltCo,5
P014,Butterburst,8
P014,KettleKing,9
P014,SeaSaltCo,4
P015,Butterburst,8
P015,KettleKing,8
P015,SeaSaltCo,6
P016,Butterburst,7
P016,KettleKing,6
P016,SeaSaltCo,5
P017,Butterburst,9
P017,KettleKing,9
P017,SeaSaltCo,8
P018,Butterburst,8
P018,KettleKing,8
P018,SeaSaltCo,8
P019,Butterburst,8
P019,KettleKing,5
P019,SeaSaltCo,3
P020,Butterburst,5
P020,KettleKing,6
P020,SeaSaltCo,5
P021,Butterburst,9
P021,KettleKing,7
P021,SeaSaltCo,6
P022,Butterburst,8
P022,KettleKing,8
P022,SeaSaltCo,5
P023,Butterburst,6
P023,KettleKing,4
P023,SeaSaltCo,3
P024,Butterburst,6
P024,KettleKing,5
P024,SeaSaltCo,3
P025,Butterburst,8
P025,KettleKing,6
P025,SeaSaltCo,5
P026,Butterburst,10
P026,KettleKing,5
P026,SeaSaltCo,7
P027,Butterburst,6
P027,KettleKing,6
P027,SeaSaltCo,6
P028,Butterburst,8
P028,KettleKing,8
P028,SeaSaltCo,9
P029,Butterburst,9
P029,KettleKing,5
P029,SeaSaltCo,6
P030,Butterburst,5
P030,KettleKing,6
P030,SeaSaltCo,5
P031,Butterburst,8
P031,KettleKing,9
P031,SeaSaltCo,5
P032,Butterburst,7
P032,KettleKing,6
P032,SeaSaltCo,7
P033,Butterburst,3
P033,KettleKing,6
P033,SeaSaltCo,5
P034,Butterburst,10
P034,KettleKing,9
P034,SeaSaltCo,6
P035,Butterburst,8
P035,KettleKing,6
P035,SeaSaltCo,8
P036,Butterburst,10
P036,KettleKing,9
P036,SeaSaltCo,7
P037,Butterburst,5
P037,KettleKing,3
P037,SeaSaltCo,6
P038,Butterburst,10
P038,KettleKing,8
P038,SeaSaltCo,7
P039,Butterburst,9
P039,KettleKing,9
P039,SeaSaltCo,7
P040,Butterburst,8
P040,KettleKing,8
P040,SeaSaltCo,6
P041,Butterburst,6
P041,KettleKing,6
P041,SeaSaltCo,4
P042,Butterburst,5
P042,KettleKing,5
P042,SeaSaltCo,4
P043,Butterburst,5
P043,KettleKing,7
P043,SeaSaltCo,5
P044,Butterburst,4
P044,KettleKing,6
P044,SeaSaltCo,2
P045,Butterburst,5
P045,KettleKing,7
P045,SeaSaltCo,4
P046,Butterburst,7
P046,KettleKing,5
P046,SeaSaltCo,6
P047,Butterburst,9
P047,KettleKing,5
P047,SeaSaltCo,5
P048,Butterburst,6
P048,KettleKing,7
P048,SeaSaltCo,7
P049,Butterburst,5
P049,KettleKing,5
P049,SeaSaltCo,5
P050,Butterburst,8
P050,KettleKing,5
P050,SeaSaltCo,6
P051,Butterburst,8
P051,KettleKing,9
P051,SeaSaltCo,6
P052,Butterburst,7
P052,KettleKing,7
P052,SeaSaltCo,6
P053,Butterburst,6
P053,KettleKing,5
P053,SeaSaltCo,6
P054,Butterburst,5
P054,KettleKing,6
P054,SeaSaltCo,6
P055,Butterburst,7
P055,KettleKing,5
P055,SeaSaltCo,3
P056,Butterburst,10
P056,KettleKing,8
P056,SeaSaltCo,9
P057,Butterburst,6
P057,KettleKing,6
P057,SeaSaltCo,3
P058,Butterburst,8
P058,KettleKing,7
P058,SeaSaltCo,8
P059,Butterburst,10
P059,KettleKing,9
P059,SeaSaltCo,7
P060,Butterburst,10
P060,KettleKing,5
P060,SeaSaltCo,3
P061,Butterburst,9
P061,KettleKing,9
P061,SeaSaltCo,7
P062,Butterburst,6
P062,KettleKing,6
P062,SeaSaltCo,5
P063,Butterburst,9
P063,KettleKing,8
P063,SeaSaltCo,6
P064,Butterburst,10
P064,KettleKing,5
P064,SeaSaltCo,5
P065,Butterburst,8
P065,KettleKing,5
P065,SeaSaltCo,5
P066,Butterburst,9
P066,KettleKing,7
P066,SeaSaltCo,7
P067,Butterburst,8
P067,KettleKing,7
P067,SeaSaltCo,8
P068,Butterburst,8
P068,KettleKing,7
P068,SeaSaltCo,3
P069,Butterburst,7
P069,KettleKing,6
P069,SeaSaltCo,5
P070,Butterburst,6
P070,KettleKing,6
P070,SeaSaltCo,4
P071,Butterburst,4
P071,KettleKing,6
P071,SeaSaltCo,5
P072,Butterburst,7
P072,KettleKing,7
P072,SeaSaltCo,6
P073,Butterburst,5
P073,KettleKing,6
P073,SeaSaltCo,6
P074,Butterburst,7
P074,KettleKing,5
P074,SeaSaltCo,6
P075,Butterburst,7
P075,KettleKing,6
P075,SeaSaltCo,5
P076,Butterburst,9
P076,KettleKing,9
P076,SeaSaltCo,9
P077,Butterburst,7
P077,KettleKing,7
P077,SeaSaltCo,2
P078,Butterburst,10
P078,KettleKing,7
P078,SeaSaltCo,9
P079,Butterburst,7
P079,KettleKing,6
P079,SeaSaltCo,7
P080,Butterburst,4
P080,KettleKing,4
P080,SeaSaltCo,2
P081,Butterburst,6
P081,KettleKing,4
P081,SeaSaltCo,3
P082,Butterburst,5
P082,KettleKing,2
P082,SeaSaltCo,3
P083,Butterburst,9
P083,KettleKing,5
P083,SeaSaltCo,5
P084,Butterburst,7
P084,KettleKing,6
P084,SeaSaltCo,4
P085,Butterburst,6
P085,KettleKing,4
P085,SeaSaltCo,7
P086,Butterburst,6
P086,KettleKing,6
P086,SeaSaltCo,5
P087,Butterburst,7
P087,KettleKing,8
P087,SeaSaltCo,5
P088,Butterburst,8
P088,KettleKing,6
P088,SeaSaltCo,6
P089,Butterburst,10
P089,KettleKing,5
P089,SeaSaltCo,4
P090,Butterburst,6
P090,KettleKing,3
P090,SeaSaltCo,2
P091,Butterburst,10
P091,KettleKing,8
P091,SeaSaltCo,6
P092,Butterburst,4
P092,KettleKing,4
P092,SeaSaltCo,5
P093,Butterburst,4
P093,KettleKing,1
P093,SeaSaltCo,4
P094,Butterburst,7
P094,KettleKing,6
P094,SeaSaltCo,7
P095,Butterburst,6
P095,KettleKing,6
P095,SeaSaltCo,6
P096,Butterburst,8
P096,KettleKing,6
P096,SeaSaltCo,6
P097,Butterburst,10
P097,KettleKing,4
P097,SeaSaltCo,4
P098,Butterburst,5
P098,KettleKing,7
P098,SeaSaltCo,6
P099,Butterburst,7
P099,KettleKing,5
P099,SeaSaltCo,5
P100,Butterburst,8
P100,KettleKing,9
P100,SeaSaltCo,7
```
///

/// note | The data is made up
The ratings were generated for this tutorial. They are realistic but fake, so don't base a popcorn purchase on them.
///

## Step 1: Set up a folder

Make a new folder and put `popcorn_ratings.csv` in it. The code is split across a few files, all in that same folder. Run everything from that folder.

/// tab | Python
```text
best-popcorn/
├── popcorn_ratings.csv
├── popcorn_variables.py     # the variable definitions (Step 3)
└── best_popcorn.py          # the analysis (Steps 2 and 4–7)
```

You also need SciPy for the statistical tests: `pip install scipy`.
///

/// tab | MATLAB
```text
best-popcorn/
├── popcorn_ratings.csv
├── Rating.m  BrandSummary.m  FriedmanTest.m  BestVsChallenger.m   % variables (Step 3)
├── load_ratings.m  summarize_ratings.m  friedman_test.m  compare_to_best.m
└── best_popcorn.m           % the analysis script
```

Put each function in **its own file** instead of at the bottom of the script. scidb identifies a function by its source file, so it can tell when the function's code changes.

You also need the **Statistics and Machine Learning Toolbox** (`friedman`, `signrank`) and MATLAB R2021a or newer for the `name=value` syntax.
///

When the script first runs, it creates a database file, `popcorn.duckdb`, and two log files in this folder: `scistack.log`, a short journal of each run, and `scistack-debug.log`, with full detail.

## Step 2: Configure the database

With scifor you called `set_schema`. With scidb you call `configure_database` instead. It does the same job, and it also opens (or creates) the database file:

/// tab | Python
```python
# best_popcorn.py
import numpy as np
import pandas as pd
from scipy import stats
import scidb
from scidb import Fixed

from popcorn_variables import Rating, BrandSummary, FriedmanTest, BestVsChallenger

db = scidb.configure_database("popcorn.duckdb", ["participant", "brand"])
```
///

/// tab | MATLAB
```matlab
% best_popcorn.m
scidb.configure_database("popcorn.duckdb", ["participant", "brand"]);
```
///

The schema is the same as before: each rating is identified by **who** gave it and **which brand** it's for. In scidb the schema keys are also **addresses**. Every value in the database is stored at a location such as `participant="P001", brand="Butterburst"`. A result can also be stored at a coarser location, for example `brand="Butterburst"` alone or no keys at all, and you'll see both below.

## Step 3: Declare the variables

A **variable** is a named kind of data: "a rating", "a brand's summary statistics". Each one gets its own table in the database, so `Rating` records and `BrandSummary` records can never get mixed up. Declaring a variable takes one line. You don't need to describe its columns, because scidb takes them from the first value saved.

The analysis needs four variables: one for the raw data and one for each result.

/// tab | Python
```python
# popcorn_variables.py
from scidb import BaseVariable


class Rating(BaseVariable):
    """One participant's 1-10 rating of one brand."""


class BrandSummary(BaseVariable):
    """n, mean, median, and SD of one brand's ratings."""


class FriedmanTest(BaseVariable):
    """Do the three brands differ at all?"""


class BestVsChallenger(BaseVariable):
    """The best brand compared, participant by participant, with one other brand."""
```

The classes live in their own module so that any later script can import them and read the stored results.
///

/// tab | MATLAB
Each variable is a `classdef` in its own file:

```matlab
% Rating.m
classdef Rating < scidb.BaseVariable
    % One participant's 1-10 rating of one brand.
end
```

```matlab
% BrandSummary.m
classdef BrandSummary < scidb.BaseVariable
    % n, mean, median, and SD of one brand's ratings.
end
```

```matlab
% FriedmanTest.m
classdef FriedmanTest < scidb.BaseVariable
    % Do the three brands differ at all?
end
```

```matlab
% BestVsChallenger.m
classdef BestVsChallenger < scidb.BaseVariable
    % The best brand compared, participant by participant, with one other brand.
end
```
///

## Step 4: Import the CSV

The ratings need to go into the database as `Rating` records, one per `(participant, brand)` location. Like every other step, the import is a function run through `for_each`, so the database records that these ratings came from `popcorn_ratings.csv`.

The function just reads the file:

/// tab | Python
```python
def load_ratings(csv_path, sep):
    """Read the whole ratings file: one row per (participant, brand)."""
    return pd.read_csv(csv_path, sep=sep)
```
///

/// tab | MATLAB
```matlab
% load_ratings.m
function ratings = load_ratings(csv_path, sep)
    % Read the whole ratings file: one row per (participant, brand).
    ratings = readtable(csv_path, Delimiter=sep, TextType="string");
end
```
///

Run it **without naming any schema keys**, so it runs exactly once and reads the whole file:

/// tab | Python
```python
scidb.for_each(
    load_ratings,
    inputs={"csv_path": "popcorn_ratings.csv",   # constant inputs
            "sep": ","},                         # the column separator
    outputs=[Rating],                            # save the return value as Rating
)
```
///

/// tab | MATLAB
```matlab
scidb.for_each(@load_ratings, ...
    struct("csv_path", "popcorn_ratings.csv", ...   % constant inputs
           "sep", ","), ...                         % the column separator
    {Rating()});                                     % save the return value as Rating
```
///

There are two new things here compared with scifor:

- **`outputs`** names the variable the return value is saved as. scifor only handed results back. scidb also saves them.
- **The 300-row table becomes 300 records, not one.** The returned table has `participant` and `brand` columns, and this run didn't fix either key, so scidb treats each row as addressing its own location. Row `P001, Butterburst, 5` is saved as a `Rating` of 5 at `participant="P001", brand="Butterburst"`. A log line confirms it: `spreading rows into separate records`.

`sep` could have been left at `read_csv`'s default. Passing it as an input instead means the database records which separator each `Rating` was read with, and anyone who gets this project can change it in one place: a spreadsheet saved in many European languages uses `;` (see [Step 10](#step-10-optional-share-the-project-with-a-colleague)). Constant inputs like these are saved with each result, so reading with a different `sep` would make a separate set of results rather than overwriting these.

From here on, no step reads the CSV. Every input comes from the database.

## Step 5: Summarize each brand

The summary function is the same as in the scifor version: it takes one brand's ratings and returns one row of statistics.

/// tab | Python
```python
def summarize_ratings(ratings):
    """All of one brand's ratings in, one row of summary statistics out."""
    r = np.ravel(ratings)
    return pd.DataFrame({
        "n":      [len(r)],
        "mean":   [r.mean()],
        "median": [np.median(r)],
        "sd":     [r.std(ddof=1)],
    })


summary = scidb.for_each(
    summarize_ratings,
    inputs={"ratings": Rating["rating"]},  # load Rating, pass only its "rating" column
    outputs=[BrandSummary],
    brand=[],                              # [] means "every brand in the database"
)
summary = summary.sort_values("mean", ascending=False, ignore_index=True)
print(summary[["brand", "n", "mean", "median", "sd"]].round(2))
```
///

/// tab | MATLAB
```matlab
% summarize_ratings.m
function out = summarize_ratings(ratings)
    % All of one brand's ratings in, one row of summary statistics out.
    out = table(numel(ratings), mean(ratings), median(ratings), std(ratings), ...
        VariableNames=["n", "mean", "median", "sd"]);
end
```

```matlab
summary = scidb.for_each(@summarize_ratings, ...
    struct("ratings", Rating("rating")), ...   % load Rating, pass only its "rating" column
    {BrandSummary()}, ...
    brand=[]);                                  % [] means "every brand in the database"
summary = sortrows(summary, "mean", "descend");
disp(summary(:, ["brand", "n", "mean", "median", "sd"]))
```
///

This time the input is the variable **`Rating`** instead of a table already in memory, and scidb loads it from the database for each brand. `Rating["rating"]` (MATLAB: `Rating("rating")`) is a **column selection**: it passes only the `rating` column as a plain array of 100 numbers.

As in the scifor version, iterating over `brand` alone pools all participants into each call. The difference is what happens to each result: it is saved as a `BrandSummary` at `brand="Butterburst"` (with no participant), the brand's own location. `for_each` also still returns the results as a table, which the next step uses:

| brand       | n   | mean | median | sd   |
|-------------|-----|------|--------|------|
| Butterburst | 100 | 7.30 | 7      | 1.80 |
| KettleKing  | 100 | 6.35 | 6      | 1.69 |
| SeaSaltCo   | 100 | 5.48 | 6      | 1.71 |

## Step 6: Is there any difference at all?

The omnibus test is the **Friedman test**: a repeated-measures test that compares ranks within each participant, suited to 1–10 ratings. (The scifor version explains why this test fits.)

In the scifor version this was a direct function call, because there was nothing to loop over. With scidb, it's worth running it through `for_each` anyway so that the result is saved and its origin recorded. Naming **no schema keys** gives exactly one call that receives **every** `Rating`, and the result is saved at the dataset level, with no participant or brand:

/// tab | Python
```python
def friedman_test(ratings):
    """All ratings in (one row per participant x brand), one row of test results out."""
    wide = ratings.pivot(index="participant", columns="brand", values="rating")
    chi2, p = stats.friedmanchisquare(*(wide[b] for b in wide.columns))
    return pd.DataFrame({"n_participants": [len(wide)], "chi2": [chi2], "p_value": [p]})


friedman = scidb.for_each(
    friedman_test,
    inputs={"ratings": Rating},
    outputs=[FriedmanTest],
    as_table=True,   # keep participant/brand columns; the pivot needs them
)
print(friedman[["n_participants", "chi2", "p_value"]])
```
///

/// tab | MATLAB
```matlab
% friedman_test.m
function out = friedman_test(ratings)
    % All ratings in (one row per participant x brand), one row of test results out.
    wide = unstack(ratings(:, ["participant", "brand", "rating"]), "rating", "brand");
    [p, tbl] = friedman(wide{:, 2:end}, 1, "off");
    out = table(height(wide), tbl{2, 5}, p, ...
        VariableNames=["n_participants", "chi2", "p_value"]);
end
```

```matlab
friedman = scidb.for_each(@friedman_test, ...
    struct("ratings", Rating()), ...
    {FriedmanTest()}, ...
    as_table=true);   % keep participant/brand columns; the reshape needs them
disp(friedman(:, ["n_participants", "chi2", "p_value"]))
```
///

`as_table=True` passes the loaded ratings as a table **with** the `participant` and `brand` columns, which the function needs to reshape the data into one column per brand. You should see χ²(2) = 58.08, p ≈ 2.4 × 10⁻¹³. The brands are not all equally liked.

## Step 7: Which one is best?

Butterburst had the highest mean. Is it rated higher than **each** of the other brands? As before, each participant's two ratings are paired up and compared with a **Wilcoxon signed-rank test**, once per challenger brand. `Fixed` pins the `best` input to Butterburst while `challenger` follows the iteration.

The Bonferroni correction is new here. In the scifor version it was applied to the results table afterwards. With scidb, anything computed outside `for_each` isn't saved, so the correction moves **into the function**, and the number of comparisons comes in as a **constant input**:

/// tab | Python
```python
def compare_to_best(best, challenger, n_comparisons):
    """Paired comparison: each participant's rating of the best brand vs. a challenger."""
    paired = best[["participant", "rating"]].merge(
        challenger[["participant", "rating"]],
        on="participant", suffixes=("_best", "_challenger"),
    )
    diff = paired["rating_best"] - paired["rating_challenger"]
    p = stats.wilcoxon(paired["rating_best"], paired["rating_challenger"]).pvalue
    return pd.DataFrame({
        "n_pairs":             [len(paired)],
        "mean_diff":           [diff.mean()],
        "n_prefer_best":       [(diff > 0).sum()],
        "n_prefer_challenger": [(diff < 0).sum()],
        "p_value":             [p],
        "p_bonferroni":        [min(p * n_comparisons, 1.0)],
    })


best_brand = summary.loc[0, "brand"]
challengers = list(summary.loc[1:, "brand"])

posthoc = scidb.for_each(
    compare_to_best,
    inputs={
        "best":          Fixed(Rating, brand=best_brand),  # always Butterburst
        "challenger":    Rating,                           # the current brand
        "n_comparisons": len(challengers),                 # a constant: 2
    },
    outputs=[BestVsChallenger],
    as_table=True,
    brand=challengers,
)
print(posthoc)
```
///

/// tab | MATLAB
```matlab
% compare_to_best.m
function out = compare_to_best(best, challenger, n_comparisons)
    % Paired comparison: each participant's rating of the best brand vs. a challenger.
    best = renamevars(best(:, ["participant", "rating"]), "rating", "rating_best");
    challenger = renamevars(challenger(:, ["participant", "rating"]), "rating", "rating_challenger");
    paired = innerjoin(best, challenger, Keys="participant");
    d = paired.rating_best - paired.rating_challenger;
    p = signrank(paired.rating_best, paired.rating_challenger);
    out = table(height(paired), mean(d), sum(d > 0), sum(d < 0), p, min(p * n_comparisons, 1), ...
        VariableNames=["n_pairs", "mean_diff", "n_prefer_best", "n_prefer_challenger", ...
                       "p_value", "p_bonferroni"]);
end
```

```matlab
best_brand = string(summary.brand(1));
challengers = string(summary.brand(2:end))';

posthoc = scidb.for_each(@compare_to_best, ...
    struct("best",          scidb.Fixed(Rating(), brand=best_brand), ...  % always Butterburst
           "challenger",    Rating(), ...                              % the current brand
           "n_comparisons", numel(challengers)), ...                   % a constant: 2
    {BestVsChallenger()}, ...
    as_table=true, brand=challengers);
disp(posthoc)
```
///

Each comparison is saved as a `BestVsChallenger` at the challenger's brand location. The constant `n_comparisons=2` is saved along with it. If you later re-ran with a different value, such as 3 after adding a fourth brand, the result would be stored as a separate **variant** next to this one instead of overwriting it.

| brand      | n_pairs | mean_diff | n_prefer_best | n_prefer_challenger | p_value  | p_bonferroni |
|------------|---------|-----------|---------------|---------------------|----------|--------------|
| KettleKing | 100     | 0.95      | 53            | 19                  | ≈ 2e-06  | ≈ 4e-06      |
| SeaSaltCo  | 100     | 1.82      | 72            | 11                  | < 1e-11  | < 1e-11      |

(Participants who gave both brands the same score count toward neither preference column. MATLAB's `signrank` applies a continuity correction, so its p-values differ from SciPy's in the later digits.)

**Butterburst is the best popcorn:** it is rated higher than both KettleKing and SeaSaltCo, with Bonferroni-corrected p < 0.001 for each comparison.

## Step 8: The same statistics, straight from the database

Steps 6 and 7 meant choosing each test and writing a function for it. `scistackstats`, which comes with SciStack, works out the tests from a description of the study. It's the engine behind [Stat Studio](https://scistack.readthedocs.io/en/latest/ui/gui/summarize/), so code and GUI give the same numbers. (The scifor version explains this step in more detail.)

You describe the study the way you would describe a figure of it: **group** by `brand`, the thing you're comparing, and **collapse** `participant`, so that each brand summarises everyone. From that, scistackstats finds that `participant` is the unit (n = 100), that `brand` is within-subject (everyone rated every brand), and therefore that the Friedman test and paired Wilcoxon tests apply.

With scidb, there's no table to build: `load_stats_tables` reads the `Rating` records from the database.

/// tab | Python
```python
from scistackplot import PlotKind, PlotSpec, Role
from scistackstats import Analysis, StatsSpec, describe_table, run
from scistackstats import export_comparisons, export_descriptives
from scistackstatsdb import load_stats_tables

design = PlotSpec(
    measures=["Rating"],                                   # the variable's name
    roles={"brand": Role.GROUP, "participant": Role.COLLAPSE},
    groups=["brand"],
    kind=PlotKind.BAR,
)
spec = StatsSpec(design=design, comparisons=[Analysis(family="nonparametric")])

tables = load_stats_tables(db, spec)
print(export_descriptives(describe_table(spec, tables), spec, "md", tables=tables))
report = run(spec, tables)
print(export_comparisons(report, spec, "md", tables=tables))
```
///

/// tab | MATLAB
`scistackstats` is a Python package, and there's no MATLAB version of it yet. The database is the same file whichever language wrote it, so run the Python code on `popcorn.duckdb` after the MATLAB steps.
///

| Comparison | Δ [95% CI] | Statistic | p | p (adj.) | Rank-biserial r | n |
|---|---|---|---|---|---|---|
| Butterburst − KettleKing | 1.00 [0.50, 1.00] | W = 479.00 | < .001 | < .001 | 0.64 | 100 |
| Butterburst − SeaSaltCo | 2.00 [1.50, 2.00] | W = 178.50 | < .001 | < .001 | 0.90 | 100 |
| KettleKing − SeaSaltCo | 1.00 [0.50, 1.00] | W = 631.50 | < .001 | < .001 | 0.60 | 100 |

The overall test matches Step 6, χ²(2) = 58.08, p < .001 (Kendall's W = 0.29). Here every pair is compared and the p-values are Holm-adjusted. Each difference comes with a confidence interval and a named effect size, and a note under the table states the tests, the unit, n and the correction.

These tables are printed, not saved: `run` doesn't write to the database. To make the statistics a recorded step, open `Rating` in Stat Studio with the same design and click **Add to pipeline**. That creates a `stat_` function whose results are saved and tracked like any other step.

## Step 9: Read the results back

This is where the database pays off. Open a **new** Python or MATLAB session in the same folder. Without re-running anything, connect to the database and load what was saved:

/// tab | Python
```python
import scidb
from popcorn_variables import Rating, BrandSummary, FriedmanTest, BestVsChallenger

db = scidb.configure_database("popcorn.duckdb", ["participant", "brand"])

print(Rating.load(participant="P001", brand="Butterburst").data)  # one raw rating
print(BrandSummary.load(brand="Butterburst").data)                # one brand's summary
print(FriedmanTest.load().data)                                   # the dataset-level test
print(BestVsChallenger.load(brand="KettleKing").data)             # one comparison
```
///

/// tab | MATLAB
```matlab
scidb.configure_database("popcorn.duckdb", ["participant", "brand"]);

disp(Rating().load(participant="P001", brand="Butterburst").data)  % one raw rating
disp(BrandSummary().load(brand="Butterburst").data)                % one brand's summary
disp(FriedmanTest().load().data)                                   % the dataset-level test
disp(BestVsChallenger().load(brand="KettleKing").data)             % one comparison
```
///

`load` takes a location and returns the record stored there. `.data` holds the value, here the one-row table the function returned. The location depends on how the value was produced: the raw rating is at a participant and a brand, a summary at a brand only, and the Friedman test at no location at all, so `FriedmanTest.load()` takes no arguments.

Every saved value also records how it was made. Ask where a comparison came from:

/// tab | Python
```python
prov = db.get_provenance(BestVsChallenger, brand="KettleKing")
print(prov["function_name"])  # compare_to_best
print(prov["constants"])      # includes n_comparisons = 2
print(prov["inputs"])         # the Rating records it read
```
///

/// tab | MATLAB
```matlab
p = BestVsChallenger().provenance(brand="KettleKing");
disp(p.function_name)  % compare_to_best
disp(p.constants)      % includes n_comparisons = 2
disp(p.inputs)         % the Rating records it read
```
///

The `Rating` records it points to have their own history, which leads back to `load_ratings` and `popcorn_ratings.csv`. Each number in the analysis can be traced back to the CSV it came from.

/// tip | Running the script again
Running the whole analysis a second time doesn't create duplicates: when identical data is saved at the same location, scidb keeps the existing record. You can re-run after any change without cleaning up first.
///

## Step 10 (optional): Share the project with a colleague

Suppose a colleague at another store ran the same taste test and wants to analyse it the same way. Their file differs from yours in two ways:

- It's called `store2_ratings.csv`.
- Their spreadsheet program saved it with semicolons between the columns (`participant;brand;rating`), as Excel does in many European languages.

### What you do: export

In the `best-popcorn` folder, with your environment active:

```bash
scistack export
```

This writes `popcorn.scistack` next to the database. It holds your scripts (`best_popcorn.py`, `popcorn_variables.py`, or the `.m` files), the run history, and the exact version of every package you have installed. It does **not** hold `popcorn_ratings.csv`, or your results unless you add `--data`. Send the file to your colleague.

### What your colleague does: import

They create and activate a new environment, install SciStack and SciPy into it (`pip install scipy`), then:

```bash
scistack bundle-info popcorn.scistack                 # what's inside: schema, files, environment
scistack import popcorn.scistack --into ~/best-popcorn-store2 --trust
```

`--into` names a new folder; an import never writes into an existing project. `--trust` says they trust your code. It also installs, at your versions, whatever a project's `pyproject.toml` lists; this folder has none, which is why they install SciPy themselves. Your history comes along, kept read-only for `scistack verify` (below).

Then, in the new folder:

1. Copy `store2_ratings.csv` into it.
2. In `best_popcorn.py` (or `best_popcorn.m`), change the two constants in the import step:

    /// tab | Python
    ```python
    scidb.for_each(
        load_ratings,
        inputs={"csv_path": "store2_ratings.csv",
                "sep": ";"},
        outputs=[Rating],
    )
    ```
    ///

    /// tab | MATLAB
    ```matlab
    scidb.for_each(@load_ratings, ...
        struct("csv_path", "store2_ratings.csv", ...
               "sep", ";"), ...
        {Rating()});
    ```
    ///

3. Run the script: `python best_popcorn.py`, or `best_popcorn` in MATLAB.

Their results go into their own `popcorn.duckdb`. Nothing they change reaches your project, and nothing you change later reaches theirs.

/// note | Different column names
`--schema` and `--map` on `scistack import` rename schema keys in everything SciStack stores: path templates, saved plots, saved tables. They can't rename them in your **code**, and this script names `participant` in several places (`pivot(index="participant")`, the `merge` in `compare_to_best`, the `load` calls). If their file calls the column `shopper`, the simplest fix is to rename that column header to `participant`. In the GUI version of this example, where no code names the keys, `--map participant=shopper` (or the import dialog's question) is enough.
///

### Reproducing your results

To check your analysis rather than extend it, your colleague imports the same way, copies **your** `popcorn_ratings.csv` into the new folder, changes nothing, and runs the script. Then:

```bash
scistack verify
```

It runs nothing: it compares their results with the history in the bundle, step by step, and reports which reproduced, which differ (and where the difference starts), and which haven't run.

See [Export & Import Projects](../sharing/export-import.md) for every option.

## Summary

Here is what changed between the scifor and scidb versions:

| | scifor | scidb |
|---|---|---|
| Setup | `set_schema(keys)` | `configure_database("popcorn.duckdb", keys)` |
| Inputs | Tables in memory | Variables, loaded from the database (`Rating`, `Rating["rating"]`) |
| Outputs | Returned only | Returned **and** saved (`outputs=[BrandSummary]`) |
| Raw data | Read from the CSV each run | Imported once; each row becomes a `Rating` record at its own location |
| One-off calculations | Called directly | Run through `for_each` with no keys, saved at the dataset level |
| Settings (e.g. `n_comparisons`) | Not recorded | Saved with the result; different values make separate variants |
| scistackstats input | A `LongTable` built from the table in memory | `load_stats_tables(db, spec)` reads the variable's records |
| Afterwards | Gone when the session ends | `load(...)` any result; `get_provenance` / `provenance` shows how it was made |
| Sharing | Send the script and the CSV | `scistack export`, `scistack import`, then `scistack verify` to check the results reproduce |

## The complete code

//// details | Full code (click to expand)
    type: example

/// tab | Python
```python
# popcorn_variables.py
from scidb import BaseVariable


class Rating(BaseVariable):
    """One participant's 1-10 rating of one brand."""


class BrandSummary(BaseVariable):
    """n, mean, median, and SD of one brand's ratings."""


class FriedmanTest(BaseVariable):
    """Do the three brands differ at all?"""


class BestVsChallenger(BaseVariable):
    """The best brand compared, participant by participant, with one other brand."""
```

```python
# best_popcorn.py
import numpy as np
import pandas as pd
from scipy import stats
import scidb
from scidb import Fixed
from scistackplot import PlotKind, PlotSpec, Role
from scistackstats import Analysis, StatsSpec, describe_table, run
from scistackstats import export_comparisons, export_descriptives
from scistackstatsdb import load_stats_tables

from popcorn_variables import Rating, BrandSummary, FriedmanTest, BestVsChallenger


def load_ratings(csv_path, sep):
    """Read the whole ratings file: one row per (participant, brand)."""
    return pd.read_csv(csv_path, sep=sep)


def summarize_ratings(ratings):
    """All of one brand's ratings in, one row of summary statistics out."""
    r = np.ravel(ratings)
    return pd.DataFrame({
        "n":      [len(r)],
        "mean":   [r.mean()],
        "median": [np.median(r)],
        "sd":     [r.std(ddof=1)],
    })


def friedman_test(ratings):
    """All ratings in (one row per participant x brand), one row of test results out."""
    wide = ratings.pivot(index="participant", columns="brand", values="rating")
    chi2, p = stats.friedmanchisquare(*(wide[b] for b in wide.columns))
    return pd.DataFrame({"n_participants": [len(wide)], "chi2": [chi2], "p_value": [p]})


def compare_to_best(best, challenger, n_comparisons):
    """Paired comparison: each participant's rating of the best brand vs. a challenger."""
    paired = best[["participant", "rating"]].merge(
        challenger[["participant", "rating"]],
        on="participant", suffixes=("_best", "_challenger"),
    )
    diff = paired["rating_best"] - paired["rating_challenger"]
    p = stats.wilcoxon(paired["rating_best"], paired["rating_challenger"]).pvalue
    return pd.DataFrame({
        "n_pairs":             [len(paired)],
        "mean_diff":           [diff.mean()],
        "n_prefer_best":       [(diff > 0).sum()],
        "n_prefer_challenger": [(diff < 0).sum()],
        "p_value":             [p],
        "p_bonferroni":        [min(p * n_comparisons, 1.0)],
    })


db = scidb.configure_database("popcorn.duckdb", ["participant", "brand"])

# Import: one Rating record per (participant, brand) row
scidb.for_each(load_ratings, inputs={"csv_path": "popcorn_ratings.csv", "sep": ","}, outputs=[Rating])

# Summary per brand (pooled over participants)
summary = scidb.for_each(summarize_ratings, inputs={"ratings": Rating["rating"]},
                         outputs=[BrandSummary], brand=[])
summary = summary.sort_values("mean", ascending=False, ignore_index=True)
print(summary[["brand", "n", "mean", "median", "sd"]].round(2))

# Omnibus test: one call over every rating, saved at the dataset level
friedman = scidb.for_each(friedman_test, inputs={"ratings": Rating},
                          outputs=[FriedmanTest], as_table=True)
print(friedman[["n_participants", "chi2", "p_value"]])

# Post hoc: best brand vs. each challenger
best_brand = summary.loc[0, "brand"]
challengers = list(summary.loc[1:, "brand"])
posthoc = scidb.for_each(
    compare_to_best,
    inputs={"best": Fixed(Rating, brand=best_brand), "challenger": Rating,
            "n_comparisons": len(challengers)},
    outputs=[BestVsChallenger],
    as_table=True,
    brand=challengers,
)
print(posthoc)

# The same statistics from the study's design, read from the database
design = PlotSpec(
    measures=["Rating"],
    roles={"brand": Role.GROUP, "participant": Role.COLLAPSE},
    groups=["brand"],
    kind=PlotKind.BAR,
)
spec = StatsSpec(design=design, comparisons=[Analysis(family="nonparametric")])
tables = load_stats_tables(db, spec)
print(export_descriptives(describe_table(spec, tables), spec, "md", tables=tables))
report = run(spec, tables)
print(export_comparisons(report, spec, "md", tables=tables))
```
///

/// tab | MATLAB
```matlab
% Rating.m
classdef Rating < scidb.BaseVariable
    % One participant's 1-10 rating of one brand.
end
```

```matlab
% BrandSummary.m
classdef BrandSummary < scidb.BaseVariable
    % n, mean, median, and SD of one brand's ratings.
end
```

```matlab
% FriedmanTest.m
classdef FriedmanTest < scidb.BaseVariable
    % Do the three brands differ at all?
end
```

```matlab
% BestVsChallenger.m
classdef BestVsChallenger < scidb.BaseVariable
    % The best brand compared, participant by participant, with one other brand.
end
```

```matlab
% load_ratings.m
function ratings = load_ratings(csv_path, sep)
    % Read the whole ratings file: one row per (participant, brand).
    ratings = readtable(csv_path, Delimiter=sep, TextType="string");
end
```

```matlab
% summarize_ratings.m
function out = summarize_ratings(ratings)
    % All of one brand's ratings in, one row of summary statistics out.
    out = table(numel(ratings), mean(ratings), median(ratings), std(ratings), ...
        VariableNames=["n", "mean", "median", "sd"]);
end
```

```matlab
% friedman_test.m
function out = friedman_test(ratings)
    % All ratings in (one row per participant x brand), one row of test results out.
    wide = unstack(ratings(:, ["participant", "brand", "rating"]), "rating", "brand");
    [p, tbl] = friedman(wide{:, 2:end}, 1, "off");
    out = table(height(wide), tbl{2, 5}, p, ...
        VariableNames=["n_participants", "chi2", "p_value"]);
end
```

```matlab
% compare_to_best.m
function out = compare_to_best(best, challenger, n_comparisons)
    % Paired comparison: each participant's rating of the best brand vs. a challenger.
    best = renamevars(best(:, ["participant", "rating"]), "rating", "rating_best");
    challenger = renamevars(challenger(:, ["participant", "rating"]), "rating", "rating_challenger");
    paired = innerjoin(best, challenger, Keys="participant");
    d = paired.rating_best - paired.rating_challenger;
    p = signrank(paired.rating_best, paired.rating_challenger);
    out = table(height(paired), mean(d), sum(d > 0), sum(d < 0), p, min(p * n_comparisons, 1), ...
        VariableNames=["n_pairs", "mean_diff", "n_prefer_best", "n_prefer_challenger", ...
                       "p_value", "p_bonferroni"]);
end
```

```matlab
% best_popcorn.m
scidb.configure_database("popcorn.duckdb", ["participant", "brand"]);

% Import: one Rating record per (participant, brand) row
scidb.for_each(@load_ratings, struct("csv_path", "popcorn_ratings.csv", "sep", ","), {Rating()});

% Summary per brand (pooled over participants)
summary = scidb.for_each(@summarize_ratings, struct("ratings", Rating("rating")), ...
    {BrandSummary()}, brand=[]);
summary = sortrows(summary, "mean", "descend");
disp(summary(:, ["brand", "n", "mean", "median", "sd"]))

% Omnibus test: one call over every rating, saved at the dataset level
friedman = scidb.for_each(@friedman_test, struct("ratings", Rating()), ...
    {FriedmanTest()}, as_table=true);
disp(friedman(:, ["n_participants", "chi2", "p_value"]))

% Post hoc: best brand vs. each challenger
best_brand = string(summary.brand(1));
challengers = string(summary.brand(2:end))';
posthoc = scidb.for_each(@compare_to_best, ...
    struct("best", scidb.Fixed(Rating(), brand=best_brand), ...
           "challenger", Rating(), ...
           "n_comparisons", numel(challengers)), ...
    {BestVsChallenger()}, as_table=true, brand=challengers);
disp(posthoc)
```
///
////
