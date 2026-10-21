# Best Popcorn - scifor

One hundred shoppers in a grocery store each tasted three brands of popcorn and rated each one from 1 (awful) to 10 (amazing). Which brand is best, and is the difference big enough to be more than chance?

This walkthrough answers that question with `scifor` alone: no database and no project setup. You load a CSV, describe how it is organized, and let `scifor.for_each` handle the looping. Every step has Python and MATLAB versions.

**What you'll learn**

- How to choose a **schema** for a real dataset.
- How `for_each` slices a table for you, and how iterating over _some_ of the schema keys **aggregates** over the rest.
- How to compare every condition against one fixed condition with **`Fixed`**.
- How **`as_table`** lets your function see the schema columns when it needs them.
- How **`scistackstats`** picks and runs the tests from a description of the study, the same way Stat Studio does in the GUI.

## The data

The dataset has 300 rows: 100 participants × 3 brands. It is in _long_ format, meaning one row per rating:

| participant | brand       | rating |
| ----------- | ----------- | ------ |
| P001        | Butterburst | 5      |
| P001        | KettleKing  | 5      |
| P001        | SeaSaltCo   | 4      |
| P002        | Butterburst | 7      |
| …           | …           | …      |

**Download:** [popcorn_ratings.csv](data/popcorn_ratings.csv){ download="popcorn_ratings.csv" }

If the download doesn't work for you, expand the box below, copy the contents (the copy button is in the top-right corner of the code block), and save them as `popcorn_ratings.csv`.

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

Make a new folder and put `popcorn_ratings.csv` in it. Create a script file in the same folder (`best_popcorn.py` or `best_popcorn.m`) and run everything from there.

Besides SciStack, you need one statistics library:

/// tab | Python
[SciPy](https://scipy.org/) provides the statistical tests. Install it with `pip install scipy`. pandas and NumPy are already installed with SciStack.
///

/// tab | MATLAB
The statistical tests (`friedman`, `signrank`) come from the **Statistics and Machine Learning Toolbox**. MATLAB R2021a or newer is needed for the `name=value` argument syntax used below.
///

## Step 2: Load the data and declare the schema

First, read the CSV into a table.

/// tab | Python

```python
import numpy as np
import pandas as pd
from scipy import stats
from scifor import set_schema, for_each, Fixed

ratings = pd.read_csv("popcorn_ratings.csv")
print(ratings.head())
```

///

/// tab | MATLAB

```matlab
ratings = readtable("popcorn_ratings.csv", TextType="string");
head(ratings)
```

///

Next, tell `scifor` how the data is organized. The [schema](../getting-started/whats-a-schema.md) lists the columns that identify each row, which here are **who** gave the rating and **which brand** they rated. Every `(participant, brand)` pair identifies exactly one rating. `rating` is not part of the schema because it is the measured value (a _variable_), not part of how the data is organized.

/// tab | Python

```python
set_schema(["participant", "brand"])
```

///

/// tab | MATLAB

```matlab
scifor.set_schema(["participant", "brand"]);
```

///

## Step 3: Summarize each brand

Before any statistical test, look at the numbers. The goal is a mean, median, and standard deviation for each brand.

Without SciStack, you would write a loop: for each brand, filter the table to that brand's rows, compute the statistics, and append a row to a results table. With `for_each`, you write only the middle part, a function that receives **one brand's ratings** and returns **one row of statistics**:

/// tab | Python

```python
def summarize_ratings(ratings):
    """All of one brand's ratings in, one row of summary statistics out."""
    r = ratings.ravel()
    return pd.DataFrame({
        "n":      [len(r)],
        "mean":   [r.mean()],
        "median": [np.median(r)],
        "sd":     [r.std(ddof=1)],
    })
```

///

/// tab | MATLAB
Local functions must go at the **bottom** of a MATLAB script, so add this one at the end of `best_popcorn.m`:

```matlab
function out = summarize_ratings(ratings)
    % All of one brand's ratings in, one row of summary statistics out.
    out = table(numel(ratings), mean(ratings), median(ratings), std(ratings), ...
        VariableNames=["n", "mean", "median", "sd"]);
end
```

///

The function knows nothing about brands, participants, or the CSV. It takes numbers and returns numbers, so you can test it on its own with any array of ratings.

Now hand it to `for_each`:

/// tab | Python

```python
summary = for_each(
    summarize_ratings,
    inputs={"ratings": ratings},   # function argument name -> table
    brand=[],                      # [] means "every brand in the data"
)
summary = summary.sort_values("mean", ascending=False, ignore_index=True)
print(summary.round(2))
```

///

/// tab | MATLAB

```matlab
summary = scifor.for_each(@summarize_ratings, ...
    struct("ratings", ratings), ...   % function argument name -> table
    brand=[]);                         % [] means "every brand in the data"
summary = sortrows(summary, "mean", "descend");
disp(summary)
```

///

Here is what `for_each` did:

1. **Found the combinations to run.** You asked to iterate over `brand`, and `[]` means "use every value that appears in the input table", so it ran three combinations: `Butterburst`, `KettleKing`, and `SeaSaltCo`.
2. **Sliced the input for each combination.** For `brand="Butterburst"` it kept only the Butterburst rows and dropped the schema columns (`participant`, `brand`). Only the `rating` column was left, so your function received a plain column of 100 numbers.
3. **Aggregated over the key you didn't name.** The schema has two keys, but you only iterated over `brand`. Each call therefore got **all participants** for that brand. Iterating over a subset of the schema keys is how you aggregate in SciStack: you list the keys you want to keep, and the rest are pooled.
4. **Assembled the results.** Each one-row table the function returned was stacked into a single table, with a `brand` column added so you can tell the rows apart.

You will also see a few log lines from `scifor` summarizing the run (combinations completed and failed). You should get:

| brand       | n   | mean | median | sd   |
| ----------- | --- | ---- | ------ | ---- |
| Butterburst | 100 | 7.30 | 7      | 1.80 |
| KettleKing  | 100 | 6.35 | 6      | 1.69 |
| SeaSaltCo   | 100 | 5.48 | 6      | 1.71 |

Butterburst has the highest average rating, but the spreads (standard deviations of about 1.7–1.8) are about as large as the gaps between the brands. That isn't enough to call a winner, so the next step is a statistical test.

/// tip | Preview before running
Add `dry_run=True` to any `for_each` call to print the combinations it would run and the data each one would receive, without calling your function. It's useful when a result looks wrong and you want to check how the data is being sliced.
///

## Step 4: Is there any difference at all?

The first question is the overall one: **do the three brands differ at all?** Comparing brands two at a time comes after that, and only if the overall answer is yes.

Two features of the study determine which test to use:

- **Repeated measures.** Every participant rated every brand, so the three groups of ratings are not independent. Someone who loves popcorn rates all three brands high. The test should compare each person's ratings with each other, not pool everyone together.
- **Ordinal ratings.** A 1–10 taste score is a ranking, not a physical measurement. A 6 is better than a 5, but the step from 5 to 6 isn't guaranteed to equal the step from 9 to 10.

The **Friedman test** fits both. It ranks the three brands within each participant, then asks whether some brand is ranked consistently higher than the others across participants. It is the rank-based version of a repeated-measures ANOVA.

This step doesn't need `for_each`, because there is nothing to loop over: it is a single test on the whole dataset. `scifor` is for when a function has to run once per combination, and a one-off calculation can just be called directly.

The test needs the data in _wide_ format, with one row per participant and one column per brand:

/// tab | Python

```python
wide = ratings.pivot(index="participant", columns="brand", values="rating")
chi2, p = stats.friedmanchisquare(*(wide[b] for b in wide.columns))
print(f"Friedman: chi2(2) = {chi2:.2f}, p = {p:.2g}")
```

///

/// tab | MATLAB

```matlab
wide = unstack(ratings, "rating", "brand");   % participant + one column per brand
[p, tbl] = friedman(wide{:, 2:end}, 1, "off");
fprintf("Friedman: chi2(2) = %.2f, p = %.2g\n", tbl{2, 5}, p);
```

///

```text
Friedman: chi2(2) = 58.08, p = 2.4e-13
```

The p-value is far below the usual 0.05 cutoff, so **the brands are not all equally liked**. The next step is to find out which brand stands out.

## Step 5: Which one is best?

Butterburst had the highest mean, so the question becomes: **is Butterburst rated higher than each of the other brands?** That means two comparisons, Butterburst vs. KettleKing and Butterburst vs. SeaSaltCo.

Each comparison pairs up the two ratings that one participant gave and runs a **Wilcoxon signed-rank test** on those pairs. It is the paired, rank-based counterpart of the paired t-test, chosen for the same reasons as the Friedman test.

This is a job for `for_each`: the same function runs once per challenger brand, and one of its inputs (Butterburst's ratings) stays the same every time. **`Fixed`** handles that input. It wraps a table and pins one or more schema keys to a value, so this input ignores the current iteration's `brand` and always gets Butterburst's rows.

The comparison function needs to know _which participant_ each rating came from so that it can line up the pairs. By default `for_each` removes the schema columns before calling the function. **`as_table=True`** keeps them, and the function receives filtered tables with `participant`, `brand`, and `rating` columns:

/// tab | Python

```python
def compare_to_best(best, challenger):
    """Paired comparison: each participant's rating of the best brand vs. a challenger."""
    paired = best.merge(challenger, on="participant", suffixes=("_best", "_challenger"))
    diff = paired["rating_best"] - paired["rating_challenger"]
    p = stats.wilcoxon(paired["rating_best"], paired["rating_challenger"]).pvalue
    return pd.DataFrame({
        "n_pairs":             [len(paired)],
        "mean_diff":           [diff.mean()],
        "n_prefer_best":       [(diff > 0).sum()],
        "n_prefer_challenger": [(diff < 0).sum()],
        "p_value":             [p],
    })
```

///

/// tab | MATLAB
Add this function at the bottom of the script as well:

```matlab
function out = compare_to_best(best, challenger)
    % Paired comparison: each participant's rating of the best brand vs. a challenger.
    best = renamevars(best(:, ["participant", "rating"]), "rating", "rating_best");
    challenger = renamevars(challenger(:, ["participant", "rating"]), "rating", "rating_challenger");
    paired = innerjoin(best, challenger, Keys="participant");
    d = paired.rating_best - paired.rating_challenger;
    p = signrank(paired.rating_best, paired.rating_challenger);
    out = table(height(paired), mean(d), sum(d > 0), sum(d < 0), p, ...
        VariableNames=["n_pairs", "mean_diff", "n_prefer_best", "n_prefer_challenger", "p_value"]);
end
```

///

Joining on `participant`, instead of assuming both tables are in the same row order, guarantees that each pair really is the same person's two ratings.

Now run it once for each challenger. The challengers are every brand except the best, taken from the sorted summary in Step 3:

/// tab | Python

```python
best_brand = summary.loc[0, "brand"]
challengers = list(summary.loc[1:, "brand"])

posthoc = for_each(
    compare_to_best,
    inputs={
        "best":       Fixed(ratings, brand=best_brand),  # always Butterburst
        "challenger": ratings,                           # the current brand
    },
    as_table=True,
    brand=challengers,
)

# Two tests were run, so correct for multiple comparisons (Bonferroni).
posthoc["p_bonferroni"] = (posthoc["p_value"] * len(posthoc)).clip(upper=1)
print(posthoc)
```

///

/// tab | MATLAB

```matlab
best_brand = string(summary.brand(1));
challengers = string(summary.brand(2:end))';

posthoc = scifor.for_each(@compare_to_best, ...
    struct("best",       scifor.Fixed(ratings, brand=best_brand), ...  % always Butterburst
           "challenger", ratings), ...                               % the current brand
    as_table=true, brand=challengers);

% Two tests were run, so correct for multiple comparisons (Bonferroni).
posthoc.p_bonferroni = min(posthoc.p_value * height(posthoc), 1);
disp(posthoc)
```

///

For `brand="KettleKing"`, the `challenger` input gets the KettleKing rows, while the `best` input, because of `Fixed`, gets the Butterburst rows. The same happens for `SeaSaltCo`. The result has one row per challenger:

| brand      | n_pairs | mean_diff | n_prefer_best | n_prefer_challenger | p_value | p_bonferroni |
| ---------- | ------- | --------- | ------------- | ------------------- | ------- | ------------ |
| KettleKing | 100     | 0.95      | 53            | 19                  | ≈ 2e-06 | ≈ 4e-06      |
| SeaSaltCo  | 100     | 1.82      | 72            | 11                  | < 1e-11 | < 1e-11      |

(Participants who gave both brands the same score count toward neither preference column.)

/// note | Why the p-values differ slightly between Python and MATLAB
Both languages use a normal approximation for the signed-rank test at this sample size, but MATLAB's `signrank` applies a continuity correction and SciPy's `wilcoxon` doesn't by default. The p-values differ in their later digits, and the conclusion is the same.
///

The **Bonferroni correction** multiplies each p-value by the number of comparisons. Running several tests increases the chance that at least one is "significant" by luck alone, and the correction compensates for that. Both comparisons remain far below 0.05 after correction.

## Step 6: The same statistics from the study's design

Steps 4 and 5 took some work. You chose each test, reshaped the data for it, paired the ratings by hand, and applied the correction yourself. `scistackstats`, which comes with SciStack, does all of that from one description of the study. It's the same engine that runs [Stat Studio](https://scistack.readthedocs.io/en/latest/ui/gui/summarize/) in the GUI, so code and GUI give the same numbers.

You describe the study the way you would describe a figure of it. **Group** by `brand`, because that's what you're comparing. **Collapse** `participant`, because each bar summarises all the participants. From those two roles, scistackstats works out the rest:

- **The unit.** The collapsed key, `participant`, is the independent unit, with n = 100.
- **Within or between.** Every participant rated every brand, so `brand` is a repeated measure (within-subject).
- **The tests.** A within-subject factor with three levels gets the Friedman test overall and paired tests for each pair.

`family="nonparametric"` asks for rank-based tests, for the same reason as in Step 4: the ratings are scores, not measurements.

/// tab | Python

```python
from scistackplot import LongTable, PlotKind, PlotSpec, Role
from scistackstats import Analysis, StatsSpec, describe_table, run
from scistackstats import export_comparisons, export_descriptives

design = PlotSpec(
    measures=["rating"],
    roles={"brand": Role.GROUP, "participant": Role.COLLAPSE},
    groups=["brand"],
    kind=PlotKind.BAR,
)
spec = StatsSpec(design=design, comparisons=[Analysis(family="nonparametric")])

tables = {"rating": LongTable.from_frame(
    ratings,
    factors=["participant", "brand"],
    measures=["rating"],
    schema_levels=["participant", "brand"],   # the schema, in order
)}

print(export_descriptives(describe_table(spec, tables), spec, "md", tables=tables))
report = run(spec, tables)
print(export_comparisons(report, spec, "md", tables=tables))
```

///

/// tab | MATLAB
`scistackstats` is a Python package, and there's no MATLAB version of it yet. To get these tables from MATLAB data, save the table with `writetable(ratings, "popcorn_ratings.csv")` and run the Python code on that file.
///

`describe_table` gives the descriptive table (each brand's mean (SD) and n), and `run` runs every comparison. `export_comparisons` writes both as Markdown. Other formats are `"txt"`, `"csv"`, `"latex"`, `"html"` (which pastes into Word as a table) and `"json"`. Here is the comparisons table it prints:

| Comparison | Δ [95% CI] | Statistic | p | p (adj.) | Rank-biserial r | n |
|---|---|---|---|---|---|---|
| Butterburst − KettleKing | 1.00 [0.50, 1.00] | W = 479.00 | < .001 | < .001 | 0.64 | 100 |
| Butterburst − SeaSaltCo | 2.00 [1.50, 2.00] | W = 178.50 | < .001 | < .001 | 0.90 | 100 |
| KettleKing − SeaSaltCo | 1.00 [0.50, 1.00] | W = 631.50 | < .001 | < .001 | 0.60 | 100 |

The overall test below it matches Step 4, χ²(2) = 58.08, p < .001, now with an effect size (Kendall's W = 0.29). A few things differ from Steps 4 and 5:

- **Every pair is compared**, including KettleKing against SeaSaltCo, not just the best brand against the rest.
- **Holm instead of Bonferroni.** Holm controls false alarms just as well, but misses fewer real differences. You can choose `Analysis(correction="bonferroni")` instead.
- **Each difference has a size and a range.** Δ is the Hodges–Lehmann shift, the typical difference between the two brands' ratings, with its 95% confidence interval. The effect size is named exactly (rank-biserial *r*), so a reader knows which formula was used.
- **The table explains itself.** The note under it states the tests, the unit, n and the correction, and lists anything the tests had to handle, such as the 28 participants who gave Butterburst and KettleKing the same score.

/// note | What if you don't ask for rank-based tests?
By default (`Analysis()`), scistackstats checks the data and picks tests from what it finds. These ratings look bell-shaped enough (Shapiro–Wilk p = 0.31), so it would pick a repeated-measures ANOVA and paired t-tests, and say why in the report. The conclusion is the same here. Asking for rank-based tests explicitly is the better choice for 1–10 scores, because a score of 6 vs 5 isn't guaranteed to be the same step as 10 vs 9.
///

## Conclusion

The analysis supports a write-up like this:

> Taste ratings differed across the three brands (Friedman χ²(2) = 58.1, p < 0.001). Butterburst (mean 7.3/10) was rated higher than both KettleKing (mean 6.4; Wilcoxon signed-rank, Bonferroni-corrected p < 0.001) and SeaSaltCo (mean 5.5; p < 0.001). 53 of 100 participants preferred Butterburst to KettleKing, and 72 of 100 preferred it to SeaSaltCo.

**Butterburst is the best popcorn.**

Here are the `scifor` features this walkthrough used:

| Concept                                                              | Where it appeared                           |
| -------------------------------------------------------------------- | ------------------------------------------- |
| `set_schema` names the columns that identify a row                   | `["participant", "brand"]`                  |
| `for_each` slices each input per combination and calls your function | Steps 3 and 5                               |
| `key=[]` iterates over every value present in the data               | `brand=[]`                                  |
| Iterating over a subset of the keys aggregates over the rest         | Summary per brand, pooled over participants |
| Returning a one-row table adds columns to the result                 | `summary`, `posthoc`                        |
| `Fixed(table, key=value)` pins an input to one condition             | `best` is always Butterburst                |
| `as_table=True` keeps the schema columns for your function           | Pairing ratings by `participant`            |
| `scistackstats` runs the statistics from the design (group / collapse) | Step 6: Friedman + every pair, Holm       |

## The complete script

//// details | Full script (click to expand)
type: example

/// tab | Python

```python
# best_popcorn.py
import numpy as np
import pandas as pd
from scipy import stats
from scifor import set_schema, for_each, Fixed
from scistackplot import LongTable, PlotKind, PlotSpec, Role
from scistackstats import Analysis, StatsSpec, describe_table, run
from scistackstats import export_comparisons, export_descriptives


def summarize_ratings(ratings):
    """All of one brand's ratings in, one row of summary statistics out."""
    r = ratings.ravel()
    return pd.DataFrame({
        "n":      [len(r)],
        "mean":   [r.mean()],
        "median": [np.median(r)],
        "sd":     [r.std(ddof=1)],
    })


def compare_to_best(best, challenger):
    """Paired comparison: each participant's rating of the best brand vs. a challenger."""
    paired = best.merge(challenger, on="participant", suffixes=("_best", "_challenger"))
    diff = paired["rating_best"] - paired["rating_challenger"]
    p = stats.wilcoxon(paired["rating_best"], paired["rating_challenger"]).pvalue
    return pd.DataFrame({
        "n_pairs":             [len(paired)],
        "mean_diff":           [diff.mean()],
        "n_prefer_best":       [(diff > 0).sum()],
        "n_prefer_challenger": [(diff < 0).sum()],
        "p_value":             [p],
    })


# Load + schema
ratings = pd.read_csv("popcorn_ratings.csv")
set_schema(["participant", "brand"])

# Summary per brand (pooled over participants)
summary = for_each(summarize_ratings, inputs={"ratings": ratings}, brand=[])
summary = summary.sort_values("mean", ascending=False, ignore_index=True)
print(summary.round(2))

# Omnibus test: do the brands differ at all?
wide = ratings.pivot(index="participant", columns="brand", values="rating")
chi2, p = stats.friedmanchisquare(*(wide[b] for b in wide.columns))
print(f"Friedman: chi2(2) = {chi2:.2f}, p = {p:.2g}")

# Post hoc: best brand vs. each challenger
best_brand = summary.loc[0, "brand"]
challengers = list(summary.loc[1:, "brand"])
posthoc = for_each(
    compare_to_best,
    inputs={"best": Fixed(ratings, brand=best_brand), "challenger": ratings},
    as_table=True,
    brand=challengers,
)
posthoc["p_bonferroni"] = (posthoc["p_value"] * len(posthoc)).clip(upper=1)
print(posthoc)

# The same statistics from the study's design (scistackstats)
design = PlotSpec(
    measures=["rating"],
    roles={"brand": Role.GROUP, "participant": Role.COLLAPSE},
    groups=["brand"],
    kind=PlotKind.BAR,
)
spec = StatsSpec(design=design, comparisons=[Analysis(family="nonparametric")])
tables = {"rating": LongTable.from_frame(
    ratings, factors=["participant", "brand"], measures=["rating"],
    schema_levels=["participant", "brand"],
)}
print(export_descriptives(describe_table(spec, tables), spec, "md", tables=tables))
report = run(spec, tables)
print(export_comparisons(report, spec, "md", tables=tables))
```

///

/// tab | MATLAB

```matlab
% best_popcorn.m

% Load + schema
ratings = readtable("popcorn_ratings.csv", TextType="string");
scifor.set_schema(["participant", "brand"]);

% Summary per brand (pooled over participants)
summary = scifor.for_each(@summarize_ratings, struct("ratings", ratings), brand=[]);
summary = sortrows(summary, "mean", "descend");
disp(summary)

% Omnibus test: do the brands differ at all?
wide = unstack(ratings, "rating", "brand");
[p, tbl] = friedman(wide{:, 2:end}, 1, "off");
fprintf("Friedman: chi2(2) = %.2f, p = %.2g\n", tbl{2, 5}, p);

% Post hoc: best brand vs. each challenger
best_brand = string(summary.brand(1));
challengers = string(summary.brand(2:end))';
posthoc = scifor.for_each(@compare_to_best, ...
    struct("best", scifor.Fixed(ratings, brand=best_brand), "challenger", ratings), ...
    as_table=true, brand=challengers);
posthoc.p_bonferroni = min(posthoc.p_value * height(posthoc), 1);
disp(posthoc)

% --- Local functions (must be at the end of a script) ---

function out = summarize_ratings(ratings)
    % All of one brand's ratings in, one row of summary statistics out.
    out = table(numel(ratings), mean(ratings), median(ratings), std(ratings), ...
        VariableNames=["n", "mean", "median", "sd"]);
end

function out = compare_to_best(best, challenger)
    % Paired comparison: each participant's rating of the best brand vs. a challenger.
    best = renamevars(best(:, ["participant", "rating"]), "rating", "rating_best");
    challenger = renamevars(challenger(:, ["participant", "rating"]), "rating", "rating_challenger");
    paired = innerjoin(best, challenger, Keys="participant");
    d = paired.rating_best - paired.rating_challenger;
    p = signrank(paired.rating_best, paired.rating_challenger);
    out = table(height(paired), mean(d), sum(d > 0), sum(d < 0), p, ...
        VariableNames=["n_pairs", "mean_diff", "n_prefer_best", "n_prefer_challenger", "p_value"]);
end
```

///
////

## Where to go next

The results in this walkthrough exist only in memory: close Python or MATLAB and they're gone, and nothing records which function and settings produced each number. That's fine for a quick analysis. For a project that grows (more studies, more brands, a re-run after fixing a bug), the next layer, `scidb`, uses the same `for_each` but saves each result to a database and records how it was produced. [Best Popcorn - scidb](best-popcorn-scidb.md) repeats this analysis with it.
