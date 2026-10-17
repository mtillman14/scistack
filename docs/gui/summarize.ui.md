# Summarize: Stat Studio

Stat Studio produces the tables that go in a paper: descriptive statistics, such as mean (SD) per condition, and statistical comparisons with effect sizes and confidence intervals. You don't write any code, and you don't need to reshape your data for a statistics package.

Stat Studio uses the **same design as your figure**. The roles you set in Plot Studio (what's grouped, what's collapsed) already describe your study: which factors you're comparing, and who your participants are. Stat Studio reads that design, so the numbers in your table always describe the same data as the bars in your figure.

/// note | Statistics in SciStack are in beta
All the statistical calculations are done by the [csv-stats](https://pypi.org/project/csv-stats/) package. When a test isn't available there yet, Stat Studio says *not yet in csv-stats* rather than switching to a different calculation without telling you. More tests are being added.
///

/// info | 📷 Screenshot
**Show:** Stat Studio with the left rail open and the Comparisons tab showing a results table, the "Results as sentences" list beneath it.
///

## Opening Stat Studio

Open a figure in [Plot Studio](visualize.md) and click **StatStudio** in its toolbar. Stat Studio opens in its own tab, already set up from the figure: the same variable, the same grouping, the same collapsed keys, and a summary that matches the error bars (a figure with SD error bars starts with a mean (SD) table).

If the figure is a **saved plot**, the table and the figure are **linked**, and they can be kept in step (see [Figures and tables together](#figures-and-tables-together)). Save the figure first if you want this link.

## How the window is laid out

| Area | What's there |
|---|---|
| **Left rail** | The settings, in sections: **Measures**, **Design**, **Descriptives**, **Comparisons**, **Format**, **Brackets on the figure** and **Save**. |
| **Results** (right) | Two tabs, **Descriptives** and **Comparisons**, with the export controls above them. A banner at the top appears if the table's design has drifted from its linked figure. |

The results update as you change settings.

## Measures: what's being analysed

A **measure** is a variable you want statistics for, such as `Rating` or `StepLength`. One table can hold several measures, analysed one after another with the same design. Choose one from **Add a measure…** and click **Add**.

If a variable is a table with several columns, such as a CSV with one column per outcome, tick the columns you want. Each column becomes its own measure.

## Design: what you're comparing, and who your participants are

The design uses the same roles as Plot Studio, under names that suit a table:

| In Stat Studio | In Plot Studio | What it means for the statistics |
|---|---|---|
| **Group (columns)** | Grouping | A factor you're comparing. Its levels become the table's columns. |
| **Separate tables** | Separate figures | A complete analysis for each level, such as one per site. |
| **Separate sections** | Separate panels | The same, laid out as sections of one table. |
| **Collapse (average)** | Collapse | Averaged away before testing. The last collapsed key is your **unit of analysis**. |

Under the roles, Stat Studio shows what it worked out from your data:

- **The unit of analysis and *n***, for example *Unit: participant; trial averaged first; n = 12*. This is what each test counts as one independent observation.
- **Whether each factor is within or between.** If every participant appears at every level of `brand`, then `brand` is **within** (a repeated measure). If each participant appears at only one level of `group`, then `group` is **between**. If the guess is wrong for your study, you can override it.

/// tip | Why the unit of analysis matters
Suppose each participant did ten trials and you test on all the trials. The test then treats ten measurements from one person as ten independent people, which makes results look far more certain than they are. This is called **pseudoreplication**. Collapsing `trial` gives one value per participant, and Stat Studio warns you when nothing has been collapsed down to a unit.
///

With two or more grouped keys, the **innermost** one (moved with the arrows) is the **primary comparison**. Its levels are compared within each level of the outer keys. For example, `pre` is compared with `post` inside each group.

## Descriptives

Choose what each cell shows with **Cell**: **Mean (SD)**, **Mean (SEM)**, **Mean [95% CI]**, **Median [IQR]**, or nothing. Tick any extra statistics you want as their own columns: n, mean, median, SD, SEM, 95% CI, IQR, quartiles, minimum, maximum, range, coefficient of variation, skewness, kurtosis and percentiles.

The 95% CI is calculated the same way as the 95% CI error bars in Plot Studio, so a figure and its table always agree.

## Comparisons

An **analysis** is a set of choices about how to test. Most tables need one. Click **+ Analysis** to add another, for example to report a parametric and a nonparametric analysis side by side.

| Setting | What it does |
|---|---|
| **parametric / nonparametric / spm** | Which family of tests to use. Nonparametric tests compare ranks instead of means, so they suit ratings, scores and skewed data. |
| **Pairwise comparisons (primary)** | Compare every pair of levels of the primary factor. |
| **Omnibus (main effects + interactions)** | One overall test of whether the levels differ at all. |
| **Pairwise test** / **Omnibus test** | Leave as **From the design** and Stat Studio picks the right test from what it worked out (see below). Or pick one yourself. |
| **Assume equal variances** | Use Student's t-test instead of Welch's. Welch's is the safer default. |
| **Correction** / **Family** | How p values are adjusted for running several comparisons (**Holm** by default), and which comparisons count as one family. |
| **Effect size** | Which effect size to report. Each one is named exactly, such as *d_z (paired)* rather than just "Cohen's d", so readers know which formula was used. |
| **Difference spread** | How to describe the uncertainty of each difference. The default is the confidence interval of the difference itself, not the SD of each group. |
| **α**, **CI** | The significance level and the confidence level. |

When the tests are left as **From the design**, they're chosen like this:

| | Parametric | Nonparametric |
|---|---|---|
| **Pairwise**, factor is within (paired) | Paired t-test | Wilcoxon signed-rank |
| **Pairwise**, factor is between | Welch's t-test (Student's if you assume equal variances) | Mann–Whitney U |
| **Omnibus**, one within factor | ANOVA | Friedman |
| **Omnibus**, one between factor | ANOVA | Kruskal–Wallis |
| **Omnibus**, two or more factors | ANOVA | None yet; the pairwise rank tests still run |

Stat Studio never changes a test without telling you. If a test can't be used with your design, it shows the reason. For example, Friedman needs three or more levels, and with two levels the Wilcoxon test is the right one.

There's also an **spm** family for curves, such as gait cycles: it compares whole curves point by point (statistical parametric mapping). Like the other tests, it depends on what csv-stats supports so far.

## Reading the results

The **Comparisons** tab shows a table for each analysis. Each row is one comparison, with the difference and its direction (*B − A*), the confidence interval of the difference, the test statistic, the p value and the adjusted p value, and the effect size. The omnibus test, if you asked for it, has a table of its own.

Under each table, **Results as sentences** says the same thing in words, ready to adapt for a results section. Click **Copy sentences** to copy them.

Every table also has a **caption** that states what a cell is, the unit of analysis and *n*, the tests, the correction, and the effect-size definition. The caption is included in every export, so the table always says how it was made.

## Formatting and exporting

- **Format.** The number of decimal places (automatic per measure by default), decimal places for p values, and optional significance stars on the adjusted p values.
- **Export.** Choose a format, then click **Copy** or **Save to file…**. **html** pastes into Word as a real table with borders. **md**, **csv**, **latex**, **txt** and **json** are there for everything else.

## Figures and tables together

When a table is linked to a saved figure, you can explore in either one. If they drift apart, for example because you grouped by something new in Plot Studio, both show a banner listing what's different, with a button to copy the design across: **Sync to PlotStudio** in Stat Studio, or **Sync to StatStudio** in Plot Studio.

Only the **design** is synced: the variables, roles, filters, versions and level order. Figure styling (colours, sizes, plot type) and test settings are left alone.

**Brackets on the figure.** Tick **Draw test brackets on the linked figure**, choose which pairs (for example, only the significant ones) and whether to label them with stars or adjusted p values, then save the table. The figure then draws the brackets from the test results. You can hide them, and they update when the results do.

/// info | 📷 Screenshot
**Show:** a bar chart with test brackets drawn from Stat Studio, next to the Stat Studio tab that produced them.
///

## Saving and reusing

- **Save.** Name the table and click **Save**. Saved tables are listed underneath; click one to reopen it. Each save adds a new version.
- **Presets.** **Save settings as preset** keeps every setting except the measures. Apply it to another table so all your outcomes get the same treatment.

## Adding the analysis to your pipeline

Click **Add to pipeline** to turn the table into a `stat_` step in your project. It then appears on the canvas like any other step. From then on, the results are saved in the database with a record of where they came from. They turn red when their data changes, and they're included in **📄 Report**.

## Next steps

- [Best Popcorn](../examples/best-popcorn.md): a complete analysis from CSV to figure to statistics.
- [Visualize](visualize.md): more on the roles and the figure that Stat Studio starts from.
