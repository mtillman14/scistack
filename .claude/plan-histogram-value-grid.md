# Plan: histogram bins on the values' spacing

Status: approved 2026-11-15 (E width 0.5 = x confirmed; MAX_SCALE_SLOTS =
200). Replaces the whole-number-only
rule that is half-written in `scistackplot/src/scistackplot/reduce/bins.py`.
The file does NOT run in its current state: the notes block still calls the
removed `_scistack_whole_numbers`.

## Problem

The numbers on a rating scale (or any fixed scale) sit on a step x. Auto,
count and width bins all ignore x, so their edges can land on values or
between them:
- Best Popcorn 1-10, auto: width 0.82, so the bin [5.09, 5.91) is empty.
- Width 1 starting at 1: the bin [9, 10] holds both the 9s and the 10s.
- 10, 20, .. 100 with whole-number edges: [90.5, 99.5) is empty.

## Finding the step x (user's algorithm)

Sort the values, drop NaN, and take the gaps between neighbours.
- A gap of 0 is a duplicate; drop it.
- Every remaining gap is k·x with k ≥ 1, so the smallest gap is d_min = k_min·x.
- Try x = d_min / m for m = 1, 2, .. 10. Accept the first x that every gap is
  a whole multiple of, within a 1e-6 relative tolerance. This is Euclid's
  gcd, but it tolerates float noise and works for decimal steps.
- No m passes: the data is continuous, x = 0, and the bins are numpy's as
  before.
- One distinct value: x = 1.
- Off on a date axis.

| data | gaps (no 0s) | d_min | m | x | ks | bar width | bars `[xmin, xmax]`: values held (auto mode) |
|---|---|---|---|---|---|---|---|
| A `1,2,2,3,4,4,4,5` complete + duplicates | 1,1,1,1 | 1 | 1 | 1 | 1,1,1,1 | 1 | `[0.5, 1.5]`: 1 · `[1.5, 2.5]`: 2,2 · `[2.5, 3.5]`: 3 · `[3.5, 4.5]`: 4,4,4 · `[4.5, 5.5]`: 5 |
| B `1,2,2,3,7,8,10` missing, min k = 1 | 1,1,4,1,2 | 1 | 1 | 1 | 1,1,4,1,2 | 1 | `[0.5, 1.5]`: 1 · `[1.5, 2.5]`: 2,2 · `[2.5, 3.5]`: 3 · `[3.5, 4.5]`: none · `[4.5, 5.5]`: none · `[5.5, 6.5]`: none · `[6.5, 7.5]`: 7 · `[7.5, 8.5]`: 8 · `[8.5, 9.5]`: none · `[9.5, 10.5]`: 10 |
| C `2,4,4,8` min k > 1, constant | 2,4 | 2 | 1 | **2** (true scale 1 is unknowable) | 1,2 | 2 | `[1, 3]`: 2 · `[3, 5]`: 4,4 · `[5, 7]`: none · `[7, 9]`: 8 |
| D `10,30,30,60,100` min k > 1, varying | 20,30,40 | 20 | 2 | 10 | 2,3,4 | 10 | `[5, 15]`: 10 · `[15, 25]`: none · `[25, 35]`: 30,30 · `[35, 45]`: none · `[45, 55]`: none · `[55, 65]`: 60 · `[65, 75]`: none · `[75, 85]`: none · `[85, 95]`: none · `[95, 105]`: 100 |
| E `1.0,2.5,2.5,4.0,5.0` half-point scale | 1.5,1.5,1.0 | 1.0 | 2 | 0.5 | 3,3,2 | 0.5 | `[0.75, 1.25]`: 1.0 · `[1.25, 1.75]`: none · `[1.75, 2.25]`: none · `[2.25, 2.75]`: 2.5,2.5 · `[2.75, 3.25]`: none · `[3.25, 3.75]`: none · `[3.75, 4.25]`: 4.0 · `[4.25, 4.75]`: none · `[4.75, 5.25]`: 5.0 |
| F `0.1,0.3,0.6` float noise | 0.19999..,0.3 | 0.2 | 2 | 0.1 | 2,3 | 0.1 | `[0.05, 0.15]`: 0.1 · `[0.15, 0.25]`: none · `[0.25, 0.35]`: 0.3 · `[0.35, 0.45]`: none · `[0.45, 0.55]`: none · `[0.55, 0.65]`: 0.6 |
| G `1.37,2.91,3.02,4.58` continuous | 1.54,0.11,1.56 | 0.11 | none of 1-10 | 0 | — | 1.07, numpy's own bins | `[1.37, 2.44]`: 1.37 · `[2.44, 3.51]`: 2.91,3.02 · `[3.51, 4.58]`: 4.58 |
| H `-3,-1,1,3` negative | 2,2,2 | 2 | 1 | 2 | 1,1,1 | 2 | `[-4, -2]`: -3 · `[-2, 0]`: -1 · `[0, 2]`: 1 · `[2, 4]`: 3 |

Also Best Popcorn 1-10: x = 1, 10 bars `[0.5, 1.5]` .. `[9.5, 10.5]`. And
10, 20, .. 100: x = 10, 10 bars `[5, 15]` .. `[95, 105]`.

D is why the smallest gap alone is not enough: x = 20 fails on the gap of 30.

How the bars column is computed (auto mode; user, 2026-11-15):
- **one bar per possible value**: on a scale, the bar width is x, so every
  slot the scale allows gets its own bar.
- **edges halfway between values**: the first edge is the lowest value − x/2,
  the last is the highest value + x/2.
- **empty bars are data**: a slot with no value is an empty bar (B's 4, 5, 6
  and 9; D's 20, 40, 50, 70-90; E's half points). These are values never
  recorded, not gaps made by the bins.
- **G's edges are numpy's**: 3.21 / 1.07 is 3 in exact arithmetic. If float
  rounding gives 3.0000000000000004, numpy makes 4 bars. The test compares
  G to `np.histogram_bin_edges` directly, never to these literals.
- **F's x**: computed as 0.19999999999999998 / 2, so it is rounded to 12
  significant digits before use and display (0.1).

### Too many slots: the fallback

When the scale has more than `MAX_SCALE_SLOTS` (200; user, 2026-11-15) possible
values between the lowest and the highest, one bar each is unreadable.
Examples: whole numbers 0-5000 (5001 slots), or data recorded to 0.01 over a
range of 100 (10,001 slots). The bar width is then numpy's auto width,
rounded UP to a multiple of x. The edges still sit halfway between values,
so every bar holds the same number of slots and none falls between two
values.

## Using x

With half = x/2 and the offset = min mod x, every edge sits half a step off
the values:
- auto: width x, one bar per slot. Above `MAX_SCALE_SLOTS` slots, numpy's
  width rounded UP to a multiple of x (the fallback above). Change from the
  half-written code, which always rounds numpy's width up.
- width: the typed width, anchored at a multiple of the width (relative to
  the grid) minus half.
- count: the range widened by half at each end.

x = 0 reproduces today's rule exactly.

## Steps

- `grid`: replace `_scistack_whole_numbers` with `_scistack_grid(values)` in
  `EDGES_SOURCE` (emitted verbatim by codegen, so the export agrees). Round x
  to 12 significant digits (example F). Rename the `whole` argument to
  `use_grid`.
- `notes`:
  - `GRID_NOTE`: "The values sit on steps of {x}, so the bin edges sit halfway
    between them."
  - `OFF_GRID_NOTE`, when a bin width is not a multiple of x: "Bins {w} wide on
    values {x} apart don't all hold the same number of values. A width that's
    a multiple of {x} does."
  - Replaces `WHOLE_NUMBER_NOTE` / `NARROW_WHOLE_NOTE`.
- `logging`: a `histogram bins: grid x=…, m=…, N distinct value(s)` line, or
  `no grid (continuous)`.
- `tests`: A-H parametrized against `_scistack_grid`, plus:
  - A-H auto edges equal the table's bars, G against numpy;
  - popcorn auto / width 1 / count 10 give one bar per rating;
  - 10..100 auto gives 10 bars of width 10;
  - the cap: 0..100 whole numbers give 101 bars of width 1; 0..200 gives
    201 slots, so it falls back;
  - 0..5000 whole numbers fall back: width a whole multiple of 1, edges at
    .5;
  - decimals off the grid keep numpy's bins;
  - the date axis is unchanged;
  - codegen parity on popcorn and 10..100.
- `docs`: rewrite the 2026-11-15 bullet in
  `docs/claude/histogram-and-regression.md`.

## Known limits

- C: sparse data shows only its own spacing (2, not 1). The bins are still
  right for the values present. A typed "value spacing" box would fix this;
  not now.
- Data recorded to fixed decimals (e.g. 0.01) gets a grid of 0.01. This is
  harmless: a 0.005 shift.
- `MAX_SCALE_SLOTS` = 200 (user, 2026-11-15). A 0-100 percentage scale (101
  slots) gets one bar per value; a 0-500 scale falls back to wider bars
  (still halfway edges).
- m is capped at 10, so a scale is missed when its smallest gap is more than
  10 steps. The data is then treated as continuous, which is today's
  behaviour.
