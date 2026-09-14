# EachOf — Generalized Variant Expansion

## Problem

Before `EachOf`, variants in `for_each()` were only created by different constant values (e.g., `low_hz=20` vs `low_hz=30`). Two other natural sources of variation had no concise expression:

1. **Multiple variable types** feeding the same input parameter (e.g., running the same statistical test on `StepLength` and `StepTime`)
2. **Multiple `where=` filters** selecting different schema-id subsets (e.g., left steps vs right steps vs all steps)

Without `EachOf`, both required separate `for_each()` calls with duplicated arguments.

## Solution

`EachOf` is a wrapper that means "iterate over these alternatives as separate variants." It can wrap any of three things:

- **Variable types** in `inputs`: `EachOf(StepLength, StepTime)`
- **Constants** in `inputs`: `EachOf(0.05, 0.01)`
- **`where=` filters**: `EachOf(Side == "L", Side == "R", None)`

Multiple `EachOf` axes combine as a cartesian product. A single `for_each()` call with all three axes:

```python
for_each(
    run_anova,
    inputs={
        "metric": EachOf(CadenceVar, StrideVelocity),
        "alpha": EachOf(0.05, 0.01),
    },
    outputs=[AnovaResult],
    where=EachOf(Where(side="L"), Where(side="R"), None),
    subject=[], session=[],
)
```

produces `2 types x 2 alphas x 3 filters = 12` variant branches, each iterating over all `(subject, session)` combinations.

## Key design properties

1. **Single-value collapse**: `EachOf(X)` behaves identically to passing `X` directly. No special case needed — there is simply one iteration of one alternative.

2. **No downstream changes**: `EachOf` is resolved at the very top of `for_each()` by expanding into multiple recursive calls, each with concrete values. All existing machinery (version_keys, branch_params, rid expansion, save/load) sees only concrete values and works unchanged.

3. **Natural discrimination**: Each axis already produces distinct records:
   - Different variable types produce different `__inputs` version keys
   - Different constants produce different `__constants` version keys and `branch_params`
   - Different `where=` filters produce different `__where` version keys

4. **Incremental**: Adding a second alternative later (e.g., going from `alpha=0.05` to `EachOf(0.05, 0.01)`) creates new records that coexist with existing ones. Nothing is overwritten.

## Implementation

- **`scifor/src/scifor/each_of.py`** — `EachOf` class (holds `alternatives` list). Moved here from scidb during the scifor/scidb modifier-class unification (see `docs/claude/scifor-scidb-modifier-unification.md`) — the container was DB-agnostic and duplicated scidb's own copy for no reason.
- **`scidb/src/scidb/foreach.py`** — expansion logic at top of `for_each()`: scans inputs and `where` for `EachOf` instances, computes cartesian product, recursively calls `for_each()` (still scidb's own, unchanged) with concrete values, concatenates results. This recursion could NOT move to scifor — each alternative needs independent save/skip_computed/lineage treatment, concepts scifor's pure loop doesn't have.
- **`scifor/src/scifor/foreach.py`** — a second, independent, simpler expansion step (mirroring the logic above minus the DB-only params) was added so standalone/no-DB `scifor.for_each()` can use `EachOf` too, for the first time. scidb's own expansion always resolves `EachOf` before anything reaches `scifor.for_each`, so this new step never actually triggers on the scidb call path — it's purely additive for standalone callers.
- **`scidb/src/scidb/__init__.py`** — `EachOf` re-exported straight from scifor (`from scifor import ... EachOf ...`), not its own class anymore.

### MATLAB bridge (added 2026-08-05)

`EachOf` was Python-only for a long time — every other modifier class
(`Fixed`, `Merge`, `ColumnSelection`, `ColName`, `PathInput`, `PathOutput`,
`Variant`, `AcrossVariants`) had a MATLAB classdef under `+scifor`/`+scidb`,
but `EachOf` did not, and neither MATLAB `for_each.m` knew how to expand
one (`scifor.EachOf(...)` raised "unable to resolve name"). This surfaced
when a real pipeline needed a `scifor.PathInput` to span two on-disk
locations (assessment-day vs. training-day folders) without changing the
shared analysis function.

The MATLAB bridge mirrors the Python dual-implementation shape above:

- **`scimatlab/src/scimatlab/matlab/+scifor/EachOf.m`** — plain builder
  (`alternatives` cell array), mirrors `+scifor/Fixed.m`'s shape. Lives only
  in `+scifor`, matching the Python precedent that `scidb.EachOf` is just a
  re-export rather than its own class.
- **`+scidb/for_each.m`** — its own Step 0 recursion, inserted *before*
  `describe_input_for_python` is called on anything (same "must be first"
  rule as Python): scans `inputs` fields (and `opts.where`) for
  `scifor.EachOf`, cartesian-products the alternatives via the existing
  `scidb.internal.cartesian_product` helper (previously only used for
  metadata combos), recursively calls `scidb.for_each` per combo, and
  `vertcat`s the branch `result_tbl`s. Needs its own copy for the same
  reason Python's scidb layer does — save/skip_computed/lineage per branch.
- **`+scifor/for_each.m`** — the standalone-parity counterpart, same idea
  but `varargout`-aware (scifor returns multiple tables via `varargout`,
  unlike scidb's single `result_tbl`): each output index is `vertcat`'d
  separately across branches.
- **Column-mismatch guard** (`vertcat_each_of_results`, duplicated as a
  local function in both `.m` files): MATLAB's table `vertcat` has no
  pandas-style NaN-union leniency for mismatched columns the way Python's
  `pd.concat` does, so branches whose alternatives resolve to different
  schema-key/metadata columns (e.g. two `PathInput` templates using
  different placeholder names) raise a named, explanatory error
  (`scidb:for_each:EachOfColumnMismatch` / `scifor:for_each:EachOfColumnMismatch`)
  instead of a raw MATLAB failure. Practical consequence: every `EachOf`
  alternative for one input must resolve to the same placeholder/schema-key
  names, even when the literal folders/`root_folder` differ.
- **Tests** — `scimatlab/tests/matlab/scifor/TestSciforEachOf.m` and
  `scimatlab/tests/matlab/scidb/TestEachOf.m`, mirroring the Python
  coverage in `scifor/tests/test_each_of.py` / `scidb/tests/test_each_of.py`.

## Relationship to existing variant machinery

`EachOf` sits above the existing variant system, not alongside it. The hierarchy:

```
EachOf expansion (top of for_each — recursive decomposition)
  |
  v
ForEachConfig / version_keys (constants, inputs, where serialization)
  |
  v
rid expansion / branch_params (upstream variant tracking across pipeline steps)
  |
  v
save / load / list_versions (DB-level record discrimination)
```

Each layer is unaware of `EachOf`. By the time any downstream code runs, it sees a normal `for_each()` call with concrete values.
