# Plan: `scidb.Variant(...)` — per-input branch_param pinning for for_each / load

Status: **APPROVED DESIGN — ready to implement** (drafted 2026-05-30)

## Goal

Let a user pin a for_each input (or a `.load()`) to a specific **branch_param
variant**, composably with the existing input wrappers (`Fixed`, `Merge`,
`ColumnSelection`). Example:

```python
# Run fn over only the low_hz=20 variant of FilteredEMG
for_each(fn, {"x": Variant(FilteredEMG, low_hz=20)}, [Out], subject=[1,2])

# Combined with the other wrappers
Merge(Variant(Fixed(StepLength["GR"], session="BL"), low_hz=20), SubjectGrouping)
```

This also **fixes aggregation-mode variant smushing**: pinning an input to one
variant means variant expansion (Step 12) only sees matching records, so an
aggregation no longer pools multiple distinct variants into one table.

## Core design principle

branch_param pinning is an **orthogonal, load-time filter**, distinct from the other
wrappers' concerns:

| Wrapper | Concern | Acts at |
|---|---|---|
| `ColumnSelection` (`MyVar["col"]`) | which columns | after load |
| `Fixed(…, session="BL")` | which schema metadata (override the combo) | per-combo, scifor loop |
| `Merge(…)` | join several inputs | top level |
| **`Variant(…, low_hz=20)`** (new) | which branch_param variant | **load time** |

`where=` is already threaded as a parameter through the whole `_load_input` recursion
(`_load_input(spec, db, where)`; Fixed/ColumnSelection/Merge each recurse with it).
**`Variant` does the same with a `branch_params_filter`** — it is simply the wrapper
that *injects* that parameter into its subtree. Each wrapper passes it through to its
inner load; the leaf `load_all_as_df(branch_params_filter=…)` applies it (reusing the
existing `_match_branch_param`). Because it's threaded rather than wrapper-aware,
**composition is order-agnostic** — no N×M wrapper combinations.

## Why load-time, not per-combo (important)

`Variant` must filter *before* loading, unlike `Fixed` (which loads the full table and
overrides metadata per-combo in scifor). Reason: branch_params are **stripped** from
the DataFrame during variant tracking (Step 11, after `rid_to_bp` is built), so they
are not available at scifor's per-combo filter stage. Filtering at load time is also
cheaper and makes variant expansion see only the matching variant(s).

## Composition rules

- `Variant` may wrap: a variable type, a `ColumnSelection`, or a `Fixed`. Both orders
  with `Fixed` are valid and load identically (`Fixed(Variant(X, low_hz=20), session="BL")`
  ≡ `Variant(Fixed(X, session="BL"), low_hz=20)`).
- `Variant` may be a `Merge` constituent: `Merge(Variant(A, low_hz=20), B)`. This is the
  primary multi-input case — **per-constituent** pinning.
- **`Variant(Merge(…))` is an ERROR** — mirror `Fixed(Merge(…))`. branch_params are
  namespaced per producing function (`bandpass.low_hz`), so one `low_hz=20` cannot
  sensibly broadcast across constituents with different namespaces (would match nothing
  on the others and silently drop all their rows). Pin per constituent instead.
- Nested `Variant(Variant(…))`: merge the dicts; **raise on key conflict**.

### EachOf composition (verified — works for free)

`EachOf` is a **call-level multiplexer** handled in Step 1 *before* input loading
(`foreach.py:244-296`): for each combination of alternatives it substitutes the
alternative into `concrete_inputs[param]` (or `concrete_where`) and makes a **recursive
`for_each` call**, concatenating results. Because the recursive call runs the normal
pipeline, an alternative can be any valid input spec — including a `Variant`. So:

```python
# Run once per pinned variant, results concatenated — NO extra code needed
EachOf(Variant(FilteredEMG, low_hz=20), Variant(FilteredEMG, low_hz=50))
```

Distinct output identity comes from `Variant.to_key()` → `__inputs` version key *and*
from the differing upstream branch_params propagated to outputs.

**Rule: `EachOf` is always the outermost wrapper.** Step 1 only detects `EachOf` as the
direct value of an input param or of `where=`; it does NOT look inside
`Merge`/`Fixed`/`ColumnSelection`/`Variant`. So `Variant`/`Fixed`/`Merge` nest *inside*
each `EachOf` alternative, never the reverse. `Variant(EachOf(…))` is unsupported (the
nested `EachOf` would never expand and would reach `_load_input` as an opaque object).
This already holds for the existing wrappers (EachOf can't be a Merge constituent
today). Add a clear "EachOf cannot be nested inside Variant" guard in `Variant.__init__`
for a good error message.

## Implementation steps

### 1. `scidb/src/scidb/variant.py` (new) — mirror `fixed.py`
- `class Variant: __init__(self, var_type, **branch_params)`.
- `to_key()` → canonical string (for version keys / display), e.g.
  `Variant(FilteredEMG, low_hz=20)`.
- `@property __name__` for `format_inputs` / error messages.
- Reject `Variant(Merge(...))` at construction (clear TypeError, like Fixed).
- Reject `Variant(EachOf(...))` at construction (EachOf must stay outermost — see
  EachOf composition rule above).
- Export from `scidb/src/scidb/__init__.py`.

### 2. Thread `branch_params_filter` through the loader (`foreach.py`)
- Add `branch_params_filter` parameter to `_load_input(...)` and
  `_load_var_type_as_spread(...)`, defaulting to `None`, threaded exactly like `where`.
- New `Variant` branch in `_load_input`: merge its `branch_params` into the inherited
  `branch_params_filter` (error on conflict) and recurse into `var_type`.
- `Fixed` / `ColumnSelection` branches: pass `branch_params_filter` through to their
  inner `_load_input` / `_load_var_type_as_spread` calls.
- `Merge` branch: each constituent already gets its own `_load_input`; the per-constituent
  `Variant` injects its own filter there. (No Merge-level branch_params.)

### 3. Make `where` + `branch_params_filter` coexist in the fast path (`database.py`)
- Today `load_all_as_df` fast path: `if where is not None: _load_with_where(...)` (no
  bp filter) `else: _find_record(..., branch_params_filter=...)` — mutually exclusive
  (`database.py:~2629`).
- Simplest fix: after `_load_with_where` returns its records, apply branch_params
  filtering as a **post-step**, reusing the same `_match_branch_param` loop
  `_find_record` already uses (`database.py:~1570-1576`). Factor that loop into a small
  shared helper so both paths call it.
- Equality matching is free (existing `_match_branch_param`: exact + bare-name suffix).
  Ordering operators (`>`, `<`, …) are a deferred extension (would need a richer
  per-input spec than a dict + an operator-aware matcher).

### 4. MATLAB parity
- `sci-matlab/src/sci_matlab/matlab/+scidb/Variant.m` — builder mirroring `Fixed.m`,
  holding inner spec + a branch_params struct.
- `describe_input_for_python` (in `+scidb/for_each.m`) — add a `'variant'` kind:
  `py.dict('kind','variant','inner',<inner spec>,'branch_params',<py.dict>)`.
- `bridge.py::_reconstruct_input_for_keys` — add a `variant` clause building a Python
  `Variant(inner, **branch_params)`. (Recursive; one clause, like `fixed`.)
- Verify non-schema keys survive the MATLAB→Python trip (they're a plain dict).

### 5. Tests + logging
- Python (`scidb/tests/`): pin a plain input; `Variant` + `Fixed`; `Variant` +
  `ColumnSelection`; `Variant` inside `Merge`; `EachOf(Variant(...), Variant(...))`
  runs once per variant and concatenates; aggregation no longer smushes (the motivating
  case); `Variant(Merge(...))` raises; `Variant(EachOf(...))` raises; conflicting nested
  Variants raise.
- MATLAB (`sci-matlab/tests/matlab/scidb/`): the same combinations through the bridge
  (extend `TestMerge` / `TestForEachWhere`).
- Debug logs on the branch_params split + predicate application (per CLAUDE.md NOTE 2).

### 6. Docs
- New `docs/claude/variant-branch-param-pinning.md` (composition model + load-time vs
  per-combo semantics + the aggregation connection).
- Cross-link from `where-filter-system.md`, `scidb-for-each-internals.md`, and
  `scidb-identity-and-data-flow.md` (variant tracking section).

## Out of scope (note as future)
- Ordering operators on branch_params (`low_hz > 15`) — needs richer spec + matcher.
- Auto-grouping aggregation by branch_params (the deeper fix to Step 12 so variants stay
  separate *without* explicit pinning). `Variant` mitigates the symptom; this would
  remove the footgun entirely. Larger change; separate effort.

## Key files
| File | Change |
|---|---|
| `scidb/src/scidb/variant.py` | new `Variant` wrapper |
| `scidb/src/scidb/__init__.py` | export `Variant` |
| `scidb/src/scidb/foreach.py` | thread `branch_params_filter`; `Variant` branch in `_load_input` |
| `scidb/src/scidb/database.py` | `where` + `branch_params_filter` coexist in fast path; shared `_match_branch_param` helper |
| `sci-matlab/src/sci_matlab/matlab/+scidb/Variant.m` | MATLAB builder |
| `sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m` | `describe_input_for_python` `'variant'` kind |
| `sci-matlab/src/sci_matlab/bridge.py` | `_reconstruct_input_for_keys` `variant` clause |
