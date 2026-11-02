# Variant — Per-Input branch_param Pinning

## Problem

A variable computed by `for_each` with a swept constant (e.g. `bandpass(low_hz=20)`
vs `low_hz=50`) is stored as multiple **branch_param variants** at the same schema
location. Downstream, there was no concise way to say "use *only* the `low_hz=20`
variant of this input":

- A plain input (`FilteredEMG`) loads **all** variants. In per-combo mode this
  raises `AmbiguousVersionError`; in **aggregation mode** it historically pooled
  every variant into one table ("variant smushing" — since fixed by the D1
  auto-split; see "Out of scope" below).
- `Fixed(..., session="BL")` overrides **schema metadata**, not branch_params.
- `where=` filters by **schema-id** / stored `__where` provenance, not by an
  upstream branch_param value.

## Solution

`Variant(var_type, **branch_params)` pins an input to a specific branch_param
variant. It is the wrapper that *injects* a `branch_params_filter` into its
subtree at **load time**.

```python
# Run fn over only the low_hz=20 variant of FilteredEMG
for_each(fn, {"x": Variant(FilteredEMG, low_hz=20)}, [Out], subject=[1, 2])
```

This also **fixes aggregation-mode smushing**: pinning means variant expansion
(foreach Step 12) only ever sees matching records, so an aggregation no longer
pools distinct variants into one table.

## Core design principle: an orthogonal, threaded, load-time filter

Each input wrapper owns one concern and acts at a different stage:

| Wrapper | Concern | Acts at |
|---|---|---|
| `ColumnSelection` (`MyVar["col"]`) | which columns | after load |
| `Fixed(…, session="BL")` | which schema metadata (override the combo) | per-combo, scifor loop |
| `Merge(…)` | join several inputs | top level |
| **`Variant(…, low_hz=20)`** | which branch_param variant | **load time** |

`where=` is already threaded as a parameter through the whole `_load_input`
recursion (`_load_input(spec, db, where, branch_params_filter=None)`). `Variant`
adds a second threaded parameter, `branch_params_filter`, the exact same way:

- The `Variant` branch in `_load_input` merges its `branch_params` into the
  inherited `branch_params_filter` (raising on a conflicting value) and recurses
  into `var_type`.
- `Fixed` / `ColumnSelection` / each `Merge` constituent pass the filter
  **through** to their inner `_load_input` / `_load_var_type_as_spread` calls.
- The leaf `load_all_as_df(branch_params_filter=…)` applies it, reusing the
  existing `_match_branch_param` matcher (exact, plus bare-name suffix match like
  `low_hz` → `bandpass.low_hz`).

Because the filter is **threaded rather than wrapper-aware**, composition is
**order-agnostic** — there is no N×M matrix of wrapper combinations to maintain.

## Why load-time, not per-combo (important)

`Variant` filters *before* loading, unlike `Fixed`, which loads the full table
and overrides metadata per-combo inside the scifor loop. The reason is timing:
branch_params are **stripped** from the DataFrame during variant tracking
(foreach Step 11, after `rid_to_bp` is built), so they are no longer available at
scifor's per-combo filter stage. Filtering at load time is also cheaper and lets
variant expansion see only the matching variant(s).

A corollary: pinning only takes effect on the **bulk-load** path
(`db.load_all_as_df`). If an inner type falls back to a `PerComboLoader` (custom
serialization, no `load_all_as_df`), the branch_params filter is not applied —
those types are the rare exception, and the normal bulk path covers the
motivating cases.

## Composition rules

- `Variant` may wrap a variable type, a `ColumnSelection`, or a `Fixed`. Both
  orders with `Fixed` load **identically**:
  `Fixed(Variant(X, low_hz=20), session="BL")` ≡
  `Variant(Fixed(X, session="BL"), low_hz=20)`.
- `Variant` may be a `Merge` constituent — the primary multi-input case, giving
  **per-constituent** pinning: `Merge(Variant(A, low_hz=20), B)`.
- **`Variant(Merge(…))` is an ERROR** (mirrors `Fixed(Merge(…))`). branch_params
  are namespaced per producing function (`bandpass.low_hz`), so one `low_hz=20`
  cannot sensibly broadcast across constituents with different namespaces — it
  would match nothing on the others and silently drop their rows. Pin per
  constituent instead.
- **Nested `Variant(Variant(…))`** merges the dicts and **raises on a key
  conflict**.
- **`Variant(EachOf(…))` is an ERROR.** `EachOf` is always the outermost wrapper
  (see below).

### `where=` + `Variant` coexist

Historically the `load_all_as_df` fast path was mutually exclusive: `where`
routed through `_load_with_where` (no bp filter) and the no-`where` path went
through `_find_record(..., branch_params_filter=…)`. The branch_params matching
loop was factored out of `_find_record` into a shared helper,
`_filter_records_by_branch_params`, and applied as a **post-step** after
`_load_with_where` in both `load_all_as_df` and the `load` generator. So a
`where=` filter and a `Variant` pin now compose: the `where` selects the
record set, then the branch_params filter narrows it to the pinned variant.

### `EachOf` composition (works for free)

`EachOf` is a **call-level multiplexer** handled in foreach Step 1, *before* input
loading: for each combination of alternatives it substitutes the alternative into
the input (or `where=`) and makes a **recursive `for_each` call**, concatenating
results. Because the recursive call runs the normal pipeline, an alternative can
be any valid input spec — including a `Variant`:

```python
# Run once per pinned variant, results concatenated — no extra code needed
EachOf(Variant(FilteredEMG, low_hz=20), Variant(FilteredEMG, low_hz=50))
```

Step 1 only detects `EachOf` as the *direct* value of an input param or of
`where=`; it does not look inside `Merge`/`Fixed`/`ColumnSelection`/`Variant`.
Hence the rule **`EachOf` is outermost**, and `Variant(EachOf(…))` is rejected at
construction (the nested `EachOf` would never expand and would reach
`_load_input` as an opaque object). Distinct output identity for each alternative
comes from `Variant.to_key()` (→ the `__inputs` version key) plus the differing
upstream branch_params propagated to outputs.

## Identity

`Variant.to_key()` produces a canonical string (e.g.
`Variant(FilteredEMG, low_hz=20)`) that `ForEachConfig._serialize_inputs` writes
into the `__inputs` version key. Two `for_each` calls that pin different variants
therefore fork into distinct output records rather than colliding.

## MATLAB parity

The MATLAB surface mirrors the Python one and routes everything through the
Python loader (so correctness lives in one place):

- `+scidb/Variant.m` — builder holding the inner spec + a `branch_params` struct;
  rejects `Merge` and enforces the same nested-conflict rule.
- `+scidb/for_each.m::describe_input_for_python` — emits a `'variant'` kind:
  `py.dict('kind','variant','inner',<inner spec>,'branch_params',<py.dict>)`.
- `bridge.py::_reconstruct_input_for_keys` — a `variant` clause rebuilds the
  Python `Variant(inner, **branch_params)`. Because `Variant` is consumed at load
  time (it never becomes a scifor wrapper), there is **no** `'variant'` case in
  `build_scifor_input_from_desc` — symmetric with the Python side, where `Variant`
  does not appear in `for_each_describe_loaded_input`.

## Out of scope (future)

- **Ordering operators** on branch_params (`low_hz > 15`) — needs a richer
  per-input spec than a dict plus an operator-aware matcher.
- ~~**Auto-grouping aggregation by branch_params**~~ — **IMPLEMENTED** (D1,
  2026-07): aggregation mode now auto-splits by branch_param signature
  (one call per group, `__vsig_*` discriminator columns), so variants stay
  separate *without* explicit pinning; `AcrossVariants(X)` is the explicit
  pooling opt-in. See the aggregation section of
  [archive/scidb-for-each-internals.md](archive/scidb-for-each-internals.md) and
  [endpoints-viz-and-stats-design.md](endpoints-viz-and-stats-design.md).
  `Variant` remains useful to run only ONE group (load-time narrowing).

## Related

- [where-filter-system.md](where-filter-system.md) — the threaded `where=`
  parameter that `branch_params_filter` parallels.
- [where-provenance-and-merge.md](where-provenance-and-merge.md) — how `where=`
  selects one variant per combo through `Merge` via stored `__where` provenance.
- [archive/scidb-for-each-internals.md](archive/scidb-for-each-internals.md) — `_load_input` /
  `_load_var_type_as_spread` recursion and the input-wrapper model.
- [archive/scidb-identity-and-data-flow.md](archive/scidb-identity-and-data-flow.md) — variant
  tracking (Steps 11–12) and why branch_params are stripped before per-combo.
- [each-of-variant-expansion.md](each-of-variant-expansion.md) — the call-level
  multiplexer that `Variant` nests inside.
