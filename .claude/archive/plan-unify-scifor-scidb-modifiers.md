# Unify scifor/scidb for_each modifier classes

## Context

Investigating a MATLAB/Python parity bug (a static `PathInput` incorrectly
erroring on an unresolved schema key) surfaced that `scidb.PathInput` is
already a clean re-export of `scifor.PathInput` — one class, in scifor,
with scidb layering DB-specific behavior around it via `isinstance` checks
— but the for_each *orchestration* around it (discovery, per-combo
resolution) is duplicated: it exists in scidb and in MATLAB's
`+scifor/for_each.m`, with no pure-Python scifor equivalent.

The user broadened this to a general principle: anything that isn't
inherently DuckDB-specific (scifor's real boundary is "no DB knowledge,"
not "no I/O") should live in scifor once, with scidb re-exporting, to
minimize technical debt. This plan covers everything checked so far.

## The six roles inside a "modifier" class, and where each belongs

Reading the actual code (not assumed) shows these classes bundle distinct
responsibilities that don't all sit in the same package today for the
same reason:

1. **Container/spec** — what the wrapper holds + its constructor. scifor's
   `Fixed`/`Merge`/`ColumnSelection`/`ColName` hold a DataFrame; scidb's
   hold a "variable type" (a class with `.load()`). Same shape, different
   cargo — this is the piece that's genuinely duplicated and should
   collapse to one class.
2. **Per-combo execution** (filter/merge/extract against an
   already-loaded DataFrame) — lives **only in scifor** already. scidb
   converts its wrappers into scifor's before delegating to scifor's
   loop. Not duplicated, correctly placed.
3. **Loading** (turning a variable type into a DataFrame via DuckDB) —
   lives **only in scidb** already (`_load_input`, `PerComboLoader`).
   Can't move — inherently needs DB knowledge. Correctly placed.
4. **Identity serialization (`to_key()`)** — exists today only on scidb's
   `Fixed`/`Merge`/`ColumnSelection`. Confirmed this is *not* scidb-only
   by nature: scifor already defines `to_key()` on its own pure classes
   (`ColFilter`, `PathInput`) precisely so downstream consumers (scidb)
   can hash them for version keys — scifor itself never calls it. This is
   an established contract, not scidb contamination. Porting `to_key()`
   onto the shared `Fixed`/`Merge`/`ColumnSelection` matches existing
   precedent directly.
5. **DB filter/query construction** (`ColumnSelection`'s
   `==`/`!=`/`<`/`isin()` etc.) — verified this genuinely cannot become
   shared base-class code (see Filters section below): it requires a
   `BaseVariable` reference that scifor's DataFrame-only `ColumnSelection`
   has no field for. Stays scidb-only, as a subclass/mixin over the
   shared base (not a duplicate class) — see design below.
6. **Surrounding orchestration** — PathInput's discovery/resolve glue is
   pure filesystem, moves to scifor. EachOf's recursion threads
   `save`/`db`/`skip_computed`/lineage per alternative — inherently a
   scidb concept, verified by tracing the actual recursive call; only its
   trivial `.alternatives` container moves.

## Scope

**In scope — unify container classes (role 1) + `to_key()` (role 4):**
`Fixed`, `Merge`, `ColumnSelection`, `ColName`, `EachOf` (container only),
plus finishing `PathInput`'s orchestration (role 6).

**Confirmed out of scope, with evidence:**
- **`Variant`/`AcrossVariants`** — genuinely DB-specific (branch-param
  filtering against `db.dataset_schema_keys`).
- **Filter class hierarchies** (`scifor.filters` vs `scidb.filters`) —
  investigated in depth and **rejected as a unification target**. This
  isn't "duplicated implementation with a DB wrapper," it's two different
  operations that share comparison syntax:
  - scidb's `ColumnFilter`/`InFilter`/`VariableFilter` all take a
    **required `variable_class` constructor argument** — which table to
    query — because a `CompoundFilter` can legally combine leaves
    referencing *different* variable classes. This can't be deferred to
    "compile time"; it's the semantic content of the filter, baked into
    construction. scifor's `ColFilter(column, op, value)` has no
    equivalent field because it always means "the DataFrame currently
    being filtered" — structurally incompatible, not just
    context-wrapped.
  - Proven further: scidb's `load(where=...)` (`database.py:2400`) gates
    on `isinstance(where, Filter)` (scidb's own ABC) and hard-errors
    (`TypeError`) on anything else — passing a scifor `ColFilter` there
    today is already a hard error, confirming the two are non-interchangeable.
  - `CompoundFilter`'s constructor argument order **differs between the
    two packages** (`scifor.CompoundFilter(op, left, right)` vs.
    `scidb.CompoundFilter(left, right, op)`) — aliasing them would
    silently transpose fields at every existing scidb call site with no
    exception, a correctness trap. Their `to_key()` string formats also
    already differ, and scidb's is embedded in version-key hashing —
    harmonizing "for consistency" would silently change hashes for every
    existing `where=` usage.
  - **Guardrail for future work** (not an action item): never alias or
    merge `CompoundFilter`/`NotFilter` between the two packages, and
    never change either package's filter `to_key()` string format.
  - `ColumnSelection`'s comparison operators (role 5) were checked against
    this finding: they must keep constructing scidb's `ColumnFilter`/
    `InFilter` (which need `self.var_type`), so they can't be reduced to
    calls into scifor's `Col`. Confirms role 5's answer: subclass/mixin
    in scidb, not shared-class methods.
- **`csv_export.py`** (same filename, both packages) — not duplication.
  scifor's does a generic in-memory `pd.merge` join; scidb's is a
  from-scratch schema-id-keyed export querying stored DB records. Two
  unrelated algorithms.
- **`Constant`** (scidb-only) — a generic value-annotation wrapper for a
  GUI sidebar feature, no duplicate in scifor, zero coupling to for_each
  internals. Nothing to collapse.

## Design decisions

1. **Fixed/Merge/ColumnSelection/ColName** — one class per concept, in
   scifor, keeping scifor's current attribute names (`.data`, `.tables` —
   much smaller internal blast radius than scidb's `.var_type`/
   `.var_specs`, ~18 hits in one scifor file vs. ~83 across 11 scidb
   files). `to_key()` ported into all three (Fixed/Merge/ColumnSelection)
   verbatim, just renamed attributes — matches the `PathInput`/`ColFilter`
   precedent already in the codebase.
2. **`ColumnSelection`'s DB comparison operators** — become a scidb-side
   subclass (`class ColumnSelection(scifor.ColumnSelection):` adding
   `__eq__`/`__ne__`/etc. + `.load()` + `.to_csv()`) rather than living on
   the shared base, guarded-import or otherwise. Keeps scifor's class
   fully inert w.r.t. DB concepts (no dead/guarded imports in the "pure"
   layer) while still eliminating the duplicate *container* — scidb's
   `BaseVariable.__class_getitem__` constructs the scidb subclass;
   `isinstance(x, scifor.ColumnSelection)` is still `True` for it, so
   scifor's for_each loop and `_is_loadable`-style checks work unchanged.
3. **`EachOf`** — trivial container (`.alternatives`, `__repr__`) moves to
   a new `scifor/each_of.py`. The recursive expansion orchestration
   (traced in full: threads `save`/`db`/`track_lineage`/`skip_computed`/
   `finalized` per alternative — concepts scifor's pure `for_each` doesn't
   have at all) **stays in scidb, unchanged**, recursing into
   `scidb.for_each` exactly as today. Payoff: scifor's own standalone
   `for_each` gains a new, independent, simpler EachOf expansion step
   (recursing into itself) for the first time — purely additive, zero
   interaction with scidb's existing path since scidb always resolves
   EachOf before anything reaches `scifor.for_each`.
4. **`PathInput`** — class already unified. Move the discovery/empty-key-
   resolution glue and per-combo resolution (today scidb-only) into
   scifor as a small reusable function scidb calls, plus per-combo
   resolution wired the same way `PathOutput` already resolves inside
   scifor's loop (`_resolve_path_outputs`, called before `_call_fn` and
   inside `_run_column_iteration`).
5. **Filters** — no change, see Scope above.

## Landmines (verified, not hypothetical)

**A — version-key/lineage byte-identity (Fixed/Merge/ColumnSelection).**
`foreach_config.py._serialize_inputs()` does `spec.to_key()` when
`hasattr(spec, "to_key")`, feeding `ForEachConfig.to_version_keys()`
→ `call_id` → skip_computed/lineage. `to_key()` exists **only** on
scidb's current `Fixed`/`Merge`/`ColumnSelection`. If the unified classes
are taken from scifor's code as-is (no `to_key()`), every such input
silently falls back to `repr(spec)` (a memory address) — forking
`call_id` for every existing call site (mass unwanted recompute,
non-deterministic across runs). **`to_key()` must move into scifor's
classes in the same commit that deletes scidb's own definitions.** Add a
byte-identical-before/after regression test.

**B — real crash, traced end-to-end.** Once `ColumnSelection`/`Fixed` are
shared, `ColumnSelection(plain_dataframe, [...])` becomes constructible
under scidb for the first time (today impossible — scidb's version is
only ever built from a variable type). Traced: this reaches
`_resolve_per_combo_loader`, which does `spec.var_type.load(**load_kw)`
with **no DataFrame guard** → `AttributeError`. Fix: add an explicit
DataFrame-passthrough fast path to `_load_input`'s `ColumnSelection`
branch and to `_resolve_per_combo_loader`'s `ColumnSelection`/
`Fixed(ColumnSelection(...))` branches, landed before scidb's own class
definitions are deleted.

**C — attribute-rename blast radius + false-positive trap.**
`.var_type`→`.data`, `.var_specs`→`.tables` touches ~50 call sites across
`scidb/foreach.py`, `csv_export.py`, `pipeline.py`, `provenance_query.py`,
`provenance_save.py`. `Variant`/`AcrossVariants` each have their **own,
unrelated** `.var_type` attribute (out of scope, must not be touched) — a
blind find-and-replace would corrupt them. Every site must be confirmed
as operating on a `Fixed`/`ColumnSelection`/`Merge` instance first.

**D — `_is_loadable`'s `hasattr(v, "load")` fallback (PathInput-specific,
carried over from the earlier PathInput-only draft of this plan).**
`PathInput` has a real `.load()`, so removing it from `_is_loadable`'s
isinstance tuple alone does nothing — the `hasattr` fallback still
catches it. Needs an explicit early-out. The same two `foreach_config.py`
functions (`_get_direct_constants`, `_serialize_inputs`) need matching
updates so PathInput's `.to_key()` keeps landing in `__inputs` instead of
leaking into `__constants` as a raw object.

## Sequencing

1. **scifor additions — purely additive, zero scidb risk.** PathInput
   orchestration; `to_key()` on `Fixed`/`Merge`/`ColumnSelection`; new
   `each_of.py` + EachOf expansion step in `scifor.for_each`. New scifor
   tests validate all of this in isolation; scidb doesn't call any of it
   yet.
2. **`_is_loadable` + `foreach_config.py` fixes** (Landmine D) for
   PathInput specifically.
3. **Attribute rename alone** (Landmine C), inside scidb's *own*
   still-existing class definitions first, before swapping import source
   — isolates "does the rename break anything" from "does the class swap
   break anything." Full scidb suite, expect zero diffs.
4. **The risky step**: swap scidb's imports to scifor's unified classes,
   delete scidb's `fixed.py`/`merge.py`/`each_of.py`/`colname.py`
   entirely, and turn `column_selection.py` into the DB-subclass from
   design decision 2 (not deleted — narrowed). In order: add the
   `to_key()` byte-identical regression test (Landmine A); add the
   DataFrame fast paths (Landmine B); swap imports; delete/narrow the
   files; update `scidb/__init__.py`. Run every regression canary before
   and after; diff `ForEachConfig(...).to_version_keys()` for
   representative inputs — expect byte-identical.
5. **scidb PathInput wiring** — replace its discovery/empty-key steps
   with a call into scifor's new function. Independent of step 4.
6. **New tests**: `ColumnSelection(plain_df, [...])`/
   `Fixed(plain_df, ...)` now working under `scidb.for_each` (net-new,
   previously impossible to construct); `isinstance(scidb.Fixed(...),
   scifor.Fixed)`; version-key stability; scifor-standalone `EachOf` and
   `PathInput` tests.

## Regression canaries (existing scidb tests, behavior must not change)

`test_schema_key_types.py`, `test_pathinput_static_schema_keys.py`,
`test_variant_queries.py`, `test_pipeline_registry.py`,
`test_inspect_phase3.py`, `test_inspect_phase5.py`,
`test_rerun_output_edges.py`, `test_each_of.py`,
`test_column_selection_combo_pruning.py`, `test_for_columns.py`,
`test_variable_filter_merge.py`, `test_aggregation_with_variants.py`,
`test_coarse_input_provenance.py`, `test_to_csv.py`,
`test_variant_pinning.py`, `test_filters.py`,
`test_provenance_identity.py`, `test_schema_key_filter.py`,
`test_orphaned_records.py`, `test_introspect.py`. No dedicated call_id/
version-key test currently exists — add one fresh as part of step 6.

## Critical files

`scifor/src/scifor/foreach.py`, `fixed.py`, `merge.py`,
`column_selection.py`, `colname.py`, new `each_of.py`, `pathinput.py`
(unchanged) — `scidb/src/scidb/foreach.py`, `foreach_config.py`,
`__init__.py`, `variable.py`, `csv_export.py`, `pipeline.py`,
`provenance_query.py`, `provenance_save.py`, `column_selection.py`
(narrowed to a subclass, not deleted).

## After approval

Write a copy of this plan to `.claude/plan-unify-scifor-scidb-modifiers.md`
in the repo (project convention), as the first implementation action.
