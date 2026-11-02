# scifor/scidb Modifier-Class Unification

## Purpose

`scifor` (pure loop orchestrator) and `scidb` (DuckDB-backed layer built on
top of scifor) used to define **separate classes** for the same `for_each`
input-wrapper concepts — `Fixed`, `Merge`, `ColumnSelection`, `ColName`,
`EachOf` — one flavor operating on DataFrames (scifor), one on DB "variable
types" (scidb). scidb's own `for_each` already converted its wrappers into
scifor's before delegating the per-combo loop to scifor, so the *execution*
logic was never duplicated — only the **container class definitions** were.
This note records the unification: what moved, what stayed split and why,
and the two correctness landmines that had to be fixed in the same commit as
the class merge (not as follow-ups).

Corrected architectural rule that motivated this (see
`docs/claude/input-markers-colname-pathinput-pathoutput.md` for the PathInput
half of this same story): scifor's real boundary is **"no DuckDB knowledge,"
not "no I/O."** Filesystem access (`PathInput`/`PathOutput`) is fine and
expected in scifor; only things that need `db.dataset_schema_keys` /
`db.distinct_schema_values` / branch-param/schema_id resolution are
genuinely scidb-only.

## The six roles inside a "modifier" class

Before touching anything, each class was decomposed into the distinct
responsibilities it bundles — not all of them belonged in the same package
for the same reason:

1. **Container/spec** — what the wrapper holds + its constructor. This was
   the genuinely duplicated piece: scifor's held a DataFrame, scidb's held a
   variable type. Same shape, different cargo → collapsed to one class.
2. **Per-combo execution** (filter/merge/extract against an already-loaded
   DataFrame) — lived only in scifor already (`prepare_input`,
   `_prepare_merge`, etc. in `scifor/foreach.py`). Not duplicated.
3. **Loading** (variable type → DataFrame via DuckDB) — lives only in scidb
   (`_load_input`, `PerComboLoader`/`_resolve_per_combo_loader`). Can't
   move — inherently needs DB knowledge.
4. **Identity serialization (`to_key()`)** — already had precedent for
   living on pure scifor classes even though scifor itself never calls it
   (`PathInput.to_key()`, `filters.ColFilter.to_key()` — pure interface
   contracts for downstream consumers). Ported `Fixed`/`Merge`/
   `ColumnSelection`'s `to_key()` onto the shared class to match.
5. **DB filter/query construction** (`ColumnSelection`'s `==`/`isin()` etc.)
   — investigated whether these could delegate to scifor's own generic
   `Col("column") == value` mechanism (`scifor/filters.py`) instead of
   building `scidb.model.filters.ColumnFilter` directly. **Rejected** — see
   "Filters were NOT unified" below. Stays scidb-only, as a subclass method,
   not a duplicate container.
6. **Surrounding orchestration** — PathInput's discovery/resolve glue is
   pure filesystem → moved to scifor (see the PathInput doc). EachOf's
   recursion threads `save`/`db`/`skip_computed`/lineage per alternative →
   inherently scidb, only the trivial `.alternatives` container moved (see
   `docs/claude/each-of-variant-expansion.md`).

## What actually moved

| Class | Now lives in | scidb's role |
|---|---|---|
| `Fixed` | `scifor/src/scifor/fixed.py` | `from scifor import Fixed` — straight re-export |
| `Merge` | `scifor/src/scifor/merge.py` (base) | `scidb/src/scidb/model/merge.py` — thin **subclass** restoring a DB-aware `.to_csv()` (see Landmine E) |
| `ColName` | `scifor/src/scifor/colname.py` | straight re-export |
| `EachOf` | `scifor/src/scifor/each_of.py` (container only) | straight re-export; scidb's own recursive *expansion* is separate and unchanged |
| `ColumnSelection` | `scifor/src/scifor/column_selection.py` (base) | `scidb/src/scidb/model/column_selection.py` — thin **subclass** adding `==`/`!=`/`<`/`<=`/`>`/`>=`/`isin()` (build `scidb.model.filters` objects) + `.load()` + `.to_csv()` |
| `PathInput` | `scifor/src/scifor/pathinput.py` (already unified before this pass) | scidb layers discovery-orchestration delegation + a schema-key-type-aware resolver on top — see the PathInput doc |

Attribute names: the unified classes kept **scifor's** names (`Fixed.data`,
`Merge.tables`, `ColumnSelection.data`) rather than scidb's (`.var_type`,
`.var_specs`) — scifor's internal usage was far smaller (~18 hits in one
file vs. scidb's ~83 across 11 files), so renaming scidb's call sites was
the smaller diff. `Variant`/`AcrossVariants` (genuinely DB-specific,
out of scope) have their **own, unrelated** `.var_type` attribute that was
never touched — a real trap during the rename (see Landmine 3 below).

Deleted entirely: `scidb/src/scidb/fixed.py`, `colname.py`, `each_of.py`.
`scidb/src/scidb/model/column_selection.py` and `merge.py` were rewritten (not
deleted) as the subclasses described above.

## Why `ColumnSelection` and `Merge` need a scidb subclass

`ColumnSelection.__eq__`/`isin()` build `scidb.model.filters.ColumnFilter`/
`InFilter` objects — these require a live `BaseVariable` **class reference**
(which table to query) baked into the filter at construction time. scifor's
`ColumnSelection` wraps a DataFrame; it has no field for "which table," and
there's no way to defer that requirement to a later "compile" step (see
"Filters were NOT unified" below for why). So these methods can't become
shared base-class code — they stay scidb-only, added via subclassing rather
than duplicating the whole container.

Consequence for internal code: scidb's isinstance checks against
`ColumnSelection`/`Merge` (in `foreach.py`, `csv_export.py`, `pipeline.py`,
`provenance_query.py`, `provenance_save.py`) import the **base** class from
`scifor`, not scidb's subclass — `isinstance(x, scifor.ColumnSelection)` is
`True` for both a scidb-constructed instance (`MyVar["col"]`) and a bare
`scifor.ColumnSelection(df, [...])` passed directly into `scidb.for_each`
(new capability, see Landmine 2). Checking against scidb's subclass instead
would silently exclude the bare case. Only `scidb/src/scidb/model/variable.py`
(`BaseVariable.__class_getitem__` / `.for_columns()`, the actual
construction sites needing the DB-only surface) imports scidb's
`ColumnSelection` subclass directly. `scidb/__init__.py`'s public
`ColumnSelection`/`Merge` exports are the subclasses too (so `MyVar["col"]`
and `Merge(MyVar, OtherVar)` users get the full DB-aware surface by
default) — the internal `_scifor.Merge(*loaded_tables)` construction inside
`_load_input` is a deliberate exception: it wraps already-loaded DataFrames
for scifor's own loop, not something a user calls `.to_csv()` on.

`Merge`'s DB-only method is narrower than `ColumnSelection`'s: just
`.to_csv()`. scidb's pre-unification `Merge` never had an `.as_df()`
(scifor-only, DataFrame join), so nothing needed restoring there.

## Filters were NOT unified (investigated and rejected)

`scifor.filters` (`Col`, `ColFilter`, `CompoundFilter`, `NotFilter`) and
`scidb.model.filters` (`ColumnFilter`, `InFilter`, `CompoundFilter`, `NotFilter`,
`VariableFilter`, `SchemaKey*`, `RawFilter`) look like the same duplication
pattern but are not:

- scidb's `ColumnFilter`/`InFilter`/`VariableFilter` all take a **required**
  `variable_class` constructor argument (which table to query) — a
  `CompoundFilter` can legally combine leaves referencing *different*
  variable classes, so this can't be hoisted out of construction into a
  later resolve step. scifor's `ColFilter(column, op, value)` has no
  equivalent field because it always means "the DataFrame currently being
  filtered" — structurally incompatible, not just DB-context-wrapped.
- `scidb.for_each(..., where=...)`/`.load(where=...)` already hard-errors
  (`TypeError`) if handed a bare scifor `ColFilter` today — proof the two
  are non-interchangeable at every level, not just leaf construction.
- **Guardrail, not just a historical note**: `scifor.CompoundFilter(op,
  left, right)` vs. `scidb.CompoundFilter(left, right, op)` — different
  constructor argument order, same class name. Never alias or merge these
  two classes; doing so would silently transpose fields at every existing
  scidb call site with no exception raised. Their `to_key()` string formats
  also already differ, and scidb's is embedded in `for_each` version-key
  hashing (`foreach_config.py`'s `__where`) — never change either format
  "for consistency."

## Landmines fixed in the same commit as the class merge

**A — version-key/lineage byte-identity.**
`foreach_config.py._serialize_inputs()` puts `spec.to_key()` into
`ForEachConfig.to_config_keys()["__inputs"]` (→ `call_id` → skip_computed /
lineage) whenever `hasattr(spec, "to_key")`. Before this pass, `to_key()`
existed **only** on scidb's own `Fixed`/`Merge`/`ColumnSelection`. If the
unified classes had been taken from scifor's pre-existing code as-is (no
`to_key()`), every such input would have silently fallen back to
`repr(spec)` (a memory address) — forking `call_id` for every existing call
site (mass unwanted recompute, non-deterministic across process runs).
Fixed by porting `to_key()` onto the shared classes in the same change that
removed scidb's own definitions. Regression coverage:
`scidb/tests/test_unified_modifier_classes.py::TestVersionKeyStability`.

**B — DataFrame-passthrough crash.**
Once `ColumnSelection`/`Fixed` are the same class in both packages,
`ColumnSelection(some_dataframe, [...])` becomes constructible under scidb
for the first time (previously impossible — scidb's version was only ever
built from a variable type). Without a fix, this reaches
`_resolve_per_combo_loader`, which does `spec.data.load(**load_kw)` with no
DataFrame guard → `AttributeError`. Fixed by adding explicit
`isinstance(var_spec.data, pd.DataFrame)` fast paths to `_load_input`'s
`ColumnSelection` branch (returns the spec unchanged, letting scifor's own
loop handle it as a normal data input) and to `Fixed`'s branch (the
recursive `_load_input` call already had this fast path for plain
DataFrames — it was `ColumnSelection` specifically that was missing it).
Regression coverage: `test_unified_modifier_classes.py::TestBareDataFrameInputsUnderScidb`.

**C — attribute-rename false-positive trap.** `.var_type`→`.data`,
`.var_specs`→`.tables` touched ~50 call sites. `Variant`/`AcrossVariants`
each have their **own, unrelated** `.var_type` attribute (out of scope,
genuinely DB-specific). Every site was confirmed as operating on a
`Fixed`/`ColumnSelection`/`Merge` instance (not `Variant`/`AcrossVariants`)
before renaming — see e.g. `pipeline.py::_loadable_classes`, which branches
on `isinstance(spec, (Variant, AcrossVariants))` (keeps `.var_type`) vs.
`isinstance(spec, (Fixed, ColumnSelection))` (uses `.data`) in the same
function.

**D — `_is_loadable`'s `hasattr(v, "load")` fallback (PathInput-specific).**
See `docs/claude/input-markers-colname-pathinput-pathoutput.md` — `PathInput`
has a real `.load()`, so removing it from the isinstance tuple alone did
nothing; needed an explicit early `isinstance(var_spec, PathInput): return
False` before the fallback.

**E — `Merge.to_csv()` silently switched implementations (caught by tests,
not by design review).** Unlike the other four classes, `Merge`'s DB-only
surface wasn't identified up front — the original plan explicitly reasoned
about `Merge` and concluded (correctly, for the *container*) that it should
unify like `Fixed`. What got missed: `Merge.to_csv()` is a **method on the
class itself**, and scidb's pre-unification `Merge` had a genuinely
different `to_csv()` (schema-id-keyed DB export, `scidb/csv_export.py`)
than scifor's `Merge.to_csv()` (generic in-memory `pd.merge` join,
`scifor/csv_export.py` — the same two algorithms Landmine-adjacent
"Filters were NOT unified" and the main doc's "out of scope" section
already correctly identified as non-duplicative). Taking `Merge` straight
from scifor with no subclass meant `Merge(ScalarValue, ...).to_csv(...)`
silently started running the DataFrame-join version against variable
*types* — surfaced only by `test_to_csv.py`'s existing Merge tests raising
`AttributeError: type object 'X' has no attribute 'columns'`, not by any
review before landing. Fixed the same way as `ColumnSelection`: a thin
scidb subclass (`scidb/src/scidb/model/merge.py`) restoring just `.to_csv()`.
**Lesson for next time**: when a class unifies, audit every *method* on the
pre-unification versions individually (not just the constructor/attributes)
for behavioral differences — a class-level "this container is the same
shape" conclusion doesn't imply every method on it is safe to take from
either side.

## Key files

| File | Role |
|---|---|
| `scifor/src/scifor/fixed.py`, `merge.py`, `column_selection.py`, `colname.py`, `each_of.py` | Unified class definitions (`to_key()` added to `fixed.py`/`merge.py`/`column_selection.py`) |
| `scidb/src/scidb/model/column_selection.py` | DB-only `ColumnSelection` subclass (comparison operators, `.load()`, `.to_csv()`) |
| `scidb/src/scidb/model/merge.py` | DB-only `Merge` subclass (schema-id-keyed `.to_csv()` — Landmine E) |
| `scidb/src/scidb/run/foreach.py` | `_is_loadable` (PathInput early-out), `_load_input`/`_resolve_per_combo_loader` (DataFrame fast paths, renamed attrs), `_has_pathinput`/`_find_pathinput` |
| `scidb/src/scidb/run/foreach_config.py` | `_get_direct_constants`/`_serialize_inputs` (PathInput classification) |
| `scidb/src/scidb/run/pipeline.py`, `csv_export.py`, `provenance_query.py`, `provenance_save.py`, `variant.py`, `across_variants.py` | Renamed attribute access + import-source swap; `Variant`/`AcrossVariants`' own `.var_type` left untouched |
| `scimatlab/src/scimatlab/bridge.py` | Import-source swap for `Fixed`/`Merge`/`ColName` (MATLAB bridge's spec-reconstruction path); `Fixed(PathInput(...))` handling simplified since `scidb.Fixed`/`scifor.Fixed` collapsed to one class |
| `scidb/tests/test_unified_modifier_classes.py` | isinstance-unification, Landmine B, Landmine A regression tests |
| `scifor/tests/test_to_key.py` | `to_key()` output format for all three classes |

## Out of scope, confirmed

- `Variant`/`AcrossVariants` — genuinely DB-specific.
- `scifor.filters` / `scidb.model.filters` — see above.
- `csv_export.py` (same filename in both packages) — scifor's does a generic
  in-memory `pd.merge` join; scidb's is a from-scratch schema-id-keyed
  export querying stored records. Two unrelated algorithms.
- `Constant` (scidb-only, `constant.py`) — generic value-annotation wrapper
  for a GUI sidebar feature, no duplicate in scifor, no for_each coupling.
- MATLAB's `+scifor/for_each.m` PathInput discovery — the ask was scidb
  (Python) specifically; MATLAB already calls the same shared Python
  `PathInput.apply_discovery()`, just with its own orchestration glue.

## See Also

- `docs/claude/input-markers-colname-pathinput-pathoutput.md` — PathInput's
  half of this story in detail (discovery orchestration, per-combo
  resolution, the `LoopHooks.path_input_resolver` hook).
- `docs/claude/each-of-variant-expansion.md` — EachOf's container-vs-orchestration
  split.
- `docs/claude/column-selection.md` — predates this unification (and several
  package renames before it — still references `scirun-lib`, `src/scidb`);
  not refreshed as part of this pass, flagged separately.
