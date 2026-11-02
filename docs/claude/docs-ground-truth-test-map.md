# Docs Ground-Truth: Package → Test Map

**Purpose:** When writing or correcting user-facing documentation (`docs/`,
`README.md`), the existing prose is **not trusted** — it conveys the right
conceptual intent per package but is largely stale on API specifics, and the
`README.md` and `docs/index.md` actively contradict each other (see "Known
contradictions" below). The **authoritative source for current behavior is each
package's `tests/` directory.** This note maps documentation topics to the test
files that define their real behavior, so future sessions don't re-derive it.

Rule: when old docs disagree with tests, **the tests win.**

## Package layout (confirmed on disk, 2026-06)

Folders were renamed from earlier short names. Current names:

| Folder | Package / role |
|---|---|
| `scifor` | Lowest-level batch execution engine (plain tables/DataFrames, no DB) |
| `scidb` | Typed variable storage + DB-backed `for_each` + provenance graph + node state |
| `sciduckdb` | DuckDB database layer (under scidb) |
| `scicanonicalhash` | Deterministic content and function hashing |
| `scimatlab` | MATLAB bridge/wrapper |

Dependency direction (from README, treat as intent — verify specifics via
imports in tests): `scidb` → `scifor` + `sciduckdb` + `scicanonicalhash`.

## Topic → ground-truth test files

| Doc topic | Authoritative tests |
|---|---|
| Architecture & layering | layering is implied by cross-package imports; `scidb/tests/test_hist_foreach.py`, `scidb/tests/test_integration.py` |
| Variables & storage / `BaseVariable` API | `scidb/tests/test_integration.py`, `test_introspect.py`, `test_constant.py`; `sciduckdb/tests/test_sciduck.py` |
| Database & configuration | `scidb/tests/test_integration.py`, `test_discover.py`, `test_orphaned_records.py`, `test_load_all_ordering.py`; `sciduckdb/tests/test_sciduck.py` |
| Lineage & provenance | `scidb/tests/test_identity_parity.py`; `scicanonicalhash/tests/test_function_hash.py` |
| Caching & node states | `scidb/tests/test_hist_skip_computed.py`, `test_hist_state*.py`; `scidb/tests/test_call_id.py` |
| Versioning & content hashing | `scicanonicalhash/tests/test_hashing.py`, `test_function_hash.py`; hashing-related cases in `scidb/tests` |
| Batch processing (`for_each`) | `scifor/tests/test_foreach_standalone.py`, `test_schema.py`, `test_merge_*.py`; `scidb/tests/test_for_columns.py`, `test_each_of.py`, `test_aggregation*.py`, `test_variant_*.py`; `scidb/tests/test_hist_foreach.py`, `test_hist_fixed.py`, `test_hist_merge.py`, `test_hist_unified_variant_tracking.py` |
| Filtering & selection | `scifor/tests/test_filters.py`; `scidb/tests/test_filters.py`, `test_where.py`, `test_exclusions.py`, `test_schema_key_filter.py`, `test_variable_filter_merge.py` |
| Browsing & exporting (CSV/DataFrame) | `scifor/tests/test_merge_as_df.py`, `test_merge_to_csv.py`; `scidb/tests/test_to_csv.py` |
| MATLAB setup & parity | `scimatlab/tests/test_bridge.py`, `test_bridge_notfound.py`, `test_bridge_where.py` |
| Path inputs / file generation | `scifor/tests/test_pathinput_discover.py`, `test_pathinput_regex.py`; `scidb/tests/test_hist_state_pathinput.py`, `test_hist_generates_file.py` |

## API names to re-verify against tests (do not copy from prose)

The existing docs are inconsistent on these — confirm the real current spelling
from the tests above before documenting:

- Decorator: `@thunk` (old index.md) vs `@lineage_fcn` (README) vs `Thunk()` wrapper
- Setup: `configure_database(...)` vs `set_schema(...)`
- Batch: `for_each(...)` signature, `Fixed(...)`, `Constant`, `each_of`, column markers
- Provenance accessor: `db.get_provenance(...)` shape and keys

## Known contradictions (resolve, don't propagate)

- `docs/index.md` pitches a **single-package** `scidb` with `@thunk` + a SQLite
  lineage file. `README.md` pitches the **layered** `scifor → scidb/scilineage
  → scihist` model. Neither is verified against tests.
- `README.md` contradicts itself: `@thunk` vs `@lineage_fcn`; `set_schema` vs
  `configure_database`; broken/duplicated code fences; a "Layer 2" header with
  no "Layer 1".

## Related

- Restructure/IA plan: `.claude/docs-restructure-plan.md`
- This map should be updated if package folders are renamed again or test files
  are reorganized.
