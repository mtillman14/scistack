# Plan: Remove stale `load_all()` syntax from docs

## Background

Two methods were removed and merged into `load()`:
- `BaseVariable.load_all()` (public) → merged into `load(version="all"|"latest", as_df=..., where=...)`
- `DatabaseManager.load_all()` (internal) → replaced by `db.load_all_as_df()` (bulk DataFrame)
  and `db.load(..., version_id=...)` (generator).

Still valid: `db.load_all_as_df()`, internal `version_id=` param, MATLAB `as_table=`.

Already-correct docs that *document the removal* (leave untouched):
`docs/guide/{filters,database,variables}.md`, `docs/api/{index,filters,variables}.md`,
`docs/project/faq.md`.

## Replacement rules

| Old | New |
|---|---|
| `Type.load_all(as_df=True)` | `Type.load(as_df=True)` |
| `Type.load_all(where=...)` | `Type.load(where=...)` |
| `Type.load_all(**metadata)` (load all records) | `Type.load(version="all", **metadata)` |
| Python `load_all(as_table=true)` | `load(as_df=True)` (Python) / `load(as_table=true)` (MATLAB) |
| internal `db.load_all(...)` (generator) | `db.load(..., version_id=...)` |
| internal `db.load_all(...)` (DataFrame) | `db.load_all_as_df(...)` |
| internal `var_type.load_all(version_id="latest")` | `var_type.load(version="latest")` |
| conceptual "has a `load_all()` method / bulk-load" | "supports bulk loading" / `load_all_as_df` |

## Scope tiers

### Tier A — public-API examples (low risk, user-facing) — ALWAYS DO
- `README.md:267,368,384`
- `docs/claude/multi-database-workflow.md:41,137,138`
- `docs/claude/where-filter-system.md:11-30,272` (code examples)
- `docs/claude/gui-readiness.md:20,21,119`
- `docs/claude/scidb-identity-and-data-flow.md:818` (API summary table)

### Tier B — Python `as_table=` correction — ALWAYS DO
- `docs/claude/as-table-column-types.md:5,45` (Python is `as_df`; `as_table` MATLAB-only)

### Tier C — internal-mechanics prose (larger, method-name-only staleness)
- `docs/claude/scidb-for-each-internals.md` (×12)
- `docs/claude/scidb-identity-and-data-flow.md:76,250,310,524,607,673,788,885`
- `docs/claude/where-filter-system.md:40,76,223,225,268`
- `docs/claude/matlab-load-performance.md:25,33,74,76,82`
- `docs/claude/scihist-for-each-internals.md:624`
- `docs/claude/duckdb-column-types.md:117`
- `docs/claude/archive/matlab-for-each-current-state.md:54,90,119,398`
- `docs/claude/layer-friction-analysis.md:284`

### Tier D — scidb-net optional network layer
- `scidb-net/README.md:158,219` — verify whether `/load_all` endpoint still exists before editing.

## Out of scope
- `.claude/*.md` plan files (historical record)
- test files (`test_load_all_*.py`)
- `scidb_internals_questions2.md` (scratch Q&A)
