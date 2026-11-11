# Plan: Incorporate `repro` into SciStack

## Goal
Add a `repro` submodule to the `scidb` package that lets users score the
reproducibility of their analysis code.

```python
from scidb import repro
report = repro.score("my_project/")
print(report.summary())
```

---

## Where it lives

**`scidb/src/scidb/repro.py`** — a new module inside the existing core package.

Rationale:
- Zero new dependencies (stdlib only: `ast`, `re`, `json`, `pathlib`)
- Conceptually part of the core SciStack value proposition (reproducibility)
- Exposed as `from scidb import repro` or `from scidb.repro import score`

No new sub-package is needed; a single file is sufficient.

---

## What's brought over from sciforge verbatim

The following are taken directly from `sciforge/sciforge/repro/__init__.py`
with no meaningful changes needed:

- `Severity` enum and `_DEDUCTION` table
- `Finding` and `ReproReport` dataclasses (including `summary()`, `to_dict()`,
  `critical_findings()`)
- All individual checker functions:
  - `_check_seeds` — stochastic ops without a seed set
  - `_check_envlock` — unpinned/missing dependency lock files
  - `_check_hardpaths` — hardcoded absolute paths in strings
  - `_check_datetime_leakage` — `datetime.now()` used as a feature
  - `_check_notebook_cell_order` — Jupyter cells run out of order
  - `_check_float_precision` — mixed float16/float32
- The main `score(target, max_files=200)` function and its file-collection logic

---

## What's modified or added

### 1. SciStack-aware check: `_check_scistack_hardpaths`
The existing `_check_hardpaths` catches hardcoded Unix/Windows absolute paths.
Add a SciStack-specific variant that detects hardcoded `.duckdb` file paths
passed directly to `configure_database()` instead of reading from a config or
env var. Severity: MEDIUM.

```python
# Bad — hardcoded path
configure_database("/home/alice/data/experiment.duckdb", ...)

# Good — from config/env
configure_database(os.environ["SCISTACK_DB_PATH"], ...)
```

### 2. SciStack-aware check: `_check_untracked_functions`
Detect functions that appear to be analysis functions (take and return arrays/
DataFrames) but are not decorated with `@thunk`. This is a LOW-severity
best-practice suggestion, not a hard error.

Pattern: functions whose name matches common analysis verbs
(`compute_`, `extract_`, `filter_`, `process_`, `calculate_`) and aren't
wrapped with `@thunk`.

### 3. Expose via `scidb/__init__.py`
Add `repro` to the public API:
```python
from scidb import repro  # module-level import
```

---

## Checks summary (final list)

| Check ID | Severity | Source |
|----------|----------|--------|
| `SEEDS` | HIGH | sciforge (verbatim) |
| `ENVLOCK` | HIGH | sciforge (verbatim) |
| `HARDPATHS` | HIGH | sciforge (verbatim) |
| `DATETIME` | MEDIUM | sciforge (verbatim) |
| `NOTEBOOK_ORDER` | CRITICAL | sciforge (verbatim) |
| `FLOATPREC` | LOW | sciforge (verbatim) |
| `DB_HARDPATH` | MEDIUM | new (SciStack-specific) |
| `UNTRACKED_FN` | LOW | new (SciStack-specific) |

---

## Tests

Add `scidb/tests/test_repro.py` with:
- Score a clean temp `.py` file → score == 100
- Score a file with `np.random.` but no seed → SEEDS finding
- Score a file with `/home/alice/data.csv` → HARDPATHS finding
- Score a dir with no `requirements.txt` or `pyproject.toml` → ENVLOCK finding
- Score a notebook with out-of-order cells → NOTEBOOK_ORDER finding
- Score a file with hardcoded `.duckdb` path → DB_HARDPATH finding
- `report.to_dict()` is JSON-serialisable
- `report.critical_findings()` filters correctly

---

## What's explicitly out of scope

- `GLOBAL_STATE` check (from sciforge README but not in the source code)
- `DATASPLIT` check (same — described but not implemented in sciforge)
- Integration with `for_each` or the lineage DB (future work)
- CLI entrypoint (can be added later; start with Python API only)
