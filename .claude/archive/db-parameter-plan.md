# Plan: Add `db=` Parameter for Multi-Database Support

## Context

The framework uses a single global database pattern. `configure_database()` sets `_local.database` and `Thunk.query` as globals, and all `BaseVariable.save()`/`.load()` calls route through `get_database()` which returns that singleton. This prevents working with multiple databases (e.g. two aims with different schemas) simultaneously.

The agreed design adds:
1. `DatabaseManager.set_current_db()` — explicitly sets this instance as the global active database
2. `db=None` parameter on all user-facing methods — one-shot override without changing the global default
3. `get_database()` remains unchanged — always returns the current active database from anywhere

## Changes

### 1. `database.py` — Foundation

**File**: `/workspace/src/scidb/database.py`

**A. Add `set_current_db()` method to `DatabaseManager`** (after `reopen()`, ~line 1446):
```python
def set_current_db(self):
    """Set this DatabaseManager as the active global database."""
    from .thunk import Thunk
    Thunk.query = self
    _local.database = self
    self._closed = False
```

**B. Refactor `configure_database()`** — replace lines 96-97 with `db.set_current_db()`:
```python
# Before:
Thunk.query = db
_local.database = db

# After:
db.set_current_db()
```

### 2. `variable.py` — Core API

**File**: `/workspace/src/scidb/variable.py`

Add `db=None` parameter to five classmethods. When `db` is provided, use it directly; otherwise fall back to `get_database()`.

**A. `save()` (line 137)** — add `db=None` after `index`:
```python
def save(cls, data, index=None, db=None, **metadata):
    ...
    from .database import get_database
    _db = db or get_database()
    return _db.save_variable(cls, data, index=index, **metadata)
```

**B. `load()` (line 198)** — add `db=None` after `as_table`:
```python
def load(cls, version="latest", loc=None, iloc=None, as_table=False, db=None, **metadata):
    ...
    from .database import get_database
    _db = db or get_database()
    # Replace all `db.` calls with `_db.`
```

**C. `load_all()` (line 299)** — add `db=None` after `version_id`:
```python
def load_all(cls, as_df=False, include_record_id=False, version_id="all", db=None, **metadata):
    ...
    from .database import get_database
    _db = db or get_database()
```

**D. `list_versions()` (line 383)** — add `db=None`:
```python
def list_versions(cls, db=None, **metadata):
    ...
    from .database import get_database
    _db = db or get_database()
    return _db.list_versions(cls, **metadata)
```

**E. `save_from_dataframe()` (line 414)** — add `db=None`, pass through to `cls.save()`:
```python
def save_from_dataframe(cls, df, data_column, metadata_columns, db=None, **common_metadata):
    ...
    record_id = cls.save(data, db=db, **full_metadata)
```

### 3. `pathinput.py` — Compatibility

**File**: `/workspace/scirun-lib/src/scirun/pathinput.py`

Add `db=None` to `PathInput.load()` so `for_each` can uniformly pass `db=db` to all loadable inputs without it leaking into `str.format()`:
```python
def load(self, db=None, **metadata):
    # db is accepted and ignored
```

### 4. `foreach.py` (scirun-lib) — Primary for_each

**File**: `/workspace/scirun-lib/src/scirun/foreach.py`

**A.** Add `db=None` parameter to `for_each()` signature (before `**metadata_iterables`).

**B.** Pass `db=db` to load calls (line 136):
```python
loaded_inputs[param_name] = var_type.load(db=db, **load_metadata)
```

**C.** Pass `db=db` to save calls (line 188):
```python
output_type.save(output_value, db=db, **save_metadata)
```

### 5. `foreach.py` (src/scidb) — Secondary for_each

**File**: `/workspace/src/scidb/foreach.py`

Same changes as step 4: add `db=None` parameter, pass through to `.load()` and `.save()`.

### 6. Update docs

**File**: `/workspace/docs/claude/multi-database-workflow.md`

Update to show the new `db=` parameter and `set_current_db()` API instead of the sequential close/reconfigure pattern.

## Files Modified (in order)

1. `/workspace/src/scidb/database.py` — `set_current_db()` + refactor `configure_database()`
2. `/workspace/src/scidb/variable.py` — `db=None` on 5 classmethods
3. `/workspace/scirun-lib/src/scirun/pathinput.py` — `db=None` on `load()`
4. `/workspace/scirun-lib/src/scirun/foreach.py` — `db=None` passthrough
5. `/workspace/src/scidb/foreach.py` — `db=None` passthrough
6. `/workspace/docs/claude/multi-database-workflow.md` — updated docs

## Verification

1. Run existing tests to confirm backwards compatibility (no existing test should break since `db=None` preserves current behavior):
   ```
   python -m pytest tests/ scirun-lib/tests/ -x
   ```

2. Add a new test in `tests/test_integration.py` that:
   - Creates two databases with different schemas
   - Saves data to each using `db=` parameter
   - Loads from each using `db=` parameter
   - Verifies `set_current_db()` switches the default
   - Verifies `for_each()` works with `db=` parameter
