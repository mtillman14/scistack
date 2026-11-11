# Lineage Redesign Plan

## Problem

The current `_lineage` table uses a **per-edge row design**: one row per input or
constant, storing only type names and value reprs. This loses three critical pieces
of information:

1. **Constant names are lost** — `low_hz=20` is stored as just `"20"` in `source`.
   You can't tell `low_hz` from `high_hz`.
2. **Input record_ids are lost** — `source = "RawEMG"` (type name only).
   You can't trace which specific instance was the input.
3. **Chained ThunkOutput inputs are stored as "unknown"** — because THUNK_OUTPUT
   lineage dicts have `source_function`, not `type`. `inp.get("type", "unknown")`
   returns literally `"unknown"`.

Existing tests pass because they only assert `function_name` and `source_type` —
they don't verify that constant names or input record_ids are preserved.

## Solution: One row per computation, JSON columns (Option A)

Replace the per-edge row design with one row per unique computation output.
`_record_metadata` remains the timestamp-based audit trail. `_lineage` is a
pure provenance lookup keyed by `output_record_id`.

### New `_lineage` schema

```sql
CREATE TABLE IF NOT EXISTS _lineage (
    output_record_id VARCHAR PRIMARY KEY,
    lineage_hash     VARCHAR NOT NULL,
    target           VARCHAR NOT NULL,   -- output variable type name
    function_name    VARCHAR NOT NULL,
    function_hash    VARCHAR NOT NULL,
    inputs           VARCHAR NOT NULL DEFAULT '[]',   -- JSON from LineageRecord.inputs
    constants        VARCHAR NOT NULL DEFAULT '[]',   -- JSON from LineageRecord.constants
    timestamp        VARCHAR NOT NULL
)
```

Insert only. `ON CONFLICT (output_record_id) DO NOTHING` — idempotent, never
overwrites. The full audit trail (every save event + timestamps) stays in
`_record_metadata`.

### Why this is simpler

Old: 3 rows for `bandpass_filter(raw, low_hz=20, high_hz=450)`:
```
(lh, "RawEMG", "variable",  "FilteredEMG", "bandpass_filter", hash, ts)
(lh, "20",     "constant",  "FilteredEMG", "bandpass_filter", hash, ts)
(lh, "450",    "constant",  "FilteredEMG", "bandpass_filter", hash, ts)
```

New: 1 row:
```
(record_id, lh, "FilteredEMG", "bandpass_filter", hash,
 '[{"name":"signal","source_type":"variable","type":"RawEMG","record_id":"xyz","metadata":{"subject":1}}]',
 '[{"name":"low_hz","value_repr":"20"},{"name":"high_hz","value_repr":"450"}]',
 ts)
```

## Audit use case: "every computation this week in order"

```sql
SELECT l.function_name, l.inputs, l.constants, rm.timestamp
FROM _lineage l
JOIN _record_metadata rm ON l.output_record_id = rm.record_id
WHERE rm.timestamp >= '2026-02-14'
ORDER BY rm.timestamp
```

---

## Implementation Steps

### 1. `_ensure_lineage_table()` in `database.py`

- Create with new schema if table doesn't exist
- Migration: if old `source` column detected, drop and recreate
  (old lineage data is incomplete/incorrect anyway — no real loss)

```python
def _ensure_lineage_table(self):
    # Migration: detect old edge-based schema by checking for 'source' column
    cols = {row[1] for row in self._duck._fetchall("PRAGMA table_info('_lineage')")}
    if cols and "source" in cols:
        self._duck._execute("DROP TABLE _lineage")

    self._duck._execute("""
        CREATE TABLE IF NOT EXISTS _lineage (
            output_record_id VARCHAR PRIMARY KEY,
            lineage_hash     VARCHAR NOT NULL,
            target           VARCHAR NOT NULL,
            function_name    VARCHAR NOT NULL,
            function_hash    VARCHAR NOT NULL,
            inputs           VARCHAR NOT NULL DEFAULT '[]',
            constants        VARCHAR NOT NULL DEFAULT '[]',
            timestamp        VARCHAR NOT NULL
        )
    """)
```

Note: DuckDB doesn't support `PRAGMA table_info` — use
`information_schema.columns` instead.

### 2. `_save_lineage()` in `database.py`

Replace per-edge loop with a single INSERT:

```python
def _save_lineage(self, output_record_id, output_type, lineage, lineage_hash=None,
                  user_id=None, schema_keys=None, output_content_hash=None):
    lh = lineage_hash or output_record_id
    inputs_json = json.dumps(lineage.inputs, sort_keys=True)
    constants_json = json.dumps(lineage.constants, sort_keys=True)
    timestamp = datetime.now().isoformat()

    self._duck._execute(
        "INSERT INTO _lineage "
        "(output_record_id, lineage_hash, target, function_name, function_hash, "
        " inputs, constants, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (output_record_id) DO NOTHING",
        [output_record_id, lh, output_type, lineage.function_name,
         lineage.function_hash, inputs_json, constants_json, timestamp],
    )
```

### 3. `save_ephemeral_lineage()` in `database.py`

Same pattern. `ephemeral_id` serves as both `output_record_id` and `lineage_hash`:

```python
def save_ephemeral_lineage(self, ephemeral_id, variable_type, lineage,
                            user_id=None, schema_keys=None):
    inputs_json = json.dumps(lineage.inputs, sort_keys=True)
    constants_json = json.dumps(lineage.constants, sort_keys=True)
    timestamp = datetime.now().isoformat()

    self._duck._execute(
        "INSERT INTO _lineage "
        "(output_record_id, lineage_hash, target, function_name, function_hash, "
        " inputs, constants, timestamp) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (output_record_id) DO NOTHING",
        [ephemeral_id, ephemeral_id, variable_type, lineage.function_name,
         lineage.function_hash, inputs_json, constants_json, timestamp],
    )
```

### 4. `get_provenance()` in `database.py`

Direct lookup by `output_record_id`, parse JSON:

```python
def get_provenance(self, variable_class, version=None, **metadata):
    if version:
        record_id = version
    else:
        var = self.load(variable_class, metadata)
        record_id = var.record_id

    rows = self._duck._fetchall(
        "SELECT function_name, function_hash, inputs, constants "
        "FROM _lineage WHERE output_record_id = ?",
        [record_id],
    )
    if not rows:
        return None
    function_name, function_hash, inputs_json, constants_json = rows[0]
    return {
        "function_name": function_name,
        "function_hash": function_hash,
        "inputs": json.loads(inputs_json),
        "constants": json.loads(constants_json),
    }
```

### 5. `get_provenance_by_schema()` in `database.py`

Join `_record_metadata` → `_schema` for schema filter, then join to `_lineage`:

```python
def get_provenance_by_schema(self, **schema_keys):
    conditions = ["rm.lineage_hash IS NOT NULL"]
    params = []
    for key, value in schema_keys.items():
        conditions.append(f's."{key}" = ?')
        params.append(_schema_str(value))
    where = " AND ".join(conditions)

    rows = self._duck._fetchall(
        f"SELECT rm.record_id "
        f"FROM _record_metadata rm "
        f"LEFT JOIN _schema s ON rm.schema_id = s.schema_id "
        f"WHERE {where}",
        params,
    )

    results = []
    for (record_id,) in rows:
        prov = self.get_provenance(None, version=record_id)
        if prov:
            prov["output_record_id"] = record_id
            results.append(prov)
    return results
```

### 6. `get_pipeline_structure()` in `database.py`

Query `_lineage` directly; parse inputs JSON to extract type names:

```python
def get_pipeline_structure(self):
    rows = self._duck._fetchall(
        "SELECT DISTINCT target, function_name, function_hash, inputs FROM _lineage"
    )
    seen = set()
    structure = []
    for target, function_name, function_hash, inputs_json in rows:
        inputs = json.loads(inputs_json)
        input_types = tuple(sorted(
            inp.get("type", inp.get("source_function", "unknown"))
            for inp in inputs
            if inp.get("source_type") != "constant"
        ))
        key = (function_name, function_hash, target, input_types)
        if key not in seen:
            seen.add(key)
            structure.append({
                "function_name": function_name,
                "function_hash": function_hash,
                "output_type": target,
                "input_types": list(input_types),
            })
    return structure
```

### 7. `has_lineage()` in `database.py`

No change needed — still queries `_record_metadata.lineage_hash IS NOT NULL`.

### 8. Update tests in `test_lineage_mode.py`

Two tests query `_lineage` directly and need column name updates:

- `test_ephemeral_mode_creates_ephemeral_lineage_entry`:
  `WHERE lineage_hash LIKE 'ephemeral:%'`
  → `WHERE output_record_id LIKE 'ephemeral:%'`

- `test_ephemeral_no_duplicate_entries`:
  `SELECT COUNT(DISTINCT lineage_hash) FROM _lineage WHERE lineage_hash LIKE 'ephemeral:%'`
  → `SELECT COUNT(DISTINCT output_record_id) FROM _lineage WHERE output_record_id LIKE 'ephemeral:%'`

Add new assertions to `TestProvenanceQueries` to verify named constants and
input metadata are preserved (this was always the intended behavior per README).

---

## Files changed

- `src/scidb/database.py`: `_ensure_lineage_table`, `_save_lineage`,
  `save_ephemeral_lineage`, `get_provenance`, `get_provenance_by_schema`,
  `get_pipeline_structure`
- `tests/test_lineage_mode.py`: update 2 direct `_lineage` queries, add richer
  provenance assertions
