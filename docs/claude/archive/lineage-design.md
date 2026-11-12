# Lineage / Provenance Design

> **Archived 2026-11-10** (grug follow-up, one live doc per concept). Superseded by
> database-model.md (the as-built provenance model) and lineage-simplification.md (its rationale). Kept for history; do not treat as current.

## Overview

Lineage tracks how each saved variable was computed: which function was
called, which saved variables were its inputs, and what constant values
were passed.  The system has two separate concerns:

- **`_lineage`** — provenance lookup: "how was this computed?"
- **`_record_metadata`** — audit trail: "when was this saved, and by whom?"

## `_lineage` Table Schema

One row per unique computation output (keyed by `output_record_id`):

```sql
CREATE TABLE _lineage (
    output_record_id VARCHAR PRIMARY KEY,
    lineage_hash     VARCHAR NOT NULL,   -- content-addressed computation ID
    target           VARCHAR NOT NULL,   -- output variable type name
    function_name    VARCHAR NOT NULL,
    function_hash    VARCHAR NOT NULL,
    inputs           VARCHAR NOT NULL DEFAULT '[]',   -- JSON
    constants        VARCHAR NOT NULL DEFAULT '[]',   -- JSON
    timestamp        VARCHAR NOT NULL
)
```

Insert only. `ON CONFLICT (output_record_id) DO NOTHING` — idempotent.
The same computation always produces the same `output_record_id` (content-
addressed hash), so re-running a pipeline never creates duplicate rows.

### `inputs` JSON format

Each element represents one non-constant argument to the function:

```json
[
  {
    "name": "signal",
    "source_type": "variable",
    "type": "RawEMG",
    "record_id": "abc123...",
    "metadata": {"subject": 1}
  }
]
```

In MATLAB the argument names are positional (`"arg_0"`, `"arg_1"`, …)
because MATLAB cannot introspect parameter names at runtime.

### `constants` JSON format

Each element represents one constant (non-variable) argument:

```json
[
  {"name": "low_hz",  "value_repr": "20"},
  {"name": "high_hz", "value_repr": "450"}
]
```

Again, MATLAB uses `"arg_N"` names.

## Why Two Tables?

`_record_metadata` gets a new row every time a variable is saved (many
saves of the same computation = many rows, each with its own timestamp).
`_lineage` stores each unique computation exactly once.

To answer "every computation this week in timestamp order":

```sql
SELECT l.function_name, l.inputs, l.constants, rm.timestamp
FROM _lineage l
JOIN _record_metadata rm ON l.output_record_id = rm.record_id
WHERE rm.timestamp >= '2026-02-14'
ORDER BY rm.timestamp
```

## `DatabaseManager` API

| Method | Description |
|---|---|
| `get_provenance(cls, **meta)` | Returns `{function_name, function_hash, inputs, constants}` for the latest matching record, or `None`. |
| `get_provenance(None, version=record_id)` | Looks up provenance directly by `record_id`. |
| `get_provenance_by_schema(**schema_keys)` | Returns list of provenance dicts for all records matching the schema filter. |
| `get_pipeline_structure()` | Returns the unique set of `(function, input_types → output_type)` edges across all stored lineage. |
| `has_lineage(cls, **meta)` | Returns True if the latest matching record has a `lineage_hash`. |

## Python vs MATLAB Argument Names

Python `@lineage_fcn`-decorated functions use `inspect.signature()` to capture
the actual parameter names, so constants appear as `{"name": "low_hz", …}`.

MATLAB `scidb.LineageFcn` passes inputs as `{"arg_0": ..., "arg_1": ..., …}`,
so constants appear as `{"name": "arg_1", …}`.  The value is still fully
preserved in `value_repr`.

## Provenance in MATLAB

`BaseVariable.provenance()` calls `db.get_provenance()` in Python and
converts the result through:

```
Python dict → pylist_to_cell → pydict_to_struct → MATLAB struct
```

The result is a struct with fields:
- `prov.function_name` — string
- `prov.function_hash` — 64-char hex string
- `prov.inputs` — cell array of structs (`name`, `source_type`, `type`,
  `record_id`, `metadata`)
- `prov.constants` — cell array of structs (`name`, `value_repr`)

Returns `[]` if no lineage was recorded (e.g. raw data saved directly).
