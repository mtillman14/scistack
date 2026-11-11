Here is Claude's plan:  
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
Plan: Database Schema Refactor — Version Keys, Merged IDs, Unified Database

Context

Three problems being solved simultaneously:

1.  for_each() config is invisible to versioning: Changing where=, Fixed, Merge, ColumnSelection, or other kwargs produces different output but saves under the same parameter slot. No audit trail of
    what configuration produced what data.
2.  parameter_id and version_id are redundant: parameter_id is just a pre-computed integer alias for version_keys JSON. version_id is a counter that doesn't handle parameter reverts (if you go from
    smoothing=0.2 → 0.3 → 0.2, the reverted 0.2 gets a new version_id instead of recognizing it's the same computation). Replace both with (record_id, timestamp) — content-addressed data with an
    append-only activity log.
3.  Two databases are unnecessary complexity: PipelineDB (SQLite) stores lineage separately from DuckDB data. They're connected only by record_id strings. Merge into one DuckDB database for
    transactional consistency and simpler deployment.

No migration or backwards compatibility is needed. MATLAB updates deferred to later.

New Schema

\_schema — UNCHANGED

(schema_id INTEGER PK, schema_level VARCHAR, "subject" VARCHAR, "session" VARCHAR, ...)

\_registered_types — UNCHANGED

(type_name VARCHAR PK, table_name VARCHAR, schema_version INTEGER, registered_at TIMESTAMP)

\_variable_groups — UNCHANGED

(group_name VARCHAR, variable_name VARCHAR, PK(group_name, variable_name))

\_variables — SIMPLIFIED (one row per variable, not per parameter set)

CREATE TABLE \_variables (
variable_name VARCHAR PRIMARY KEY,
schema_level VARCHAR NOT NULL,
dtype VARCHAR,
created_at TIMESTAMP DEFAULT current_timestamp,
description VARCHAR DEFAULT ''
)
Removed: parameter_id, version_keys. version_keys moves to \_record_metadata. dtype is stored once per variable (updated on save if needed).

\_record_metadata — REDESIGNED

CREATE TABLE \_record_metadata (
record_id VARCHAR NOT NULL,
timestamp VARCHAR NOT NULL,
variable_name VARCHAR NOT NULL,
schema_id INTEGER NOT NULL,
version_keys VARCHAR DEFAULT '{}',
content_hash VARCHAR,
lineage_id VARCHAR,
schema_version INTEGER,
user_id VARCHAR,
PRIMARY KEY (record_id, timestamp)
)
Changes:

- PK is (record_id, timestamp) — same record_id can appear multiple times (reverts, re-runs)
- parameter_id/version_id removed
- version_keys moved here from \_variables — each record knows its own config
- lineage_hash renamed to lineage_id — references target_id in \_lineage
- Every execution inserts a new row (even identical re-runs), creating a full audit trail
- Data deduplication happens in the data table via record_id PK

Data tables — SIMPLIFIED (one per variable)

CREATE TABLE "{VarName}\_data" (
record_id VARCHAR PRIMARY KEY,
...data_columns...
)
Only record_id + data columns. No schema_id, parameter_id, version_id. Deduplicated by content — same data = same record_id = ON CONFLICT DO NOTHING.

\_lineage — NEW (schema-blind DAG, replaces SQLite PipelineDB)

CREATE TABLE \_lineage (
source VARCHAR NOT NULL,
target VARCHAR NOT NULL,
function_name VARCHAR NOT NULL,
function_hash VARCHAR NOT NULL,
timestamp VARCHAR NOT NULL,
PRIMARY KEY (source, function_name, target, timestamp)
)

This is a schema-blind, type-level DAG describing the pipeline structure. \_record_metadata is schema-aware and execution-context-aware; \_lineage is purely about the computation graph.

Semantics:

- One row per (input, output) pair per for_each() call — NOT per iteration/subject
- source: human-readable string identifying the input
  - For variable inputs: the class name (e.g., "RawEMG")
  - For JSON-ifiable constants: JSON string of the value (e.g., "0.2", '"baseline"', '[1, 2, 3]')
  - For non-JSON-ifiable constants: repr() of the value
- target: the output variable class name (e.g., "FilteredEMG")
- function_name / function_hash: the function that produces the output
- timestamp: when this DAG edge was created/confirmed

Example: for_each(bandpass_filter, inputs={"signal": RawEMG, "smoothing": 0.2}, outputs=[FilteredEMG])

┌────────┬─────────────┬─────────────────┬───────────────┬───────────┐
│ source │ target │ function_name │ function_hash │ timestamp │
├────────┼─────────────┼─────────────────┼───────────────┼───────────┤
│ RawEMG │ FilteredEMG │ bandpass_filter │ abc │ T1 │
├────────┼─────────────┼─────────────────┼───────────────┼───────────┤
│ 0.2 │ FilteredEMG │ bandpass_filter │ abc │ T1 │
└────────┴─────────────┴─────────────────┴───────────────┴───────────┘

Connection to \_record_metadata: The lineage_id column in \_record_metadata links a specific data record to its place in the pipeline DAG. lineage_id can be constructed from (target, function_name,
function_hash) or stored as a hash of those. The constant's actual value is recoverable from both the source column directly and from \_record_metadata.version_keys.

Edge obsolescence: Each for_each() call inserts a complete set of edges for its output(s), all with the same timestamp. The "current" DAG is the edges with the latest timestamp per (target,
function_name) (NOT per (source, target, function_name)):
WITH latest AS (
SELECT target, function_name, MAX(timestamp) as max_ts
FROM \_lineage GROUP BY target, function_name
)
SELECT l.\* FROM \_lineage l
JOIN latest lt ON l.target = lt.target
AND l.function_name = lt.function_name
AND l.timestamp = lt.max_ts
Why group by (target, function_name) and not (source, target, function_name)? Because for_each() writes a complete set of edges per call. If an input is removed (e.g., smoothing=0.2 replaced by
smoothing=0.3), grouping by (source, ...) would leave the old (0.2, FilteredEMG, filter, T1) edge as "current" since no newer edge with source=0.2 exists. Grouping by (target, function_name) means
ALL edges for (FilteredEMG, filter) at T2 replace ALL edges at T1, correctly dropping removed inputs.

Old edges become stale automatically — no deletion needed, no user-facing syntax. for_each() must re-insert ALL edges (including unchanged ones) so they stay in the latest timestamp group.

Key Behavioral Changes

"Latest" query

WITH ranked AS (
SELECT rm._, ROW_NUMBER() OVER (
PARTITION BY rm.variable_name, rm.schema_id, rm.version_keys
ORDER BY rm.timestamp DESC
) AS rn
FROM \_record_metadata rm
WHERE rm.variable_name = ?
)
SELECT _ FROM ranked WHERE rn = 1
Groups by version_keys instead of parameter_id. Each distinct version_keys combo gets its own "latest" row.

Idempotency

Every save() call inserts a new (record_id, timestamp) row in \_record_metadata. The data table uses ON CONFLICT (record_id) DO NOTHING — data is only stored once. Full audit trail of executions +
deduplicated data storage.

record_id generation — UNCHANGED

record_id = hash(class_name, schema_version, content_hash, nested_metadata) — same function in canonical-hash/src/canonicalhash/hashing.py. nested_metadata["version"] now includes for_each config
keys.

configure_database() — SIMPLIFIED

def configure_database(
dataset_db_path: str | Path,
dataset_schema_keys: list[str],
pipeline_db_path: str | Path = None, # deprecated, ignored
lineage_mode: str = "strict",
) -> DatabaseManager:

Performance Impact

The refactor is a net performance improvement, primarily on the save path:

Change: Eliminate \_resolve_parameter_slot()
Effect: Big win — removes 2-3 queries per save()
Magnitude: At 2000 iterations, saves 4,000-6,000+ queries per for_each()
────────────────────────────────────────
Change: \_find_record() latest query
Effect: Win — eliminates Python-side json.loads() per row
Magnitude: O(N) JSON parsing removed
────────────────────────────────────────
Change: Data table SELECT on load
Effect: Win — WHERE record_id = ? (1 column) vs 3-column composite
Magnitude: Simpler index lookup
────────────────────────────────────────
Change: Lineage SQLite → DuckDB
Effect: Win — no cross-DB coordination
Magnitude: Eliminates SQLite connection overhead
────────────────────────────────────────
Change: String vs integer in PARTITION BY
Effect: Slight slowdown in filter resolution
Magnitude: Negligible — DuckDB dictionary encoding makes VARCHAR comparison near-integer speed for small cardinality (1-10 distinct version_keys)
────────────────────────────────────────
Change: Data table INSERT with PK index
Effect: Neutral — PK index overhead offset by built-in ON CONFLICT dedup
Magnitude: Replaces explicit idempotency SELECT

Key insight: The current \_resolve_parameter_slot() already does JSON string comparison (version_keys = ?) on every save. The refactor moves this comparison into DuckDB's PARTITION BY (which
benefits from dictionary encoding) and eliminates the extra round-trip queries.

Implementation Phases

Phase 0: Add to_key() serialization methods

Pure additions, no behavioral changes.

src/scidb/filters.py — add abstract to_key() to Filter, implement on all 6 subclasses:

- VariableFilter: f"{cls.**name**} {op} {value!r}" → "Side == 'R'"
- ColumnFilter: f"{cls.**name**}['{col}'] {op} {value!r}" → "MyVar['Side'] == 'L'"
- InFilter: f"{cls.**name**}['{col}'] IN {sorted(values)}" → "MyVar['Side'] IN ['L', 'R']"
- CompoundFilter: f"({left.to_key()}) {op} ({right.to_key()})" → "(Side == 'R') AND (Speed > 1.2)"
- NotFilter: f"NOT ({inner.to_key()})" → "NOT (Side == 'R')"
- RawFilter: f"RAW: {sql}" → "RAW: \"Side\" = 'L'"

scirun-lib/src/scirun/column_selection.py — add to_key():

- f"{var_type.**name**}[{columns!r}]" → "MyVar['col_a', 'col_b']"

scirun-lib/src/scirun/fixed.py — add to_key():

- f"Fixed({inner_key}, {sorted kv pairs})" → "Fixed(RawEMG, session='baseline')"
- If var_type is a ColumnSelection, calls its to_key() for the inner part

scirun-lib/src/scirun/merge.py — add to_key():

- f"Merge({', '.join(spec_keys)})" → "Merge(VarA, Fixed(VarB, session='A'))"
- Recursively calls to_key() on each spec

Phase 1: Merge PipelineDB into DuckDB

Only changes WHERE lineage is stored. Replaces old SQLite lineage table with new DuckDB \_lineage DAG table.

src/scidb/database.py:

- Add \_ensure_lineage_table(): creates new \_lineage table in DuckDB (the DAG schema above)
- **init**(): call \_ensure_lineage_table() instead of creating PipelineDB instance
- Rewrite \_save_lineage(): INSERT edges into DuckDB \_lineage. For each for_each call, insert one row per (input, output) pair. Compute target_id = hash(variable_name, function_name, function_hash).
  Compute source_id = hash of variable class name (for variable inputs) or canonical_hash of value (for constants).
- Rewrite save_ephemeral_lineage(): same pattern
- Rewrite find_by_lineage(): query DuckDB \_lineage by target_id (replaces lineage_hash lookup)
- Rewrite get_provenance(), get_provenance_by_schema(), get_pipeline_structure(), has_lineage(): query DuckDB \_lineage
- close() / reopen(): remove PipelineDB lifecycle calls
- configure_database(): make pipeline_db_path optional with deprecation warning

Phase 2: Simplify data tables

Change data tables from (schema_id, parameter_id, version_id, ...data) to (record_id PK, ...data).

src/scidb/database.py:

- \_save_columnar(): create table with record_id VARCHAR PRIMARY KEY + data columns. Insert with ON CONFLICT (record_id) DO NOTHING.
- \_save_single_value(): same change
- save_batch(): build data rows as (record_id, ...data). Bulk insert with conflict handling.
- \_load_by_record_row(): SELECT \* FROM "{table}" WHERE record_id = ?
- load_all(): bulk fetch by record_id IN (...)
- \_create_variable_view(): update VIEW to join data table via record_id through \_record_metadata

sciduck/src/sciduck/sciduck.py:

- \_ensure_variable_table(): create with record_id VARCHAR PRIMARY KEY + data columns
- save(): accept record_id, insert with ON CONFLICT DO NOTHING
- load(): load by record_id

src/scidb/filters.py:

- \_resolve_variable_schema_ids(): rewrite to join data table through \_record_metadata using record_id
- \_get_all_schema_ids_for_variable(): same

Phase 3: Merge parameter_id/version_id into (record_id, timestamp)

Remove parameter_id and version_id everywhere.

src/scidb/database.py:

- \_ensure_record_metadata_table(): new schema with (record_id, timestamp) PK, version_keys, lineage_id columns
- \_save_record_metadata(): always INSERT new (record_id, timestamp) row
- Remove \_resolve_parameter_slot() entirely
- save(): compute version_keys from non-schema metadata, always insert metadata row with current timestamp, insert data with ON CONFLICT DO NOTHING
- save_batch(): no parameter_id resolution. For each item: compute record_id, insert data (dedup), insert metadata (always).
- \_find_record(): rewrite "latest" CTE to PARTITION BY variable_name, schema_id, version_keys ORDER BY timestamp DESC. Version key filtering on \_record_metadata.version_keys.
- load_all(): remove parameter_id-based dtype lookup. dtype stored once per variable in \_variables.

sciduck/src/sciduck/sciduck.py:

- Simplify \_variables to one row per variable (no parameter_id in PK)
- Remove \_next_parameter_id(), \_latest_parameter_id()
- Update list_variables(), list_versions(), delete()

src/scidb/variable.py:

- BaseVariable.**init**(): remove self.version_id, self.parameter_id
- \_results_to_dataframe(): remove version_id from output
- load_all(): remove version_id parameter

src/scidb/filters.py:

- All queries: replace PARTITION BY ... parameter_id ... ORDER BY version_id DESC with PARTITION BY ... version_keys ORDER BY timestamp DESC

Phase 4: Store for_each() config as version keys

Wire everything together.

scirun-lib/src/scirun/foreach_config.py — NEW FILE:
class ForEachConfig:
"""Serializes for_each() computation config into version keys."""

     def __init__(self, fn, inputs, where=None, distribute=False,
                  as_table=None, pass_metadata=None):
         ...

     def to_version_keys(self) -> dict:
         """Return dict of config keys to merge into save_metadata."""
         keys = {}
         keys["fn"] = getattr(self.fn, "__name__", repr(self.fn))
         keys["inputs"] = self._serialize_inputs()  # JSON of loadable input specs
         if self.where is not None:
             keys["where"] = self.where.to_key()
         if self.distribute:
             keys["distribute"] = True
         if self.as_table:
             keys["as_table"] = self.as_table
         if self.pass_metadata is not None:
             keys["pass_metadata"] = self.pass_metadata
         return keys

     def _serialize_inputs(self) -> str:
         """Serialize loadable inputs to a canonical JSON string."""
         from .foreach import _is_loadable
         result = {}
         for name in sorted(self.inputs):
             spec = self.inputs[name]
             if _is_loadable(spec):
                 if hasattr(spec, 'to_key'):
                     result[name] = spec.to_key()
                 elif isinstance(spec, type):
                     result[name] = spec.__name__
         return json.dumps(result, sort_keys=True)

scirun-lib/src/scirun/foreach.py:

- Import ForEachConfig
- Near top of for_each(), construct config and compute keys once:
  config = ForEachConfig(fn=fn, inputs=inputs, where=where,
  distribute=distribute, as_table=as_table,
  pass_metadata=pass_metadata)
  config_keys = config.to_version_keys()
- At save time (~line 299):
  save_metadata = {**metadata, **constant_inputs, \*\*config_keys}
- Also at save time: insert \_lineage DAG edges (one call per for_each, not per iteration):
  if not dry_run and save:
  db.save_lineage_edges(fn, inputs, outputs) # inserts DAG edges

src/scidb/variable.py — overload where= in load():

- When where is a Filter object: serialize via to_key(), add "where": where.to_key() to metadata for version_keys matching. Also apply as schema_id filter (existing behavior).
- Both uses happen simultaneously — version_keys matching narrows by config, schema_id filtering narrows by data condition.

Files to modify (summary)

┌─────────────────────────────────────────────┬────────────┬──────────────────────────────────────────────────────────────────────────────────────────────┐
│ File │ Phase │ Changes │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ src/scidb/filters.py │ 0, 2, 3 │ Add to_key(); rewrite filter queries │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ scirun-lib/src/scirun/column_selection.py │ 0 │ Add to_key() │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ scirun-lib/src/scirun/fixed.py │ 0 │ Add to_key() │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ scirun-lib/src/scirun/merge.py │ 0 │ Add to_key() │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ src/scidb/database.py │ 1, 2, 3, 4 │ Lineage merger; data table simplification; parameter/version removal; lineage edge insertion │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ sciduck/src/sciduck/sciduck.py │ 2, 3 │ Data table simplification; \_variables simplification │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ scirun-lib/src/scirun/foreach_config.py │ 4 │ NEW — ForEachConfig class │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ scirun-lib/src/scirun/foreach.py │ 4 │ Serialize config into save_metadata; insert DAG edges │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ src/scidb/variable.py │ 3, 4 │ Remove parameter_id/version_id; overload where= in load() │
├─────────────────────────────────────────────┼────────────┼──────────────────────────────────────────────────────────────────────────────────────────────┤
│ canonical-hash/src/canonicalhash/hashing.py │ — │ No changes needed │
└─────────────────────────────────────────────┴────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘

Verification

After each phase, run:
pytest scirun-lib/tests/ && pytest src/tests/ && pytest sciduck/tests/

End-to-end verification after all phases:

1.  configure_database("test.duckdb", ["subject", "trial"]) — no pipeline_db_path
2.  Save data manually and via for_each with various configs
3.  Verify MyVar.load(smoothing=0.2) returns correct version_keys group
4.  Verify changing for_each inputs/where/fn creates new version_keys groups
5.  Verify reverting parameters re-uses same record_id with new timestamp
6.  Verify MyVar.load(where=Side=="R") works as both filter and version key match
7.  Verify lineage DAG: get_provenance() traces the computation graph
8.  Verify idempotent re-runs insert new metadata rows but don't duplicate data
9.  Verify \_lineage has one set of edges per for_each call (schema-blind)
