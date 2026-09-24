# Plan: faster canonical_hash with byte-identical output (F30)

Status: BUILT 2026-09-24 on branch perf/fast-canonical-hash (uncommitted). RE-MEASURED by user: wide 1x54 0.746 -> 0.177 ms/record (4.2x), 30x4 0.164 -> 0.074; json.dumps 62 -> 4 calls/record. Two corpus frames (pd.NA string column, tz-aware datetime) are unserializable in the FROZEN code too — the test asserts identical raising. Oracle = tests/reference_serializer.py (verbatim dad361a4). A = fast column path via `_mgr.iget_values`, verified once per (columns, dtypes) signature against `df[col].to_numpy()`, WARN+fallback on mismatch (no batch API needed: save_batch just calls canonical_hash). B = exact-type scalar dispatch + dtype-string cache. save_batch timing line: `hash frames fast=N reference=M`.

Step 0 MEASURED (user, numpy 2.4.6 / pandas 3.0.3):

| shape | total | column_access | serialize num | serialize obj | sha256 |
|---|---|---|---|---|---|
| 1 row x 54 cols (DummyMixed-like) | 0.746 ms | **0.493 ms (66%)** | 0.059 | 0.087 | 0.002 |
| 30 rows x 4 cols (1 object col) | 0.164 ms | 0.044 | 0.005 | **0.080 (49%)** | 0.002 |

62 / 34 json.dumps calls per record. cProfile: pandas `DataFrame.__getitem__`
(arrow-backed column index under pandas 3) dominates the wide case;
`numpy dtype __str__` (str(dtype) per array) shows in both. Order: **A first**
(wide one-row records, the save_batch shape), then **B** (object columns;
plus caching str(dtype)).

## Why
`save_batch` spends almost all of `per_row_hashing` in `canonical_hash(data)`:
7.46 s of 8.25 s for 7,000 one-row x 54-column DataFrames (~1.07 ms/record);
MATLAB runs ~3-4 ms/row (§4.3). SHA-256 over a few KB is microseconds; the
cost is Python around it:
1. `df[col].to_numpy()` per column per record (pandas fixed overhead);
2. object columns (arrays, dicts, mixed) go `tolist()` + recursion, with one
   `json.dumps` call PER SCALAR.

## Hard constraint: identical bytes
The content hash feeds `record_id`. Any change in the serialized bytes makes
every existing record look new (duplicates, lost skip_computed, broken
lineage) and would need a migration — ruled out (beta, no migrations). So:
no new algorithm, no format change. Every optimization below must produce
exactly `_serialize_for_hash`'s bytes.

## Levers
**A. Batch column extraction** (`canonical_hash_batch(frames)`). Frames of
one variable normally share columns and dtypes: group by
`(tuple(columns), tuple(dtypes))`, concat each group once, `to_numpy()` each
column once, slice per record. A numeric slice `arr[i:i+n]` has the same
shape/dtype/bytes as that record's own `to_numpy()`. Only for groups with an
exact dtype signature match (concat could upcast int->float with NaN);
anything else takes the per-record path.

**B. Same-bytes fast serializer** for Python scalars/containers: exact-type
dispatch (float -> `repr` with NaN/Infinity special-cased as json does;
int -> `str`; bool -> `true/false`; None -> `null`), `json.dumps` kept for
`str` (escaping is where the risk is) and anything unusual. Replaces the
per-scalar `json.dumps` inside object columns.

**C.** Numeric columns already hash raw bytes; A removes the pandas overhead
of reaching them.

Not doing: faster hash algorithm (changes ids), threads (GIL-bound), skipping
the hash (it is identity).

## Safety
1. Golden tests: a corpus of shapes (scalars incl. NaN/+-inf/-0.0/huge ints,
   bools, None, unicode/escaped strings, nested dict/list/tuple, numpy
   scalars, object/mixed columns, multi-row frames, column order, index
   variants, array.array) — new path == `_serialize_for_hash` bytes, and
   `canonical_hash_batch` == `[canonical_hash(f) for f in frames]`.
2. Runtime cross-check: the first record of every batch group also goes
   through the reference path; on mismatch, WARN with the column/type and
   the whole batch falls back to the reference path. A bug costs speed,
   never identity.
3. Timing: `save_batch` keeps `canonical_hash=`; adds `hash_fast_groups` /
   `hash_fallback_records` counts to the [timing] line.

## Stages
0. User runs the profiler (column_access vs serialize_obj vs sha256).
1. Golden corpus + reference-equality tests (before any optimization).
2. The larger of A / B per step 0, behind the runtime cross-check.
3. The other one.
4. `save_batch` uses `canonical_hash_batch`; re-measure on the 7k test
   (`TestForEachTimingInstrumentation`) and a real MATLAB save.

Owner: `scicanonicalhash` (the hash's one home); `scidb.database.save_batch`
only calls it.
