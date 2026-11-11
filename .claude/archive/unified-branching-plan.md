Plan: Unified Branch Parameter Tracking (Items 2 + 3)

Context

Two related gaps prevent parameter sweeps from working correctly:

Item 2 (write-time collision): ForEachConfig omits scalar constants from the version key. Two for_each calls differing only in a constant (e.g., low_hz=20 vs
low_hz=30) produce identical version keys — the second run silently displaces the first.

Item 3 (read-time discovery): No load API for filtering by the parameters that caused branching — whether those are scalar constants (low_hz=20) or dynamic input
column values (Side="L" from a CSV). The user must know to use version=record_id which is opaque.

What already exists:

- Dynamic input discriminators (e.g. Side) already flow through \_save_results() → \_split_metadata() → version_keys. They're tracked, just not exposed.
- where= filters which input rows to process before running. Unchanged by this plan.
- version= selects a record by record_id. Remains as an escape hatch.

The solution: Add a branch_params column to \_record_metadata. This is a flat JSON dict containing ALL branch-discriminating parameters for a record — both scalar
constants (namespaced by function) and dynamic input column values (bare names). It's accumulated incrementally: each saved record inherits upstream records'
branch_params and adds its own step's discriminators. This enables a unified, intuitive load API that works the same regardless of how branching occurred.

---

Target Python API

# ─── Setup ────────────────────────────────────────────────────────────────────

db = configure_database("experiment.duckdb", ["subject", "session"])

class RawSignal(BaseVariable): pass
class FilteredSignal(BaseVariable): pass
class DetectedSpikes(BaseVariable): pass

# ─── Save raw data — no branch params ────────────────────────────────────────

RawSignal.save(raw_array, subject="S01", session="1")

# ─── Parameter sweep (scalar constants) ──────────────────────────────────────

for_each(bandpass_filter,
inputs={"signal": RawSignal, "low_hz": 20, "high_hz": 450},
outputs=[FilteredSignal], subject=[], session=[])
for_each(bandpass_filter,
inputs={"signal": RawSignal, "low_hz": 30, "high_hz": 450},
outputs=[FilteredSignal], subject=[], session=[])

# Two FilteredSignal variants per schema location:

# branch_params = {"bandpass_filter.low_hz": 20, "bandpass_filter.high_hz": 450}

# branch_params = {"bandpass_filter.low_hz": 30, "bandpass_filter.high_hz": 450}

# ─── Parameter sweep (dynamic input column) ───────────────────────────────────

# "Side" comes from a CSV variable, takes values "L" and "R"

for_each(compute_spectrum,
inputs={"signal": RawSignal, "side_info": SideTable},
outputs=[Spectrum], subject=[], session=[])

# Two Spectrum variants per schema location:

# branch_params = {"Side": "L"}

# branch_params = {"Side": "R"}

# ─── Downstream step inherits upstream branch_params ─────────────────────────

for_each(detect_spikes,
inputs={"signal": FilteredSignal, "threshold": 0.5},
outputs=[DetectedSpikes], subject=[], session=[])
for_each(detect_spikes,
inputs={"signal": FilteredSignal, "threshold": 0.6},
outputs=[DetectedSpikes], subject=[], session=[])

# Four DetectedSpikes variants per schema location:

# {"bandpass_filter.low_hz": 20, "bandpass_filter.high_hz": 450, "detect_spikes.threshold": 0.5}

# {"bandpass_filter.low_hz": 20, "bandpass_filter.high_hz": 450, "detect_spikes.threshold": 0.6}

# {"bandpass_filter.low_hz": 30, "bandpass_filter.high_hz": 450, "detect_spikes.threshold": 0.5}

# {"bandpass_filter.low_hz": 30, "bandpass_filter.high_hz": 450, "detect_spikes.threshold": 0.6}

# ─── Load: unambiguous — one variant ─────────────────────────────────────────

raw = RawSignal.load(subject="S01", session="1") # fine, one record

# ─── Load: ambiguous without filter → helpful error ──────────────────────────

spikes = DetectedSpikes.load(subject="S01", session="1")

# → AmbiguousVersionError:

# 4 variants exist for DetectedSpikes at subject=S01, session=1.

# Specify branch parameters to select one:

# low_hz=20, high_hz=450, threshold=0.5 (record_id: "abc")

# low_hz=20, high_hz=450, threshold=0.6 (record_id: "def")

# low_hz=30, high_hz=450, threshold=0.5 (record_id: "ghi")

# low_hz=30, high_hz=450, threshold=0.6 (record_id: "jkl")

# ─── Load: filter by constants (bare name works when unambiguous) ─────────────

spikes = DetectedSpikes.load(subject="S01", session="1", low_hz=20, threshold=0.5)

# ─── Load: filter by dynamic input column value ───────────────────────────────

spectrum = Spectrum.load(subject="S01", session="1", Side="L")

# ─── Load: mixed — constant + dynamic ────────────────────────────────────────

# (hypothetical downstream step that took both FilteredSignal and SideTable)

result = Analysis.load(subject="S01", session="1", low_hz=20, Side="L")

# ─── Load: namespaced key when same name appears in two steps ─────────────────

# ("threshold" used in both filter_step and detect_spikes)

spikes = DetectedSpikes.load(subject="S01", \*\*{"filter_step.threshold": 0.1,
"detect_spikes.threshold": 0.5})

# ─── Load: by record_id (always unambiguous) ─────────────────────────────────

spikes = DetectedSpikes.load(subject="S01", session="1", version="abc")

# ─── Inspect branch_params on a loaded variable ──────────────────────────────

print(spikes.branch_params)

# → {"bandpass_filter.low_hz": 20, "bandpass_filter.high_hz": 450, "detect_spikes.threshold": 0.5}

# ─── List all variants ────────────────────────────────────────────────────────

versions = db.list_versions(DetectedSpikes, subject="S01", session="1")

# [

# {"record_id": "abc",

# "schema": {"subject": "S01", "session": "1"},

# "branch_params": {"bandpass_filter.low_hz": 20, ..., "detect_spikes.threshold": 0.5},

# "timestamp": "..."},

# ... (4 total)

# ]

# ─── list_versions filtered by branch params ─────────────────────────────────

low20 = db.list_versions(DetectedSpikes, subject="S01", session="1", low_hz=20)

# Returns 2 entries — only the low_hz=20 branch

# ─── Loop over all variants for plotting ─────────────────────────────────────

for v in db.list_versions(DetectedSpikes, subject="S01", session="1"):
spikes = DetectedSpikes.load(subject="S01", session="1", version=v["record_id"])
bp = v["branch_params"]
plot(spikes, label=f"low_hz={bp['bandpass_filter.low_hz']}, threshold={bp['detect_spikes.threshold']}")

# ─── Further computation — branch_params propagates automatically ─────────────

for_each(compute_firing_rate,
inputs={"spikes": DetectedSpikes},
outputs=[FiringRate], subject=[], session=[])

# FiringRate gets 4 variants, each inheriting the full branch_params chain

---

branch_params Dict Format

Constants (scalar kwargs passed to for_each) → namespaced by function name:
{"bandpass_filter.low_hz": 20, "bandpass_filter.high_hz": 450}

Dynamic input discriminators (non-schema columns from input DataFrames that vary across rows and appear in the result table) → bare names:
{"Side": "L"}

Inherited from upstream records: union of all input records' branch_params.

Constant name collision: if two steps use the same constant name (e.g., both use threshold), they're distinguished by their function prefix (filter_step.threshold vs
detect_spikes.threshold). The bare name threshold in a load call matches via suffix search; an AmbiguousParamError is raised if multiple functions used that name.

Dynamic input column name collision: Within a single for_each call, column conflicts can't arise — pandas renames duplicates before \_save_results sees the result
table. Cross-step conflicts (Step 1 stores {"Side": "L"}, Step 2 also contributes a "Side" key with a different value) are very unlikely in practice: a column named
Side almost always means the same thing throughout a scientific pipeline. If both values agree, there's no conflict. If they conflict, it typically indicates a
pipeline data-mixing error. Handling: last writer wins; emit a warning during save when a branch_params key is overwritten with a different value, pointing the user
to version=record_id for precise selection.

---

Matching logic at load time

Non-schema, non-version kwargs in load() and list_versions() are treated as branch_params filters:

def \_match_branch_param(branch_params_dict, key, value): # 1. Exact match (covers both bare dynamic names and namespaced constant names)
if key in branch_params_dict:
return branch_params_dict[key] == value # 2. Suffix match for bare constant names (e.g. "low_hz" → "bandpass_filter.low_hz")
suffix = f".{key}"
hits = [(k, v) for k, v in branch_params_dict.items() if k.endswith(suffix)]
if len(hits) == 1:
return hits[0][1] == value
if len(hits) > 1:
raise AmbiguousParamError(f"'{key}' matches multiple branch params: {[h[0] for h in hits]}")
return False

---

Implementation

1.  \_record_metadata schema — add branch_params and excluded columns

File: scidb/src/scidb/database.py:460–475 (\_ensure_record_metadata_table)

branch_params VARCHAR DEFAULT '{}',
excluded BOOLEAN DEFAULT FALSE

No migration needed (development only). Also update \_create_variable_view() (line 492) to include both columns in the SELECT.

---

2.  Variant exclusion API

File: scidb/src/scidb/database.py

Both exclude_variant and include_variant accept either:

- A bare record_id string (direct, opaque)
- A variable class + schema/branch kwargs (intuitive, mirrors load() exactly)

def exclude_variant(self, record_id_or_type, \*\*kwargs) -> None:
"""Mark a variant as excluded from automatic inclusion in for_each and load().

     Usage:
         db.exclude_variant("abc123")                                     # by record_id
         db.exclude_variant(DetectedSpikes, subject="S01", low_hz=20)    # by params
     """
     record_id = self._resolve_record_id(record_id_or_type, **kwargs)
     self._duck._execute(
         "UPDATE _record_metadata SET excluded = TRUE WHERE record_id = ?",
         [record_id]
     )

def include_variant(self, record_id_or_type, **kwargs) -> None:
"""Re-include a previously excluded variant."""
record_id = self.\_resolve_record_id(record_id_or_type, **kwargs)
self.\_duck.\_execute(
"UPDATE \_record_metadata SET excluded = FALSE WHERE record_id = ?",
[record_id]
)

def \_resolve_record_id(self, record_id_or_type, **kwargs) -> str:
"""Resolve a record_id string or (variable_class, **kwargs) to a single record_id.
Raises AmbiguousVersionError if multiple records match, NotFoundError if none."""
if isinstance(record_id_or_type, str):
return record_id_or_type # Variable class path — reuse load() resolution logic
results = self.\_find_record_by_type(record_id_or_type, include_excluded=True, \*\*kwargs)
if len(results) == 0:
raise NotFoundError(...)
if len(results) > 1:
raise AmbiguousVersionError(...)
return results[0]["record_id"]

\_find_record() adds AND NOT rm.excluded to SQL by default. Add include_excluded: bool = False parameter to opt out — used by list_versions(...,
include_excluded=True), load(..., include_excluded=True), and \_resolve_record_id.

list_versions() includes "excluded": bool in each returned dict.

Typical workflows:

# By record_id (from list_versions or loaded variable)

spikes = DetectedSpikes.load(subject="S01", low_hz=20, threshold=0.5)
db.exclude_variant(spikes.record_id)

# By branch params (most intuitive)

db.exclude_variant(DetectedSpikes, subject="S01", session="1", low_hz=20, threshold=0.5)

# Re-include

db.include_variant(DetectedSpikes, subject="S01", session="1", low_hz=20, threshold=0.5)

# See all variants including excluded ones

all_v = db.list_versions(DetectedSpikes, subject="S01", include_excluded=True)

---

3.  \_save_record_metadata() — accept and store branch_params

File: scidb/src/scidb/database.py:547–573

Add branch_params: dict | None parameter; store as JSON in the new column.

---

4.  ForEachConfig.to_version_keys() — add direct constants

File: scidb/src/scidb/foreach_config.py:35–51

Add helper \_get_direct_constants() that filters self.inputs via \_is_loadable():

def \_get_direct_constants(self) -> dict:
from .foreach import \_is_loadable
return {k: v for k, v in self.inputs.items() if not \_is_loadable(v)}

In to_version_keys(), serialize them under \_\_constants:

direct = self.\_get_direct_constants()
if direct:
keys["__constants"] = json.dumps(direct, sort_keys=True)

This fixes the version key collision bug (item 2).

---

5.  \_load_var_type_all() — attach \_\_branch_params column to DataFrame

File: scidb/src/scidb/foreach.py:301–351

When building the row dict for each loaded variable, add:
row["__branch_params"] = getattr(var, "branch_params", "{}")

branch_params is attached to the variable at load time (step 7 below).

---

6.  \_save_results() — compute branch_params for each result row

File: scidb/src/scidb/foreach.py:358–388

For each result row, before calling output_obj.save():

1.  Collect upstream branch_params: merge all \_\_branch_params values from input columns in the row.
2.  Add constants: from config_keys["__constants"], namespaced as fn_name.param:
    fn_name = config_keys.get("**fn", "")
    direct = json.loads(config_keys.get("**constants", "{}"))
    for k, v in direct.items():
    merged[f"{fn_name}.{k}"] = v
3.  Add dynamic discriminators: non-schema, non-\_\_ metadata columns from the row that are NOT input column names — these are columns the function introduced or
    propagated from the data.
4.  Pass \_\_branch_params=json.dumps(merged) in save_metadata.
5.  Strip \_\_branch_params from display output.

---

7.  database.save() — route \_\_branch_params to the new column

File: scidb/src/scidb/database.py (save / \_save_record_metadata call site)

\_\_branch_params is a recognized internal metadata key. Extract it from save_metadata and pass to \_save_record_metadata() as branch_params. Do NOT include it in
version_keys.

---

8.  database.\_load_by_record_row() — attach branch_params to loaded variable

File: scidb/src/scidb/database.py:1156

After constructing the variable instance, set:
var.branch_params = json.loads(row.get("branch_params") or "{}")

The row already has branch_params since \_find_record() SELECTs all \_record_metadata columns.

---

9.  database.\_find_record() — add branch_params_filter and include_excluded parameters

File: scidb/src/scidb/database.py:986–1082

Add branch_params_filter: dict | None = None and include_excluded: bool = False.

Default SQL gains AND NOT rm.excluded. With include_excluded=True, that clause is omitted.

After existing filters, apply branch_params filter:

if branch_params_filter and len(df) > 0:
for key, value in branch_params_filter.items():
df = df[df["branch_params"].apply(
lambda bp, k=key, v=value: \_match_branch_param(json.loads(bp or "{}"), k, v)
)]

Add \_match_branch_param() as a module-level helper.

---

10. database.load() — split non-schema kwargs → branch_params_filter

File: scidb/src/scidb/database.py (load method, called by BaseVariable.load())

Split incoming kwargs:

- Schema keys → existing schema filter
- version= → record_id filter (existing)
- Everything else → branch_params_filter

After \_find_record():

- 0 results → NotFoundError (existing)
- 1 result → return it (existing)
  1 results → raise AmbiguousVersionError listing variants with their branch_params and record_ids

---

11. database.list_versions() — include branch_params + filter support

File: scidb/src/scidb/database.py:1804–1839

- Include branch_params in each returned dict (parsed from JSON).
- Accept \*\*kwargs beyond schema keys; pass non-schema kwargs as branch_params_filter to \_find_record().
- Accept include_excluded=True to show excluded variants (includes "excluded": bool key in each dict).

---

12. New exceptions

File: scidb/src/scidb/exceptions.py

class AmbiguousVersionError(SciStackError):
"""Raised when load() matches multiple variants and no branch filter narrows to one."""

class AmbiguousParamError(SciStackError):
"""Raised when a bare param name matches multiple namespaced keys in branch_params."""

Export both from scidb/src/scidb/**init**.py.

---

Files Modified

┌───────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ File │ Changes │
├───────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ scidb/src/scidb/database.py │ Schema, \_save_record_metadata, \_load_by_record_row, \_find_record, load, list_versions, exclude_variant, include_variant │
├───────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ scidb/src/scidb/foreach_config.py │ to_version_keys() adds constants, \_get_direct_constants() helper │
├───────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ scidb/src/scidb/foreach.py │ \_load_var_type_all adds **branch_params col; \_save_results computes and forwards it │
├───────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ scidb/src/scidb/exceptions.py │ AmbiguousVersionError, AmbiguousParamError │
├───────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ scidb/src/scidb/**init\_\_.py │ Export new exceptions │
├───────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ docs/claude/gui-readiness.md │ Update items 2 + 3 status │
└───────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

---

Open Implementation Question

Variant auto-iteration in for_each: When \_load_var_type_all() returns a DataFrame with multiple rows per schema location (2 FilteredSignal variants for S01/1), scifor
iterates over them as a natural join. If scifor takes all matching rows, all 4 combinations are produced automatically. If scifor takes only the first match, a small
additional change is needed. This will be verified immediately after the initial implementation.

---

Verification

db = configure_database(":memory:", ["subject", "session"])

class Raw(BaseVariable): pass
class Filtered(BaseVariable): pass
class Spikes(BaseVariable): pass

Raw.save(np.array([1,2,3]), subject="S01", session="1")

for_each(bandpass_filter, {"signal": Raw, "low_hz": 20}, [Filtered], subject=["S01"], session=["1"])
for_each(bandpass_filter, {"signal": Raw, "low_hz": 30}, [Filtered], subject=["S01"], session=["1"])

assert len(db.list_versions(Filtered, subject="S01", session="1")) == 2

for_each(detect, {"signal": Filtered, "threshold": 0.5}, [Spikes], subject=["S01"], session=["1"])
for_each(detect, {"signal": Filtered, "threshold": 0.6}, [Spikes], subject=["S01"], session=["1"])

assert len(db.list_versions(Spikes, subject="S01", session="1")) == 4

try:
Spikes.load(subject="S01", session="1")
assert False, "should raise AmbiguousVersionError"
except AmbiguousVersionError:
pass

s = Spikes.load(subject="S01", session="1", low_hz=20, threshold=0.5)
assert s.branch_params["bandpass_filter.low_hz"] == 20
assert s.branch_params["detect.threshold"] == 0.5

variants = db.list_versions(Spikes, subject="S01", session="1", low_hz=20)
assert len(variants) == 2
