Plan: @thunk(generates_file=True) — Side-Effect Function Tracking

Context

The @thunk decorator tracks function lineage, and BaseVariable.save() persists output data + lineage to disk, enabling cache hits on repeated calls. Plotting and report-generating
functions produce files directly rather than returning data, so there's nothing meaningful to .save(). But we still need cache-hit behavior: skip re-execution when the same
function runs with the same inputs.

Solution: A new generates_file=True flag on @thunk. This flag propagates to ThunkOutput, where save_variable() detects it and saves lineage only (PipelineDB) with a lineage-derived
record_id, without storing data in DuckDB. for_each detects the flag and passes metadata as function kwargs.

class Figure(BaseVariable):
pass

@thunk(generates*file=True)
def plot_signal(data, subject, session):
plt.title(f"Subject {subject}, Session {session}")
plt.savefig(f"signal_s{subject}*{session}.png")

# Manual loop:

data = ProcessedData.load(subject=1, session="A")
result = plot_signal(data, subject=1, session="A")
Figure.save(result, subject=1, session="A")

# Or with for_each (metadata passed as kwargs automatically):

for_each(plot_signal, inputs={"data": ProcessedData}, outputs=[Figure],
subject=subjects, session=sessions)

# Next run — cache hit, function skipped:

result = plot_signal(data, subject=1, session="A")

# result.data is None, result.is_complete is True

Implementation

1.  Modify thunk-lib/src/thunk/core.py — Add generates_file flag

Thunk.**init** (line ~55): Add generates_file: bool = False parameter, store as self.generates_file. Do NOT include in the hash — it's a storage concern, not a computation concern.

thunk() decorator (line ~385): Add generates_file: bool = False parameter, pass through to Thunk().

No changes to PipelineThunk or ThunkOutput — the flag is accessed via thunk_output.pipeline_thunk.thunk.generates_file.

2.  Modify src/scidb/database.py — Lineage-only save path

save_variable() (line ~409): Inside the existing isinstance(data, ThunkOutput) block, add an early-return branch at the top:

if isinstance(data, ThunkOutput):
if data.pipeline_thunk.thunk.generates_file:
lineage = extract_lineage(data)
pipeline_lineage_hash = data.pipeline_thunk.compute_lineage_hash()
generated_id = f"generated:{pipeline_lineage_hash[:32]}"
user_id = get_user_id()
nested_metadata = self.\_split_metadata(metadata)
self.\_save_lineage(
output_record_id=generated_id,
output_type=variable_class.**name**,
lineage=lineage,
lineage_hash=pipeline_lineage_hash,
user_id=user_id,
schema_keys=nested_metadata.get("schema"),
output_content_hash=None,
)
return generated_id # ... existing ThunkOutput handling continues ...

find_by_lineage() (line ~896): Handle generated: prefix alongside existing ephemeral: check. Prefer real data records over generated records:

results = []
has_generated = False
for record in records:
rid = record["output_record_id"]
if rid.startswith("ephemeral:"):
continue
if rid.startswith("generated:"):
has_generated = True
continue # ... existing load-from-DuckDB logic ...

if results:
return results
if has_generated:
return [None]
return None

Returning [None] flows into Thunk.**call**() (core.py:107) → ThunkOutput(data=None, is_complete=True) → cache hit, function skipped.

3.  Modify scirun-lib/src/scirun/foreach.py — Pass metadata as kwargs

for_each() signature: Add pass_metadata: bool | None = None parameter.

Logic: When deciding whether to pass metadata to fn:

- If pass_metadata is explicitly set, use that value
- Otherwise, default to getattr(fn, 'generates_file', False)

In the execution loop (line ~110-113):
should_pass_metadata = pass_metadata if pass_metadata is not None else getattr(fn, 'generates_file', False)

# ...

if should_pass_metadata:
result = fn(**loaded_inputs, **metadata)
else:
result = fn(\*\*loaded_inputs)

In dry-run output: When should_pass_metadata is true, show the metadata values being passed as function args.

4.  Create tests/test_generates_file.py

Tests covering:

- Decorator flag: @thunk(generates_file=True) sets the flag; does NOT affect function hash
- Save behavior: Figure.save(result) returns a generated: prefixed ID; no data in DuckDB; lineage in PipelineDB with correct function name, inputs, schema_keys
- Cache hit: Function not re-executed on second call; result.data is None, result.is_complete is True
- Idempotency: Figure.save() on a cache-hit result is a no-op (PipelineDB upsert)
- Distinct computations: Different inputs or different function code → no cache hit
- Persistence: Cache hit survives database reconnection
- for_each integration: generates_file=True function auto-receives metadata kwargs; outputs=[Figure] works; second for_each run hits cache for all iterations
- pass_metadata override: pass_metadata=True forces metadata passing for non-generates_file functions; pass_metadata=False suppresses it for generates_file functions

Files Summary
┌──────────────────────────────────┬────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ File │ Action │ Change │
├──────────────────────────────────┼────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ thunk-lib/src/thunk/core.py │ Modify │ Add generates_file param to Thunk.**init** and thunk() (~6 lines) │
├──────────────────────────────────┼────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ src/scidb/database.py │ Modify │ Lineage-only branch in save_variable() (~15 lines), generated: handling in find_by_lineage() (~8 lines) │
├──────────────────────────────────┼────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ scirun-lib/src/scirun/foreach.py │ Modify │ Add pass_metadata param, conditional metadata passing (~10 lines) │
├──────────────────────────────────┼────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ tests/test_generates_file.py │ Create │ Comprehensive tests (~250 lines) │
└──────────────────────────────────┴────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
Verification

1.  pytest tests/test_generates_file.py -v — new tests
2.  pytest tests/ -v — no regressions
3.  Manual scenario: configure DB → save raw data → run @thunk(generates_file=True) function → Figure.save() → re-run → confirm cache hit
