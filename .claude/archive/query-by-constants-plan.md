Plan: Support constants in for_each inputs

Context

Currently for_each(fn, inputs={...}, outputs=[...], \*\*metadata) only accepts variable types (classes with .load()) or Fixed wrappers in the inputs dict. The user wants to also pass plain constant
values like smoothing=0.2 alongside variable inputs:

for_each(
filter_data,
inputs={"values": RawForce, "smoothing": 0.2},
outputs=[FilteredForce],
subject=[1, 2, 3],
)

Constants should be:

1.  Passed as kwargs to the function
2.  Included in the save metadata (becoming version keys) so they're queryable via .load(smoothing=0.2)

Files to modify

1.  /workspace/scirun-lib/src/scirun/foreach.py — core logic
2.  /workspace/scidb-matlab/src/scidb_matlab/matlab/+scidb/for_each.m — MATLAB equivalent
3.  /workspace/scirun-lib/tests/test_foreach.py — tests

Detection strategy

A value in the inputs dict is a constant if it is NOT any of the recognized "loadable" types:

- A class (type) — e.g., RawForce → checked via isinstance(var_spec, type)
- A Fixed instance — checked via isinstance(var_spec, Fixed)
- A PathInput instance — checked via duck-typing: hasattr(var_spec, 'load')

Everything else (int, float, str, bool, list, dict, None, numpy array) is a constant.

Helper function \_is_loadable(var_spec):
def \_is_loadable(var_spec):
return isinstance(var_spec, (type, Fixed)) or hasattr(var_spec, 'load')

Changes to foreach.py

Step 1: Add \_is_loadable() helper

def \_is_loadable(var_spec: Any) -> bool:
"""Check if an input spec is a loadable type (class, Fixed, or PathInput-like)."""
return isinstance(var_spec, (type, Fixed)) or (
not isinstance(var_spec, type) and hasattr(var_spec, 'load')
)

Wait — isinstance(var_spec, type) already catches classes. And PathInput instances have .load(). But a plain float does NOT have .load(). So actually the simplest check:

def \_is_loadable(var_spec):
return isinstance(var_spec, (type, Fixed)) or hasattr(var_spec, 'load')

This works because:

- RawForce (a class) → isinstance(type) = True
- Fixed(RawForce, ...) → isinstance(Fixed) = True
- PathInput(...) → has .load() = True
- 0.2 → none of the above = False
- "bandpass" → none of the above = False

Note: str has no .load() method, int/float/bool/list/dict don't either. This is safe.

Step 2: Split inputs into loadable vs constants at the top of for_each()

Before the main loop, separate the inputs dict:

loadable_inputs = {}
constant_inputs = {}
for param_name, var_spec in inputs.items():
if \_is_loadable(var_spec):
loadable_inputs[param_name] = var_spec
else:
constant_inputs[param_name] = var_spec

Step 3: Modify the load loop (lines 95-108)

Only iterate over loadable_inputs, not all inputs:

for param_name, var_spec in loadable_inputs.items(): # ... existing Fixed/type loading logic ...

Step 4: Merge constants into loaded_inputs before function call

After loading, add constants to the function kwargs:

loaded_inputs.update(constant_inputs)

Constants don't need unwrapping (they're already plain values), so this goes AFTER the unwrap step for non-Thunk functions.

Step 5: Merge constants into save metadata (line 143)

Change the save call from:
output_type.save(output_value, **metadata)
to:
output_type.save(output_value, **metadata, \*\*constant_inputs)

Step 6: Update display functions

- \_format_inputs(): Show constants as smoothing: 0.2 (no type name)
- \_print_dry_run_iteration(): Show constants as pass smoothing = 0.2 instead of a load line
- Log line (line 115): Include constant params in the display

Step 7: Update dry-run save display

The dry-run save line should show constants in the metadata:
save FilteredForce.save(..., subject=1, session=pre, smoothing=0.2)

Changes to MATLAB for_each.m

Mirror the same logic:

1.  After parsing inputs struct, split into loadable vs constant fields
2.  Detection: isa(var_spec, 'scidb.Fixed') or isa(var_spec, 'scidb.BaseVariable') or isa(var_spec, 'scidb.PathInput') → loadable. Everything else → constant.
3.  Skip .load() for constants, add them directly to loaded args
4.  Merge constants into meta_nv before saving
5.  Update display helpers

Tests to add in test_foreach.py

New test class TestForEachWithConstants:

1.  test_constant_passed_to_function — verify the function receives the constant value
2.  test_constant_saved_as_metadata — verify output save metadata includes the constant
3.  test_constant_with_variable_inputs — mixed constants and variable types work together
4.  test_multiple_constants — multiple constants all work
5.  test_constant_in_dry_run — dry-run displays constants correctly

Verification

1.  Run existing tests: cd /workspace/scirun-lib && python -m pytest tests/ -v
2.  Run new tests to confirm constant behavior
3.  Verify dry-run output formatting looks correct
