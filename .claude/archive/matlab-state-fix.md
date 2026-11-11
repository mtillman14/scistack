# Fix: MATLAB Function Node State Always Red

## Problem
MATLAB function nodes always show "red" state after a successful run because the state computation uses the MATLAB output parameter names (e.g., `extracted_data`) instead of the edge-resolved variable class names (e.g., `XSENSLoaded`).

## Root Cause
In `scistack-gui/scistack_gui/api/pipeline.py`, lines 335-341, `resolved_output_types` is overridden for MATLAB functions to use declared output names for handle rendering. But the same overridden list is then used for state computation at line 343, which looks up the names in `BaseVariable._all_subclasses` — where the MATLAB parameter names don't exist.

## Fix
In `scistack-gui/scistack_gui/api/pipeline.py`:
1. Save the edge-resolved output types before the MATLAB override
2. Use the edge-resolved types for state computation
3. Keep the MATLAB declared names for handle rendering (passed to `build_manual_node`)

## Files Changed
- `scistack-gui/scistack_gui/api/pipeline.py` (lines ~330-346)
