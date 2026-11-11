# Plan: Run State Color Coding

## Goal
Color VariableNodes and FunctionNodes green / grey / red to reflect pipeline run state.

## Rules
- **FunctionNode green**: own state is green AND all input variables are green
- **FunctionNode grey**: partially run (some schema_id × variant combos have output, not all) OR any input is grey (but none red)
- **FunctionNode red**: no output records for any combo OR any input is red
- **VariableNode**: inherits upstream FunctionNode's effective state
- **ConstantNode**: fixed colour, no state tracking (deferred)
- Staleness is purely structural — propagated upstream → downstream via DAG traversal

## Effective state propagation
```
effective_state(fn) = min(own_state(fn), min over inputs of effective_state(input_var))
effective_state(var) = effective_state(upstream_fn)   # if no upstream fn → green
```
`min` ordering: red < grey < green

## Own-state computation for a function
For each variant V = (function_name, output_type, input_types, constants, record_count):
1. Derive `filter_constants` = constants entries whose key does NOT start with `f"{fn_name}."`.
   These are the constants that should already be present in the input variable's branch_params.
2. For each input variable type I:
   - Query `_record_metadata`: count distinct schema_ids where variable_name=I, excluded=FALSE,
     and branch_params contains every key=value in filter_constants (JSON field checks).
   - `expected = min(counts across all input vars)` (inner-join semantics)
3. Per-variant state:
   - `record_count >= expected > 0` → green
   - `0 < record_count < expected` → grey
   - `record_count == 0` → red
4. Function own_state = most pessimistic state across all its variants.

## Files to change

### Backend
- `scistack-gui/scistack_gui/api/pipeline.py`
  - New helper `_get_expected_count(db, variable_name, filter_constants) -> int`
    - Builds WHERE clause dynamically from filter_constants key-value pairs using
      `json_extract_string(branch_params, '$.key') = 'value'` (DuckDB JSON syntax)
  - New helper `_compute_run_states(db, variants, fn_input_params) -> dict[str, str]`
    - Returns `{node_id: "green"|"grey"|"red"}` for all fn__ and var__ nodes
    - Step 1: compute own_state for each function
    - Step 2: topological propagation through the DAG
  - Modify `_build_graph`: call `_compute_run_states`, attach `run_state` to each node's data

### Frontend
- `VariableNode.tsx`: accept `run_state?: "green"|"grey"|"red"` in data; update `styles.container`
  border/background based on run_state
- `FunctionNode.tsx`: same

## Colour palette
| State | Border | Background |
|-------|--------|------------|
| green | #16a34a | #f0fdf4 |
| grey  | #6b7280 | #f3f4f6 |
| red   | #dc2626 | #fef2f2 |
| none  | existing | existing |

## Tests
- Add unit tests for `_get_expected_count` and `_compute_run_states` in `tests/test_api.py`
- Cover: all-green chain, partial run → grey, no outputs → red, staleness propagation
