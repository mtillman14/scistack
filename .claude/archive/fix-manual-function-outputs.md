# Fix: Manual function nodes show "No output"

## Problem
When a user creates a new function node in the GUI and connects it to a new variable node via a manual edge, clicking "Run" shows "No output" because:

1. `pipeline.py` gives manual function nodes `output_types: []` — it doesn't inspect manual edges
2. `run.py` requires `list_pipeline_variants()` to have entries for the function — a new function has none, so it errors with "No pipeline history found"

## Fix

### 1. `pipeline.py` — Infer output_types/input_params from manual edges

In the manual node building section (~line 420), after creating manual function nodes:
- Read all manual edges
- For edges where source is this function node → extract target variable label as an output_type
- For edges where target is this function node → extract source variable label as an input_param
- Use both DB-derived and manual node labels for resolution

### 2. `run.py` — Fallback to edge topology when no DB variants exist

When `fn_variants` is empty (~line 68):
- Read manual edges from pipeline_store
- Find edges pointing TO this function (inputs) and FROM this function (outputs)
- Resolve node IDs to variable type names (from both DB nodes and manual nodes)
- Match edge target handles to function parameter names
- Build a synthetic variant dict with the inferred input_types, output_type, and empty constants
- Run for_each with that
