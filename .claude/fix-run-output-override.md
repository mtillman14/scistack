# Fix: Run should respect manual edge output wiring over DB history

## Problem
When a user deletes an output node (e.g. MaxHR_80Perc) and replaces it with a new one (MaxHR_Perc), re-running the function still outputs to the old type because:

1. `run.py` gets `fn_variants` from `db.list_pipeline_variants()` — these have the OLD output type
2. Since `fn_variants` is non-empty, the manual-edge inference block is skipped entirely
3. The run outputs to the old type → old node reappears in DAG
4. New output node never gets data → stays red

## Fix
In `run.py`, after getting `fn_variants` from DB, also check manual edges for this function's current output types. If manual edges define outputs, override the `output_type` in fn_variants.

### Changes
- **`run.py`**: After line ~68, add logic to scan manual edges for the function's current outputs, and override `output_type` in fn_variants if manual edges define different outputs.
- **`pipeline_store.py`**: Add `_pipeline_hidden_nodes` table to track user-deleted DB-derived nodes. Add `hide_node()`, `unhide_node()`, `get_hidden_nodes()`.
- **`layout.py`**: `delete_node()` also hides DB-derived nodes. `write_manual_node()` unhides if re-adding.
- **`pipeline.py`**: `_build_graph()` filters out hidden nodes and their edges.
