# Plan: Function Node Configuration (Schema Filter + Run Options)

## Goal
Expose `for_each()` kwargs in the GUI so users can control which schema combos to run over and toggle run options (dry_run, save, distribute).

## Current State
- `run.py:307-311` builds `schema_kwargs` by grabbing ALL distinct values for every schema key — no filtering
- None of `dry_run`, `save`, `distribute` are exposed
- FunctionSettingsPanel only shows variant table (constants)

## Implementation Steps

### Step 1: Backend — extend RunRequest and _run_in_thread
**File:** `scistack-gui/scistack_gui/api/run.py`

- Add `schema_filter: dict[str, list] | None = None` to `RunRequest`
- Add `run_options: dict | None = None` to `RunRequest` (keys: `dry_run`, `save`, `distribute`)
- In `_run_in_thread`: if `schema_filter` provided, use it instead of all distinct values
- Pass `dry_run`, `save`, `distribute` through to `for_each()` call

### Step 2: Frontend — add schema filter to FunctionSettingsPanel
**File:** `scistack-gui/frontend/src/components/Sidebar/FunctionSettingsPanel.tsx`

- Fetch schema keys + values from `GET /schema` on mount
- Render each key with checkboxes for its values (all checked by default)
- Store selected values in component state
- Pass schema filter up to parent or store on node data

### Step 3: Frontend — add run options to FunctionSettingsPanel
**File:** `scistack-gui/frontend/src/components/Sidebar/FunctionSettingsPanel.tsx`

- Add "Run Options" section with checkboxes:
  - Dry run (default off)
  - Save results (default on)
  - Distribute (default off)
- Store in component state

### Step 4: Frontend — pass config through handleRun
**File:** `scistack-gui/frontend/src/components/DAG/FunctionNode.tsx`

- Store schema_filter and run_options on function node's `data`
- `handleRun` reads from node data and includes in `start_run` call

### Step 5: Persist node config
- Schema filter and run options stored in manual node metadata (same pattern as constant values)
- Survives page reload

### Step 6: Testing / verification
- Manual test: select subset of schema values, run, verify only those combos execute
- Manual test: dry_run toggle shows preview without executing
- Manual test: save=false runs but doesn't persist

### Step 2b: Schema Level section
**File:** `scistack-gui/frontend/src/components/Sidebar/FunctionSettingsPanel.tsx`

- New "Schema Level" section with checkboxes for each schema key
- Controls which schema keys are iterated over (e.g. only `subject`, or both `subject` + `trial`)
- All keys checked by default (null = iterate all)
- Stored as `schemaLevel: string[] | null` on function node data
- Backend skips keys not in `schemaLevel` from `schema_kwargs`

## Deferred
- `as_table` and `where` kwargs — power-user features for later
- Per-variant schema filtering (same filter applies to whole function node)
