# Plan: Create Variable Types from the GUI

## Goal
Allow users to define new `BaseVariable` subclasses from the GUI. The class definition is appended to the user's `--module` Python file, then `refresh_module()` is called to make it immediately available.

## Approach
Append to user module file + refresh (Approach 1).

## Steps

### 1. Backend: `POST /api/variables/create` endpoint
**File:** `scistack-gui/scistack_gui/api/variables.py`

- Accepts JSON body: `{ "name": str, "docstring": str | null }`
- Validates:
  - `name` is a valid Python identifier (`str.isidentifier()`)
  - `name` is not already in `BaseVariable._all_subclasses`
  - `name` doesn't start with `_`
  - Module file path is available (i.e., `--module` was passed at startup)
- Appends to the module file:
  ```python
  
  
  class NewVarName(BaseVariable):
      """Optional docstring."""
      pass
  ```
- Calls `registry.refresh_module()` to pick up the new class
- Broadcasts `dag_updated` via WebSocket so the DAG refreshes
- Returns `{ "ok": true, "name": "NewVarName" }` or error

### 2. Frontend: "Add Variable" button + dialog
**File:** New component or inline in `App.tsx` header area

- A "+" or "Add Variable" button in the header (next to Refresh)
- Clicking opens a simple dialog/modal with:
  - Name field (text input)
  - Docstring field (optional textarea)
  - Create button
- On submit: `POST /api/variables/create` with `{ name, docstring }`
- On success: the WebSocket `dag_updated` event will trigger DAG refresh automatically
- On error: show the error message inline

### 3. No scidb layer changes needed
`BaseVariable.__init_subclass__` already handles auto-registration. The `refresh_module()` re-executes the file which defines the new class, triggering auto-registration. No changes to scidb.

## Edge Cases
- Module file not passed at startup → return error
- Invalid Python identifier → return error
- Name collision with existing variable → return error
- File write permission issues → return error with message
