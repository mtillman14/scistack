# Refresh Button Plan

## Problem
When the user adds a new function to their pipeline module, it doesn't appear in the GUI until the Python server is restarted. The module is imported once at startup and the registry is static.

## Solution

### Backend changes
1. **`registry.py`**: Store the module path and `module` object. Add `refresh_module()` that re-executes the module file and re-scans for functions. Need to handle `BaseVariable._all_subclasses` which auto-registers on class definition.
2. **New endpoint** in a new or existing router: `POST /api/refresh` that calls `registry.refresh_module()` and broadcasts `dag_updated` via WebSocket.

### Frontend changes
3. **`App.tsx`**: Add a refresh button in the header bar that calls `POST /api/refresh`.

## Key considerations
- `importlib.reload()` won't work since we used `spec_from_file_location` — need to re-exec the spec
- Old function objects in the registry should be replaced, not accumulated
- `BaseVariable._all_subclasses` is populated by metaclass; re-importing the module will re-register classes (they overwrite by name, so this is safe)
