# Plan: PathInput Node Type for GUI

## Problem

`scifor.PathInput` is a loadable input to `for_each` that resolves a path template per iteration combo. Currently it shows up in `list_pipeline_variants()` → `input_types` as a repr string like `"PathInput('{subject}/trial_{trial}.mat', root_folder=PosixPath('/data'))"`. The GUI treats all `input_types` values as variable type names, so PathInput entries either create bogus variable nodes or are invisible.

Constants don't cover PathInput because:
- PathInput is in `__inputs` (loadable), not `__constants` (scalar)
- It has no discrete selectable values — it's a template
- It doesn't create branch_params / variant splits

## Approach

### Layer 1: scidb (serialization improvement)

**File: `scifor/src/scifor/pathinput.py`**
- Add a `to_key()` method that returns a structured, parseable string:
  ```python
  def to_key(self) -> str:
      return json.dumps({"__type": "PathInput", "template": self.path_template,
                         "root_folder": str(self.root_folder) if self.root_folder else None})
  ```
- This means `_serialize_inputs` will call `to_key()` instead of `repr()`, giving us clean JSON in `__inputs`.

**Backward compat:** `_build_graph` should handle both the old `repr()` format (`"PathInput('...')"`) and the new `to_key()` JSON format.

### Layer 2: Backend (`_build_graph` in `pipeline.py`)

**File: `scistack-gui/scistack_gui/api/pipeline.py`**

In `_build_graph`, when iterating over `variants`:
1. For each entry in `input_types`, check if the value is a PathInput (starts with `"PathInput("` or is JSON with `__type: "PathInput"`).
2. If so, **don't** add it to `all_var_types` or `fn_input_params` as a variable. Instead, collect it into a new `path_inputs` dict: `{param_name: {template, root_folder, functions}}`.
3. Build `pathInputNode` nodes from `path_inputs`.
4. Build edges from `pathInput__<param_name>` → `fn__<fn_name>`.

Node data shape:
```python
{
    "id": f"pathInput__{param_name}",
    "type": "pathInputNode",
    "position": {"x": 0, "y": 0},
    "data": {
        "label": param_name,
        "template": "{subject}/trial_{trial}.mat",
        "root_folder": "/data"  # or null
    }
}
```

### Layer 3: Frontend

**New file: `scistack-gui/frontend/src/components/DAG/PathInputNode.tsx`**
- Displays param name as title
- Shows path template in monospace
- Shows root_folder (if set) in smaller text
- Source handle on right (feeds into functions)
- No target handle (it's always a root/source node)
- Visual style: distinct from variable (no record count/state) and constant (no checkboxes). Suggest a muted orange/amber theme to distinguish from teal constants and blue variables.

**File: `scistack-gui/frontend/src/components/DAG/PipelineDAG.tsx`**
- Import `PathInputNode`
- Add `pathInputNode: PathInputNode` to `nodeTypes`
- Add `pathInputNode` to the `onNodeClick` handler
- Add pathInputNode handling in the `onDrop` handler

**File: `scistack-gui/frontend/src/components/Sidebar/Sidebar.tsx`** (or wherever settings panels are dispatched)
- Add a `PathInputSettingsPanel` for when a pathInput node is selected
- Shows template and root_folder as read-only info (these come from the code, not user-editable)

**File: `scistack-gui/frontend/src/components/Sidebar/EditTab.tsx`**
- No changes needed initially — PathInput nodes are auto-discovered from pipeline data, not manually created (unlike constants). Could add later if we want manual creation.

## Detection Logic (parsing `input_types` values)

A helper function `_parse_input_type(value: str)` in `pipeline.py`:
```python
def _parse_input_type(value: str) -> dict | None:
    """If value represents a PathInput, return parsed info; else None."""
    # New format: JSON with __type
    if value.startswith("{"):
        try:
            parsed = json.loads(value)
            if parsed.get("__type") == "PathInput":
                return parsed
        except json.JSONDecodeError:
            pass
    # Legacy format: repr string
    if value.startswith("PathInput("):
        # Parse template from repr
        import re
        m = re.match(r"PathInput\('([^']*)'", value)
        if m:
            template = m.group(1)
            root_match = re.search(r"root_folder=(?:PosixPath|WindowsPath|Pure\w*Path)?\(?'([^']*)'\)?", value)
            root = root_match.group(1) if root_match else None
            return {"__type": "PathInput", "template": template, "root_folder": root}
    return None
```

## Implementation Order

1. Add `to_key()` to `PathInput` (scifor layer)
2. Add `_parse_input_type()` helper to `pipeline.py`
3. Update `_build_graph` to detect and emit `pathInputNode` nodes + edges
4. Create `PathInputNode.tsx` component
5. Register in `PipelineDAG.tsx` nodeTypes
6. Create `PathInputSettingsPanel.tsx`
7. Wire up sidebar dispatch

## Open Questions

- Should PathInput nodes be draggable from the EditTab palette? (Probably not initially — they come from code, not manual creation.)
- Should we show which schema keys the template references (e.g. `{subject}`, `{trial}`)? Could be useful but not critical for v1.
