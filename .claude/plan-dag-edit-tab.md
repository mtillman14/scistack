# Plan: DAG Edit Tab — Drag-and-Drop Node Creation

## Goal
Add an "Edit" tab to the sidebar that lists all functions and variables from the
user's codebase (via the registry). Items can be dragged onto the React Flow
canvas to place new nodes. Placed nodes persist across refreshes via the layout
store.

## Backend changes

### 1. `GET /api/registry`
New endpoint in `scistack_gui/api/registry.py`.
Returns:
```json
{ "functions": ["bandpass_filter", ...], "variables": ["RawEMG", ...] }
```
Sources:
- `registry._functions.keys()` for functions
- `BaseVariable._all_subclasses.keys()` for variables

Register in `app.py`.

### 2. Extend layout store (`scistack_gui/layout.py`)
Add a `manual_nodes` dict to the persisted layout JSON:
```json
{
  "positions": { "fn__bandpass_filter": {"x": 100, "y": 200}, ... },
  "manual_nodes": {
    "fn__bandpass_filter": { "type": "functionNode", "label": "bandpass_filter" },
    ...
  }
}
```
- `PUT /api/layout/:node_id` already saves position — extend it to also accept
  `type` and `label` in the request body, storing them in `manual_nodes`.
- `GET /api/layout` already returns positions — extend it to also return
  `manual_nodes`.

### 3. Merge manual nodes in `GET /api/pipeline`
In `pipeline.py::_build_graph`, after building nodes from DB variants, merge in
any `manual_nodes` from the layout store that aren't already present (by node_id).
This way manually-placed nodes survive `dag_updated` refreshes, and disappear
naturally once they have real DB data (since their id will then come from the DB).

## Frontend changes

### 4. `GET /api/registry` fetch in new `EditTab` component
New file: `src/components/Sidebar/EditTab.tsx`
- On mount, fetches `/api/registry`
- Renders two sections: "Functions" and "Variables"
- Each item: `draggable`, on `dragstart` sets
  `dataTransfer.setData('application/scistack-node', JSON.stringify({ nodeType, label }))`

### 5. Add "Edit" tab to `Sidebar.tsx`
Add `'Edit'` to the `TABS` array, render `<EditTab />` when active.

### 6. Drag-and-drop in `PipelineDAG.tsx`
- Wrap `ReactFlow` div with `onDragOver` (prevent default to allow drop)
- Add `onDrop` handler:
  1. Parse `dataTransfer` to get `nodeType` and `label`
  2. Use `screenToFlowPosition({ x: event.clientX, y: event.clientY })` to get
     canvas coordinates
  3. Build node id: `fn__<label>` or `var__<label>`
  4. Skip if node id already exists in canvas
  5. Add node to React Flow state via `setNodes`
  6. `PUT /api/layout/:id` with `{ x, y, type: nodeType, label }` to persist

## File list
- NEW `scistack_gui/api/registry.py`
- MOD `scistack_gui/layout.py`
- MOD `scistack_gui/api/layout.py`
- MOD `scistack_gui/api/pipeline.py`
- MOD `scistack_gui/app.py`
- NEW `scistack-gui/frontend/src/components/Sidebar/EditTab.tsx`
- MOD `scistack-gui/frontend/src/components/Sidebar/Sidebar.tsx`
- MOD `scistack-gui/frontend/src/components/DAG/PipelineDAG.tsx`
