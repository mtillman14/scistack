# Plan: Function Node Settings Panel

## Goal
Allow users to click a function node on the DAG canvas and edit/view its settings in the sidebar.

## What "settings" means here
- **Known variants**: constants from the DB for this function (e.g. `low_hz=20, high_hz=400`)
- **Custom run**: ability to enter new constants and trigger a run with them

## Architecture

### State sharing
Add `SelectedNodeContext` (similar to `RunLogContext`) so both `PipelineDAG` and `Sidebar` can read/write which node is selected.

### Backend change
`pipeline.py` → add `variants` list to function node data (reuse the `variants` list already built):
```python
data: {"label": fn, "variants": [{"constants": ..., "input_types": ..., "output_type": ..., "record_count": ...}, ...]}
```

### Frontend changes
1. **`SelectedNodeContext.tsx`** (new) — provides `selectedNode: Node | null` + `setSelectedNode`
2. **`App.tsx`** — wrap with `<SelectedNodeProvider>`
3. **`PipelineDAG.tsx`** — add `onNodeClick` (sets selected for functionNode, clears otherwise) + `onPaneClick` (clears); keep selectedNode fresh when nodes update
4. **`Sidebar.tsx`** — add "Node" tab that appears when function node is selected; auto-switch to it on selection
5. **`FunctionSettingsPanel.tsx`** (new) — displays function info:
   - Function name (title)
   - Known variants table (constants + record count)
   - Custom constants form (key/value rows + add row button)
   - Run button that posts to `/api/run` with custom constants

## UX flow
1. User clicks function node → sidebar auto-switches to "Node" tab
2. User sees existing variants from DB
3. User can enter custom constants in the form
4. Clicking Run in the panel triggers the function with those constants
5. Clicking elsewhere on canvas deselects (tab reverts to previous)

## Files to create/modify
- `src/context/SelectedNodeContext.tsx` (new)
- `src/components/Sidebar/FunctionSettingsPanel.tsx` (new)
- `src/App.tsx` (add provider)
- `src/components/DAG/PipelineDAG.tsx` (add click handlers + node freshness)
- `src/components/Sidebar/Sidebar.tsx` (add Node tab)
- `scistack_gui/api/pipeline.py` (add variants to function nodes)
