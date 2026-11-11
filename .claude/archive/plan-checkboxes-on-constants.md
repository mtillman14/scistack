# Plan: Move Variant Checkboxes from VariableNode to ConstantNode

## Goal
Checkboxes that let the user select which variant to run belong on ConstantNode
(where variants originate), not VariableNode.

## Backend — `pipeline.py`

- Extract constant nodes from `list_pipeline_variants()`:
  - For each variant, iterate `constants` dict → group by constant name
  - Aggregate `record_count` per `(const_name, value)` across all functions/outputs
  - Track which functions each constant feeds into → build edges `const__{name} → fn__{fn}`
- Build `constantNode` entries with `data: {label, values: [{value, record_count}]}`
- Manually-placed constant nodes from layout that aren't in DB yet get `values: []`
  (same empty-data pattern as manually-placed variable nodes)
- Remove `var_variants` / variant data from `variableNode` data
  (VariableNode only needs `label` and `total_records` now)

## Frontend — `ConstantNode.tsx`

- Add `values: {value: string, record_count: number, checked: boolean}[]` to data interface
- Render checkboxes for each value (same pattern VariableNode currently uses)
- Toggle via `useReactFlow().setNodes`
- Show nothing extra when `values` is empty (not-yet-run constants)

## Frontend — `VariableNode.tsx`

- Remove variant checkboxes entirely
- Show just total record count (use `total_records` from data, or "empty" if 0)

## Frontend — `PipelineDAG.tsx`

- Move the `checked: true` initialisation from `variants` on variableNodes
  to `values` on constantNodes
