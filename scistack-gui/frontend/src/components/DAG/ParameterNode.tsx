/**
 * ParameterNode — a named parameter in the pipeline: one or more values.
 *
 * One `scidb.Parameter` class, one node type (D6, see
 * docs/claude/entity-editability-model.md): adding a value is adding an
 * argument, so a node never changes type or id under the user.
 *
 * Replaces the former ConstantNode + SweepNode. The merge went in the
 * constant node's direction because it was already the richer widget: the
 * old sweep node had no per-value checkboxes and no DB-history rows, and
 * now inherits both.
 *
 * Shows the name and a checkboxed list of the distinct values it has taken
 * across pipeline runs, plus whatever source currently declares. Checked
 * values are "selected" for downstream runs; unchecking persists as a
 * hidden value and never deletes data (see
 * pipeline_store.hide_constant_value, and execution_service's filtering,
 * which excludes unchecked values from multi-value fan-outs too).
 *
 * The canvas deliberately shows the bare value and nothing else — no record
 * count, no `src` badge, no EachOf fan-out badge. All three are still in
 * `data` and all three are surfaced by ParameterSettingsPanel; on the node
 * they were noise on what should read as a short list of values.
 *
 * Values written in one go by the sidebar's Generate section arrive as a
 * SINGLE row with `kind: 'generated'`, whose `value` is a compact label
 * (`0:2:20 — 11 values`) rendered backend-side in
 * `graph_builder.render_value_group_label` — so the canvas and the sidebar
 * show the identical string rather than two implementations of it. Its one
 * checkbox toggles every member at once. Values added individually keep the
 * plain per-value rows they have always had.
 *
 * A Parameter with no values at all is a real, declared state (it is what
 * "New parameter" creates), so the node says "no value yet" rather than
 * rendering as an empty box.
 *
 * State management: checked state lives inside each value object in the
 * node's `data`, seeded from the backend on every graph fetch
 * (build_parameter_nodes). Toggling updates local state immediately
 * (useReactFlow().setNodes -- same pattern as VariableNode) and fires the
 * hide/unhide call in the background; a `dag_updated` broadcast from that
 * call re-fetches the graph and reconciles if the optimistic update and
 * persisted state ever disagree.
 *
 * Always a source node (feeds into functions); no target handle.
 */

import { useCallback } from 'react'
import { Handle, Position, useReactFlow } from '@xyflow/react'
import { callBackend } from '../../api'
import { useVariantSelection } from '../../context/VariantSelectionContext'
import type {
  VariantAxis,
  VariantSelectionValue,
} from '../../context/VariantSelectionContext'

export interface ParameterValue {
  value: string
  record_count: number
  checked: boolean
  is_current_source_value?: boolean
  /** Present only on a row that stands for a whole GENERATED set (written in
   *  one go by the panel's "Replace values"). `value` is then the compact
   *  label the backend rendered — `0:2:20 — 11 values` — and `members` are
   *  the individual values it stands for. Absent on every value added one at
   *  a time, which is what keeps those rendering exactly as they always have. */
  kind?: 'generated'
  members?: string[]
  /** The generation that produced the set — `{start, end, step}` for a range,
   *  `{members}` for a pasted list. Unused on the canvas; the sidebar re-seeds
   *  its Generate inputs from it. */
  spec?: { start?: number; end?: number; step?: number; members?: (number | string)[] }
}

export interface ParameterNodeData {
  label: string
  values: ParameterValue[]
  /** Where this Parameter is declared, if known — powers the canvas
   *  context menu's "Refresh from file" / "Open source" actions
   *  (PipelineDAG.tsx). Absent for a DB-only value with no current
   *  declaration. */
  source_file?: string | null
  source_line?: number | null
  /** True only when source_file is the configured writable entities file —
   *  re-reading it can actually change this Parameter's value, so this
   *  gates "Refresh from file" (a legacy .py/.m declaration is read-only
   *  and a reload of the entities file wouldn't touch it). */
  declared_in_entities_file?: boolean
}

interface Props {
  id: string
  data: ParameterNodeData & {
    /** Set by the variant popup: the variant-axis COLUMN this node supplies,
     *  resolved there from the pipeline edges' `targetHandle`. Absent on the
     *  canvas, and absent in the popup for a node that feeds no axis. */
    variantAxisColumn?: string | null
  }
}

/**
 * The node, in whichever of its two roles applies.
 *
 * Branching at the component boundary rather than inside one body keeps the
 * execution machinery — `useReactFlow`'s node writes and the hide/unhide calls
 * they pair with — out of the variant popup entirely, instead of merely
 * unused there. Same reasoning as FunctionNode's split.
 */
export default function ParameterNode({ id, data }: Props) {
  // Non-null only inside Plot Studio's variant popup, where these checkboxes
  // select what a FIGURE shows rather than what a RUN executes. See the
  // context's own docstring — the two must never share state.
  const variant = useVariantSelection()
  if (variant) {
    return (
      <VariantParameterNode
        label={data.label}
        axis={variant.axisForColumn(data.variantAxisColumn)}
        selection={variant}
      />
    )
  }
  return <PipelineParameterNode id={id} data={data} />
}

function PipelineParameterNode({ id, data }: Props) {
  const { setNodes } = useReactFlow()

  const toggleValue = useCallback((index: number) => {
    const target = data.values[index]
    if (!target) return
    const nextChecked = !target.checked
    setNodes(nds => nds.map(node => {
      if (node.id !== id) return node
      const values = (node.data.values as ParameterValue[]).map((v, i) =>
        i === index ? { ...v, checked: nextChecked } : v
      )
      return { ...node, data: { ...node.data, values } }
    }))
    if (target.kind === 'generated') {
      // The set is the unit: one call hides or unhides every member, rather
      // than one request per value in a range that may hold dozens.
      callBackend('set_parameter_group_checked', {
        name: data.label,
        values: target.members ?? [],
        checked: nextChecked,
      }).catch(console.error)
      return
    }
    const method = nextChecked ? 'unhide_parameter_value' : 'hide_parameter_value'
    callBackend(method, { name: data.label, value: target.value }).catch(console.error)
  }, [id, data.label, data.values, setNodes])

  // A lone generated row still needs its checkbox — it stands for many values,
  // so "only one row" does not mean "nothing to exclude".
  const showCheckboxes =
    data.values.length > 1 || data.values.some(v => v.kind === 'generated')

  return (
    <div style={styles.container}>
      <Handle type="source" position={Position.Right} />
      <div style={styles.label}>{data.label}</div>

      {data.values.length === 0 && (
        <div style={styles.noValue}>no value yet</div>
      )}

      {data.values.length > 0 && (
        <div style={styles.listbox}>
          {data.values.map((v, i) => (
            <label key={i} style={showCheckboxes ? styles.valueRow : styles.valueRowNoCheck}>
              {showCheckboxes && (
                <input
                  type="checkbox"
                  checked={v.checked}
                  onChange={() => toggleValue(i)}
                  style={styles.checkbox}
                />
              )}
              <span style={!showCheckboxes || v.checked ? styles.valueLabel : styles.valueLabelUnchecked}>
                {v.value}
              </span>
            </label>
          ))}
        </div>
      )}
    </div>
  )
}

/**
 * The same node, selecting variants instead of configuring runs.
 *
 * Levels come from the DATA (the variant axis), not from the parameter's
 * declared values: the question here is which records to draw, and a value
 * declared but never run has none. A parameter with no axis at all — nothing
 * downstream of it distinguishes this measure's records — renders inert rather
 * than offering checkboxes that would do nothing.
 */
export function VariantParameterNode({
  label,
  axis,
  selection,
}: {
  label: string
  axis: VariantAxis | null
  selection: VariantSelectionValue
}) {
  const inert = !axis || axis.levels.length === 0
  return (
    <div
      style={{ ...styles.container, ...(inert ? styles.inert : null) }}
      title={
        inert
          ? `${label} does not distinguish this measure's records — nothing to select.`
          : undefined
      }
    >
      <Handle type="source" position={Position.Right} />
      <div style={styles.label}>{label}</div>
      {inert && <div style={styles.noValue}>not a variant here</div>}
      {!inert && (
        <div style={styles.listbox}>
          {axis!.levels.map(level => (
            <label key={level} style={styles.valueRow}>
              <input
                type="checkbox"
                checked={selection.isLevelSelected(axis!.column, level)}
                onChange={() => selection.toggleLevel(axis!.column, level)}
                className="nodrag nopan"
                style={styles.checkbox}
              />
              <span style={styles.valueLabel}>{level}</span>
            </label>
          ))}
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    background: '#1e3a2f',
    border: '2px solid #2a9d8f',
    borderRadius: 6,
    padding: '6px 12px',
    minWidth: 140,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.10)',
  },
  label: {
    fontWeight: 600,
    color: '#4ecdc4',
    fontFamily: 'monospace',
    textAlign: 'center',
    marginBottom: 4,
  },
  listbox: {
    display: 'flex',
    flexDirection: 'column',
    gap: 3,
    maxHeight: 90,
    overflowY: 'auto',
  },
  valueRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
    cursor: 'pointer',
    userSelect: 'none',
  },
  valueRowNoCheck: {
    display: 'flex',
    alignItems: 'center',
    userSelect: 'none',
  },
  checkbox: {
    margin: 0,
    cursor: 'pointer',
    accentColor: '#2a9d8f',
    flexShrink: 0,
  },
  valueLabel: {
    fontSize: 11,
    color: '#b2ded9',
    fontFamily: 'monospace',
  },
  noValue: {
    fontSize: 10,
    color: '#5a7f78',
    fontStyle: 'italic',
    textAlign: 'center',
  },
  valueLabelUnchecked: {
    fontSize: 11,
    color: '#555',
    fontFamily: 'monospace',
    textDecoration: 'line-through',
  },
  // Variant popup only: a node that cannot affect the plotted measure. Dimmed
  // rather than hidden, so the graph still reads as the pipeline the user knows.
  inert: {
    opacity: 0.4,
    borderStyle: 'dashed',
  },
}
