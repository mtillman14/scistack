/**
 * ConstantNode — represents a named constant in the pipeline.
 *
 * Shows the constant name and a checkboxed list of the distinct values it has
 * taken across pipeline runs. Checked values are "selected" for downstream runs.
 *
 * State management: checked state lives inside each value object in the node's
 * `data`. We update it via useReactFlow().setNodes — same pattern as VariableNode.
 */

import { useCallback } from 'react'
import { Handle, Position, useReactFlow } from '@xyflow/react'

export interface ConstantValue {
  value: string
  record_count: number
  checked: boolean
}

export interface ConstantNodeData {
  label: string
  values: ConstantValue[]
}

interface Props {
  id: string
  data: ConstantNodeData
}

export default function ConstantNode({ id, data }: Props) {
  const { setNodes } = useReactFlow()

  const toggleValue = useCallback((index: number) => {
    setNodes(nds => nds.map(node => {
      if (node.id !== id) return node
      const values = (node.data.values as ConstantValue[]).map((v, i) =>
        i === index ? { ...v, checked: !v.checked } : v
      )
      return { ...node, data: { ...node.data, values } }
    }))
  }, [id, setNodes])

  const showCheckboxes = data.values.length > 1

  return (
    <div style={styles.container}>
      <Handle type="source" position={Position.Right} />
      <div style={styles.label}>{data.label}</div>

      {data.values.length > 0 && (
        <div style={styles.listbox}>
          {data.values.map((v, i) => {
            const rowLabel = `${v.value} · ${v.record_count} rec${v.record_count !== 1 ? 's' : ''}`
            return (
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
                  {rowLabel}
                </span>
              </label>
            )
          })}
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
  valueLabelUnchecked: {
    fontSize: 11,
    color: '#555',
    fontFamily: 'monospace',
    textDecoration: 'line-through',
  },
}
