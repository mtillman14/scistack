/**
 * VariableNode — represents a named variable type in the pipeline.
 *
 * Shows the variable type name and its total record count.
 * Variant selection (which constants produced this variable) now lives
 * on the upstream ConstantNode.
 */

import { Handle, Position } from '@xyflow/react'

export interface Variant {
  constants: Record<string, unknown>
  checked?: boolean
}

export interface VariableNodeData {
  label: string
  total_records: number
  run_state?: 'green' | 'grey' | 'red'
}

interface Props {
  data: VariableNodeData
}

const STATE_STYLES: Record<string, { border: string; background: string }> = {
  green: { border: '#16a34a', background: '#f0fdf4' },
  grey:  { border: '#6b7280', background: '#f3f4f6' },
  red:   { border: '#dc2626', background: '#fef2f2' },
}

export default function VariableNode({ data }: Props) {
  const stateStyle = data.run_state ? STATE_STYLES[data.run_state] : null

  return (
    <div style={{
      ...styles.container,
      ...(stateStyle ? { border: `2px solid ${stateStyle.border}`, background: stateStyle.background } : {}),
    }}>
      <Handle type="target" position={Position.Left} />

      <div style={styles.label}>{data.label}</div>

      <div style={styles.count}>
        {data.total_records > 0
          ? `${data.total_records} record${data.total_records !== 1 ? 's' : ''}`
          : <span style={styles.empty}>empty</span>
        }
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    background: '#fff',
    border: '2px solid #4a90d9',
    borderRadius: 8,
    padding: '8px 12px',
    minWidth: 160,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.12)',
  },
  label: {
    fontWeight: 600,
    color: '#1a1a2e',
    marginBottom: 4,
  },
  count: {
    fontSize: 11,
    color: '#666',
  },
  empty: {
    fontStyle: 'italic',
    color: '#bbb',
  },
}
