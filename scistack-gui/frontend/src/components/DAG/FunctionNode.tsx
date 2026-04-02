/**
 * FunctionNode — represents a pipeline function (e.g. bandpass_filter).
 *
 * Visually distinct from VariableNode: diamond-ish shape, different colour.
 * Will later host a Run button and spinner.
 */

import { Handle, Position } from '@xyflow/react'

interface FunctionNodeData {
  label: string
}

interface Props {
  data: FunctionNodeData
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    background: '#f0f4ff',
    border: '2px solid #7b68ee',
    borderRadius: 6,
    padding: '8px 14px',
    minWidth: 160,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.10)',
    textAlign: 'center',
  },
  label: {
    fontWeight: 600,
    color: '#3a1a8e',
    fontFamily: 'monospace',
  },
}

export default function FunctionNode({ data }: Props) {
  return (
    <div style={styles.container}>
      <Handle type="target" position={Position.Left} />
      <div style={styles.label}>{data.label}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
