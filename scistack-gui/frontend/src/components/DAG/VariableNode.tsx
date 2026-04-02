/**
 * VariableNode — represents a named variable type in the pipeline.
 *
 * Shows the variable type name and (eventually) a scrollable list of
 * its variants. For now it renders the label and a placeholder for variants.
 *
 * React Flow passes a `data` prop to every custom node. The shape of `data`
 * matches what the backend sends in GET /api/pipeline.
 */

import { Handle, Position } from '@xyflow/react'

interface Variant {
  constants: Record<string, unknown>
  record_count: number
}

interface VariableNodeData {
  label: string
  variants: Variant[]
}

interface Props {
  data: VariableNodeData
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    background: '#fff',
    border: '2px solid #4a90d9',
    borderRadius: 8,
    padding: '8px 14px',
    minWidth: 160,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.12)',
  },
  label: {
    fontWeight: 600,
    color: '#1a1a2e',
    marginBottom: 4,
  },
  variants: {
    fontSize: 11,
    color: '#555',
    marginTop: 4,
    maxHeight: 80,
    overflowY: 'auto',
  },
  variantRow: {
    padding: '1px 0',
  },
  noVariants: {
    fontStyle: 'italic',
    color: '#aaa',
    fontSize: 11,
  },
}

export default function VariableNode({ data }: Props) {
  return (
    <div style={styles.container}>
      {/* Left handle: edges coming in (this node as output of a function) */}
      <Handle type="target" position={Position.Left} />

      <div style={styles.label}>{data.label}</div>

      <div style={styles.variants}>
        {data.variants.length === 0 ? (
          <span style={styles.noVariants}>no data</span>
        ) : (
          data.variants.map((v, i) => {
            const label =
              Object.keys(v.constants).length === 0
                ? `${v.record_count} record${v.record_count !== 1 ? 's' : ''}`
                : Object.entries(v.constants)
                    .map(([k, val]) => `${k}=${val}`)
                    .join(', ')
            return (
              <div key={i} style={styles.variantRow}>
                {label}
              </div>
            )
          })
        )}
      </div>

      {/* Right handle: edges going out (this node as input to a function) */}
      <Handle type="source" position={Position.Right} />
    </div>
  )
}
