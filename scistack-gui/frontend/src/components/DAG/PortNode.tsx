/**
 * PortNode — a submodule's generic input or output node (Simulink's Inport /
 * Outport). It sets the name and order of one port on every placement of the
 * submodule (backend owner: domain/ports.py, docs/claude/submodule-ports.md).
 *
 * An input node has only a right handle: what the outside feeds in leaves it.
 * An output node has only a left handle: what reaches it leaves the submodule.
 * Double-click the name to rename. Ports run top to bottom in the order these
 * nodes do, so drag one up or down to reorder them.
 */

import { useState } from 'react'
import { Handle, Position } from '@xyflow/react'
import { callBackend } from '../../api'

interface Props {
  id: string
  type?: string
  data: { label: string }
}

export default function PortNode({ id, type, data }: Props) {
  const isInput = type === 'inputPortNode'
  const [draft, setDraft] = useState<string | null>(null)

  const commit = () => {
    if (draft === null) return
    const name = draft.trim()
    setDraft(null)
    if (!name || name === data.label) return
    callBackend('rename_port', { node_id: id, name })
      .catch(err => window.alert(`Could not rename port: ${(err as Error).message}`))
  }

  return (
    <div
      style={{ ...styles.container, borderColor: isInput ? '#0369a1' : '#b45309' }}
      title={isInput ? 'Input port of this submodule' : 'Output port of this submodule'}
    >
      {!isInput && <Handle type="target" position={Position.Left} />}
      <span style={styles.kind}>{isInput ? '⇥ in' : 'out ⇥'}</span>
      {draft === null ? (
        <span
          style={styles.label}
          onDoubleClick={e => { e.stopPropagation(); setDraft(data.label) }}
        >
          {data.label}
        </span>
      ) : (
        <input
          className="nodrag"
          style={styles.input}
          autoFocus
          value={draft}
          onChange={e => setDraft(e.target.value)}
          onBlur={commit}
          onKeyDown={e => {
            if (e.key === 'Enter') commit()
            if (e.key === 'Escape') setDraft(null)
          }}
        />
      )}
      {isInput && <Handle type="source" position={Position.Right} />}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '6px 10px',
    background: '#ffffff',
    color: '#1f2937',
    border: '2px solid',
    borderRadius: 16,
    fontSize: 12,
  },
  kind: { fontSize: 10, color: '#6b7280' },
  label: { fontWeight: 600 },
  input: { width: 90, fontSize: 12 },
}
