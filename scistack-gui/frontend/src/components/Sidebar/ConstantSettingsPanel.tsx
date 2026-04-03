/**
 * ConstantSettingsPanel — shown in the sidebar when a constant node is selected.
 *
 * Lets the user add and remove variant values for that constant.
 * Values are stored in the node's `data.values` array and updated via useReactFlow.
 */

import { useState } from 'react'
import { useReactFlow } from '@xyflow/react'
import type { ConstantValue } from '../DAG/ConstantNode'

interface Props {
  id: string
  label: string
  values: ConstantValue[]
}

export default function ConstantSettingsPanel({ id, label, values }: Props) {
  const { setNodes } = useReactFlow()
  const [draft, setDraft] = useState('')

  const addValue = () => {
    const v = draft.trim()
    if (!v) return
    if (values.some(existing => existing.value === v)) {
      setDraft('')
      return
    }
    const newValue: ConstantValue = { value: v, record_count: 0, checked: true }
    setNodes(nds => nds.map(node =>
      node.id === id
        ? { ...node, data: { ...node.data, values: [...(node.data.values as ConstantValue[]), newValue] } }
        : node
    ))
    setDraft('')
  }

  const removeValue = (index: number) => {
    setNodes(nds => nds.map(node => {
      if (node.id !== id) return node
      const updated = (node.data.values as ConstantValue[]).filter((_, i) => i !== index)
      return { ...node, data: { ...node.data, values: updated } }
    }))
  }

  return (
    <div style={styles.root}>
      <div style={styles.constName}>{label}</div>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Variants</div>

        {values.length === 0 && (
          <div style={styles.empty}>No values yet</div>
        )}

        {values.map((v, i) => (
          <div key={i} style={styles.valueRow}>
            <span style={styles.valuePill}>{v.value}</span>
            {v.record_count > 0 && (
              <span style={styles.recCount}>{v.record_count} rec</span>
            )}
            <button style={styles.removeBtn} onClick={() => removeValue(i)} title="Remove">
              ×
            </button>
          </div>
        ))}
      </section>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Add Variant</div>
        <div style={styles.addRow}>
          <input
            style={styles.input}
            placeholder="value…"
            value={draft}
            onChange={e => setDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') addValue()
              if (e.key === 'Escape') setDraft('')
            }}
          />
          <button style={styles.addBtn} onClick={addValue}>Add</button>
        </div>
      </section>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  constName: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#4ecdc4',
    marginBottom: 12,
    wordBreak: 'break-all',
  },
  section: {
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 10,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginBottom: 6,
  },
  empty: {
    color: '#555',
    fontStyle: 'italic',
    fontSize: 11,
  },
  valueRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    marginBottom: 4,
    borderBottom: '1px solid #1e1e3a',
    paddingBottom: 4,
  },
  valuePill: {
    flex: 1,
    background: '#1e3a2f',
    borderRadius: 3,
    padding: '2px 6px',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
  recCount: {
    color: '#555',
    fontSize: 10,
    whiteSpace: 'nowrap',
  },
  removeBtn: {
    background: 'transparent',
    border: 'none',
    color: '#666',
    cursor: 'pointer',
    fontSize: 14,
    padding: '0 2px',
    lineHeight: 1,
  },
  addRow: {
    display: 'flex',
    gap: 6,
  },
  input: {
    flex: 1,
    background: '#1a1a2e',
    border: '1px solid #333',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    padding: '3px 6px',
    minWidth: 0,
  },
  addBtn: {
    background: '#2a9d8f',
    border: 'none',
    borderRadius: 3,
    color: '#fff',
    fontSize: 11,
    padding: '3px 8px',
    cursor: 'pointer',
    fontWeight: 600,
  },
}
