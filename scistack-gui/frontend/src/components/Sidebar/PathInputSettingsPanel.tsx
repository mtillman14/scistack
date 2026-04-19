/**
 * PathInputSettingsPanel — shown in the sidebar when a PathInput node is selected.
 *
 * Editable fields for path template and root folder.  Changes update the
 * React Flow node data (so the canvas reflects edits live) and persist to
 * the backend on Enter or blur.  Escape reverts to the last saved value.
 */

import { useRef, useEffect } from 'react'
import { useReactFlow } from '@xyflow/react'
import { callBackend } from '../../api'
import { useCommittedInput } from '../../hooks/useCommittedInput'

interface Props {
  id: string
  label: string
  template: string
  root_folder: string | null
}

function parseTemplateKeys(template: string): string[] {
  const matches = template.match(/\{(\w+)\}/g)
  if (!matches) return []
  return [...new Set(matches.map(m => m.slice(1, -1)))]
}

export default function PathInputSettingsPanel({ id, label, template, root_folder }: Props) {
  const { setNodes } = useReactFlow()

  // Refs so each field's callbacks can read the other field's latest draft
  // without stale-closure issues.
  const latestTemplate = useRef(template)
  const latestRoot = useRef(root_folder ?? '')

  useEffect(() => {
    latestTemplate.current = template
    latestRoot.current = root_folder ?? ''
  }, [id]) // eslint-disable-line react-hooks/exhaustive-deps

  const updateCanvas = (newTemplate: string, newRoot: string) => {
    const rootVal = newRoot.trim() || null
    setNodes(nds => nds.map(n =>
      n.id === id
        ? { ...n, data: { ...n.data, template: newTemplate, root_folder: rootVal } }
        : n
    ))
  }

  const saveToBackend = (newTemplate: string, newRoot: string) => {
    const rootVal = newRoot.trim() || null
    callBackend('update_path_input', { name: label, template: newTemplate, root_folder: rootVal })
      .catch(err => console.error('[PathInputSettings] save error:', err))
  }

  const templateInput = useCommittedInput({
    initialValue: template,
    resetKey: id,
    onLiveChange: val => { latestTemplate.current = val; updateCanvas(val, latestRoot.current) },
    onSave: val => saveToBackend(val, latestRoot.current),
  })

  const rootInput = useCommittedInput({
    initialValue: root_folder ?? '',
    resetKey: id,
    onLiveChange: val => { latestRoot.current = val; updateCanvas(latestTemplate.current, val) },
    onSave: val => saveToBackend(latestTemplate.current, val),
  })

  const keys = parseTemplateKeys(templateInput.value)

  return (
    <div style={styles.root}>
      <div style={styles.name}>{label}</div>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Path Template</div>
        <input
          style={styles.input}
          placeholder="{subject}/trial_{trial}.mat"
          {...templateInput}
        />
      </section>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Root Folder</div>
        <input
          style={styles.input}
          placeholder="/data (optional)"
          {...rootInput}
        />
      </section>

      {keys.length > 0 && (
        <section style={styles.section}>
          <div style={styles.sectionTitle}>Schema Keys</div>
          <div style={styles.keysRow}>
            {keys.map(k => (
              <span key={k} style={styles.keyPill}>{k}</span>
            ))}
          </div>
        </section>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  name: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#fbbf24',
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
  input: {
    display: 'block',
    width: '100%',
    background: '#1a1a2e',
    border: '1px solid #444',
    borderRadius: 3,
    color: '#e5c8a0',
    fontSize: 11,
    fontFamily: 'monospace',
    padding: '5px 6px',
    outline: 'none',
    boxSizing: 'border-box',
  },
  keysRow: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 5,
  },
  keyPill: {
    fontSize: 11,
    fontFamily: 'monospace',
    background: '#3d2e1a',
    border: '1px solid #92702a',
    borderRadius: 3,
    padding: '2px 6px',
    color: '#fbbf24',
  },
}
