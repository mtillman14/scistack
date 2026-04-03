/**
 * EditTab — palette of draggable function, variable, and constant nodes.
 *
 * Drag an item onto the canvas to place a new node.
 * The drag payload is JSON in the 'application/scistack-node' dataTransfer key:
 *   { nodeType: 'functionNode' | 'variableNode' | 'constantNode', label: string }
 */

import { useEffect, useState, useRef } from 'react'

interface Registry {
  functions: string[]
  variables: string[]
}

export default function EditTab() {
  const [registry, setRegistry] = useState<Registry>({ functions: [], variables: [] })
  const [constants, setConstants] = useState<string[]>([])
  const [adding, setAdding] = useState(false)
  const [draft, setDraft] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    fetch('/api/registry')
      .then(r => r.json())
      .then(setRegistry)
      .catch(console.error)
    fetchConstants()
  }, [])

  function fetchConstants() {
    fetch('/api/constants')
      .then(r => r.json())
      .then(setConstants)
      .catch(console.error)
  }

  useEffect(() => {
    if (adding) inputRef.current?.focus()
  }, [adding])

  const commitDraft = () => {
    const name = draft.trim()
    if (name) {
      fetch('/api/constants', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      }).then(fetchConstants)
    }
    setDraft('')
    setAdding(false)
  }

  const onDragStart = (
    e: React.DragEvent,
    nodeType: 'functionNode' | 'variableNode' | 'constantNode',
    label: string,
  ) => {
    e.dataTransfer.setData(
      'application/scistack-node',
      JSON.stringify({ nodeType, label }),
    )
    e.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div style={styles.root}>
      <Section title="Functions">
        {registry.functions.map(fn => (
          <DragItem
            key={fn}
            label={fn}
            color="#7b68ee"
            onDragStart={e => onDragStart(e, 'functionNode', fn)}
          />
        ))}
      </Section>
      <Section title="Variables">
        {registry.variables.map(v => (
          <DragItem
            key={v}
            label={v}
            color="#2a9d8f"
            onDragStart={e => onDragStart(e, 'variableNode', v)}
          />
        ))}
      </Section>
      <Section
        title="Constants"
        action={
          <button style={styles.addBtn} onClick={() => setAdding(true)} title="New constant">
            +
          </button>
        }
      >
        {constants.map(c => (
          <DragItem
            key={c}
            label={c}
            color="#2a9d8f"
            onDragStart={e => onDragStart(e, 'constantNode', c)}
          />
        ))}
        {adding && (
          <input
            ref={inputRef}
            style={styles.draftInput}
            value={draft}
            placeholder="constant name…"
            onChange={e => setDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') commitDraft()
              if (e.key === 'Escape') { setDraft(''); setAdding(false) }
            }}
            onBlur={commitDraft}
          />
        )}
      </Section>
    </div>
  )
}

function Section({
  title,
  children,
  action,
}: {
  title: string
  children: React.ReactNode
  action?: React.ReactNode
}) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionHeader}>
        <span style={styles.sectionTitle}>{title}</span>
        {action}
      </div>
      {children}
    </div>
  )
}

function DragItem({
  label,
  color,
  onDragStart,
}: {
  label: string
  color: string
  onDragStart: (e: React.DragEvent) => void
}) {
  return (
    <div
      draggable
      onDragStart={onDragStart}
      style={{ ...styles.item, borderLeftColor: color }}
    >
      {label}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '4px 0',
  },
  section: {
    marginBottom: 8,
  },
  sectionHeader: {
    display: 'flex',
    alignItems: 'center',
    padding: '6px 12px 4px',
  },
  sectionTitle: {
    flex: 1,
    fontSize: 11,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
  },
  addBtn: {
    background: 'transparent',
    border: 'none',
    color: '#7b68ee',
    fontSize: 18,
    lineHeight: 1,
    cursor: 'pointer',
    padding: '0 2px',
  },
  draftInput: {
    display: 'block',
    width: 'calc(100% - 24px)',
    margin: '2px 12px',
    background: '#1a1a2e',
    border: '1px solid #7b68ee',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 12,
    fontFamily: 'monospace',
    padding: '4px 6px',
    outline: 'none',
    boxSizing: 'border-box',
  },
  item: {
    padding: '5px 12px',
    fontSize: 12,
    fontFamily: 'monospace',
    color: '#ccc',
    borderLeft: '3px solid',
    cursor: 'grab',
    userSelect: 'none',
  },
}
