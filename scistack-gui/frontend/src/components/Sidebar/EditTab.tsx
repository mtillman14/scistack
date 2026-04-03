/**
 * EditTab — palette of draggable function and variable nodes.
 *
 * Drag an item onto the canvas to place a new node.
 * The drag payload is JSON in the 'application/scistack-node' dataTransfer key:
 *   { nodeType: 'functionNode' | 'variableNode', label: string }
 */

import { useEffect, useState } from 'react'

interface Registry {
  functions: string[]
  variables: string[]
}

export default function EditTab() {
  const [registry, setRegistry] = useState<Registry>({ functions: [], variables: [] })

  useEffect(() => {
    fetch('/api/registry')
      .then(r => r.json())
      .then(setRegistry)
      .catch(console.error)
  }, [])

  const onDragStart = (
    e: React.DragEvent,
    nodeType: 'functionNode' | 'variableNode',
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
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>{title}</div>
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
  sectionTitle: {
    fontSize: 11,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    padding: '6px 12px 4px',
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
