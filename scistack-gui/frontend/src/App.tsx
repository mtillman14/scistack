/**
 * Root application component.
 *
 * Layout:
 *   ┌─────────────────────────────────────┐
 *   │  header: SciStack + db name         │
 *   ├─────────────────────────────────────┤
 *   │  PipelineDAG (fills remaining space)│
 *   └─────────────────────────────────────┘
 */

import { useEffect, useState } from 'react'
import PipelineDAG from './components/DAG/PipelineDAG'

const styles: Record<string, React.CSSProperties> = {
  root: {
    display: 'flex',
    flexDirection: 'column',
    width: '100%',
    height: '100%',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 12,
    padding: '10px 18px',
    background: '#1a1a2e',
    color: '#fff',
    fontSize: 14,
    flexShrink: 0,
  },
  title: {
    fontWeight: 700,
    fontSize: 16,
    letterSpacing: 0.5,
  },
  separator: {
    opacity: 0.4,
  },
  dbName: {
    fontFamily: 'monospace',
    opacity: 0.8,
  },
  schemaKeys: {
    marginLeft: 'auto',
    opacity: 0.6,
    fontSize: 12,
  },
  dagArea: {
    flex: 1,
    minHeight: 0,   // important: lets the flex child shrink below its content size
  },
}

export default function App() {
  const [schema, setSchema] = useState<{ keys: string[] }>({ keys: [] })

  useEffect(() => {
    fetch('/api/schema')
      .then((r) => r.json())
      .then(setSchema)
      .catch(console.error)
  }, [])

  return (
    <div style={styles.root}>
      <header style={styles.header}>
        <span style={styles.title}>SciStack</span>
        <span style={styles.separator}>|</span>
        <span style={styles.dbName}>pipeline</span>
        {schema.keys.length > 0 && (
          <span style={styles.schemaKeys}>
            schema: [{schema.keys.join(', ')}]
          </span>
        )}
      </header>
      <div style={styles.dagArea}>
        <PipelineDAG />
      </div>
    </div>
  )
}
