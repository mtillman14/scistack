/**
 * Root application component.
 *
 * Layout:
 *   ┌─────────────────────────────────────────────┐
 *   │  header: SciStack + db name                 │
 *   ├───────────────────────────────┬─────────────┤
 *   │  PipelineDAG (left 3/4)       │  sidebar    │
 *   │                               │  (right 1/4)│
 *   └───────────────────────────────┴─────────────┘
 */

import { useEffect, useState } from 'react'
import { ReactFlowProvider } from '@xyflow/react'
import PipelineDAG from './components/DAG/PipelineDAG'
import Sidebar from './components/Sidebar/Sidebar'
import { RunLogProvider } from './context/RunLogContext'

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
  body: {
    display: 'flex',
    flexDirection: 'row',
    flex: 1,
    minHeight: 0,
  },
  dagArea: {
    flex: 3,
    minWidth: 0,
    minHeight: 0,
  },
  sidebar: {
    flex: 1,
    minWidth: 0,
    borderLeft: '1px solid #2a2a4a',
    background: '#12122a',
  },
}

export default function App() {
  const [schema, setSchema] = useState<{ keys: string[] }>({ keys: [] })
  const [dbName, setDbName] = useState('')

  useEffect(() => {
    fetch('/api/schema')
      .then((r) => r.json())
      .then(setSchema)
      .catch(console.error)
    fetch('/api/info')
      .then((r) => r.json())
      .then((d) => setDbName(d.db_name))
      .catch(console.error)
  }, [])

  return (
    <RunLogProvider>
    <div style={styles.root}>
      <header style={styles.header}>
        <span style={styles.title}>SciStack</span>
        <span style={styles.separator}>|</span>
        <span style={styles.dbName}>{dbName || 'loading…'}</span>
        {schema.keys.length > 0 && (
          <span style={styles.schemaKeys}>
            schema: [{schema.keys.join(', ')}]
          </span>
        )}
      </header>
      <div style={styles.body}>
        <div style={styles.dagArea}>
          <ReactFlowProvider>
            <PipelineDAG />
          </ReactFlowProvider>
        </div>
        <div style={styles.sidebar}>
          <Sidebar />
        </div>
      </div>
    </div>
    </RunLogProvider>
  )
}
