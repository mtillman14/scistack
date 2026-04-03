/**
 * RunsTab — displays per-run collapsible log sections, most recent first.
 */

import { useState } from 'react'
import { useRunLog } from '../../context/RunLogContext'
import type { RunEntry } from '../../context/RunLogContext'

export default function RunsTab() {
  const { runs } = useRunLog()

  if (runs.length === 0) {
    return <div style={styles.empty}>No runs yet.</div>
  }

  return (
    <div>
      {runs.map(run => <RunSection key={run.run_id} run={run} />)}
    </div>
  )
}

function RunSection({ run }: { run: RunEntry }) {
  // Most recent run starts expanded; older ones start collapsed.
  const [open, setOpen] = useState(true)

  const statusColor = run.status === 'running' ? '#f0c040' : '#6be16b'
  const statusLabel = run.status === 'running' ? '⏳' : '✓'

  return (
    <div style={styles.section}>
      <button style={styles.header} onClick={() => setOpen(o => !o)}>
        <span style={{ color: statusColor, marginRight: 6 }}>{statusLabel}</span>
        <span style={styles.fnName}>{run.function_name}</span>
        <span style={styles.runId}>{run.run_id}</span>
        <span style={styles.chevron}>{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <div style={styles.logBox}>
          {run.lines.length === 0
            ? <span style={styles.noOutput}>No output.</span>
            : run.lines.map((line, i) => <span key={i}>{line}</span>)
          }
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  empty: {
    color: '#555',
    fontSize: 13,
    padding: '16px',
    textAlign: 'center',
  },
  section: {
    borderBottom: '1px solid #1e1e3a',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    width: '100%',
    padding: '7px 12px',
    background: 'transparent',
    border: 'none',
    cursor: 'pointer',
    textAlign: 'left',
    gap: 4,
  },
  fnName: {
    flex: 1,
    fontFamily: 'monospace',
    fontSize: 12,
    color: '#ccc',
    fontWeight: 600,
  },
  runId: {
    fontFamily: 'monospace',
    fontSize: 10,
    color: '#555',
    marginRight: 8,
  },
  chevron: {
    color: '#888',
    fontSize: 12,
  },
  logBox: {
    display: 'flex',
    flexDirection: 'column',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b0c4b0',
    background: '#0d0d1f',
    padding: '6px 12px',
    whiteSpace: 'pre-wrap',
    overflowX: 'auto',
  },
  noOutput: {
    color: '#444',
    fontStyle: 'italic',
  },
}
