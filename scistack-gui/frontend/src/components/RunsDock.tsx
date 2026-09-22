/**
 * RunsDock — always-visible run status, docked bottom-left of the canvas
 * (rendered as a React Flow `Panel position="bottom-left"` in PipelineDAG,
 * stacking above the hidden-edges panel the same way Controls/Panel already
 * coexist there).
 *
 * Collapsed: a small pill showing run count, a "N running" badge when any
 * run is in flight, and the latest run's status icon — the at-a-glance
 * observability the Runs tab used to require a tab-switch to see.
 * Expanded (click the pill): inline scrollable list reusing RunsTab's cards.
 * Popout (⤢ in the expanded header): the same list in a larger centered
 * modal for reading long run histories without the dock's cramped width.
 */

import { useState } from 'react'
import { useRunLog } from '../context/RunLogContext'
import RunsTab from './Sidebar/RunsTab'

export default function RunsDock() {
  const { runs } = useRunLog()
  const [expanded, setExpanded] = useState(false)
  const [poppedOut, setPoppedOut] = useState(false)

  const latest = runs[0]
  const runningCount = runs.filter(
    r => r.status === 'running' || r.status === 'cancelling',
  ).length

  const statusIcon = !latest
    ? '·'
    : latest.status === 'running' || latest.status === 'cancelling'
    ? '⏳'
    : latest.status === 'error'
    ? '✗'
    : latest.status === 'cancelled'
    ? '⊘'
    // Its own mark, deliberately not '✗': we do not know that it failed.
    : latest.status === 'unknown'
    ? '?'
    : '✓'
  const statusColor = !latest
    ? '#555'
    : latest.status === 'running' || latest.status === 'cancelling'
    ? '#f0c040'
    : latest.status === 'error'
    ? '#e06060'
    : latest.status === 'cancelled'
    ? '#888'
    : latest.status === 'unknown'
    ? '#d9a441'
    : '#6be16b'

  return (
    <>
      <div style={styles.dock}>
        <button style={styles.pill} onClick={() => setExpanded(e => !e)} type="button">
          <span style={{ color: statusColor }}>{statusIcon}</span>
          <span style={styles.pillLabel}>Runs</span>
          <span style={styles.count}>{runs.length}</span>
          {runningCount > 0 && (
            <span style={styles.runningBadge}>{runningCount} running</span>
          )}
          <span style={styles.chevron}>{expanded ? '▾' : '▴'}</span>
        </button>
        {expanded && (
          <div style={styles.panel}>
            <div style={styles.panelHeader}>
              <span style={styles.panelTitle}>Runs</span>
              <button
                style={styles.iconBtn}
                onClick={() => setPoppedOut(true)}
                title="Open in a larger window"
                type="button"
              >
                ⤢
              </button>
              <button
                style={styles.iconBtn}
                onClick={() => setExpanded(false)}
                title="Collapse"
                type="button"
              >
                ×
              </button>
            </div>
            <div style={styles.panelBody}>
              <RunsTab />
            </div>
          </div>
        )}
      </div>
      {poppedOut && (
        <div style={styles.modalOverlay} onClick={() => setPoppedOut(false)}>
          <div style={styles.modal} onClick={e => e.stopPropagation()}>
            <div style={styles.modalHeader}>
              <span style={styles.panelTitle}>Runs</span>
              <button
                style={styles.iconBtn}
                onClick={() => setPoppedOut(false)}
                title="Close"
                type="button"
              >
                ×
              </button>
            </div>
            <div style={styles.modalBody}>
              <RunsTab />
            </div>
          </div>
        </div>
      )}
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  dock: {
    fontFamily: 'inherit',
  },
  pill: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    padding: '5px 10px',
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 14,
    color: '#ccc',
    fontSize: 12,
    cursor: 'pointer',
    boxShadow: '0 2px 8px rgba(0,0,0,0.35)',
  },
  pillLabel: {
    fontWeight: 600,
  },
  count: {
    color: '#888',
    fontFamily: 'monospace',
  },
  runningBadge: {
    background: '#3a2e0a',
    color: '#f0c040',
    borderRadius: 8,
    padding: '1px 7px',
    fontSize: 10,
    fontWeight: 600,
  },
  chevron: {
    color: '#666',
    fontSize: 10,
  },
  panel: {
    marginTop: 6,
    width: 320,
    maxHeight: 360,
    display: 'flex',
    flexDirection: 'column',
    background: '#12122a',
    border: '1px solid #3a3a5a',
    borderRadius: 6,
    boxShadow: '0 4px 16px rgba(0,0,0,0.45)',
    overflow: 'hidden',
  },
  panelHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 4,
    padding: '6px 8px',
    borderBottom: '1px solid #2a2a4a',
    flexShrink: 0,
  },
  panelTitle: {
    flex: 1,
    fontWeight: 700,
    fontSize: 13,
    color: '#fff',
  },
  panelBody: {
    overflowY: 'auto',
    flex: 1,
  },
  iconBtn: {
    background: 'transparent',
    border: 'none',
    color: '#888',
    fontSize: 14,
    cursor: 'pointer',
    padding: '2px 6px',
    lineHeight: 1,
  },
  modalOverlay: {
    position: 'fixed',
    inset: 0,
    background: 'rgba(0, 0, 0, 0.6)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 9000,
  },
  modal: {
    width: 560,
    maxHeight: '80vh',
    display: 'flex',
    flexDirection: 'column',
    background: '#12122a',
    border: '1px solid #3a3a5a',
    borderRadius: 8,
    boxShadow: '0 10px 40px rgba(0, 0, 0, 0.6)',
  },
  modalHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: 4,
    padding: '10px 14px',
    borderBottom: '1px solid #2a2a4a',
    flexShrink: 0,
  },
  modalBody: {
    overflowY: 'auto',
    flex: 1,
  },
}
