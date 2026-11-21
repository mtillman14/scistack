/**
 * The header's "⋯" menu: the actions a user needs rarely (2026-11-21 UI
 * simplification). The header keeps the everyday ones (Refresh Code, Report,
 * Data, Schema keys).
 *
 * The menu owns the open state of the dialogs it starts (Clear data, Report
 * bug), so closing the menu never closes a dialog. Restart and Paths belong
 * to App (their state is shared), so they arrive as callbacks.
 */

import { useState, type CSSProperties } from 'react'
import { useScope } from '../context/ScopeContext'
import BugReportDialog from './BugReportDialog'
import ClearDataDialog from './DAG/ClearDataDialog'

type Dialog = 'clear' | 'bug' | null

export default function HeaderMoreMenu({
  buttonStyle,
  restarting,
  onRestart,
  onOpenPaths,
}: {
  buttonStyle: CSSProperties
  restarting: boolean
  onRestart: () => void
  onOpenPaths: () => void
}) {
  const { bumpGraph } = useScope()
  const [open, setOpen] = useState(false)
  const [dialog, setDialog] = useState<Dialog>(null)

  const pick = (fn: () => void, what: string) => () => {
    console.info(`[header] ⋯ ${what}`)
    setOpen(false)
    fn()
  }

  return (
    <span style={styles.anchor}>
      <button
        style={buttonStyle}
        onClick={() => setOpen(o => !o)}
        type="button"
        title="More: restart Python, code paths, clear data, report a bug"
        data-testid="header-more"
        aria-haspopup="menu"
        aria-expanded={open}
      >
        {restarting ? 'Restarting…' : '⋯'}
      </button>
      {open && (
        <>
          <div style={styles.backdrop} onClick={() => setOpen(false)} />
          <div style={styles.menu} role="menu">
            <button style={styles.item} type="button" role="menuitem" disabled={restarting}
              onClick={pick(onRestart, 'restart python')}
              title="Kill and restart the Python process. Use it when Refresh Code is not enough (edits to SciStack itself, a stuck process)">
              ⟳ Restart Python
            </button>
            <button style={styles.item} type="button" role="menuitem"
              onClick={pick(onOpenPaths, 'paths')}
              title="Configured code paths (Python + MATLAB) and discovered exports">
              📁 Paths…
            </button>
            <div style={styles.divider} />
            <button style={{ ...styles.item, ...styles.danger }} type="button" role="menuitem"
              onClick={pick(() => setDialog('clear'), 'clear data')}
              title="Delete every record and its run history; keep every canvas (nodes, edges, values)">
              Clear data…
            </button>
            <button style={styles.item} type="button" role="menuitem"
              onClick={pick(() => setDialog('bug'), 'report bug')}
              data-testid="bug-report-button"
              title="Report a bug: saves your project to attach and opens a prefilled issue on GitHub">
              🐞 Report bug…
            </button>
          </div>
        </>
      )}
      {dialog === 'clear' && (
        <ClearDataDialog onClose={() => { setDialog(null); bumpGraph() }} />
      )}
      {dialog === 'bug' && <BugReportDialog onClose={() => setDialog(null)} />}
    </span>
  )
}

const styles: Record<string, CSSProperties> = {
  anchor: { position: 'relative', display: 'inline-block' },
  backdrop: { position: 'fixed', inset: 0, zIndex: 999 },
  menu: {
    position: 'absolute',
    top: '100%',
    right: 0,
    marginTop: 4,
    zIndex: 1000,
    minWidth: 180,
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 4,
    boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
    overflow: 'hidden',
  },
  item: {
    display: 'block',
    width: '100%',
    padding: '6px 14px',
    background: 'transparent',
    color: '#eee',
    border: 'none',
    fontSize: 12,
    textAlign: 'left',
    cursor: 'pointer',
    whiteSpace: 'nowrap',
    fontFamily: 'inherit',
  },
  danger: { color: '#e0a0a0' },
  divider: { height: 1, margin: '3px 0', background: '#3a3a5a' },
}
