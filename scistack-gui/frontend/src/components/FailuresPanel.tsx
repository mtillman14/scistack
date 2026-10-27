/**
 * FailuresPanel â what failed on one function node in its last run, grouped
 * by reason: the exception, how many combos, which ones, and the traceback
 * with the user's frames shown and SciStack/library frames collapsed.
 * Clicking a frame opens the file at that line (VS Code) or shows the path.
 *
 * Opened from the node's red badge or the extension's "Show failures"
 * notification button (NodeFailuresContext). See docs/claude/error-surfacing.md.
 */

import { useState } from 'react'
import { callBackend, isVSCodeMode } from '../api'
import { useNodeFailures } from '../context/NodeFailuresContext'
import { segmentFrames } from './DAG/runFailures'
import type { FailureFrame, FailureGroup } from './DAG/runFailures'
import { dialog, dialogTitle, overlay } from './modalStyles'

function FrameLine({ frame }: { frame: FailureFrame }) {
  const [shown, setShown] = useState<string | null>(null)
  const open = async () => {
    if (!isVSCodeMode) {
      setShown(`${frame.file}:${frame.line}`)
      return
    }
    try {
      const r = await callBackend('reveal_in_editor', { file: frame.file, line: frame.line }) as { ok?: boolean; error?: string }
      if (r && r.ok === false) setShown(`${frame.file}:${frame.line} (${r.error ?? 'could not open'})`)
    } catch (err) {
      setShown(`${frame.file}:${frame.line} (${String(err)})`)
    }
  }
  const user = frame.origin === 'user'
  return (
    <div style={styles.frame}>
      <button
        type="button"
        style={{ ...styles.frameLink, ...(user ? styles.frameUser : null) }}
        onClick={open}
        title={user ? 'Open this line' : `${frame.origin} code â open this line`}
      >
        {shortPath(frame.file)}:{frame.line} in {frame.function}
      </button>
      {frame.code && <div style={styles.code}>{frame.code}</div>}
      {shown && <div style={styles.shownPath}>{shown}</div>}
    </div>
  )
}

function Traceback({ frames }: { frames: FailureFrame[] }) {
  const [expanded, setExpanded] = useState<Set<number>>(new Set())
  return (
    <div style={styles.traceback}>
      {segmentFrames(frames).map((seg, i) => {
        if (seg.kind === 'frame') return <FrameLine key={i} frame={seg.frame} />
        if (expanded.has(i)) {
          return seg.frames.map((f, j) => <FrameLine key={`${i}.${j}`} frame={f} />)
        }
        const origins = [...new Set(seg.frames.map(f => f.origin))].join('/')
        return (
          <button
            key={i}
            type="button"
            style={styles.collapsed}
            onClick={() => setExpanded(prev => new Set(prev).add(i))}
          >
            â¦ {seg.frames.length} {origins} frame{seg.frames.length === 1 ? '' : 's'} (show)
          </button>
        )
      })}
    </div>
  )
}

function GroupCard({ group, nodeId }: { group: FailureGroup; nodeId: string }) {
  const { openRemedy } = useNodeFailures()
  const frames = group.example?.frames ?? []
  const stage = group.example?.stage
  return (
    <div style={styles.card}>
      {group.blame_label && <div style={styles.blame}>{group.blame_label}</div>}
      <div style={styles.reasonRow}>
        <span style={styles.excType}>{group.exc_type}</span>
        <span style={styles.count}>{group.count} combo{group.count === 1 ? '' : 's'}</span>
      </div>
      <pre style={styles.message}>{group.message}</pre>
      {group.remedy && (
        <button type="button" style={styles.howTo} onClick={() => openRemedy(group.node_id ?? nodeId, group)}>
          How to fix…
        </button>
      )}
      {group.target && group.target !== group.fn_name && (
        <div style={styles.meta}>in {group.target}</div>
      )}
      {stage === 'prepare_inputs' && (
        <div style={styles.meta}>Failed while preparing the inputs, before the function was called.</div>
      )}
      {group.combos.length > 0 && (
        <div style={styles.meta}>
          {group.combos.join('; ')}
          {group.count > group.combos.length ? ` (+${group.count - group.combos.length} more)` : ''}
        </div>
      )}
      {frames.length > 0
        ? <Traceback frames={frames} />
        : group.user_frame && <FrameLine frame={group.user_frame} />}
    </div>
  )
}

export default function FailuresPanel() {
  const { failures, openNodeId, closePanel } = useNodeFailures()
  if (openNodeId === null) return null
  // '' (from a notification with no node) shows every node's failures.
  const entries = openNodeId
    ? [[openNodeId, failures[openNodeId]] as const]
    : Object.entries(failures)
  const groups = entries.flatMap(([, e]) => e?.groups ?? [])
  const stopped = entries.some(([, e]) => e?.stopped)
  const title = groups[0]?.fn_name ?? 'Run'
  return (
    <div style={overlay} onClick={closePanel}>
      <div style={{ ...dialog, ...styles.panel }} onClick={e => e.stopPropagation()}>
        <div style={dialogTitle}>
          {title}: {groups.reduce((n, g) => n + g.count, 0)} failed combo(s)
          {stopped ? ' â stopped at the first failure' : ''}
        </div>
        {groups.length === 0
          ? <div style={styles.meta}>No failures recorded for this node's last run.</div>
          : groups.map((g, i) => <GroupCard key={i} group={g} nodeId={openNodeId} />)}
        <div style={styles.footer}>
          <button type="button" style={styles.close} onClick={closePanel}>Close</button>
        </div>
      </div>
    </div>
  )
}

function shortPath(file: string): string {
  const parts = file.split(/[\\/]/)
  return parts.length > 2 ? `â¦/${parts.slice(-2).join('/')}` : file
}

const styles: Record<string, React.CSSProperties> = {
  panel: { width: 'min(720px, 92vw)' },
  card: {
    border: '1px solid #4b2a2a',
    borderLeft: '3px solid #dc2626',
    borderRadius: 4,
    padding: '8px 10px',
    marginBottom: 10,
    background: '#221722',
  },
  // Who can fix it â the backend sends the wording (scistacklog.journal).
  blame: { fontSize: 12, fontWeight: 600, color: '#fde68a', marginBottom: 2 },
  reasonRow: { display: 'flex', justifyContent: 'space-between', alignItems: 'baseline' },
  excType: { fontFamily: 'monospace', fontWeight: 700, color: '#fca5a5' },
  count: { fontSize: 11, color: '#bbb' },
  message: {
    fontFamily: 'monospace',
    fontSize: 12,
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
    margin: '4px 0',
    maxHeight: 160,
    overflow: 'auto',
  },
  meta: { fontSize: 11, color: '#aaa', margin: '2px 0' },
  traceback: { marginTop: 6, borderTop: '1px solid #333', paddingTop: 4 },
  frame: { margin: '3px 0' },
  frameLink: {
    background: 'none',
    border: 'none',
    padding: 0,
    color: '#9ca3af',
    fontFamily: 'monospace',
    fontSize: 11,
    cursor: 'pointer',
    textAlign: 'left',
  },
  frameUser: { color: '#93c5fd', textDecoration: 'underline', fontWeight: 600 },
  code: { fontFamily: 'monospace', fontSize: 11, color: '#e5e7eb', paddingLeft: 12 },
  shownPath: { fontFamily: 'monospace', fontSize: 11, color: '#fde68a', paddingLeft: 12, userSelect: 'text' },
  collapsed: {
    background: 'none',
    border: 'none',
    padding: 0,
    color: '#6b7280',
    fontSize: 11,
    fontStyle: 'italic',
    cursor: 'pointer',
    display: 'block',
    margin: '3px 0',
  },
  // Opens RemedyDialog for a failure that carries a remedy.
  howTo: {
    background: 'none',
    border: 'none',
    padding: 0,
    color: '#fbbf24',
    fontSize: 12,
    textDecoration: 'underline',
    cursor: 'pointer',
    margin: '2px 0',
  },
  footer: { display: 'flex', justifyContent: 'flex-end', marginTop: 8 },
  close: {
    background: '#2a2a4a',
    color: '#eee',
    border: '1px solid #444',
    borderRadius: 4,
    padding: '4px 12px',
    cursor: 'pointer',
  },
}
