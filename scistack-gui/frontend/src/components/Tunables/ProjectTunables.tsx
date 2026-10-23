/**
 * Every placement of a library pipeline in the project, nested ones
 * included, with its tunables (.claude/plan-library-fork-chain.md E2).
 *
 * A view over `project_tunables`; each row is a `TunablesPanel` for that
 * placement, so an edit made here goes through the placement's own binding
 * (one owner) exactly as one made on the placed node.
 */

import { useCallback, useEffect, useState } from 'react'
import { callBackend } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import TunablesPanel from './TunablesPanel'
import type { Tunables } from './tunables'

export default function ProjectTunables() {
  const [rows, setRows] = useState<Tunables[]>([])
  const [open, setOpen] = useState(false)
  const [error, setError] = useState('')

  const fetchRows = useCallback(() => {
    callBackend('project_tunables')
      .then(d => { setRows(d as Tunables[]); setError('') })
      .catch(err => setError((err as Error).message))
  }, [])

  useEffect(() => { if (open) fetchRows() }, [open, fetchRows])
  useBackendMessage(useCallback((msg: Record<string, unknown>) => {
    if (open && (msg.type === 'dag_updated' || msg.method === 'dag_updated')) fetchRows()
  }, [open, fetchRows]))

  return (
    <div style={styles.wrap}>
      <button style={styles.header} type="button" onClick={() => setOpen(o => !o)}>
        {open ? '▾' : '▸'} Tunables of placed libraries
      </button>
      {open && rows.length === 0 && !error && <div style={styles.empty}>No library pipeline is placed.</div>}
      {open && rows.map(r => (
        <div key={r.use_id} style={styles.item}>
          <div style={styles.path}>{(r.path ?? []).join(' › ')} <span style={styles.lib}>({r.library})</span></div>
          <TunablesPanel useId={r.use_id} initial={r} onChanged={fetchRows} />
        </div>
      ))}
      {error && <div style={styles.error}>{error}</div>}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  wrap: { marginTop: 8 },
  header: {
    background: 'transparent', border: 'none', color: '#888', cursor: 'pointer', fontSize: 10,
    fontWeight: 700, textTransform: 'uppercase', letterSpacing: 0.6, padding: 0,
  },
  item: { margin: '4px 0 8px 6px' },
  path: { fontSize: 11, color: '#bbb', fontFamily: 'monospace' },
  lib: { color: '#777' },
  empty: { fontSize: 11, color: '#666', padding: '2px 6px' },
  error: { fontSize: 11, color: '#f87171', wordBreak: 'break-word' },
}
