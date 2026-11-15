/**
 * SchemaKeysDialog — change the schema keys of an EMPTY dataset (GitHub
 * issue #10). Opened by RemedyDialog's "Change the schema keys…" button, which
 * the duplicate-location remedy only offers while the dataset holds no data.
 *
 * Left: the dataset's current keys (read-only). Right: an editable list seeded
 * by the backend's proposal (the PathInput's placeholder keys, then the loaded
 * data's columns). Rules (what a valid key is, what "empty" means, the change
 * itself) live in scidb; this only edits a list and sends it.
 * See docs/claude/changing-schema-keys.md.
 */

import { useEffect, useState } from 'react'
import { callBackend } from '../api'
import { RERUN_NODE_EVENT } from './DAG/runFailures'
import type { RerunNodeDetail } from './DAG/runFailures'
import { dialog, dialogTitle, overlay } from './modalStyles'

interface Proposal {
  existing: string[]
  proposed: { key: string; source: 'path_input' | 'data_column' }[]
  dataset_empty: boolean
}

interface Row {
  id: number
  key: string
  source: 'path_input' | 'data_column' | 'existing' | 'added'
  /** The existing key this row came from; a different `key` is a rename. */
  origin: string | null
}

const SOURCE_LABEL: Record<Row['source'], string> = {
  path_input: 'from the PathInput',
  data_column: 'data column',
  existing: 'existing key',
  added: 'added',
}

export default function SchemaKeysDialog({ nodeId, fnName, dataColumns, onClose, onApplied }: {
  nodeId: string
  fnName: string
  dataColumns: string[]
  onClose: () => void
  /** The keys were changed and the node was asked to run again. */
  onApplied: () => void
}) {
  const [existing, setExisting] = useState<string[]>([])
  const [rows, setRows] = useState<Row[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    let cancelled = false
    callBackend('schema_key_proposal', { node_id: nodeId, data_columns: dataColumns })
      .then(raw => {
        if (cancelled) return
        const p = raw as Proposal
        setExisting(p.existing)
        setRows(p.proposed.map((item, i) => ({
          id: i,
          key: item.key,
          source: item.source,
          origin: p.existing.includes(item.key) ? item.key : null,
        })))
        if (!p.dataset_empty) setError('This dataset already holds data, so its schema keys cannot be changed.')
        // eslint-disable-next-line no-console
        console.debug(`[schema-keys] proposal node=${nodeId} existing=${p.existing} proposed=${p.proposed.map(x => x.key)}`)
      })
      .catch(err => { if (!cancelled) setError(`Could not load the proposal: ${String(err)}`) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [nodeId, dataColumns.join('\u0001')])

  const update = (id: number, patch: Partial<Row>) =>
    setRows(rs => rs.map(r => (r.id === id ? { ...r, ...patch } : r)))
  const remove = (id: number) => setRows(rs => rs.filter(r => r.id !== id))
  const move = (index: number, by: -1 | 1) => setRows(rs => {
    const j = index + by
    if (j < 0 || j >= rs.length) return rs
    const next = rs.slice()
    ;[next[index], next[j]] = [next[j], next[index]]
    return next
  })
  const add = () =>
    setRows(rs => [...rs, { id: Math.max(-1, ...rs.map(r => r.id)) + 1, key: '', source: 'added', origin: null }])

  const renames: Record<string, string> = {}
  for (const r of rows) if (r.origin && r.key.trim() && r.key.trim() !== r.origin) renames[r.origin] = r.key.trim()
  const keys = rows.map(r => r.key.trim())
  const kept = new Set(rows.map(r => r.origin).filter((o): o is string => !!o))
  const dropped = existing.filter(k => !kept.has(k))

  const apply = async () => {
    setBusy(true)
    setError(null)
    // eslint-disable-next-line no-console
    console.debug(`[schema-keys] apply node=${nodeId} keys=${keys} renames=${JSON.stringify(renames)}`)
    try {
      const r = await callBackend('change_schema_keys', { keys, renames }) as {
        ok: boolean; error?: string; leftovers?: string[]
      }
      if (!r.ok) {
        setError(r.error ?? 'The schema keys could not be changed.')
        return
      }
      const detail: RerunNodeDetail = { nodeId }
      window.dispatchEvent(new CustomEvent(RERUN_NODE_EVENT, { detail }))
      onApplied()
    } catch (err) {
      setError(`Could not change the schema keys: ${String(err)}`)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div style={overlay} onClick={onClose}>
      <div style={{ ...dialog, ...styles.panel }} onClick={e => e.stopPropagation()}>
        <div style={dialogTitle}>Change the schema keys</div>
        <div style={styles.text}>
          {fnName} loaded a table with more than one row per location. Choose the keys that tell
          its rows apart. Order matters: coarse to fine (for example subject, then visit).
        </div>
        <div style={styles.note}>
          Measure columns are listed too. Delete every column that is not a key.
        </div>
        {loading && <div style={styles.text}>Loading…</div>}
        {!loading && (
          <div style={styles.columns}>
            <div style={styles.left}>
              <div style={styles.heading}>Current keys</div>
              {existing.map(k => <div key={k} style={styles.mono}>{k}</div>)}
            </div>
            <div style={styles.right}>
              <div style={styles.heading}>New keys</div>
              {rows.map((r, i) => (
                <div key={r.id} style={styles.row}>
                  <input
                    style={styles.input}
                    value={r.key}
                    placeholder="key name"
                    onChange={e => update(r.id, { key: e.target.value })}
                  />
                  <span style={styles.tag}>{SOURCE_LABEL[r.origin ? 'existing' : r.source]}</span>
                  <button type="button" style={styles.small} onClick={() => move(i, -1)} disabled={i === 0} title="Move up">↑</button>
                  <button type="button" style={styles.small} onClick={() => move(i, 1)} disabled={i === rows.length - 1} title="Move down">↓</button>
                  <button type="button" style={styles.small} onClick={() => remove(r.id)} title="Delete this key">✕</button>
                </div>
              ))}
              <button type="button" style={styles.button} onClick={add}>Add key</button>
            </div>
          </div>
        )}
        {dropped.length > 0 && (
          <div style={styles.note}>Removed: {dropped.join(', ')}</div>
        )}
        {error && <div style={styles.error}>{error}</div>}
        <div style={styles.footer}>
          <button type="button" style={styles.button} onClick={onClose} disabled={busy}>Cancel</button>
          <button
            type="button"
            style={styles.button}
            onClick={apply}
            disabled={busy || loading || keys.length === 0 || keys.some(k => !k)}
          >
            Change keys and run again
          </button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: { width: 'min(720px, 94vw)', borderTop: '3px solid #f59e0b' },
  text: { fontSize: 13, lineHeight: 1.45, margin: '4px 0 8px' },
  note: { fontSize: 11, color: '#aaa', margin: '4px 0' },
  error: { fontSize: 12, color: '#f87171', margin: '8px 0', userSelect: 'text' },
  columns: { display: 'flex', gap: 16, margin: '8px 0' },
  left: { flex: '0 0 160px' },
  right: { flex: 1 },
  heading: { fontSize: 12, fontWeight: 700, color: '#aaa', marginBottom: 6 },
  mono: { fontFamily: 'monospace', fontSize: 12, padding: '2px 0' },
  row: { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 },
  input: {
    flex: 1, background: '#12122a', color: '#eee', border: '1px solid #444',
    borderRadius: 4, padding: '3px 6px', fontFamily: 'monospace', fontSize: 12,
  },
  tag: { fontSize: 10, color: '#aaa', minWidth: 90 },
  small: {
    background: '#2a2a4a', color: '#eee', border: '1px solid #444',
    borderRadius: 4, padding: '1px 6px', cursor: 'pointer',
  },
  footer: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 10 },
  button: {
    background: '#2a2a4a', color: '#eee', border: '1px solid #444',
    borderRadius: 4, padding: '4px 12px', cursor: 'pointer',
  },
}
