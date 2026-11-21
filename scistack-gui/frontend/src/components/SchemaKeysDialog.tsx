/**
 * SchemaKeysDialog — change the schema keys (GitHub issue #10). Opened by
 * RemedyDialog's "Change the schema keys…" button (with the failed node,
 * which is run again afterwards) and by the header's "Schema keys…" button
 * (no node: the proposal is the current keys, and nothing runs).
 *
 * After a change it shows what happened: the keys before and after, where a
 * renamed key's references were renamed (config, entities file, canvas,
 * saved plots and stats; scidb.project.key_rename), and what still names an
 * old key and must be fixed by hand.
 *
 * Two modes, decided by the backend's `dataset_empty`: an EMPTY dataset takes
 * the editor below (any key list, renames included); a dataset that HOLDS DATA
 * can only grow: its tree is shown read-only with which branches hold data, and
 * "Add a branch" appends new keys (a branch, or a deeper level), which changes
 * the meaning of nothing that exists (scidb.store.schema_keys_change).
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
import { declarationOf, describeBranches, parseSchemaText } from './schemaText'
import { referenceLines, type ChangeResult } from './schemaKeysResult'

interface Proposal {
  existing: string[]
  proposed: { key: string; source: 'path_input' | 'data_column' }[]
  dataset_empty: boolean
  /** The tree as one list per branch. */
  existing_branches?: string[][]
  /** Which branches hold data (a used key cannot be renamed, moved or deleted). */
  usage?: { branch: string[]; used: boolean; records: number }[]
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

/** Fired on window after the keys change, so whatever shows them (the
 * header's `schema: [...]`, the Inventory and Coverage popups' keys) asks
 * the backend again instead of keeping the keys it fetched at startup. */
export const SCHEMA_KEYS_CHANGED_EVENT = 'scistack:schema-keys-changed'

export default function SchemaKeysDialog({ nodeId = null, fnName = '', dataColumns = [], onClose, onApplied }: {
  /** The failed node (remedy popup), or null (header button: nothing reruns). */
  nodeId?: string | null
  fnName?: string
  dataColumns?: string[]
  onClose: () => void
  /** The keys were changed (and, with a node, it was asked to run again). */
  onApplied: () => void
}) {
  const [existing, setExisting] = useState<string[]>([])
  const [rows, setRows] = useState<Row[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [hasData, setHasData] = useState(false)
  const [branches, setBranches] = useState<string[][]>([])
  const [usage, setUsage] = useState<NonNullable<Proposal['usage']>>([])
  const [newBranchText, setNewBranchText] = useState('')
  const [done, setDone] = useState<ChangeResult | null>(null)

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
        setHasData(!p.dataset_empty)
        setBranches(p.existing_branches ?? [p.existing])
        setUsage(p.usage ?? [])
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
  // A dataset with data only grows: the existing tree, then the new branch(es).
  const newBranches = parseSchemaText(newBranchText)
  const sent: string[] | string[][] = hasData
    ? declarationOf([...branches, ...newBranches])
    : keys
  const kept = new Set(rows.map(r => r.origin).filter((o): o is string => !!o))
  const dropped = existing.filter(k => !kept.has(k))

  const apply = async () => {
    setBusy(true)
    setError(null)
    // eslint-disable-next-line no-console
    console.debug(`[schema-keys] apply node=${nodeId} keys=${JSON.stringify(sent)} renames=${JSON.stringify(hasData ? {} : renames)}`)
    try {
      const r = await callBackend('change_schema_keys', { keys: sent, renames: hasData ? {} : renames }) as
        ChangeResult | { ok: false; error?: string }
      if (!r.ok) {
        setError(r.error ?? 'The schema keys could not be changed.')
        return
      }
      // eslint-disable-next-line no-console
      console.debug(`[schema-keys] changed ${r.old_keys} -> ${r.new_keys} references=${JSON.stringify(r.references)} leftovers=${r.leftovers?.length ?? 0}`)
      window.dispatchEvent(new CustomEvent(SCHEMA_KEYS_CHANGED_EVENT, { detail: { keys: r.new_keys } }))
      if (nodeId) {
        const detail: RerunNodeDetail = { nodeId }
        window.dispatchEvent(new CustomEvent(RERUN_NODE_EVENT, { detail }))
      }
      setDone(r)
    } catch (err) {
      setError(`Could not change the schema keys: ${String(err)}`)
    } finally {
      setBusy(false)
    }
  }

  if (done) {
    return (
      <div style={overlay} onClick={onApplied}>
        <div style={{ ...dialog, ...styles.panel }} onClick={e => e.stopPropagation()}>
          <div style={dialogTitle}>Schema keys changed</div>
          <div style={styles.mono}>{done.old_keys.join(', ')} → {done.new_keys.join(', ')}</div>
          {referenceLines(done).length > 0 && (
            <>
              <div style={{ ...styles.heading, marginTop: 10 }}>Renamed everywhere it was named</div>
              {referenceLines(done).map(line => <div key={line} style={styles.note}>{line}</div>)}
            </>
          )}
          {(done.leftovers?.length ?? 0) > 0 && (
            <>
              <div style={{ ...styles.heading, marginTop: 10 }}>Still to check by hand</div>
              {done.leftovers!.map(line => <div key={line} style={styles.leftover}>{line}</div>)}
            </>
          )}
          {nodeId && <div style={styles.note}>{fnName} is running again with the new keys.</div>}
          <div style={styles.footer}>
            <button type="button" style={styles.button} onClick={onApplied}>Done</button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div style={overlay} onClick={onClose}>
      <div style={{ ...dialog, ...styles.panel }} onClick={e => e.stopPropagation()}>
        <div style={dialogTitle}>Change the schema keys</div>
        {nodeId ? (
          <>
            <div style={styles.text}>
              {fnName} loaded a table with more than one row per location. Choose the keys that tell
              its rows apart. Order matters: coarse to fine (for example subject, then visit).
            </div>
            <div style={styles.note}>
              Measure columns are listed too. Delete every column that is not a key.
            </div>
          </>
        ) : (
          <div style={styles.text}>
            Add, remove, rename or reorder the keys that locate every record. Order matters:
            coarse to fine (for example subject, session, trial). A renamed key is renamed
            everywhere the project names it: the config, PathInputs, node settings, saved plots
            and stats.
          </div>
        )}
        {loading && <div style={styles.text}>Loading…</div>}
        {!loading && hasData && (
          <div>
            <div style={styles.text}>
              This dataset already holds data, so the keys it uses are fixed. You can add a
              branch (or a deeper level) of new keys, and change keys that no record or run uses.
            </div>
            <div style={styles.heading}>Current keys</div>
            {usage.length > 0
              ? usage.map(u => (
                  <div key={u.branch.join('>')} style={styles.mono}>
                    {u.branch.join(' → ')} · {u.used ? `in use (${u.records} record(s))` : 'unused'}
                  </div>
                ))
              : branches.map(b => <div key={b.join('>')} style={styles.mono}>{b.join(' → ')}</div>)}
            <div style={{ ...styles.heading, marginTop: 10 }}>Add a branch</div>
            <input
              style={{ ...styles.input, width: '100%', boxSizing: 'border-box' }}
              value={newBranchText}
              placeholder="brand, location, person   (start with a key that exists to branch from it)"
              onChange={e => setNewBranchText(e.target.value)}
            />
            {newBranches.length > 0 && <div style={styles.note}>{describeBranches(newBranches)}</div>}
          </div>
        )}
        {!loading && !hasData && (
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
        {!nodeId && !loading && <BetweenKeys />}
        {error && <div style={styles.error}>{error}</div>}
        <div style={styles.footer}>
          <button type="button" style={styles.button} onClick={onClose} disabled={busy}>Cancel</button>
          <button
            type="button"
            style={styles.button}
            onClick={apply}
            disabled={busy || loading || (hasData ? newBranches.length === 0 : keys.length === 0 || keys.some(k => !k))}
          >
            {(hasData ? 'Add keys' : 'Change keys') + (nodeId ? ' and run again' : '')}
          </button>
        </div>
      </div>
    </div>
  )
}

/**
 * Between keys (GitHub #28): a key whose levels are taken within each
 * stratum of the between keys when planned locations are built (a study
 * arm, a site). Declared explicitly, never worked out from the data; an
 * undeclared key is crossed. scidb owns the setting
 * (`scidb.store.between_keys`); a tick saves it at once.
 */
function BetweenKeys() {
  const [keys, setKeys] = useState<string[]>([])
  const [between, setBetween] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    callBackend('data_view', { part: 'between' })
      .then(r => {
        const d = r as { between: string[]; schema_keys: string[] }
        setKeys(d.schema_keys)
        setBetween(d.between)
      })
      .catch(err => setError((err as Error).message))
  }, [])

  const toggle = async (key: string) => {
    const next = between.includes(key) ? between.filter(k => k !== key) : [...between, key]
    try {
      const r = await callBackend('set_between_keys', { keys: next }) as { between: string[] }
      setBetween(r.between)
      setError(null)
      console.info('[SchemaKeys] between keys now', r.between)
    } catch (err) {
      setError((err as Error).message)
    }
  }

  if (keys.length === 0) return null
  return (
    <div style={{ marginTop: 10 }}>
      <div style={styles.heading}>Between keys</div>
      <div style={styles.note}>
        Tick a key that splits the study into groups (an arm, a site). Other keys' levels are then
        expected only within each group, so a subject of one arm is never missing at another arm's
        sessions. An unticked key is crossed with every other.
      </div>
      {keys.map(k => (
        <label key={k} style={{ ...styles.mono, display: 'inline-flex', gap: 4, marginRight: 12 }}>
          <input type="checkbox" checked={between.includes(k)} onChange={() => void toggle(k)} /> {k}
        </label>
      ))}
      {error && <div style={styles.error}>{error}</div>}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: { width: 'min(720px, 94vw)', borderTop: '3px solid #f59e0b' },
  text: { fontSize: 13, lineHeight: 1.45, margin: '4px 0 8px' },
  note: { fontSize: 11, color: '#aaa', margin: '4px 0' },
  error: { fontSize: 12, color: '#f87171', margin: '8px 0', userSelect: 'text' },
  leftover: { fontSize: 12, color: '#fbbf24', margin: '2px 0', userSelect: 'text' },
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
