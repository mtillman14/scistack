/**
 * InventoryPopup — modal opened from the header's 📋 Inventory button.
 *
 * "Which combinations of the schema keys I chose have no data?" Every status,
 * both views ("missing only" / "all") and the CSV layout come from
 * `scidb.inventory` through the `schema_inventory` / `export_schema_inventory`
 * handlers; this component only draws what it is sent
 * (docs/claude/data-inventory.md). The backend sends at most 5,000 rows; the
 * CSV export always writes every row.
 *
 * Each key has one role: Cross (every value expected at every combination),
 * Within (the inventory runs separately per combination of these keys'
 * values, e.g. a study arm), or Off (projected out).
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { callBackend, isVSCodeMode } from '../api'
import * as modalStyles from './modalStyles'

type Role = 'cross' | 'within' | 'off'

const ROLES: { role: Role; label: string; title: string }[] = [
  { role: 'cross', label: 'Cross', title: 'Every value is expected at every combination' },
  { role: 'within', label: 'Within', title: 'Run the inventory separately for each value (e.g. a study arm); never crossed' },
  { role: 'off', label: 'Off', title: 'Projected out' },
]

interface StatusCounts { present: number; missing: number; excluded: number }

interface StratumSummary {
  values: Record<string, string>
  product_size: number
  counts: StatusCounts
}

interface InventoryResult {
  keys: string[]
  within: string[]
  strata: StratumSummary[]
  levels: Record<string, string[]>
  columns: string[]
  rows: string[][]
  total_rows: number
  truncated: boolean
  counts: { present: number; missing: number; excluded: number }
  product_size: number
  missing_only: boolean
  collapse: boolean
  // Text of a collapsed Cross cell ("(all)"); owned by scidb.inventory.ALL_LEVELS.
  all_marker: string
}

const STATUS_COLORS: Record<string, string> = {
  present: '#9ae6b4',
  missing: '#f87171',
  excluded: '#888',
}

function Tally({ counts }: { counts: StatusCounts }) {
  return (
    <>
      <span style={{ color: STATUS_COLORS.present }}>{counts.present.toLocaleString()} present</span>,{' '}
      <span style={{ color: STATUS_COLORS.missing }}>{counts.missing.toLocaleString()} missing</span>,{' '}
      <span style={{ color: STATUS_COLORS.excluded }}>{counts.excluded.toLocaleString()} excluded</span>
    </>
  )
}

export default function InventoryPopup({
  schemaKeys,
  onClose,
}: {
  schemaKeys: string[]
  onClose: () => void
}) {
  const [roles, setRoles] = useState<Record<string, Role>>(
    () => Object.fromEntries(schemaKeys.map(k => [k, 'cross' as Role])),
  )
  const [missingOnly, setMissingOnly] = useState(true)
  const [collapse, setCollapse] = useState(true)
  const [result, setResult] = useState<InventoryResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  // Only the newest query may land: a slow answer to an older checkbox state
  // must not overwrite the table for the current one.
  const queryId = useRef(0)

  const keys = schemaKeys.filter(k => roles[k] === 'cross')
  const within = schemaKeys.filter(k => roles[k] === 'within')

  const run = useCallback(() => {
    if (keys.length === 0) return
    const id = ++queryId.current
    setLoading(true)
    setError('')
    callBackend('schema_inventory', { keys, within, missing_only: missingOnly, collapse })
      .then(d => {
        if (id === queryId.current) setResult(d as InventoryResult)
      })
      .catch(err => {
        if (id !== queryId.current) return
        setResult(null)
        setError((err as Error).message)
      })
      .finally(() => {
        if (id === queryId.current) setLoading(false)
      })
    // keys/within are derived from roles; joining keeps the dependency stable.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [keys.join('\u0000'), within.join('\u0000'), missingOnly, collapse])

  useEffect(() => { run() }, [run])

  const doExport = async () => {
    if (keys.length === 0) return
    const defaultName = missingOnly ? 'missing_combinations.csv' : 'inventory.csv'
    let path: string | null
    if (isVSCodeMode) {
      const picked = await callBackend('pick_save_path', {
        defaultName,
        formats: ['csv'],
        filterName: 'CSV',
      })
      path = (picked as { path: string | null }).path
    } else {
      path = window.prompt('Save the inventory CSV as (absolute path):', defaultName)
    }
    if (!path) return // cancelled
    setError('')
    setMessage('Exporting…')
    try {
      const r = await callBackend('export_schema_inventory', {
        keys,
        within,
        missing_only: missingOnly,
        collapse,
        out_path: path,
      }) as { path: string; rows: number }
      setMessage(`Wrote ${r.rows.toLocaleString()} row(s) to ${r.path}`)
    } catch (err) {
      setMessage('')
      setError((err as Error).message)
    }
  }

  const statusCol = result ? result.columns.indexOf('status') : -1

  return (
    <div style={modalStyles.overlay} onClick={onClose}>
      <div style={styles.dialog} onClick={e => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={modalStyles.dialogTitle}>Data inventory</span>
          <button style={styles.closeBtn} onClick={onClose} title="Close" type="button">×</button>
        </div>
        <div style={styles.note}>
          Every combination of the Cross keys' values, against the rows the
          database holds. A deeper row counts for the keys you choose. Within
          keys (e.g. a study arm) run the inventory separately for each of
          their values, crossing only the values found there.
        </div>

        <div style={styles.controls}>
          <span style={styles.label}>Keys:</span>
          {schemaKeys.map(k => (
            <span key={k} style={styles.keyRole}>
              <span>{k}</span>
              <span style={styles.segment}>
                {ROLES.map(({ role, label, title }) => (
                  <button
                    key={role}
                    type="button"
                    title={title}
                    style={roles[k] === role ? styles.segOn : styles.segOff}
                    onClick={() => setRoles(r => ({ ...r, [k]: role }))}
                  >
                    {label}
                  </button>
                ))}
              </span>
            </span>
          ))}
        </div>
        <div style={styles.controls}>
          <span style={styles.label}>Show:</span>
          <label style={styles.check}>
            <input type="radio" checked={missingOnly} onChange={() => setMissingOnly(true)} />
            <span>Missing only</span>
          </label>
          <label style={styles.check}>
            <input type="radio" checked={!missingOnly} onChange={() => setMissingOnly(false)} />
            <span>All, with status</span>
          </label>
          <label
            style={styles.check}
            title="One row per block whose combinations all have one status, with (all) in the deeper key columns"
          >
            <input type="checkbox" checked={collapse} onChange={e => setCollapse(e.target.checked)} />
            <span>Collapse uniform blocks</span>
          </label>
          <span style={{ flex: 1 }} />
          <button
            style={styles.btn}
            onClick={run}
            disabled={keys.length === 0 || loading}
            type="button"
            title="Run the inventory again"
          >
            {loading ? 'Running…' : '↻ Run'}
          </button>
          <button
            style={styles.primary}
            onClick={doExport}
            disabled={keys.length === 0 || !result}
            type="button"
            title="Write the full table (never truncated) as CSV"
          >
            Export CSV…
          </button>
        </div>

        {keys.length === 0 && <div style={styles.hint}>Make at least one schema key Cross.</div>}
        {error && <div style={styles.errorBlock}>{error}</div>}
        {message && <div style={styles.message}>{message}</div>}

        {result && keys.length > 0 && (
          <>
            <div style={styles.counts}>
              {result.product_size.toLocaleString()} combination(s):{' '}
              <Tally counts={result.counts} />
            </div>
            {result.within.length > 0 && result.strata.map(s => (
              <div key={Object.values(s.values).join('\u0000')} style={styles.stratum}>
                {Object.entries(s.values).map(([k, v]) => `${k}=${v}`).join(', ')}:{' '}
                {s.product_size.toLocaleString()} combination(s):{' '}
                <Tally counts={s.counts} />
              </div>
            ))}
            {result.truncated && (
              <div style={styles.hint}>
                Showing the first {result.rows.length.toLocaleString()} of{' '}
                {result.total_rows.toLocaleString()} rows — Export CSV writes them all.
              </div>
            )}
            {result.rows.length === 0 ? (
              <div style={styles.hint}>
                {result.missing_only ? 'Nothing is missing.' : 'No combinations.'}
              </div>
            ) : (
              <div style={styles.tableWrap}>
                <table style={styles.table}>
                  <thead>
                    <tr>
                      {result.columns.map(c => <th key={c} style={styles.th}>{c}</th>)}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, i) => (
                      <tr key={i}>
                        {row.map((cell, j) => (
                          <td
                            key={j}
                            style={{
                              ...styles.td,
                              ...(j === statusCol ? { color: STATUS_COLORS[cell] ?? '#ccc' } : {}),
                              ...(j !== statusCol && cell === result.all_marker
                                ? { fontStyle: 'italic', color: '#aaa' }
                                : {}),
                            }}
                          >
                            {cell}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  dialog: { ...modalStyles.dialog, width: 640, display: 'flex', flexDirection: 'column', fontSize: 12 },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' },
  closeBtn: {
    background: 'transparent',
    border: 'none',
    color: '#aaa',
    fontSize: 18,
    cursor: 'pointer',
    lineHeight: 1,
  },
  note: { color: '#888', fontSize: 11, marginBottom: 10 },
  controls: { display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 10, margin: '4px 0' },
  label: { color: '#aaa', minWidth: 44 },
  check: { display: 'flex', alignItems: 'center', gap: 4, cursor: 'pointer' },
  counts: { marginTop: 10, marginBottom: 4, color: '#ddd' },
  stratum: { marginLeft: 14, marginBottom: 2, color: '#bbb', fontSize: 11 },
  keyRole: { display: 'flex', alignItems: 'center', gap: 4 },
  segment: { display: 'inline-flex', border: '1px solid #3a3a5a', borderRadius: 3, overflow: 'hidden' },
  segOn: {
    background: '#7b68ee',
    border: 'none',
    color: '#fff',
    padding: '1px 6px',
    fontSize: 11,
    cursor: 'pointer',
  },
  segOff: {
    background: 'transparent',
    border: 'none',
    color: '#aaa',
    padding: '1px 6px',
    fontSize: 11,
    cursor: 'pointer',
  },
  hint: { color: '#888', fontSize: 11, margin: '6px 0' },
  message: { marginTop: 6, color: '#9ae6b4', wordBreak: 'break-all' },
  errorBlock: { marginTop: 6, color: '#f87171', wordBreak: 'break-word' },
  tableWrap: {
    marginTop: 6,
    maxHeight: '45vh',
    overflow: 'auto',
    border: '1px solid #2a2a4a',
    borderRadius: 4,
  },
  table: { borderCollapse: 'collapse', width: '100%' },
  th: {
    position: 'sticky',
    top: 0,
    background: '#22223a',
    textAlign: 'left',
    padding: '4px 8px',
    borderBottom: '1px solid #3a3a5a',
    color: '#ccc',
  },
  td: { padding: '2px 8px', borderBottom: '1px solid #23233a', fontFamily: 'monospace', color: '#ccc' },
  btn: {
    background: 'transparent',
    border: '1px solid #3a3a5a',
    borderRadius: 3,
    color: '#ccc',
    padding: '4px 10px',
    cursor: 'pointer',
  },
  primary: {
    background: '#7b68ee',
    border: 'none',
    borderRadius: 3,
    color: '#fff',
    padding: '4px 12px',
    cursor: 'pointer',
  },
}
