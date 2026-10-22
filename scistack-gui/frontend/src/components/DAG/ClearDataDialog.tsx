/**
 * Clear all data, keep the pipeline (.claude/plan-clear-data.md, GUI half).
 *
 * Two RPCs, like the CLI: `clear_data_plan` (adopts every canvas first, so
 * the counts describe what the clear will see) and `clear_data` with the
 * plan's fingerprint (refused if the database changed in between). The
 * canvas is a stored document since plan-canvas-as-document Stage 2, so it
 * survives the clear with nothing to snapshot or restore.
 */

import { useEffect, useState } from 'react'
import { callBackend } from '../../api'

interface ClearPlan {
  by_variable: Record<string, number>
  total_records: number
  runs_kept: number
  pins_to_release: string[]
  fingerprint: string
  warnings: string[]
}

const CONFIRM_WORD = 'clear'

export default function ClearDataDialog({ onClose }: { onClose: () => void }) {
  const [plan, setPlan] = useState<ClearPlan | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [typed, setTyped] = useState('')
  const [backup, setBackup] = useState(true)
  const [busy, setBusy] = useState(false)
  const [done, setDone] = useState<string | null>(null)

  useEffect(() => {
    callBackend('clear_data_plan', {})
      .then(p => setPlan(p as ClearPlan))
      .catch(err => setError((err as Error).message))
  }, [])

  const confirm = async () => {
    if (!plan) return
    setBusy(true)
    setError(null)
    try {
      const r = await callBackend('clear_data', {
        fingerprint: plan.fingerprint,
        reason: 'cleared from the GUI',
        backup,
      }) as { deleted_records: number; backup_path: string | null }
      setDone(
        `Deleted ${r.deleted_records} record(s). The pipeline is kept.` +
          (r.backup_path ? ` Backup: ${r.backup_path}` : '')
      )
    } catch (err) {
      setError((err as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const variables = plan ? Object.entries(plan.by_variable) : []
  return (
    <div style={s.overlay} onClick={onClose}>
      <div style={s.dialog} onClick={e => e.stopPropagation()}>
        <div style={s.title}>Clear all data (keep the pipeline)</div>
        {!plan && !error && <div style={s.body}>Working out what would be deleted…</div>}
        {plan && !done && (
          <div style={s.body}>
            <div>
              {plan.total_records} record(s) in {variables.length} variable(s) will be
              deleted, with their run history. Every canvas, its nodes, edges and
              Parameter values, and the run log ({plan.runs_kept} run(s)) are kept.
            </div>
            {variables.length > 0 && (
              <ul style={s.list}>
                {variables.map(([v, n]) => (
                  <li key={v}>
                    <code>{v}</code>: {n}
                  </li>
                ))}
              </ul>
            )}
            {plan.warnings.map(w => (
              <div key={w} style={s.warn}>{w}</div>
            ))}
            <label style={s.row}>
              <input type="checkbox" checked={backup} onChange={e => setBackup(e.target.checked)} />
              keep a backup copy of the database file first
            </label>
            <label style={s.row}>
              type <code>{CONFIRM_WORD}</code> to confirm:
              <input value={typed} onChange={e => setTyped(e.target.value)} style={s.input} />
            </label>
          </div>
        )}
        {done && <div style={s.body}>{done}</div>}
        {error && <div style={s.error}>{error}</div>}
        <div style={s.footer}>
          <button type="button" style={s.button} onClick={onClose}>
            {done ? 'Close' : 'Cancel'}
          </button>
          {plan && !done && (
            <button
              type="button"
              style={{ ...s.button, ...s.danger }}
              disabled={busy || typed.trim() !== CONFIRM_WORD}
              onClick={confirm}
            >
              {busy ? 'Clearing…' : 'Clear data'}
            </button>
          )}
        </div>
      </div>
    </div>
  )
}

const s: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.55)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 2000,
  },
  dialog: {
    background: '#1a1a2e', color: '#ddd', border: '1px solid #c0504d',
    borderRadius: 6, padding: '16px 20px', width: 'min(560px, 88vw)',
    boxShadow: '0 8px 32px rgba(0,0,0,0.5)',
  },
  title: { fontSize: 14, fontWeight: 600, color: '#e07070', marginBottom: 10 },
  body: { fontSize: 12, lineHeight: 1.55, marginBottom: 10 },
  list: { margin: '6px 0 6px 18px', padding: 0, maxHeight: 160, overflowY: 'auto' },
  warn: { color: '#e0b050', fontSize: 11, marginTop: 4 },
  row: { display: 'flex', alignItems: 'center', gap: 6, marginTop: 8 },
  input: {
    background: '#0f0f1e', color: '#ddd', border: '1px solid #3a3a5a',
    borderRadius: 3, padding: '2px 6px', fontSize: 12, width: 90,
  },
  error: { color: '#e07070', fontSize: 12, marginBottom: 10 },
  footer: { display: 'flex', justifyContent: 'flex-end', gap: 8 },
  button: {
    padding: '5px 14px', background: '#2a2a4a', color: '#ccc',
    border: '1px solid #3a3a5a', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  danger: { background: '#5a2020', borderColor: '#c0504d', color: '#fff' },
}
