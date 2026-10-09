/**
 * "✓ verify" — did your re-run reproduce the exporter's results?
 * (.claude/plan-portability.md Stage 9; scidb/verify.py.)
 *
 * Compares this project's history with the exporter's (the newest archived
 * history, or a .scistack bundle) and never runs anything. The backend
 * decides every class; this only shows counts and the first divergences.
 */

import { useState } from 'react'
import { callBackend } from '../api'

interface Entry {
  type: string
  function: string
  location: Record<string, string>
  exporter_hash: string
  recipient_hash?: string
  max_abs_diff?: number
  compared_values?: boolean
}

interface Report {
  against: string
  data: boolean
  rtol: number
  atol: number
  ok: boolean
  counts: Record<string, number>
  new: number
  first_divergences: Entry[]
  code_changed: string[]
  not_run: Entry[]
}

const where = (loc: Record<string, string>) =>
  Object.entries(loc).map(([k, v]) => `${k}=${v}`).join(', ') || 'dataset'

export default function VerifyButton() {
  const [open, setOpen] = useState<null | { top: number; left: number }>(null)
  const [against, setAgainst] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [report, setReport] = useState<Report | null>(null)

  const run = () => {
    setBusy(true)
    setError('')
    setReport(null)
    callBackend('verify_reproduction', { against: against.trim() || null })
      .then(d => setReport(d as Report))
      .catch(err => setError((err as Error).message))
      .finally(() => setBusy(false))
  }

  return (
    <>
      <button
        style={styles.trigger}
        type="button"
        title="Compare your re-run with the exporter's history (never runs anything)"
        onClick={e => {
          const r = e.currentTarget.getBoundingClientRect()
          setOpen(open ? null : { top: r.bottom + 4, left: Math.max(8, r.right - 400) })
        }}
      >
        ✓ verify
      </button>
      {open && (
        <div style={{ ...styles.popover, top: open.top, left: open.left }}>
          <div style={styles.title}>Verify reproduction</div>
          <label style={styles.label}>Against (blank: the newest archived history; or a .scistack path)</label>
          <input style={styles.input} value={against} onChange={e => setAgainst(e.target.value)} disabled={busy} />
          {report && (
            <div style={styles.report}>
              <div style={{ color: report.ok ? '#9ae6b4' : '#fbbf24', fontWeight: 600 }}>
                {report.ok ? 'Everything reproduced' : 'Differences found'}
              </div>
              <div style={styles.muted}>
                {report.data
                  ? `values compared within rtol=${report.rtol}, atol=${report.atol}`
                  : "exact comparison (the exporter's data is not available)"}
              </div>
              {Object.entries(report.counts).filter(([, n]) => n > 0).map(([c, n]) => (
                <div key={c}>{c.replace(/_/g, ' ')}: {n}</div>
              ))}
              {report.new > 0 && <div>new (yours only): {report.new}</div>}
              {report.first_divergences.slice(0, 10).map((e, i) => (
                <div key={i} style={styles.warn}>
                  first divergence: {e.function} → {e.type} at {where(e.location)}
                  {e.compared_values && e.max_abs_diff !== undefined ? ` (max |diff| ${e.max_abs_diff.toPrecision(3)})` : ''}
                </div>
              ))}
              {report.code_changed.map(fn => <div key={fn} style={styles.warn}>code changed: {fn}</div>)}
              {report.not_run.slice(0, 5).map((e, i) => (
                <div key={i} style={styles.muted}>not run: {e.function} → {e.type} at {where(e.location)}</div>
              ))}
            </div>
          )}
          {error && <div style={styles.error}>{error}</div>}
          <div style={styles.actions}>
            <button style={styles.btn} onClick={() => setOpen(null)} type="button">Close</button>
            <button style={styles.primary} onClick={run} disabled={busy} type="button">
              {busy ? 'Comparing…' : 'Verify'}
            </button>
          </div>
        </div>
      )}
    </>
  )
}

const styles: Record<string, React.CSSProperties> = {
  trigger: {
    padding: '8px 10px', background: 'transparent', border: 'none', color: '#666',
    fontSize: 12, cursor: 'pointer', whiteSpace: 'nowrap',
  },
  popover: {
    position: 'fixed', zIndex: 1000, width: 400, background: '#1a1a2e', border: '1px solid #3a3a5a',
    borderRadius: 6, padding: 12, boxShadow: '0 6px 20px rgba(0,0,0,0.5)', color: '#ccc', fontSize: 12,
  },
  title: { fontWeight: 700, fontSize: 13, color: '#fff', marginBottom: 6 },
  label: { display: 'block', color: '#999', fontSize: 11, marginTop: 4 },
  input: {
    width: '100%', boxSizing: 'border-box', background: '#111127', border: '1px solid #3a3a5a',
    borderRadius: 3, color: '#ddd', fontSize: 12, padding: '4px 6px', outline: 'none',
  },
  report: { display: 'flex', flexDirection: 'column', gap: 3, marginTop: 8, wordBreak: 'break-word' },
  muted: { color: '#888' },
  warn: { color: '#fbbf24' },
  error: { marginTop: 8, color: '#f87171', wordBreak: 'break-word' },
  actions: { display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 10 },
  btn: { background: 'transparent', border: '1px solid #3a3a5a', borderRadius: 3, color: '#ccc', padding: '4px 10px', cursor: 'pointer' },
  primary: { background: '#7b68ee', border: 'none', borderRadius: 3, color: '#fff', padding: '4px 12px', cursor: 'pointer' },
}
