/**
 * TryPopup — "▶ Try on…": run ONE function at some locations, save nothing
 * (GitHub #28).
 *
 * Pick locations with the node's own location picker (selection mode), then
 * start an ordinary run with `run_options.try = true`. The backend runs it
 * with save off and run recording suppressed (`scidb.run.try_step`), so not
 * even `_run_summary` is written, and `run_done.try_results` carries what it
 * computed: each output's value preview, the findings it would have stored,
 * failures, printed output and timing. Downstream steps cannot consume it.
 */

import { useCallback, useState } from 'react'
import { callBackend } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import { overlay } from '../modalStyles'
import SchemaLocationPicker from '../PlotStudio/SchemaLocationPicker'
import type { LocationSelection } from '../PlotStudio/locationSelection'

export interface TryTarget {
  nodeId: string
  functionName: string
  outputs: string[]
  schemaLevel?: string[] | null
  runOptions?: Record<string, unknown> | null
  language?: string
}

interface TryResult {
  function_name: string
  rows: { location: Record<string, string>; outputs: Record<string, { shape_text: string; text: string; dtype: string }> }[]
  total_rows: number
  truncated?: boolean
  findings: { variable: string; location: Record<string, string>; label: string; text: string }[]
  failures: { reason?: string; count?: number; blame_label?: string }[]
  printed: string
  duration_ms: number
  notes: string[]
}

export default function TryPopup({ target, onClose }: { target: TryTarget; onClose: () => void }) {
  const [selection, setSelection] = useState<LocationSelection | null>(null)
  const [picking, setPicking] = useState(true)
  const [runId, setRunId] = useState<string | null>(null)
  const [results, setResults] = useState<TryResult[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useBackendMessage(useCallback((msg) => {
    if ((msg.type ?? msg.method) !== 'run_done' || msg.run_id !== runId) return
    const tr = msg.try_results as TryResult[] | undefined
    console.info(`[Try] run_done ${runId}: ${tr?.length ?? 0} result(s)`)
    setResults(tr ?? [])
    if (!msg.success && msg.error) setError(String(msg.error))
  }, [runId]))

  const start = async () => {
    const id = `try-${Math.random().toString(36).slice(2, 10)}`
    setRunId(id)
    setPicking(false)
    setResults(null)
    setError(null)
    try {
      await callBackend('start_run', {
        function_name: target.functionName,
        node_id: target.nodeId,
        variants: [],
        run_id: id,
        schema_selection: selection,
        schema_level: target.schemaLevel ?? null,
        run_options: { ...(target.runOptions ?? {}), try: true },
        language: target.language,
        output_types: target.outputs,
      })
    } catch (err) {
      setError((err as Error).message)
      setResults([])
    }
  }

  if (target.language === 'matlab') {
    return (
      <div style={overlay} onClick={onClose}>
        <div style={styles.dialog} onClick={e => e.stopPropagation()}>
          <b>Try is not available for MATLAB functions yet.</b>
          <div><button type="button" style={styles.btn} onClick={onClose}>Close</button></div>
        </div>
      </div>
    )
  }
  if (picking) {
    return (
      <>
        <SchemaLocationPicker
          variable=""
          nodeId={target.nodeId}
          value={selection}
          onChange={setSelection}
          onPick={path => setSelection({ include: [path], exclude_levels: {} })}
          onClose={onClose}
        />
        <div style={styles.tryBar}>
          <b>▶ Try {target.functionName}</b> on the ticked locations (nothing is saved)
          <button type="button" style={styles.btn} onClick={start}>Try</button>
          <button type="button" style={styles.btn} onClick={onClose}>Cancel</button>
        </div>
      </>
    )
  }
  return (
    <div style={overlay} onClick={onClose}>
      <div style={styles.dialog} onClick={e => e.stopPropagation()} data-testid="try-popup">
        <div style={styles.head}>
          <b>Try — {target.functionName}</b> <span style={styles.muted}>nothing was saved</span>
          <button type="button" style={styles.close} onClick={onClose}>×</button>
        </div>
        {error && <div style={styles.error}>{error}</div>}
        {results === null && <div style={styles.muted}>Running…</div>}
        {results?.map((r, i) => (
          <div key={i} style={styles.section}>
            <div style={styles.muted}>{r.total_rows} location(s) in {r.duration_ms} ms{r.truncated ? ' (first shown)' : ''}</div>
            {r.failures.map((f, j) => <div key={j} style={styles.error}>✕ {f.blame_label ?? 'Failed'} at {f.count} location(s): {f.reason}</div>)}
            {r.findings.map((f, j) => <div key={j} style={styles.warn}>⚠ {f.text}</div>)}
            {r.notes.map((n, j) => <div key={j} style={styles.muted}>{n}</div>)}
            <table style={styles.table}>
              <tbody>
                {r.rows.map((row, j) => (
                  <tr key={j}>
                    <td style={styles.td}>{Object.entries(row.location).map(([k, v]) => `${k}=${v}`).join(', ') || '(whole dataset)'}</td>
                    {Object.entries(row.outputs).map(([name, o]) => (
                      <td key={name} style={styles.td}>{name}: {o.shape_text}{o.text ? ` = ${o.text}` : ''}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            {r.printed && <pre style={styles.printed}>{r.printed}</pre>}
          </div>
        ))}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  dialog: { background: '#fff', color: '#1f2937', borderRadius: 8, padding: 14, width: 720, maxHeight: '80vh', overflow: 'auto', fontSize: 12 },
  head: { display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 },
  close: { marginLeft: 'auto', border: 'none', background: 'none', fontSize: 18, cursor: 'pointer' },
  muted: { color: '#6b7280' },
  error: { color: '#b91c1c' },
  warn: { color: '#b45309' },
  section: { borderTop: '1px solid #e5e7eb', paddingTop: 6, marginTop: 6 },
  table: { borderCollapse: 'collapse', marginTop: 6 },
  td: { border: '1px solid #f1f5f9', padding: '2px 6px', whiteSpace: 'nowrap' },
  printed: { background: '#0f172a', color: '#e2e8f0', padding: 8, maxHeight: 200, overflow: 'auto', fontSize: 11 },
  btn: { marginLeft: 8, border: '1px solid #cbd5e1', borderRadius: 4, padding: '2px 10px', cursor: 'pointer', background: '#f8fafc' },
  tryBar: {
    position: 'fixed', left: '50%', bottom: 24, transform: 'translateX(-50%)', zIndex: 10001,
    background: '#ecfeff', border: '1px solid #0e7490', borderRadius: 6, padding: '8px 12px', fontSize: 13,
  },
}
