/**
 * CoveragePopup — which variables have which values of a schema key, in
 * pipeline order, with the drop-out funnel ("40 subjects, then 38, then 31").
 *
 * Each cell summarises that variable's expected locations at that value
 * (scidb.provenance.coverage through `expectation_coverage`), so it can never
 * disagree with the variable's own Expected locations popup, which a click
 * on the cell opens, filtered to that value.
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import { callBackend } from '../../api'
import * as modalStyles from '../modalStyles'
import ExpectedLocationsPopup from './ExpectedLocationsPopup'
import { STATUS_ICON, TONE_COLOR, type CoverageCell, type CoverageResult } from './expectationTypes'
import { styles } from './expectationStyles'

function cellText(cell: CoverageCell | undefined): { text: string; color: string; title: string } {
  if (!cell || cell.tone === 'empty') return { text: '', color: TONE_COLOR.empty, title: 'nothing expected or present' }
  const parts = Object.entries(cell.counts).filter(([, n]) => n > 0).map(([s, n]) => `${n} ${s.replace('_', ' ')}`)
  const title = parts.join(', ')
  if (cell.tone === 'problem') return { text: `${STATUS_ICON.missing_unexpected}${cell.problems > 1 ? cell.problems : ''}`, color: TONE_COLOR.problem, title }
  if (cell.tone === 'excluded') return { text: STATUS_ICON.excluded, color: TONE_COLOR.excluded, title }
  if (cell.present === 0) return { text: STATUS_ICON.missing_expected, color: TONE_COLOR.ok, title }
  return { text: STATUS_ICON.present, color: TONE_COLOR.ok, title }
}

export default function CoveragePopup({ schemaKeys, onClose }: { schemaKeys: string[]; onClose: () => void }) {
  const [by, setBy] = useState<string[]>(schemaKeys.slice(0, 1))
  const [result, setResult] = useState<CoverageResult | null>(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const [open, setOpen] = useState<{ variable: string; values?: Record<string, string> } | null>(null)
  const queryId = useRef(0)

  const load = useCallback(() => {
    if (by.length === 0) return
    const id = ++queryId.current
    setLoading(true)
    setError('')
    callBackend('expectation_coverage', { by })
      .then(d => { if (id === queryId.current) setResult(d as CoverageResult) })
      .catch(err => { if (id === queryId.current) { setResult(null); setError((err as Error).message) } })
      .finally(() => { if (id === queryId.current) setLoading(false) })
  }, [by])

  useEffect(() => { load() }, [load])

  return (
    <div style={modalStyles.overlay} onClick={onClose}>
      <div style={styles.dialog} data-testid="coverage-popup" onClick={e => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={modalStyles.dialogTitle}>Coverage</span>
          <button style={styles.closeBtn} onClick={onClose} title="Close" type="button">×</button>
        </div>
        <div style={styles.note}>
          Each variable (pipeline order) at each value: {STATUS_ICON.present} as expected,{' '}
          {STATUS_ICON.missing_unexpected}N N locations not as expected, {STATUS_ICON.missing_expected} only
          missing as expected, {STATUS_ICON.excluded} excluded. The last row counts the values each
          variable has data for. Click a cell for its locations and why.
        </div>
        <div style={styles.controls}>
          <span style={styles.label}>By:</span>
          {schemaKeys.map(k => (
            <label key={k} style={{ display: 'flex', gap: 3, alignItems: 'center' }}>
              <input
                type="checkbox"
                checked={by.includes(k)}
                onChange={e => setBy(prev => schemaKeys.filter(x => (x === k ? e.target.checked : prev.includes(x))))}
              />
              {k}
            </label>
          ))}
          {loading && <span style={styles.hint}>Loading… (one expected-locations check per variable)</span>}
        </div>
        {error && <div style={styles.error}>{error}</div>}
        {result && (
          <>
            <div style={styles.tableWrap}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    {result.by.map(k => <th key={k} style={styles.th}>{k}</th>)}
                    {result.variables.map(v => (
                      <th key={v} style={{ ...styles.th, cursor: 'pointer' }} title={`Open ${v}'s expected locations`}
                        onClick={() => setOpen({ variable: v })}>
                        {v}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.values.map(value => {
                    const key = value.join('|')
                    return (
                      <tr key={key} data-testid="coverage-row">
                        {value.map((v, i) => <td key={i} style={styles.td}>{v}</td>)}
                        {result.variables.map(variable => {
                          const c = cellText(result.cells[variable]?.[key])
                          return (
                            <td
                              key={variable}
                              style={{ ...styles.td, color: c.color, cursor: 'pointer', textAlign: 'center' }}
                              title={`${variable} at ${result.by.map((k, i) => `${k}=${value[i]}`).join(', ')}: ${c.title}`}
                              onClick={() => setOpen({
                                variable,
                                values: Object.fromEntries(result.by.map((k, i) => [k, value[i]])),
                              })}
                            >
                              {c.text}
                            </td>
                          )
                        })}
                      </tr>
                    )
                  })}
                  <tr>
                    <td style={{ ...styles.td, color: '#aaa' }} colSpan={result.by.length}>with data</td>
                    {result.funnel.map(f => (
                      <td key={f.variable} style={{ ...styles.td, textAlign: 'center', color: '#ddd' }}
                        title={`${f.variable} has data at ${f.with_data} of the ${f.expected} values where anything is expected`}>
                        {f.with_data}/{f.expected}
                      </td>
                    ))}
                  </tr>
                </tbody>
              </table>
            </div>
            {Object.entries(result.skipped).map(([v, why]) => (
              <div key={v} style={styles.hint}>Not shown: {v} ({why})</div>
            ))}
          </>
        )}
      </div>
      {open && (
        <div onClick={e => e.stopPropagation()}>
          <ExpectedLocationsPopup
            variable={open.variable}
            initialValues={open.values}
            onClose={() => { setOpen(null); load() }}
          />
        </div>
      )}
    </div>
  )
}
