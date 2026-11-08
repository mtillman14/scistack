/**
 * DataPreviewPopup: what a variable or an input port's column selection holds.
 *
 * Opened from a Variable node's right-click and from the preview button on an
 * input in the function settings panel. Everything shown comes from
 * `scidb.inspect.preview` through `get_data_preview`, which reads with the
 * loader `for_each` uses, so this is what a function receives.
 */

import { useEffect, useState } from 'react'
import { callBackend } from '../../api'
import * as modalStyles from '../modalStyles'
import {
  cellText,
  describeLoadedTypes,
  hasMixedTypes,
  type DataPreviewResult,
  type DataPreviewTarget,
} from './dataPreview'

export default function DataPreviewPopup({
  target,
  onClose,
}: {
  target: DataPreviewTarget
  onClose: () => void
}) {
  const [location, setLocation] = useState<Record<string, string>>({})
  const [result, setResult] = useState<DataPreviewResult | null>(null)
  const columnsKey = JSON.stringify(target.columns ?? null)
  const locationKey = JSON.stringify(location)

  useEffect(() => {
    let cancelled = false
    setResult(null)
    callBackend('get_data_preview', {
      variable: target.variable,
      columns: target.columns?.length ? target.columns : null,
      location,
    })
      .then(raw => { if (!cancelled) setResult(raw as DataPreviewResult) })
      .catch(err => {
        if (!cancelled) setResult({ ok: false, error: String(err) })
      })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target.variable, columnsKey, locationKey])

  const levels = result?.levels ?? {}
  const rows = result?.rows ?? []
  const rowColumns = rows.length ? Object.keys(rows[0]) : []
  const title = target.param ? `${target.param}: ${target.variable}` : target.variable

  return (
    <div style={modalStyles.overlay} onClick={onClose}>
      <div style={styles.dialog} onClick={e => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={modalStyles.dialogTitle}>Data preview: {title}</span>
          <button style={styles.close} onClick={onClose} title="Close" type="button">×</button>
        </div>

        <div style={styles.filters}>
          {(result?.schema_keys ?? Object.keys(levels)).filter(k => levels[k]?.length).map(key => (
            <label key={key} style={styles.filter}>
              {key}
              <select
                value={location[key] ?? ''}
                onChange={e => {
                  const next = { ...location }
                  if (e.target.value) next[key] = e.target.value
                  else delete next[key]
                  setLocation(next)
                }}
              >
                <option value="">all</option>
                {levels[key].map(level => <option key={level} value={level}>{level}</option>)}
              </select>
            </label>
          ))}
        </div>

        {!result && <div style={styles.message}>Loading…</div>}
        {result && !result.ok && <div style={styles.error}>{result.error}</div>}
        {result?.ok && (
          <div style={styles.body}>
            <div style={styles.message}>
              {result.n_records} record(s), {result.n_rows} row(s). As a function receives them.
            </div>
            {result.note && <div style={styles.message}>{result.note}</div>}
            {!!result.missing_columns?.length && (
              <div style={styles.error}>
                Not columns of this variable: {result.missing_columns.join(', ')}
              </div>
            )}
            <table style={styles.table}>
              <thead>
                <tr>
                  {['Column', 'Stored', 'Loaded', 'Non-blank', 'Inside cells', 'Samples'].map(h => (
                    <th key={h} style={styles.th}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(result.columns ?? []).map(c => (
                  <tr key={c.name}>
                    <td style={styles.td}>{c.name}</td>
                    <td style={styles.td}>{c.stored_type}</td>
                    <td
                      style={hasMixedTypes(c) ? styles.mixed : styles.td}
                      title={hasMixedTypes(c) ? 'Cells of this column are not all one type.' : undefined}
                    >
                      {describeLoadedTypes(c.loaded_types)}
                    </td>
                    <td style={styles.td}>{c.non_null}/{c.n_rows}</td>
                    <td
                      style={c.cells && c.cells.problems > 0 ? styles.mixed : styles.td}
                      title={c.cells && c.cells.problems > 0 ? 'Some cells hold NaN values, or are blank, empty or constant. scidb nans <variable> lists them.' : undefined}
                    >
                      {c.cells?.description ?? ''}
                    </td>
                    <td style={styles.td}>{c.samples.join(' | ')}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            <div style={styles.message}>
              {result.truncated ? `First ${rows.length} of ${result.n_rows} rows` : 'Rows'}
            </div>
            <table style={styles.table}>
              <thead>
                <tr>{rowColumns.map(c => <th key={c} style={styles.th}>{c}</th>)}</tr>
              </thead>
              <tbody>
                {rows.map((row, i) => (
                  <tr key={i}>
                    {rowColumns.map(c => <td key={c} style={styles.td}>{cellText(row[c])}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  dialog: {
    ...modalStyles.dialog,
    width: '80vw',
    maxWidth: 1100,
    maxHeight: '80vh',
    display: 'flex',
    flexDirection: 'column',
  },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
  close: { background: 'none', border: 'none', color: 'inherit', fontSize: 18, cursor: 'pointer' },
  filters: { display: 'flex', flexWrap: 'wrap', gap: 12, margin: '8px 0' },
  filter: { display: 'flex', gap: 4, alignItems: 'center', fontSize: 12 },
  body: { overflow: 'auto', flex: 1 },
  message: { fontSize: 12, margin: '6px 0' },
  error: { fontSize: 12, margin: '6px 0', color: '#f87171' },
  table: { borderCollapse: 'collapse', fontSize: 12, marginBottom: 8 },
  th: { textAlign: 'left', padding: '3px 8px', borderBottom: '1px solid #555', whiteSpace: 'nowrap' },
  td: { padding: '3px 8px', borderBottom: '1px solid #333', whiteSpace: 'nowrap' },
  mixed: { padding: '3px 8px', borderBottom: '1px solid #333', whiteSpace: 'nowrap', color: '#fbbf24' },
}
