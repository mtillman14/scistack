/**
 * NotPlottedPopup — what the figure leaves out, opened from the toolbar's
 * "Not plotted" button (user, 2026-11-01; `.claude/plan-plot-omissions.md`).
 *
 * One row per left-out (or partly averaged) value: its location, the
 * variable, the reason and its detail. Everything shown is Python's
 * (`scistackplot.omissions`); this component only lays it out
 * (`notPlotted.ts`). "Save as CSV" writes every row — the popup shows at
 * most 5,000 per figure — through the same save job as "Save data (CSV)"
 * (`export.NOT_PLOTTED_DEPTH`). The footnote checkbox is
 * `StyleOptions.not_plotted_note`.
 */

import {
  notPlottedColumns,
  notPlottedRows,
  notPlottedTruncated,
  type NotPlottedTotal,
  type OmissionReport,
} from './notPlotted'

export default function NotPlottedPopup({
  total,
  figures,
  noteOn,
  saving,
  onToggleNote,
  onSave,
  onClose,
}: {
  total: NotPlottedTotal
  figures: { label: string; omissions?: OmissionReport | null }[]
  noteOn: boolean
  saving: boolean
  onToggleNote: (on: boolean) => void
  onSave: () => void
  onClose: () => void
}) {
  const rows = notPlottedRows(figures)
  const columns = notPlottedColumns(rows)
  const truncated = notPlottedTruncated(figures)
  const tagged = rows.some(row => row.figure)
  return (
    <div style={styles.overlay} onClick={onClose}>
      <div style={styles.dialog} onClick={e => e.stopPropagation()} role="dialog" aria-label="Not plotted">
        <div style={styles.header}>
          <span style={styles.title}>{total.button || 'Not plotted'}</span>
          <button type="button" style={styles.close} onClick={onClose} aria-label="Close">
            ✕
          </button>
        </div>
        <div style={styles.sentence}>{total.sentence}</div>
        <div style={styles.hint}>
          Rows removed on purpose (location filter, Filters, exclusions) are not listed.
        </div>
        <div style={styles.tableWrap}>
          <table style={styles.table}>
            <thead>
              <tr>
                {tagged && <th style={styles.th}>Figure</th>}
                {columns.map(name => (
                  <th key={name} style={styles.th}>{name}</th>
                ))}
                <th style={styles.th}>Variable</th>
                <th style={styles.th}>Reason</th>
                <th style={styles.th}>Detail</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={index}>
                  {tagged && <td style={styles.td}>{row.figure}</td>}
                  {columns.map(name => (
                    <td key={name} style={styles.td}>{row.keys[name] ?? ''}</td>
                  ))}
                  <td style={styles.td}>{row.variable}</td>
                  <td style={styles.td}>{row.label}</td>
                  <td style={styles.td}>{row.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {truncated > 0 && (
          <div style={styles.hint}>{truncated.toLocaleString()} more not shown here; Save as CSV writes every row.</div>
        )}
        <div style={styles.footer}>
          <label style={styles.check} title="Write the sentence above under the figure, in the preview and the saved image.">
            <input type="checkbox" checked={noteOn} onChange={e => onToggleNote(e.target.checked)} />
            <span>Show as a footnote on the figure</span>
          </label>
          <button type="button" style={styles.button} onClick={onSave} disabled={saving}>
            Save as CSV
          </button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: 'var(--ps-scrim)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1100,
  },
  dialog: {
    width: 720, maxWidth: '92vw', maxHeight: '80vh', background: 'var(--ps-bg)',
    color: 'var(--ps-text-body)', border: '1px solid var(--ps-border-strong)', borderRadius: 8,
    padding: 16, display: 'flex', flexDirection: 'column', gap: 8, fontSize: 12,
  },
  header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
  title: { fontSize: 13, fontWeight: 600 },
  close: {
    background: 'transparent', border: 'none', color: 'var(--ps-text-secondary)',
    fontSize: 16, cursor: 'pointer', lineHeight: 1,
  },
  sentence: { color: 'var(--ps-text-body)' },
  hint: { color: 'var(--ps-text-secondary)', fontSize: 11 },
  tableWrap: { overflow: 'auto', border: '1px solid var(--ps-border)', borderRadius: 4, minHeight: 80 },
  table: { borderCollapse: 'collapse', width: '100%' },
  th: {
    position: 'sticky', top: 0, background: 'var(--ps-surface)', textAlign: 'left',
    padding: '3px 6px', borderBottom: '1px solid var(--ps-border)', fontWeight: 600,
  },
  td: { padding: '2px 6px', borderBottom: '1px solid var(--ps-border)', whiteSpace: 'nowrap' },
  footer: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 8 },
  check: { display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' },
  button: {
    padding: '4px 12px', background: 'var(--ps-control)', color: 'var(--ps-text-control)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer',
  },
}
