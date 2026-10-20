/**
 * One backend `Grid` as an APA-style table (StatStudio's Descriptives and
 * Comparisons tabs, and the wizard's results step). The cells are the
 * backend's strings; nothing is formatted here.
 */

import { gridToTsv, gridWidth, type Grid } from './statsSpec'

export default function GridTable({ grid }: { grid: Grid }) {
  const width = gridWidth(grid)
  return (
    <div style={styles.gridBox}>
      <div style={styles.gridTitle}>{grid.title}</div>
      <div style={styles.scroll}>
        <table style={styles.table}>
          <thead>
            {grid.header_rows.map((row, i) => (
              <tr key={i}>
                {row.map((cell, j) => (
                  <th key={j} colSpan={cell.span}
                      style={{ ...styles.th, ...(i === grid.header_rows.length - 1 ? styles.thLast : {}),
                               textAlign: j < grid.label_columns ? 'left' : 'center' }}>
                    {cell.text}
                  </th>
                ))}
              </tr>
            ))}
          </thead>
          <tbody>
            {grid.rows.map((row, i) => row.kind === 'section' ? (
              <tr key={i}><td colSpan={width} style={styles.section}>{row.cells[0]}</td></tr>
            ) : (
              <tr key={i}>
                {row.cells.map((cell, j) => (
                  <td key={j} style={{ ...styles.td, textAlign: j < grid.label_columns ? 'left' : 'center' }}>{cell}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div style={styles.caption}><i>Note.</i> {grid.caption}</div>
      {grid.notes.length > 0 && (
        <ul style={styles.notes}>{grid.notes.map((n, i) => <li key={i}>{n}</li>)}</ul>
      )}
      <button type="button" style={styles.smallButton}
              onClick={() => navigator.clipboard?.writeText(gridToTsv(grid)).catch(() => undefined)}
              title="Copy as tab-separated text (pastes into a spreadsheet)">
        Copy as text
      </button>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  gridBox: { background: 'var(--ps-surface)', border: '1px solid var(--ps-border-faint)', borderRadius: 4, padding: 10, marginBottom: 12 },
  gridTitle: { fontStyle: 'italic', marginBottom: 6, color: 'var(--ps-text-strong)' },
  scroll: { overflowX: 'auto' },
  table: { borderCollapse: 'collapse', borderTop: '1px solid var(--ps-border-strong)', borderBottom: '1px solid var(--ps-border-strong)' },
  th: { fontWeight: 'normal', padding: '3px 10px' },
  thLast: { borderBottom: '1px solid var(--ps-border-strong)' },
  td: { padding: '2px 10px', fontVariantNumeric: 'tabular-nums' },
  section: { fontStyle: 'italic', padding: '4px 10px', color: 'var(--ps-text-secondary)' },
  caption: { fontSize: 12, marginTop: 6, color: 'var(--ps-text-body)', maxWidth: 900 },
  notes: { fontSize: 12, color: 'var(--ps-text-secondary)', margin: '4px 0', paddingLeft: 18 },
  smallButton: { background: 'var(--ps-control)', color: 'var(--ps-text-control)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '2px 8px', cursor: 'pointer', fontSize: 12 },
}
