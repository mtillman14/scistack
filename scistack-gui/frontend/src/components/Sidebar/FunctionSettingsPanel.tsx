/**
 * FunctionSettingsPanel — shown in the sidebar when a function node is selected.
 *
 * Displays the Cartesian product of all constant node values on the canvas as
 * a read-only variant list. Refreshes automatically when constant node values change.
 */

interface VariantRow {
  [constantName: string]: string
}

interface Props {
  label: string
  variants: VariantRow[]
  constantNames: string[]
}

export default function FunctionSettingsPanel({ label, variants, constantNames }: Props) {
  return (
    <div style={styles.root}>
      <div style={styles.fnName}>{label}</div>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Variants</div>

        {variants.length === 0 && (
          <div style={styles.empty}>
            {constantNames.length === 0
              ? 'No constant nodes on canvas.'
              : 'No values defined on constant nodes.'}
          </div>
        )}

        {variants.length > 0 && (
          <table style={styles.table}>
            <thead>
              <tr>
                {constantNames.map(name => (
                  <th key={name} style={styles.th}>{name}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {variants.map((row, i) => (
                <tr key={i} style={styles.variantRow}>
                  {constantNames.map(name => (
                    <td key={name} style={styles.td}>
                      <span style={styles.pill}>{row[name] ?? '—'}</span>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  fnName: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#a89cf0',
    marginBottom: 12,
    wordBreak: 'break-all',
  },
  section: {
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 10,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginBottom: 6,
  },
  empty: {
    color: '#555',
    fontStyle: 'italic',
    fontSize: 11,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  th: {
    textAlign: 'left',
    fontSize: 10,
    color: '#888',
    fontWeight: 600,
    padding: '2px 4px 4px 0',
    borderBottom: '1px solid #2a2a4a',
    fontFamily: 'monospace',
  },
  variantRow: {
    borderBottom: '1px solid #1e1e3a',
  },
  td: {
    padding: '4px 4px 4px 0',
    verticalAlign: 'middle',
  },
  pill: {
    display: 'inline-block',
    background: '#1e1e3a',
    borderRadius: 3,
    padding: '1px 5px',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
}
