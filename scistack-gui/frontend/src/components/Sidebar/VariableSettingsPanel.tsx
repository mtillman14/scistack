/**
 * VariableSettingsPanel — shown in the sidebar when a variable node is selected.
 *
 * Fetches GET /api/variables/{label}/records and displays a quick plot and
 * a variant summary: one row per unique branch_params combination + record
 * count. Per-record schema locations are deliberately not listed.
 */

import { useEffect, useState } from 'react'
import { callBackend, isVSCodeMode } from '../../api'
import VariablePlot from './VariablePlot'
import PlotStudio from '../PlotStudio/PlotStudio'

interface VariantSummary {
  label: string
  branch_params: Record<string, unknown>
  record_count: number
  /* Set only when this variable's records came from more than one version of
     its function's source — see docs/claude/function-version-variants.md. */
  fn_name: string | null
  fn_hash: string | null
  fn_version: string | null
  is_latest: boolean | null
}

interface VariableRecordsResponse {
  variants: VariantSummary[]
}

interface Props {
  label: string
}

export default function VariableSettingsPanel({ label }: Props) {
  const [data, setData] = useState<VariableRecordsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [studioOpen, setStudioOpen] = useState(false)

  // The backend sets fn_version only when the type genuinely holds more than
  // one source version, so this doubles as "is the distinction worth showing".
  const hasVersions = (data?.variants ?? []).some(v => v.fn_version)

  useEffect(() => {
    setData(null)
    setError(null)
    callBackend('get_variable_records', { name: label })
      .then(d => setData(d as VariableRecordsResponse))
      .catch(err => setError(String(err)))
  }, [label])

  return (
    <div style={styles.root}>
      <div style={styles.varName}>{label}</div>

      {error && <div style={styles.error}>{error}</div>}
      {!data && !error && <div style={styles.loading}>Loading…</div>}

      {data && (
        <>
          <section style={styles.section}>
            <div style={styles.sectionTitle}>Plot</div>
            <VariablePlot label={label} />
            {/* The quick plot above answers "what does this look like?".
                The studio is for building a figure worth keeping. */}
            <button
              type="button"
              style={styles.openStudio}
              onClick={() => {
                // Own tab in VS Code (the canvas stays visible), modal in a browser.
                if (isVSCodeMode) {
                  callBackend('open_plot_panel', { variable: label })
                    .catch(() => setStudioOpen(true))
                } else {
                  setStudioOpen(true)
                }
              }}
            >
              Open Plot Studio →
            </button>
          </section>

          {/* Variant summary */}
          <section style={styles.section}>
            <div style={styles.sectionTitle}>Variants</div>
            {data.variants.length === 0 ? (
              <div style={styles.empty}>No records.</div>
            ) : (
              <>
                {hasVersions && (
                  /* Two records can be identical in every visible way and still
                     have come from different code. Say so outright — this is the
                     case that silently plotted the wrong data. */
                  <div style={styles.versionNote}>
                    This variable holds records produced by more than one version
                    of its function's source.
                  </div>
                )}
                <table style={styles.table}>
                  <thead>
                    <tr>
                      <th style={styles.th}>Variant</th>
                      {hasVersions && <th style={styles.th}>Code</th>}
                      <th style={{ ...styles.th, textAlign: 'right' }}>Records</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.variants.map((v, i) => (
                      <tr key={i} style={styles.row}>
                        <td style={styles.td}>
                          <span style={styles.pill}>{v.label}</span>
                        </td>
                        {hasVersions && (
                          <td style={styles.td} title={v.fn_hash ?? undefined}>
                            {v.fn_version ? (
                              <span
                                style={{
                                  ...styles.pill,
                                  ...(v.is_latest ? styles.latestPill : styles.stalePill),
                                }}
                              >
                                {v.fn_version}
                                {v.is_latest ? ' · latest' : ''}
                              </span>
                            ) : (
                              <span style={{ color: '#666' }}>—</span>
                            )}
                          </td>
                        )}
                        <td style={{ ...styles.td, textAlign: 'right', color: '#888' }}>
                          {v.record_count}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            )}
          </section>

        </>
      )}

      {studioOpen && (
        <PlotStudio variable={label} onClose={() => setStudioOpen(false)} />
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  openStudio: {
    marginTop: 6,
    width: '100%',
    padding: '4px 8px',
    background: '#22223a',
    color: '#b2ded9',
    border: '1px solid #3a3a5a',
    borderRadius: 4,
    cursor: 'pointer',
    fontSize: 11,
  },
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  varName: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#4a90d9',
    marginBottom: 12,
    wordBreak: 'break-all',
  },
  loading: {
    color: '#555',
    fontStyle: 'italic',
    fontSize: 11,
  },
  error: {
    color: '#e07070',
    fontSize: 11,
    fontStyle: 'italic',
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
  row: {
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
  // Green/amber rather than green/red: an older code version is not an error,
  // it is history that happens not to be current.
  latestPill: {
    background: '#14331f',
    color: '#7fd39b',
  },
  stalePill: {
    background: '#33290f',
    color: '#d9b45f',
  },
  versionNote: {
    marginBottom: 6,
    padding: '5px 7px',
    borderLeft: '2px solid #d9b45f',
    background: '#221c0c',
    color: '#d9c48f',
    fontSize: 11,
    lineHeight: 1.4,
  },
}
