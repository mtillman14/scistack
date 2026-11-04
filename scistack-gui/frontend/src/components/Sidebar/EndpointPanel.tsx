/**
 * EndpointPanel — artifact/stat preview for a selected endpoint node
 * (plot_/stat_ functions), rendered in the Node tab above the function
 * settings.
 *
 * Two sections:
 *  - Draft results: outputs of the last 👁 Show run for this function —
 *    drafts write NO records, so the show_rendered push message is the
 *    only handle on them.
 *  - Recorded artifacts: endpoint records via GET /api/endpoints/{fn}/
 *    artifacts (figures with stamp-derived provenance captions, stats as
 *    tables).
 *
 * VS Code webview mode has no HTTP origin to serve image bytes from, so
 * it lists paths + provenance text only (v1 — asWebviewUri plumbing is
 * deferred).
 */

import { useEffect, useState, useCallback } from 'react'
import { callBackend, isVSCodeMode } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'

interface FigureEntry {
  record_id: string
  fn: string
  variable: string
  schema: Record<string, string>
  branch_params: Record<string, unknown>
  artifact_path: string
  artifact_exists: boolean
  stamp_ok: boolean | null
  timestamp: string | null
}

interface StatEntry {
  record_id: string
  fn: string
  variable: string
  schema: Record<string, string>
  branch_params: Record<string, unknown>
  result: unknown
  result_parsed: boolean
  timestamp: string | null
}

interface ArtifactsResponse {
  figures: FigureEntry[]
  stats: StatEntry[]
  warnings: string[]
}

interface Props {
  fnName: string
  kind: 'plot' | 'stat'
}

const IMAGE_EXTENSIONS = ['.png', '.svg', '.jpg', '.jpeg', '.gif', '.webp']

function isImagePath(path: string): boolean {
  const lower = path.toLowerCase()
  return IMAGE_EXTENSIONS.some(ext => lower.endsWith(ext))
}

function artifactUrl(path: string): string {
  return `/api/artifacts/file?path=${encodeURIComponent(path)}`
}

function provenanceCaption(schema: Record<string, string>,
                           branchParams: Record<string, unknown>): string {
  const parts = [
    ...Object.entries(schema).map(([k, v]) => `${k}=${v}`),
    // Branch params carry fn-qualified keys (fn.constant); show the short name.
    ...Object.entries(branchParams).map(
      ([k, v]) => `${k.split('.').pop()}=${v}`),
  ]
  return parts.join(', ')
}

export default function EndpointPanel({ fnName, kind }: Props) {
  const [artifacts, setArtifacts] = useState<ArtifactsResponse | null>(null)
  const [error, setError] = useState('')
  const [draft, setDraft] = useState<unknown[] | null>(null)

  const fetchArtifacts = useCallback(() => {
    callBackend('get_endpoint_artifacts', { fn_name: fnName })
      .then(d => { setArtifacts(d as ArtifactsResponse); setError('') })
      .catch(err => setError((err as Error).message))
  }, [fnName])

  useEffect(() => {
    setDraft(null)
    fetchArtifacts()
  }, [fetchArtifacts])

  // Draft outputs from 👁 Show land here; recorded runs refresh the
  // recorded section via the same dag_updated signal the canvas uses.
  useBackendMessage(useCallback((msg) => {
    const msgType = (msg.type ?? msg.method) as string
    const params = (msg.params ?? msg) as Record<string, unknown>
    if (msgType === 'show_rendered' && (params.step ?? msg.step) === fnName) {
      setDraft(((msg.rendered ?? params.rendered) as unknown[]) ?? [])
    } else if (msgType === 'dag_updated') {
      fetchArtifacts()
    }
  }, [fnName, fetchArtifacts]))

  return (
    <div style={styles.root}>
      <div style={styles.heading}>
        {kind === 'plot' ? '◫' : 'Σ'} {fnName}
      </div>

      {draft !== null && (
        <Section title="Draft (not recorded)">
          {draft.length === 0 && <div style={styles.empty}>No outputs rendered.</div>}
          {draft.map((item, i) => (
            <DraftItem key={i} item={item} />
          ))}
        </Section>
      )}

      {error && <div style={styles.error}>{error}</div>}
      {artifacts && (
        <>
          {artifacts.warnings.length > 0 && (
            <div style={styles.warnings}>
              {artifacts.warnings.map((w, i) => (
                <div key={i}>⚠ {w}</div>
              ))}
            </div>
          )}
          <Section title={`Recorded (${artifacts.figures.length + artifacts.stats.length})`}>
            {artifacts.figures.length === 0 && artifacts.stats.length === 0 && (
              <div style={styles.empty}>
                Nothing recorded yet. Run this step, or use 👁 Show for a draft preview.
              </div>
            )}
            {artifacts.figures.map(fig => (
              <FigureCard key={fig.record_id} fig={fig} />
            ))}
            {artifacts.stats.map(stat => (
              <StatCard key={stat.record_id} stat={stat} />
            ))}
          </Section>
        </>
      )}
    </div>
  )
}

function DraftItem({ item }: { item: unknown }) {
  if (typeof item === 'string' && isImagePath(item)) {
    return isVSCodeMode
      ? <div style={styles.pathLine}>{item}</div>
      : (
        <figure style={styles.figure}>
          <img src={artifactUrl(item)} style={styles.image} alt={item} />
          <figcaption style={styles.caption}>{item}</figcaption>
        </figure>
      )
  }
  if (item !== null && typeof item === 'object') {
    return <KeyValueTable data={item as Record<string, unknown>} />
  }
  return <div style={styles.pathLine}>{String(item)}</div>
}

function FigureCard({ fig }: { fig: FigureEntry }) {
  const caption = provenanceCaption(fig.schema, fig.branch_params)
  return (
    <div style={styles.card}>
      {!fig.artifact_exists ? (
        <div style={styles.missing}>missing file: {fig.artifact_path}</div>
      ) : isVSCodeMode ? (
        <div style={styles.pathLine}>{fig.artifact_path}</div>
      ) : (
        <img src={artifactUrl(fig.artifact_path)} style={styles.image}
             alt={caption} />
      )}
      <div style={styles.caption}>
        {caption}
        {fig.stamp_ok === false && (
          <span style={styles.stale}> · STALE stamp</span>
        )}
        {fig.stamp_ok === null && (
          <span style={styles.noStamp}> · no stamp</span>
        )}
      </div>
    </div>
  )
}

function StatCard({ stat }: { stat: StatEntry }) {
  const caption = provenanceCaption(stat.schema, stat.branch_params)
  return (
    <div style={styles.card}>
      {stat.result_parsed && stat.result !== null
        && typeof stat.result === 'object'
        ? <KeyValueTable data={stat.result as Record<string, unknown>} />
        : <div style={styles.pathLine}>{String(stat.result)}</div>}
      <div style={styles.caption}>{caption}</div>
    </div>
  )
}

function KeyValueTable({ data }: { data: Record<string, unknown> }) {
  return (
    <table style={styles.table}>
      <tbody>
        {Object.entries(data).map(([k, v]) => (
          <tr key={k}>
            <td style={styles.tdKey}>{k}</td>
            <td style={styles.tdVal}>
              {typeof v === 'number' ? +v.toFixed(6) : JSON.stringify(v)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>{title}</div>
      {children}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '8px 12px',
    borderBottom: '1px solid #2a2a4a',
  },
  heading: {
    fontFamily: 'monospace',
    fontSize: 13,
    fontWeight: 700,
    color: '#67e8f9',
    marginBottom: 6,
  },
  section: {
    marginBottom: 10,
  },
  sectionTitle: {
    fontSize: 11,
    fontWeight: 700,
    color: '#888',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginBottom: 4,
  },
  card: {
    background: '#1a1a2e',
    border: '1px solid #2a2a4a',
    borderRadius: 4,
    padding: 6,
    marginBottom: 6,
  },
  figure: {
    margin: 0,
    marginBottom: 6,
  },
  image: {
    maxWidth: '100%',
    borderRadius: 3,
    background: '#fff',
    display: 'block',
  },
  caption: {
    fontSize: 10,
    fontFamily: 'monospace',
    color: '#7a9ec2',
    marginTop: 4,
    wordBreak: 'break-all',
  },
  stale: {
    color: '#f87171',
    fontWeight: 700,
  },
  noStamp: {
    color: '#a16207',
  },
  missing: {
    fontSize: 11,
    color: '#f87171',
    fontFamily: 'monospace',
    wordBreak: 'break-all',
  },
  pathLine: {
    fontSize: 11,
    fontFamily: 'monospace',
    color: '#ccc',
    wordBreak: 'break-all',
    padding: '2px 0',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 11,
    fontFamily: 'monospace',
  },
  tdKey: {
    color: '#888',
    padding: '2px 8px 2px 0',
    verticalAlign: 'top',
    whiteSpace: 'nowrap',
  },
  tdVal: {
    color: '#ccc',
    padding: '2px 0',
    wordBreak: 'break-all',
  },
  warnings: {
    fontSize: 11,
    color: '#fbbf24',
    marginBottom: 6,
  },
  empty: {
    fontSize: 11,
    color: '#555',
    fontStyle: 'italic',
  },
  error: {
    fontSize: 11,
    color: '#f87171',
    marginBottom: 6,
  },
}
