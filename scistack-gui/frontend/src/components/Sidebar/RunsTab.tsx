/**
 * RunsTab — displays per-run structured cards, most recent first.
 *
 * Each card shows: function name, input/output variables, constants,
 * live progress with current schema combo, and a collapsible raw log.
 */

import { useState, useEffect, useCallback } from 'react'
import { useRunLog } from '../../context/RunLogContext'
import { callBackend, isVSCodeMode } from '../../api'
import type { RunEntry } from '../../context/RunLogContext'

export default function RunsTab() {
  const { runs } = useRunLog()
  // Force re-render every 30s so relative timestamps update.
  const [, setTick] = useState(0)
  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 30_000)
    return () => clearInterval(id)
  }, [])

  if (runs.length === 0) {
    return <div style={styles.empty}>No runs yet.</div>
  }

  return (
    <div>
      {runs.map(run => <RunCard key={run.run_id} run={run} />)}
    </div>
  )
}

/* ── Relative timestamp helper ──────────────────────────────── */

function relativeTime(epochMs: number): string {
  const diff = Date.now() - epochMs
  if (diff < 0) return 'just now'
  const secs = Math.floor(diff / 1000)
  if (secs < 60) return 'just now'
  const mins = Math.floor(secs / 60)
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  return `${days}d ago`
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  const secs = ms / 1000
  if (secs < 60) return `${secs.toFixed(1)}s`
  const mins = Math.floor(secs / 60)
  const remSecs = Math.round(secs % 60)
  return `${mins}m ${remSecs}s`
}

/* ── Run card component ─────────────────────────────────────── */

function RunCard({ run }: { run: RunEntry }) {
  const [logOpen, setLogOpen] = useState(false)

  const handleOpenSource = useCallback(async () => {
    try {
      const src = await callBackend('get_function_source', { name: run.function_name }) as {
        ok: boolean; file?: string; line?: number; error?: string
      }
      if (!src.ok) return
      if (isVSCodeMode) {
        await callBackend('reveal_in_editor', { file: src.file, line: src.line })
      } else {
        window.alert(`${run.function_name} is defined at:\n${src.file}:${src.line}`)
      }
    } catch { /* ignore */ }
  }, [run.function_name])

  const statusIcon =
    run.status === 'running' ? '⏳' :
    run.status === 'error' ? '✗' : '✓'
  const statusColor =
    run.status === 'running' ? '#f0c040' :
    run.status === 'error' ? '#e06060' : '#6be16b'

  // Build per-repetition display from variants array.
  // Fall back to flat display if no variants tracked yet (e.g. legacy/in-flight).
  const variants = run.variants.length > 0
    ? run.variants
    : [{
        constants: run.constants,
        input_types: run.input_types,
        output_type: run.output_types[0] ?? '',
      }]
  const showRepLabel = variants.length > 1

  // Progress info
  const total = run.records_total ?? 0
  const done = run.records_done
  const pct = total > 0 ? Math.round((done / total) * 100) : 0

  return (
    <div style={styles.card}>
      {/* Header: status + function name + time */}
      <div style={styles.header}>
        <span style={{ color: statusColor, marginRight: 6, flexShrink: 0 }}>{statusIcon}</span>
        <span
          style={styles.fnName}
          onDoubleClick={handleOpenSource}
          title="Double-click to open source"
        >
          {run.function_name}
        </span>
        <span style={styles.time}>{relativeTime(run.started_at)}</span>
      </div>

      {/* Repetitions */}
      {showRepLabel && (
        <div style={styles.repLabel}>{variants.length} repetitions</div>
      )}
      {variants.map((v, i) => {
        const inputNames = Object.values(v.input_types)
        const constEntries = Object.entries(v.constants)
        const inputParts = [
          ...inputNames,
          ...constEntries.map(([k, val]) => `${k}=${val}`),
        ]
        return (
          <div key={i} style={styles.repBlock}>
            {showRepLabel && (
              <div style={styles.repHeader}>#{i + 1}</div>
            )}
            {inputParts.length > 0 && (
              <div style={styles.repInputs}>
                {inputParts.join(', ')}
                {v.output_type ? ` → ${v.output_type}` : ''}
              </div>
            )}
            {inputParts.length === 0 && v.output_type && (
              <div style={styles.repInputs}>→ {v.output_type}</div>
            )}
          </div>
        )
      })}

      {/* Progress */}
      {run.status === 'running' && total > 0 && (
        <div style={styles.progressSection}>
          <div style={styles.progressBarOuter}>
            <div style={{ ...styles.progressBarInner, width: `${pct}%` }} />
          </div>
          <div style={styles.progressLabel}>{done}/{total} records</div>
          {run.current_combo && (
            <div style={styles.currentCombo}>
              ▸ {Object.entries(run.current_combo).map(([k, v]) => `${k}=${v}`).join(', ')}
            </div>
          )}
        </div>
      )}

      {/* Completed summary */}
      {run.status !== 'running' && (
        <div style={styles.summaryRow}>
          {total > 0 && <span>{done}/{total} records</span>}
          {run.duration_ms != null && (
            <span style={styles.duration}>
              {total > 0 ? ' · ' : ''}{formatDuration(run.duration_ms)}
            </span>
          )}
        </div>
      )}

      {/* Error summary */}
      {run.status === 'error' && run.error_summary && (
        <div style={styles.errorRow}>{run.error_summary}</div>
      )}

      {/* Log toggle */}
      {run.lines.length > 0 && (
        <>
          <button style={styles.logToggle} onClick={() => setLogOpen(o => !o)}>
            {logOpen ? 'Log ▾' : 'Log ▸'}
          </button>
          {logOpen && (
            <div style={styles.logBox}>
              {run.lines.map((line, i) => <span key={i}>{line}</span>)}
            </div>
          )}
        </>
      )}
    </div>
  )
}

/* ── Styles ─────────────────────────────────────────────────── */

const styles: Record<string, React.CSSProperties> = {
  empty: {
    color: '#555',
    fontSize: 13,
    padding: '16px',
    textAlign: 'center',
  },
  card: {
    borderBottom: '1px solid #1e1e3a',
    padding: '8px 12px',
  },
  header: {
    display: 'flex',
    alignItems: 'center',
    gap: 4,
  },
  fnName: {
    flex: 1,
    fontFamily: 'monospace',
    fontSize: 12,
    color: '#ccc',
    fontWeight: 600,
    cursor: 'pointer',
  },
  time: {
    fontFamily: 'monospace',
    fontSize: 10,
    color: '#555',
    flexShrink: 0,
  },
  repLabel: {
    fontFamily: 'monospace',
    fontSize: 10,
    color: '#888',
    marginTop: 4,
    paddingLeft: 20,
  },
  repBlock: {
    marginTop: 3,
    paddingLeft: 20,
  },
  repHeader: {
    fontFamily: 'monospace',
    fontSize: 10,
    color: '#666',
  },
  repInputs: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#7a9ec2',
    marginTop: 1,
    paddingLeft: 0,
  },
  progressSection: {
    marginTop: 5,
    paddingLeft: 20,
  },
  progressBarOuter: {
    height: 4,
    background: '#1a1a2e',
    borderRadius: 2,
    overflow: 'hidden',
  },
  progressBarInner: {
    height: '100%',
    background: '#f0c040',
    borderRadius: 2,
    transition: 'width 0.3s ease',
  },
  progressLabel: {
    fontFamily: 'monospace',
    fontSize: 10,
    color: '#888',
    marginTop: 2,
  },
  currentCombo: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#c0d8a0',
    marginTop: 2,
  },
  summaryRow: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#888',
    marginTop: 4,
    paddingLeft: 20,
  },
  duration: {
    color: '#666',
  },
  errorRow: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#e06060',
    marginTop: 3,
    paddingLeft: 20,
  },
  logToggle: {
    background: 'transparent',
    border: 'none',
    color: '#555',
    fontFamily: 'monospace',
    fontSize: 10,
    cursor: 'pointer',
    padding: '4px 0 0 20px',
  },
  logBox: {
    display: 'flex',
    flexDirection: 'column',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b0c4b0',
    background: '#0d0d1f',
    padding: '6px 12px',
    marginTop: 4,
    whiteSpace: 'pre-wrap',
    overflowX: 'auto',
    maxHeight: 200,
    overflowY: 'auto',
  },
}
