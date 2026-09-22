/**
 * PipelineRunController — the plan-preview gate for pipeline runs (R2/G2).
 *
 * Renders the plan-preview dialog whenever a run control posts a
 * PlanRequest (function-node "Run until here", pipeline-node "Run",
 * canvas "Run endpoints"), and routes the run's WebSocket/JSON-RPC
 * messages (run_output / run_done) into the existing run console
 * (RunLogContext → Runs tab).
 *
 * Cooperative cancel is a no-op for pipeline runs (v1: Pipeline._run has
 * no between-step hook) — only force-cancel works, exposed on the Runs-tab
 * card (see RunsTab), so no soft-cancel button appears here.
 */

import { useEffect, useState, useCallback, useRef } from 'react'
import { callBackend } from '../api'
import { useBackendMessage } from '../hooks/useBackendMessage'
import { usePlanRun, type PlanRequest } from '../context/PlanRunContext'
import { useRunLog } from '../context/RunLogContext'

interface PlanEntry {
  step: string
  pipeline: string
  endpoint: boolean
  state: 'green' | 'red' | 'unknown'
  n_combos: number
}

export default function PipelineRunController() {
  const { planRequest, clearPlan } = usePlanRun()
  const { startRun, appendLine, finishRun } = useRunLog()
  // Pipeline run_ids this controller is responsible for. A ref (not state)
  // so the message handler sees additions immediately — a run can finish
  // before the first re-render completes.
  const activeRuns = useRef<Set<string>>(new Set())

  useBackendMessage(useCallback((msg) => {
    // Support both WebSocket format (msg.type) and JSON-RPC notification format (msg.method)
    const msgType = (msg.type ?? msg.method) as string
    const params = (msg.params ?? msg) as Record<string, unknown>
    const runId = (msg.run_id ?? params?.run_id) as string | undefined
    if (!runId || !activeRuns.current.has(runId)) return
    if (msgType === 'run_output') {
      appendLine(runId, (msg.text ?? params.text) as string)
    } else if (msgType === 'run_done') {
      const success = (params.success ?? true) as boolean
      const durationMs = params.duration_ms as number | undefined
      const cancelled = (params.cancelled ?? false) as boolean
      const error = params.error as string | undefined
      const unknown = (params.unknown ?? false) as boolean
      finishRun(runId, success, durationMs, cancelled, error, unknown)
      activeRuns.current.delete(runId)
    }
  }, [appendLine, finishRun]))

  const handleRun = useCallback(async (req: PlanRequest) => {
    // Generate run_id on the frontend BEFORE the request so the message
    // handler is already filtering on the correct ID when output arrives.
    const runId = Math.random().toString(36).slice(2, 10)
    activeRuns.current.add(runId)
    startRun(runId, req.label, 'pipeline')
    clearPlan()
    try {
      await callBackend('start_pipeline_run', {
        pipeline_id: req.pipeline_id,
        mode: req.mode,
        target: req.target ?? '',
        finalized: req.finalized ?? null,
        skip_computed: true,
        run_id: runId,
      })
    } catch (err) {
      activeRuns.current.delete(runId)
      appendLine(runId, `Error: ${(err as Error).message}\n`)
      finishRun(runId, false)
    }
  }, [startRun, appendLine, finishRun, clearPlan])

  if (!planRequest) return null
  return (
    <PlanPreviewDialog
      request={planRequest}
      onRun={handleRun}
      onCancel={clearPlan}
    />
  )
}

/* ── Plan-preview dialog ─────────────────────────────────────── */

const STATE_CHIP: Record<string, { color: string; background: string }> = {
  green:   { color: '#166534', background: '#bbf7d0' },
  red:     { color: '#991b1b', background: '#fecaca' },
  unknown: { color: '#374151', background: '#e5e7eb' },
}

function modeDescription(req: PlanRequest): string {
  if (req.mode === 'until') return `Run every step up to and including '${req.target}'`
  if (req.mode === 'endpoints') {
    return `Run all endpoints (${req.finalized ? 'finalized' : 'draft'})`
  }
  return 'Run all steps'
}

function PlanPreviewDialog({
  request,
  onRun,
  onCancel,
}: {
  request: PlanRequest
  onRun: (req: PlanRequest) => void
  onCancel: () => void
}) {
  const [entries, setEntries] = useState<PlanEntry[] | null>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    setEntries(null)
    setError('')
    callBackend('get_pipeline_plan', {
      pipeline_id: request.pipeline_id,
      target: request.mode === 'until' ? (request.target ?? '') : '',
    })
      .then(d => setEntries(d as PlanEntry[]))
      .catch(err => setError((err as Error).message))
  }, [request])

  return (
    <div style={styles.overlay} role="dialog" aria-modal="true">
      <div style={styles.dialog}>
        <div style={styles.title}>Run {request.label}</div>
        <div style={styles.subtitle}>{modeDescription(request)}</div>

        {error && <div style={styles.error}>{error}</div>}
        {!error && entries === null && (
          <div style={styles.loading}>Planning…</div>
        )}
        {!error && entries !== null && entries.length === 0 && (
          <div style={styles.loading}>Nothing to run — the plan is empty.</div>
        )}
        {!error && entries !== null && entries.length > 0 && (
          <div style={styles.tableWrap}>
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.th}>step</th>
                  <th style={styles.th}>pipeline</th>
                  <th style={{ ...styles.th, textAlign: 'right' }}>combos</th>
                  <th style={styles.th}>state</th>
                </tr>
              </thead>
              <tbody>
                {entries.map((e, i) => (
                  <tr key={`${e.pipeline}-${e.step}-${i}`}
                      style={e.endpoint ? styles.endpointRow : undefined}>
                    <td style={styles.tdStep}>
                      {e.endpoint && <span style={styles.endpointTag}>endpoint</span>}
                      {e.step}
                    </td>
                    <td style={styles.tdPipeline}>{e.pipeline}</td>
                    <td style={{ ...styles.td, textAlign: 'right' }}>{e.n_combos}</td>
                    <td style={styles.td}>
                      <span style={{ ...styles.chip, ...STATE_CHIP[e.state] ?? STATE_CHIP.unknown }}>
                        {e.state}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <div style={styles.footer}>
          <button style={styles.cancelBtn} onClick={onCancel} type="button">
            Cancel
          </button>
          <button
            style={error || entries === null || entries.length === 0 ? styles.runBtnDisabled : styles.runBtn}
            onClick={() => onRun(request)}
            disabled={!!error || entries === null || entries.length === 0}
            type="button"
          >
            ▶ Run
          </button>
        </div>
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed',
    inset: 0,
    background: 'rgba(0, 0, 0, 0.6)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 9000,
  },
  dialog: {
    background: '#1a1a2e',
    color: '#eee',
    border: '1px solid #3a3a5a',
    borderRadius: 6,
    padding: '18px 22px',
    minWidth: 460,
    maxWidth: 720,
    maxHeight: '80vh',
    display: 'flex',
    flexDirection: 'column',
    boxShadow: '0 10px 40px rgba(0, 0, 0, 0.6)',
  },
  title: {
    fontSize: 15,
    fontWeight: 700,
    marginBottom: 2,
    fontFamily: 'monospace',
  },
  subtitle: {
    fontSize: 12,
    opacity: 0.7,
    marginBottom: 12,
  },
  loading: {
    fontSize: 12,
    opacity: 0.7,
    padding: '16px 0',
  },
  error: {
    fontSize: 12,
    color: '#f87171',
    background: '#0f0f1e',
    border: '1px solid #7f1d1d',
    borderRadius: 4,
    padding: '8px 10px',
    whiteSpace: 'pre-wrap',
    marginBottom: 8,
  },
  tableWrap: {
    overflowY: 'auto',
    border: '1px solid #2a2a4a',
    borderRadius: 4,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 12,
  },
  th: {
    textAlign: 'left',
    padding: '5px 10px',
    fontSize: 10,
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: 0.6,
    color: '#888',
    borderBottom: '1px solid #2a2a4a',
    background: '#12122a',
    position: 'sticky',
    top: 0,
  },
  td: {
    padding: '5px 10px',
    borderBottom: '1px solid #22223a',
  },
  tdStep: {
    padding: '5px 10px',
    borderBottom: '1px solid #22223a',
    fontFamily: 'monospace',
  },
  tdPipeline: {
    padding: '5px 10px',
    borderBottom: '1px solid #22223a',
    fontFamily: 'monospace',
    opacity: 0.7,
  },
  endpointRow: {
    background: 'rgba(162, 28, 175, 0.12)',
  },
  endpointTag: {
    display: 'inline-block',
    marginRight: 6,
    padding: '0 5px',
    background: '#a21caf',
    color: '#fff',
    borderRadius: 8,
    fontSize: 9,
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    verticalAlign: 'middle',
  },
  chip: {
    display: 'inline-block',
    padding: '1px 8px',
    borderRadius: 10,
    fontSize: 10,
    fontWeight: 700,
  },
  footer: {
    display: 'flex',
    justifyContent: 'flex-end',
    gap: 8,
    marginTop: 14,
  },
  cancelBtn: {
    padding: '5px 14px',
    background: '#2a2a4a',
    color: '#ccc',
    border: '1px solid #3a3a5a',
    borderRadius: 4,
    cursor: 'pointer',
    fontSize: 12,
  },
  runBtn: {
    padding: '5px 14px',
    background: '#7b68ee',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
  },
  runBtnDisabled: {
    padding: '5px 14px',
    background: '#3a3a5a',
    color: '#888',
    border: 'none',
    borderRadius: 4,
    cursor: 'not-allowed',
    fontWeight: 600,
    fontSize: 12,
  },
}
