/**
 * FunctionNode — represents a pipeline function (e.g. compute_rolling_vo2).
 *
 * Features:
 *   - Run button: posts to /api/run with the checked variants from connected
 *     input nodes, then streams output via WebSocket.
 *   - Spinner while running.
 *   - Run output is sent to the sidebar Runs tab via RunLogContext.
 */

import { useState, useCallback, useEffect, useRef } from 'react'
import { Handle, Position, useReactFlow, useUpdateNodeInternals } from '@xyflow/react'
import { callBackend, isVSCodeMode } from '../../api'
import { SourceLocationDialog } from '../SourceLocationDialog'
import type { SourceLocation } from '../SourceLocationDialog'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import { useRunLog } from '../../context/RunLogContext'
import { useScope } from '../../context/ScopeContext'
import { useVariantSelection } from '../../context/VariantSelectionContext'
import type { VariantSelectionValue } from '../../context/VariantSelectionContext'
import type { Variant } from './VariableNode'

interface FnVariantRow {
  constants: Record<string, unknown>
  // Per-call-site state — nodes group call sites by wiring, so each
  // constant-value variant keeps its own chip (state never blurs).
  state?: 'green' | 'pending' | 'red'
  // True for synthesized rows: a staged pending value with no call site yet.
  staged?: boolean
}

interface FunctionNodeData {
  label: string
  input_params?: Record<string, string>  // param_name → type_name
  output_types?: string[]
  constant_params?: string[]
  variants?: FnVariantRow[]
  // Endpoint classification from scidb's _endpoint_kind (plot_/stat_ name
  // prefixes). Endpoint nodes get a kind badge and a Show button.
  endpoint_kind?: 'plot' | 'stat'
  // 'pending' is GUI-only: a staged (unrun) constant value — not in the DB.
  run_state?: 'green' | 'pending' | 'red'
  // True when a required input edge was deleted (hidden, not just stale) —
  // always red, and won't actually run until reconnected (backend refuses
  // with an explicit error rather than silently no-op'ing — see
  // execution_service.disconnected_reason).
  disconnected?: boolean
  // The picker's pair (include prefixes + exclude_levels rule); null = all.
  schemaSelection?: { include: [string, string][][]; exclude_levels: Record<string, string[]> } | null
  schemaLevel?: string[] | null
  runOptions?: { dry_run: boolean; save: boolean; distribute: boolean; as_table: boolean }
  // Set to 'matlab' for functions backed by a .m file. The extension uses this
  // to intercept start_run and route to handleMatlabRun instead of calling
  // into the Python registry (which doesn't know about MATLAB functions).
  language?: string
}

const STATE_STYLES: Record<string, { border: string; background: string }> = {
  green:   { border: '#16a34a', background: '#f0fdf4' },
  // Yellow (not orange — orange belongs to path-input nodes): a change
  // staged in the GUI that isn't in the database yet.
  pending: { border: '#eab308', background: '#fefce8' },
  red:     { border: '#dc2626', background: '#fef2f2' },
}

const VARIANT_DOT: Record<string, string> = {
  green:   '#16a34a',
  pending: '#eab308',
  red:     '#dc2626',
}

interface Props {
  id: string
  data: FunctionNodeData
}

/**
 * The node, in whichever of its two roles applies.
 *
 * The split is at the component boundary rather than inside one body, and that
 * is not stylistic. The pipeline node reaches for `useScope`, `useRunLog` and a
 * WebSocket subscription — none of which exist in the Plot Studio's own webview
 * tab (PlotRoot mounts no providers), and all of which would throw the moment
 * the variant popup rendered a function. Hooks cannot be skipped inside a
 * component, so the branch happens before either body runs: the popup mounts
 * *only* selection state, and the canvas mounts *only* execution state.
 */
export default function FunctionNode({ id, data }: Props) {
  const variantSelection = useVariantSelection()
  if (variantSelection) {
    return <VariantFunctionNode data={data} selection={variantSelection} />
  }
  return <PipelineFunctionNode id={id} data={data} />
}

function PipelineFunctionNode({ id, data }: Props) {
  const { getNodes, getEdges } = useReactFlow()
  const updateNodeInternals = useUpdateNodeInternals()
  const { currentScope } = useScope()
  const [running, setRunning] = useState(false)
  const [cancelling, setCancelling] = useState(false)
  // Non-null while the "defined at" dialog is open (standalone mode only —
  // in VS Code the source is revealed in the editor instead).
  const [sourceLoc, setSourceLoc] = useState<SourceLocation | null>(null)
  const { startRun, appendLine, finishRun, markCancelling, setRunMeta, updateProgress } = useRunLog()
  // Ref (not state) so the WebSocket handler always sees the current value
  // without waiting for a React re-render — critical when the pipeline
  // finishes before the first render cycle completes.
  const runIdRef = useRef<string | null>(null)

  useBackendMessage(useCallback((msg) => {
    // Support both WebSocket format (msg.type) and JSON-RPC notification format (msg.method)
    const msgType = (msg.type ?? msg.method) as string
    const params = (msg.params ?? msg) as Record<string, unknown>
    const runId = (msg.run_id ?? (params as Record<string, unknown>)?.run_id) as string | undefined
    if (runId !== runIdRef.current) return
    if (msgType === 'run_output') {
      const text = (msg.text ?? params.text) as string
      appendLine(runId!, text)
    } else if (msgType === 'run_start') {
      setRunMeta(runId!, {
        constants: (params.constants ?? {}) as Record<string, unknown>,
        input_types: (params.input_types ?? {}) as Record<string, string>,
        output_type: (params.output_type ?? '') as string,
        started_at: (params.started_at ?? Date.now() / 1000) as number,
      })
    } else if (msgType === 'run_progress') {
      updateProgress(runId!, {
        event: params.event as string,
        current: params.current as number,
        total: params.total as number,
        completed: params.completed as number,
        skipped: params.skipped as number,
        metadata: (params.metadata ?? {}) as Record<string, string>,
        error: params.error as string | undefined,
      })
    } else if (msgType === 'run_done') {
      const success = (params.success ?? true) as boolean
      const durationMs = params.duration_ms as number | undefined
      const cancelled = (params.cancelled ?? false) as boolean
      const error = params.error as string | undefined
      finishRun(runId!, success, durationMs, cancelled, error)
      setRunning(false)
      setCancelling(false)
    }
  }, [appendLine, finishRun, setRunMeta, updateProgress]))

  const handleRun = useCallback(async () => {
    // Generate run_id on the frontend BEFORE the fetch so the WebSocket
    // handler is already filtering on the correct ID when messages arrive.
    const newRunId = Math.random().toString(36).slice(2, 10)
    runIdRef.current = newRunId   // synchronous — handler sees it immediately
    setRunning(true)
    startRun(newRunId, data.label)

    // Find input variable nodes connected to this function node.
    const edges = getEdges().filter(e => e.target === id)
    const nodes = getNodes()
    const inputNodeIds = edges.map(e => e.source)

    // Collect checked variants from all connected input nodes.
    const checkedVariants: Record<string, unknown>[] = []
    for (const nodeId of inputNodeIds) {
      const node = nodes.find(n => n.id === nodeId)
      if (!node) continue
      const variants = (node.data.variants as Variant[]) ?? []
      if (variants.length <= 1) continue   // no meaningful selection
      for (const v of variants) {
        if (v.checked && Object.keys(v.constants).length > 0) {
          checkedVariants.push(v.constants)
        }
      }
    }

    const wf = (data as unknown as Record<string, unknown>).whereFilters as unknown[] | undefined
    try {
      await callBackend('start_run', {
        function_name: data.label,
        node_id: id,
        variants: checkedVariants,
        run_id: newRunId,
        schema_selection: data.schemaSelection ?? null,
        schema_level: data.schemaLevel ?? null,
        run_options: data.runOptions ?? null,
        where_filters: (wf && wf.length > 0) ? wf : null,
        // A hint only. The BACKEND decides whether this is a MATLAB run
        // (api/run.route_matlab_single_run asks matlab_registry), so a
        // browser session works the same as the VS Code host.
        language: data.language,
        // Output types for MATLAB command generation when no DB variants exist.
        output_types: data.output_types ?? null,
      })
    } catch (err) {
      // start_run can legitimately fail before any run exists — most often
      // because MATLAB currently holds the DuckDB file lock. Without this
      // the rejection escapes into an unhandled promise and the button
      // stays stuck on "⏳ Running…" with the reason nowhere on screen.
      const message = err instanceof Error ? err.message : String(err)
      appendLine(newRunId, `Error: ${message}\n`)
      finishRun(newRunId, false, 0, false, message)
      setRunning(false)
    }
  }, [id, data, getNodes, getEdges, startRun, appendLine, finishRun])

  // Show (endpoint nodes only): draft-run this endpoint + ancestors via
  // the pipeline compiler — zero DB writes; rendered outputs arrive on the
  // show_rendered message and display in the sidebar EndpointPanel. No
  // plan dialog on purpose: this is the everyday "let me look at it" loop.
  const handleShow = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation()
    const newRunId = Math.random().toString(36).slice(2, 10)
    runIdRef.current = newRunId
    setRunning(true)
    startRun(newRunId, `show ${data.label}`, 'pipeline')
    try {
      await callBackend('start_pipeline_run', {
        pipeline_id: currentScope,
        mode: 'show',
        target: data.label,
        run_id: newRunId,
      })
    } catch (err) {
      appendLine(newRunId, `Error: ${(err as Error).message}\n`)
      finishRun(newRunId, false)
      setRunning(false)
    }
  }, [data.label, currentScope, startRun, appendLine, finishRun])

  const handleCancel = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation()
    const runId = runIdRef.current
    if (!runId) return
    if (!cancelling) {
      // First click → cooperative cancel
      setCancelling(true)
      markCancelling(runId)
      try {
        await callBackend('cancel_run', { run_id: runId })
      } catch (err) {
        // Re-allow another attempt if the backend call itself failed.
        setCancelling(false)
        // eslint-disable-next-line no-console
        console.warn(`cancel_run failed for ${runId}:`, err)
      }
    } else {
      // Second click → force cancel (ctypes-injected KeyboardInterrupt)
      try {
        await callBackend('force_cancel_run', { run_id: runId })
      } catch (err) {
        // eslint-disable-next-line no-console
        console.warn(`force_cancel_run failed for ${runId}:`, err)
      }
    }
  }, [cancelling, markCancelling])

  const handleOpenSource = useCallback(async (e: React.MouseEvent) => {
    e.stopPropagation()
    try {
      const src = await callBackend('get_function_source', { name: data.label }) as {
        ok: boolean; file?: string; line?: number; error?: string
      }
      if (!src.ok) {
        window.alert(`Could not locate source for '${data.label}': ${src.error ?? 'unknown error'}`)
        return
      }
      if (isVSCodeMode) {
        await callBackend('reveal_in_editor', { file: src.file, line: src.line })
      } else {
        // In-app dialog rather than window.alert — alert text can't be
        // selected or copied, and the path is the whole message.
        setSourceLoc({ name: data.label, file: src.file ?? '', line: src.line ?? 0 })
      }
    } catch (err) {
      window.alert(`Failed to open source: ${err}`)
    }
  }, [data.label])

  const stateStyle = data.run_state ? STATE_STYLES[data.run_state] : null

  const inputParams = data.input_params ?? {}
  const outTypes = data.output_types ?? []
  const constParams = data.constant_params ?? []
  // All left-side handles: variable inputs first (by param name), then
  // parameters.
  //
  // The parameter handle id MUST be `param__{name}` — the backend's
  // PARAM_ID_PREFIX, and exactly what graph_builder.build_edges writes as
  // the targetHandle of a DB-derived Parameter→function edge. It said
  // `const__` here (a leftover from before Constants and Sweeps merged into
  // Parameters), so a synthesized edge's handle never matched a rendered
  // one, and a hand-drawn edge onto this handle reached the backend with a
  // prefix nothing recognised. That used to be absorbed by a fallback that
  // guessed the parameter from the SOURCE NODE'S LABEL — right only when
  // the declared name and the parameter name happen to coincide. The
  // fallback is gone (inputs are built from edges alone), so the id has to
  // be the real one.
  const leftHandles = [
    ...Object.entries(inputParams).map(([param, type]) => ({
      id: `in__${param}`,
      label: param,
      title: type ? `${param}: ${type}` : param,
    })),
    ...constParams.map(c => ({ id: `param__${c}`, label: c, title: c })),
  ]

  // React Flow caches each handle's measured bounds at mount. A node
  // dropped on the canvas changes its handle set twice afterwards — first
  // when get_function_params resolves asynchronously, then again on the
  // dag_updated refetch (PipelineDAG.onDrop) — and every handle's computed
  // `top` depends on the TOTAL count, so they all move. Without this the
  // cached bounds stay stale until some unrelated interaction forces a
  // re-measure, which is why the handle appeared misplaced until another
  // node was dragged onto the canvas.
  //
  // The console.debug is deliberately NOT gated behind a dev-only flag: the
  // bug is reproduced against built bundles, so a DEV gate would hide the
  // diagnostic exactly when it's wanted. console.debug sits at the console's
  // "verbose" level and is hidden unless explicitly enabled.
  const handleKey = `${leftHandles.map(h => h.id).join('|')}>${outTypes.join('|')}`
  useEffect(() => {
    updateNodeInternals(id)
    // eslint-disable-next-line no-console
    console.debug(
      `[FunctionNode ${id}] handle set changed -> ${leftHandles.length} target(s), `
      + `${outTypes.length} source(s): ${handleKey}`
    )
  }, [id, handleKey])  // eslint-disable-line react-hooks/exhaustive-deps

  // Setting `transform` here overrides React Flow's own rule for the side,
  // so the X component has to be restored explicitly or the dot sits inset
  // instead of straddling the node border:
  //   .react-flow__handle-left  { transform: translate(-50%, -50%) }
  //   .react-flow__handle-right { transform: translate( 50%, -50%) }
  const handleStyle = (
    index: number,
    total: number,
    side: 'left' | 'right',
  ): React.CSSProperties => ({
    top: `${((index + 1) / (total + 1)) * 100}%`,
    transform: `translate(${side === 'left' ? '-50%' : '50%'}, -50%)`,
  })

  return (
    <div style={{
      ...styles.container,
      ...(stateStyle ? { border: `2px solid ${stateStyle.border}`, background: stateStyle.background } : {}),
      ...(data.disconnected ? { borderStyle: 'dashed' } : {}),
    }}>
      {sourceLoc && (
        <SourceLocationDialog location={sourceLoc} onClose={() => setSourceLoc(null)} />
      )}

      {leftHandles.length > 0
        ? leftHandles.map((h, i) => (
            <Handle
              key={h.id}
              id={h.id}
              type="target"
              position={Position.Left}
              style={handleStyle(i, leftHandles.length, 'left')}
              title={h.title}
            />
          ))
        : <Handle type="target" position={Position.Left} />
      }

      {data.disconnected && (
        <div style={styles.disconnectedBadge} title="A required input's edge was deleted — reconnect it before running">
          🔌 disconnected
        </div>
      )}

      {data.endpoint_kind && (
        <div style={{
          ...styles.kindBadge,
          ...(data.endpoint_kind === 'plot' ? styles.kindPlot : styles.kindStat),
        }}>
          {data.endpoint_kind === 'plot' ? '◫ plot' : 'Σ stat'}
        </div>
      )}

      <div
        style={styles.label}
        onDoubleClick={handleOpenSource}
        title="Double-click to open function source in editor"
      >
        {data.label}
      </div>

      {(() => {
        // Variant rows: one per constant-value call site of this wiring,
        // each with its own state chip. Shown when there is more than one
        // variant or a staged (pending) value to surface.
        const variants = data.variants ?? []
        const withConstants = variants.filter(
          v => Object.keys(v.constants ?? {}).length > 0
        )
        if (withConstants.length <= 1 && !withConstants.some(v => v.staged)) {
          return null
        }
        return (
          <div style={styles.variantList}>
            {withConstants.map((v, i) => {
              const text = Object.entries(v.constants)
                .map(([k, val]) => `${k}=${val}`)
                .join(' ')
              const dotColor = v.state ? VARIANT_DOT[v.state] : '#9ca3af'
              return (
                <div key={i} style={styles.variantRow} title={
                  v.staged
                    ? `${text} — staged in the GUI, not run yet`
                    : `${text} — ${v.state ?? 'unknown'}`
                }>
                  <span style={{ ...styles.variantDot, background: dotColor }} />
                  <span style={v.staged ? styles.variantTextStaged : styles.variantText}>
                    {text}{v.staged ? ' (staged)' : ''}
                  </span>
                </div>
              )
            })}
          </div>
        )
      })()}

      {(() => {
        const isMatlab = data.language === 'matlab'
        if (!running) {
          // Endpoint nodes: Run (eager, records) + Show (draft, no writes).
          if (data.endpoint_kind && !isMatlab) {
            return (
              <div style={styles.splitButton}>
                <button style={styles.runHalf} onClick={handleRun} type="button">
                  ▶ Run
                </button>
                <button
                  style={styles.showHalf}
                  onClick={handleShow}
                  type="button"
                  title="Draft-run this endpoint (+ ancestors) and preview — nothing is recorded"
                >
                  👁 Show
                </button>
              </div>
            )
          }
          return (
            <button
              style={styles.button}
              onClick={handleRun}
              disabled={running}
            >
              ▶ Run
            </button>
          )
        }
        // MATLAB: no cancel — the run is in the MATLAB terminal, not the
        // Python worker thread. Keep the plain disabled button.
        if (isMatlab) {
          return (
            <button style={styles.buttonRunning} disabled>
              ⏳ Running…
            </button>
          )
        }
        // Python: split button with cancel / force-cancel segments.
        const leftLabel = cancelling ? '⏳ Cancelling…' : '⏳ Running…'
        const rightLabel = cancelling ? '⚠' : '✕'
        const rightTitle = cancelling
          ? 'Force cancel — best effort. Injects KeyboardInterrupt; if that fails, '
            + 'use the SciStack: Restart Python Process command.'
          : 'Cancel run (cooperative — finishes the current combo, no partial saves).'
        return (
          <div style={styles.splitButton}>
            <button
              style={styles.splitButtonLeft}
              disabled
              type="button"
            >
              {leftLabel}
            </button>
            <button
              style={cancelling ? styles.splitButtonRightForce : styles.splitButtonRight}
              onClick={handleCancel}
              title={rightTitle}
              type="button"
            >
              {rightLabel}
            </button>
          </div>
        )
      })()}

      {outTypes.length > 0
        ? outTypes.map((t, i) => (
            <Handle
              key={t}
              id={`out__${t}`}
              type="source"
              position={Position.Right}
              style={handleStyle(i, outTypes.length, 'right')}
              title={t}
            />
          ))
        : <Handle type="source" position={Position.Right} />
      }
    </div>
  )
}

/**
 * The same node, choosing which recorded version of itself a figure draws.
 *
 * The dropdown always offers "latest" first, and that is not a synonym for the
 * highest ordinal: scidb resolves it per schema location, so a subject nobody
 * re-ran keeps contributing its own newest record instead of dropping out of
 * the figure. Picking a named version deliberately gives that up.
 *
 * A function that has run under exactly one version still gets the dropdown —
 * it is a legitimate (if uninteresting) choice, and hiding the control would
 * make the common case look broken. A function with NO recorded runs renders
 * inert: there is nothing to select.
 *
 * A SECOND dropdown appears only when the function has run under more than one
 * `distribute`/`as_table` setting for the plotted measure (a `Run:<fn>` axis).
 * Those flags are part of scidb's invocation identity, so such re-runs are
 * distinct records at the same locations — a choice of the same shape as a
 * body version, made on the same node. It is absent rather than disabled in
 * the common case because "distribute=false" alone is not a choice.
 */
function VariantFunctionNode({
  data,
  selection,
}: {
  data: FunctionNodeData
  selection: VariantSelectionValue
}) {
  const label = data.label
  const outputs = data.output_types ?? []
  // Same handle set as the pipeline node — derived from `data` alone, so the
  // graph keeps the shape the user knows without any of the canvas's state.
  const leftHandles = [
    ...Object.entries(data.input_params ?? {}).map(([param, type]) => ({
      id: `in__${param}`,
      title: type ? `${param}: ${type}` : param,
    })),
    ...(data.constant_params ?? []).map(c => ({ id: `param__${c}`, title: c })),
  ]
  const handleStyle = (
    index: number,
    total: number,
    side: 'left' | 'right',
  ): React.CSSProperties => ({
    top: `${((index + 1) / (total + 1)) * 100}%`,
    transform: `translate(${side === 'left' ? '-50%' : '50%'}, -50%)`,
  })

  const axis = selection.axisForFunction(label)
  const runAxis = selection.runAxisForFunction(label)
  const versions = selection.versionsFor(label)
  const inert = versions.length === 0
  // No axis means this function's versions do not distinguish the plotted
  // measure's records — either it is not upstream of it, or it only ever ran
  // one version. Selecting is then meaningless, so say so instead of pretending.
  const effective = axis ? selection.versionFor(axis.column) : 'latest'
  const effectiveRun = runAxis ? selection.versionFor(runAxis.column) : 'latest'
  const hasAxis = !!axis || !!runAxis

  return (
    <div
      style={{ ...styles.container, ...(inert || !hasAxis ? styles.variantInert : null) }}
      title={
        inert
          ? `${label} has no recorded runs.`
          : !hasAxis
            ? `${label}'s version does not distinguish this measure's records.`
            : undefined
      }
    >
      {leftHandles.length > 0
        ? leftHandles.map((h, i) => (
            <Handle
              key={h.id}
              id={h.id}
              type="target"
              position={Position.Left}
              style={handleStyle(i, leftHandles.length, 'left')}
              title={h.title}
            />
          ))
        : <Handle type="target" position={Position.Left} />
      }
      <div style={{ ...styles.label, cursor: 'default', textDecoration: 'none' }}>
        {label}
      </div>
      {inert ? (
        <div style={styles.variantNone}>never run</div>
      ) : (
        <select
          value={effective}
          disabled={!axis}
          onChange={e => axis && selection.setVersion(axis.column, e.target.value)}
          // React Flow's drag/pan gestures otherwise swallow the pointer before
          // the native menu opens.
          className="nodrag nopan"
          style={styles.versionSelect}
          title={
            axis
              ? 'Which version of this function produced the records to plot'
              : 'Not a variant axis for this measure'
          }
        >
          <option value="latest">latest</option>
          {versions.map(v => (
            <option key={v.version} value={v.version}>
              {v.version}
              {v.first_saved ? ` — ${v.first_saved.slice(0, 10)}` : ''}
            </option>
          ))}
        </select>
      )}
      {!inert && runAxis && (
        <select
          value={effectiveRun}
          onChange={e => selection.setVersion(runAxis.column, e.target.value)}
          className="nodrag nopan"
          style={{ ...styles.versionSelect, marginTop: 4 }}
          title="Which for_each run options (distribute / as_table) produced the records to plot"
        >
          <option value="latest">latest run options</option>
          {runAxis.levels.map(level => (
            <option key={level} value={level}>
              {level}
            </option>
          ))}
        </select>
      )}
      {outputs.length > 0
        ? outputs.map((t, i) => (
            <Handle
              key={t}
              id={`out__${t}`}
              type="source"
              position={Position.Right}
              style={handleStyle(i, outputs.length, 'right')}
              title={t}
            />
          ))
        : <Handle type="source" position={Position.Right} />
      }
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  container: {
    background: '#f0f4ff',
    border: '2px solid #7b68ee',
    borderRadius: 6,
    padding: '8px 12px',
    minWidth: 180,
    fontSize: 13,
    boxShadow: '0 2px 6px rgba(0,0,0,0.10)',
  },
  label: {
    fontWeight: 600,
    color: '#3a1a8e',
    fontFamily: 'monospace',
    marginBottom: 6,
    textAlign: 'center',
    cursor: 'pointer',
    textDecoration: 'underline',
    textDecorationStyle: 'dotted',
    textUnderlineOffset: '2px',
  },
  variantList: {
    display: 'flex',
    flexDirection: 'column',
    gap: 2,
    marginBottom: 6,
    maxHeight: 96,
    overflowY: 'auto',
  },
  // Variant popup only.
  variantInert: { opacity: 0.4, borderStyle: 'dashed' },
  variantNone: {
    fontSize: 10,
    color: '#6b6b8f',
    fontStyle: 'italic',
    textAlign: 'center',
  },
  versionSelect: {
    width: '100%',
    background: '#fff',
    color: '#3a1a8e',
    border: '1px solid #7b68ee',
    borderRadius: 4,
    fontSize: 11,
    padding: '2px 4px',
  },
  variantRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 5,
  },
  variantDot: {
    width: 7,
    height: 7,
    borderRadius: '50%',
    flexShrink: 0,
  },
  variantText: {
    fontSize: 10,
    fontFamily: 'monospace',
    color: '#444',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  },
  variantTextStaged: {
    fontSize: 10,
    fontFamily: 'monospace',
    color: '#a16207',
    fontStyle: 'italic',
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
  },
  kindBadge: {
    display: 'block',
    margin: '0 auto 3px',
    padding: '0 8px',
    width: 'fit-content',
    borderRadius: 10,
    fontSize: 9,
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: 0.6,
  },
  disconnectedBadge: {
    display: 'block',
    margin: '0 auto 4px',
    padding: '1px 8px',
    width: 'fit-content',
    borderRadius: 10,
    fontSize: 9,
    fontWeight: 700,
    textTransform: 'uppercase',
    letterSpacing: 0.4,
    background: '#fef2f2',
    color: '#dc2626',
    border: '1px solid #dc2626',
  },
  kindPlot: {
    background: '#cffafe',
    color: '#0e7490',
    border: '1px solid #0891b2',
  },
  kindStat: {
    background: '#ede9fe',
    color: '#5b21b6',
    border: '1px solid #6d28d9',
  },
  runHalf: {
    flex: 1,
    padding: '4px 0',
    background: '#7b68ee',
    color: '#fff',
    border: 'none',
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
  },
  showHalf: {
    flex: 1,
    padding: '4px 0',
    background: '#0891b2',
    color: '#fff',
    border: 'none',
    borderLeft: '1px solid #fff',
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
  },
  button: {
    width: '100%',
    padding: '4px 0',
    background: '#7b68ee',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: 12,
  },
  buttonRunning: {
    width: '100%',
    padding: '4px 0',
    background: '#b0a8f0',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'not-allowed',
    fontWeight: 600,
    fontSize: 12,
  },
  splitButton: {
    display: 'flex',
    width: '100%',
    borderRadius: 4,
    overflow: 'hidden',
  },
  splitButtonLeft: {
    flex: '0 0 70%',
    padding: '4px 0',
    background: '#b0a8f0',
    color: '#fff',
    border: 'none',
    cursor: 'not-allowed',
    fontWeight: 600,
    fontSize: 12,
  },
  splitButtonRight: {
    flex: '0 0 30%',
    padding: '4px 0',
    background: '#dc2626',
    color: '#fff',
    border: 'none',
    borderLeft: '1px solid #fff',
    cursor: 'pointer',
    fontWeight: 700,
    fontSize: 12,
  },
  splitButtonRightForce: {
    flex: '0 0 30%',
    padding: '4px 0',
    background: '#991b1b',
    color: '#fde68a',
    border: 'none',
    borderLeft: '1px solid #fff',
    cursor: 'pointer',
    fontWeight: 700,
    fontSize: 12,
  },
}
