/**
 * FunctionNode — represents a pipeline function (e.g. compute_rolling_vo2).
 *
 * Features:
 *   - Run button: posts to /api/run with the checked variants from connected
 *     input nodes, then streams output via WebSocket.
 *   - Spinner while running.
 *   - Run output is sent to the sidebar Runs tab via RunLogContext.
 */

import { useState, useCallback, useRef } from 'react'
import { Handle, Position, useReactFlow } from '@xyflow/react'
import { callBackend, isVSCodeMode } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import { useRunLog } from '../../context/RunLogContext'
import type { Variant } from './VariableNode'

interface FunctionNodeData {
  label: string
  input_params?: Record<string, string>  // param_name → type_name
  output_types?: string[]
  constant_params?: string[]
  run_state?: 'green' | 'grey' | 'red'
}

const STATE_STYLES: Record<string, { border: string; background: string }> = {
  green: { border: '#16a34a', background: '#f0fdf4' },
  grey:  { border: '#6b7280', background: '#f3f4f6' },
  red:   { border: '#dc2626', background: '#fef2f2' },
}

interface Props {
  id: string
  data: FunctionNodeData
}

export default function FunctionNode({ id, data }: Props) {
  const { getNodes, getEdges } = useReactFlow()
  const [running, setRunning] = useState(false)
  const { startRun, appendLine, finishRun, setRunMeta, updateProgress } = useRunLog()
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
      finishRun(runId!, success, durationMs)
      setRunning(false)
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

    await callBackend('start_run', {
      function_name: data.label,
      variants: checkedVariants,
      run_id: newRunId,
    })
  }, [id, data.label, getNodes, getEdges, startRun])

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
        window.alert(`${data.label} is defined at:\n${src.file}:${src.line}`)
      }
    } catch (err) {
      window.alert(`Failed to open source: ${err}`)
    }
  }, [data.label])

  const stateStyle = data.run_state ? STATE_STYLES[data.run_state] : null

  const inputParams = data.input_params ?? {}
  const outTypes = data.output_types ?? []
  const constParams = data.constant_params ?? []
  // All left-side handles: variable inputs first (by param name), then constants
  const leftHandles = [
    ...Object.entries(inputParams).map(([param, type]) => ({
      id: `in__${param}`,
      label: param,
      title: type ? `${param}: ${type}` : param,
    })),
    ...constParams.map(c => ({ id: `const__${c}`, label: c, title: c })),
  ]

  const handleStyle = (index: number, total: number): React.CSSProperties => ({
    top: `${((index + 1) / (total + 1)) * 100}%`,
    transform: 'translateY(-50%)',
  })

  return (
    <div style={{
      ...styles.container,
      ...(stateStyle ? { border: `2px solid ${stateStyle.border}`, background: stateStyle.background } : {}),
    }}>
      {leftHandles.length > 0
        ? leftHandles.map((h, i) => (
            <Handle
              key={h.id}
              id={h.id}
              type="target"
              position={Position.Left}
              style={handleStyle(i, leftHandles.length)}
              title={h.title}
            />
          ))
        : <Handle type="target" position={Position.Left} />
      }

      <div
        style={styles.label}
        onDoubleClick={handleOpenSource}
        title="Double-click to open function source in editor"
      >
        {data.label}
      </div>

      <button
        style={running ? styles.buttonRunning : styles.button}
        onClick={handleRun}
        disabled={running}
      >
        {running ? '⏳ Running…' : '▶ Run'}
      </button>

      {outTypes.length > 0
        ? outTypes.map((t, i) => (
            <Handle
              key={t}
              id={`out__${t}`}
              type="source"
              position={Position.Right}
              style={handleStyle(i, outTypes.length)}
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
}
