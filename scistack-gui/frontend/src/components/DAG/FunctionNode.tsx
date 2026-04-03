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
import { useWebSocket } from '../../hooks/useWebSocket'
import { useRunLog } from '../../context/RunLogContext'
import type { Variant } from './VariableNode'

interface FunctionNodeData {
  label: string
}

interface Props {
  id: string
  data: FunctionNodeData
}

export default function FunctionNode({ id, data }: Props) {
  const { getNodes, getEdges } = useReactFlow()
  const [running, setRunning] = useState(false)
  const { startRun, appendLine, finishRun } = useRunLog()
  // Ref (not state) so the WebSocket handler always sees the current value
  // without waiting for a React re-render — critical when the pipeline
  // finishes before the first render cycle completes.
  const runIdRef = useRef<string | null>(null)

  useWebSocket(useCallback((msg) => {
    if (msg.run_id !== runIdRef.current) return
    if (msg.type === 'run_output') {
      appendLine(msg.run_id as string, msg.text as string)
    } else if (msg.type === 'run_done') {
      finishRun(msg.run_id as string)
      setRunning(false)
    }
  }, [appendLine, finishRun]))

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

    await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        function_name: data.label,
        variants: checkedVariants,
        run_id: newRunId,
      }),
    })
  }, [id, data.label, getNodes, getEdges, startRun])

  return (
    <div style={styles.container}>
      <Handle type="target" position={Position.Left} />

      <div style={styles.label}>{data.label}</div>

      <button
        style={running ? styles.buttonRunning : styles.button}
        onClick={handleRun}
        disabled={running}
      >
        {running ? '⏳ Running…' : '▶ Run'}
      </button>

      <Handle type="source" position={Position.Right} />
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
