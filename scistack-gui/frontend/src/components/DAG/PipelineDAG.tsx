/**
 * PipelineDAG — the main React Flow canvas.
 *
 * Fetches GET /api/pipeline on mount, applies dagre layout, and renders
 * the interactive pipeline graph.
 *
 * React Flow concepts used here:
 *   - ReactFlow component: the canvas itself
 *   - useNodesState / useEdgesState: React state hooks that React Flow provides
 *     for tracking the node/edge arrays (including position changes from dragging)
 *   - nodeTypes: maps the "type" string from our backend data to a React component
 *   - Background / Controls / MiniMap: built-in UI chrome from React Flow
 */

import { useEffect, useCallback } from 'react'
import {
  ReactFlow,
  useNodesState,
  useEdgesState,
  useReactFlow,
  Background,
  Controls,
  MiniMap,
  type Node,
  type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import VariableNode from './VariableNode'
import FunctionNode from './FunctionNode'
import { applyDagreLayout } from '../../layout'
import { useWebSocket } from '../../hooks/useWebSocket'

// Tell React Flow which React component to render for each node "type" string.
// These match the "type" field we set in GET /api/pipeline.
const nodeTypes = {
  variableNode: VariableNode,
  functionNode: FunctionNode,
}

export default function PipelineDAG() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { screenToFlowPosition } = useReactFlow()

  const fetchPipeline = useCallback(async () => {
    const [pipelineRes, layoutRes] = await Promise.all([
      fetch('/api/pipeline'),
      fetch('/api/layout'),
    ])
    const data = await pipelineRes.json()
    const layoutData = await layoutRes.json()
    const savedPositions: Record<string, { x: number; y: number }> =
      layoutData.positions ?? layoutData  // handle both new and legacy format

    // Initialise all variants as checked (selected for running).
    const initialised = data.nodes.map((node: Node) => ({
      ...node,
      data: {
        ...node.data,
        variants: ((node.data as { variants?: unknown[] }).variants ?? []).map(
          (v: unknown) => ({ ...(v as object), checked: true })
        ),
      },
    }))

    const laidOut = applyDagreLayout(initialised, data.edges, savedPositions)
    setNodes(laidOut)
    setEdges(data.edges)
  }, [setNodes, setEdges])

  useEffect(() => {
    fetchPipeline()
  }, [fetchPipeline])

  // Refresh DAG whenever the backend signals that data changed.
  useWebSocket(useCallback((msg) => {
    if (msg.type === 'dag_updated') fetchPipeline()
  }, [fetchPipeline]))

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    const raw = e.dataTransfer.getData('application/scistack-node')
    if (!raw) return
    const { nodeType, label } = JSON.parse(raw) as { nodeType: string; label: string }

    const position = screenToFlowPosition({ x: e.clientX, y: e.clientY })
    const prefix = nodeType === 'functionNode' ? 'fn' : 'var'
    const nodeId = `${prefix}__${label}__${Math.random().toString(36).slice(2, 8)}`

    setNodes(prev => {
      const newNode: Node = {
        id: nodeId,
        type: nodeType,
        position,
        data: {
          label,
          ...(nodeType === 'variableNode'
            ? { variants: [], total_records: 0 }
            : {}),
        },
      }
      return [...prev, newNode]
    })

    // Persist so it survives a DAG refresh.
    fetch(`/api/layout/${encodeURIComponent(nodeId)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ x: position.x, y: position.y, node_type: nodeType, label }),
    })
  }, [screenToFlowPosition, setNodes])

  const onNodeDragStop = useCallback((_: unknown, node: Node) => {
    fetch(`/api/layout/${encodeURIComponent(node.id)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(node.position),
    })
  }, [])

  return (
    <div
      style={{ width: '100%', height: '100%' }}
      onDragOver={onDragOver}
      onDrop={onDrop}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeDragStop={onNodeDragStop}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
      >
        <Background />
        <Controls />
        <MiniMap />
      </ReactFlow>
    </div>
  )
}
