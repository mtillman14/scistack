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
import ConstantNode from './ConstantNode'
import { applyDagreLayout } from '../../layout'
import { useWebSocket } from '../../hooks/useWebSocket'
import { useSelectedNode } from '../../context/SelectedNodeContext'

// Tell React Flow which React component to render for each node "type" string.
// These match the "type" field we set in GET /api/pipeline.
const nodeTypes = {
  variableNode: VariableNode,
  functionNode: FunctionNode,
  constantNode: ConstantNode,
}

export default function PipelineDAG() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const { screenToFlowPosition } = useReactFlow()
  const { selectedNode, setSelectedNode } = useSelectedNode()

  const fetchPipeline = useCallback(async () => {
    const [pipelineRes, layoutRes] = await Promise.all([
      fetch('/api/pipeline'),
      fetch('/api/layout'),
    ])
    const data = await pipelineRes.json()
    const layoutData = await layoutRes.json()
    const savedPositions: Record<string, { x: number; y: number }> =
      layoutData.positions ?? layoutData  // handle both new and legacy format

    // Initialise all constant values as checked (selected for running).
    const initialised = data.nodes.map((node: Node) => {
      if (node.type !== 'constantNode') return node
      return {
        ...node,
        data: {
          ...node.data,
          values: ((node.data as { values?: unknown[] }).values ?? []).map(
            (v: unknown) => ({ ...(v as object), checked: true })
          ),
        },
      }
    })

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

  // Keep selectedNode data fresh after DAG refreshes.
  useEffect(() => {
    if (!selectedNode) return
    const updated = nodes.find(n => n.id === selectedNode.id)
    if (updated && updated !== selectedNode) setSelectedNode(updated)
    else if (!updated) setSelectedNode(null)
  }, [nodes])  // eslint-disable-line react-hooks/exhaustive-deps

  const onNodeClick = useCallback((_: unknown, node: Node) => {
    if (node.type === 'functionNode' || node.type === 'constantNode') setSelectedNode(node)
    else setSelectedNode(null)
  }, [setSelectedNode])

  const onPaneClick = useCallback(() => {
    setSelectedNode(null)
  }, [setSelectedNode])

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
          ...(nodeType === 'variableNode' ? { total_records: 0 } : {}),
          ...(nodeType === 'constantNode' ? { values: [] } : {}),
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

  const onNodesDelete = useCallback((deleted: Node[]) => {
    for (const node of deleted) {
      fetch(`/api/layout/${encodeURIComponent(node.id)}`, { method: 'DELETE' })
    }
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
        onNodesDelete={onNodesDelete}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
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
