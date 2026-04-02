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
  Background,
  Controls,
  MiniMap,
  type Node,
  type Edge,
  type NodeChange,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import VariableNode from './VariableNode'
import FunctionNode from './FunctionNode'
import { applyDagreLayout } from '../../layout'

// Tell React Flow which React component to render for each node "type" string.
// These match the "type" field we set in GET /api/pipeline.
const nodeTypes = {
  variableNode: VariableNode,
  functionNode: FunctionNode,
}

export default function PipelineDAG() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])

  const fetchPipeline = useCallback(async () => {
    const res = await fetch('/api/pipeline')
    const data = await res.json()

    // savedPositions will come from the layout endpoint later (Task 6).
    // For now pass an empty object so all nodes get dagre positions.
    const savedPositions: Record<string, { x: number; y: number }> = {}
    const laidOut = applyDagreLayout(data.nodes, data.edges, savedPositions)
    setNodes(laidOut)
    setEdges(data.edges)
  }, [setNodes, setEdges])

  useEffect(() => {
    fetchPipeline()
  }, [fetchPipeline])

  // When the user stops dragging a node, we'll persist the position.
  // For now just log it — Task 6 will wire this to PUT /api/layout/{id}.
  const onNodeDragStop = useCallback((_: unknown, node: Node) => {
    console.log('Node moved:', node.id, node.position)
  }, [])

  return (
    <div style={{ width: '100%', height: '100%' }}>
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
