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
 *   - Background / Controls: built-in UI chrome from React Flow
 */

import { useEffect, useCallback, useRef } from 'react'
import {
  ReactFlow,
  addEdge,
  useNodesState,
  useEdgesState,
  useReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
  type EdgeChange,
  type Connection,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

import VariableNode from './VariableNode'
import FunctionNode from './FunctionNode'
import ConstantNode from './ConstantNode'
import PathInputNode from './PathInputNode'
import { applyDagreLayout } from '../../layout'
import { callBackend } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import { useSelectedNode } from '../../context/SelectedNodeContext'

// Tell React Flow which React component to render for each node "type" string.
// These match the "type" field we set in GET /api/pipeline.
const nodeTypes = {
  variableNode: VariableNode,
  functionNode: FunctionNode,
  constantNode: ConstantNode,
  pathInputNode: PathInputNode,
}

export default function PipelineDAG() {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChangeBase] = useEdgesState<Edge>([])
  const { screenToFlowPosition, fitView } = useReactFlow()
  const { selectedNode, setSelectedNode } = useSelectedNode()
  const isFirstLoad = useRef(true)

  const fetchPipeline = useCallback(async () => {
    // Fetch pipeline first — _build_graph has a side effect (graduate_manual_node)
    // that writes to layout.json. Layout must be read AFTER that write, otherwise
    // savedPositions will have stale keys and dagre will recalculate positions.
    const data = await callBackend('get_pipeline') as { nodes: Node[]; edges: Edge[] }
    const layoutData = await callBackend('get_layout') as Record<string, unknown>
    const savedPositions =
      (layoutData.positions ?? layoutData) as Record<string, { x: number; y: number }>  // handle both new and legacy format

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

    // On refreshes, use current on-screen positions so nodes never jump.
    // Only fall back to saved/dagre positions for nodes not already on screen.
    setNodes(prev => {
      const currentPositions: Record<string, { x: number; y: number }> = {}
      for (const n of prev) {
        currentPositions[n.id] = n.position
      }
      const merged = { ...savedPositions, ...currentPositions }
      return applyDagreLayout(initialised, data.edges, merged)
    })
    setEdges(data.edges)

    // Only fit the viewport on the very first load.
    if (isFirstLoad.current) {
      isFirstLoad.current = false
      // Small delay so React Flow has rendered the nodes before fitting.
      setTimeout(() => fitView({ padding: 0.2 }), 50)
    }
  }, [setNodes, setEdges, fitView])

  useEffect(() => {
    fetchPipeline()
  }, [fetchPipeline])

  // Refresh DAG whenever the backend signals that data changed.
  useBackendMessage(useCallback((msg) => {
    if (msg.type === 'dag_updated' || msg.method === 'dag_updated') fetchPipeline()
  }, [fetchPipeline]))

  // Keep selectedNode data fresh after DAG refreshes.
  useEffect(() => {
    if (!selectedNode) return
    const updated = nodes.find(n => n.id === selectedNode.id)
    if (updated && updated !== selectedNode) setSelectedNode(updated)
    else if (!updated) setSelectedNode(null)
  }, [nodes])  // eslint-disable-line react-hooks/exhaustive-deps

  const onNodeClick = useCallback((_: unknown, node: Node) => {
    if (node.type === 'functionNode' || node.type === 'constantNode' || node.type === 'variableNode' || node.type === 'pathInputNode') {
      setSelectedNode(node)
    } else {
      setSelectedNode(null)
    }
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
    const prefix = nodeType === 'functionNode' ? 'fn' : nodeType === 'constantNode' ? 'const' : nodeType === 'pathInputNode' ? 'pathInput' : 'var'
    const nodeId = `${prefix}__${label}__${Math.random().toString(36).slice(2, 8)}`

    const buildFnData = async () => {
      if (nodeType !== 'functionNode') return { run_state: 'red' as const }
      try {
        const { params } = await callBackend('get_function_params', { name: label }) as { params: string[] }
        const input_params: Record<string, string> = {}
        for (const p of params) input_params[p] = ''
        return { input_params, output_types: [] as string[], constant_params: [] as string[], run_state: 'red' as const }
      } catch {
        return { run_state: 'red' as const }
      }
    }

    buildFnData().then(fnExtra => {
      setNodes(prev => {
        const newNode: Node = {
          id: nodeId,
          type: nodeType,
          position,
          data: {
            label,
            ...(nodeType === 'variableNode' ? { total_records: 0, run_state: 'red' } : {}),
            ...(nodeType === 'functionNode' ? fnExtra : {}),
            ...(nodeType === 'constantNode' ? { values: [] } : {}),
            ...(nodeType === 'pathInputNode' ? { template: '', root_folder: null } : {}),
          },
        }
        return [...prev, newNode]
      })
    })

    // Persist so it survives a DAG refresh.
    callBackend('put_layout', { node_id: nodeId, x: position.x, y: position.y, node_type: nodeType, label })
  }, [screenToFlowPosition, setNodes])

  const onNodeDragStop = useCallback((_: unknown, node: Node) => {
    callBackend('put_layout', { node_id: node.id, x: node.position.x, y: node.position.y })
  }, [])

  const onNodesDelete = useCallback((deleted: Node[]) => {
    for (const node of deleted) {
      callBackend('delete_layout', { node_id: node.id })
    }
  }, [])

  const onConnect = useCallback((connection: Connection) => {
    const edgeId = `manual__${Math.random().toString(36).slice(2, 8)}`
    const edge: Edge = {
      ...connection,
      id: edgeId,
      data: { manual: true },
    }
    setEdges(prev => addEdge(edge, prev))
    callBackend('put_edge', {
      edge_id: edgeId,
      source: connection.source,
      target: connection.target,
      source_handle: connection.sourceHandle ?? null,
      target_handle: connection.targetHandle ?? null,
    })
  }, [setEdges])

  const onEdgesChange = useCallback((changes: EdgeChange[]) => {
    for (const change of changes) {
      if (change.type === 'remove' && change.id.startsWith('manual__')) {
        callBackend('delete_edge', { edge_id: change.id })
      }
    }
    // DB-derived edges represent real data — block removal so they don't
    // flicker away and reappear on the next pipeline refresh.
    onEdgesChangeBase(changes.filter(c => c.type !== 'remove' || c.id.startsWith('manual__')))
  }, [onEdgesChangeBase])

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
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onPaneClick={onPaneClick}
        nodeTypes={nodeTypes}
      >
        <Background />
        <Controls />
      </ReactFlow>
    </div>
  )
}
