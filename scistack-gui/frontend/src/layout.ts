/**
 * Auto-layout for the pipeline DAG using dagre.
 *
 * dagre is a directed graph layout algorithm. We feed it the nodes and edges,
 * it assigns (x, y) coordinates so the graph reads left-to-right without
 * overlapping nodes.
 *
 * IMPORTANT: dagre is only used for NEW nodes that don't yet have a saved
 * position. Once a node has a persisted position it never moves unless the
 * user drags it.
 */

import dagre from '@dagrejs/dagre'
import type { Node, Edge } from '@xyflow/react'

const NODE_WIDTH = 180
const NODE_HEIGHT = 60

export function applyDagreLayout(
  nodes: Node[],
  edges: Edge[],
  savedPositions: Record<string, { x: number; y: number }>,
): Node[] {
  const g = new dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'LR', ranksep: 120, nodesep: 60 })

  for (const node of nodes) {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT })
  }
  for (const edge of edges) {
    g.setEdge(edge.source, edge.target)
  }

  dagre.layout(g)

  return nodes.map((node) => {
    // If the user has saved a position for this node, use it — never override.
    if (savedPositions[node.id]) {
      return { ...node, position: savedPositions[node.id] }
    }
    // Otherwise use what dagre computed.
    const { x, y } = g.node(node.id)
    return {
      ...node,
      position: { x: x - NODE_WIDTH / 2, y: y - NODE_HEIGHT / 2 },
    }
  })
}
