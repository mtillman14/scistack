/**
 * Sidebar — right-panel with tabs.
 *
 * Tabs:
 *   - Runs: collapsible per-run log sections, most recent first.
 *   - Edit: palette of draggable function and variable nodes.
 *   - Node: settings panel for the selected function or constant node (auto-activates on selection).
 *
 * When a function node is selected, the Node tab shows a read-only list of all
 * pipeline variants — the Cartesian product of every constant node's values on the canvas.
 */

import { useState, useEffect, useMemo } from 'react'
import { useStore } from '@xyflow/react'
import RunsTab from './RunsTab'
import EditTab from './EditTab'
import FunctionSettingsPanel from './FunctionSettingsPanel'
import ConstantSettingsPanel from './ConstantSettingsPanel'
import { useSelectedNode } from '../../context/SelectedNodeContext'
import type { Node } from '@xyflow/react'
import type { ConstantValue } from '../DAG/ConstantNode'

const BASE_TABS = ['Runs', 'Edit'] as const
type BaseTab = typeof BASE_TABS[number]
type Tab = BaseTab | 'Node'

interface FnNodeData {
  label: string
}

interface ConstantNodeData {
  label: string
  values: ConstantValue[]
}

function isFunctionNode(node: Node | null): node is Node & { data: FnNodeData } {
  return node?.type === 'functionNode'
}

function isConstantNode(node: Node | null): node is Node & { data: ConstantNodeData } {
  return node?.type === 'constantNode'
}

/** Compute the Cartesian product of value arrays. */
function cartesian(arrays: string[][]): string[][] {
  if (arrays.length === 0) return []
  return arrays.reduce<string[][]>(
    (acc, arr) => acc.flatMap(row => arr.map(v => [...row, v])),
    [[]]
  )
}

export default function Sidebar() {
  const { selectedNode } = useSelectedNode()
  const [activeTab, setActiveTab] = useState<Tab>('Runs')

  // Subscribe directly to the React Flow store so we re-render when node/edge data changes.
  const nodes = useStore(s => s.nodes)
  const edges = useStore(s => s.edges)

  // Auto-switch to Node tab when a function or constant node is selected; revert when deselected.
  useEffect(() => {
    if (isFunctionNode(selectedNode) || isConstantNode(selectedNode)) {
      setActiveTab('Node')
    } else if (activeTab === 'Node') {
      setActiveTab('Runs')
    }
  }, [selectedNode])  // eslint-disable-line react-hooks/exhaustive-deps

  const hasNodeTab = isFunctionNode(selectedNode) || isConstantNode(selectedNode)
  const tabs: Tab[] = hasNodeTab ? ['Runs', 'Edit', 'Node'] : ['Runs', 'Edit']

  // Compute variant combinations from constant nodes connected to the selected function node.
  // Re-derived whenever nodes or edges change (value edits, new connections, etc.).
  const { constantNames, variants } = useMemo(() => {
    if (!isFunctionNode(selectedNode)) return { constantNames: [], variants: [] }

    // BFS upstream: walk edges in reverse to find all ancestor node IDs.
    const visited = new Set<string>()
    const queue = [selectedNode.id]
    while (queue.length > 0) {
      const current = queue.shift()!
      for (const e of edges) {
        if (e.target === current && !visited.has(e.source)) {
          visited.add(e.source)
          queue.push(e.source)
        }
      }
    }

    const constantNodes = nodes.filter(
      n => n.type === 'constantNode' && visited.has(n.id)
    ) as Array<Node & { data: ConstantNodeData }>

    if (constantNodes.length === 0) return { constantNames: [], variants: [] }

    const names = constantNodes.map(n => n.data.label)
    const valueLists = constantNodes.map(n =>
      (n.data.values ?? []).map((v: ConstantValue) => v.value)
    )

    // If any constant has no values, there are no valid variants.
    if (valueLists.some(vals => vals.length === 0)) return { constantNames: names, variants: [] }

    const combos = cartesian(valueLists)
    const variantRows = combos.map(combo =>
      Object.fromEntries(names.map((name, i) => [name, combo[i]]))
    )

    return { constantNames: names, variants: variantRows }
  }, [nodes, edges, selectedNode])

  return (
    <div style={styles.root}>
      <div style={styles.tabBar}>
        {tabs.map(tab => (
          <button
            key={tab}
            style={activeTab === tab ? styles.tabActive : styles.tab}
            onClick={() => setActiveTab(tab)}
          >
            {tab}
          </button>
        ))}
      </div>
      <div style={styles.content}>
        {activeTab === 'Runs' && <RunsTab />}
        {activeTab === 'Edit' && <EditTab />}
        {activeTab === 'Node' && isFunctionNode(selectedNode) && (
          <FunctionSettingsPanel
            label={(selectedNode.data as FnNodeData).label}
            variants={variants}
            constantNames={constantNames}
          />
        )}
        {activeTab === 'Node' && isConstantNode(selectedNode) && (
          <ConstantSettingsPanel
            id={selectedNode.id}
            label={(selectedNode.data as ConstantNodeData).label}
            values={(selectedNode.data as ConstantNodeData).values}
          />
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    overflow: 'hidden',
  },
  tabBar: {
    display: 'flex',
    flexShrink: 0,
    borderBottom: '1px solid #2a2a4a',
    background: '#12122a',
  },
  tab: {
    padding: '8px 16px',
    background: 'transparent',
    border: 'none',
    borderBottom: '2px solid transparent',
    color: '#888',
    fontSize: 13,
    cursor: 'pointer',
    fontWeight: 500,
  },
  tabActive: {
    padding: '8px 16px',
    background: 'transparent',
    border: 'none',
    borderBottom: '2px solid #7b68ee',
    color: '#fff',
    fontSize: 13,
    cursor: 'pointer',
    fontWeight: 600,
  },
  content: {
    flex: 1,
    overflowY: 'auto',
    padding: '8px 0',
  },
}
