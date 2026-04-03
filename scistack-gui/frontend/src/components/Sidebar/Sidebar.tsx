/**
 * Sidebar — right-panel with tabs.
 *
 * Tabs:
 *   - Runs: collapsible per-run log sections, most recent first.
 *   - Edit: palette of draggable function and variable nodes.
 *   - Node: settings panel for the selected function node (auto-activates on selection).
 */

import { useState, useEffect } from 'react'
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

interface FnVariant {
  constants: Record<string, unknown>
  input_types: Record<string, string>
  output_type: string
  record_count: number
}

interface FnNodeData {
  label: string
  variants?: FnVariant[]
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

export default function Sidebar() {
  const { selectedNode } = useSelectedNode()
  const [activeTab, setActiveTab] = useState<Tab>('Runs')

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
            variants={(selectedNode.data as FnNodeData).variants ?? []}
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
