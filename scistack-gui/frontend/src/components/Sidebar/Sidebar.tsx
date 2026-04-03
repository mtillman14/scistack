/**
 * Sidebar — right-panel with tabs.
 *
 * Tabs:
 *   - Runs: collapsible per-run log sections, most recent first.
 */

import { useState } from 'react'
import RunsTab from './RunsTab'
import EditTab from './EditTab'

const TABS = ['Runs', 'Edit'] as const
type Tab = typeof TABS[number]

export default function Sidebar() {
  const [activeTab, setActiveTab] = useState<Tab>('Runs')

  return (
    <div style={styles.root}>
      <div style={styles.tabBar}>
        {TABS.map(tab => (
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
