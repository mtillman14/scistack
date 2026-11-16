/**
 * The where= filters a function node carries, one chip each (issue #13).
 *
 * The chip TEXT comes from the backend (`whereChips`, scidb's `filter_label`);
 * this file never words a filter. Click a chip = a pooled histogram of the
 * filter's variable, coloured by whether each location matches. Right-click =
 * Histogram, "Plot <input> against it" (one entry per variable input of the
 * node), Edit (select the node: the Filters section is in its sidebar) and
 * Remove. Opening is by window event; PipelineDAG owns opening studios
 * (docs/claude/where-in-gui.md).
 */

import { useState } from 'react'
import { callBackend } from '../../api'
import {
  OPEN_WHERE_CHECK_EVENT,
  type StoredWhereFilter,
  type WhereCheckTarget,
} from '../PlotStudio/whereCheck'

export interface WhereChip {
  index: number
  text: string
  variable: string
  column: string | null
}

interface Props {
  nodeId: string
  functionName: string
  chips: WhereChip[]
  /** The node's stored rows, the check's filters. */
  filters: StoredWhereFilter[]
  /** The variable types wired into the node's inputs (`input_params` values). */
  inputVariables: string[]
}

function openCheck(target: WhereCheckTarget) {
  console.info('[where_check] open', {
    node: target.nodeId,
    filter: target.filterIndex,
    against: target.against,
  })
  window.dispatchEvent(new CustomEvent<WhereCheckTarget>(OPEN_WHERE_CHECK_EVENT, { detail: target }))
}

export default function WhereChips({ nodeId, functionName, chips, filters, inputVariables }: Props) {
  const [menu, setMenu] = useState<{ chip: WhereChip; x: number; y: number } | null>(null)
  if (chips.length === 0) return null

  const target = (chip: WhereChip, against: string | null): WhereCheckTarget => ({
    nodeId,
    functionName,
    filters,
    filterIndex: chip.index,
    against,
  })

  const remove = (chip: WhereChip) => {
    const rows = filters.filter((_, i) => i !== chip.index)
    callBackend('set_where_filters', { node_id: nodeId, filters: rows })
      .catch(err => console.error('[where_check] remove failed', err))
  }

  return (
    <div style={styles.row} className="nodrag" data-testid="where-chips">
      {chips.map(chip => (
        <button
          key={chip.index}
          type="button"
          style={styles.chip}
          title="where= filter — click: histogram of this variable, coloured by which locations match. Right-click for more."
          onClick={e => { e.stopPropagation(); openCheck(target(chip, null)) }}
          onContextMenu={e => {
            e.preventDefault()
            e.stopPropagation()
            setMenu({ chip, x: e.clientX, y: e.clientY })
          }}
        >
          {chip.text}
        </button>
      ))}
      {menu && (
        <>
          {/* A click anywhere else closes the menu. */}
          <div style={styles.scrim} onClick={() => setMenu(null)} onContextMenu={e => { e.preventDefault(); setMenu(null) }} />
          <div style={{ ...styles.menu, left: menu.x, top: menu.y }}>
            <button type="button" style={styles.menuItem} onClick={() => { openCheck(target(menu.chip, null)); setMenu(null) }}>
              Histogram of {menu.chip.variable}
            </button>
            {inputVariables.map(variable => (
              <button
                key={variable}
                type="button"
                style={styles.menuItem}
                onClick={() => { openCheck(target(menu.chip, variable)); setMenu(null) }}
              >
                Plot {variable} against {menu.chip.variable}
              </button>
            ))}
            <button type="button" style={styles.menuItem} onClick={() => setMenu(null)} title="Select the node: its Filters section edits this row">
              Edit (in the sidebar)
            </button>
            <button type="button" style={styles.menuItem} onClick={() => { remove(menu.chip); setMenu(null) }}>
              Remove
            </button>
          </div>
        </>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  row: { display: 'flex', flexWrap: 'wrap', gap: 4, margin: '4px 0' },
  chip: {
    fontSize: 10,
    fontFamily: 'monospace',
    padding: '1px 6px',
    borderRadius: 8,
    border: '1px solid #94a3b8',
    background: '#f1f5f9',
    color: '#334155',
    cursor: 'pointer',
  },
  scrim: { position: 'fixed', inset: 0, zIndex: 999 },
  menu: {
    position: 'fixed',
    zIndex: 1000,
    background: '#fff',
    border: '1px solid #cbd5e1',
    borderRadius: 6,
    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
    padding: 4,
    display: 'flex',
    flexDirection: 'column',
    minWidth: 180,
  },
  menuItem: {
    textAlign: 'left',
    fontSize: 12,
    padding: '4px 8px',
    border: 'none',
    background: 'transparent',
    cursor: 'pointer',
  },
}
