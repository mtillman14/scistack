/**
 * "+ argument": the arguments a function node does not draw yet.
 *
 * The backend decides which ports a node draws (graph_builder.port_visibility:
 * required arguments and whatever feeds them) and sends the rest as
 * `optional_ports`. This lists them with their defaults and a filter box, so
 * one argument of a many-argument function (pandas.read_csv's `sep`) can be
 * picked out. Two callers, one list:
 *
 *  - FunctionNode, inline under the node: choosing reveals the port
 *    (set_shown_argument), then the user drags onto it.
 *  - PipelineDAG, at the pointer, after a connection was dropped on the
 *    "+ argument" control: choosing creates the edge directly.
 *
 * Filtering/labels are pure (inputPorts.ts, node --test).
 */

import { useEffect, useRef, useState } from 'react'
import type { OptionalPort } from './inputPorts'
import { filterOptionalPorts, optionalPortLabel } from './inputPorts'

interface Props {
  ports: OptionalPort[]
  onChoose: (name: string) => void
  onClose: () => void
  /** Shown above the list, e.g. "Connect to which argument?". */
  title?: string
  style?: React.CSSProperties
}

export function OptionalArgumentPicker({ ports, onChoose, onClose, title, style }: Props) {
  const [query, setQuery] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  const shown = filterOptionalPorts(ports, query)

  useEffect(() => { inputRef.current?.focus() }, [])

  return (
    // nodrag/nopan/nowheel: typing, clicking and scrolling the list must not
    // move the node or the canvas.
    <div className="nodrag nopan nowheel" style={{ ...styles.panel, ...style }}
      onClick={e => e.stopPropagation()}
      onDoubleClick={e => e.stopPropagation()}
    >
      {title && <div style={styles.title}>{title}</div>}
      <input
        ref={inputRef}
        style={styles.input}
        placeholder={`Filter ${ports.length} argument${ports.length === 1 ? '' : 's'}…`}
        value={query}
        onChange={e => setQuery(e.target.value)}
        onKeyDown={e => {
          if (e.key === 'Escape') onClose()
          if (e.key === 'Enter' && shown.length > 0) onChoose(shown[0].name)
        }}
      />
      <div style={styles.list}>
        {shown.length === 0
          ? <div style={styles.empty}>No argument matches</div>
          : shown.map(p => (
              <button
                key={p.name}
                type="button"
                style={styles.row}
                title={optionalPortLabel(p)}
                onClick={() => onChoose(p.name)}
              >
                <span style={styles.name}>{p.name}</span>
                {p.default && <span style={styles.default}>= {p.default}</span>}
              </button>
            ))}
      </div>
      <button type="button" style={styles.close} onClick={onClose}>Close</button>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  panel: {
    background: '#ffffff',
    border: '1px solid #7b68ee',
    borderRadius: 6,
    padding: 6,
    width: 220,
    boxShadow: '0 4px 12px rgba(0,0,0,0.18)',
    fontSize: 11,
    zIndex: 10,
  },
  title: { fontWeight: 600, color: '#3a1a8e', marginBottom: 4 },
  input: {
    width: '100%',
    boxSizing: 'border-box',
    fontSize: 11,
    padding: '3px 5px',
    marginBottom: 4,
    border: '1px solid #c4c4d8',
    borderRadius: 4,
  },
  list: { maxHeight: 180, overflowY: 'auto', display: 'flex', flexDirection: 'column' },
  row: {
    display: 'flex',
    gap: 6,
    alignItems: 'baseline',
    textAlign: 'left',
    background: 'none',
    border: 'none',
    padding: '2px 4px',
    cursor: 'pointer',
    fontFamily: 'monospace',
    fontSize: 11,
  },
  name: { color: '#3a1a8e' },
  default: {
    color: '#6b6b8f',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  empty: { color: '#6b6b8f', padding: '2px 4px' },
  close: {
    marginTop: 4,
    fontSize: 10,
    background: 'none',
    border: 'none',
    color: '#6b6b8f',
    cursor: 'pointer',
    float: 'right',
  },
}
