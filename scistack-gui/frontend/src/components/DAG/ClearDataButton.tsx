/**
 * "Clear data…" in the header bar above the canvas. It clears the whole
 * project, not one scope or tab, so it does not live on the canvas.
 * Closing the dialog refreshes the canvas, whose nodes then show no records.
 */

import { useState, type CSSProperties } from 'react'
import { useScope } from '../../context/ScopeContext'
import ClearDataDialog from './ClearDataDialog'

export default function ClearDataButton() {
  const { bumpGraph } = useScope()
  const [open, setOpen] = useState(false)
  return (
    <>
      <button
        style={style}
        onClick={() => setOpen(true)}
        type="button"
        title="Delete every record and its run history; keep every canvas (nodes, edges, values)"
      >
        Clear data…
      </button>
      {open && (
        <ClearDataDialog
          onClose={() => {
            setOpen(false)
            bumpGraph()
          }}
        />
      )}
    </>
  )
}

const style: CSSProperties = {
  padding: '4px 12px',
  background: '#2a2a4a',
  color: '#e0a0a0',
  border: '1px solid #5a3a3a',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
  fontFamily: 'inherit',
}
