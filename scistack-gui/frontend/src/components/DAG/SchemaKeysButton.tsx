/**
 * "Schema keys…" in the header bar above the canvas, beside "Clear data…".
 * Opens SchemaKeysDialog with no node: change the keys of the whole project
 * without waiting for a run to collide (the remedy popup's route). The rules
 * (what may change, and renaming every reference) live in scidb; see
 * docs/claude/changing-schema-keys.md.
 * Closing after a change refreshes the canvas (node levels may have been renamed).
 */

import { useState, type CSSProperties } from 'react'
import { useScope } from '../../context/ScopeContext'
import SchemaKeysDialog from '../SchemaKeysDialog'

export default function SchemaKeysButton() {
  const { bumpGraph } = useScope()
  const [open, setOpen] = useState(false)
  return (
    <>
      <button
        style={style}
        onClick={() => {
          // eslint-disable-next-line no-console
          console.debug('[schema-keys] header button opened the dialog')
          setOpen(true)
        }}
        type="button"
        title="Add, rename, remove or reorder the project's schema keys"
      >
        Schema keys…
      </button>
      {open && (
        <SchemaKeysDialog
          onClose={() => setOpen(false)}
          onApplied={() => {
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
  color: '#ddd',
  border: '1px solid #444',
  borderRadius: 4,
  cursor: 'pointer',
  fontSize: 12,
  fontFamily: 'inherit',
}
