/**
 * ↶ / ↷ for one surface's undo stack, plus the notice when a step could not
 * be applied (docs/claude/undo-redo.md). Colours inherit from the toolbar it
 * sits in, so the same component works on the dark canvas and in either
 * Plot Studio theme.
 */

import type { CSSProperties } from 'react'
import { useUndoSnapshot } from '../hooks/useUndo'
import type { UndoStack } from '../undo'

const isMac = typeof navigator !== 'undefined' && /Mac|iPhone|iPad/.test(navigator.platform)
const UNDO_KEY = isMac ? '⌘Z' : 'Ctrl+Z'
const REDO_KEY = isMac ? '⇧⌘Z' : 'Ctrl+Shift+Z'

export default function UndoButtons({ stack, style }: { stack: UndoStack; style?: CSSProperties }) {
  const snap = useUndoSnapshot(stack)
  return (
    <span style={{ ...styles.wrap, ...style }}>
      <button
        type="button"
        style={{ ...styles.btn, opacity: snap.canUndo && !snap.busy ? 1 : 0.4 }}
        disabled={!snap.canUndo || snap.busy}
        onClick={() => { void stack.undo() }}
        title={snap.undoLabel ? `Undo: ${snap.undoLabel} (${UNDO_KEY})` : `Nothing to undo (${UNDO_KEY})`}
        aria-label="Undo"
      >
        ↶
      </button>
      <button
        type="button"
        style={{ ...styles.btn, opacity: snap.canRedo && !snap.busy ? 1 : 0.4 }}
        disabled={!snap.canRedo || snap.busy}
        onClick={() => { void stack.redo() }}
        title={snap.redoLabel ? `Redo: ${snap.redoLabel} (${REDO_KEY})` : `Nothing to redo (${REDO_KEY})`}
        aria-label="Redo"
      >
        ↷
      </button>
      {snap.notice && <span style={styles.notice} role="status">{snap.notice}</span>}
    </span>
  )
}

const styles: Record<string, CSSProperties> = {
  wrap: { display: 'inline-flex', alignItems: 'center', gap: 4 },
  btn: {
    background: 'transparent',
    color: 'inherit',
    border: '1px solid currentColor',
    borderRadius: 4,
    padding: '1px 8px',
    fontSize: 14,
    lineHeight: '18px',
    cursor: 'pointer',
  },
  notice: { fontSize: 11, maxWidth: 420, color: '#f59e0b', whiteSpace: 'normal' },
}
