/**
 * Click-and-drag horizontal resizing for a side panel.
 *
 * `useResizablePanel` holds one panel's width; `ResizeHandle` is the strip you
 * drag. The width rule (direction, clamping, the stored value) is
 * `panelResize.ts`'s — this file only wires it to pointers and storage.
 *
 * The handle is a SIBLING of the panel in its flex row, not a child: a child
 * strip would scroll away with a panel whose body scrolls, and a sibling with
 * negative margins overlaps both neighbours without taking any width.
 *
 * A panel that was never dragged keeps its own default sizing (`width` is
 * null and `widthStyle` is empty), so the canvas sidebar still opens at a
 * quarter of the window. Double-click the handle to go back to that default.
 * Widths are remembered per viewer in browser storage, which may be missing or
 * throw; every read and write is guarded.
 */

import { useCallback, useEffect, useRef, useState } from 'react'
import type React from 'react'

import {
  KEY_STEP_PX,
  clampWidth,
  dragWidth,
  maxWidth,
  parseStoredWidth,
  storageKey,
  type PanelEdge,
} from '../panelResize'

export interface ResizablePanelOptions {
  /** Stable name; the storage key and the log line both use it. */
  panel: string
  /** Which edge of the panel the handle sits on. */
  edge: PanelEdge
  /** Narrowest drag, px. */
  min: number
  /** Width left for the panel's neighbours in the same row, px. */
  reserve: number
}

export interface ResizablePanel {
  /** The dragged width, or null while the panel uses its default. */
  width: number | null
  /** Spread onto the panel's style AFTER its own width/flex. */
  widthStyle: React.CSSProperties
  /** Attach to the panel element; its parent is the row being shared. */
  panelRef: React.RefObject<HTMLDivElement>
  handleProps: ResizeHandleProps
}

function readStored(panel: string): number | null {
  try {
    return parseStoredWidth(window.localStorage.getItem(storageKey(panel)))
  } catch {
    return null
  }
}

function writeStored(panel: string, width: number | null) {
  try {
    if (width == null) window.localStorage.removeItem(storageKey(panel))
    else window.localStorage.setItem(storageKey(panel), String(width))
  } catch {
    // Not remembered; the panel still resizes.
  }
}

export function useResizablePanel({ panel, edge, min, reserve }: ResizablePanelOptions): ResizablePanel {
  const [width, setWidth] = useState<number | null>(() => readStored(panel))
  const panelRef = useRef<HTMLDivElement>(null)
  const widthRef = useRef(width)
  widthRef.current = width
  const drag = useRef<{ startX: number; startWidth: number; max: number } | null>(null)

  const limitsNow = useCallback(() => {
    const container = panelRef.current?.parentElement?.getBoundingClientRect().width ?? Infinity
    return { min, max: maxWidth(container, reserve) }
  }, [min, reserve])

  const currentWidth = useCallback(
    () => widthRef.current ?? panelRef.current?.getBoundingClientRect().width ?? min,
    [min],
  )

  const commit = useCallback(
    (from: number, to: number | null, why: string) => {
      writeStored(panel, to)
      console.info('[panel] resized', { panel, why, from, to: to ?? 'default' })
    },
    [panel],
  )

  const onPointerDown = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      if (e.button !== 0) return
      e.preventDefault()
      // Captured, so the drag keeps tracking over a plotly or React Flow
      // canvas that would otherwise swallow the moves.
      e.currentTarget.setPointerCapture(e.pointerId)
      drag.current = { startX: e.clientX, startWidth: currentWidth(), max: limitsNow().max }
      document.body.style.cursor = 'col-resize'
      document.body.style.userSelect = 'none'
    },
    [currentWidth, limitsNow],
  )

  const onPointerMove = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      const d = drag.current
      if (!d) return
      setWidth(dragWidth(d.startWidth, d.startX, e.clientX, edge, { min, max: d.max }))
    },
    [edge, min],
  )

  const endDrag = useCallback(
    (e: React.PointerEvent<HTMLDivElement>) => {
      const d = drag.current
      if (!d) return
      drag.current = null
      if (e.currentTarget.hasPointerCapture(e.pointerId)) e.currentTarget.releasePointerCapture(e.pointerId)
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      // A click without a move leaves nothing to remember.
      if (widthRef.current != null && widthRef.current !== d.startWidth) {
        commit(Math.round(d.startWidth), widthRef.current, 'drag')
      }
    },
    [commit],
  )

  const onDoubleClick = useCallback(() => {
    const from = Math.round(currentWidth())
    setWidth(null)
    commit(from, null, 'reset')
  }, [commit, currentWidth])

  const onKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLDivElement>) => {
      if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return
      e.preventDefault()
      const from = currentWidth()
      const towardPanel = (e.key === 'ArrowLeft') === (edge === 'left')
      const to = clampWidth(from + (towardPanel ? KEY_STEP_PX : -KEY_STEP_PX), limitsNow())
      setWidth(to)
      commit(Math.round(from), to, 'key')
    },
    [commit, currentWidth, edge, limitsNow],
  )

  // A drag interrupted by unmount must not leave the whole page's cursor stuck.
  useEffect(() => () => {
    if (drag.current) {
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
    }
  }, [])

  const widthStyle: React.CSSProperties =
    width == null
      ? {}
      : { width, flex: `0 0 ${width}px`, maxWidth: `calc(100% - ${reserve}px)`, minWidth: min }

  return {
    width,
    widthStyle,
    panelRef,
    handleProps: {
      onPointerDown,
      onPointerMove,
      onPointerUp: endDrag,
      onPointerCancel: endDrag,
      onDoubleClick,
      onKeyDown,
    },
  }
}

export interface ResizeHandleProps {
  onPointerDown: (e: React.PointerEvent<HTMLDivElement>) => void
  onPointerMove: (e: React.PointerEvent<HTMLDivElement>) => void
  onPointerUp: (e: React.PointerEvent<HTMLDivElement>) => void
  onPointerCancel: (e: React.PointerEvent<HTMLDivElement>) => void
  onDoubleClick: () => void
  onKeyDown: (e: React.KeyboardEvent<HTMLDivElement>) => void
}

export default function ResizeHandle(handlers: ResizeHandleProps) {
  const [hot, setHot] = useState(false)
  return (
    <div
      role="separator"
      aria-orientation="vertical"
      tabIndex={0}
      title="Drag to resize · double-click to reset"
      style={{ ...handleStyle, ...(hot ? handleHotStyle : null) }}
      onPointerEnter={() => setHot(true)}
      onPointerLeave={() => setHot(false)}
      onFocus={() => setHot(true)}
      onBlur={() => setHot(false)}
      {...handlers}
    />
  )
}

// Theme-neutral on purpose: the same handle sits on the dark canvas and on
// Plot Studio in either theme.
const handleStyle: React.CSSProperties = {
  flex: '0 0 6px',
  width: 6,
  margin: '0 -3px',
  position: 'relative',
  zIndex: 10,
  cursor: 'col-resize',
  touchAction: 'none',
  background: 'transparent',
  transition: 'background 120ms',
  outline: 'none',
}

const handleHotStyle: React.CSSProperties = {
  background: 'rgba(120, 140, 255, 0.45)',
}
