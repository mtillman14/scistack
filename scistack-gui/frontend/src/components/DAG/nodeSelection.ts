/**
 * What "selected" looks like on the canvas, and keeping it across refreshes.
 *
 * The one owner of the selection colour: nodes and edges both take it from
 * here, so a selected node and a selected edge read as the same thing.
 *
 * The colour has to hold up on the dark canvas and on a light background, and
 * every other hue is already spoken for: each node type has its own border and
 * green / amber / red mean run state. A mid-tone violet is about equally far
 * from black and white, so it clears 3:1 on both (nodeSelection.test.ts).
 *
 * Function nodes have a violet border of their own. The ring therefore sits
 * apart from the node as an outline with a gap, so it stays visible there too.
 *
 * Pure: no React, no DOM. PipelineDAG injects the CSS and calls carrySelection.
 */

export const SELECTION_COLOR = '#8b5cf6'

/** The same colour with an alpha, for the glow and the hover ring. */
function selectionAlpha(alpha: number): string {
  const n = parseInt(SELECTION_COLOR.slice(1), 16)
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`
}

/**
 * Targets each node component's root div, so no node type has to know about
 * selection. `!important` on box-shadow is needed because those roots set their
 * resting shadow inline.
 */
export const SELECTION_CSS = `
  .react-flow__node > div {
    outline: 2px solid transparent;
    outline-offset: 3px;
    transition: outline-color 120ms ease, box-shadow 120ms ease;
  }
  .react-flow__node:hover > div {
    outline-color: ${selectionAlpha(0.45)};
  }
  .react-flow__node.selected > div {
    outline-color: ${SELECTION_COLOR};
    box-shadow: 0 0 16px 4px ${selectionAlpha(0.55)} !important;
  }
  .react-flow__node.dragging > div {
    box-shadow: 0 8px 20px 2px ${selectionAlpha(0.55)} !important;
  }
  .react-flow__edge.selected .react-flow__edge-path {
    stroke: ${SELECTION_COLOR};
    stroke-width: 3;
    filter: drop-shadow(0 0 4px ${selectionAlpha(0.6)});
  }
`

/**
 * A canvas refresh replaces the node list with the server's snapshot, which
 * knows nothing about selection. Without this, the highlight (and the Copy and
 * Extract buttons) would vanish on any refresh, including the one that follows
 * dragging a node, while the sidebar still showed that node.
 */
export function carrySelection<T extends { id: string; selected?: boolean }>(
  prev: readonly T[],
  next: T[],
): T[] {
  const selected = new Set(prev.filter(n => n.selected).map(n => n.id))
  if (selected.size === 0) return next
  return next.map(n => (selected.has(n.id) ? { ...n, selected: true } : n))
}
