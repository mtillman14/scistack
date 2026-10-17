/**
 * The one owner of how a side panel's width follows a drag.
 *
 * Every horizontally resizable side panel in the GUI — the canvas sidebar, the
 * Plot Studio controls and Saved-plots rails, the DAG pickers' sidebar — reads
 * its width rule from here, through `components/ResizeHandle.tsx`. No panel
 * clamps or parses a width of its own, so they cannot drift apart.
 *
 * React-free, so `npm test` runs it.
 */

/** Which edge of the panel carries the handle. */
export type PanelEdge = 'left' | 'right'

export interface PanelLimits {
  /** The narrowest the panel may be dragged. */
  min: number
  /** The widest, from the container's width at drag time. */
  max: number
}

/** Keyboard step for ←/→ on a focused handle. */
export const KEY_STEP_PX = 16

export const STORAGE_PREFIX = 'scistack.panelWidth.'

/**
 * The width within `limits`. A container narrower than `min` leaves no room
 * to honour both, and the floor wins: a panel that cannot be read is worse
 * than one that crowds its neighbour.
 */
export function clampWidth(px: number, limits: PanelLimits): number {
  return Math.round(Math.max(limits.min, Math.min(px, limits.max)))
}

/**
 * The width after the pointer moved from `startX` to `x`. A handle on the
 * panel's LEFT edge (a panel docked right) grows the panel as it moves left.
 */
export function dragWidth(
  startWidth: number,
  startX: number,
  x: number,
  edge: PanelEdge,
  limits: PanelLimits,
): number {
  const delta = edge === 'left' ? startX - x : x - startX
  return clampWidth(startWidth + delta, limits)
}

/** The widest the panel may be, leaving `reserve` px for whatever it sits beside. */
export function maxWidth(containerWidth: number, reserve: number): number {
  return Math.max(0, containerWidth - reserve)
}

/**
 * A stored width, or null for "never resized" (the panel's own default).
 * Browser storage can hold anything, or nothing.
 */
export function parseStoredWidth(raw: string | null | undefined): number | null {
  if (raw == null || raw === '') return null
  const px = Number(raw)
  return Number.isFinite(px) && px > 0 ? Math.round(px) : null
}

export function storageKey(panel: string): string {
  return STORAGE_PREFIX + panel
}
