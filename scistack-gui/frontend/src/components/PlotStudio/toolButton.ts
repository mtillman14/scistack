import type { CSSProperties } from 'react'

/** The one owner of how a Plot Studio toolbar button looks when it cannot be
 *  used. `styles.toolButton` sets its own background, colour and pointer
 *  cursor, so the browser's default disabled look never shows: a disabled
 *  "Save data (CSV)" looked exactly like an enabled one, and a click on it
 *  silently did nothing (2026-10-30). Every toolbar button that can be
 *  unavailable goes through here, so none can drift. */
export const UNAVAILABLE_LOOK: CSSProperties = {
  opacity: 0.45,
  cursor: 'not-allowed',
}

export function toolButtonStyle(base: CSSProperties, unavailable: boolean): CSSProperties {
  return unavailable ? { ...base, ...UNAVAILABLE_LOOK } : base
}

/** A click on a button that is unavailable FOR A REASON (not merely busy):
 *  the reason to show in the status line, or null when the click should
 *  proceed. The button is `aria-disabled` rather than `disabled` for exactly
 *  this — a `disabled` button never fires its click, so it could only ever
 *  explain itself in a hover tooltip. */
export function blockedClickNotice(
  available: boolean | undefined,
  reason: string | null | undefined,
  fallback = 'Not available for this plot',
): string | null {
  if (available) return null
  return reason || fallback
}
