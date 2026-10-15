/**
 * Who moves the figure cursor (the ◀ ▶ index) when a resolve comes back.
 *
 * The user owns the cursor. The backend may only CORRECT it: when a fan-out
 * shrank, it clamps a stale index and reports the one it actually rendered,
 * and the arrows must then agree with the figure on screen.
 *
 * That correction is only valid for the cursor the request was sent with. A
 * request answered after the user has clicked on is about a position they
 * have left, and adopting its index dragged the cursor back. Back-dragging
 * changed the index, which queued a resolve for the old figure, whose answer
 * dragged it forward again — two neighbouring figures alternating forever
 * under rapid clicking (2026-10-15). Hence the rule below.
 */

/** The index to adopt from a resolve answer, or null to leave the cursor alone.
 *
 *  `requested` is the index the request was sent with, `current` the cursor
 *  now, `rendered` what the backend reports it drew (absent on an error). */
export function adoptedFigureIndex(
  requested: number,
  current: number,
  rendered: number | null | undefined
): number | null {
  if (typeof rendered !== 'number') return null
  // The user moved on while this was running: their newer position wins, and
  // the resolve their move scheduled will answer for it.
  if (requested !== current) return null
  return rendered
}
