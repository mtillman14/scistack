/**
 * Pure (React-free) rules about where an entity is declared, so they can run
 * under `npm test` (tsconfig.test.json) and have one owner for every panel.
 */

export interface EntityEditability {
  editable: boolean
  reason: 'read_only' | 'unknown' | null
  file: string | null
  line: number | null
  message: string
}

/** `foo.py:42`, or just the file name when the line is unknown. */
export function formatLocation(at: { file: string; line: number | null }): string {
  // Either separator: declarations on Windows arrive as `Y:\...\foo.m`.
  const name = at.file.split(/[\\/]/).pop() || at.file
  return at.line ? `${name}:${at.line}` : name
}

/**
 * Whether a settings panel should grey out its edit controls.
 *
 * Only a declaration known to live OUTSIDE the entities file locks the
 * panel. `null` (answer not arrived yet, or the request failed) and
 * `"unknown"` (not registered) stay editable: the write path still refuses
 * those, and failing open means a slow backend never locks an entity the
 * user is allowed to edit.
 */
export function isLockedForEditing(e: EntityEditability | null): boolean {
  return e?.reason === 'read_only'
}
