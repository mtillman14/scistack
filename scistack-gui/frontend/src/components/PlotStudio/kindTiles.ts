/**
 * The plot-type grid's tiles. Usually one per kind; a TEXT measure has two
 * per bar / heatmap / spaghetti — "Count" and "Proportion" are separate bar
 * graphs (user, 2026-11-01). Which tiles exist, which is selected and what a
 * click applies all come from the backend (`capability.categorical_kind_entries`);
 * this module only reads them, so the panel never re-derives the kind→stat
 * rule (`categorical.mode_for_kind`, CLAUDE.md NOTE 3).
 */

export type TileRole = 'group' | 'facet' | 'iterate' | 'collapse'

export interface KindTile {
  kind: string
  /** Distinguishes two tiles of one kind (`"bar:count"`); absent = the kind. */
  id?: string
  available: boolean
  /** The assignment the tile opens with, when the backend re-defaults it. */
  assignment?: { roles: Record<string, TileRole>; groups: string[]; color: string | null } | null
  /** The `spec.categorical` patch a text measure's tile applies. */
  categorical?: { stat: string } | null
  /** Whether the current spec IS this tile (backend-decided); absent = kind match. */
  selected?: boolean
}

export interface TileSpec {
  kind: string
  roles: Record<string, TileRole>
  groups?: string[]
  color?: string | null
  categorical?: { stat?: string; level?: string | null }
}

/** The React key and radio identity of a tile. */
export function tileKey(tile: KindTile): string {
  return tile.id ?? tile.kind
}

/** Whether ``tile`` is the selected radio for ``spec``. */
export function isTileSelected(tile: KindTile, spec: { kind: string } | null | undefined): boolean {
  if (!spec) return false
  return tile.selected ?? spec.kind === tile.kind
}

/**
 * ``spec`` after clicking ``tile``: its kind, its categorical stat, and — only
 * when ``useAssignment`` (the capability report describes the spec on screen)
 * — the assignment it brings. A stale suggestion must never overwrite a role
 * the user set since (`PlotStudio.setKind`).
 */
export function applyTile<S extends TileSpec>(spec: S, tile: KindTile, useAssignment: boolean): S {
  const suggested = useAssignment ? tile.assignment ?? null : null
  return {
    ...spec,
    kind: tile.kind,
    ...(tile.categorical
      ? { categorical: { ...(spec.categorical ?? {}), stat: tile.categorical.stat } }
      : {}),
    ...(suggested ? { roles: suggested.roles, groups: suggested.groups, color: suggested.color } : {}),
  }
}
