/**
 * Per-panel overrides in Plot Studio (Appearance > Panels): pure logic, no
 * React, so `npm test` can run it. docs/claude/per-panel-overrides.md.
 *
 * The panel DISPLAYS what Python decided (`layout.meta.panel_overrides`,
 * built by `render.base.panel_override_meta`): which panels exist, each one's
 * `match` (its facet values already turned into text by the backend), the
 * title and range drawn, and the override that applied. This module only
 * edits the spec's `panel_overrides` list. It never works out which override
 * applies to a panel — `scistackplot.panels.override_for` does — and never
 * turns a facet value into text itself.
 */

/** `scistackplot.panels.PanelOverride`. Absent/null fields inherit. */
export interface PanelOverride {
  match: Record<string, string>
  y_minimum?: number | null
  y_maximum?: number | null
  y_label?: string | null
  /** true hides, false forces on against the grid toggle, null follows it. */
  y_label_hidden?: boolean | null
}

/** One faceted panel as the backend reports it. */
export interface MetaPanel {
  match: Record<string, string>
  display_title: string
  /** The title drawn; "" when hidden. */
  y_title: string
  grid_row: number
  grid_col: number
  y_limits: [number, number] | null
  override: PanelOverride | null
}

export interface PanelOverrideMeta {
  panels: MetaPanel[]
  /** Overrides matching no panel of this figure (kept, inert). */
  unmatched: PanelOverride[]
  y_titles: YTitles
  /** False once the panels have different ranges: every panel shows its numbers. */
  shares_y: boolean
}

export type YTitles = 'every_panel' | 'first_column'

/** The per-panel Show control: follow the grid toggle, or force either way. */
export type ShowState = 'follow' | 'show' | 'hide'

type Patch = Partial<Omit<PanelOverride, 'match'>>

const FIELDS = ['y_minimum', 'y_maximum', 'y_label', 'y_label_hidden'] as const

/** A stable identity for a match, independent of key order (select values). */
export function matchId(match: Record<string, string>): string {
  return JSON.stringify(Object.keys(match).sort().map(name => [name, match[name]]))
}

export function sameMatch(a: Record<string, string>, b: Record<string, string>): boolean {
  return matchId(a) === matchId(b)
}

/** Whether every field inherits (the backend skips such an entry). */
export function isEmptyOverride(override: PanelOverride): boolean {
  return FIELDS.every(field => override[field] === null || override[field] === undefined)
}

/**
 * The list with the entry for `match` patched. The LAST entry with this match
 * is edited, as the backend's "last one wins"; other duplicates are dropped so
 * the edit is what the figure shows. An entry left with nothing set is removed
 * (a setting, not data). A patch for a panel with no entry appends one.
 */
export function upsertOverride(
  list: PanelOverride[] | undefined,
  match: Record<string, string>,
  patch: Patch,
): PanelOverride[] {
  const current = list ?? []
  const last = [...current].reverse().find(entry => sameMatch(entry.match, match))
  const merged: PanelOverride = { ...(last ?? { match: { ...match } }) }
  for (const field of FIELDS) {
    if (!(field in patch)) continue
    const value = patch[field]
    if (value === null || value === undefined) delete merged[field]
    else (merged as unknown as Record<string, unknown>)[field] = value
  }
  const others = current.filter(entry => !sameMatch(entry.match, match))
  return isEmptyOverride(merged) ? others : [...others, merged]
}

/** Every entry for `match` removed ("Clear" for one panel). */
export function clearOverride(
  list: PanelOverride[] | undefined,
  match: Record<string, string>,
): PanelOverride[] {
  return (list ?? []).filter(entry => !sameMatch(entry.match, match))
}

/** The spec's entry for a panel (the last with that match), or null. */
export function overrideFor(
  list: PanelOverride[] | undefined,
  match: Record<string, string>,
): PanelOverride | null {
  return [...(list ?? [])].reverse().find(entry => sameMatch(entry.match, match)) ?? null
}

export function showState(override: PanelOverride | null): ShowState {
  const hidden = override?.y_label_hidden
  if (hidden === true) return 'hide'
  if (hidden === false) return 'show'
  return 'follow'
}

export function hiddenFor(state: ShowState): boolean | null {
  return state === 'hide' ? true : state === 'show' ? false : null
}

/** The Panels section is offered only for a grid of faceted panels. */
export function offersPanels(meta: PanelOverrideMeta | undefined): boolean {
  return (meta?.panels.length ?? 0) > 1
}

/** Non-empty overrides in the spec, for the Appearance summary. */
export function overrideCount(list: PanelOverride[] | undefined): number {
  return (list ?? []).filter(entry => !isEmptyOverride(entry)).length
}

/** "RQUAD", or "HAM · L" for a two-factor panel — an unmatched entry's name. */
export function matchLabel(match: Record<string, string>): string {
  return Object.values(match).join(' · ')
}
