/**
 * Which studio opened, and from which way in (docs/claude/studio-unification.md,
 * Stage 0). Plot Studio and StatStudio have the same entry points; this logs
 * each opening once to scistack.log (`[Studio] open kind=… from=…`) so we can
 * see which ones are used.
 *
 * Pure: the caller hands in its `callBackend`, so this runs under node --test.
 */

export type StudioKind = 'plot' | 'stat'

/** Every way into a studio. One list, used by every caller and by the host. */
export const STUDIO_ENTRY_POINTS = [
  'variable-sidebar',  // the variable's settings panel: Open Plot Studio → / Open Stat Studio →
  'variable-menu',     // right-click a variable: 📈 Plot / 📊 Statistics
  'location',          // 🗂 View Schema Locations, then a row
  'plot-node',         // right-click a plot_ node
  'stat-node',         // right-click a stat_ node
  'palette',           // Command Palette
  'csv',               // Explorer: Plot CSV File
  'plot-studio',       // Plot Studio's StatStudio button
  'saved',             // a saved plot or table, opened in its studio
] as const

export type StudioEntryPoint = typeof STUDIO_ENTRY_POINTS[number]

export function entryPoint(value: unknown): StudioEntryPoint | 'unknown' {
  return (STUDIO_ENTRY_POINTS as readonly string[]).includes(value as string)
    ? (value as StudioEntryPoint)
    : 'unknown'
}

export interface StudioOpenParams {
  kind: StudioKind
  opened_from: StudioEntryPoint | 'unknown'
  variable: string | null
}

export function studioOpenParams(kind: StudioKind, from: unknown, variable?: string | null): StudioOpenParams {
  return { kind, opened_from: entryPoint(from), variable: variable || null }
}

/** Fire-and-forget: a failed log line never stops a studio opening. */
export function reportStudioOpen(
  call: (method: string, params: Record<string, unknown>) => Promise<unknown>,
  kind: StudioKind,
  from: unknown,
  variable?: string | null,
): void {
  const params = studioOpenParams(kind, from, variable)
  console.info('[Studio] open', params)
  call('report_studio_open', { ...params }).catch(() => undefined)
}
