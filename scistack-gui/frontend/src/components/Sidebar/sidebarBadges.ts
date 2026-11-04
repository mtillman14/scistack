/**
 * Pure (React-free): which of a function node's settings differ from their
 * defaults, as badges for the sidebar (.claude/plan-palette-order-and-delete.md,
 * step `non_default_badges`). Run under `npm test` (tsconfig.test.json).
 *
 * "Default" is never decided here. Run-option defaults come from the backend
 * (`get_run_option_defaults`, read from scidb.for_each's signature —
 * scistack_gui/domain/run_options.py), and "the whole location tree" is
 * `isInert` from locationSelection.ts. This module only compares.
 *
 * One list feeds both places a badge shows — the chip beside the section
 * title and the summary in PanelHeader — so the two cannot disagree.
 */

import { isInert, type LocationSelection } from '../PlotStudio/locationSelection.js'

/** `get_run_option_defaults()`. */
export interface RunOptionDefaults {
  dry_run: boolean
  save: boolean
  distribute: boolean
  as_table: boolean
  on_error: string
  on_duplicate: string
}

/** A node's stored run options; every field optional (unset = default). */
export interface RunOptionsLike {
  dry_run?: boolean
  save?: boolean
  distribute?: boolean
  as_table?: boolean
  on_error?: string
  on_duplicate?: string
}

export type BadgeSection = 'variants' | 'inputs' | 'filters' | 'schema' | 'run_options'

/** `warn`: changes what gets saved (amber). `info`: everything else. */
export type BadgeTone = 'warn' | 'info'

export interface Badge {
  section: BadgeSection
  label: string
  tone: BadgeTone
  /** One sentence for the tooltip. */
  title: string
}

/** The per-node inputs this module reads; the panel passes them through. */
export interface FunctionBadgeInput {
  runOptions: RunOptionsLike | null | undefined
  defaults: RunOptionDefaults | null
  hiddenComboCount: number
  whereFilterCount: number
  schemaSelection: LocationSelection | null | undefined
  columnSelections: Readonly<Record<string, { columns: string[]; iterate: boolean; groups?: unknown[] } | undefined>>
}

/** Section display order: the order the sections appear in the panel. */
export const SECTION_ORDER: readonly BadgeSection[] = ['variants', 'inputs', 'filters', 'schema', 'run_options']

/**
 * Badges for one run-option blob. An unset option IS its default, so an
 * empty blob gives no badges — which is also how the stored options and the
 * served defaults are kept from disagreeing.
 */
export function runOptionBadges(opts: RunOptionsLike | null | undefined, defaults: RunOptionDefaults | null): Badge[] {
  if (!defaults) return []
  const o = opts ?? {}
  const value = <K extends keyof RunOptionDefaults>(k: K): RunOptionDefaults[K] =>
    (o[k] ?? defaults[k]) as RunOptionDefaults[K]
  const out: Badge[] = []
  const add = (label: string, tone: BadgeTone, title: string) =>
    out.push({ section: 'run_options', label, tone, title })

  if (value('dry_run') !== defaults.dry_run) {
    add(value('dry_run') ? 'Dry run' : 'Not a dry run', 'warn',
      value('dry_run') ? 'Dry run is on: a Run previews and executes nothing.' : 'Dry run is off.')
  }
  if (value('save') !== defaults.save) {
    add(value('save') ? 'Saving' : 'Not saving', 'warn',
      value('save') ? 'Results are saved.' : 'Save results is off: outputs are not written to the database.')
  }
  if (value('distribute') !== defaults.distribute) {
    add('Distribute', 'info', 'Outputs are saved at a lower schema level.')
  }
  if (value('as_table') !== defaults.as_table) {
    add('As table', 'info', 'Inputs keep their schema columns in DataFrames.')
  }
  if (value('on_error') !== defaults.on_error) {
    add(value('on_error') === 'stop' ? 'Stop on error' : `On error: ${value('on_error')}`, 'warn',
      'The first failing combo stops the run (combos already done are saved).')
  }
  if (value('on_duplicate') !== defaults.on_duplicate) {
    add(value('on_duplicate') === 'combine' ? 'Combine duplicates' : `Duplicates: ${value('on_duplicate')}`, 'info',
      'Rows at the same location become one record.')
  }
  return out
}

/** Every badge for a function node, in panel section order. */
export function functionBadges(input: FunctionBadgeInput): Badge[] {
  const out: Badge[] = []
  if (input.hiddenComboCount > 0) {
    out.push({
      section: 'variants',
      label: `${input.hiddenComboCount} hidden`,
      tone: 'info',
      title: `${input.hiddenComboCount} variant combo(s) are hidden and excluded from runs.`,
    })
  }
  const selections = Object.values(input.columnSelections ?? {}).filter(
    (s): s is { columns: string[]; iterate: boolean; groups?: unknown[] } => !!s,
  )
  if (selections.some(s => s.iterate)) {
    const grouped = selections.some(s => s.iterate && (s.groups?.length ?? 0) > 0)
    out.push({
      section: 'inputs',
      label: grouped ? 'Per group' : 'Per column',
      tone: 'info',
      title: grouped ? 'The function runs once per column group.' : 'The function runs once per column.',
    })
  } else if (selections.some(s => s.columns.length > 0)) {
    out.push({ section: 'inputs', label: 'Columns picked', tone: 'info', title: 'Only some columns of an input are passed.' })
  }
  if (input.whereFilterCount > 0) {
    out.push({
      section: 'filters',
      label: input.whereFilterCount === 1 ? '1 filter' : `${input.whereFilterCount} filters`,
      tone: 'info',
      title: 'Where-filters narrow which records are processed.',
    })
  }
  if (input.schemaSelection && !isInert(input.schemaSelection)) {
    out.push({ section: 'schema', label: 'Subset', tone: 'info', title: 'Runs at only some schema locations.' })
  }
  out.push(...runOptionBadges(input.runOptions, input.defaults))
  return out
}

/** The badges of one section (for the chip beside its title). */
export function badgesFor(badges: readonly Badge[], section: BadgeSection): Badge[] {
  return badges.filter(b => b.section === section)
}
