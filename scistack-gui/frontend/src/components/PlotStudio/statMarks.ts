/**
 * Test results on the figure (plan-stats-on-figure.md): the pure half.
 *
 * The figure OWNS the request (`spec.stat_marks`); Python owns the facts
 * (`scistackstats.marks.marks_for`) and the words (`scistackstats.format`).
 * This file only (a) edits the request, (b) decides WHEN the facts must be
 * fetched again: a test is re-run only when a design field, the request or
 * the data changes, never for a colour or a size. Which fields are design is
 * Python's list (`design_fields` of `plot_describe`); nothing is listed here.
 *
 * Pure (no React): tested under node --test.
 */

import type { Derived, PlotSpec, StatMarks } from '../../generated/api-types.js'

export type { Derived, StatMarks }

/** What `stats_marks` answers (`scistackstats.marks.Marks.to_dict`). */
export interface MarksResult extends Derived {
  /** Comparison id per bar, aligned with `bars` (what `hidden` holds). */
  ids: string[]
  reason: string | null
  notes: string[]
  /** What ran, in a phrase ("Paired t-tests, Holm-adjusted"). */
  test: string | null
}

/** Whether the request asks for anything at all. */
export function wanted(marks: StatMarks | undefined | null): boolean {
  return !!(marks && (marks.brackets || marks.omnibus || marks.caption))
}

export function setStatMarks(spec: PlotSpec, patch: Partial<StatMarks>): PlotSpec {
  return { ...spec, stat_marks: { ...(spec.stat_marks ?? {}), ...patch } }
}

/** Hide (or show again) one comparison's bracket by its comparison id. */
export function toggleHiddenBracket(spec: PlotSpec, comparisonId: string): PlotSpec {
  const hidden = new Set(spec.stat_marks?.hidden ?? [])
  if (hidden.has(comparisonId)) hidden.delete(comparisonId)
  else hidden.add(comparisonId)
  return setStatMarks(spec, { hidden: [...hidden] })
}

function pick(spec: Record<string, unknown>, path: string): unknown {
  let at: unknown = spec
  for (const part of path.split('.')) {
    if (at === null || typeof at !== 'object') return undefined
    at = (at as Record<string, unknown>)[part]
  }
  return at
}

/** The request key: the design fields, the `stat_marks` request and the
 *  source. `null` when nothing is asked for. A display edit leaves it alone,
 *  so no test is re-run for it. */
export function marksKey(
  spec: PlotSpec | null,
  designFields: string[] | undefined,
  csvPath?: string,
  settings?: Record<string, unknown> | null,
): string | null {
  if (!spec || !wanted(spec.stat_marks)) return null
  const design: Record<string, unknown> = {}
  const asRecord = spec as unknown as Record<string, unknown>
  // Without Python's list the whole spec is the key: more re-runs, never stale.
  for (const path of designFields ?? Object.keys(asRecord)) design[path] = pick(asRecord, path)
  return JSON.stringify({ design, marks: spec.stat_marks, csvPath: csvPath ?? null, settings: settings ?? null })
}

/** The test settings of the table (everything but its design copy and the
 *  wizard position), or null. The tab's `StatsSpec` owns them; the figure
 *  only forwards them so brackets use the test the table shows. */
export function testSettings(stats: Record<string, unknown> | null | undefined): Record<string, unknown> | null {
  if (!stats) return null
  const { design: _design, guide: _guide, ...rest } = stats
  void _design; void _guide
  return rest
}

/** `Derived` to send with a request, or null when it would add nothing. */
export function derivedFrom(result: MarksResult | null): Derived | null {
  if (!result) return null
  const { bars = [], panel_notes = [], caption = null } = result
  if (!bars.length && !panel_notes.length && !caption) return null
  return { bars, panel_notes, caption }
}
