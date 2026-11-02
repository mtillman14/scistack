/**
 * "Not plotted": the toolbar button and popup's pure logic.
 *
 * Every reason, count and sentence is Python's (`scistackplot.omissions`,
 * the one owner): each figure carries its own report (`FigurePayload.
 * omissions`) and the response the fan-out's total (`not_plotted`, through
 * `omissions.combine`). This module only lays the items out as table rows.
 */

/** One left-out value (omissions.Omission.to_dict). */
export interface OmissionItem {
  keys: Record<string, string>
  variable: string
  reason: string
  label: string
  detail: string
}

/** One figure's report (omissions.OmissionReport.to_dict). */
export interface OmissionReport {
  drawn: number
  lost: number
  offered: number
  counts: Record<string, number>
  labels: Record<string, string>
  button: string
  sentence: string
  items: OmissionItem[]
  /** Items past the 5,000 sent (the CSV always has them all). */
  truncated: number
}

/** The response's total over the figures sent (plot_service._not_plotted_total). */
export interface NotPlottedTotal {
  button: string
  sentence: string
  lost: number
  offered: number
  counts: Record<string, number>
}

export interface NotPlottedRow {
  figure: string
  keys: Record<string, string>
  variable: string
  label: string
  detail: string
}

/** Every figure's items as table rows, in figure order, each tagged with its
 *  figure's label (empty for a plot with one figure). */
export function notPlottedRows(
  figures: { label: string; omissions?: OmissionReport | null }[],
): NotPlottedRow[] {
  const tag = figures.length > 1
  return figures.flatMap(figure =>
    (figure.omissions?.items ?? []).map(item => ({
      figure: tag ? figure.label : '',
      keys: item.keys,
      variable: item.variable,
      label: item.label,
      detail: item.detail,
    })),
  )
}

/** The key columns of the table: every key any row names, first-seen order. */
export function notPlottedColumns(rows: NotPlottedRow[]): string[] {
  const seen: string[] = []
  for (const row of rows) for (const key of Object.keys(row.keys)) if (!seen.includes(key)) seen.push(key)
  return seen
}

/** How many items the figures did not send (past each one's 5,000 cap). */
export function notPlottedTruncated(figures: { omissions?: OmissionReport | null }[]): number {
  return figures.reduce((n, figure) => n + (figure.omissions?.truncated ?? 0), 0)
}

/** The "Save data" depth key that writes the whole list
 *  (scistackplot.export.NOT_PLOTTED_DEPTH). */
export const NOT_PLOTTED_DEPTH = '__not_plotted'
