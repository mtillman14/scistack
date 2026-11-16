/**
 * A where= check, frontend side (docs/claude/where-in-gui.md).
 *
 * A check is an ordinary figure whose spec carries `where_check`: the node's
 * filters, each with a tick. The backend owns everything that matters (the
 * verdicts, the factors, the opening spec); this module only
 *   - builds the request that opens one (`whereCheckOpen`),
 *   - edits a filter row of the open spec (`editCheckFilter`), and
 *   - says which rows Apply writes back to the node (`rowsToApply`).
 *
 * Pure: `npm test` runs it (`whereCheck.test.ts`).
 */

import type { WhereCheck, WhereCheckFilter } from '../../generated/api-types.js'

/** One filter row of a node's stored `whereFilters`. */
export type StoredWhereFilter = {
  variable: string
  column?: string | null
  op: string
  value: string
}

/** What a chip click asks for. `against` null = histogram of the filter's own
 *  variable; a name = that input of the node plotted against it. */
export interface WhereCheckTarget {
  nodeId: string
  /** The function's name: the banner says what is being checked. */
  functionName: string
  filters: StoredWhereFilter[]
  filterIndex: number
  against: string | null
}

/** The window event a chip fires; PipelineDAG opens the studio. One funnel,
 *  like the data preview's event. */
export const OPEN_WHERE_CHECK_EVENT = 'scistack:open-where-check'

/** The tab's `where_check` request: `plot_describe`'s `where_check` and the
 *  variable it opens on. The variable is the filter's own (histogram) or the
 *  node's input (plot against). */
export function whereCheckOpen(target: WhereCheckTarget): {
  variable: string
  where_check: Record<string, unknown>
} {
  const focus = target.filters[target.filterIndex]
  return {
    variable: target.against ?? focus.variable,
    where_check: {
      node_id: target.nodeId,
      function_name: target.functionName,
      filters: target.filters.map(f => ({ ...f, enabled: true })),
      filter_index: target.filterIndex,
      against: target.against !== null,
    },
  }
}

/** The spec's check after one row's op / value / tick is edited. A spec with
 *  no check, or an index out of range, is returned unchanged. */
export function editCheckFilter<S extends { where_check?: WhereCheck | null }>(
  spec: S,
  index: number,
  patch: Partial<Pick<WhereCheckFilter, 'op' | 'value' | 'enabled'>>,
): S {
  const check = spec.where_check
  const rows = check?.filters
  if (!check || !rows || index < 0 || index >= rows.length) return spec
  return {
    ...spec,
    where_check: { ...check, filters: rows.map((row, i) => (i === index ? { ...row, ...patch } : row)) },
  }
}

/** The rows Apply writes to the node: the ticked ones, as stored rows (no
 *  tick). An unticked filter was only being explored; writing it back would
 *  filter the node by something the figure was not counting. */
export function rowsToApply(check: WhereCheck | null | undefined): StoredWhereFilter[] {
  return (check?.filters ?? [])
    .filter(row => row.enabled !== false)
    .map(row => ({
      variable: row.variable,
      column: row.column ?? null,
      op: row.op ?? '==',
      value: row.value ?? '',
    }))
}

