/**
 * Pure helpers for the sidebar's Delete dialog (DeleteEntityDialog.tsx).
 *
 * The backend owns what a Delete means (`entity_delete_service.delete_entity_plan`);
 * these only turn its plan into the dialog's request and text, so the
 * component stays thin and the wording is unit-tested.
 * See docs/claude/deleting-entries-with-data.md.
 */

export type DeleteKind = 'variable' | 'parameter' | 'path_input' | 'function' | 'submodule'

export interface ValueOption {
  value: unknown
  declared: boolean
  has_data: boolean
}

export interface DeletePlanData {
  total_records: number
  by_variable: Record<string, number>
  seed_by_variable?: Record<string, number>
  downstream_by_variable?: Record<string, number>
  lost_locations?: Record<string, unknown[]>
  fingerprint: string | null
  warnings?: string[]
  undoable: boolean
}

export interface Placement {
  pipeline_id: string
  pipeline_name: string
  hidden_pipeline: boolean
  node_ids: string[]
}

export interface DeletePlan {
  ok: boolean
  error?: string
  reason?: string
  kind?: DeleteKind
  name?: string
  label?: string
  data?: DeletePlanData
  declaration?: { action: string; message?: string; file?: string; removed?: string }
  placements?: Placement[]
  blocked?: string[]
  value_options?: ValueOption[]
}

export const KIND_LABEL: Record<DeleteKind, string> = {
  variable: 'Variable',
  parameter: 'Parameter',
  path_input: 'Path Input',
  function: 'Function',
  submodule: 'Submodule',
}

/** The `values` to send: null when every option is ticked (the whole
 *  Parameter, declaration and node included), else the ticked values. */
export function valuesForRequest(options: ValueOption[], ticked: ReadonlySet<number>): unknown[] | null {
  if (options.length === 0 || ticked.size === options.length) return null
  return options.filter((_, i) => ticked.has(i)).map(o => o.value)
}

export function formatValue(value: unknown): string {
  return typeof value === 'string' ? value : JSON.stringify(value)
}

/** One line per variable: "FilteredSignal — 4 records (2 downstream)". */
export function dataLines(data: DeletePlanData): string[] {
  return Object.entries(data.by_variable).map(([variable, n]) => {
    const down = data.downstream_by_variable?.[variable] ?? 0
    const lost = data.lost_locations?.[variable]?.length ?? 0
    const parts = [`${n} record${n === 1 ? '' : 's'}`]
    if (down) parts.push(`${down === n ? 'all' : down} computed downstream`)
    if (lost) parts.push(`${lost} location${lost === 1 ? '' : 's'} left with none`)
    return `${variable} — ${parts.join(', ')}`
  })
}

/** What Ctrl/Cmd+Z will do, said plainly. */
export function undoLine(data: DeletePlanData | undefined): string {
  if (!data || data.total_records === 0) return 'Undo with Ctrl/Cmd+Z.'
  if (!data.undoable) {
    return 'This database has no file, so the deleted data CANNOT be brought back. '
      + 'Ctrl/Cmd+Z restores everything else.'
  }
  return 'Ctrl/Cmd+Z restores everything, the data included, while it is in the undo '
    + 'history (the last 500 changes). After that the data is gone for good; re-running '
    + 'recomputes it.'
}
