/**
 * The data preview's shapes and text. The content is the backend's
 * (`scidb.inspect.preview`, handler `get_data_preview`); nothing here decides
 * what a column holds.
 */

export interface ColumnPreview {
  name: string
  stored_type: string
  loaded_types: Record<string, number>
  non_null: number
  n_rows: number
  samples: string[]
}

export interface DataPreviewResult {
  ok: boolean
  error?: string
  variable?: string
  schema_keys?: string[]
  location?: Record<string, string>
  levels?: Record<string, string[]>
  n_records?: number
  n_rows?: number
  columns?: ColumnPreview[]
  rows?: Record<string, unknown>[]
  truncated?: boolean
  missing_columns?: string[]
  note?: string | null
}

/** Opens the popup from anywhere (PipelineDAG mounts it and listens). */
export const OPEN_DATA_PREVIEW_EVENT = 'scistack:open-data-preview'

export interface DataPreviewTarget {
  variable: string
  /** An input port's picked columns; none means every column. */
  columns?: string[]
  /** The input port it was opened from, for the title. */
  param?: string
}

export function openDataPreview(target: DataPreviewTarget): void {
  window.dispatchEvent(new CustomEvent(OPEN_DATA_PREVIEW_EVENT, { detail: target }))
}

/** `str x2, float x1`, or `none` when every cell is blank. */
export function describeLoadedTypes(types: Record<string, number>): string {
  const parts = Object.entries(types).map(([name, count]) => `${name} x${count}`)
  return parts.length ? parts.join(', ') : 'none'
}

/** A column whose cells are not all one type is what a function trips on. */
export function hasMixedTypes(column: ColumnPreview): boolean {
  return Object.keys(column.loaded_types).length > 1
}

export function cellText(value: unknown): string {
  return value === null || value === undefined ? '' : String(value)
}
