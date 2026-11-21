/**
 * Who asks for the data view, and with what filter (GitHub #28). The same
 * window-event pattern as `openDataPreview`: the header's Data button, a
 * node's reason chip, a variable's right-click and the Activity Bar tree all
 * call `openDataView`; App mounts the one popup.
 */

/**
 * The GUI entry points the data view replaces: 📋 Inventory, ✅ Coverage and
 * 🗂 View Schema Locations. Hidden, not deleted (Chesterton's fence); the
 * scidb code and CLI are unchanged. Expected locations has its own switch,
 * `SHOW_EXPECTED_LOCATIONS` (expectationTypes.ts).
 */
export const SHOW_LEGACY_DATA_ENTRIES = false

export interface DataViewRequest {
  /** Columns to show (pipeline order is the backend's); null = every variable. */
  variables?: string[] | null
  /** Show only rows where a column has one of these states. */
  states?: string[]
  /** The step whose chip was clicked, for the title. */
  functionName?: string | null
  /** Open "Clean up outdated…" straight away. */
  cleanup?: boolean
}

export const OPEN_DATA_VIEW_EVENT = 'scistack:open-data-view'

export function openDataView(req: DataViewRequest = {}): void {
  console.info('[DataView] open', req)
  window.dispatchEvent(new CustomEvent(OPEN_DATA_VIEW_EVENT, { detail: req }))
}

export interface TryRequest {
  nodeId: string
  functionName: string
  outputs: string[]
}

export const OPEN_TRY_EVENT = 'scistack:open-try'

/** "🧪 Try on…" from a function node's right-click. */
export function openTry(req: TryRequest): void {
  console.info('[Try] open', req)
  window.dispatchEvent(new CustomEvent(OPEN_TRY_EVENT, { detail: req }))
}
