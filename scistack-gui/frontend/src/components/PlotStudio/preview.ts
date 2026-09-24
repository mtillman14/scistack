/**
 * The preview's size for the label and legend decisions
 * (`plot_service._render_preview`, `scistackplot.layout_decisions`).
 *
 * `export`: decided at the figure's own Width x Height and drawn at that
 * size (1 pt = 1 px, the font-size convention), so the preview is what Save
 * writes. `pane`: decided at the pane's size, filling it; the Figure size
 * section then says which export size would reproduce the view.
 */

export type PreviewMode = 'export' | 'pane'

export interface PreviewRequest {
  mode: PreviewMode
  width_px?: number
  height_px?: number
}

/** What `layout.meta.preview` says the figure was decided at. */
export interface PreviewMeta {
  mode: PreviewMode
  width_in: number
  height_in: number
}

/** Pane sizes are rounded to this, so a drag re-renders every 20 px rather
 *  than every pixel (each re-render is a matplotlib layout server-side). */
export const PANE_BUCKET_PX = 20

/** The smallest pane size worth deciding at; below it the panel is still
 *  being laid out and the numbers mean nothing. */
export const MIN_PANE_PX = 120

/** 1 pt = 1 px: a pane of N px is N / 72 inches of saved figure. */
export const PX_PER_IN = 72

function bucket(px: number): number {
  return Math.max(MIN_PANE_PX, Math.round(px / PANE_BUCKET_PX) * PANE_BUCKET_PX)
}

/** The `preview` parameter for `plot_resolve`. */
export function previewRequest(mode: PreviewMode, paneWidth: number, paneHeight: number): PreviewRequest {
  if (mode === 'export') return { mode }
  return { mode, width_px: bucket(paneWidth), height_px: bucket(paneHeight) }
}

/** Two requests with the same key produce the same decisions. */
export function previewKey(request: PreviewRequest): string {
  return request.mode === 'export'
    ? 'export'
    : `pane:${request.width_px}x${request.height_px}`
}

/** The export size (inches, 2 dp) a pane-fitted view corresponds to. */
export function paneExportSize(meta: PreviewMeta | undefined): { width: number; height: number } | null {
  if (!meta || meta.mode !== 'pane') return null
  return { width: Math.round(meta.width_in * 100) / 100, height: Math.round(meta.height_in * 100) / 100 }
}
