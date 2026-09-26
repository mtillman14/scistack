/**
 * Copy the previewed figure to the clipboard as a PNG — the pure half: how
 * large a bitmap to ask for, and decoding what Plotly returns. The Plotly and
 * clipboard calls live in `clipboardPng.ts`, which node cannot run.
 *
 * This copies the PREVIEW (Plotly, in the webview), not the matplotlib export:
 * it is instant, where "Save image" renders at full resolution in the
 * background. A downsampled preview is copied downsampled — the panel already
 * says so above the figure.
 *
 * The figure copied is the one the renderer sent (`figure.figure`), never the
 * dark-mode screen recolouring: a pasted figure lands on a white slide or page,
 * and light mode is the saved figure's paper.
 *
 * Resolution: the export-size preview is drawn at 1 pt = 1 px, so a figure is
 * 72 px per inch before scaling. `COPY_DPI / 72` makes the pasted bitmap 300
 * dpi at the figure's own size. Two ceilings keep a big facet grid from asking
 * for a canvas Chromium refuses (16384 px a side) or a PNG too large to paste
 * comfortably. Chromium re-encodes clipboard images, dropping any pHYs chunk,
 * so the target app sees only pixels and may paste the image large — scale it
 * down there; the detail is kept.
 */

export const COPY_DPI = 300
/** Pixels per inch of the preview before scaling (1 pt = 1 px). */
export const PREVIEW_PPI = 72
/** Longest side of the copied bitmap. */
export const MAX_SIDE_PX = 8192
/** Total pixels of the copied bitmap (~40 MP, ~160 MB of RGBA canvas). */
export const MAX_AREA_PX = 40_000_000

/** Scale factor for Plotly.toImage: COPY_DPI at the figure's size, reduced
 *  only as far as the two ceilings require. Never below 1. */
export function copyScale(width: number, height: number): number {
  const wanted = COPY_DPI / PREVIEW_PPI
  if (!(width > 0) || !(height > 0)) return wanted
  const bySide = MAX_SIDE_PX / Math.max(width, height)
  const byArea = Math.sqrt(MAX_AREA_PX / (width * height))
  return Math.max(1, Math.min(wanted, bySide, byArea))
}

/** The effective dpi a scale gives, for the notice and the log. */
export function effectiveDpi(scale: number): number {
  return Math.round(scale * PREVIEW_PPI)
}

/** Decode a base64 `data:` URL. Not `fetch(url)`: the webview's CSP has no
 *  `connect-src data:`, so a fetch of the data URL would be refused. */
export function dataUrlToBlob(url: string): Blob {
  const comma = url.indexOf(',')
  const header = url.slice(0, comma)
  if (comma < 0 || !header.endsWith(';base64')) {
    throw new Error(`expected a base64 data URL, got "${url.slice(0, 40)}…"`)
  }
  const mime = header.slice('data:'.length, -';base64'.length)
  const binary = atob(url.slice(comma + 1))
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i)
  return new Blob([bytes], { type: mime })
}

