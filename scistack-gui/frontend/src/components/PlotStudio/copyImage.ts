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


/** A copy failure as text, whatever was thrown. Plotly.toImage rejects with
 *  the <img>'s bare `error` Event when the SVG will not load (the webview CSP
 *  refused its `blob:` URL, 2026-09-27) — no message, no stack — so an
 *  `(err as Error).message` logged "(no message)". Name the step and the kind
 *  of thing thrown instead. */
export function describeCopyFailure(step: string, err: unknown): string {
  let what: string
  if (err instanceof Error) {
    what = `${err.name}: ${err.message || '(empty message)'}`
  } else if (err && typeof err === 'object' && 'type' in err) {
    // An Event (Plotly's image onerror). Its target says what failed to load.
    const target = (err as { target?: { tagName?: string; src?: string } }).target
    const src = target?.src ? ` src=${target.src.slice(0, 40)}` : ''
    what =
      `${(err as { constructor?: { name?: string } }).constructor?.name ?? 'Event'} ` +
      `"${String((err as { type: unknown }).type)}"` +
      (target?.tagName ? ` on <${target.tagName.toLowerCase()}>${src}` : '') +
      ' — likely the webview CSP refused the image (img-src needs blob:)'
  } else {
    what = `${typeof err}: ${String(err)}`
  }
  return `${step} failed: ${what}`
}
