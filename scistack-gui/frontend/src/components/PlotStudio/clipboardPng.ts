/**
 * Put the previewed figure on the clipboard as a PNG — the browser half of
 * `copyImage.ts` (which owns the resolution rule and is unit-tested; this file
 * reaches Plotly and the DOM, so it cannot be).
 */

import Plotly from 'plotly.js-cartesian-dist-min'
import { copyScale, dataUrlToBlob, effectiveDpi } from './copyImage'

export interface CopyResult {
  widthPx: number
  heightPx: number
  dpi: number
  bytes: number
  ms: number
}

/** Render `figure` at `width` x `height` (CSS px) scaled to COPY_DPI and put
 *  it on the clipboard. Must be called from a click handler: the clipboard
 *  item is created synchronously with a PROMISE of the blob, so the user
 *  gesture is still live when the browser checks it. */
export async function copyFigurePng(
  figure: { data: unknown[]; layout: Record<string, unknown> },
  width: number,
  height: number
): Promise<CopyResult> {
  if (!navigator.clipboard?.write || typeof ClipboardItem === 'undefined') {
    throw new Error('this webview has no image clipboard (navigator.clipboard.write)')
  }
  const started = performance.now()
  const scale = copyScale(width, height)
  let bytes = 0
  const blob = (async () => {
    const url = await Plotly.toImage(
      {
        data: figure.data,
        layout: { ...figure.layout, autosize: false, width, height },
      },
      { format: 'png', width, height, scale }
    )
    const png = dataUrlToBlob(url)
    bytes = png.size
    return png
  })()
  await navigator.clipboard.write([new ClipboardItem({ 'image/png': blob })])
  return {
    widthPx: Math.round(width * scale),
    heightPx: Math.round(height * scale),
    dpi: effectiveDpi(scale),
    bytes,
    ms: Math.round(performance.now() - started),
  }
}
