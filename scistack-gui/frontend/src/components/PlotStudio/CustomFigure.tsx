/**
 * CustomFigure — the preview of a figure drawn by the user's own `plot_`
 * function (custom plot kinds, docs/claude/custom-plot-kinds.md).
 *
 * The backend decides what it is (`scistackplot.custom.preview_payload`):
 *
 * - `image`: a PNG of exactly what Save writes (a matplotlib figure), drawn
 *   at 1 pt = 1 px. Not interactive, and always on the light paper of the
 *   saved file, whatever the theme.
 * - `plotly`: the plotly figure the function returned, drawn interactively.
 */

import createPlotlyComponent from 'react-plotly.js/factory'
import Plotly from 'plotly.js-cartesian-dist-min'

// The panel's own plotly bundle (cartesian traces only): a figure with other
// trace types draws empty here and is still saved in full by kaleido.
const Plot = createPlotlyComponent(Plotly)

/** CSS pixels per inch of the preview: 1 pt = 1 px (PREVIEW_PX_PER_IN). */
export const CUSTOM_PREVIEW_PX_PER_IN = 72

export interface CustomPreviewMeta {
  preview?: { mode: 'pane' | 'export'; width_in: number; height_in: number }
}

export type CustomPreviewPayload =
  | {
      kind: 'image'
      mime: string
      data: string
      width_in: number
      height_in: number
      dpi: number
      meta?: CustomPreviewMeta
    }
  | {
      kind: 'plotly'
      figure: { data: unknown[]; layout: Record<string, unknown> }
      meta?: CustomPreviewMeta
    }

/** The size the preview is drawn at on screen, in CSS pixels. */
export function customPreviewSize(payload: CustomPreviewPayload): { width: number; height: number } | null {
  const preview = payload.meta?.preview
  if (payload.kind === 'image') {
    return {
      width: Math.round(payload.width_in * CUSTOM_PREVIEW_PX_PER_IN),
      height: Math.round(payload.height_in * CUSTOM_PREVIEW_PX_PER_IN),
    }
  }
  if (preview?.mode === 'export') {
    return {
      width: Math.round(preview.width_in * CUSTOM_PREVIEW_PX_PER_IN),
      height: Math.round(preview.height_in * CUSTOM_PREVIEW_PX_PER_IN),
    }
  }
  return null
}

/** The PNG as a Blob, for "Copy". */
export function customPreviewBlob(payload: CustomPreviewPayload): Blob | null {
  if (payload.kind !== 'image') return null
  const bytes = Uint8Array.from(atob(payload.data), c => c.charCodeAt(0))
  return new Blob([bytes], { type: payload.mime })
}

export default function CustomFigure({
  payload,
  fillHeight,
  label,
}: {
  payload: CustomPreviewPayload
  /** The pane height a pane-filling plotly figure takes. */
  fillHeight: number
  /** For the image's alt text. */
  label: string
}) {
  const size = customPreviewSize(payload)
  if (payload.kind === 'image') {
    return (
      <div style={{ overflow: 'auto', maxWidth: '100%' }}>
        <img
          src={`data:${payload.mime};base64,${payload.data}`}
          alt={label || 'Figure drawn by the plot_ function'}
          width={size?.width}
          height={size?.height}
          style={{ display: 'block' }}
        />
      </div>
    )
  }
  const layout = size
    ? { ...payload.figure.layout, autosize: false, width: size.width, height: size.height }
    : { ...payload.figure.layout, autosize: true, height: fillHeight }
  return (
    <div style={{ overflow: 'auto', maxWidth: '100%' }}>
      <Plot
        data={payload.figure.data}
        layout={layout}
        config={{ displaylogo: false, responsive: !size }}
        style={size ? { width: size.width, height: size.height } : { width: '100%' }}
        useResizeHandler={!size}
      />
    </div>
  )
}
