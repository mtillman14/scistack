/**
 * PlotRoot — the Plot Studio as a whole webview, rather than a modal.
 *
 * The extension hosts this in its own editor tab (see extension/src/plotPanel.ts)
 * so the pipeline canvas stays visible beside it. Every plot opens its OWN tab,
 * so the target arrives once, in the injected `window.__SCISTACK_VIEW__`, and
 * never changes for the life of the webview.
 *
 * (It used to change: one tab was reused and retargeted through an
 * `open_plot_studio` notification, which meant plotting a second variable
 * destroyed the figure you were looking at. Comparing two figures is the
 * normal reason to open two.)
 */

import PlotStudio from './components/PlotStudio/PlotStudio'
import type { PathStep } from './components/PlotStudio/SchemaLocationPicker'

export interface PlotViewConfig {
  view: 'plot'
  variable: string | null
  csvPath: string | null
  /** One schema location to open on — the canvas picker's row click. */
  location: PathStep[] | null
}

export default function PlotRoot({ initial }: { initial: PlotViewConfig }) {
  return (
    <PlotStudio
      variable={initial.variable ?? ''}
      csvPath={initial.csvPath ?? undefined}
      initialLocation={initial.location ?? undefined}
      embedded
      onClose={() => undefined}
    />
  )
}
