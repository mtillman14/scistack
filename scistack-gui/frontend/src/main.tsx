import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import PlotRoot, { type PlotViewConfig } from './PlotRoot'
import ClientErrorBoundary from './components/ClientErrorBoundary'
import { DEFAULT_PLOT_THEME, plotThemeVars } from './components/PlotStudio/plotTheme'

/**
 * One bundle, two roots. The extension opens the Plot Studio in its own editor
 * tab and marks that webview with an injected `window.__SCISTACK_VIEW__`
 * (extension/src/plotPanel.ts). A second vite target would double the build for
 * one component, and the two views share the whole api/transport layer anyway.
 */
declare global {
  interface Window {
    __SCISTACK_VIEW__?: PlotViewConfig
  }
}

const view = window.__SCISTACK_VIEW__

// The Plot Studio's colour tokens, on the page root in the default (dark)
// palette. The studio overrides them on its own root with the chosen mode; this
// is for the pieces reused OUTSIDE it — the canvas's SchemaLocationPicker, the
// provenance panel's VariantDagPopup — which would otherwise resolve every
// `var(--ps-…)` to nothing. See components/PlotStudio/plotTheme.ts.
for (const [name, value] of Object.entries(plotThemeVars(DEFAULT_PLOT_THEME))) {
  document.documentElement.style.setProperty(name, value)
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    {/* Outermost, so a render crash anywhere shows itself and reaches
        scidb.log instead of blanking the webview. */}
    {view?.view === 'plot' ? (
      <ClientErrorBoundary where="Plot Studio tab">
        <PlotRoot initial={view} />
      </ClientErrorBoundary>
    ) : (
      <ClientErrorBoundary where="pipeline view">
        <App />
      </ClientErrorBoundary>
    )}
  </StrictMode>
)
