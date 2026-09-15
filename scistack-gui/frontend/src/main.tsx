import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import PlotRoot, { type PlotViewConfig } from './PlotRoot'
import ClientErrorBoundary from './components/ClientErrorBoundary'

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
