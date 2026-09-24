/**
 * usePlotTheme — the Plot Studio's light/dark mode, shared by every tab.
 *
 * The choice is a per-user preference, not a property of a figure or a
 * database, so it lives OUTSIDE the webview:
 *
 * - VS Code: the extension host owns it (extension/src/plotTheme.ts, in
 *   `globalState`). It injects the current mode into each new tab as
 *   `window.__SCISTACK_PLOT_THEME__`, and a toggle in one tab is sent as
 *   `set_plot_theme`; the host stores it and pushes `plot_theme_changed` to
 *   every other open tab, so all figures switch together.
 * - Browser build (`scistack-gui` CLI): localStorage, which other tabs of the
 *   same server see through the `storage` event.
 *
 * One store per webview, read through `useSyncExternalStore`: the studio's
 * shell paints the chrome and the figure picks its text colour, and two
 * `useState`s there could disagree after a toggle.
 */

import { useSyncExternalStore } from 'react'
import { addNotificationHandler, callBackend, isVSCodeMode } from '../../api'
import { DEFAULT_PLOT_THEME, type PlotThemeMode, parsePlotThemeMode } from './plotTheme'

declare global {
  interface Window {
    __SCISTACK_PLOT_THEME__?: string
  }
}

/** localStorage key for the browser build (the host's key is its own). */
const STORAGE_KEY = 'scistack.plotTheme'

function readStored(): PlotThemeMode | null {
  try {
    return parsePlotThemeMode(window.localStorage.getItem(STORAGE_KEY))
  } catch {
    return null // storage blocked: fall back to the default
  }
}

function initialMode(): PlotThemeMode {
  if (typeof window === 'undefined') return DEFAULT_PLOT_THEME
  if (isVSCodeMode) return parsePlotThemeMode(window.__SCISTACK_PLOT_THEME__) ?? DEFAULT_PLOT_THEME
  return readStored() ?? DEFAULT_PLOT_THEME
}

let current: PlotThemeMode = initialMode()
const listeners = new Set<() => void>()

function apply(mode: PlotThemeMode, why: string): void {
  if (mode === current) return
  console.info(`[plot theme] ${current} -> ${mode} (${why})`)
  current = mode
  listeners.forEach(listener => listener())
}

if (typeof window !== 'undefined') {
  if (isVSCodeMode) {
    addNotificationHandler(msg => {
      if (msg.method !== 'plot_theme_changed') return
      const mode = parsePlotThemeMode((msg.params as { mode?: unknown } | undefined)?.mode)
      if (mode) apply(mode, 'changed in another tab')
    })
  } else {
    window.addEventListener('storage', event => {
      if (event.key !== STORAGE_KEY) return
      const mode = parsePlotThemeMode(event.newValue)
      if (mode) apply(mode, 'changed in another tab')
    })
  }
}

/** Switch every Plot Studio to `mode` and remember it. */
export function setPlotTheme(mode: PlotThemeMode): void {
  apply(mode, 'toggled here')
  if (isVSCodeMode) {
    // The host stores it and tells the other tabs. A failure leaves this tab
    // switched but the choice unsaved, which is worth a log line, not a dialog.
    callBackend('set_plot_theme', { mode }).catch(err => {
      console.warn(`[plot theme] could not save ${mode}: ${err}`)
    })
    return
  }
  try {
    window.localStorage.setItem(STORAGE_KEY, mode)
  } catch {
    // storage blocked: the switch still holds for this page
  }
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener)
  return () => {
    listeners.delete(listener)
  }
}

export function usePlotTheme(): PlotThemeMode {
  return useSyncExternalStore(subscribe, () => current)
}
