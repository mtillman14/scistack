/**
 * Plot Studio colour themes — the ONE owner of every colour the studio's
 * chrome draws.
 *
 * Components never name a colour. They name a token, `var(--ps-<token>)`, and
 * the element that owns the theme (the studio's root, see `plotThemeVars`)
 * defines every token for the chosen mode. Switching mode therefore rewrites
 * one style attribute instead of re-rendering 190 style objects, and a colour
 * cannot be dark in one component and light in its neighbour.
 *
 * The dark palette is the studio's original look, value for value: turning the
 * toggle into existence must not repaint anyone's dark studio. The page root
 * also carries the dark tokens (`main.tsx`), so the pieces reused OUTSIDE the
 * studio — the canvas's SchemaLocationPicker, the provenance panel's
 * VariantDagPopup — keep drawing as before.
 *
 * Guarded by plotTheme.test.ts: no colour literal in PlotStudio/*.tsx, and no
 * `var(--ps-…)` that names a token missing here.
 *
 * Deliberately React- and DOM-free so `npm test` can run it under node.
 */

export type PlotThemeMode = 'dark' | 'light'

/** The look a studio opens with before anyone has chosen one. */
export const DEFAULT_PLOT_THEME: PlotThemeMode = 'dark'

/** Everything a Plot Studio colour can be. Grouped by role, not by hue. */
export interface PlotThemeTokens {
  // Surfaces
  bg: string
  surface: string
  surfaceSunken: string
  surfaceNote: string
  field: string
  codeBg: string
  control: string
  controlAlt: string
  rowSelected: string
  // Borders
  borderFaint: string
  border: string
  borderSoft: string
  borderStrong: string
  borderNeutral: string
  borderNeutralStrong: string
  // Text, strongest to faintest
  textStrong: string
  text: string
  textBody: string
  textControl: string
  textSecondary: string
  textSecondaryAlt: string
  textMuted: string
  textMutedSlate: string
  textMutedGrey: string
  textMutedIndigo: string
  textMutedViolet: string
  textMutedPurple: string
  textMutedNeutral: string
  textFaint: string
  textFaintIndigo: string
  textFaintSlate: string
  textFaintGrey: string
  textFaintCool: string
  // Accent (the studio's violet)
  accent: string
  accentFg: string
  accentBgStrong: string
  accentBorder: string
  accentText: string
  accentTextAlt: string
  accentTextLight: string
  dropBg: string
  dropBorder: string
  targetBg: string
  targetBorder: string
  // Warnings and notes
  warn: string
  warnBg: string
  warnBorder: string
  caution: string
  amber: string
  amberBg: string
  amberBorder: string
  noteBg: string
  noteBorder: string
  noteText: string
  noteHint: string
  // Errors and statuses
  error: string
  errorSoft: string
  statusRed: string
  statusGreen: string
  statusFull: string
  // Information
  cyan: string
  cyanBorder: string
  infoText: string
  infoBg: string
  infoBorder: string
  link: string
  // Overlays
  scrim: string
  scrimStrong: string
  scrimCode: string
  shadow: string
  /**
   * The figure's ink (text, frame, ticks, error bars) as `screenFigure` draws
   * it. Plotly parses colours itself, so it is used as a VALUE, never as a
   * `var(...)`. Light mode never recolours the figure; its value is the
   * export's own black (scistackplot/paper.py), for the record.
   */
  plotText: string
}

export const PLOT_THEMES: Record<PlotThemeMode, PlotThemeTokens> = {
  dark: {
    bg: '#16162a',
    surface: '#1a1a2e',
    surfaceSunken: '#161626',
    surfaceNote: '#1d1d33',
    field: '#12121f',
    codeBg: '#0e0e1a',
    control: '#22223a',
    controlAlt: '#22223c',
    rowSelected: '#24244a',
    borderFaint: '#20203a',
    border: '#2a2a4a',
    borderSoft: '#2c2c4a',
    borderStrong: '#3a3a5a',
    borderNeutral: '#333',
    borderNeutralStrong: '#3a3a3a',
    textStrong: '#fff',
    text: '#eee',
    textBody: '#ddd',
    textControl: '#ccc',
    textSecondary: '#bbb',
    textSecondaryAlt: '#aaa',
    textMuted: '#999',
    textMutedSlate: '#9aa0b4',
    textMutedGrey: '#9ca3af',
    textMutedIndigo: '#9a9ab8',
    textMutedViolet: '#8b8ba7',
    textMutedPurple: '#8a8aa8',
    textMutedNeutral: '#888',
    textFaint: '#777',
    textFaintIndigo: '#7c7ca0',
    textFaintSlate: '#6b6b7a',
    textFaintGrey: '#666',
    textFaintCool: '#6b7280',
    accent: '#7b68ee',
    accentFg: '#fff',
    accentBgStrong: '#3b3280',
    accentBorder: '#4c3a8a',
    accentText: '#9d92f5',
    accentTextAlt: '#9d8cff',
    accentTextLight: '#c4b5fd',
    dropBg: '#2a2438',
    dropBorder: '#6b5a9a',
    targetBg: '#1e2a44',
    targetBorder: '#4f7fd0',
    warn: '#fbbf24',
    warnBg: '#2a2416',
    warnBorder: '#6b5a1a',
    caution: '#e0b050',
    amber: '#eab308',
    amberBg: '#2a2a1a',
    amberBorder: '#4a4a2a',
    noteBg: '#221c0c',
    noteBorder: '#d9b45f',
    noteText: '#d9c48f',
    noteHint: '#b8a878',
    error: '#f87171',
    errorSoft: '#ff8a8a',
    statusRed: '#dc2626',
    statusGreen: '#16a34a',
    statusFull: '#d7dae3',
    cyan: '#67e8f9',
    cyanBorder: '#1a5a6b',
    infoText: '#a9c7ff',
    infoBg: '#161a2e',
    infoBorder: '#2f4172',
    link: '#7aa2f7',
    scrim: 'rgba(0,0,0,0.6)',
    scrimStrong: 'rgba(0,0,0,0.72)',
    scrimCode: 'rgba(0,0,0,0.75)',
    shadow: 'rgba(0,0,0,0.5)',
    plotText: '#ccc',
  },
  light: {
    bg: '#f7f7fb',
    surface: '#eceef5',
    surfaceSunken: '#f1f2f7',
    surfaceNote: '#f3f3f9',
    field: '#ffffff',
    codeBg: '#f1f2f6',
    control: '#ffffff',
    controlAlt: '#e9ebf3',
    rowSelected: '#e4e0fb',
    borderFaint: '#e6e7ef',
    border: '#d9dbe6',
    borderSoft: '#d9dbe6',
    borderStrong: '#c1c4d4',
    borderNeutral: '#dddddd',
    borderNeutralStrong: '#cccccc',
    textStrong: '#111111',
    text: '#1c1e2a',
    textBody: '#262938',
    textControl: '#30333f',
    textSecondary: '#474b5c',
    textSecondaryAlt: '#525667',
    textMuted: '#666a78',
    textMutedSlate: '#5b6175',
    textMutedGrey: '#626977',
    textMutedIndigo: '#5e6282',
    textMutedViolet: '#646482',
    textMutedPurple: '#646486',
    textMutedNeutral: '#6c6c6c',
    textFaint: '#767676',
    textFaintIndigo: '#67678d',
    textFaintSlate: '#777786',
    textFaintGrey: '#858585',
    textFaintCool: '#6b7280',
    accent: '#6550e0',
    accentFg: '#fff',
    accentBgStrong: '#dcd6fc',
    accentBorder: '#b3a9ef',
    accentText: '#5641d2',
    accentTextAlt: '#5641d2',
    accentTextLight: '#6247cf',
    dropBg: '#f1eefc',
    dropBorder: '#a397d6',
    targetBg: '#e5edfb',
    targetBorder: '#4f7fd0',
    warn: '#a55a00',
    warnBg: '#fdf5e1',
    warnBorder: '#e6c475',
    caution: '#9a6300',
    amber: '#b58600',
    amberBg: '#fcf8e2',
    amberBorder: '#e4d59a',
    noteBg: '#fbf5e6',
    noteBorder: '#c9982a',
    noteText: '#6c571c',
    noteHint: '#877645',
    error: '#c42525',
    errorSoft: '#c42525',
    statusRed: '#dc2626',
    statusGreen: '#15803d',
    statusFull: '#3b4052',
    cyan: '#0e7490',
    cyanBorder: '#8fd0de',
    infoText: '#1d4ea6',
    infoBg: '#edf2fd',
    infoBorder: '#b5c6ea',
    link: '#2d5ecf',
    scrim: 'rgba(20,20,40,0.35)',
    scrimStrong: 'rgba(20,20,40,0.4)',
    scrimCode: 'rgba(20,20,40,0.45)',
    shadow: 'rgba(0,0,0,0.18)',
    plotText: '#000000',
  },
}

/** `surfaceSunken` → `--ps-surface-sunken`. */
export function plotThemeVarName(token: keyof PlotThemeTokens): string {
  return `--ps-${token.replace(/[A-Z]/g, ch => `-${ch.toLowerCase()}`)}`
}

/**
 * The custom properties that define every token for `mode`. Spread into the
 * theme owner's `style` (the studio root also sets `color-scheme`, so native
 * controls — selects, checkboxes, scrollbars — follow along).
 */
export function plotThemeVars(mode: PlotThemeMode): Record<string, string> {
  const tokens = PLOT_THEMES[mode]
  const vars: Record<string, string> = {}
  for (const token of Object.keys(tokens) as (keyof PlotThemeTokens)[]) {
    vars[plotThemeVarName(token)] = tokens[token]
  }
  return vars
}

/** A stored or injected mode, or null for anything that is not one. */
export function parsePlotThemeMode(value: unknown): PlotThemeMode | null {
  return value === 'dark' || value === 'light' ? value : null
}

export function otherPlotTheme(mode: PlotThemeMode): PlotThemeMode {
  return mode === 'dark' ? 'light' : 'dark'
}

// --- the figure ---------------------------------------------------------------

type Json = Record<string, unknown>

/** `xaxis`, `xaxis2`, `yaxis12` — the layout keys that are panel axes. */
const AXIS_KEY = /^[xy]axis\d*$/

/**
 * The figure as the studio should draw it.
 *
 * Light: exactly as the renderer sent it. The renderer draws the EXPORT's paper
 * (scistackplot/paper.py: white, a black frame, outward ticks, no grid), so the
 * light preview is the file Save writes, and nothing here may touch it.
 *
 * Dark: a screen view, not the saved figure. The background goes transparent
 * and the black ink (text, frame, ticks, error bars) turns light so it can be
 * read on the dark studio. Data colours are left alone. The tooltip on the
 * mode toggle says which view is which.
 */
export function screenFigure(
  figure: { data: unknown[]; layout: Json },
  mode: PlotThemeMode,
): { data: unknown[]; layout: Json } {
  if (mode === 'light') return figure
  const ink = PLOT_THEMES[mode].plotText
  const layout: Json = {
    ...figure.layout,
    paper_bgcolor: 'transparent',
    plot_bgcolor: 'transparent',
    font: { ...(figure.layout.font as Json | undefined), color: ink },
  }
  for (const [key, axis] of Object.entries(figure.layout)) {
    if (AXIS_KEY.test(key) && axis && typeof axis === 'object') {
      layout[key] = { ...(axis as Json), linecolor: ink, tickcolor: ink }
    }
  }
  const data = figure.data.map(item => {
    const trace = item as Json
    return trace?.error_y && typeof trace.error_y === 'object'
      ? { ...trace, error_y: { ...(trace.error_y as Json), color: ink } }
      : item
  })
  return { data, layout }
}
