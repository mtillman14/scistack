/**
 * The Plot Studio's colours have one owner — plotTheme.ts.
 *
 * Light mode works by redefining `var(--ps-…)` tokens on the studio root, so a
 * colour written straight into a component would stay dark in a light studio,
 * and a token misspelt in a component would resolve to nothing in both. Neither
 * shows up in a type check. There is no React render harness in this suite (see
 * tsconfig.test.json), so both rules are checked against the SOURCE, the way
 * pickerCanvas.test.ts checks the canvas provider.
 *
 * Runs from `frontend/` (`npm test`), which is where the sources are resolved.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { readdirSync, readFileSync } from 'node:fs'
import { join, resolve } from 'node:path'
import {
  DEFAULT_PLOT_THEME,
  PLOT_THEMES,
  otherPlotTheme,
  parsePlotThemeMode,
  plotThemeVarName,
  plotThemeVars,
  screenFigure,
  type PlotThemeTokens,
} from './plotTheme.js'

const PLOT_STUDIO = resolve(process.cwd(), 'src/components/PlotStudio')

const tsxFiles = () =>
  readdirSync(PLOT_STUDIO)
    .filter(name => name.endsWith('.tsx'))
    .map(name => [name, readFileSync(join(PLOT_STUDIO, name), 'utf8')] as const)

const COLOUR_LITERAL = /#[0-9a-fA-F]{3,8}\b|rgba?\(/g

test('no PlotStudio component names a colour itself', () => {
  const offenders: string[] = []
  for (const [name, source] of tsxFiles()) {
    source.split('\n').forEach((line, i) => {
      if (COLOUR_LITERAL.test(line)) offenders.push(`${name}:${i + 1}: ${line.trim()}`)
      COLOUR_LITERAL.lastIndex = 0
    })
  }
  assert.deepEqual(offenders, [], 'use a var(--ps-…) token from plotTheme.ts')
})

test('every var(--ps-…) a component uses is a defined token', () => {
  const defined = new Set(Object.keys(plotThemeVars('dark')))
  const unknown: string[] = []
  for (const [name, source] of tsxFiles()) {
    for (const match of source.matchAll(/var\((--ps-[a-z0-9-]+)\)/g)) {
      if (!defined.has(match[1])) unknown.push(`${name}: ${match[1]}`)
    }
  }
  assert.deepEqual(unknown, [])
})

test('both modes define the same tokens, each a non-empty value', () => {
  const dark = Object.keys(PLOT_THEMES.dark).sort()
  assert.deepEqual(Object.keys(PLOT_THEMES.light).sort(), dark)
  for (const mode of ['dark', 'light'] as const) {
    for (const [token, value] of Object.entries(PLOT_THEMES[mode])) {
      assert.ok(value.trim(), `${mode}.${token} is empty`)
    }
  }
})

test('light mode actually changes the look', () => {
  // A token left at its dark value in the light palette would be a dark patch
  // in a light studio. accentFg (white on the violet button) and the status
  // glyphs are the same on purpose.
  const shared = new Set<keyof PlotThemeTokens>([
    'accentFg', 'targetBorder', 'statusRed', 'textFaintCool',
  ])
  const same = (Object.keys(PLOT_THEMES.dark) as (keyof PlotThemeTokens)[])
    .filter(token => !shared.has(token) && PLOT_THEMES.dark[token] === PLOT_THEMES.light[token])
  assert.deepEqual(same, [])
})

test('token names map to kebab-case custom properties', () => {
  assert.equal(plotThemeVarName('surfaceSunken'), '--ps-surface-sunken')
  assert.equal(plotThemeVarName('bg'), '--ps-bg')
  assert.equal(plotThemeVars('light')['--ps-bg'], PLOT_THEMES.light.bg)
})

/** What scistackplot's plotly renderer sends: the export's paper (paper.py). */
const exportFigure = () => ({
  data: [
    { type: 'bar', x: ['a'], y: [1], marker: { color: '#1f77b4' },
      error_y: { array: [0.1], color: '#000000', thickness: 1.5, width: 3 } },
    { type: 'scatter', x: ['a'], y: [1], marker: { color: '#ff7f0e' } },
  ],
  layout: {
    paper_bgcolor: '#ffffff',
    plot_bgcolor: '#ffffff',
    font: { size: 10, color: '#000000', family: 'DejaVu Sans' },
    xaxis: { showline: true, mirror: true, linecolor: '#000000', tickcolor: '#000000', ticks: 'outside' },
    yaxis2: { showline: true, mirror: true, linecolor: '#000000', tickcolor: '#000000', ticks: 'outside' },
    meta: { rows: 1 },
  },
})

test('light mode draws the figure exactly as the renderer sent it', () => {
  // The light preview IS the saved file: any recolouring here would break
  // the parity scistackplot's paper.py exists for.
  const figure = exportFigure()
  const shown = screenFigure(figure, 'light')
  assert.equal(shown, figure)
  assert.deepEqual(shown, exportFigure())
})

test('dark mode recolours the ink for the screen and nothing else', () => {
  const ink = PLOT_THEMES.dark.plotText
  const figure = exportFigure()
  const shown = screenFigure(figure, 'dark')
  const layout = shown.layout as Record<string, any>
  const data = shown.data as Record<string, any>[]

  assert.equal(layout.paper_bgcolor, 'transparent')
  assert.equal(layout.plot_bgcolor, 'transparent')
  assert.deepEqual(layout.font, { size: 10, color: ink, family: 'DejaVu Sans' })
  for (const key of ['xaxis', 'yaxis2']) {
    assert.equal(layout[key].linecolor, ink)
    assert.equal(layout[key].tickcolor, ink)
    assert.equal(layout[key].ticks, 'outside')
  }
  assert.deepEqual(layout.meta, { rows: 1 })
  assert.equal(data[0].error_y.color, ink)
  assert.equal(data[0].error_y.width, 3)
  // Data colours are the figure's; the screen view never repaints them.
  assert.deepEqual(data[0].marker, { color: '#1f77b4' })
  assert.equal(data[1], figure.data[1])
  // The input is not mutated: the same payload is drawn again on a toggle.
  assert.deepEqual(figure, exportFigure())
})

test('parsePlotThemeMode accepts only the two modes', () => {
  assert.equal(parsePlotThemeMode('dark'), 'dark')
  assert.equal(parsePlotThemeMode('light'), 'light')
  assert.equal(parsePlotThemeMode('Light'), null)
  assert.equal(parsePlotThemeMode(undefined), null)
  assert.equal(parsePlotThemeMode(null), null)
  assert.equal(DEFAULT_PLOT_THEME, 'dark')
  assert.equal(otherPlotTheme('dark'), 'light')
  assert.equal(otherPlotTheme('light'), 'dark')
})
