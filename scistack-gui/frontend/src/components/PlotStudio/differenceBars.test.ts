import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  DEFAULT_LABEL,
  HOVER_OPACITY,
  IDLE,
  PICKED_OPACITY,
  addBar,
  barCount,
  barText,
  barsForPanel,
  highlightShapes,
  panelForAxis,
  pickStep,
  pickingPrompt,
  removeBar,
  samePair,
  setLabel,
  targetAt,
  type DiffPanelMeta,
  type DiffTarget,
  type DifferenceBar,
  type DifferenceMeta,
} from './differenceBars.js'

const target = (slot: number, session: string, label = session): DiffTarget => ({
  slot,
  position: slot,
  x0: slot - 0.5,
  x1: slot + 0.5,
  values: { session },
  x_text: session,
  label,
})

const PRE = target(0, 'pre', 'Pre')
const POST = target(1, 'post', 'Post')
const FOLLOW = target(2, 'follow', 'Follow')

const panel = (index: number, side: string): DiffPanelMeta => ({
  index,
  match: { group: 'sham', side },
  display_title: side,
  xaxis: index === 0 ? 'x' : `x${index + 1}`,
  yaxis: index === 0 ? 'y' : `y${index + 1}`,
  targets: [PRE, POST, FOLLOW],
  bars: [],
  unresolved: [],
  unfit: [],
  y_limits: [0, 10],
})

const META: DifferenceMeta = { panels: [panel(0, 'L'), panel(1, 'R')], not_in_figure: [], estimated: false }
const R = META.panels[1]

const bar = (a: string, b: string, label = '*'): DifferenceBar => ({
  match: { group: 'sham', side: 'R' },
  a: { session: a },
  b: { session: b },
  label,
})

// --- which tick a click picks --------------------------------------------------

test('a click names its panel by the x axis plotly reports', () => {
  assert.equal(panelForAxis(META, 'x2'), R)
  assert.equal(panelForAxis(META, 'x'), META.panels[0])
  assert.equal(panelForAxis(META, 'x9'), undefined)
  assert.equal(panelForAxis(null, 'x'), undefined)
})

test('a number picks the tick whose stretch holds it (sample points at offsets too)', () => {
  assert.equal(targetAt(R, 0), PRE)
  assert.equal(targetAt(R, 1.18), POST) // a sample point beside its bar
  assert.equal(targetAt(R, 1.5), POST) // a shared boundary: the nearer centre wins, ties keep the first
  assert.equal(targetAt(R, 2.4), FOLLOW)
  assert.equal(targetAt(R, 7), undefined)
})

test('a category name picks the tick by its text', () => {
  assert.equal(targetAt(R, 'follow'), FOLLOW)
  assert.equal(targetAt(R, 'later'), undefined)
  assert.equal(targetAt(R, '1'), POST) // a numeric string is a position
  assert.equal(targetAt(R, undefined), undefined)
})

// --- editing the list ------------------------------------------------------------

test('adding stores the panel match and both ends, with the default label', () => {
  const next = addBar([], R.match, PRE.values, POST.values)
  assert.deepEqual(next, [bar('pre', 'post', DEFAULT_LABEL)])
})

test('adding the same pair again, in either order, changes nothing', () => {
  const once = addBar([], R.match, PRE.values, POST.values)
  assert.equal(addBar(once, R.match, POST.values, PRE.values), once)
})

test('a bar from a tick to itself is never added', () => {
  assert.deepEqual(addBar([], R.match, PRE.values, PRE.values), [])
})

test('removing drops the pair whichever way round it was stored', () => {
  const list = [bar('pre', 'post'), bar('post', 'follow')]
  assert.deepEqual(removeBar(list, bar('post', 'pre')), [bar('post', 'follow')])
})

test('a label is set per pair; emptied it is the default', () => {
  const list = [bar('pre', 'post'), bar('post', 'follow')]
  assert.equal(setLabel(list, bar('pre', 'post'), '**')[0].label, '**')
  assert.equal(setLabel(list, bar('pre', 'post'), '')[0].label, DEFAULT_LABEL)
  assert.equal(setLabel(list, bar('pre', 'post'), '**')[1].label, '*')
})

test('pairs are the same whatever the key order of their dicts', () => {
  const x: DifferenceBar = { match: { a: '1', b: '2' }, a: { s: 'pre' }, b: { s: 'post' } }
  const y: DifferenceBar = { match: { b: '2', a: '1' }, a: { s: 'post' }, b: { s: 'pre' } }
  assert.ok(samePair(x, y))
})

test('a panel lists only its own bars, in spec order', () => {
  const other: DifferenceBar = { ...bar('pre', 'post'), match: { group: 'sham', side: 'L' } }
  assert.deepEqual(barsForPanel([bar('pre', 'post'), other], R.match), [bar('pre', 'post')])
  assert.equal(barCount([bar('pre', 'post'), other]), 2)
  assert.equal(barCount(undefined), 0)
})

test('a bar reads by its ticks’ text in the panel', () => {
  assert.equal(barText(bar('pre', 'follow'), R), 'Pre ↔ Follow')
  assert.equal(barText(bar('pre', 'follow')), 'pre ↔ follow')
})

// --- picking -----------------------------------------------------------------------

test('idle, a click does nothing', () => {
  assert.deepEqual(pickStep(IDLE, R, PRE), { state: IDLE })
})

test('two clicks in one panel add the bar and end picking', () => {
  const first = pickStep({ phase: 'first' }, R, PRE)
  assert.equal(first.state.phase, 'second')
  assert.equal(first.add, undefined)
  const second = pickStep(first.state, R, FOLLOW)
  assert.equal(second.state, IDLE)
  assert.deepEqual(second.add, { match: R.match, a: PRE.values, b: FOLLOW.values })
})

test('clicking the first tick again, or nothing, keeps waiting', () => {
  const first = pickStep({ phase: 'first' }, R, PRE).state
  assert.equal(pickStep(first, R, PRE).state, first)
  assert.equal(pickStep(first, R, undefined).state, first)
})

test('a second click in another panel starts over there', () => {
  const first = pickStep({ phase: 'first' }, R, PRE).state
  const moved = pickStep(first, META.panels[0], POST)
  assert.equal(moved.add, undefined)
  assert.ok(moved.state.phase === 'second' && moved.state.panelIndex === 0)
})

test('the prompt says what to click next', () => {
  assert.match(pickingPrompt({ phase: 'first' }), /first end/)
  assert.match(pickingPrompt(pickStep({ phase: 'first' }, R, PRE).state), /First end: Pre/)
  assert.equal(pickingPrompt(IDLE), '')
})

// --- highlight -------------------------------------------------------------------

test('hovering a tick shades its whole column in its panel', () => {
  const [band] = highlightShapes(META, { panelIndex: 1, slot: 1 }, null, 'blue')
  assert.deepEqual(band, {
    type: 'rect',
    xref: 'x2',
    yref: 'y2 domain',
    x0: 0.5,
    x1: 1.5,
    y0: 0,
    y1: 1,
    fillcolor: 'blue',
    opacity: HOVER_OPACITY,
    line: { width: 0 },
    layer: 'below',
    name: 'difference-hover',
  })
})

test('the first pick stays shaded, stronger, and hovering it adds nothing', () => {
  const picked = { panelIndex: 1, slot: 0 }
  const both = highlightShapes(META, { panelIndex: 1, slot: 2 }, picked, 'blue')
  assert.deepEqual(both.map(s => [s.name, s.opacity]), [
    ['difference-pick', PICKED_OPACITY],
    ['difference-hover', HOVER_OPACITY],
  ])
  assert.equal(highlightShapes(META, picked, picked, 'blue').length, 1)
})

test('no meta or an unknown tick shades nothing', () => {
  assert.deepEqual(highlightShapes(null, { panelIndex: 1, slot: 1 }, null, 'blue'), [])
  assert.deepEqual(highlightShapes(META, { panelIndex: 1, slot: 9 }, null, 'blue'), [])
})
