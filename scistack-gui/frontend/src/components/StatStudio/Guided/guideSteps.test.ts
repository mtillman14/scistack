import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import {
  applyAction, canGoTo, clampToBlocked, currentStep, goTo, next, previous, stepStatus, type GuideStep,
} from './guideSteps.js'
import type { StatsSpec } from '../statsSpec.js'

const step = (id: string, blocked: string | null = null): GuideStep => ({
  id, title: id, question: id, concepts: [], facts: [], cards: [], recommendation: null, blocked,
  warnings: [], needs_results: false, table: null,
})

const STEPS = ['measure', 'unit', 'factors', 'within_between', 'data_check'].map(id => step(id))

const base = (guide?: StatsSpec['guide']): StatsSpec => ({
  format: 1,
  design: { measures: ['Dist_m'], roles: { group: 'group', session: 'group', subject: 'collapse' }, groups: ['group', 'session'] },
  dvs: [],
  descriptives: { cell: 'mean_sd', statistics: ['n'] },
  comparisons: [{ id: 'a1', family: 'auto' }],
  variant_mode: 'pinned',
  guide,
})

test('a fresh table starts on the first step and may only go there', () => {
  const spec = base({ mode: 'guided', done: [] })
  assert.equal(currentStep(spec, STEPS), 'measure')
  assert.equal(canGoTo(spec, STEPS, 'measure'), true)
  assert.equal(canGoTo(spec, STEPS, 'unit'), false)
  assert.equal(stepStatus(spec, STEPS, 'unit'), 'locked')
})

test('next finishes the step and opens the following one', () => {
  const spec = next(base({ mode: 'guided', done: [] }), STEPS)
  assert.deepEqual(spec.guide, { mode: 'guided', done: ['measure'], step: 'unit' })
  assert.equal(stepStatus(spec, STEPS, 'measure'), 'done')
  assert.equal(stepStatus(spec, STEPS, 'unit'), 'current')
})

test('jump back to any finished step, and forward again to finished ones', () => {
  const spec = base({ mode: 'guided', done: ['measure', 'unit', 'factors'], step: 'within_between' })
  const back = goTo(spec, STEPS, 'unit')
  assert.equal(currentStep(back, STEPS), 'unit')
  assert.equal(canGoTo(back, STEPS, 'factors'), true, 'finished steps stay reachable')
  assert.equal(canGoTo(back, STEPS, 'within_between'), true, 'the first unfinished step too')
  assert.equal(canGoTo(back, STEPS, 'data_check'), false, 'never past it')
})

test('never forward past a blocked step, and next does nothing there', () => {
  const steps = [step('measure'), step('unit'), step('factors', 'Choose at least one thing to compare.'), step('within_between')]
  const spec = base({ mode: 'guided', done: ['measure', 'unit', 'factors', 'within_between'], step: 'factors' })
  assert.equal(canGoTo(spec, steps, 'within_between'), false)
  assert.equal(stepStatus(spec, steps, 'factors'), 'blocked')
  assert.deepEqual(next(spec, steps), spec)
  assert.equal(currentStep(previous(spec, steps), steps), 'unit')
})

test('a step that becomes blocked behind the shown one pulls the wizard back to it', () => {
  const steps = [step('measure'), step('unit', 'Choose a measure first.'), step('factors')]
  const spec = base({ mode: 'guided', done: ['measure', 'unit'], step: 'factors' })
  assert.equal(currentStep(clampToBlocked(spec, steps), steps), 'unit')
  assert.deepEqual(clampToBlocked(spec, STEPS), spec)
})

test('the actions edit the spec through the spec helpers', () => {
  const spec = base()
  assert.deepEqual(applyAction(spec, { op: 'set_role', factor: 'session', role: 'iterate' }).design.groups, ['group'])
  assert.deepEqual(applyAction(spec, { op: 'set_primary', factor: 'session' }).design.groups, ['session', 'group'])
  const kind = applyAction(spec, { op: 'set_override', analysis: 'a1', factor: 'session', kind: 'within' })
  assert.deepEqual(kind.comparisons[0].overrides, { session: 'within' })
  const cleared = applyAction(kind, { op: 'set_override', analysis: 'a1', factor: 'session', kind: null })
  assert.deepEqual(cleared.comparisons[0].overrides, {})
  const patched = applyAction(spec, { op: 'set_analysis', patch: { correction: 'bonferroni', effect_size: null } })
  assert.equal(patched.comparisons[0].correction, 'bonferroni')
  assert.equal('effect_size' in patched.comparisons[0], false)
  const left = applyAction(spec, { op: 'exclude_level', factor: 'session', level: 'MO3' })
  assert.deepEqual(left.design.filters, [{ column: 'session', exclude: ['MO3'] }])
  assert.deepEqual(applyAction(spec, null), spec)
})

test('set_analysis on a table with no analysis adds one first', () => {
  const spec = { ...base(), comparisons: [] }
  const patched = applyAction(spec, { op: 'set_analysis', patch: { missing: 'complete_cases' } })
  assert.equal(patched.comparisons.length, 1)
  assert.equal(patched.comparisons[0].missing, 'complete_cases')
})

// The wizard words nothing about statistics: every term comes from
// scistackstats.explain. A statistics word in a Guided component is a second
// owner of that wording.
const VOCABULARY = [
  'Shapiro', 'Levene', 'Holm', 'Bonferroni', 'Benjamini', 'within-subject', 'between-subject', 'ANOVA',
  't-test', 'Mann', 'Wilcoxon', 'Friedman', 'Kruskal', 'p-value', 'effect size', 'mixed model', 'Welch',
  'confidence interval', 'normality', 'variance', 'Cohen', 'Hedges', 'omnibus', 'pairwise',
]

test('the Guided components hold no statistics vocabulary', () => {
  const dir = join(process.cwd(), 'src', 'components', 'StatStudio', 'Guided')
  const offenders: string[] = []
  for (const name of readdirSync(dir).filter(n => n.endsWith('.tsx'))) {
    const text = readFileSync(join(dir, name), 'utf8')
      .replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '') // comments may name things
    for (const word of VOCABULARY) {
      if (text.toLowerCase().includes(word.toLowerCase())) offenders.push(`${name}: ${word}`)
    }
  }
  assert.deepEqual(offenders, [])
})
