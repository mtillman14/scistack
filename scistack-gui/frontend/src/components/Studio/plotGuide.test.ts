import { test } from 'node:test'
import assert from 'node:assert/strict'
import { NEW_GUIDE, asGuideSpec, initialView, roleChanges } from './plotGuide.js'
import { applyAction, currentStep, next } from '../StatStudio/Guided/guideSteps.js'
import type { GuideStep } from '../StatStudio/Guided/guideSteps.js'

const step = (id: string): GuideStep => ({
  id, title: id, question: id, concepts: [], facts: [], cards: [], recommendation: null, blocked: null,
  warnings: [], needs_results: false, table: null,
})
const STEPS = ['measure', 'unit', 'factors', 'figure'].map(step)

test('a fresh figure opens step by step until the viewer chose Advanced', () => {
  assert.equal(initialView(null, {}), 'guided')
  assert.equal(initialView('guided', {}), 'guided')
  assert.equal(initialView('advanced', {}), 'advanced')
  assert.equal(initialView(null, { plotNode: true }), 'advanced')
})

test('the figure spec drives the shared wizard navigation', () => {
  const plot = { measures: ['Rating'], roles: { brand: 'group', participant: 'iterate' }, groups: ['brand'] }
  const spec = asGuideSpec(plot, NEW_GUIDE)
  assert.equal(spec.design, plot)
  assert.equal(currentStep(spec, STEPS), 'measure')
  assert.equal(currentStep(next(spec, STEPS), STEPS), 'unit')
})

test('a wizard choice comes back as the role edits the figure applies', () => {
  const plot = { measures: ['Rating'], roles: { brand: 'group', participant: 'iterate' }, groups: ['brand'] }
  const after = applyAction(asGuideSpec(plot, NEW_GUIDE), { op: 'set_role', factor: 'participant', role: 'collapse' })
  assert.deepEqual(roleChanges(plot.roles, after.design.roles), [{ factor: 'participant', role: 'collapse' }])
  assert.deepEqual(roleChanges(plot.roles, plot.roles), [])
})
