import { test } from 'node:test'
import assert from 'node:assert/strict'

import { appliedSummary, madeOnLabel } from './presets.js'
import type { RestoreNote } from './savedPlots.js'

const note = (kind: string): RestoreNote => ({ path: 'x', kind, message: 'm' })

test('made-on label names the variable and its shape when known', () => {
  assert.equal(madeOnLabel({ made_on: 'StepLength', made_on_shape: 'scalar' }), 'made on StepLength (scalar)')
  assert.equal(madeOnLabel({ made_on: 'StepLength', made_on_shape: null }), 'made on StepLength')
  assert.equal(madeOnLabel({ made_on: null, made_on_shape: 'scalar' }), '')
})

test('a clean apply says only which preset', () => {
  assert.equal(appliedSummary('Session box', []), 'Applied "Session box".')
})

test('an apply with notes counts each kind once', () => {
  const summary = appliedSummary('Session box', [
    note('not_in_data'), note('not_in_data'), note('kind_unavailable'), note('unknown_setting'),
  ])
  assert.equal(
    summary,
    'Applied "Session box": 2 settings not in this variable\'s data; plot type changed ' +
      'to fit this variable; 1 setting no longer applies.'
  )
})
