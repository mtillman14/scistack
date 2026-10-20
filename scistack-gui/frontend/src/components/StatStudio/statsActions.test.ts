import { test } from 'node:test'
import assert from 'node:assert/strict'
import { pendingActions, recommendedFor } from './statsActions.js'

const action = { factor: 'session', message: 'Comparisons need one decision: …', choices: ['within', 'between'] }

test('one pending action per analysis and factor, however many strata repeat it', () => {
  const results = [{ id: 'a1', strata: [{ action }, { action }, { action: null }] }]
  assert.deepEqual(pendingActions(results), [{ analysisId: 'a1', ...action }])
})

test('no result or no actions means nothing pending', () => {
  assert.deepEqual(pendingActions(undefined), [])
  assert.deepEqual(pendingActions([{ id: 'a1', strata: [{}] }]), [])
})

test('the recommendation comes from the first stratum that has one', () => {
  const rec = { omnibus: 'lmm', pairwise: 'lmm_pairwise', evidence: ['2 subjects lack some sessions'] }
  const results = [{ id: 'a1', strata: [{ selection: { why: 'x' } }, { selection: { recommended: rec } }] }]
  assert.deepEqual(recommendedFor(results, 'a1'), rec)
  assert.equal(recommendedFor(results, 'a2'), null)
})
