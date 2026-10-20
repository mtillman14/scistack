import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  aliasCount,
  aliasRows,
  parseSpellings,
  withAlias,
  withoutAlias,
} from './pathInputAliases.js'

test('parseSpellings splits on commas, trims, drops blanks and repeats', () => {
  assert.deepEqual(parseSpellings(' Baseline, 1. Baseline ,, Baseline'), ['Baseline', '1. Baseline'])
  assert.deepEqual(parseSpellings('   '), [])
})

test('withAlias adds a new key and canonical', () => {
  assert.deepEqual(withAlias({}, 'session', 'BL', ['Baseline']), { session: { BL: ['Baseline'] } })
})

test('withAlias merges into an existing canonical without repeats', () => {
  const a = { session: { BL: ['Baseline'] } }
  assert.deepEqual(withAlias(a, 'session', 'BL', ['Baseline', '1. Baseline']), {
    session: { BL: ['Baseline', '1. Baseline'] },
  })
})

test('withAlias drops a spelling equal to its canonical', () => {
  assert.deepEqual(withAlias({}, 'session', 'BL', ['BL', 'Baseline']), { session: { BL: ['Baseline'] } })
})

test('withAlias trims key and canonical and never mutates its input', () => {
  const a = { session: { BL: ['Baseline'] } }
  const next = withAlias(a, ' session ', ' FU ', ['FollowUp'])
  assert.deepEqual(next, { session: { BL: ['Baseline'], FU: ['FollowUp'] } })
  assert.deepEqual(a, { session: { BL: ['Baseline'] } })
})

test('withoutAlias removes one canonical and an emptied key', () => {
  const a = { session: { BL: ['Baseline'], FU: ['FollowUp'] }, trial: { T1: ['one'] } }
  assert.deepEqual(withoutAlias(a, 'session', 'BL'), {
    session: { FU: ['FollowUp'] },
    trial: { T1: ['one'] },
  })
  assert.deepEqual(withoutAlias(a, 'trial', 'T1'), { session: { BL: ['Baseline'], FU: ['FollowUp'] } })
  assert.deepEqual(withoutAlias({}, 'session', 'BL'), {})
})

test('aliasRows lists one row per canonical; aliasCount counts them', () => {
  const a = { session: { BL: ['Baseline'], '01': ['1. Pre'] } }
  // "01" is not an array-index key, so declaration order is kept.
  assert.deepEqual(aliasRows(a), [
    { key: 'session', canonical: 'BL', spellings: ['Baseline'] },
    { key: 'session', canonical: '01', spellings: ['1. Pre'] },
  ])
  assert.equal(aliasCount(a), 2)
  assert.equal(aliasCount(undefined), 0)
})
