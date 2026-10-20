import { test } from 'node:test'
import assert from 'node:assert/strict'
import { insertCovariate, namesIn } from './modelFormulaText.js'

test('a picked covariate is crossed into the fixed part, before the random terms', () => {
  assert.equal(insertCovariate('group * time + (1 | subject)', 'Age'), 'group * time * Age + (1 | subject)')
  assert.equal(insertCovariate('group * time', 'Age'), 'group * time * Age')
})

test('a name that is not an R identifier is backticked', () => {
  assert.equal(insertCovariate('group + (1 | subject)', 'age (y)'), 'group * `age (y)` + (1 | subject)')
})

test('a name already in the text is not inserted twice', () => {
  assert.equal(insertCovariate('group * Age + (1 + Age | subject)', 'Age'), 'group * Age + (1 + Age | subject)')
  assert.equal(insertCovariate('group * `age (y)` + (1 | s)', 'age (y)'), 'group * `age (y)` + (1 | s)')
})

test('names are read bare and backticked', () => {
  assert.deepEqual([...namesIn('a * `b c` + (1 | s:t)')].sort(), ['a', 'b c', 's', 't'])
})
