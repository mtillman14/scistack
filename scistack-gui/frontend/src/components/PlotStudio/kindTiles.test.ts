import { test } from 'node:test'
import assert from 'node:assert/strict'
import { applyTile, isTileSelected, tileKey, type KindTile, type TileSpec } from './kindTiles.js'

const count: KindTile = { kind: 'bar', id: 'bar:count', available: true, categorical: { stat: 'count' }, selected: true }
const proportion: KindTile = {
  kind: 'bar',
  id: 'bar:proportion',
  available: true,
  categorical: { stat: 'proportion' },
  selected: false,
  assignment: { roles: { Level: 'group', subject: 'collapse' }, groups: ['Level'], color: 'Level' },
}
const spec: TileSpec = { kind: 'bar', roles: { Level: 'group', subject: 'collapse', trial: 'collapse' }, groups: ['Level'], color: 'Level', categorical: { stat: 'count', level: 'fall' } }

test('two tiles of one kind have different keys', () => {
  assert.notEqual(tileKey(count), tileKey(proportion))
  assert.equal(tileKey({ kind: 'box', available: true }), 'box')
})

test('the backend decides which of two bar tiles is selected', () => {
  assert.equal(isTileSelected(count, spec), true)
  assert.equal(isTileSelected(proportion, spec), false)
})

test('a plain tile is selected by its kind', () => {
  assert.equal(isTileSelected({ kind: 'bar', available: true }, spec), true)
  assert.equal(isTileSelected({ kind: 'box', available: true }, spec), false)
  assert.equal(isTileSelected({ kind: 'bar', available: true }, null), false)
})

test('a proportion tile sets the stat and keeps the chosen category', () => {
  const next = applyTile(spec, proportion, false)
  assert.equal(next.kind, 'bar')
  assert.deepEqual(next.categorical, { stat: 'proportion', level: 'fall' })
  // A stale report: the user's roles are kept.
  assert.deepEqual(next.roles, spec.roles)
})

test('a fresh report applies the tile assignment', () => {
  const next = applyTile(spec, proportion, true)
  assert.deepEqual(next.roles, proportion.assignment!.roles)
  assert.deepEqual(next.groups, ['Level'])
})

test('a numeric tile leaves the categorical options alone', () => {
  const next = applyTile(spec, { kind: 'box', available: true }, true)
  assert.equal(next.kind, 'box')
  assert.deepEqual(next.categorical, spec.categorical)
})
