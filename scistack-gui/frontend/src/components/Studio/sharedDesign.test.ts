import { test } from 'node:test'
import assert from 'node:assert/strict'
import { DESIGN_CHANGED, designChangeFor, newDesignOrigin } from './sharedDesign.js'

test('origins are unique per tab and name their kind', () => {
  const a = newDesignOrigin('plot')
  const b = newDesignOrigin('plot')
  assert.notEqual(a, b)
  assert.ok(newDesignOrigin('stat').startsWith('stat-'))
})

test('a change about this plot from another tab is taken', () => {
  const msg = { method: DESIGN_CHANGED, params: { plot_id: 'p1', version: 3, origin: 'stat-x' } }
  assert.deepEqual(designChangeFor(msg, 'p1', 'plot-y'), { plot_id: 'p1', version: 3, origin: 'stat-x' })
})

test('its own broadcast, another plot, another method or no link are ignored', () => {
  const msg = { method: DESIGN_CHANGED, params: { plot_id: 'p1', version: 3, origin: 'plot-y' } }
  assert.equal(designChangeFor(msg, 'p1', 'plot-y'), null)
  assert.equal(designChangeFor({ ...msg, params: { ...msg.params, origin: 'z' } }, 'p2', 'plot-y'), null)
  assert.equal(designChangeFor({ method: 'other', params: msg.params }, 'p1', 'q'), null)
  assert.equal(designChangeFor(msg, null, 'q'), null)
})
