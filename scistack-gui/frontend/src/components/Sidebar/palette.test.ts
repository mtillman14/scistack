import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  LIBRARY_GROUP,
  UNKNOWN_GROUP,
  filterBySearch,
  groupByFile,
  matchesSearch,
  relativeTime,
  sortByLastSaved,
  type FunctionSource,
} from './palette.js'

test('matchesSearch is a case-insensitive substring of the name', () => {
  assert.equal(matchesSearch('filt', 'BandpassFilter'), true)
  assert.equal(matchesSearch('FILT', 'bandpass_filter'), true)
  assert.equal(matchesSearch('xyz', 'bandpass_filter'), false)
})

test('matchesSearch also matches the declaring file name, not its folders', () => {
  assert.equal(matchesSearch('emg_utils', 'rectify', '/proj/src/emg_utils.py'), true)
  assert.equal(matchesSearch('proj', 'rectify', '/proj/src/emg_utils.py'), false)
  assert.equal(matchesSearch('emg', 'rectify', 'C:\\proj\\emg.m'), true)
})

test('an empty or blank query matches everything, in server order', () => {
  const items = ['b', 'a', 'c']
  assert.deepEqual(filterBySearch(items, '', x => x), ['b', 'a', 'c'])
  assert.deepEqual(filterBySearch(items, '   ', x => x), ['b', 'a', 'c'])
})

test('filterBySearch keeps server order and can return nothing', () => {
  const items = ['zeta_a', 'alpha', 'beta_a']
  assert.deepEqual(filterBySearch(items, '_a', x => x), ['zeta_a', 'beta_a'])
  assert.deepEqual(filterBySearch(items, 'nope', x => x), [])
})

test('groupByFile groups per file, keeping server order inside each group', () => {
  const sources: Record<string, FunctionSource> = {
    a_fn: { file: '/p/z_mod.py', line: 1, library: false },
    b_fn: { file: '/p/a_mod.py', line: 1, library: false },
    c_fn: { file: '/p/z_mod.py', line: 9, library: false },
    'numpy.mean': { file: null, line: null, library: true },
    lost: { file: null, line: null, library: false },
  }
  const groups = groupByFile(['a_fn', 'b_fn', 'c_fn', 'numpy.mean', 'lost'], sources)

  assert.deepEqual(groups.map(g => g.label), ['a_mod.py', 'z_mod.py', 'Library', 'Source not found'])
  assert.deepEqual(groups[1].items, ['a_fn', 'c_fn'])
  assert.equal(groups[1].title, '/p/z_mod.py')
  assert.equal(groups[2].key, LIBRARY_GROUP)
  assert.equal(groups[3].key, UNKNOWN_GROUP)
})

test('groupByFile keeps two same-named files in different folders apart', () => {
  const groups = groupByFile(['x', 'y'], {
    x: { file: '/p/b/utils.py', line: 1, library: false },
    y: { file: '/p/a/utils.py', line: 1, library: false },
  })
  assert.deepEqual(groups.map(g => g.key), ['/p/a/utils.py', '/p/b/utils.py'])
})

test('a function with no source entry is "Source not found", never dropped', () => {
  const groups = groupByFile(['ghost'], {})
  assert.equal(groups.length, 1)
  assert.deepEqual(groups[0].items, ['ghost'])
})

test('sortByLastSaved: newest first, never-saved last, ties keep server order', () => {
  const order = sortByLastSaved(
    ['A', 'B', 'C', 'D', 'E'],
    {
      A: '2026-11-01T10:00:00',
      B: null,
      C: '2026-11-03T10:00:00',
      E: '2026-11-01T10:00:00',
    },
  )
  assert.deepEqual(order, ['C', 'A', 'E', 'B', 'D'])
})

test('relativeTime', () => {
  const now = Date.parse('2026-11-03T12:00:00Z')
  assert.equal(relativeTime(null, now), 'never saved')
  assert.equal(relativeTime('2026-11-03T11:59:30Z', now), 'just now')
  assert.equal(relativeTime('2026-11-03T11:55:00Z', now), '5 min ago')
  assert.equal(relativeTime('2026-11-03T09:00:00Z', now), '3 h ago')
  assert.equal(relativeTime('2026-11-01T12:00:00Z', now), '2 d ago')
})
