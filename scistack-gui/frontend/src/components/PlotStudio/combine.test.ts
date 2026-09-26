/**
 * Combines — the panel's two spec edits: the slot switch and the bucket model.
 *
 * What a combine MEANS is Python's (`scistackplot/tests/test_groups.py`:
 * the replaced source is collapsed, the combine nests where its source would).
 * These pin the edits the panel makes before the spec ever reaches it.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  addCombine,
  assignLevels,
  combineSummary,
  levelRange,
  readBuckets,
  removeCombine,
  renameBucket,
  renameCombine,
  slotHolder,
  switchSlot,
  unusedLevels,
  writeBuckets,
  type LevelGroup,
  type SlotSpec,
} from './combine.js'

const LEVELS = ['stim1', 'stim2', 'stim3', 'sham']

function stim(extra: Partial<LevelGroup> = {}): LevelGroup {
  return {
    name: 'Stim',
    source: 'condition',
    mapping: { stim1: 'STIM', stim2: 'STIM', stim3: 'STIM', sham: 'SHAM' },
    unmatched: null,
    active: false,
    ...extra,
  }
}

function base(): SlotSpec {
  return {
    roles: { condition: 'group', subject: 'collapse', session: 'group' },
    groups: ['condition', 'session'],
    color: 'condition',
    level_groups: [stim(), stim({ name: 'Other', mapping: { sham: 'S' } })],
  }
}

// --- the slot switch -----------------------------------------------------------

test('switching to a combine moves role, grouping position and colour to it', () => {
  const spec = switchSlot(base(), 'condition', 'Stim')

  assert.deepEqual(spec.roles, { Stim: 'group', subject: 'collapse', session: 'group' })
  assert.deepEqual(spec.groups, ['Stim', 'session'])
  assert.equal(spec.color, 'Stim')
  assert.deepEqual(spec.level_groups!.map(g => g.active), [true, false])
})

test('switching back restores the spec exactly', () => {
  const start = base()
  const there = switchSlot(start, 'condition', 'Stim')
  const back = switchSlot(there, 'condition', 'condition')

  assert.deepEqual(back.roles, start.roles)
  assert.deepEqual(back.groups, start.groups)
  assert.equal(back.color, start.color)
  assert.deepEqual(back.level_groups!.map(g => g.active), [false, false])
})

test('switching between two combines of one source leaves one active', () => {
  const spec = switchSlot(switchSlot(base(), 'condition', 'Stim'), 'condition', 'Other')

  assert.equal(slotHolder(spec.level_groups, 'condition'), 'Other')
  assert.deepEqual(spec.level_groups!.map(g => g.active), [false, true])
  assert.deepEqual(spec.groups, ['Other', 'session'])
  assert.equal(spec.roles!.Other, 'group')
  assert.equal('Stim' in spec.roles!, false)
})

test('a slot with no role moves nothing but the active flag', () => {
  const spec = switchSlot({ ...base(), roles: {} }, 'condition', 'Stim')

  assert.deepEqual(spec.roles, {})
  assert.equal(slotHolder(spec.level_groups, 'condition'), 'Stim')
})

test('an unknown choice changes nothing', () => {
  const start = base()
  assert.equal(switchSlot(start, 'condition', 'nope'), start)
  assert.equal(switchSlot(start, 'condition', 'condition'), start)
})

test('absent `active` reads as active, as the backend reads it', () => {
  const groups = [{ ...stim(), active: undefined }]
  assert.equal(slotHolder(groups, 'condition'), 'Stim')
})

test('adding a combine gives it the slot, identity-mapped, with a unique name', () => {
  const spec = addCombine(
    { roles: { condition: 'facet' }, groups: [], color: null, level_groups: [] } as SlotSpec,
    'condition',
    LEVELS,
    ['condition', 'condition combined']
  )

  const [group] = spec.level_groups!
  assert.equal(group.name, 'condition combined 2')
  assert.deepEqual(group.mapping, { stim1: 'stim1', stim2: 'stim2', stim3: 'stim3', sham: 'sham' })
  assert.equal(group.active, true)
  assert.deepEqual(spec.roles, { 'condition combined 2': 'facet' })
})

test('removing the active combine hands the slot back to the source first', () => {
  const active = switchSlot(base(), 'condition', 'Stim')

  const spec = removeCombine(active, 0)

  assert.deepEqual(spec.level_groups!.map(g => g.name), ['Other'])
  assert.deepEqual(spec.groups, ['condition', 'session'])
  assert.equal(spec.roles!.condition, 'group')
  assert.equal(spec.color, 'condition')
})

test('renaming the active combine carries its slot to the new name', () => {
  const active = switchSlot(base(), 'condition', 'Stim')

  const spec = renameCombine(active, 0, 'IsStim')

  assert.equal(spec.level_groups![0].name, 'IsStim')
  assert.deepEqual(spec.groups, ['IsStim', 'session'])
  assert.equal(spec.roles!.IsStim, 'group')
  assert.equal(spec.color, 'IsStim')
})

// --- the bucket model -------------------------------------------------------------

test('buckets read in mapping order, members in source order, empty labels dropped', () => {
  const group = stim({ mapping: { sham: 'SHAM', stim2: 'STIM', stim1: 'STIM', stim3: '' } })

  assert.deepEqual(readBuckets(group, LEVELS), [
    { label: 'SHAM', levels: ['sham'] },
    { label: 'STIM', levels: ['stim1', 'stim2'] },
  ])
  assert.deepEqual(unusedLevels(readBuckets(group, LEVELS), LEVELS), ['stim3'])
})

test('writing buckets keeps bucket order — the legend order', () => {
  const mapping = writeBuckets([
    { label: 'SHAM', levels: ['sham'] },
    { label: 'STIM', levels: ['stim1', 'stim2'] },
  ])

  assert.deepEqual(Object.entries(mapping), [
    ['sham', 'SHAM'],
    ['stim1', 'STIM'],
    ['stim2', 'STIM'],
  ])
})

test('assigning a level to a bucket takes it out of its old one', () => {
  const buckets = readBuckets(stim(), LEVELS)

  const next = assignLevels(buckets, ['stim3'], 'SHAM', LEVELS)

  assert.deepEqual(next, [
    { label: 'STIM', levels: ['stim1', 'stim2'] },
    { label: 'SHAM', levels: ['stim3', 'sham'] },
  ])
})

test('a new bucket is appended, and a bucket left empty disappears', () => {
  const buckets = [{ label: 'X', levels: ['sham'] }]

  const next = assignLevels(buckets, ['sham'], 'Y', LEVELS)

  assert.deepEqual(next, [{ label: 'Y', levels: ['sham'] }])
})

test('assigning to null drops the levels from every bucket', () => {
  const next = assignLevels(readBuckets(stim(), LEVELS), ['stim1', 'sham'], null, LEVELS)

  assert.deepEqual(next, [{ label: 'STIM', levels: ['stim2', 'stim3'] }])
})

test('renaming a bucket keeps its position; renaming onto another merges', () => {
  const buckets = readBuckets(stim(), LEVELS)

  assert.deepEqual(renameBucket(buckets, 'STIM', 'ON').map(b => b.label), ['ON', 'SHAM'])
  assert.deepEqual(renameBucket(buckets, 'SHAM', 'STIM'), [
    { label: 'STIM', levels: ['stim1', 'stim2', 'stim3', 'sham'] },
  ])
  assert.equal(renameBucket(buckets, 'STIM', ''), buckets, 'an empty name is ignored')
})

test('shift-click selects the range between anchor and click, either direction', () => {
  assert.deepEqual(levelRange(LEVELS, 'stim1', 'stim3'), ['stim1', 'stim2', 'stim3'])
  assert.deepEqual(levelRange(LEVELS, 'sham', 'stim2'), ['stim2', 'stim3', 'sham'])
  assert.deepEqual(levelRange(LEVELS, null, 'stim2'), ['stim2'])
  assert.deepEqual(levelRange(LEVELS, 'gone', 'stim2'), ['stim2'])
})

test('the collapsed-row summary counts buckets and dropped levels', () => {
  assert.equal(combineSummary(stim(), LEVELS), '4 → 2')
  assert.equal(combineSummary(stim({ mapping: { stim1: 'STIM' } }), LEVELS), '4 → 1, 3 dropped')
  assert.equal(
    combineSummary(stim({ mapping: { stim1: 'STIM' }, unmatched: 'other' }), LEVELS),
    '4 → 2'
  )
})
