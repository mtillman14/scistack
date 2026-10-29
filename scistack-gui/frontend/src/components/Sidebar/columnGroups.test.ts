import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  addColumnToGroup,
  describeOutputHandle,
  describeOverlaps,
  describePlan,
  groupsFromSelection,
  moveGroup,
  removeColumnFromGroup,
  removeGroup,
  renameGroup,
  selectionFromGroups,
  type GroupPlan,
} from './columnGroups.js'

const SIX = ['6MWT', '6MWT_Date']
const TEN = ['10MWT', '10MWT_Date']

test('groupsFromSelection reads stored groups, or one group per picked column', () => {
  assert.deepEqual(groupsFromSelection(undefined), [])
  assert.deepEqual(
    groupsFromSelection({ columns: ['a', 'b'], iterate: true }),
    [{ columns: ['a'] }, { columns: ['b'] }],
  )
  assert.deepEqual(
    groupsFromSelection({ columns: [...SIX, ...TEN], iterate: true, groups: [{ name: 'six', columns: SIX }, { columns: TEN }] }),
    [{ name: 'six', columns: SIX }, { columns: TEN }],
  )
})

test('selectionFromGroups names groups for one output and not for several', () => {
  const groups = [{ columns: SIX }, { name: 'ten', columns: TEN }]
  assert.deepEqual(selectionFromGroups(groups, 1), {
    columns: [...SIX, ...TEN],
    iterate: true,
    groups: [{ name: '6MWT', columns: SIX }, { name: 'ten', columns: TEN }],
  })
  assert.deepEqual(selectionFromGroups(groups, 2).groups, [{ columns: SIX }, { columns: TEN }])
})

test('the union lists a shared column once', () => {
  const sel = selectionFromGroups([{ columns: ['a', 'd'] }, { columns: ['b', 'd'] }], 2)
  assert.deepEqual(sel.columns, ['a', 'd', 'b'])
})

test('editing helpers', () => {
  const g = [{ columns: SIX }, { columns: TEN }]
  assert.deepEqual(moveGroup(g, 1, -1), [{ columns: TEN }, { columns: SIX }])
  assert.equal(moveGroup(g, 0, -1), g)
  assert.deepEqual(removeGroup(g, 0), [{ columns: TEN }])
  assert.deepEqual(addColumnToGroup(g, 0, 'x')[0].columns, [...SIX, 'x'])
  assert.deepEqual(addColumnToGroup(g, 0, '6MWT')[0].columns, SIX)
  assert.deepEqual(removeColumnFromGroup(g, 1, '10MWT')[1].columns, ['10MWT_Date'])
  assert.equal(renameGroup(g, 0, 'six')[0].name, 'six')
})

test('describePlan and describeOverlaps word the backend answer', () => {
  const split: GroupPlan = {
    ok: true,
    mode: 'split',
    groups: [{ name: 'SixMWT', columns: SIX }],
    overlaps: { d: ['A', 'B'] },
    error: null,
  }
  assert.equal(describePlan(split, ['SixMWT', 'TenMWT']), 'Split: each group saves to its own output')
  assert.equal(
    describePlan({ ...split, mode: 'combine' }, ['Obj2']),
    'Combine into Obj2: one column per group',
  )
  assert.equal(describePlan({ ...split, ok: false, error: 'counts must match' }, []), 'counts must match')
  assert.equal(describePlan(null, []), '')
  assert.equal(describeOverlaps(split), 'Shared by more than one group — each still receives it: d (in A, B)')
  assert.equal(describeOverlaps({ ...split, overlaps: {} }), '')
})

test('describeOutputHandle lists what each output receives', () => {
  assert.equal(describeOutputHandle('SixMWT', undefined), 'SixMWT')
  assert.equal(
    describeOutputHandle('SixMWT', [{ name: 'SixMWT', columns: SIX }]),
    'SixMWT\nfrom columns: 6MWT, 6MWT_Date',
  )
  assert.equal(
    describeOutputHandle('Walks', [{ name: '6MWT', columns: SIX }, { name: '10MWT', columns: TEN }]),
    'Walks\n6MWT ← 6MWT, 6MWT_Date\n10MWT ← 10MWT, 10MWT_Date',
  )
})
