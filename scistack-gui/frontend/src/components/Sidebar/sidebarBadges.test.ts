import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  badgesFor,
  functionBadges,
  runOptionBadges,
  type FunctionBadgeInput,
  type RunOptionDefaults,
} from './sidebarBadges.js'

const DEFAULTS: RunOptionDefaults = {
  dry_run: false,
  save: true,
  distribute: false,
  as_table: false,
  on_error: 'continue',
  on_duplicate: 'error',
}

const BLANK: FunctionBadgeInput = {
  runOptions: {},
  defaults: DEFAULTS,
  hiddenComboCount: 0,
  whereFilterCount: 0,
  schemaSelection: { include: [], exclude_levels: {} },
  columnSelections: {},
}

test('defaults give no badges', () => {
  assert.deepEqual(functionBadges(BLANK), [])
  assert.deepEqual(runOptionBadges({ ...DEFAULTS }, DEFAULTS), [])
})

test('an empty or missing run-options blob reads as the defaults', () => {
  assert.deepEqual(runOptionBadges({}, DEFAULTS), [])
  assert.deepEqual(runOptionBadges(null, DEFAULTS), [])
  assert.deepEqual(runOptionBadges(undefined, DEFAULTS), [])
})

test('no defaults yet (request in flight) gives no run-option badges', () => {
  assert.deepEqual(runOptionBadges({ dry_run: true }, null), [])
})

test('each flipped run option gives exactly one badge, with the right tone', () => {
  const cases: Array<[Partial<RunOptionDefaults>, string, 'warn' | 'info']> = [
    [{ dry_run: true }, 'Dry run', 'warn'],
    [{ save: false }, 'Not saving', 'warn'],
    [{ distribute: true }, 'Distribute', 'info'],
    [{ as_table: true }, 'As table', 'info'],
    [{ on_error: 'stop' }, 'Stop on error', 'warn'],
    [{ on_duplicate: 'combine' }, 'Combine duplicates', 'info'],
  ]
  for (const [opts, label, tone] of cases) {
    const badges = runOptionBadges(opts, DEFAULTS)
    assert.equal(badges.length, 1, label)
    assert.equal(badges[0].label, label)
    assert.equal(badges[0].tone, tone)
    assert.equal(badges[0].section, 'run_options')
  }
})

test('defaults are compared, not assumed: a different default flips the answer', () => {
  const saveOff = { ...DEFAULTS, save: false }
  assert.deepEqual(runOptionBadges({ save: false }, saveOff), [])
  assert.equal(runOptionBadges({ save: true }, saveOff)[0].label, 'Saving')
})

test('hidden combos, filters, subset and columns each badge their section', () => {
  const badges = functionBadges({
    ...BLANK,
    hiddenComboCount: 3,
    whereFilterCount: 2,
    schemaSelection: { include: [[['subject', '01']]], exclude_levels: {} },
    columnSelections: { x: { columns: ['a'], iterate: false } },
  })
  assert.deepEqual(badges.map(b => [b.section, b.label]), [
    ['variants', '3 hidden'],
    ['inputs', 'Columns picked'],
    ['filters', '2 filters'],
    ['schema', 'Subset'],
  ])
  assert.equal(badgesFor(badges, 'filters').length, 1)
})

test('for_columns shows "Per column" (or "Per group") instead of "Columns picked"', () => {
  assert.equal(
    functionBadges({ ...BLANK, columnSelections: { x: { columns: [], iterate: true } } })[0].label,
    'Per column',
  )
  assert.equal(
    functionBadges({
      ...BLANK,
      columnSelections: { x: { columns: [], iterate: true, groups: [{ name: 'g', columns: ['a'] }] } },
    })[0].label,
    'Per group',
  )
})

test('an exclude-only selection is a subset; an empty exclude list is not', () => {
  assert.equal(
    functionBadges({ ...BLANK, schemaSelection: { include: [], exclude_levels: { trial: ['3'] } } })[0].label,
    'Subset',
  )
  assert.deepEqual(
    functionBadges({ ...BLANK, schemaSelection: { include: [], exclude_levels: { trial: [] } } }),
    [],
  )
})
