import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  addAnalysis, addDV, gridToTsv, gridWidth, moveGroup, nextAnalysisId, removeAnalysis, removeDV,
  setCell, setFormatting, setOverride, setRole, targets, toggleHiddenBracket, toggleStatistic,
  updateAnalysis, type Grid, type StatsSpec,
} from './statsSpec.js'

const base = (): StatsSpec => ({
  format: 1,
  design: { measures: ['Ratings'], roles: { brand: 'group', subject: 'collapse' }, groups: ['brand'] },
  dvs: [],
  descriptives: { cell: 'mean_sd', statistics: ['n'] },
  comparisons: [{ id: 'a1', family: 'parametric' }],
  variant_mode: 'pinned',
})

test('targets fall back to the plotted variable', () => {
  assert.deepEqual(targets(base()), [{ variable: 'Ratings' }])
})

test('addDV keeps the implicit first DV and refuses duplicates', () => {
  const two = addDV(base(), 'EMG', ['TA', 'SOL'])
  assert.deepEqual(two.dvs, [{ variable: 'Ratings' }, { variable: 'EMG', columns: ['TA', 'SOL'] }])
  assert.equal(addDV(two, 'EMG', ['TA', 'SOL']), two)
  assert.equal(removeDV(base(), 0).dvs.length, 0, 'the last DV cannot be removed')
  assert.deepEqual(removeDV(two, 0).dvs, [{ variable: 'EMG', columns: ['TA', 'SOL'] }])
})

test('roles keep the grouping order to GROUP holders', () => {
  let spec = setRole(base(), 'session', 'group')
  assert.deepEqual(spec.design.groups, ['brand', 'session'])
  spec = moveGroup(spec, 'brand', 1)
  assert.deepEqual(spec.design.groups, ['session', 'brand'])
  spec = setRole(spec, 'brand', 'iterate')
  assert.deepEqual(spec.design.groups, ['session'])
  assert.equal(moveGroup(spec, 'session', 1), spec, 'nothing outside the list')
})

test('statistics toggle in catalog order', () => {
  const order = ['n', 'mean', 'sd', 'skewness']
  let spec = toggleStatistic(base(), 'skewness', order)
  spec = toggleStatistic(spec, 'mean', order)
  assert.deepEqual(spec.descriptives.statistics, ['n', 'mean', 'skewness'])
  assert.equal(setCell(spec, 'median_iqr').descriptives.cell, 'median_iqr')
})

test('analyses: unique ids, patch, clear a field, overrides', () => {
  let spec = addAnalysis(base(), 'nonparametric')
  assert.equal(spec.comparisons[1].id, 'a2')
  assert.equal(nextAnalysisId(spec), 'a3')
  spec = updateAnalysis(spec, 'a2', { correction: 'fdr_bh', pairwise_test: 'wilcoxon' })
  spec = updateAnalysis(spec, 'a2', { pairwise_test: undefined })
  assert.equal(spec.comparisons[1].correction, 'fdr_bh')
  assert.equal('pairwise_test' in spec.comparisons[1], false)
  spec = setOverride(spec, 'a2', 'brand', 'within')
  assert.deepEqual(spec.comparisons[1].overrides, { brand: 'within' })
  spec = setOverride(spec, 'a2', 'brand', null)
  assert.deepEqual(spec.comparisons[1].overrides, {})
  assert.equal(removeAnalysis(spec, 'a1').comparisons.length, 1)
})

test('formatting: undefined decimals means automatic', () => {
  let spec = setFormatting(base(), { decimals: 2 })
  spec = setFormatting(spec, { decimals: undefined })
  assert.equal('decimals' in (spec.formatting ?? {}), false)
})

test('hidden brackets toggle', () => {
  let spec = toggleHiddenBracket(base(), 'c1')
  assert.deepEqual(spec.overlay?.hidden, ['c1'])
  spec = toggleHiddenBracket(spec, 'c1')
  assert.deepEqual(spec.overlay?.hidden, [])
})

test('grid width and tsv', () => {
  const grid: Grid = {
    title: 'T', caption: 'C', notes: ['n1'], label_columns: 2,
    header_rows: [[{ text: '', span: 2 }, { text: 'pre', span: 2 }], [{ text: 'M', span: 1 }, { text: 'S', span: 1 }, { text: 'a', span: 1 }, { text: 'b', span: 1 }]],
    rows: [{ cells: ['x', 'y', '1', '2'], kind: 'data' }],
  }
  assert.equal(gridWidth(grid), 4)
  const lines = gridToTsv(grid).split('\n')
  assert.equal(lines[1], '\t\tpre\t')
  assert.equal(lines[lines.length - 1], 'n1')
})
