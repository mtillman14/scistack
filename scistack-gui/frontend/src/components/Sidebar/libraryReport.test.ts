/**
 * The authoring report as the Share / copy dialogs show it
 * (.claude/plan-library-fork-chain.md O24).
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { isLibraryName, reportLines, suggestCopyName, type AuthoringReport } from './libraryReport.js'

const report: AuthoringReport = {
  action: 'copy',
  library: 'emg_bob',
  folder: '/p/libs/emg_bob',
  pipeline: null,
  source: 'emg_alice',
  files: ['pyproject.toml', 'src/emg_bob/steps.py'],
  functions: { 'emg_alice.filter_emg': 'emg_bob.filter_emg', 'emg_alice.envelope': 'emg_bob.envelope' },
  kept_labels: [],
  variables: [],
  parameters: [],
  path_inputs: [],
  dependencies: ['numpy'],
  derived_from: ['emg_alice'],
  step_values: [],
  carried: { 'emg_bob.filter_emg': 2 },
  not_carried: [{ function: 'emg_bob.envelope', reason: 'its code changed' }],
  warnings: [],
  install: 'pip install -e /p/libs/emg_bob',
}

test('every rename says whether its results carried or why not', () => {
  const lines = reportLines(report)
  assert.equal(lines[0], 'Copied as emg_bob (2 file(s) written) in /p/libs/emg_bob')
  assert.ok(lines.includes(
    'emg_alice.filter_emg → emg_bob.filter_emg — results carried (2 record(s), nothing re-run)'))
  assert.ok(lines.includes('emg_alice.envelope → emg_bob.envelope — needs a run: its code changed'))
  assert.ok(lines.includes('Depends on: numpy'))
})

test('copy names', () => {
  assert.equal(suggestCopyName('emg_alice'), 'emg_alice_mine')
  assert.equal(isLibraryName('emg_bob'), true)
  assert.equal(isLibraryName('emg-bob'), false)
  assert.equal(isLibraryName('1lib'), false)
})
