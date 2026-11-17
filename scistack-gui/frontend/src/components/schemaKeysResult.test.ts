/**
 * SchemaKeysDialog's "Schema keys changed" lines, from the backend's
 * `references` (scistack-gui/tests/test_schema_keys_entry_point.py pins the
 * backend half of the same shape).
 */
import assert from 'node:assert/strict'
import { test } from 'node:test'

import { referenceLines } from './schemaKeysResult.js'

test('every place a rename reached gets one line', () => {
  assert.deepEqual(
    referenceLines({
      references: {
        config: ['schema_keys', 'aliases'],
        entities: ['RAW', '[library] schema_keys'],
        gui: { statements: 3, bindings: 1 },
        plots: { _saved_plot: 2, _plot_preset: 1 },
        stats: { _stats_spec: 1, _stats_preset: 0 },
      },
    }),
    [
      'scistack.toml: [schema_keys], [aliases]',
      'Entities file: RAW, [library] schema_keys',
      'Node settings (level, locations, filters): 3',
      'Submodule bindings: 1',
      'Saved plots (every version): 3',
      'Saved stats (every version): 1',
    ],
  )
})

test('adding keys only (no references) says nothing', () => {
  assert.deepEqual(referenceLines({ references: {} }), [])
  assert.deepEqual(referenceLines({}), [])
})

test('places that changed nothing are left out', () => {
  assert.deepEqual(
    referenceLines({
      references: { config: [], entities: [], gui: { statements: 0, bindings: 0 }, plots: {} },
    }),
    [],
  )
})
