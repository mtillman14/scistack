/**
 * Tests for pipelineTreeCore — run with `npm test` in extension/.
 * Pins: steps come in pipeline order; a row says scidb's verdict words; the
 * outputs carry their outdated variants; exclusions read key=value (scope).
 */

import { test } from 'node:test';
import * as assert from 'node:assert';
import { exclusionRows, stepRows, stepsInOrder, type GraphPayload } from './pipelineTreeCore';

const graph: GraphPayload = {
  nodes: [
    { id: 'fn__b', type: 'functionNode', data: { label: 'b_rms', output_types: ['RMS'] } },
    { id: 'fn__a', type: 'functionNode', data: {
      label: 'z_filter', output_types: ['Filt'],
      verdict: {
        word: 'not_complete', glyph: '✕', label: 'Not complete', status_line: '✕ Not complete · ✕ 3 failed',
        chip: '✕ 3 failed', new_chip: '＋2 new', reasons: [{ kind: 'failed', count: 3, text: '✕ 3 failed' }],
        needs_running: true,
      },
    } },
    { id: 'var__Raw', type: 'variableNode', data: { label: 'Raw' } },
    { id: 'var__Filt', type: 'variableNode', data: { label: 'Filt', total_records: 10, outdated_variants: 2 } },
    { id: 'var__RMS', type: 'variableNode', data: { label: 'RMS' } },
  ],
  edges: [
    { source: 'var__Raw', target: 'fn__a' },
    { source: 'fn__a', target: 'var__Filt' },
    { source: 'var__Filt', target: 'fn__b' },
    { source: 'fn__b', target: 'var__RMS' },
  ],
  exclusions: [{ keys: { subject: 'S01' }, reason: 'withdrew', changed_at: '2026-11-20T10:11:12', variable: 'Raw' }],
};

test('steps follow the pipeline, not the alphabet', () => {
  assert.deepStrictEqual(stepsInOrder(graph).map(n => n.data.label), ['z_filter', 'b_rms']);
});

test('a row carries the verdict words and the new chip', () => {
  const [first, second] = stepRows(graph);
  assert.strictEqual(first.description, '✕ Not complete — ✕ 3 failed · ＋2 new');
  assert.deepStrictEqual(first.reasons, [{ kind: 'failed', text: '✕ 3 failed' }]);
  assert.deepStrictEqual(first.outputs, [{ variable: 'Filt', description: '10 record(s) · 2 outdated variant(s)' }]);
  assert.strictEqual(second.description, 'no verdict yet');
});

test('exclusions read key=value with their scope and time', () => {
  assert.deepStrictEqual(exclusionRows(graph), [
    { label: 'subject=S01 (Raw)', description: 'withdrew · 2026-11-20 10:11' },
  ]);
});
