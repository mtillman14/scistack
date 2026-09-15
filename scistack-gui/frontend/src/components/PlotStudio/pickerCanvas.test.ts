/**
 * The picker canvas never mounts execution state — a source guard.
 *
 * The DAG node components (`components/DAG/FunctionNode`, `ParameterNode`)
 * branch on whether `VariantSelectionContext` is PRESENT: with it they render
 * their inert selection bodies; without it they render the pipeline bodies,
 * which call `useScope`/`useRunLog` and throw wherever those providers are not
 * mounted — which is the whole Plot Studio tab (`PlotRoot`). On 2026-09-15 the
 * grouping picker's first step drew the canvas without a provider and the tab
 * went blank, with nothing in scidb.log to say why.
 *
 * The fix made the provider the dialog's job (`PickerDialog` takes `selection`
 * and wraps the canvas itself). This test keeps it there. There is no React
 * render harness in this suite (see tsconfig.test.json), so the rule is checked
 * against the SOURCE, the way the Python AST guards do: every `<ReactFlow` under
 * PlotStudio/ lives in DagPicker.tsx, inside a `<VariantSelectionProvider`, and
 * no picker reaches around the dialog to wrap the canvas itself.
 *
 * Runs from `frontend/` (`npm test`), which is where the sources are resolved.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'
import { readdirSync, readFileSync } from 'node:fs'
import { join, resolve } from 'node:path'

const PLOT_STUDIO = resolve(process.cwd(), 'src/components/PlotStudio')
const CONTEXT = resolve(process.cwd(), 'src/context/VariantSelectionContext.tsx')

const tsxFiles = () =>
  readdirSync(PLOT_STUDIO)
    .filter(name => name.endsWith('.tsx'))
    .map(name => [name, readFileSync(join(PLOT_STUDIO, name), 'utf8')] as const)

test('only DagPicker mounts a ReactFlow canvas under PlotStudio', () => {
  const mounting = tsxFiles()
    .filter(([, source]) => /<ReactFlow[\s>]/.test(source))
    .map(([name]) => name)
  assert.deepEqual(mounting, ['DagPicker.tsx'])
})

test('DagPicker wraps its canvas in VariantSelectionProvider', () => {
  const source = readFileSync(join(PLOT_STUDIO, 'DagPicker.tsx'), 'utf8')
  const provider = source.indexOf('<VariantSelectionProvider')
  const canvas = source.search(/<ReactFlow[\s>]/)
  const providerEnd = source.indexOf('</VariantSelectionProvider>')
  assert.ok(provider >= 0, 'DagPicker must mount VariantSelectionProvider')
  assert.ok(provider < canvas && canvas < providerEnd,
    'the ReactFlow canvas must sit inside the provider')
  // The provider is fed by a REQUIRED prop, so no caller can leave it out.
  assert.match(source, /\n\s+selection: VariantSelectionValue\n/)
})

test('no picker wraps the canvas itself', () => {
  for (const [name, source] of tsxFiles()) {
    assert.ok(!source.includes('canvasWrapper'),
      `${name} reaches around PickerDialog with canvasWrapper`)
    if (name !== 'DagPicker.tsx') {
      assert.ok(!source.includes('<VariantSelectionProvider'),
        `${name} mounts its own VariantSelectionProvider; pass selection= instead`)
    }
  }
})

test('a step with nothing to select still gets a selection', () => {
  const context = readFileSync(CONTEXT, 'utf8')
  assert.match(context, /export const INERT_VARIANT_SELECTION: VariantSelectionValue/)
  const grouping = readFileSync(join(PLOT_STUDIO, 'GroupingDagPopup.tsx'), 'utf8')
  assert.match(grouping, /selection=\{choosing \? INERT_VARIANT_SELECTION : /)
})
