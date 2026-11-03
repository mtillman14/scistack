/**
 * `api-types.ts` is what `scripts/gen-types.mjs` makes of `api-schema.json`.
 *
 * Both files are generated and committed (P5 stage 5, design review G8). The
 * backend's `tests/test_api_handlers.py` holds the JSON to the handler table;
 * this holds the TypeScript to the JSON, so neither can go stale silently.
 * Regenerate both with `python scistack-gui/tools/gen_ts_types.py`.
 *
 * Until the backend has written `api-schema.json` once (its pytest writes it
 * when missing), there is nothing to compare and the test says so.
 */

import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { test } from 'node:test'
import { fileURLToPath, pathToFileURL } from 'node:url'

/** The frontend folder, found by walking up to its package.json (the
 * compiled copy runs from dist/test, at another depth than the source). */
function frontendRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url))
  for (let up = 0; up < 10; up++) {
    if (existsSync(join(dir, 'scripts', 'gen-types.mjs'))) return dir
    dir = dirname(dir)
  }
  throw new Error('frontend/scripts/gen-types.mjs not found walking up from this test')
}

const root = frontendRoot()
const schemaFile = join(root, 'src', 'generated', 'api-schema.json')
const typesFile = join(root, 'src', 'generated', 'api-types.ts')

test(
  'api-types.ts is current with api-schema.json',
  { skip: existsSync(schemaFile) ? false : 'api-schema.json not generated yet (run the GUI pytest)' },
  async () => {
    const generator = await import(pathToFileURL(join(root, 'scripts', 'gen-types.mjs')).href)
    const expected: string = await generator.generate(schemaFile)
    assert.ok(existsSync(typesFile), 'api-types.ts missing: run `npm run gen:types`')
    assert.equal(
      readFileSync(typesFile, 'utf8'),
      expected,
      'api-types.ts is stale: run `npm run gen:types`'
    )
  }
)

/** The spec types are generated from the backend's wire models
 *  (scistack_gui.api.spec_wire). A hand-written twin of one would drift from
 *  `to_dict` again, so these files may only re-export them. */
test('the spec types are not hand-written again', () => {
  const twins: [string, RegExp][] = [
    [join(root, 'src', 'components', 'StatStudio', 'statsSpec.ts'), /\binterface (StatsSpec|Analysis|DV|CovariateBinding|Formatting|Overlay|GuideState)\b/m],
    [join(root, 'src', 'components', 'PlotStudio', 'PlotStudio.tsx'), /\binterface (CustomPlotSpec|Spec|FactorVariable|Filter|VariantSet)\b|^type Role = '/m],
    [join(root, 'src', 'components', 'PlotStudio', 'plotSpecReducer.ts'), /\binterface (PlotSpecFields|FacetOptions|YAxis|Matcher|StylePatch)\b|^export type MatchOp = '/m],
    [join(root, 'src', 'components', 'PlotStudio', 'continuousKinds.ts'), /\binterface (HistogramOptions|RegressionOptions|KdeOptions)\b|^export type (BinMode|HistStat|HistMultiple|FitBy|StatsCorner) = '/m],
    [join(root, 'src', 'components', 'PlotStudio', 'compare.ts'), /\binterface Comparison\b|^export type CompareMode = '/m],
    [join(root, 'src', 'components', 'PlotStudio', 'differenceBars.ts'), /\binterface DifferenceBar\b/m],
    [join(root, 'src', 'components', 'PlotStudio', 'panelOverrides.ts'), /\binterface PanelOverride\b|^export type YTitles = '/m],
    [join(root, 'src', 'components', 'PlotStudio', 'combine.ts'), /\binterface LevelGroup\b/m],
    [join(root, 'src', 'components', 'PlotStudio', 'aliasEdit.ts'), /\binterface AliasEntry\b/m],
    [join(root, 'src', 'components', 'PlotStudio', 'kindTiles.ts'), /^export type TileRole = '/m],
    [join(root, 'src', 'components', 'PlotStudio', 'locationSelection.ts'), /^export type Role = '/m],
  ]
  for (const [file, pattern] of twins) {
    assert.doesNotMatch(readFileSync(file, 'utf8'), pattern, `${file} declares a spec type by hand`)
  }
})
