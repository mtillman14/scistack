import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { entryPoint, reportStudioOpen, studioOpenParams } from './studioOpen.js'

test('a known entry point is kept, anything else is unknown', () => {
  assert.equal(entryPoint('variable-menu'), 'variable-menu')
  assert.equal(entryPoint('stat-node'), 'stat-node')
  assert.equal(entryPoint('somewhere'), 'unknown')
  assert.equal(entryPoint(undefined), 'unknown')
})

test('the log params name the kind, the way in and the variable', () => {
  assert.deepEqual(studioOpenParams('stat', 'variable-sidebar', 'Rating'),
    { kind: 'stat', opened_from: 'variable-sidebar', variable: 'Rating' })
  assert.deepEqual(studioOpenParams('plot', null, ''), { kind: 'plot', opened_from: 'unknown', variable: null })
})

test('a failing log call never throws', async () => {
  const seen: string[] = []
  reportStudioOpen(async method => { seen.push(method); throw new Error('offline') }, 'plot', 'palette', 'X')
  await new Promise(resolve => setTimeout(resolve, 0))
  assert.deepEqual(seen, ['report_studio_open'])
})

// Role words have ONE owner: scistackplot.role_labels, reported by the
// backend. A studio that hardcodes one is the divergence this guards against
// (StatStudio used to say "Collapse (average)" while Plot Studio said
// "Collapse", and the wizard said "Average over").
const ROLE_WORDS = [
  'Separate figures', 'Separate panels', 'Separate tables', 'Separate sections',
  'Collapse (average)', 'Group (columns)', 'Average over', 'Analyse separately',
]

test('no studio component hardcodes a role word', () => {
  const root = join(process.cwd(), 'src', 'components')
  const offenders: string[] = []
  for (const folder of ['PlotStudio', 'StatStudio', join('StatStudio', 'Guided'), 'Studio']) {
    const dir = join(root, folder)
    for (const name of readdirSync(dir).filter(n => n.endsWith('.tsx'))) {
      const text = readFileSync(join(dir, name), 'utf8')
        .replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/.*$/gm, '') // comments may name things
      for (const word of ROLE_WORDS) {
        if (text.includes(word)) offenders.push(`${folder}/${name}: ${word}`)
      }
    }
  }
  assert.deepEqual(offenders, [])
})
