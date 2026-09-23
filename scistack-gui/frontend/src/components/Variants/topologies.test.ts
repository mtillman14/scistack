/**
 * The Variants panel's wording rules, over a fixture shaped exactly like the
 * `variable_topologies` reply — one topology, two variants, one superseded.
 *
 * That fixture IS the 2026-09-22 bug: two rows identical but for their run
 * options, one of which a load will no longer return. If the verdict ever
 * stops distinguishing them on screen, the panel is a description again
 * rather than a diagnostic, and these assertions are what say so.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  constantsLabel,
  isNotCurrent,
  locationsLabel,
  topologiesSummary,
  topologyHeading,
  variantsCommand,
} from './topologies.js'
import type { TopologiesReply, TopologyGroup, VariantRow } from './topologies.js'

function variant(over: Partial<VariantRow>): VariantRow {
  return {
    function_name: 'loadGaitRiteOneFile',
    call_id: 'aaaabbbbccccdddd',
    output_type: 'GAITRiteLoaded',
    output_num: null,
    input_types: { gaitRitePath: '<PathInput>' },
    constants: { gaitRiteConfig: '{...}' },
    record_count: 560,
    current: true,
    current_record_count: 560,
    run_options: 'distribute=false',
    function_hash: '4999059d1234abcd',
    first_saved: '2026-09-14 11:02',
    last_saved: '2026-09-19 16:41',
    verdict: 'current',
    verdict_label: 'load: CURRENT',
    locations: { total: 0, keys: [], sample: [] },
    ...over,
  }
}

const SUPERSEDED = variant({
  record_count: 560,
  current: false,
  current_record_count: 0,
  run_options: 'distribute=false',
  verdict: 'superseded',
  verdict_label: 'load: SUPERSEDED (an older run-option set)',
  locations: {
    total: 420,
    keys: ['subject', 'session', 'speed'],
    sample: [
      { subject: 'SS01', session: 'BL', speed: 'SSV' },
      { subject: 'SS01', session: 'BL', speed: 'FV' },
      { subject: 'SS02', session: 'BL', speed: 'SSV' },
    ],
  },
})

const CURRENT = variant({
  record_count: 450,
  current: true,
  current_record_count: 450,
  run_options: 'distribute=true',
  first_saved: '2026-09-22 13:49',
  last_saved: '2026-09-22 13:50',
  locations: {
    total: 450,
    keys: ['subject', 'session', 'speed', 'trial'],
    sample: [{ subject: 'SS01', session: 'BL', speed: 'SSV', trial: 1 }],
  },
})

const TOPOLOGY: TopologyGroup = {
  function_name: 'loadGaitRiteOneFile',
  input_types: [
    ['gaitRitePath', '<PathInput>'],
    ['gaitRiteConfig', 'GaitRiteConfig'],
  ],
  output_type: 'GAITRiteLoaded',
  variants: [SUPERSEDED, CURRENT],
}

const REPLY: TopologiesReply = {
  variable: 'GAITRiteLoaded',
  command: 'scidb variants GAITRiteLoaded',
  topology_count: 1,
  variant_count: 2,
  topologies: [TOPOLOGY],
}

test('the heading names the function, what feeds it and what it makes', () => {
  assert.equal(
    topologyHeading(TOPOLOGY),
    'loadGaitRiteOneFile(gaitRitePath: <PathInput>, gaitRiteConfig: GaitRiteConfig) → GAITRiteLoaded'
  )
})

test('a topology with no inputs still reads as a call, not a blank', () => {
  assert.equal(
    topologyHeading({ ...TOPOLOGY, input_types: [] }),
    'loadGaitRiteOneFile((no inputs)) → GAITRiteLoaded'
  )
})

test('constants are sorted, so the same variant reads the same every open', () => {
  const v = variant({ constants: { zed: '1', alpha: '2' } })
  assert.equal(constantsLabel(v), 'alpha=2, zed=1')
})

test('a variant with no constants says so rather than showing an empty row', () => {
  assert.equal(constantsLabel(variant({ constants: {} })), 'no constants')
})

test('the superseded variant is distinguished from the current one', () => {
  assert.equal(isNotCurrent(SUPERSEDED.verdict), true)
  assert.equal(isNotCurrent(CURRENT.verdict), false)
  assert.match(SUPERSEDED.verdict_label, /SUPERSEDED/)
  assert.match(CURRENT.verdict_label, /CURRENT/)
})

test('partial supersession is NOT rounded to either of the other two', () => {
  assert.equal(isNotCurrent('partially_superseded'), true)
  assert.notEqual('partially_superseded', 'superseded')
})

test('locations name their keys and count what is not shown', () => {
  assert.equal(
    locationsLabel(SUPERSEDED.locations),
    'subject/session/speed — SS01/BL/SSV, SS01/BL/FV, SS02/BL/SSV, … (+417)'
  )
})

test('a fully-sampled location set carries no "+n" tail', () => {
  assert.equal(
    locationsLabel({
      total: 1,
      keys: ['subject'],
      sample: [{ subject: 'SS01' }],
    }),
    'subject — SS01'
  )
})

test('a variant at no locations says so instead of rendering an empty line', () => {
  assert.equal(locationsLabel({ total: 0, keys: [], sample: [] }), 'no locations')
})

test('the terminal equivalent is printable for both detail levels', () => {
  assert.equal(variantsCommand('GAITRiteLoaded'), 'scidb variants GAITRiteLoaded')
  assert.equal(
    variantsCommand('GAITRiteLoaded', true),
    'scidb variants GAITRiteLoaded --locations'
  )
})

test('the summary leads with what a load will not return', () => {
  assert.equal(
    topologiesSummary(REPLY),
    '1 topology/ies, 2 variant(s) — 1 a load would not fully return'
  )
})

test('an all-current variable says so rather than counting to zero', () => {
  const clean: TopologiesReply = {
    ...REPLY,
    variant_count: 1,
    topologies: [{ ...TOPOLOGY, variants: [CURRENT] }],
  }
  assert.equal(topologiesSummary(clean), '1 topology/ies, 1 variant(s) — all current')
})
