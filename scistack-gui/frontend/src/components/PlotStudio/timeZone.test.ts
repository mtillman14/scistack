import { test } from 'node:test'
import assert from 'node:assert/strict'

import {
  LOCAL,
  OTHER,
  formatLimit,
  normalizeMode,
  parseLimitText,
  timeZoneOptions,
  timeZoneSummary,
  typedZone,
  type TimeZoneCapability,
} from './timeZone.js'

const CAP: TimeZoneCapability = {
  applies: true,
  measure: true,
  factors: [],
  mode: 'local',
  zones: ['America/New_York', 'Europe/Berlin'],
  problem: null,
}

test('options: local first, the data zones, UTC, then other', () => {
  const values = timeZoneOptions(CAP, 'local').map(o => o.value)
  assert.deepEqual(values, [LOCAL, 'America/New_York', 'Europe/Berlin', 'UTC', OTHER])
})

test('UTC is not listed twice when the data holds it', () => {
  const values = timeZoneOptions({ ...CAP, zones: ['UTC'] }, 'local').map(o => o.value)
  assert.deepEqual(values, [LOCAL, 'UTC', OTHER])
})

test('a typed zone the data lacks stays listed', () => {
  const values = timeZoneOptions(CAP, 'Asia/Tokyo').map(o => o.value)
  assert.ok(values.includes('Asia/Tokyo'))
})

test('normalizeMode: blank and any spelling of local are local', () => {
  assert.equal(normalizeMode(undefined), LOCAL)
  assert.equal(normalizeMode(''), LOCAL)
  assert.equal(normalizeMode('Local'), LOCAL)
  assert.equal(normalizeMode(' UTC '), 'UTC')
})

test('typedZone: blank keeps the current mode', () => {
  assert.equal(typedZone('  '), null)
  assert.equal(typedZone(' +02:00 '), '+02:00')
})

test('summary only when a datetime is in play', () => {
  assert.equal(timeZoneSummary(undefined), null)
  assert.equal(timeZoneSummary({ ...CAP, applies: false }), null)
  assert.equal(timeZoneSummary(CAP), 'local time')
  assert.equal(timeZoneSummary({ ...CAP, mode: 'UTC' }), 'UTC')
})

test('limits: numbers on a number axis, date text on a datetime axis', () => {
  assert.equal(parseLimitText('', false), null)
  assert.equal(parseLimitText('1.5', false), 1.5)
  assert.equal(parseLimitText('1.', false), 1)
  assert.equal(parseLimitText('abc', false), undefined)
  assert.equal(parseLimitText('2026-07-01', true), '2026-07-01')
  assert.equal(parseLimitText('2026-07-01T12:30', true), '2026-07-01 12:30')
  assert.equal(parseLimitText('2026-07', true), undefined)
  assert.equal(parseLimitText('', true), null)
})

test('applied limits read as wall clocks on a datetime axis', () => {
  const ms = Date.UTC(2026, 6, 1, 12, 30)
  assert.equal(formatLimit(ms, true), '2026-07-01 12:30')
  assert.equal(formatLimit(0.61234, false), '0.612')
})
