/**
 * The WebSocket follows the page's origin — run with `npm test` in frontend/.
 */

import { test } from 'node:test'
import * as assert from 'node:assert'
import { wsUrl } from './wsUrl.js'

test('the socket uses the port the page was served from', () => {
  assert.strictEqual(wsUrl({ protocol: 'http:', host: '127.0.0.1:9123' }), 'ws://127.0.0.1:9123/ws')
})

test('the default port is not special', () => {
  assert.strictEqual(wsUrl({ protocol: 'http:', host: 'localhost:8765' }), 'ws://localhost:8765/ws')
})

test('a page served over https gets a secure socket', () => {
  assert.strictEqual(wsUrl({ protocol: 'https:', host: 'lab.example.org' }), 'wss://lab.example.org/ws')
})

test('a host without a port keeps none', () => {
  assert.strictEqual(wsUrl({ protocol: 'http:', host: 'localhost' }), 'ws://localhost/ws')
})
