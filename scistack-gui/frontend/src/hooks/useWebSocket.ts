/**
 * useWebSocket — connects to the backend WebSocket and exposes incoming messages.
 *
 * The WebSocket stays open for the lifetime of the app. Components subscribe
 * by passing an onMessage callback. The hook reconnects automatically if the
 * connection drops (e.g. backend restart).
 *
 * Exactly ONE socket per page: the module-level singleton below is the only
 * `new WebSocket` in the app, and the readyState guard covers CONNECTING, so
 * React StrictMode's double-mount reuses the in-flight socket rather than
 * opening a second. If the backend ever logs two connections sharing a
 * page_id, that invariant has broken (a duplicate module instance — HMR or
 * two bundles), not "the user opened two tabs".
 *
 * Liveness: the backend reaps a connection that goes silent (api/ws.py
 * CLIENT_SILENT_TIMEOUT_S), so the ping below is REQUIRED, not decorative —
 * without it a live page is dropped and reconnects on a loop.
 */

import { useEffect, useRef, useCallback } from 'react'

type MessageHandler = (msg: Record<string, unknown>) => void

const WS_URL = `ws://${window.location.hostname}:8765/ws`

// Must stay comfortably under api/ws.py's CLIENT_SILENT_TIMEOUT_S so a
// single dropped ping never costs the connection.
const PING_INTERVAL_MS = 20_000

// Identifies this PAGE (not this socket) for the lifetime of the document.
// Two connections reporting the same page_id are one page holding two
// sockets; different page_ids are genuinely different tabs/windows. This is
// the discriminator the backend's bare "(N total)" count could not provide.
const PAGE_ID =
  globalThis.crypto?.randomUUID?.().slice(0, 8) ??
  Math.random().toString(16).slice(2, 10)

// Detect VS Code Webview — if present, WebSocket is not used.
const _isVSCode = typeof acquireVsCodeApi === 'function'

// Module-level singleton so all hook instances share one connection.
let _socket: WebSocket | null = null
let _pingTimer: ReturnType<typeof setInterval> | null = null
const _handlers = new Set<MessageHandler>()

function _stopPing(): void {
  if (_pingTimer !== null) {
    clearInterval(_pingTimer)
    _pingTimer = null
  }
}

function _send(socket: WebSocket, msg: Record<string, unknown>): void {
  if (socket.readyState !== WebSocket.OPEN) return
  try {
    socket.send(JSON.stringify(msg))
  } catch (err) {
    console.warn('[ws] send failed', err)
  }
}

function getSocket(): WebSocket | null {
  if (_isVSCode) return null  // No WebSocket in VS Code Webview mode
  if (_socket && _socket.readyState <= WebSocket.OPEN) return _socket

  const socket = new WebSocket(WS_URL)
  _socket = socket

  // Lifecycle logs: "run stuck on Running…" usually means push messages
  // aren't arriving — the browser console should show whether the socket
  // is even open (pair with the backend's [ws] lines in scistack.log).
  socket.onopen = () => {
    console.info(`[ws] connected to ${WS_URL} (page=${PAGE_ID})`)
    // Identify the page first, so the backend can attribute this connection
    // in its very first log line rather than after the first ping.
    _send(socket, { type: 'hello', page_id: PAGE_ID, url: window.location.href })
    _stopPing()
    _pingTimer = setInterval(() => _send(socket, { type: 'ping' }), PING_INTERVAL_MS)
  }

  socket.onerror = (event) => {
    console.warn('[ws] socket error', event)
  }

  socket.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data)
      _handlers.forEach(h => h(msg))
    } catch {
      // ignore malformed messages
    }
  }

  socket.onclose = (event) => {
    // Only tear down the ping if THIS socket is still the live one — a
    // superseded socket's late onclose must not stop its replacement's
    // heartbeat and get that healthy connection reaped.
    if (_socket === socket) _stopPing()
    console.warn(`[ws] closed (code=${event.code}); reconnecting in 2s`)
    // Reconnect after 2 seconds
    setTimeout(getSocket, 2000)
  }

  return socket
}

export function useWebSocket(onMessage: MessageHandler) {
  const handlerRef = useRef(onMessage)
  handlerRef.current = onMessage

  // Stable wrapper so the Set entry never changes identity
  const stableHandler = useCallback((msg: Record<string, unknown>) => {
    handlerRef.current(msg)
  }, [])

  useEffect(() => {
    if (_isVSCode) return  // No WebSocket in VS Code mode
    getSocket()
    _handlers.add(stableHandler)
    return () => { _handlers.delete(stableHandler) }
  }, [stableHandler])
}

declare function acquireVsCodeApi(): unknown
