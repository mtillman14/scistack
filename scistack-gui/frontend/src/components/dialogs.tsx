/**
 * In-app replacements for window.confirm / window.prompt / window.alert —
 * the ONE owner of "ask the user a quick question".
 *
 * A VS Code webview has no native dialogs: confirm returns false, prompt
 * returns null and alert shows nothing, all silently. So every
 * `if (!window.confirm(...)) return` was a button that did nothing in the
 * extension (Expected locations → Unmark, user report 2026-11-17).
 *
 * Call sites keep their shape and await instead:
 *
 *     if (!(await askConfirm('Stop using X?'))) return
 *     const name = await askText('New name', 'old')   // null = cancelled
 *     void showMessage(`Exported to:\n${path}`)
 *
 * <DialogHost /> (mounted once per root in main.tsx) draws them, one at a
 * time, in order. The message text is selectable, so a path in it can be
 * copied. Without a host (unit tests), a question answers "cancel" and says
 * so in the console, never a silent no-op.
 */

import { useEffect, useRef, useState } from 'react'
import type { CSSProperties } from 'react'
import * as modalStyles from './modalStyles'

type Request =
  | { kind: 'confirm'; message: string; okLabel: string; resolve: (v: boolean) => void }
  | { kind: 'text'; message: string; defaultValue: string; resolve: (v: string | null) => void }
  | { kind: 'message'; message: string; resolve: () => void }

const queue: Request[] = []
let notify: (() => void) | null = null

function enqueue(req: Request): void {
  console.info(`[dialogs] ask ${req.kind}: ${req.message.split('\n')[0]}`)
  if (!notify) {
    console.warn(`[dialogs] no DialogHost mounted; ${req.kind} answered as cancelled: ${req.message}`)
    if (req.kind === 'confirm') req.resolve(false)
    else if (req.kind === 'text') req.resolve(null)
    else req.resolve()
    return
  }
  queue.push(req)
  notify()
}

/** true = the user chose OK. */
export function askConfirm(message: string, okLabel = 'OK'): Promise<boolean> {
  return new Promise(resolve => enqueue({ kind: 'confirm', message, okLabel, resolve }))
}

/** The text typed, or null when cancelled. */
export function askText(message: string, defaultValue = ''): Promise<string | null> {
  return new Promise(resolve => enqueue({ kind: 'text', message, defaultValue, resolve }))
}

/** Resolves when dismissed; callers usually don't wait (`void showMessage(...)`). */
export function showMessage(message: string): Promise<void> {
  return new Promise(resolve => enqueue({ kind: 'message', message, resolve }))
}

export function DialogHost() {
  const [current, setCurrent] = useState<Request | null>(null)
  const [text, setText] = useState('')
  const inputRef = useRef<HTMLInputElement>(null)
  // The request on screen, outside React state: the queue is shifted here,
  // never inside a state updater (StrictMode runs updaters twice).
  const shown = useRef<Request | null>(null)

  const show = (r: Request | null) => {
    shown.current = r
    if (r?.kind === 'text') setText(r.defaultValue)
    setCurrent(r)
  }

  useEffect(() => {
    const next = () => { if (!shown.current) show(queue.shift() ?? null) }
    notify = next
    next()
    return () => { if (notify === next) notify = null }
  }, [])

  useEffect(() => {
    if (current?.kind === 'text') inputRef.current?.select()
  }, [current])

  if (!current) return null

  const finish = (ok: boolean) => {
    const req = current
    if (req.kind === 'confirm') req.resolve(ok)
    else if (req.kind === 'text') req.resolve(ok ? text : null)
    else req.resolve()
    console.info(`[dialogs] answered ${req.kind}: ${ok ? 'ok' : 'cancel'}`)
    show(queue.shift() ?? null)
  }

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') { e.stopPropagation(); finish(false) }
    if (e.key === 'Enter' && !e.shiftKey) { e.stopPropagation(); e.preventDefault(); finish(true) }
  }

  return (
    <div style={{ ...modalStyles.overlay, zIndex: 20000 }} onKeyDown={onKey} data-testid="app-dialog">
      <div style={{ ...modalStyles.dialog, minWidth: 360 }} onClick={e => e.stopPropagation()}>
        <div style={s.message} data-testid="app-dialog-message">{current.message}</div>
        {current.kind === 'text' && (
          <input
            ref={inputRef}
            style={s.input}
            value={text}
            onChange={e => setText(e.target.value)}
            autoFocus
            data-testid="app-dialog-input"
          />
        )}
        <div style={s.buttons}>
          {current.kind !== 'message' && (
            <button type="button" style={s.btn} onClick={() => finish(false)} data-testid="app-dialog-cancel">
              Cancel
            </button>
          )}
          <button
            type="button"
            style={s.primary}
            onClick={() => finish(true)}
            autoFocus={current.kind !== 'text'}
            data-testid="app-dialog-ok"
          >
            {current.kind === 'confirm' ? current.okLabel : 'OK'}
          </button>
        </div>
      </div>
    </div>
  )
}

const s: Record<string, CSSProperties> = {
  message: { whiteSpace: 'pre-wrap', wordBreak: 'break-word', userSelect: 'text', marginBottom: 12, lineHeight: 1.4 },
  input: {
    width: '100%', boxSizing: 'border-box', background: '#11111f', color: '#eee',
    border: '1px solid #3a3a5a', borderRadius: 3, padding: '4px 6px', marginBottom: 12, fontSize: 13,
  },
  buttons: { display: 'flex', justifyContent: 'flex-end', gap: 8 },
  btn: { background: 'transparent', border: '1px solid #3a3a5a', borderRadius: 3, color: '#ccc', padding: '4px 12px', cursor: 'pointer' },
  primary: { background: '#7b68ee', border: 'none', borderRadius: 3, color: '#fff', padding: '4px 14px', cursor: 'pointer' },
}
