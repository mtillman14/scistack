/**
 * ClientErrorBoundary — a render crash becomes a message, not a blank tab.
 *
 * Without a boundary, one component throwing during render unmounts the whole
 * React tree: the webview goes blank, and because a browser-side crash sends
 * no request, nothing reaches `scidb.log` either. That is how the grouping
 * picker failed on 2026-09-15 — the log showed the popup's pipeline fetch and
 * then silence, and the cause had to be re-derived from source.
 *
 * Two jobs, both observability:
 *   1. Show the error and React's component stack in place, with a button to
 *      try again (re-mounting the children) — the user keeps the rest of the
 *      tab and can read what broke.
 *   2. Forward it to Python (`report_client_error`), so it sits in scidb.log
 *      beside the RPCs that led up to it. Best effort: a failed report is
 *      swallowed, because the boundary must not itself throw.
 *
 * A class, because React only exposes `componentDidCatch` /
 * `getDerivedStateFromError` on class components.
 */

import { Component, type ErrorInfo, type ReactNode } from 'react'
import { callBackend } from '../api'

interface Props {
  /** Which part of the UI this guards — the tab, a popup. Goes in the log. */
  where: string
  children: ReactNode
}

interface State {
  error: Error | null
  componentStack: string
}

export default class ClientErrorBoundary extends Component<Props, State> {
  state: State = { error: null, componentStack: '' }

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    const componentStack = info.componentStack ?? ''
    this.setState({ componentStack })
    console.error(`[${this.props.where}] render error:`, error, componentStack)
    callBackend('report_client_error', {
      where: this.props.where,
      message: error.message,
      stack: error.stack ?? '',
      component_stack: componentStack,
    }).catch(() => undefined)
  }

  render() {
    const { error, componentStack } = this.state
    if (!error) return this.props.children
    return (
      <div style={styles.box} role="alert">
        <div style={styles.title}>
          Something in the {this.props.where} failed to render
        </div>
        <div style={styles.message}>{error.message}</div>
        {componentStack && <pre style={styles.stack}>{componentStack.trim()}</pre>}
        <div style={styles.note}>
          This was written to scidb.log as a “[client] render error”.
        </div>
        <button
          type="button"
          style={styles.button}
          onClick={() => this.setState({ error: null, componentStack: '' })}
        >
          Try again
        </button>
      </div>
    )
  }
}

const styles: Record<string, React.CSSProperties> = {
  box: {
    margin: 16, padding: 14, background: '#2a1a1e', border: '1px solid #f87171',
    borderRadius: 6, color: '#eee', fontSize: 12, maxWidth: 900,
  },
  title: { fontWeight: 600, fontSize: 13, color: '#f87171', marginBottom: 6 },
  message: { fontFamily: 'monospace', whiteSpace: 'pre-wrap', marginBottom: 8 },
  stack: {
    fontSize: 10, color: '#aaa', background: '#16162a', padding: 8,
    borderRadius: 4, maxHeight: 220, overflow: 'auto', margin: '0 0 8px',
  },
  note: { fontSize: 11, color: '#8a8aa8', fontStyle: 'italic', marginBottom: 8 },
  button: {
    padding: '5px 14px', background: '#22223a', color: '#ccc',
    border: '1px solid #3a3a5a', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
}
