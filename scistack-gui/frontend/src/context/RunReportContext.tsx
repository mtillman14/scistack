/**
 * RunReportContext — which run report window is open, and the one place
 * that decides when it opens by itself.
 *
 * Two ways in (GitHub issue #17, docs/claude/run-report.md):
 *  - a run ends: `run_done.reports` names the function nodes that left a
 *    stored run; their summaries are fetched, and the window opens with one
 *    tab per node when `shouldAutoOpen` says so (the rule is in
 *    RunReport/runReportTypes.ts);
 *  - right-click a function node → "Run report…" (`openForNode`), at any
 *    time, on the node's newest recorded run.
 *
 * The summaries fetched for the decision are handed to the window, so
 * opening it does not fetch them again.
 */

import { createContext, useCallback, useContext, useRef, useState } from 'react'
import { callBackend } from '../api'
import { useBackendMessage } from '../hooks/useBackendMessage'
import {
  AUTO_OPEN_STORAGE_KEY,
  parseAutoOpen,
  readReports,
  shouldAutoOpen,
  tabsOf,
  type ReportTab,
  type RunReport,
} from '../components/RunReport/runReportTypes'

export interface RunReportTarget {
  tabs: ReportTab[]
  /** Reports already fetched, by run id. */
  prefetched: Record<string, RunReport>
  /** Changes on every open, so the window resets its tab and caches. */
  openId: number
}

interface RunReportValue {
  target: RunReportTarget | null
  openForNode: (functionName: string, nodeId: string | null, outputs?: string[]) => void
  close: () => void
  autoOpen: boolean
  setAutoOpen: (on: boolean) => void
}

const RunReportContext = createContext<RunReportValue | null>(null)

function readAutoOpen(): boolean {
  try {
    return parseAutoOpen(window.localStorage.getItem(AUTO_OPEN_STORAGE_KEY))
  } catch {
    return true
  }
}

export function RunReportProvider({ children }: { children: React.ReactNode }) {
  const [target, setTarget] = useState<RunReportTarget | null>(null)
  const [autoOpen, setAutoOpenState] = useState<boolean>(readAutoOpen)
  const autoOpenRef = useRef(autoOpen)
  autoOpenRef.current = autoOpen
  const openId = useRef(0)

  const setAutoOpen = useCallback((on: boolean) => {
    setAutoOpenState(on)
    try {
      window.localStorage.setItem(AUTO_OPEN_STORAGE_KEY, on ? 'on' : 'off')
    } catch { /* not remembered */ }
    // eslint-disable-next-line no-console
    console.debug(`[RunReport] open after every run: ${on ? 'on' : 'off'}`)
  }, [])

  const openForNode = useCallback((functionName: string, nodeId: string | null, outputs?: string[]) => {
    // eslint-disable-next-line no-console
    console.debug(`[RunReport] open for node ${nodeId ?? '(none)'} fn=${functionName}`)
    setTarget({
      tabs: [{ nodeId, functionName, runId: null, outputs }],
      prefetched: {},
      openId: ++openId.current,
    })
  }, [])

  const close = useCallback(() => setTarget(null), [])

  useBackendMessage(useCallback((msg) => {
    const kind = (msg.type ?? msg.method) as string | undefined
    if (kind !== 'run_done') return
    const params = (msg.params ?? msg) as Record<string, unknown>
    const entries = readReports(params.reports)
    if (entries.length === 0) return
    if (!autoOpenRef.current) {
      // eslint-disable-next-line no-console
      console.debug(`[RunReport] run_done ${String(params.run_id)}: ${entries.length} report(s), auto-open is off`)
      return
    }
    const tabs = tabsOf(entries)
    void Promise.all(
      tabs.map(tab =>
        callBackend('run_report', { part: 'summary', run_id: tab.runId })
          .then(r => r as RunReport)
          .catch(err => {
            // eslint-disable-next-line no-console
            console.debug(`[RunReport] summary of ${tab.runId} unavailable: ${(err as Error).message}`)
            return null
          }),
      ),
    ).then(reports => {
      const got = reports.filter((r): r is RunReport => r !== null)
      const open = shouldAutoOpen(autoOpenRef.current, got.map(r => r.summary))
      // eslint-disable-next-line no-console
      console.debug(
        `[RunReport] run_done ${String(params.run_id)}: ${tabs.length} tab(s), `
        + `${got.length} summary(ies) read, auto-open=${open}`,
      )
      if (!open) return
      setTarget({
        tabs,
        prefetched: Object.fromEntries(got.map(r => [r.summary.run_id, r])),
        openId: ++openId.current,
      })
    })
  }, []))

  return (
    <RunReportContext.Provider value={{ target, openForNode, close, autoOpen, setAutoOpen }}>
      {children}
    </RunReportContext.Provider>
  )
}

export function useRunReport() {
  const ctx = useContext(RunReportContext)
  if (!ctx) throw new Error('useRunReport must be used within RunReportProvider')
  return ctx
}
