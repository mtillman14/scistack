/**
 * RunLogContext — shared state for function run logs.
 *
 * FunctionNode writes here; the sidebar Runs tab reads here.
 */

import { createContext, useContext, useState, useCallback } from 'react'

export interface RunEntry {
  run_id: string
  function_name: string
  lines: string[]
  status: 'running' | 'done'
}

interface RunLogContextValue {
  runs: RunEntry[]
  startRun: (run_id: string, function_name: string) => void
  appendLine: (run_id: string, line: string) => void
  finishRun: (run_id: string) => void
}

const RunLogContext = createContext<RunLogContextValue | null>(null)

export function RunLogProvider({ children }: { children: React.ReactNode }) {
  const [runs, setRuns] = useState<RunEntry[]>([])

  const startRun = useCallback((run_id: string, function_name: string) => {
    setRuns(prev => [{ run_id, function_name, lines: [], status: 'running' }, ...prev])
  }, [])

  const appendLine = useCallback((run_id: string, line: string) => {
    setRuns(prev => prev.map(r =>
      r.run_id === run_id ? { ...r, lines: [...r.lines, line] } : r
    ))
  }, [])

  const finishRun = useCallback((run_id: string) => {
    setRuns(prev => prev.map(r =>
      r.run_id === run_id ? { ...r, status: 'done' } : r
    ))
  }, [])

  return (
    <RunLogContext.Provider value={{ runs, startRun, appendLine, finishRun }}>
      {children}
    </RunLogContext.Provider>
  )
}

export function useRunLog() {
  const ctx = useContext(RunLogContext)
  if (!ctx) throw new Error('useRunLog must be used within RunLogProvider')
  return ctx
}
