/**
 * RunLogContext — shared state for function run logs.
 *
 * FunctionNode writes here; the sidebar Runs tab reads here.
 */

import { createContext, useContext, useState, useCallback } from 'react'

export interface VariantEntry {
  constants: Record<string, unknown>
  input_types: Record<string, string>     // param → variable type name
  output_type: string
}

export interface RunEntry {
  run_id: string
  function_name: string
  constants: Record<string, unknown>
  input_types: Record<string, string>     // param → variable type name
  output_types: string[]                  // accumulated across variants
  variants: VariantEntry[]                // per-repetition details
  started_at: number              // epoch ms
  duration_ms?: number
  records_total?: number
  records_done: number
  records_skipped: number
  current_combo?: Record<string, string>  // live schema combo being processed
  status: 'running' | 'done' | 'error'
  error_summary?: string
  lines: string[]                 // raw log, hidden by default
}

export interface RunProgress {
  event: string
  current: number
  total: number
  completed: number
  skipped: number
  metadata: Record<string, string>
  error?: string | null
}

export interface RunMeta {
  constants: Record<string, unknown>
  input_types: Record<string, string>
  output_type: string
  started_at: number
}

interface RunLogContextValue {
  runs: RunEntry[]
  startRun: (run_id: string, function_name: string) => void
  appendLine: (run_id: string, line: string) => void
  finishRun: (run_id: string, success: boolean, duration_ms?: number) => void
  setRunMeta: (run_id: string, meta: RunMeta) => void
  updateProgress: (run_id: string, progress: RunProgress) => void
}

const RunLogContext = createContext<RunLogContextValue | null>(null)

export function RunLogProvider({ children }: { children: React.ReactNode }) {
  const [runs, setRuns] = useState<RunEntry[]>([])

  const startRun = useCallback((run_id: string, function_name: string) => {
    setRuns(prev => [{
      run_id,
      function_name,
      constants: {},
      input_types: {},
      output_types: [],
      variants: [],
      started_at: Date.now(),
      records_done: 0,
      records_skipped: 0,
      status: 'running',
      lines: [],
    }, ...prev])
  }, [])

  const appendLine = useCallback((run_id: string, line: string) => {
    setRuns(prev => prev.map(r =>
      r.run_id === run_id ? { ...r, lines: [...r.lines, line] } : r
    ))
  }, [])

  const finishRun = useCallback((run_id: string, success: boolean, duration_ms?: number) => {
    setRuns(prev => prev.map(r => {
      if (r.run_id !== run_id) return r
      return {
        ...r,
        status: success ? 'done' : 'error',
        duration_ms: duration_ms ?? (Date.now() - r.started_at),
        current_combo: undefined,
      }
    }))
  }, [])

  const setRunMeta = useCallback((run_id: string, meta: RunMeta) => {
    setRuns(prev => prev.map(r => {
      if (r.run_id !== run_id) return r
      // Merge input_types (union of all params across variants).
      const mergedInputs = { ...r.input_types, ...meta.input_types }
      // Accumulate output_types (deduplicated).
      const newOutputs = meta.output_type && !r.output_types.includes(meta.output_type)
        ? [...r.output_types, meta.output_type]
        : r.output_types
      const variant: VariantEntry = {
        constants: meta.constants,
        input_types: meta.input_types,
        output_type: meta.output_type,
      }
      return {
        ...r,
        constants: meta.constants,
        input_types: mergedInputs,
        output_types: newOutputs,
        variants: [...r.variants, variant],
        started_at: meta.started_at * 1000, // convert seconds → ms
      }
    }))
  }, [])

  const updateProgress = useCallback((run_id: string, progress: RunProgress) => {
    setRuns(prev => prev.map(r => {
      if (r.run_id !== run_id) return r
      return {
        ...r,
        records_total: progress.total,
        records_done: progress.completed,
        records_skipped: progress.skipped,
        current_combo: progress.event === 'combo_start' ? progress.metadata : r.current_combo,
        error_summary: progress.error ?? r.error_summary,
      }
    }))
  }, [])

  return (
    <RunLogContext.Provider value={{ runs, startRun, appendLine, finishRun, setRunMeta, updateProgress }}>
      {children}
    </RunLogContext.Provider>
  )
}

export function useRunLog() {
  const ctx = useContext(RunLogContext)
  if (!ctx) throw new Error('useRunLog must be used within RunLogProvider')
  return ctx
}
