/**
 * Fetches the test results a figure asks for (`stat_marks`) — one debounced
 * request, latest wins. Keyed on `statMarks.marksKey`, so a display edit
 * never re-runs a test. Nothing is stored: the facts ride on the next
 * resolve / export / save as `derived` (`diffbars.with_derived`).
 */

import { useEffect, useState } from 'react'
import { callBackend } from '../../api'
import type { PlotSpec } from '../../generated/api-types'
import { marksKey, type MarksResult } from './statMarks'

export interface StatMarksState {
  result: MarksResult | null
  loading: boolean
  error: string
}

const NONE: StatMarksState = { result: null, loading: false, error: '' }

export function useStatMarks(
  spec: PlotSpec | null,
  designFields: string[] | undefined,
  csvPath: string | undefined,
  active = true,
  settings: Record<string, unknown> | null = null,
): StatMarksState {
  const [state, setState] = useState<StatMarksState>(NONE)
  const key = marksKey(spec, designFields, csvPath, settings)
  useEffect(() => {
    // No spec, nothing to test (marksKey is null then too; this says so to tsc).
    if (key === null || spec === null) { setState(NONE); return }
    if (!active) return // a hidden view runs no test
    let live = true
    setState(prev => ({ ...prev, loading: true, error: '' }))
    const timer = window.setTimeout(() => {
      const started = performance.now()
      callBackend('stats_marks', { plot_spec: spec, ...(settings ? { stats_spec: { ...settings, design: spec, format: 1 } } : {}), ...(csvPath ? { csv_path: csvPath } : {}) })
        .then(raw => {
          if (!live) return
          const result = raw as MarksResult
          console.info('[Plot Studio] stat_marks', {
            bars: result.bars?.length ?? 0, notes: result.panel_notes?.length ?? 0,
            caption: !!result.caption, reason: result.reason, ms: Math.round(performance.now() - started),
          })
          setState({ result, loading: false, error: '' })
        })
        .catch(err => {
          if (!live) return
          console.warn('[Plot Studio] stat_marks failed', err)
          setState({ result: null, loading: false, error: String(err) })
        })
    }, 400)
    return () => { live = false; window.clearTimeout(timer) }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key, active])
  return state
}
