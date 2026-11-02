/**
 * Plot Studio's link to StatStudio (S10, S20; docs/claude/studio-unification.md)
 * — one small component in the figure toolbar, so PlotStudio.tsx only mounts it.
 *
 * - **StatStudio** opens a stats tab set up from this figure
 *   (`scistackstats.seed_from_plot`), linked to this saved plot.
 * - **One shared design** (Stage 6): a saved plot linked to a stats table
 *   shares ONE design with it (`scistackplotdb.designs`). Each design edit
 *   here goes to `design_update` (a no-op for an unlinked plot); a real
 *   change is broadcast as `design_changed`, and the StatStudio tab takes it
 *   (`design_apply`). There is nothing to compare and no Sync button.
 * - **Test brackets**: when the linked table draws them, the bars arrive here
 *   (`stats_overlay_for_plot`) and go to the panel as `derived_bars`. The
 *   backend joins them per request and they are never saved with the plot.
 */

import { useEffect, useRef, useState } from 'react'
import { addNotificationHandler, callBackend, isVSCodeMode } from '../../api'
import { DESIGN_CHANGED, designChangeFor, newDesignOrigin } from '../Studio/sharedDesign'

interface LinkedInfo { stats_id: string; name: string; version: number }

interface Props {
  spec: Record<string, unknown> | null
  plotId: string | null
  variable: string
  csvPath?: string
  onApplySpec: (spec: Record<string, unknown>) => void
  onDerivedBars: (bars: Record<string, unknown>[]) => void
  buttonStyle: React.CSSProperties
}

export default function StatsLink({ spec, plotId, variable, csvPath, onApplySpec, onDerivedBars, buttonStyle }: Props) {
  const [linked, setLinked] = useState<LinkedInfo | null>(null)
  const [statsSpec, setStatsSpec] = useState<Record<string, unknown> | null>(null)
  const [note, setNote] = useState('')
  const specRef = useRef(spec)
  specRef.current = spec
  // This tab's id in `design_changed` broadcasts, so it ignores its own.
  const origin = useRef(newDesignOrigin('plot'))

  // The linked stats table, if any (the first), for its brackets.
  useEffect(() => {
    setLinked(null); setStatsSpec(null); onDerivedBars([])
    if (!plotId || csvPath) return
    callBackend('stats_linked', { plot_id: plotId }).then(async raw => {
      const items = (raw as { items: LinkedInfo[] }).items
      if (!items.length) return
      const opened = await callBackend('stats_open', { stats_id: items[0].stats_id }) as { ok: boolean; spec: Record<string, unknown> }
      if (!opened.ok) return
      console.info('[Plot Studio] linked stats table', { plot: plotId, stats: items[0].name })
      setLinked(items[0]); setStatsSpec(opened.spec)
    }).catch(err => console.warn('[Plot Studio] stats link lookup failed', err))
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [plotId, csvPath])

  // Every edit: the shared design (a no-op when unlinked), then the brackets.
  useEffect(() => {
    if (!spec || !plotId || csvPath) return
    const timer = window.setTimeout(() => {
      callBackend('design_update', { plot_id: plotId, spec }).then(raw => {
        const r = raw as { linked: boolean; changed: boolean; version: number | null }
        if (!r.changed) return
        console.info('[Plot Studio] shared design changed', { plot: plotId, version: r.version })
        if (isVSCodeMode) {
          callBackend('broadcast_panels', {
            method: DESIGN_CHANGED, params: { plot_id: plotId, version: r.version, origin: origin.current },
          }).catch(() => undefined)
        }
      }).catch(err => console.warn('[Plot Studio] design_update failed', err))
      const overlay = ((statsSpec?.overlay ?? {}) as { show?: boolean })
      if (!statsSpec || !overlay.show) { onDerivedBars([]); return }
      callBackend('stats_overlay_for_plot', { plot_id: plotId, plot_spec: spec }).then(raw => {
        const r = raw as { bars: Record<string, unknown>[]; reason: string | null; notes: string[] }
        console.info('[Plot Studio] test brackets', { count: r.bars.length, reason: r.reason })
        onDerivedBars(r.bars)
        setNote(r.reason ?? r.notes.join(' '))
      }).catch(() => onDerivedBars([]))
    }, 500)
    return () => window.clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [spec, statsSpec, plotId, csvPath])

  // StatStudio changed the shared design: take it onto the CURRENT spec
  // (design fields only; this figure's display settings stay).
  useEffect(() => addNotificationHandler(msg => {
    const change = designChangeFor(msg, plotId, origin.current)
    const current = specRef.current
    if (!change || !current || !plotId) return
    callBackend('design_apply', { plot_id: plotId, spec: current }).then(raw => {
      const r = raw as { spec: Record<string, unknown>; version: number | null }
      console.info('[Plot Studio] shared design taken from StatStudio', { plot: plotId, version: r.version })
      onApplySpec(r.spec)
      setNote(`Design updated from the linked stats table${linked ? ` "${linked.name}"` : ''}.`)
    }).catch(err => console.warn('[Plot Studio] design_apply failed', err))
  }), [plotId, onApplySpec, linked])

  const openStatStudio = () => {
    if (!spec) return
    callBackend('open_stat_panel', { seed: spec, linkedPlot: plotId, variable, from: 'plot-studio' })
      .catch(err => setNote(`Could not open StatStudio: ${err}`))
  }

  return (
    <>
      <button type="button" style={buttonStyle} onClick={openStatStudio}
              disabled={!isVSCodeMode || !spec}
              title={isVSCodeMode
                ? (plotId ? 'Open StatStudio set up from this figure. The table and this saved plot share one design.'
                          : 'Open StatStudio set up from this figure (save the plot first so the two share one design)')
                : 'StatStudio opens in its own VS Code tab'}>
        StatStudio
      </button>
      {note && <span style={{ color: 'var(--ps-text-muted)', fontSize: 12 }}>{note}</span>}
    </>
  )
}
