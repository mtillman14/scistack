/**
 * Plot Studio's link to StatStudio (S10, S11, S20) — one small component in
 * the figure toolbar, so PlotStudio.tsx only mounts it.
 *
 * - **StatStudio** opens a stats tab set up from this figure
 *   (`scistackstats.seed_from_plot`), linked to this saved plot.
 * - With a linked saved stats table: a **banner** lists how the designs
 *   differ (`stats_divergence`), and **Sync to StatStudio** saves the table
 *   with this figure's design and tells an open StatStudio tab.
 * - **Test brackets**: when the linked table draws them, the bars arrive here
 *   (`stats_overlay_for_plot`) and go to the panel as `derived_bars`. The
 *   backend joins them per request and they are never saved with the plot.
 * - A StatStudio "Sync to PlotStudio" arrives as `stats_design_sync`. Its
 *   design is copied onto the CURRENT spec (`stats_sync`, the backend's one
 *   copy), so local edits outside the design are kept.
 */

import { useEffect, useRef, useState } from 'react'
import { addNotificationHandler, callBackend, isVSCodeMode } from '../../api'

interface Difference { path: string; label: string; a: string; b: string }
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
  const [differences, setDifferences] = useState<Difference[]>([])
  const [note, setNote] = useState('')
  const specRef = useRef(spec)
  specRef.current = spec

  // The linked stats table, if any (the first; the link lives on the stats side).
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

  // Divergence and brackets follow the figure's spec.
  useEffect(() => {
    if (!spec || !statsSpec || !plotId) { setDifferences([]); return }
    const timer = window.setTimeout(() => {
      callBackend('stats_divergence', { stats_spec: statsSpec, plot_spec: spec })
        .then(raw => setDifferences((raw as { differences: Difference[] }).differences))
        .catch(() => setDifferences([]))
      const overlay = (statsSpec.overlay ?? {}) as { show?: boolean }
      if (!overlay.show) { onDerivedBars([]); return }
      callBackend('stats_overlay_for_plot', { plot_id: plotId, plot_spec: spec }).then(raw => {
        const r = raw as { bars: Record<string, unknown>[]; reason: string | null; notes: string[] }
        console.info('[Plot Studio] test brackets', { count: r.bars.length, reason: r.reason })
        onDerivedBars(r.bars)
        setNote(r.reason ?? r.notes.join(' '))
      }).catch(() => onDerivedBars([]))
    }, 400)
    return () => window.clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [spec, statsSpec, plotId])

  // StatStudio -> here.
  useEffect(() => addNotificationHandler(msg => {
    if (msg.method !== 'stats_design_sync') return
    const params = (msg.params ?? {}) as { plot_id?: string; plot_spec?: Record<string, unknown> }
    const current = specRef.current
    if (!plotId || params.plot_id !== plotId || !params.plot_spec || !current) return
    callBackend('stats_sync', {
      stats_spec: { format: 1, design: params.plot_spec }, plot_spec: current, direction: 'to_plot',
    }).then(raw => {
      console.info('[Plot Studio] design synced from StatStudio', { plot: plotId })
      onApplySpec((raw as { plot_spec: Record<string, unknown> }).plot_spec)
      setNote('Design synced from StatStudio.')
    })
  }), [plotId, onApplySpec])

  const openStatStudio = () => {
    if (!spec) return
    callBackend('open_stat_panel', { seed: spec, linkedPlot: plotId, variable })
      .catch(err => setNote(`Could not open StatStudio: ${err}`))
  }

  const syncToStats = async () => {
    if (!spec || !statsSpec || !linked) return
    const r = await callBackend('stats_sync', { stats_spec: statsSpec, plot_spec: spec, direction: 'to_stats' }) as { stats_spec: Record<string, unknown> }
    await callBackend('stats_saved_save', { name: linked.name, spec: r.stats_spec, overwrite: true, current_stats_id: linked.stats_id })
    await callBackend('broadcast_panels', { method: 'plot_design_sync', params: { plot_id: plotId, plot_spec: spec } }).catch(() => undefined)
    setStatsSpec(r.stats_spec)
    setNote(`Stats table "${linked.name}" now has this figure's design.`)
  }

  return (
    <>
      <button type="button" style={buttonStyle} onClick={openStatStudio}
              disabled={!isVSCodeMode || !spec}
              title={isVSCodeMode
                ? (plotId ? 'Open StatStudio set up from this figure, linked to this saved plot'
                          : 'Open StatStudio set up from this figure (save the plot to link them)')
                : 'StatStudio opens in its own VS Code tab'}>
        StatStudio
      </button>
      {linked && differences.length > 0 && (
        <span title={differences.map(d => `${d.label}: figure ${d.a} · table ${d.b}`).join('\n')}
              style={{ color: 'var(--ps-warn)', fontSize: 12 }}>
          Stats table "{linked.name}" differs ({differences.map(d => d.label).join(', ')})
          {' '}<button type="button" style={buttonStyle} onClick={syncToStats}>Sync to StatStudio</button>
        </span>
      )}
      {note && <span style={{ color: 'var(--ps-text-muted)', fontSize: 12 }}>{note}</span>}
    </>
  )
}
