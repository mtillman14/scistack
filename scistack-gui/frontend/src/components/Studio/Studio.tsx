/**
 * Studio — ONE tab for a figure and its statistics (plan-stats-on-figure.md).
 *
 * Two views, `Figure | Statistics`, over one design held in memory. The
 * views keep their own state (scroll, open rail groups, last result) because
 * both stay mounted; the inactive one is hidden and does no work (`active`:
 * no resolve, no test run) and takes one refresh when shown again.
 *
 * The design is NOT owned here: Python owns which spec fields are design
 * (`design_fields`, `scistackplot.model.design`), `sharedDesign` consumes
 * that list, and this component only carries a changed design from one view
 * to the other. A view is mounted the first time it is shown (or opened on),
 * so a view that starts later is seeded from the other's latest design and
 * there is never a first-sync to get wrong.
 */

import { useCallback, useRef, useState } from 'react'
import { callBackend } from '../../api'
import PlotStudio from '../PlotStudio/PlotStudio'
import StatStudio, { type StatViewConfig } from '../StatStudio/StatStudio'
import type { PlotViewConfig } from '../../PlotRoot'
import type { PlotSpec } from '../../generated/api-types'
import { testSettings } from '../PlotStudio/statMarks'
import { sameDesign } from './sharedDesign'

export type StudioView = 'figure' | 'statistics'
type Dict = Record<string, unknown>

interface Incoming { spec: Dict; fields: string[]; rev: number }

export default function Studio({ initial }: { initial: PlotViewConfig | StatViewConfig }) {
  const opensOnStats = initial.view === 'stat'
  const [view, setView] = useState<StudioView>(opensOnStats ? 'statistics' : 'figure')
  const [mounted, setMounted] = useState<Record<StudioView, boolean>>({
    figure: !opensOnStats, statistics: opensOnStats,
  })
  const fields = useRef<string[] | undefined>(undefined)
  const latest = useRef<{ plot?: Dict; stat?: Dict }>({})
  const rev = useRef(0)
  const [toPlot, setToPlot] = useState<Incoming | null>(null)
  const [toStat, setToStat] = useState<Incoming | null>(null)
  const [settings, setSettings] = useState<Dict | null>(null)
  const settingsKey = useRef('')
  const onStatSpec = useCallback((stats: Dict) => {
    const next = testSettings(stats)
    const key = JSON.stringify(next)
    if (key === settingsKey.current) return
    settingsKey.current = key
    setSettings(next)
  }, [])
  const [savedSettings, setSavedSettings] = useState<{ settings: Dict; rev: number } | null>(null)
  const onSavedStats = useCallback((stats: Dict | null) => {
    settingsKey.current = JSON.stringify(stats)
    setSettings(stats)
    if (stats) setSavedSettings({ settings: stats, rev: ++rev.current })
  }, [])
  const [marksRequest, setMarksRequest] = useState<{ patch: Dict; rev: number } | null>(null)

  const plotNode = initial.view === 'plot' ? initial.plotNode ?? null : null
  const csvPath = initial.csvPath ?? undefined

  const onPlotSpec = useCallback((spec: Dict) => {
    latest.current.plot = spec
    const f = fields.current
    const other = latest.current.stat
    if (f && other && !sameDesign(spec, other, f)) {
      console.info('[Studio] design edited in the Figure view')
      setToStat({ spec, fields: f, rev: ++rev.current })
    }
  }, [])
  const onStatDesign = useCallback((design: Dict) => {
    latest.current.stat = design
    const f = fields.current
    const other = latest.current.plot
    if (f && other && !sameDesign(design, other, f)) {
      console.info('[Studio] design edited in the Statistics view')
      setToPlot({ spec: design, fields: f, rev: ++rev.current })
    }
  }, [])

  // What each view was opened with, frozen when it mounts: a changing `initial`
  // would reopen the view and throw its state away.
  const opened = useRef<{ plotVariable?: string; stat?: StatViewConfig }>({})
  const openingConfigs = () => {
    if (opened.current.plotVariable === undefined) {
      const seedMeasure = (latest.current.stat?.measures as string[] | undefined)?.[0]
      opened.current.plotVariable = initial.variable ?? seedMeasure ?? ''
    }
    if (opened.current.stat === undefined) {
      opened.current.stat = initial.view === 'stat'
        ? initial
        : {
            view: 'stat', variable: initial.variable ?? null, csvPath: initial.csvPath ?? null, from: initial.from ?? null,
            seed: (latest.current.plot as unknown as PlotSpec | undefined) ?? null,
          }
    }
  }

  const showOnFigure = () => {
    setMarksRequest({ patch: { brackets: true }, rev: ++rev.current })
    show('figure')
  }

  const show = (next: StudioView) => {
    if (next === view) return
    if (next === 'figure' && !mounted.figure) delete opened.current.plotVariable
    if (next === 'statistics' && !mounted.statistics) delete opened.current.stat
    console.info(`[Studio] view ${view} → ${next}`)
    callBackend('stats_guide_log', { event: `view ${view} → ${next}`, step: null, detail: 'studio tab' }).catch(() => undefined)
    setMounted(prev => ({ ...prev, [next]: true }))
    setView(next)
  }

  // A view that starts later is seeded from the other's latest design.
  if (mounted.figure || mounted.statistics) openingConfigs()
  const plotVariable = opened.current.plotVariable ?? ''
  const statInitial = opened.current.stat as StatViewConfig

  return (
    <div style={styles.root}>
      <div style={styles.bar} role="tablist">
        <button type="button" role="tab" aria-selected={view === 'figure'}
                style={view === 'figure' ? styles.tabOn : styles.tab} onClick={() => show('figure')}>Figure</button>
        <button type="button" role="tab" aria-selected={view === 'statistics'}
                style={view === 'statistics' ? styles.tabOn : styles.tab} disabled={!!plotNode}
                title={plotNode ? "This node's own function draws the figure, so there is no table to analyse." : undefined}
                onClick={() => show('statistics')}>Statistics</button>
      </div>
      <div style={styles.body}>
        {mounted.figure && (
          <div style={{ ...styles.pane, display: view === 'figure' ? 'block' : 'none' }}>
            <PlotStudio
              variable={plotVariable}
              csvPath={csvPath}
              initialLocation={initial.view === 'plot' ? initial.location ?? undefined : undefined}
              plotNode={plotNode ?? undefined}
              whereCheck={initial.view === 'plot' ? initial.whereCheck ?? undefined : undefined}
              openedFrom={initial.from ?? undefined}
              embedded
              onClose={() => undefined}
              active={view === 'figure'}
              onSpecChange={onPlotSpec}
              incomingDesign={toPlot}
              statMarksRequest={marksRequest}
              statsSettings={settings}
              onSavedStats={onSavedStats}
              onDesignFields={f => { fields.current = f }}
            />
          </div>
        )}
        {mounted.statistics && (
          <div style={{ ...styles.pane, display: view === 'statistics' ? 'block' : 'none' }}>
            <StatStudio
              initial={statInitial}
              active={view === 'statistics'}
              onDesignChange={onStatDesign}
              incomingDesign={toStat}
              onShowOnFigure={showOnFigure}
              onSpecChange={onStatSpec}
              incomingSettings={savedSettings}
            />
          </div>
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', background: 'var(--ps-bg)' },
  bar: { display: 'flex', gap: 4, padding: '4px 8px', borderBottom: '1px solid var(--ps-border)', background: 'var(--ps-surface)' },
  tab: {
    background: 'transparent', color: 'var(--ps-text-secondary)', border: '1px solid transparent',
    borderRadius: 4, fontSize: 12, padding: '3px 12px', cursor: 'pointer',
  },
  tabOn: {
    background: 'var(--ps-control)', color: 'var(--ps-text-strong)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 4, fontSize: 12, padding: '3px 12px', cursor: 'pointer',
  },
  body: { position: 'relative', flex: 1, minHeight: 0 },
  pane: { position: 'absolute', inset: 0 },
}
