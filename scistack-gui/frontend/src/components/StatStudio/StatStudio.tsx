/**
 * StatStudio — its own tab (S10): Descriptives | Comparisons for one stats spec.
 *
 * A thin wrapper. Every number, caption, note and option list comes from the
 * backend (`stats_compute`: Grids + the `catalog`), so this file formats
 * nothing and knows no statistics vocabulary. Spec edits are pure helpers in
 * `statsSpec.ts`.
 *
 * The plot link (S11): with a linked saved plot, the banner lists how the two
 * designs differ (`stats_divergence`, from `scistackplot.design`), and
 * **Sync to PlotStudio** copies this design onto the figure. It saves a new
 * version of the saved plot and tells an open Plot Studio tab. A Plot Studio
 * "Sync to StatStudio" arrives here as a `plot_design_sync` notification.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { addNotificationHandler, callBackend, isVSCodeMode } from '../../api'
import { plotThemeVars } from '../PlotStudio/plotTheme'
import { usePlotTheme } from '../PlotStudio/usePlotTheme'
import ModelFormulaDialog from './ModelFormulaDialog'
import { pendingActions, recommendedFor, type StratumSummary } from './statsActions'
import {
  addAnalysis, addDV, moveGroup, removeAnalysis, removeDV, setCell, setFormatting, setOverlay,
  setOverride, setRole, targets, toggleStatistic, updateAnalysis, gridToTsv, gridWidth,
  type Analysis, type Grid, type StatsSpec,
} from './statsSpec'

export interface StatViewConfig {
  view: 'stat'
  variable?: string | null
  csvPath?: string | null
  statsId?: string | null
  seed?: Record<string, unknown> | null
  linkedPlot?: string | null
}

interface Catalog {
  statistics: { id: string; label: string }[]
  cells: { id: string; header: string }[]
  tests: { id: string; label: string; role: string; family: string }[]
  effect_sizes: { id: string; name: string; formula: string; tests: string[] }[]
  corrections: { id: string; label: string }[]
  scopes: { id: string; label: string }[]
  families: string[]
  family_labels?: Record<string, string>
  missing_policies?: { id: string; label: string }[]
  tails: string[]
  spreads: string[]
  bar_pairs: string[]
  bar_labels: string[]
  variant_modes?: string[]
}

interface ComparisonResult {
  id: string
  grid: Grid
  omnibus: Grid | null
  sweep?: Grid | null
  sentences: string[]
  family_statement: string
  strata?: StratumSummary[]
}

interface Readout {
  dv: string
  error?: string
  rows?: { unit: string | null; averaged: string[]; strata: string[] }
  design?: { factors: { name: string; kind: string; derived: string; overridden: boolean; units_multi: number; units_single: number; basis?: string }[]; n_units: number | null; warnings: { code: string; message: string }[]; incomplete_units?: string[] }
  tests?: { id: string; label: string; role: string; reason: string | null }[]
  /** The first analysis' model formula as it runs (scistackstats.model). */
  model?: { text: string; is_default: boolean; errors: { message: string }[]; needs_mixed_model: boolean }
}

interface ComputeResult {
  ok: boolean
  error?: string
  descriptives: Grid[]
  comparisons: ComparisonResult[]
  readout: Readout[]
  refusals: { dv: string; reason: string }[]
  not_exportable: string | null
  catalog: Catalog
}

interface Difference { path: string; label: string; a: string; b: string }

const ROLE_CHOICES: { id: string; label: string }[] = [
  { id: 'group', label: 'Group (columns)' },
  { id: 'iterate', label: 'Separate tables' },
  { id: 'facet', label: 'Separate sections' },
  { id: 'collapse', label: 'Collapse (average)' },
]
const FORMATS = ['html', 'md', 'csv', 'latex', 'txt', 'json']

function GridTable({ grid }: { grid: Grid }) {
  const width = gridWidth(grid)
  return (
    <div style={styles.gridBox}>
      <div style={styles.gridTitle}>{grid.title}</div>
      <table style={styles.table}>
        <thead>
          {grid.header_rows.map((row, i) => (
            <tr key={i}>
              {row.map((cell, j) => (
                <th key={j} colSpan={cell.span}
                    style={{ ...styles.th, ...(i === grid.header_rows.length - 1 ? styles.thLast : {}),
                             textAlign: j < grid.label_columns ? 'left' : 'center' }}>
                  {cell.text}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {grid.rows.map((row, i) => row.kind === 'section' ? (
            <tr key={i}><td colSpan={width} style={styles.section}>{row.cells[0]}</td></tr>
          ) : (
            <tr key={i}>
              {row.cells.map((cell, j) => (
                <td key={j} style={{ ...styles.td, textAlign: j < grid.label_columns ? 'left' : 'center' }}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <div style={styles.caption}><i>Note.</i> {grid.caption}</div>
      {grid.notes.length > 0 && (
        <ul style={styles.notes}>{grid.notes.map((n, i) => <li key={i}>{n}</li>)}</ul>
      )}
      <button type="button" style={styles.smallButton}
              onClick={() => navigator.clipboard?.writeText(gridToTsv(grid)).catch(() => undefined)}
              title="Copy as tab-separated text (pastes into a spreadsheet)">
        Copy as text
      </button>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  const [open, setOpen] = useState(true)
  return (
    <div style={styles.railSection}>
      <button type="button" style={styles.railHeader} onClick={() => setOpen(!open)}>
        {open ? '▾' : '▸'} {title}
      </button>
      {open && <div style={styles.railBody}>{children}</div>}
    </div>
  )
}

export default function StatStudio({ initial }: { initial: StatViewConfig }) {
  const theme = usePlotTheme()
  const csvPath = initial.csvPath ?? undefined
  const [spec, setSpec] = useState<StatsSpec | null>(null)
  const [savedInfo, setSavedInfo] = useState<{ stats_id: string; name: string; version: number } | null>(null)
  const [openNotes, setOpenNotes] = useState<string[]>([])
  const [result, setResult] = useState<ComputeResult | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [tab, setTab] = useState<'descriptives' | 'comparisons'>('descriptives')
  const [factors, setFactors] = useState<{ name: string; is_field?: boolean; levels: unknown[] }[]>([])
  const [variables, setVariables] = useState<{ name: string; columns?: string[] }[]>([])
  const [linkedPlot, setLinkedPlot] = useState<{ plot_id: string; variable: string; name: string; spec: Record<string, unknown>; view: unknown } | null>(null)
  const [differences, setDifferences] = useState<Difference[]>([])
  const [message, setMessage] = useState('')
  const [saveName, setSaveName] = useState('')
  const [exportFormat, setExportFormat] = useState('html')
  const [newDV, setNewDV] = useState('')
  const [newColumns, setNewColumns] = useState<string[]>([])
  const [savedList, setSavedList] = useState<{ stats_id: string; name: string; version: number }[]>([])
  const [presets, setPresets] = useState<{ preset_id: string; name: string }[]>([])
  // The analysis whose Model formula popup is open (plan-statstudio-model-formula).
  const [formulaFor, setFormulaFor] = useState<string | null>(null)
  const generation = useRef(0)
  const sourceParams = useMemo(() => (csvPath ? { csv_path: csvPath } : {}), [csvPath])

  // --- open -------------------------------------------------------------------
  useEffect(() => {
    callBackend('stats_open', {
      stats_id: initial.statsId ?? null, seed: initial.seed ?? null, plot_id: initial.linkedPlot ?? null,
      variable: initial.variable ?? null, ...sourceParams,
    }).then(raw => {
      const r = raw as { ok: boolean; error?: string; spec: StatsSpec; notes: { message: string }[]; seed_notes: string[]; info: typeof savedInfo }
      if (!r.ok) { setError(r.error ?? 'Could not open StatStudio.'); return }
      console.info('[StatStudio] opened', { statsId: initial.statsId, seeded: !!initial.seed, notes: r.notes.length })
      setSpec(r.spec)
      setSavedInfo(r.info)
      if (r.info) setSaveName(r.info.name)
      setOpenNotes([...r.seed_notes.map(n => `Set up from the figure: ${n}`), ...r.notes.map(n => n.message)])
    }).catch(err => setError(String(err)))
  }, [initial.statsId, initial.seed, initial.linkedPlot, initial.variable, sourceParams])

  const firstVariable = spec ? targets(spec)[0]?.variable : undefined
  useEffect(() => {
    if (!firstVariable) return
    callBackend('plot_describe', { variable: firstVariable, ...sourceParams }).then(raw => {
      const r = raw as { catalog?: { measures?: { name: string; columns?: string[] }[] }; table?: { factors: { name: string; is_field?: boolean; levels: unknown[] }[] } }
      setFactors(r.table?.factors ?? [])
      setVariables(r.catalog?.measures ?? [])
    }).catch(err => console.warn('[StatStudio] describe failed', err))
  }, [firstVariable, sourceParams])

  // --- compute (debounced, latest wins) ----------------------------------------
  useEffect(() => {
    if (!spec) return
    const mine = ++generation.current
    const timer = window.setTimeout(() => {
      setBusy(true)
      const t0 = performance.now()
      callBackend('stats_compute', { spec, ...sourceParams }).then(raw => {
        if (mine !== generation.current) return
        const r = raw as ComputeResult
        console.info('[StatStudio] compute', { ms: Math.round(performance.now() - t0), ok: r.ok })
        setResult(r)
        setError(r.ok ? '' : r.error ?? 'Could not compute.')
      }).catch(err => { if (mine === generation.current) setError(String(err)) })
        .finally(() => { if (mine === generation.current) setBusy(false) })
    }, 350)
    return () => window.clearTimeout(timer)
  }, [spec, sourceParams])

  // --- the plot link ---------------------------------------------------------------
  const linkedId = spec?.linked_plot
  useEffect(() => {
    if (!linkedId || csvPath) { setLinkedPlot(null); return }
    callBackend('plot_saved_open', { plot_id: linkedId }).then(raw => {
      const r = raw as { plot?: { plot_id: string; variable: string; name: string; spec: Record<string, unknown>; view: unknown } }
      setLinkedPlot(r.plot ?? null)
    }).catch(err => { console.warn('[StatStudio] linked plot did not open', err); setLinkedPlot(null) })
  }, [linkedId, csvPath])

  useEffect(() => {
    if (!spec || !linkedPlot) { setDifferences([]); return }
    callBackend('stats_divergence', { stats_spec: spec, plot_spec: linkedPlot.spec }).then(raw => {
      setDifferences((raw as { differences: Difference[] }).differences)
    }).catch(() => setDifferences([]))
  }, [spec, linkedPlot])

  useEffect(() => addNotificationHandler(msg => {
    if (msg.method !== 'plot_design_sync') return
    const params = (msg.params ?? {}) as { plot_id?: string; plot_spec?: Record<string, unknown> }
    if (!spec || !params.plot_id || params.plot_id !== spec.linked_plot || !params.plot_spec) return
    callBackend('stats_sync', { stats_spec: spec, plot_spec: params.plot_spec, direction: 'to_stats' }).then(raw => {
      const r = raw as { stats_spec: StatsSpec }
      console.info('[StatStudio] design synced from Plot Studio', { plot: params.plot_id })
      setSpec(r.stats_spec)
      setLinkedPlot(prev => prev ? { ...prev, spec: params.plot_spec! } : prev)
      setMessage('Design synced from Plot Studio.')
    })
  }), [spec])

  const syncToPlot = useCallback(async () => {
    if (!spec || !linkedPlot) return
    const r = await callBackend('stats_sync', { stats_spec: spec, plot_spec: linkedPlot.spec, direction: 'to_plot' }) as { plot_spec: Record<string, unknown>; notes: { message: string }[] }
    await callBackend('plot_saved_save', {
      variable: linkedPlot.variable, name: linkedPlot.name, spec: r.plot_spec, view: linkedPlot.view,
      overwrite: true, current_plot_id: linkedPlot.plot_id,
    })
    if (isVSCodeMode) {
      await callBackend('broadcast_panels', { method: 'stats_design_sync', params: { plot_id: linkedPlot.plot_id, plot_spec: r.plot_spec } }).catch(() => undefined)
    }
    setLinkedPlot({ ...linkedPlot, spec: r.plot_spec })
    console.info('[StatStudio] synced to Plot Studio', { plot: linkedPlot.plot_id, notes: r.notes.length })
    setMessage(`Saved plot "${linkedPlot.name}" now has this design${r.notes.length ? ` (${r.notes.map(n => n.message).join('; ')})` : ''}.`)
  }, [spec, linkedPlot])

  // --- saved tables, presets, export, pipeline ---------------------------------------
  const refreshLists = useCallback(() => {
    if (csvPath) return
    callBackend('stats_saved_list', {}).then(raw => setSavedList((raw as { items: typeof savedList }).items)).catch(() => undefined)
    callBackend('stats_preset_list', {}).then(raw => setPresets((raw as { items: typeof presets }).items)).catch(() => undefined)
  }, [csvPath])
  useEffect(() => { refreshLists() }, [refreshLists])

  const save = async () => {
    if (!spec || !saveName.trim()) return
    const r = await callBackend('stats_saved_save', { name: saveName, spec, overwrite: true, current_stats_id: savedInfo?.stats_id ?? null }) as { ok: boolean; info: typeof savedInfo; error?: string }
    if (!r.ok) { setMessage(r.error ?? 'Not saved.'); return }
    setSavedInfo(r.info)
    setMessage(`Saved "${r.info!.name}" (version ${r.info!.version}).`)
    refreshLists()
  }

  const openSaved = async (statsId: string) => {
    const r = await callBackend('stats_open', { stats_id: statsId }) as { ok: boolean; spec: StatsSpec; info: typeof savedInfo; notes: { message: string }[] }
    if (!r.ok) return
    setSpec(r.spec); setSavedInfo(r.info); setSaveName(r.info?.name ?? ''); setOpenNotes(r.notes.map(n => n.message))
  }

  const exportTab = async (toFile: boolean) => {
    if (!spec) return
    let path: string | null = null
    if (toFile) {
      const ext = { html: 'html', md: 'md', csv: 'csv', latex: 'tex', txt: 'txt', json: 'json' }[exportFormat] ?? 'txt'
      const picked = await callBackend('pick_save_path', { defaultName: `${tab}.${ext}`, formats: [ext], filterName: exportFormat.toUpperCase() }) as { path: string | null }
      if (!picked.path) return
      path = picked.path
    }
    const r = await callBackend('stats_export', { spec, fmt: exportFormat, which: tab, path, ...sourceParams }) as { ok: boolean; text: string; error?: string }
    if (!r.ok) { setMessage(r.error ?? 'Export failed.'); return }
    if (path) { setMessage(`Wrote ${path}.`); return }
    try {
      if (exportFormat === 'html' && typeof ClipboardItem !== 'undefined') {
        await navigator.clipboard.write([new ClipboardItem({
          'text/html': new Blob([r.text], { type: 'text/html' }),
          'text/plain': new Blob([r.text], { type: 'text/plain' }),
        })])
        setMessage('Copied as a formatted table: paste into Word.')
      } else {
        await navigator.clipboard.writeText(r.text)
        setMessage(`Copied as ${exportFormat}.`)
      }
    } catch (err) {
      setMessage(`Could not copy (${String(err)}); use "Save to file".`)
    }
  }

  const addToPipeline = async () => {
    if (!spec) return
    const r = await callBackend('stats_add_to_pipeline', { spec }) as { ok: boolean; error?: string; function_name?: string; file?: string; foreach_source?: string }
    setMessage(r.ok ? `Added ${r.function_name} to ${r.file}. Wire it on the canvas, or run:\n${r.foreach_source}` : (r.error ?? 'Not added.'))
  }

  if (error && !spec) return <div style={{ ...plotThemeVars(theme), ...styles.root }}><div style={styles.error}>{error}</div></div>
  if (!spec) return <div style={{ ...plotThemeVars(theme), ...styles.root }}><div style={styles.muted}>Opening StatStudio…</div></div>

  const catalog = result?.catalog
  const roles = (spec.design.roles ?? {}) as Record<string, string>
  const groups = spec.design.groups ?? []
  const firstAnalysis = spec.comparisons[0]
  const readout = result?.readout ?? []
  // Decisions the user must make before comparisons run (Stage F).
  const actions = pendingActions(result?.comparisons)
  const addable = variables.find(v => v.name === newDV)

  const analysisEditor = (a: Analysis) => {
    const patch = (p: Partial<Analysis>) => setSpec(updateAnalysis(spec, a.id, p))
    const pairTests = (catalog?.tests ?? []).filter(t => t.role === 'pairwise')
    const omniTests = (catalog?.tests ?? []).filter(t => t.role === 'omnibus')
    // Why a test can't run on this data's SHAPE (scistackstats
    // catalog.why_unavailable, via the readout); missing data never greys a
    // test out (user decision 2026-10-19), it is reported instead.
    const shapeReason = (id: string): string | null =>
      readout[0]?.tests?.find(t => t.id === id)?.reason ?? null
    return (
      <div key={a.id} style={styles.analysis}>
        <div style={styles.row}>
          <b>{a.id}</b>
          <select value={a.family} onChange={e => patch({ family: e.target.value })} style={styles.select}>
            {(catalog?.families ?? ['auto', 'parametric', 'nonparametric']).map(f => <option key={f} value={f}>{catalog?.family_labels?.[f] ?? f}</option>)}
          </select>
          {spec.comparisons.length > 1 && (
            <button type="button" style={styles.smallButton} onClick={() => setSpec(removeAnalysis(spec, a.id))}>✕</button>
          )}
        </div>
        {(() => {
          // What the data point to, with the evidence: shown whatever is
          // selected, so a test the user chose instead is a visible choice.
          const rec = recommendedFor(result?.comparisons, a.id)
          if (!rec) return null
          const label = (id: string | null) => (id ? (catalog?.tests.find(t => t.id === id)?.label ?? id) : 'none')
          return (
            <div style={styles.recommended}>
              <div>Recommended: {label(rec.pairwise)} (pairwise), {label(rec.omnibus)} (omnibus)</div>
              {rec.evidence.map((e, i) => <div key={i} style={styles.muted}>• {e}</div>)}
            </div>
          )
        })()}
        {(() => {
          // The model as it runs: the first analysis' text comes from the
          // backend (rebased on the design); another analysis shows its stored
          // text. Rarely changed, so it is one line and a button.
          const model = a.id === firstAnalysis?.id ? readout[0]?.model : undefined
          const shown = model?.text ?? a.formula ?? 'the default model'
          const custom = !!a.formula || (a.covariates?.length ?? 0) > 0
          return (
            <div style={styles.formulaRow}>
              <code style={styles.formula} title={model?.errors.length ? model.errors.map(e => e.message).join('; ') : shown}>
                {targets(spec)[0]?.variable} ~ {shown}
              </code>
              <button type="button" style={styles.smallButton} onClick={() => setFormulaFor(a.id)}
                      title="Covariates and custom random effects, as an lme4 formula">
                Model formula…
              </button>
              {custom && <span style={styles.chip}>{model?.errors.length ? 'formula has problems' : 'custom'}</span>}
            </div>
          )
        })()}
        <div style={styles.field}>Missing data
          <select value={a.missing ?? 'use_all'} onChange={e => patch({ missing: e.target.value })} style={styles.select}
                  title="Recorded with the analysis and stated in every caption">
            {(catalog?.missing_policies ?? [{ id: 'use_all', label: 'Use every observation' }, { id: 'complete_cases', label: 'Complete cases only' }])
              .map(m => <option key={m.id} value={m.id}>{m.label}</option>)}
          </select>
        </div>
        <label style={styles.check}><input type="checkbox" checked={a.pairwise !== false} onChange={e => patch({ pairwise: e.target.checked })} /> Pairwise comparisons (primary)</label>
        <label style={styles.check}><input type="checkbox" checked={a.omnibus !== false} onChange={e => patch({ omnibus: e.target.checked })} /> Omnibus (main effects + interactions)</label>
        <div style={styles.field}>Pairwise test
          <select value={a.pairwise_test ?? ''} onChange={e => patch({ pairwise_test: e.target.value || undefined })} style={styles.select}>
            <option value="">{a.family === 'auto' ? 'Recommended' : 'From the family'}</option>
            {pairTests.map(t => <option key={t.id} value={t.id} disabled={!!shapeReason(t.id)} title={shapeReason(t.id) ?? undefined}>{t.label}</option>)}
          </select>
        </div>
        <div style={styles.field}>Omnibus test
          <select value={a.omnibus_test ?? ''} onChange={e => patch({ omnibus_test: e.target.value || undefined })} style={styles.select}>
            <option value="">{a.family === 'auto' ? 'Recommended' : 'From the family'}</option>
            {omniTests.map(t => <option key={t.id} value={t.id} disabled={!!shapeReason(t.id)} title={shapeReason(t.id) ?? undefined}>{t.label}</option>)}
          </select>
        </div>
        <label style={styles.check}><input type="checkbox" checked={!!a.equal_var} onChange={e => patch({ equal_var: e.target.checked })} /> Assume equal variances (Student, not Welch)</label>
        <div style={styles.field}>Correction
          <select value={a.correction ?? 'holm'} onChange={e => patch({ correction: e.target.value })} style={styles.select}>
            {(catalog?.corrections ?? []).map(c => <option key={c.id} value={c.id}>{c.label}</option>)}
          </select>
        </div>
        <div style={styles.field}>Family
          <select value={a.family_scope ?? 'stratum'} onChange={e => patch({ family_scope: e.target.value })} style={styles.select}>
            {(catalog?.scopes ?? []).map(s => <option key={s.id} value={s.id}>{s.id === 'none' ? 'none' : `within each ${s.label}`}</option>)}
          </select>
        </div>
        <div style={styles.field}>Effect size
          <select value={a.effect_size ?? ''} onChange={e => patch({ effect_size: e.target.value || undefined })} style={styles.select}>
            <option value="">The design's default</option>
            {(catalog?.effect_sizes ?? []).map(es => <option key={es.id} value={es.id} title={es.formula}>{es.name}</option>)}
          </select>
        </div>
        <div style={styles.field}>Difference spread
          <select value={a.difference_spread ?? 'ci'} onChange={e => patch({ difference_spread: e.target.value })} style={styles.select}>
            {(catalog?.spreads ?? ['ci', 'se', 'sd_diff']).map(s => <option key={s} value={s}>{{ ci: 'CI of the difference', se: 'SE of the difference', sd_diff: 'SD of the differences (paired)' }[s] ?? s}</option>)}
          </select>
        </div>
        <div style={styles.row}>
          <span style={styles.field}>α <input type="number" step="0.01" min="0.001" max="0.5" value={a.alpha ?? 0.05} onChange={e => patch({ alpha: Number(e.target.value) })} style={styles.number} /></span>
          <span style={styles.field}>CI <input type="number" step="0.01" min="0.5" max="0.999" value={a.ci_level ?? 0.95} onChange={e => patch({ ci_level: Number(e.target.value) })} style={styles.number} /></span>
        </div>
      </div>
    )
  }

  return (
    <div style={{ ...plotThemeVars(theme), ...styles.root }}>
      <aside style={styles.rail}>
        <div style={styles.title}>StatStudio{busy ? ' · computing…' : ''}</div>
        <div style={styles.muted}>{targets(spec).map(d => d.columns ? `${d.variable}[${d.columns.join(', ')}]` : d.variable).join(', ')}{savedInfo ? ` · saved as "${savedInfo.name}" v${savedInfo.version}` : ''}</div>

        <Section title="Measures">
          {targets(spec).map((dv, i) => (
            <div key={i} style={styles.row}>
              <span>{dv.variable}{dv.columns ? ` [${dv.columns.join(', ')}]` : ''}</span>
              {targets(spec).length > 1 && <button type="button" style={styles.smallButton} onClick={() => setSpec(removeDV(spec, i))}>✕</button>}
            </div>
          ))}
          <div style={styles.row}>
            <select value={newDV} onChange={e => { setNewDV(e.target.value); setNewColumns([]) }} style={styles.select}>
              <option value="">Add a measure…</option>
              {variables.filter(v => v.name).map(v => <option key={v.name} value={v.name}>{v.name}</option>)}
            </select>
            <button type="button" style={styles.smallButton} disabled={!newDV}
                    onClick={() => { setSpec(addDV(spec, newDV, newColumns)); setNewDV(''); setNewColumns([]) }}>Add</button>
          </div>
          {addable?.columns && addable.columns.length > 1 && (
            <div style={styles.columns}>
              <div style={styles.muted}>Columns (each one its own measure; none = the variable as it is):</div>
              {addable.columns.map(c => (
                <label key={c} style={styles.check}>
                  <input type="checkbox" checked={newColumns.includes(c)}
                         onChange={e => setNewColumns(e.target.checked ? [...newColumns, c] : newColumns.filter(x => x !== c))} /> {c}
                </label>
              ))}
            </div>
          )}
        </Section>

        <Section title="Design">
          <div style={styles.muted}>Grouping layers are the table's columns (innermost = the primary comparison). The design syncs with the linked figure.</div>
          {factors.map(f => (
            <div key={f.name} style={styles.row}>
              <span style={styles.factorName}>{f.name}</span>
              <select value={roles[f.name] ?? 'iterate'} onChange={e => setSpec(setRole(spec, f.name, e.target.value))} style={styles.select}>
                {ROLE_CHOICES.map(r => <option key={r.id} value={r.id}>{r.label}</option>)}
              </select>
              {roles[f.name] === 'group' && (
                <>
                  <button type="button" style={styles.smallButton} title="Outward" onClick={() => setSpec(moveGroup(spec, f.name, 1))}>↑</button>
                  <button type="button" style={styles.smallButton} title="Inward" onClick={() => setSpec(moveGroup(spec, f.name, -1))}>↓</button>
                </>
              )}
            </div>
          ))}
          <div style={styles.field}>Variants
            <select value={spec.variant_mode} onChange={e => setSpec({ ...spec, variant_mode: e.target.value })} style={styles.select}
                    title="Sweep: every variant analysed separately, with a specification curve on the Comparisons tab">
              {(catalog?.variant_modes ?? ['pinned', 'sweep']).map(m => <option key={m} value={m}>{m === 'sweep' ? 'Sweep every variant' : 'Pinned (the design\'s selection)'}</option>)}
            </select>
          </div>
          {groups.length > 0 && <div style={styles.muted}>Columns, outermost first: {[...groups].reverse().join(' × ')}</div>}
          {readout.map(r => (
            <div key={r.dv} style={styles.readout}>
              <div><b>{r.dv}</b>{r.error ? `: ${r.error}` : ''}</div>
              {r.rows && <div style={styles.muted}>Unit: {r.rows.unit ?? 'none (rows independent)'}{r.rows.averaged.length ? `; ${r.rows.averaged.join(', ')} averaged first` : ''}{r.design?.n_units != null ? `; n = ${r.design.n_units}` : ''}</div>}
              {r.design?.factors.map(f => (
                <div key={f.name} style={styles.row}>
                  <span style={f.kind === 'mixed_membership' ? styles.chipNeedsAction : styles.chip}
                        title={`${f.units_multi} unit(s) at several levels, ${f.units_single} at one` + (f.basis === 'schema' ? '; within-subject because it is nested below the unit in the schema' : '')}>
                    {f.name}: {f.kind === 'mixed_membership' ? 'needs a decision' : f.kind}{f.overridden ? ' (set)' : f.basis === 'schema' ? ' (schema)' : ''}
                  </span>
                  {firstAnalysis && (
                    <select value={firstAnalysis.overrides?.[f.name] ?? ''} style={styles.select}
                            onChange={e => setSpec(setOverride(spec, firstAnalysis.id, f.name, e.target.value || null))}>
                      <option value="">derived ({f.derived})</option>
                      <option value="within">within</option>
                      <option value="between">between</option>
                    </select>
                  )}
                </div>
              ))}
              {r.design?.warnings.map((w, i) => <div key={i} style={styles.warn}>{w.message}</div>)}
            </div>
          ))}
        </Section>

        <Section title="Descriptives">
          <div style={styles.field}>Cell
            <select value={spec.descriptives.cell} onChange={e => setSpec(setCell(spec, e.target.value))} style={styles.select}>
              {(catalog?.cells ?? []).map(c => <option key={c.id} value={c.id}>{c.header}</option>)}
            </select>
          </div>
          <div style={styles.statGrid}>
            {(catalog?.statistics ?? []).map(s => (
              <label key={s.id} style={styles.check}>
                <input type="checkbox" checked={spec.descriptives.statistics.includes(s.id)}
                       onChange={() => setSpec(toggleStatistic(spec, s.id, (catalog?.statistics ?? []).map(x => x.id)))} /> {s.label}
              </label>
            ))}
          </div>
        </Section>

        <Section title="Comparisons">
          {spec.comparisons.map(analysisEditor)}
          <button type="button" style={styles.smallButton} onClick={() => setSpec(addAnalysis(spec))}>+ Analysis</button>
        </Section>

        <Section title="Format">
          <div style={styles.field}>Decimals
            <select value={spec.formatting?.decimals ?? 'auto'} style={styles.select}
                    onChange={e => setSpec(setFormatting(spec, { decimals: e.target.value === 'auto' ? undefined : Number(e.target.value) }))}>
              <option value="auto">Automatic per measure</option>
              {[0, 1, 2, 3, 4, 5].map(d => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
          <div style={styles.field}>p decimals
            <select value={spec.formatting?.p_decimals ?? 3} onChange={e => setSpec(setFormatting(spec, { p_decimals: Number(e.target.value) }))} style={styles.select}>
              {[2, 3, 4].map(d => <option key={d} value={d}>{d}</option>)}
            </select>
          </div>
          <label style={styles.check}><input type="checkbox" checked={!!spec.formatting?.stars} onChange={e => setSpec(setFormatting(spec, { stars: e.target.checked }))} /> Stars on adjusted p</label>
        </Section>

        {linkedId && (
          <Section title="Brackets on the figure">
            <label style={styles.check}><input type="checkbox" checked={!!spec.overlay?.show} onChange={e => setSpec(setOverlay(spec, { show: e.target.checked }))} /> Draw test brackets on the linked figure</label>
            <div style={styles.field}>Pairs
              <select value={spec.overlay?.pairs ?? 'significant'} onChange={e => setSpec(setOverlay(spec, { pairs: e.target.value }))} style={styles.select}>
                {(catalog?.bar_pairs ?? []).map(p => <option key={p} value={p}>{p}</option>)}
              </select>
            </div>
            <div style={styles.field}>Label
              <select value={spec.overlay?.label ?? 'stars'} onChange={e => setSpec(setOverlay(spec, { label: e.target.value }))} style={styles.select}>
                {(catalog?.bar_labels ?? []).map(p => <option key={p} value={p}>{p === 'p' ? 'adjusted p' : 'stars'}</option>)}
              </select>
            </div>
            <div style={styles.muted}>Save this table, then the figure draws them (Plot Studio reads its linked table).</div>
          </Section>
        )}

        {!csvPath && (
          <Section title="Save">
            <div style={styles.row}>
              <input value={saveName} placeholder="Table name" onChange={e => setSaveName(e.target.value)} style={styles.input} />
              <button type="button" style={styles.primaryButton} onClick={save} disabled={!saveName.trim()}>Save</button>
            </div>
            {savedList.length > 0 && <div style={styles.muted}>Saved tables:</div>}
            {savedList.map(s => (
              <button key={s.stats_id} type="button" style={styles.listButton} onClick={() => openSaved(s.stats_id)}>{s.name} (v{s.version})</button>
            ))}
            <div style={styles.muted}>Stats presets (settings only, any measure):</div>
            {presets.map(p => (
              <button key={p.preset_id} type="button" style={styles.listButton}
                      onClick={async () => { const r = await callBackend('stats_preset_apply', { preset_id: p.preset_id, spec }) as { spec: StatsSpec }; setSpec(r.spec) }}>
                Apply {p.name}
              </button>
            ))}
            <button type="button" style={styles.smallButton} disabled={!saveName.trim()}
                    onClick={async () => { await callBackend('stats_preset_save', { name: saveName, spec }); refreshLists() }}>
              Save settings as preset "{saveName}"
            </button>
          </Section>
        )}
      </aside>

      <main style={styles.main}>
        {linkedPlot && differences.length > 0 && (
          <div style={styles.banner}>
            <div><b>This table's design differs from the figure "{linkedPlot.name}":</b></div>
            <ul style={styles.notes}>
              {differences.map(d => <li key={d.path}>{d.label}: figure {d.a} · table {d.b}</li>)}
            </ul>
            <button type="button" style={styles.primaryButton} onClick={syncToPlot}>Sync to PlotStudio</button>
          </div>
        )}
        {spec.linked_plot && !linkedPlot && !csvPath && <div style={styles.muted}>Linked figure not found; save the plot to link it.</div>}
        {!spec.linked_plot && <div style={styles.muted}>Not linked to a saved plot.</div>}
        {openNotes.length > 0 && <ul style={styles.notes}>{openNotes.map((n, i) => <li key={i}>{n}</li>)}</ul>}
        {message && <pre style={styles.message}>{message}</pre>}
        {error && <div style={styles.error}>{error}</div>}

        <div style={styles.toolbar}>
          <button type="button" style={tab === 'descriptives' ? styles.tabOn : styles.tab} onClick={() => setTab('descriptives')}>Descriptives</button>
          <button type="button" style={tab === 'comparisons' ? styles.tabOn : styles.tab} onClick={() => setTab('comparisons')}
                  title={actions.length ? 'A decision is needed before comparisons can run' : undefined}>
            Comparisons{actions.length > 0 && <span style={styles.badge}>!</span>}
          </button>
          <span style={{ flex: 1 }} />
          <select value={exportFormat} onChange={e => setExportFormat(e.target.value)} style={styles.select} title="html pastes into Word as a table">
            {FORMATS.map(f => <option key={f} value={f}>{f}</option>)}
          </select>
          <button type="button" style={styles.smallButton} onClick={() => exportTab(false)}>Copy</button>
          {isVSCodeMode && <button type="button" style={styles.smallButton} onClick={() => exportTab(true)}>Save to file…</button>}
          {!csvPath && (
            <button type="button" style={styles.primaryButton} onClick={addToPipeline}
                    disabled={!!result?.not_exportable} title={result?.not_exportable ?? 'Write a stat_ endpoint into the project'}>
              Add to pipeline
            </button>
          )}
        </div>

        {/* Stage F: a decision the user must make is a banner with the
            choices, on both tabs, not a table of "not run" rows. The wording
            and choices come from scistackstats.catalog.needed_action. */}
        {actions.map(act => (
          <div key={`${act.analysisId}|${act.factor}`} style={styles.actionBanner} role="alert">
            <div>{act.message}</div>
            <div style={styles.row}>
              {act.choices.map(choice => (
                <button key={choice} type="button" style={styles.primaryButton}
                        onClick={() => setSpec(setOverride(spec, act.analysisId, act.factor, choice))}>
                  {act.factor} is {choice}-subject
                </button>
              ))}
            </div>
          </div>
        ))}
        {result?.refusals.map((r, i) => <div key={i} style={styles.warn}>{r.dv}: {r.reason}</div>)}
        {tab === 'descriptives' && result?.descriptives.map((g, i) => <GridTable key={i} grid={g} />)}
        {tab === 'comparisons' && (
          spec.comparisons.length === 0
            ? <div style={styles.muted}>No analyses: add one in the rail.</div>
            : result?.comparisons.map(c => (
              <div key={c.id}>
                <GridTable grid={c.grid} />
                {c.omnibus && <GridTable grid={c.omnibus} />}
                {c.sweep && <GridTable grid={c.sweep} />}
                <div style={styles.gridTitle}>Results as sentences</div>
                <ul style={styles.notes}>{c.sentences.map((s, i) => <li key={i}>{s}</li>)}</ul>
                <button type="button" style={styles.smallButton}
                        onClick={() => navigator.clipboard?.writeText(c.sentences.join('\n')).catch(() => undefined)}>Copy sentences</button>
              </div>
            ))
        )}
      </main>
      {formulaFor && (
        <ModelFormulaDialog
          spec={spec}
          analysisId={formulaFor}
          measure={targets(spec)[0]?.variable ?? ''}
          sourceParams={sourceParams}
          canPickCovariates={!csvPath}
          onCancel={() => setFormulaFor(null)}
          onApply={next => { setSpec(next); setFormulaFor(null); setMessage('Model formula applied.') }}
        />
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: { display: 'flex', height: '100%', background: 'var(--ps-bg)', color: 'var(--ps-text)', fontFamily: 'sans-serif', fontSize: 13 },
  rail: { width: 330, minWidth: 260, overflowY: 'auto', borderRight: '1px solid var(--ps-border)', padding: 10, background: 'var(--ps-surface)' },
  main: { flex: 1, overflow: 'auto', padding: 14 },
  title: { fontSize: 16, fontWeight: 600, marginBottom: 4, color: 'var(--ps-text-strong)' },
  muted: { color: 'var(--ps-text-muted)', fontSize: 12, margin: '4px 0' },
  railSection: { borderTop: '1px solid var(--ps-border-faint)', marginTop: 8, paddingTop: 4 },
  railHeader: { background: 'none', border: 'none', color: 'var(--ps-text-strong)', fontWeight: 600, cursor: 'pointer', padding: '4px 0', fontSize: 13 },
  railBody: { display: 'flex', flexDirection: 'column', gap: 4, paddingLeft: 4 },
  row: { display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' },
  field: { display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'space-between' },
  check: { display: 'flex', alignItems: 'center', gap: 4, fontSize: 12 },
  select: { background: 'var(--ps-field)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '2px 4px', fontSize: 12, maxWidth: 190 },
  input: { background: 'var(--ps-field)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '3px 6px', flex: 1 },
  number: { width: 60, background: 'var(--ps-field)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 3 },
  smallButton: { background: 'var(--ps-control)', color: 'var(--ps-text-control)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '2px 8px', cursor: 'pointer', fontSize: 12 },
  primaryButton: { background: 'var(--ps-accent)', color: 'var(--ps-accent-fg)', border: '1px solid var(--ps-accent-border)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer', fontSize: 12 },
  listButton: { background: 'none', border: 'none', color: 'var(--ps-accent-text)', textAlign: 'left', cursor: 'pointer', padding: '1px 0', fontSize: 12 },
  factorName: { minWidth: 70 },
  chip: { border: '1px solid var(--ps-border-soft)', borderRadius: 10, padding: '0 6px', fontSize: 11 },
  formulaRow: { display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' },
  formula: { fontFamily: 'monospace', fontSize: 11, color: 'var(--ps-text-body)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 300 },
  chipNeedsAction: { border: '1px solid var(--ps-warn)', color: 'var(--ps-warn)', borderRadius: 10, padding: '0 6px', fontSize: 11, fontWeight: 600 },
  badge: { marginLeft: 6, background: 'var(--ps-warn)', color: 'var(--ps-bg)', borderRadius: 8, padding: '0 5px', fontSize: 11, fontWeight: 700 },
  actionBanner: { border: '1px solid var(--ps-warn)', borderLeft: '4px solid var(--ps-warn)', borderRadius: 4, padding: '8px 10px', margin: '8px 0', display: 'flex', flexDirection: 'column', gap: 6 },
  recommended: { border: '1px solid var(--ps-border-soft)', borderRadius: 4, padding: '4px 6px', margin: '4px 0', fontSize: 12 },
  readout: { borderLeft: '2px solid var(--ps-border-soft)', paddingLeft: 6, margin: '4px 0' },
  analysis: { border: '1px solid var(--ps-border-faint)', borderRadius: 4, padding: 6, display: 'flex', flexDirection: 'column', gap: 4 },
  statGrid: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 2 },
  columns: { paddingLeft: 8 },
  toolbar: { display: 'flex', alignItems: 'center', gap: 6, borderBottom: '1px solid var(--ps-border)', paddingBottom: 6, marginBottom: 10 },
  tab: { background: 'none', border: '1px solid var(--ps-border-faint)', color: 'var(--ps-text-secondary)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer' },
  tabOn: { background: 'var(--ps-accent-bg-strong)', border: '1px solid var(--ps-accent-border)', color: 'var(--ps-text-strong)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer' },
  banner: { background: 'var(--ps-warn-bg)', border: '1px solid var(--ps-warn-border)', borderRadius: 4, padding: 8, marginBottom: 10 },
  message: { background: 'var(--ps-note-bg)', border: '1px solid var(--ps-note-border)', color: 'var(--ps-note-text)', borderRadius: 4, padding: 6, whiteSpace: 'pre-wrap', fontSize: 12 },
  error: { color: 'var(--ps-error)', margin: '6px 0' },
  warn: { color: 'var(--ps-warn)', fontSize: 12 },
  gridBox: { background: 'var(--ps-surface)', border: '1px solid var(--ps-border-faint)', borderRadius: 4, padding: 10, marginBottom: 12 },
  gridTitle: { fontStyle: 'italic', marginBottom: 6, color: 'var(--ps-text-strong)' },
  table: { borderCollapse: 'collapse', borderTop: '1px solid var(--ps-border-strong)', borderBottom: '1px solid var(--ps-border-strong)' },
  th: { fontWeight: 'normal', padding: '3px 10px' },
  thLast: { borderBottom: '1px solid var(--ps-border-strong)' },
  td: { padding: '2px 10px', fontVariantNumeric: 'tabular-nums' },
  section: { fontStyle: 'italic', padding: '4px 10px', color: 'var(--ps-text-secondary)' },
  caption: { fontSize: 12, marginTop: 6, color: 'var(--ps-text-body)', maxWidth: 900 },
  notes: { fontSize: 12, color: 'var(--ps-text-secondary)', margin: '4px 0', paddingLeft: 18 },
}
