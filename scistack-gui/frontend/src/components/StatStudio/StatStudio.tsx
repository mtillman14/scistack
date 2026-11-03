/**
 * StatStudio — its own tab (S10): Descriptives | Comparisons for one stats spec.
 *
 * A thin wrapper. Every number, caption, note and option list comes from the
 * backend (`stats_compute`: Grids + the `catalog`), so this file formats
 * nothing and knows no statistics vocabulary. Spec edits are pure helpers in
 * `statsSpec.ts`.
 *
 * The same studio as Plot Studio (docs/claude/studio-unification.md):
 *
 * - **The same front half.** The rail's Data and Structure groups ARE Plot
 *   Studio's, mounted in design-only mode on this table's design carrier
 *   (`PlotStudio design={…}`). Only the output differs: Measures where the
 *   figure has Chart, then Statistics (descriptives and tests) and Format.
 * - **The same words.** Role names come from the backend
 *   (`scistackplot.role_labels`, table surface); nothing here hardcodes one.
 * - **The same shell.** Rail groups (`Studio/RailGroup`), the figure-style
 *   toolbar (Format, Save to file, Copy, Export code, Add to pipeline) and
 *   the right rail (Saved tables, Presets: Plot Studio's own components).
 * - **One shared design** with a linked saved plot (`scistackplotdb.designs`):
 *   a design edit here goes to `design_update` and is broadcast as
 *   `design_changed`; the figure's edits arrive the same way. There is no
 *   divergence banner and no Sync button, because there is one design.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { addNotificationHandler, callBackend, isVSCodeMode } from '../../api'
import PlotStudio from '../PlotStudio/PlotStudio'
import PresetsSection, { type PresetSaveResult } from '../PlotStudio/PresetsSection'
import { plotThemeVars } from '../PlotStudio/plotTheme'
import type { PresetInfo } from '../PlotStudio/presets'
import SavedPlotsRail, { type SaveResult } from '../PlotStudio/SavedPlotsRail'
import type { SavedPlotInfo } from '../PlotStudio/savedPlots'
import { usePlotTheme } from '../PlotStudio/usePlotTheme'
import { Group, Section } from '../Studio/RailGroup'
import { DESIGN_CHANGED, designChangeFor, newDesignOrigin } from '../Studio/sharedDesign'
import { reportStudioOpen } from '../Studio/studioOpen'
import GridTable from './GridTable'
import GuidedStudio from './Guided/GuidedStudio'
import { clampToBlocked, currentStep, type GuidePayload } from './Guided/guideSteps'
import ModelFormulaDialog from './ModelFormulaDialog'
import { pendingActions, recommendedFor, type StratumSummary } from './statsActions'
import {
  addAnalysis, addDV, analyses, computeKey, guideState, removeAnalysis, removeDV, setCell, setFormatting,
  setGuide, setOverlay, setOverride, targets, toggleStatistic, updateAnalysis,
  type Analysis, type DescriptiveCell, type FactorKind, type Grid, type Overlay, type StatsSpec,
} from './statsSpec'
import type { PlotSpec } from '../../generated/api-types'

export interface StatViewConfig {
  view: 'stat'
  variable?: string | null
  csvPath?: string | null
  statsId?: string | null
  seed?: PlotSpec | null
  linkedPlot?: string | null
  /** Which way in (Studio/studioOpen.ts), logged once. */
  from?: string | null
  /** A stat_ node to open on: the spec its generated function embeds. */
  statNode?: string | null
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
  /** The wizard's steps (scistackstats.guide), with the run's results. */
  guide?: GuidePayload
  descriptives: Grid[]
  comparisons: ComparisonResult[]
  readout: Readout[]
  refusals: { dv: string; reason: string }[]
  not_exportable: string | null
  catalog: Catalog
}

/** A saved table as the backend lists it (`SavedStatsInfo.to_dict`). */
interface SavedStatsInfo { stats_id: string; name: string; version: number; saved_at: string; hidden: boolean; linked_plot?: string | null }
interface StatsPresetInfo { preset_id: string; name: string; version: number; saved_at: string; hidden: boolean }

interface OpenReply {
  ok: boolean
  error?: string
  spec: StatsSpec
  notes: { message: string }[]
  seed_notes: string[]
  info: SavedStatsInfo | null
  role_labels?: Record<string, string>
  design_linked?: boolean
}

const FORMATS = ['html', 'md', 'csv', 'latex', 'txt', 'json']

/** The saved-work rail is Plot Studio's; it lists items in its own shape. */
const asRailItem = (s: SavedStatsInfo): SavedPlotInfo => ({
  plot_id: s.stats_id, variable: '', name: s.name, version: s.version, saved_at: s.saved_at, hidden: s.hidden,
})
const asPresetItem = (p: StatsPresetInfo): PresetInfo => ({ ...p, made_on: null, made_on_shape: null })

const SAVED_WORDS = {
  noun: 'table',
  title: 'Saved tables',
  empty: 'No saved tables yet. Save one to reopen it later exactly as it is now.',
}
const PRESET_WORDS = {
  noun: 'table',
  hint: "This table's settings without its measures or design. Apply one to give another table the same treatment.",
}

// Which of StatStudio's own rail groups are open, per viewer. Storage may be
// missing or throw (private window, preview); the defaults always render.
type StatGroupId = 'measures' | 'statistics' | 'format'
const OPEN_GROUPS_KEY = 'scistack.statStudio.openGroups'
const DEFAULT_OPEN: Record<StatGroupId, boolean> = { measures: true, statistics: true, format: false }
function readOpenGroups(): Record<StatGroupId, boolean> {
  try {
    const raw = window.localStorage.getItem(OPEN_GROUPS_KEY)
    return raw ? { ...DEFAULT_OPEN, ...(JSON.parse(raw) as Partial<Record<StatGroupId, boolean>>) } : DEFAULT_OPEN
  } catch {
    return DEFAULT_OPEN
  }
}

export default function StatStudio({ initial }: { initial: StatViewConfig }) {
  const theme = usePlotTheme()
  const csvPath = initial.csvPath ?? undefined
  const [spec, setSpec] = useState<StatsSpec | null>(null)
  const [savedInfo, setSavedInfo] = useState<SavedStatsInfo | null>(null)
  const [openNotes, setOpenNotes] = useState<string[]>([])
  const [result, setResult] = useState<ComputeResult | null>(null)
  const [guidePayload, setGuidePayload] = useState<GuidePayload | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [tab, setTab] = useState<'descriptives' | 'comparisons'>('descriptives')
  const [variables, setVariables] = useState<{ name: string; columns?: string[] }[]>([])
  const [roleLabels, setRoleLabels] = useState<Record<string, string>>({})
  const [message, setMessage] = useState('')
  const [exportFormat, setExportFormat] = useState('html')
  const [newDV, setNewDV] = useState('')
  const [newColumns, setNewColumns] = useState<string[]>([])
  const [savedList, setSavedList] = useState<SavedStatsInfo[]>([])
  const [presets, setPresets] = useState<StatsPresetInfo[]>([])
  const [savedBusy, setSavedBusy] = useState(false)
  const [savedError, setSavedError] = useState('')
  const [savedRailCollapsed, setSavedRailCollapsed] = useState(false)
  const [appliedPreset, setAppliedPreset] = useState<PresetInfo | null>(null)
  const [code, setCode] = useState<string | null>(null)
  // `computeKey` of the table as last opened or saved: "modified" in the rail.
  const [baseline, setBaseline] = useState<string | null>(null)
  const [openGroups, setOpenGroups] = useState<Record<StatGroupId, boolean>>(readOpenGroups)
  useEffect(() => {
    try { window.localStorage.setItem(OPEN_GROUPS_KEY, JSON.stringify(openGroups)) } catch { /* not remembered */ }
  }, [openGroups])
  const toggleGroup = (id: StatGroupId) => setOpenGroups(prev => ({ ...prev, [id]: !prev[id] }))
  // The analysis whose Model formula popup is open (plan-statstudio-model-formula).
  const [formulaFor, setFormulaFor] = useState<string | null>(null)
  const generation = useRef(0)
  const sourceParams = useMemo(() => (csvPath ? { csv_path: csvPath } : {}), [csvPath])
  // This tab's id in `design_changed` broadcasts, so it ignores its own.
  const origin = useRef(newDesignOrigin('stat'))

  // --- open -------------------------------------------------------------------
  useEffect(() => {
    reportStudioOpen(callBackend, 'stat', initial.from ?? (initial.statsId ? 'saved' : initial.seed ? 'plot-studio' : null),
      initial.variable ?? initial.statNode ?? null)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  useEffect(() => {
    callBackend('stats_open', {
      stats_id: initial.statsId ?? null, seed: initial.seed ?? null, plot_id: initial.linkedPlot ?? null,
      variable: initial.variable ?? null, stat_function: initial.statNode ?? null, ...sourceParams,
    }).then(raw => {
      const r = raw as OpenReply
      if (!r.ok) { setError(r.error ?? 'Could not open StatStudio.'); return }
      console.info('[StatStudio] opened', { statsId: initial.statsId, seeded: !!initial.seed, statNode: initial.statNode, notes: r.notes.length, linked: r.design_linked })
      setSpec(r.spec)
      setBaseline(computeKey(r.spec))
      setSavedInfo(r.info)
      setRoleLabels(r.role_labels ?? {})
      setOpenNotes([...r.seed_notes.map(n => `Set up: ${n}`), ...r.notes.map(n => n.message)])
    }).catch(err => setError(String(err)))
  }, [initial.statsId, initial.seed, initial.linkedPlot, initial.variable, initial.statNode, sourceParams])

  const firstVariable = spec ? targets(spec)[0]?.variable : undefined
  const designVariable = (spec?.design.measures?.[0] as string | undefined) ?? firstVariable
  const mode = spec ? guideState(spec).mode : 'advanced'
  // The Advanced rail's design controls hand the catalog up (onDescribe). The
  // step-by-step view mounts none, so it asks once itself.
  useEffect(() => {
    if (!firstVariable || mode !== 'guided' || variables.length) return
    callBackend('plot_describe', { variable: firstVariable, ...sourceParams }).then(raw => {
      const r = raw as { catalog?: { measures?: { name: string; columns?: string[] }[] } }
      setVariables(r.catalog?.measures ?? [])
    }).catch(err => console.warn('[StatStudio] describe failed', err))
  }, [firstVariable, sourceParams, mode, variables.length])

  // --- compute (debounced, latest wins) ----------------------------------------
  // The view state (spec.guide) is not part of the key: moving between steps
  // or views never recomputes. The wizard's design steps ask for the cheap
  // preview (no tests); a step that reads results, and the Advanced view, ask
  // for the full compute (plan-statstudio-guided).
  const key = useMemo(() => (spec ? computeKey(spec) : ''), [spec])
  const specRef = useRef(spec)
  specRef.current = spec
  const shownStep = spec && guidePayload ? currentStep(spec, guidePayload.steps) : null
  const wantResults = mode === 'advanced' || !!guidePayload?.steps.find(s => s.id === shownStep)?.needs_results
  const haveResultsFor = useRef('')
  useEffect(() => {
    const current = specRef.current
    if (!current || !key) return
    if (!wantResults && haveResultsFor.current === key) return // the full result already covers the preview
    const mine = ++generation.current
    const method = wantResults ? 'stats_compute' : 'stats_preview'
    const timer = window.setTimeout(() => {
      setBusy(true)
      const t0 = performance.now()
      callBackend(method, { spec: current, ...sourceParams }).then(raw => {
        if (mine !== generation.current) return
        const r = raw as ComputeResult
        console.info(`[StatStudio] ${method}`, { ms: Math.round(performance.now() - t0), ok: r.ok })
        if (wantResults) { setResult(r); haveResultsFor.current = key }
        if (r.guide) setGuidePayload(r.guide)
        setError(r.ok ? '' : r.error ?? 'Could not compute.')
      }).catch(err => { if (mine === generation.current) setError(String(err)) })
        .finally(() => { if (mine === generation.current) setBusy(false) })
    }, 350)
    return () => window.clearTimeout(timer)
  }, [key, wantResults, sourceParams])

  // A fresh guide that blocks a step before the one shown pulls the wizard back.
  useEffect(() => {
    if (!guidePayload) return
    setSpec(prev => {
      if (!prev || guideState(prev).mode !== 'guided') return prev
      const clamped = clampToBlocked(prev, guidePayload.steps)
      if (clamped !== prev) console.info('[StatStudio] guided: back to the blocked step', { step: clamped.guide?.step })
      return clamped
    })
  }, [guidePayload])

  // --- the shared design (scistackplotdb.designs) ----------------------------------
  const linkedId = spec?.linked_plot
  const designJson = useMemo(() => (spec ? JSON.stringify(spec.design) : ''), [spec])
  useEffect(() => {
    const current = specRef.current
    if (!current || !linkedId || csvPath || !designJson) return
    const timer = window.setTimeout(() => {
      callBackend('design_update', { plot_id: linkedId, spec: current.design }).then(raw => {
        const r = raw as { linked: boolean; changed: boolean; version: number | null }
        if (!r.changed) return
        console.info('[StatStudio] shared design changed', { plot: linkedId, version: r.version })
        if (isVSCodeMode) {
          callBackend('broadcast_panels', {
            method: DESIGN_CHANGED, params: { plot_id: linkedId, version: r.version, origin: origin.current },
          }).catch(() => undefined)
        }
      }).catch(err => console.warn('[StatStudio] design_update failed', err))
    }, 500)
    return () => window.clearTimeout(timer)
  }, [designJson, linkedId, csvPath])

  useEffect(() => addNotificationHandler(msg => {
    const current = specRef.current
    const change = designChangeFor(msg, current?.linked_plot, origin.current)
    if (!change || !current) return
    callBackend('design_apply', { plot_id: change.plot_id, spec: current.design }).then(raw => {
      const r = raw as { spec: StatsSpec['design']; version: number | null }
      console.info('[StatStudio] shared design taken from Plot Studio', { plot: change.plot_id, version: r.version })
      setSpec(prev => (prev ? { ...prev, design: r.spec } : prev))
      setMessage('Design updated from the linked figure.')
    }).catch(err => console.warn('[StatStudio] design_apply failed', err))
  }), [])

  // --- saved tables, presets, export, pipeline ---------------------------------------
  const refreshLists = useCallback(() => {
    if (csvPath) return
    callBackend('stats_saved_list', {}).then(raw => setSavedList((raw as { items: SavedStatsInfo[] }).items)).catch(() => undefined)
    callBackend('stats_preset_list', {}).then(raw => setPresets((raw as { items: StatsPresetInfo[] }).items)).catch(() => undefined)
  }, [csvPath])
  useEffect(() => { refreshLists() }, [refreshLists])

  const saveTable = async (name: string, overwrite: boolean): Promise<SaveResult> => {
    if (!spec) return { ok: false }
    setSavedBusy(true); setSavedError('')
    try {
      const r = await callBackend('stats_saved_save', { name, spec, overwrite, current_stats_id: savedInfo?.stats_id ?? null }) as
        { ok: boolean; info?: SavedStatsInfo; exists?: SavedStatsInfo; error?: string }
      if (!r.ok) {
        if (r.exists) return { ok: false, exists: asRailItem(r.exists) }
        setSavedError(r.error ?? 'Not saved.')
        return { ok: false }
      }
      setSavedInfo(r.info!)
      setBaseline(computeKey(spec))
      setMessage(`Saved "${r.info!.name}" (version ${r.info!.version}).`)
      refreshLists()
      return { ok: true }
    } finally {
      setSavedBusy(false)
    }
  }

  const openSaved = async (statsId: string) => {
    const r = await callBackend('stats_open', { stats_id: statsId }) as OpenReply
    if (!r.ok) { setSavedError(r.error ?? 'Could not open.'); return }
    setSpec(r.spec); setSavedInfo(r.info); setBaseline(computeKey(r.spec)); setOpenNotes(r.notes.map(n => n.message))
  }

  const renameSaved = async (statsId: string, name: string) => {
    try {
      await callBackend('stats_saved_rename', { stats_id: statsId, name })
      refreshLists()
      if (savedInfo?.stats_id === statsId) setSavedInfo({ ...savedInfo, name })
      return true
    } catch (err) {
      setSavedError(String(err))
      return false
    }
  }

  const removeSaved = async (statsId: string) => {
    await callBackend('stats_saved_hide', { stats_id: statsId, hidden: true }).catch(err => setSavedError(String(err)))
    refreshLists()
  }

  const savePreset = async (name: string): Promise<PresetSaveResult> => {
    if (!spec) return { ok: false }
    await callBackend('stats_preset_save', { name, spec })
    refreshLists()
    return { ok: true }
  }
  const applyPreset = async (presetId: string) => {
    if (!spec) return
    const r = await callBackend('stats_preset_apply', { preset_id: presetId, spec }) as { spec: StatsSpec }
    setSpec(r.spec)
    const applied = presets.find(p => p.preset_id === presetId)
    setAppliedPreset(applied ? asPresetItem(applied) : null)
  }
  const renamePreset = async (presetId: string, name: string) => {
    try {
      await callBackend('stats_preset_rename', { preset_id: presetId, name })
      refreshLists()
      return true
    } catch (err) {
      setSavedError(String(err))
      return false
    }
  }
  const removePreset = async (presetId: string) => {
    await callBackend('stats_preset_hide', { preset_id: presetId, hidden: true }).catch(() => undefined)
    refreshLists()
  }

  const exportTab = async (toFile: boolean, which: 'descriptives' | 'comparisons' = tab) => {
    if (!spec) return
    let path: string | null = null
    if (toFile) {
      const ext = { html: 'html', md: 'md', csv: 'csv', latex: 'tex', txt: 'txt', json: 'json' }[exportFormat] ?? 'txt'
      const picked = await callBackend('pick_save_path', { defaultName: `${which}.${ext}`, formats: [ext], filterName: exportFormat.toUpperCase() }) as { path: string | null }
      if (!picked.path) return
      path = picked.path
    }
    const r = await callBackend('stats_export', { spec, fmt: exportFormat, which, path, ...sourceParams }) as { ok: boolean; text: string; error?: string }
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

  const exportCode = async () => {
    if (!spec) return
    const r = await callBackend('stats_export_code', { spec, ...sourceParams }) as { ok: boolean; code?: string; error?: string }
    if (r.ok && r.code) setCode(r.code)
    else setMessage(r.error ?? 'Not exportable.')
  }

  const addToPipeline = async () => {
    if (!spec) return
    const r = await callBackend('stats_add_to_pipeline', { spec }) as { ok: boolean; error?: string; function_name?: string; file?: string; foreach_source?: string }
    setMessage(r.ok ? `Added ${r.function_name} to ${r.file}. Wire it on the canvas, or run:\n${r.foreach_source}` : (r.error ?? 'Not added.'))
  }

  // Plot Studio's design rail edits the design carrier; this tab owns it.
  const onDesign = useCallback((design: Record<string, unknown>) => {
    setSpec(prev => (prev ? { ...prev, design: design as unknown as StatsSpec['design'] } : prev))
  }, [])
  const onDescribe = useCallback((d: { catalog?: { measures?: { name: string; columns?: string[] }[] } }) => {
    setVariables(d.catalog?.measures ?? [])
  }, [])

  if (error && !spec) return <div style={{ ...plotThemeVars(theme), ...styles.root }}><div style={styles.error}>{error}</div></div>
  if (!spec) return <div style={{ ...plotThemeVars(theme), ...styles.root }}><div style={styles.muted}>Opening StatStudio…</div></div>

  const catalog = result?.catalog
  const firstAnalysis = analyses(spec)[0]
  const readout = result?.readout ?? []
  // Decisions the user must make before comparisons run (Stage F).
  const actions = pendingActions(result?.comparisons)
  const addable = variables.find(v => v.name === newDV)
  const title = `Stats — ${targets(spec).map(d => d.columns ? `${d.variable}[${d.columns.join(', ')}]` : d.variable).join(', ')}`

  // --- pieces both views show -----------------------------------------------------
  const switchMode = (next: 'guided' | 'advanced') => {
    if (next === mode) return
    setSpec(setGuide(spec, { mode: next }))
    callBackend('stats_guide_log', { event: `view ${next}`, step: guideState(spec).step ?? null }).catch(() => undefined)
  }
  const modeSwitch = (
    <div style={styles.modeSwitch} role="group" aria-label="View">
      <button type="button" style={mode === 'guided' ? styles.tabOn : styles.tab} onClick={() => switchMode('guided')}
              title="One decision at a time, each explained">Step by step</button>
      <button type="button" style={mode === 'advanced' ? styles.tabOn : styles.tab} onClick={() => switchMode('advanced')}
              title="Every setting at once">Advanced</button>
    </div>
  )

  const measuresEditor = (
    <>
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
    </>
  )

  const notices = (
    <>
      {linkedId && !csvPath && (
        <div style={styles.muted}>This table and its saved figure share one design: a change in either shows in both.</div>
      )}
      {openNotes.length > 0 && <ul style={styles.notes}>{openNotes.map((n, i) => <li key={i}>{n}</li>)}</ul>}
      {message && <pre style={styles.message}>{message}</pre>}
      {error && <div style={styles.error}>{error}</div>}
    </>
  )

  const exportBar = (which: 'descriptives' | 'comparisons') => (
    <div style={styles.row}>
      <select value={exportFormat} onChange={e => setExportFormat(e.target.value)} style={styles.select} title="html pastes into Word as a table">
        {FORMATS.map(f => <option key={f} value={f}>{f}</option>)}
      </select>
      {isVSCodeMode && <button type="button" style={styles.smallButton} onClick={() => exportTab(true, which)}>Save to file…</button>}
      <button type="button" style={styles.smallButton} onClick={() => exportTab(false, which)}>Copy</button>
    </div>
  )

  const codeOverlay = code !== null && (
    <div style={styles.codeOverlay} onClick={() => setCode(null)}>
      <pre style={styles.code} onClick={e => e.stopPropagation()}>{code}</pre>
    </div>
  )

  const savedRail = !csvPath && (
    <SavedPlotsRail
      collapsed={savedRailCollapsed}
      onToggle={() => setSavedRailCollapsed(v => !v)}
      plots={savedList.map(asRailItem)}
      loaded={savedInfo ? asRailItem(savedInfo) : null}
      modified={baseline !== null && baseline !== key}
      notes={[]}
      busy={savedBusy}
      error={savedError}
      onSave={saveTable}
      onOpen={openSaved}
      onRename={renameSaved}
      onRemove={removeSaved}
      onDismissNotes={() => undefined}
      words={SAVED_WORDS}
      panelKey="statStudio.savedTables"
    >
      <PresetsSection
        presets={presets.map(asPresetItem)}
        applied={appliedPreset}
        notes={[]}
        busy={savedBusy}
        error=""
        onSave={savePreset}
        onApply={applyPreset}
        onRename={renamePreset}
        onRemove={removePreset}
        onDismissNotes={() => setAppliedPreset(null)}
        words={PRESET_WORDS}
      />
    </SavedPlotsRail>
  )

  // The wizard's last step: the same grids as the Advanced tabs.
  const resultsView = (
    <div>
      {result?.refusals.map((r, i) => <div key={i} style={styles.warn}>{r.dv}: {r.reason}</div>)}
      {result?.comparisons.map(c => (
        <div key={c.id}>
          <GridTable grid={c.grid} />
          {c.omnibus && <GridTable grid={c.omnibus} />}
          {c.sweep && <GridTable grid={c.sweep} />}
          <div style={styles.gridTitle}>Results as sentences</div>
          <ul style={styles.notes}>{c.sentences.map((s, i) => <li key={i}>{s}</li>)}</ul>
          <button type="button" style={styles.smallButton}
                  onClick={() => navigator.clipboard?.writeText(c.sentences.join('\n')).catch(() => undefined)}>Copy sentences</button>
        </div>
      ))}
      {exportBar('comparisons')}
      {result?.descriptives.map((g, i) => <GridTable key={i} grid={g} />)}
      {(result?.descriptives.length ?? 0) > 0 && exportBar('descriptives')}
    </div>
  )

  if (mode === 'guided') {
    return (
      <div style={{ ...plotThemeVars(theme), ...styles.root }}>
        <div style={styles.guidedMain}>
          <GuidedStudio
            spec={spec}
            onSpec={setSpec}
            guide={guidePayload}
            busy={busy}
            header={(
              <div>
                <div style={styles.guidedHeader}>
                  <span style={styles.title}>{title}{busy ? ' · working…' : ''}</span>
                  <span style={{ flex: 1 }} />
                  {modeSwitch}
                </div>
                {notices}
              </div>
            )}
            measureEditor={measuresEditor}
            results={resultsView}
          />
        </div>
        {savedRail}
        {codeOverlay}
      </div>
    )
  }

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
          <select value={a.family} onChange={e => patch({ family: e.target.value as Analysis['family'] })} style={styles.select}>
            {(catalog?.families ?? ['auto', 'parametric', 'nonparametric']).map(f => <option key={f} value={f}>{catalog?.family_labels?.[f] ?? f}</option>)}
          </select>
          {analyses(spec).length > 1 && (
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
              <button type="button" style={styles.smallButton} onClick={() => setFormulaFor(a.id ?? null)}
                      title="Covariates and custom random effects, as an lme4 formula">
                Model formula…
              </button>
              {custom && <span style={styles.chip}>{model?.errors.length ? 'formula has problems' : 'custom'}</span>}
            </div>
          )
        })()}
        <div style={styles.field}>Missing data
          <select value={a.missing ?? 'use_all'} onChange={e => patch({ missing: e.target.value as Analysis['missing'] })} style={styles.select}
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
          <select value={a.family_scope ?? 'stratum'} onChange={e => patch({ family_scope: e.target.value as Analysis['family_scope'] })} style={styles.select}>
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
          <select value={a.difference_spread ?? 'ci'} onChange={e => patch({ difference_spread: e.target.value as Analysis['difference_spread'] })} style={styles.select}>
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

  // StatStudio's own group, where Plot Studio has Chart: the output's first
  // choice (what is measured), between Data and Structure.
  const measuresGroup = (
    <Group
      title="Measures"
      open={openGroups.measures}
      onToggle={() => toggleGroup('measures')}
      summary={targets(spec).map(d => d.variable).join(', ')}
    >
      <Section title="Measures" hint="Each measure is analysed on its own, with the same design. A table with several columns can add each column as its own measure.">
        {measuresEditor}
      </Section>
    </Group>
  )

  return (
    <div style={{ ...plotThemeVars(theme), ...styles.root }}>
      <aside style={styles.rail}>
        <div style={styles.title} title={title}>{title}{busy ? ' · computing…' : ''}</div>
        <div style={styles.muted}>{savedInfo ? `saved as "${savedInfo.name}" v${savedInfo.version}` : ''}</div>
        {modeSwitch}

        {/* Data and Structure: Plot Studio's own groups on this table's
            design (one set of design controls for both studios). */}
        {designVariable && (
          <PlotStudio
            variable={designVariable}
            csvPath={csvPath}
            onClose={() => undefined}
            design={{ spec: spec.design as unknown as Record<string, unknown>, onChange: onDesign, roleLabels, between: measuresGroup, onDescribe }}
          />
        )}

        <Group
          title="Statistics"
          open={openGroups.statistics}
          onToggle={() => toggleGroup('statistics')}
          summary={`${spec.descriptives?.cell ?? ''}; ${analyses(spec).length} analysis(es)`}
        >
          <Section title="Descriptives">
            <div style={styles.field}>Cell
              <select value={spec.descriptives?.cell} onChange={e => setSpec(setCell(spec, e.target.value as DescriptiveCell))} style={styles.select}>
                {(catalog?.cells ?? []).map(c => <option key={c.id} value={c.id}>{c.header}</option>)}
              </select>
            </div>
            <div style={styles.statGrid}>
              {(catalog?.statistics ?? []).map(s => (
                <label key={s.id} style={styles.check}>
                  <input type="checkbox" checked={(spec.descriptives?.statistics ?? []).includes(s.id)}
                         onChange={() => setSpec(toggleStatistic(spec, s.id, (catalog?.statistics ?? []).map(x => x.id)))} /> {s.label}
                </label>
              ))}
            </div>
          </Section>

          <Section title="What the tests see" hint="The unit each test counts once, and whether each compared factor is measured on the same units (within) or different ones (between). Worked out from the data; override one if your study says otherwise.">
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
                              onChange={e => setSpec(setOverride(spec, firstAnalysis.id, f.name, (e.target.value || null) as FactorKind | null))}>
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
            <div style={styles.field}>Variants
              <select value={spec.variant_mode} onChange={e => setSpec({ ...spec, variant_mode: e.target.value as StatsSpec['variant_mode'] })} style={styles.select}
                      title="Sweep: every variant analysed separately, with a specification curve on the Comparisons tab">
                {(catalog?.variant_modes ?? ['pinned', 'sweep']).map(m => <option key={m} value={m}>{m === 'sweep' ? 'Sweep every variant' : 'Pinned (the design\'s selection)'}</option>)}
              </select>
            </div>
          </Section>

          <Section title="Comparisons">
            {analyses(spec).map(analysisEditor)}
            <button type="button" style={styles.smallButton} onClick={() => setSpec(addAnalysis(spec))}>+ Analysis</button>
          </Section>
        </Group>

        <Group
          title="Format"
          open={openGroups.format}
          onToggle={() => toggleGroup('format')}
          summary={`${spec.formatting?.decimals ?? 'auto'} decimals${spec.overlay?.show ? '; brackets on' : ''}`}
        >
          <Section title="Numbers">
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
                <select value={spec.overlay?.pairs ?? 'significant'} onChange={e => setSpec(setOverlay(spec, { pairs: e.target.value as Overlay['pairs'] }))} style={styles.select}>
                  {(catalog?.bar_pairs ?? []).map(p => <option key={p} value={p}>{p}</option>)}
                </select>
              </div>
              <div style={styles.field}>Label
                <select value={spec.overlay?.label ?? 'stars'} onChange={e => setSpec(setOverlay(spec, { label: e.target.value as Overlay['label'] }))} style={styles.select}>
                  {(catalog?.bar_labels ?? []).map(p => <option key={p} value={p}>{p === 'p' ? 'adjusted p' : 'stars'}</option>)}
                </select>
              </div>
              <div style={styles.muted}>Save this table, then the figure draws them (Plot Studio reads its linked table).</div>
            </Section>
          )}
        </Group>
      </aside>

      <main style={styles.main}>
        {/* The same toolbar as the figure's: the view, then everything that
            takes the result out of the panel, in the same order. */}
        <div style={styles.toolbar}>
          <button type="button" style={tab === 'descriptives' ? styles.tabOn : styles.tab} onClick={() => setTab('descriptives')}>Descriptives</button>
          <button type="button" style={tab === 'comparisons' ? styles.tabOn : styles.tab} onClick={() => setTab('comparisons')}
                  title={actions.length ? 'A decision is needed before comparisons can run' : undefined}>
            Comparisons{actions.length > 0 && <span style={styles.badge}>!</span>}
          </button>
          <span style={{ flex: 1 }} />
          <label style={styles.field}>Format
            <select value={exportFormat} onChange={e => setExportFormat(e.target.value)} style={styles.select} title="html pastes into Word as a table">
              {FORMATS.map(f => <option key={f} value={f}>{f}</option>)}
            </select>
          </label>
          {isVSCodeMode && <button type="button" style={styles.smallButton} onClick={() => exportTab(true)}>Save to file…</button>}
          <button type="button" style={styles.smallButton} onClick={() => exportTab(false)}>Copy</button>
          <button type="button" style={styles.smallButton} onClick={exportCode}
                  disabled={!!result?.not_exportable} title={result?.not_exportable ?? 'The stat_ function that reproduces this table'}>
            Export code
          </button>
          {!csvPath && (
            <button type="button" style={styles.primaryButton} onClick={addToPipeline}
                    disabled={!!result?.not_exportable} title={result?.not_exportable ?? 'Write a stat_ endpoint into the project'}>
              Add to pipeline
            </button>
          )}
        </div>

        {notices}
        {!spec.linked_plot && !csvPath && <div style={styles.muted}>Not linked to a saved plot.</div>}

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
          analyses(spec).length === 0
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
      {savedRail}
      {codeOverlay}
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
  guidedMain: { flex: 1, minWidth: 0, height: '100%', overflow: 'auto' },
  guidedHeader: { display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap', marginBottom: 6 },
  modeSwitch: { display: 'flex', gap: 4, margin: '6px 0' },
  rail: { width: 330, minWidth: 260, overflowY: 'auto', borderRight: '1px solid var(--ps-border)', padding: 10, background: 'var(--ps-surface)' },
  main: { flex: 1, overflow: 'auto', padding: 14 },
  title: { fontSize: 16, fontWeight: 600, marginBottom: 4, color: 'var(--ps-text-strong)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
  muted: { color: 'var(--ps-text-muted)', fontSize: 12, margin: '4px 0' },
  row: { display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' },
  field: { display: 'flex', alignItems: 'center', gap: 6, justifyContent: 'space-between' },
  check: { display: 'flex', alignItems: 'center', gap: 4, fontSize: 12 },
  select: { background: 'var(--ps-field)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '2px 4px', fontSize: 12, maxWidth: 190 },
  number: { width: 60, background: 'var(--ps-field)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 3 },
  smallButton: { background: 'var(--ps-control)', color: 'var(--ps-text-control)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '2px 8px', cursor: 'pointer', fontSize: 12 },
  primaryButton: { background: 'var(--ps-accent)', color: 'var(--ps-accent-fg)', border: '1px solid var(--ps-accent-border)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer', fontSize: 12 },
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
  toolbar: { display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', borderBottom: '1px solid var(--ps-border)', paddingBottom: 6, marginBottom: 10 },
  tab: { background: 'none', border: '1px solid var(--ps-border-faint)', color: 'var(--ps-text-secondary)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer' },
  tabOn: { background: 'var(--ps-accent-bg-strong)', border: '1px solid var(--ps-accent-border)', color: 'var(--ps-text-strong)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer' },
  message: { background: 'var(--ps-note-bg)', border: '1px solid var(--ps-note-border)', color: 'var(--ps-note-text)', borderRadius: 4, padding: 6, whiteSpace: 'pre-wrap', fontSize: 12 },
  error: { color: 'var(--ps-error)', margin: '6px 0' },
  warn: { color: 'var(--ps-warn)', fontSize: 12 },
  gridTitle: { fontStyle: 'italic', marginBottom: 6, color: 'var(--ps-text-strong)' },
  notes: { fontSize: 12, color: 'var(--ps-text-secondary)', margin: '4px 0', paddingLeft: 18 },
  codeOverlay: { position: 'fixed', inset: 0, background: 'var(--ps-scrim)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 },
  code: { background: 'var(--ps-surface)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 6, padding: 14, maxWidth: '85vw', maxHeight: '80vh', overflow: 'auto', fontSize: 12, whiteSpace: 'pre' },
}
