// PlotStudio state, part 1 of 8: the spec, its loading and resolving, figure navigation and the plain spec edits.
import { type Props } from './PlotStudio'
import { useRef, useEffect, useMemo, useState, useCallback } from 'react'
import { reportStudioOpen } from '../Studio/studioOpen'
import { callBackend } from '../../api'
import { DescribeResponse, Spec, Capabilities, FigurePayload, applyPickedLocation, ResolveResponse, Role } from './plotStudioTypes'
import { UndoStack } from '../../undo'
import { useUndoShortcuts, useHistoryState } from '../../hooks/useUndo'
import { type PlotSpecAction, plotSpecReducer } from './plotSpecReducer'
import { usePlotTheme } from './usePlotTheme'
import { type NotPlottedTotal } from './notPlotted'
import { FigureStepTimer } from './figureStepTiming'
import { type PreviewMode, previewRequest, previewKey } from './preview'
import { DEFAULT_VIEW, type SavedPlotInfo, type RestoreNote, variantSetsKey, iterateSignature as iterateSignatureOf } from './savedPlots'
import { type GroupId, parseOpenGroups, OPEN_GROUPS_STORAGE_KEY, serializeOpenGroups, type RailWidth, railWidthClass } from './sidebarGroups'
import { type StudioView, PLOT_VIEW_STORAGE_KEY, initialView, NEW_GUIDE } from '../Studio/plotGuide'
import { type GuideState } from '../StatStudio/statsSpec'
import { type GuidePayload } from '../StatStudio/Guided/guideSteps'
import { type PresetInfo } from './presets'
import { injectedDbName } from '../../session'
import { unseenWarnings } from './dataWarnings'
import { adoptedFigureIndex } from './figureCursor'
import { placeGroupLayer } from './groups'

/** PlotStudio: the spec, its loading and resolving, figure navigation and the plain spec edits. Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function useStudioCore({
  variable: openedVariable,
  csvPath,
  embedded = false,
  initialLocation,
  plotNode,
  design,
  openedFrom,
  onClose,
}: Props) {

  const designMode = design !== undefined
  const designRef = useRef(design)
  designRef.current = design
  // Once per panel: which way in (Stage 0 diagnostics). A design-only rail is
  // part of a StatStudio, which reports its own opening.
  useEffect(() => {
    if (!designMode) reportStudioOpen(callBackend, 'plot', openedFrom, openedVariable || plotNode || csvPath)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])
  // Threaded into every call: the backend picks CsvSource or ScidbSource from
  // it, and nothing else about the panel changes (one DataSource protocol).
  const sourceParams = useMemo(
    () => (csvPath ? { csv_path: csvPath } : {}),
    [csvPath]
  )
  // StatStudio test brackets (StatsLink): joined per request by the backend
  // (`with_derived_bars`), never saved with the plot. Resolve, export, save
  // and add-to-pipeline all send them, so the file draws what the preview drew.
  const [derivedBars, setDerivedBars] = useState<Record<string, unknown>[]>([])
  const resolveParams = useMemo(
    () => (derivedBars.length ? { ...sourceParams, derived_bars: derivedBars } : sourceParams),
    [sourceParams, derivedBars]
  )
  // The measure on screen: the one the panel was opened on, or the other half
  // of a wide variable with text fields (`Gait` <-> `Gait (text)`, owned by
  // `scistackplotdb.text_fields`; the catalog says which, `text_of`).
  const [variable, setVariable] = useState(openedVariable)
  useEffect(() => { setVariable(openedVariable) }, [openedVariable])
  const [describe, setDescribe] = useState<DescribeResponse | null>(null)
  // This panel's own undo stack (docs/claude/undo-redo.md): spec edits are
  // local steps; its project alias/colour writes and "Add to pipeline" are
  // server steps on the same stack. One per instance, like one per editor.
  const undo = useMemo(() => new UndoStack((method, params) => callBackend(method, params), 'plot'), [])
  useUndoShortcuts(undo)
  // setSpec = a user edit (an undo step); replaceSpec = loading (not one).
  const [spec, setSpec, replaceSpec] = useHistoryState<Spec | null>(undo, null, 'figure edit')
  // A plain field edit is an action of the spec reducer (plotSpecReducer.ts),
  // one undo step like any setSpec.
  const dispatchSpec = useCallback(
    (action: PlotSpecAction) => setSpec(prev => plotSpecReducer(prev, action)),
    [setSpec]
  )
  const [capabilities, setCapabilities] = useState<Capabilities | null>(null)

  // Factors of the resolved table (Variant included, selected-away columns
  // gone). `describe.table.factors` is the pre-selection view and would show
  // both a `Code:` column and the `Variant` factor that consumed it.
  //
  // Declared up here, beside the state it reads, because the role callbacks
  // below need `factorDepths` — a callback referring forward to a `const`
  // further down the body reads fine and is a trip hazard nobody should have
  // to check.
  const factors = capabilities?.factors ?? describe?.table?.factors ?? []

  // Depth per factor, from the backend (`FactorInfo.depth`). Nothing here works
  // out what "outer" means — it reads the number scistackplot publishes, so the
  // control and the figure nest a new layer the same way. See `groups.ts`.
  const factorDepths = useMemo(() => {
    const depths: Record<string, number | null | undefined> = {}
    for (const factor of factors) depths[factor.name] = factor.depth
    return depths
  }, [factors])

  // Policy, though, comes from the backend: whether the x axis can be grouped
  // at all for this measure's shape, why not when it cannot, and what role a
  // grouping ticked right now would take.
  const grouping = capabilities?.grouping

  // Light or dark. The shell paints the chrome from it; the figure only needs
  // its text colour, which plotly must be handed as a value.
  const theme = usePlotTheme()

  const [figures, setFigures] = useState<FigurePayload[]>([])
  // Which figure of the ITERATE fan-out is on screen. The fan-out runs in
  // schema order (outermost key most significant), so stepping past subject
  // 1's last trial lands on subject 2's first — the backend orders it, this is
  // just a cursor into that list.
  const [figureIndex, setFigureIndex] = useState(0)
  // "Not plotted" (scistackplot.omissions): the button's total and the popup.
  const [notPlotted, setNotPlotted] = useState<NotPlottedTotal | null>(null)
  const [notPlottedOpen, setNotPlottedOpen] = useState(false)
  // The cursor as of the latest render, for a resolve answer to compare its
  // request against (`adoptedFigureIndex`) — its closure holds the old one.
  const figureIndexRef = useRef(figureIndex)
  figureIndexRef.current = figureIndex
  // Times one ◀ ▶ step, click to drawn, for scistack.log (figureStepTiming.ts).
  const stepTimer = useRef(new FigureStepTimer())
  const [figureLabels, setFigureLabels] = useState<string[]>([])
  const figureLabelsRef = useRef(figureLabels)
  figureLabelsRef.current = figureLabels
  const [fanoutNotes, setFanoutNotes] = useState<string[]>([])
  const [specError, setSpecError] = useState('')
  const [loadError, setLoadError] = useState('')
  const [busy, setBusy] = useState(false)
  const [code, setCode] = useState<string | null>(null)
  const [notice, setNotice] = useState('')
  // Reclaiming space happens at two levels: hiding the controls rail inside
  // the panel, and asking VS Code to enlarge the tab the webview lives in.
  const [controlsHidden, setControlsHidden] = useState(false)
  // Index of the variant row whose DAG popup is open, or null.
  const [variantEditor, setVariantEditor] = useState<number | null>(null)
  // "+ Add variant" is a two-step pick, not an immediate append — see
  // addVariantFromPicker for why the row is created on apply instead.
  const [addingVariant, setAddingVariant] = useState(false)
  // The schema location picker — the whole of the "Schema keys" section.
  const [locationPickerOpen, setLocationPickerOpen] = useState(false)
  const panelRef = useRef<HTMLDivElement>(null)
  const [canvasHeight, setCanvasHeight] = useState(0)
  const [canvasWidth, setCanvasWidth] = useState(0)
  // Preview at the EXPORT size (the label and legend decisions Save will make,
  // drawn at width x height) or fitted to the PANE (decided at the pane's size;
  // the figure toolbar then says what export size reproduces it). A
  // view setting, not part of the spec.
  const [previewMode, setPreviewMode] = useState<PreviewMode>(DEFAULT_VIEW.previewMode)
  const observerRef = useRef<ResizeObserver | null>(null)

  // The controls rail's groups (sidebarGroups.ts): which are open, per viewer.
  // Browser storage may be missing or throw (private window, preview), so
  // every read and write is guarded and the defaults always render.
  const [openGroups, setOpenGroups] = useState<Record<GroupId, boolean>>(() => {
    try {
      return parseOpenGroups(window.localStorage.getItem(OPEN_GROUPS_STORAGE_KEY))
    } catch {
      return parseOpenGroups(null)
    }
  })
  useEffect(() => {
    try {
      window.localStorage.setItem(OPEN_GROUPS_STORAGE_KEY, serializeOpenGroups(openGroups))
    } catch {
      // Not remembered; the rail still works.
    }
  }, [openGroups])
  const toggleGroup = useCallback(
    (id: GroupId) => setOpenGroups(prev => ({ ...prev, [id]: !prev[id] })),
    []
  )
  // The rail's width class. Measured, never assumed: the rail is 280 px until
  // it is dragged (Shell's ResizeHandle), and rows such as the text sizes lay out by
  // it. A callback ref for the same reason as canvasRef below.
  const [railWidth, setRailWidth] = useState<RailWidth>('narrow')
  const railObserverRef = useRef<ResizeObserver | null>(null)
  const controlsRef = useCallback((node: HTMLDivElement | null) => {
    railObserverRef.current?.disconnect()
    if (!node || typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(entries => {
      const width = entries[0].contentRect.width
      // 0 is the collapsed rail; keep the last real class.
      if (width > 0) setRailWidth(railWidthClass(width))
    })
    observer.observe(node)
    railObserverRef.current = observer
  }, [])
  useEffect(() => () => railObserverRef.current?.disconnect(), [])

  // Saved plots (.claude/plan-saved-plots.md). The backend owns storage,
  // names and restoring; `savedPlots.ts` owns the view settings and the
  // "modified" comparison. A CSV has no database, so none of this applies.
  const [savedPlots, setSavedPlots] = useState<SavedPlotInfo[]>([])
  const [savedRailCollapsed, setSavedRailCollapsed] = useState(false)
  // "Newer code than this saved plot was saved with" (see switchSavedToLatest).
  const [newerPrompt, setNewerPrompt] = useState<
    { plotId: string; name: string; text: string } | null
  >(null)
  const [loadedPlot, setLoadedPlot] = useState<SavedPlotInfo | null>(null)
  // The step-by-step view (studio-unification Stage 5): the screens a figure
  // shares with a StatStudio table, from the backend (stats_plot_guide), in
  // StatStudio's own GuidedStudio. View state only: nothing in the spec.
  const [view, setView] = useState<StudioView>(() => {
    let stored: string | null = null
    try { stored = window.localStorage.getItem(PLOT_VIEW_STORAGE_KEY) } catch { /* the default */ }
    return designMode ? 'advanced' : initialView(stored, { plotNode: !!plotNode })
  })
  const [plotGuide, setPlotGuide] = useState<GuideState>(NEW_GUIDE)
  const [plotGuidePayload, setPlotGuidePayload] = useState<GuidePayload | null>(null)
  const switchView = useCallback((next: StudioView) => {
    setView(next)
    try { window.localStorage.setItem(PLOT_VIEW_STORAGE_KEY, next) } catch { /* not remembered */ }
    callBackend('stats_guide_log', { event: `view ${next}`, step: null, detail: 'figure' }).catch(() => undefined)
  }, [])
  // StatStudio "Sync to PlotStudio": its design copied onto this spec (StatsLink).
  const applyLinkedSpec = useCallback(
    (next: Spec) => setSpec(next),
    [setSpec]
  )
  const [restoreNotes, setRestoreNotes] = useState<RestoreNote[]>([])
  const [savedBusy, setSavedBusy] = useState(false)
  const [savedError, setSavedError] = useState('')
  // Plot presets (.claude/plan-plot-presets.md): settings without the data,
  // project-wide. The backend decides what a preset keeps and how it fits
  // this variable; the panel only applies the spec it is handed.
  const [plotPresets, setPlotPresets] = useState<PresetInfo[]>([])
  const [appliedPreset, setAppliedPreset] = useState<PresetInfo | null>(null)
  const [presetNotes, setPresetNotes] = useState<RestoreNote[]>([])
  const [presetBusy, setPresetBusy] = useState(false)
  const [presetError, setPresetError] = useState('')
  // `modifiedKey` of the settings as last opened or saved. Null = always
  // modified (a plot restored with notes is not in the current format yet).
  const [baseline, setBaseline] = useState<string | null>(null)
  // Set when new settings are applied wholesale (the panel opening, a saved
  // plot loading, a save). The next render's key becomes the baseline, so
  // the comparison sees the state React actually applied.
  const captureBaseline = useRef(false)

  // A CSV has no variable type; name the file instead.
  const title = describe?.plot_node?.function ?? describe?.variable ?? variable ?? (csvPath ? csvPath.split('/').pop() ?? 'CSV' : '')

  // Which database this figure is drawn from. A CSV tab is already named by
  // its file, and the browser build has one database per process, so both
  // leave this undefined rather than badge something that says nothing.
  const sourceName = csvPath ? undefined : injectedDbName() || undefined

  // Plotly needs a definite pixel height, so measure the canvas rather than
  // hardcoding one: a maximized panel should give the figure the extra space.
  //
  // A callback ref, NOT a mount effect: on mount this component renders the
  // "Loading…" branch, which has no canvas, so an effect keyed on [] observed
  // nothing and left the height at 0 — the figure then sat at its 320px floor
  // in a full-height pane.
  const canvasRef = useCallback((node: HTMLDivElement | null) => {
    observerRef.current?.disconnect()
    if (!node || typeof ResizeObserver === 'undefined') return
    setCanvasHeight(node.getBoundingClientRect().height)
    setCanvasWidth(node.getBoundingClientRect().width)
    const observer = new ResizeObserver(entries => {
      setCanvasHeight(entries[0].contentRect.height)
      setCanvasWidth(entries[0].contentRect.width)
    })
    observer.observe(node)
    observerRef.current = observer
  }, [])

  useEffect(() => () => observerRef.current?.disconnect(), [])

  // --- open ---------------------------------------------------------------
  useEffect(() => {
    let cancelled = false
    setDescribe(null)
    // Opening is loading, not editing: no undo step, and the old figure's
    // steps do not apply to the one being opened.
    replaceSpec(null)
    undo.clear()
    setFigures([])
    setNotPlotted(null)
    setLoadError('')
    setLoadedPlot(null)
    setRestoreNotes([])
    setSavedError('')
    ;(plotNode
      ? callBackend('plot_describe_function', { node_id: plotNode })
      : callBackend('plot_describe', { variable: variable || undefined, ...sourceParams }))
      .then(raw => {
        if (cancelled) return
        const response = raw as DescribeResponse
        setDescribe(response)
        // Design-only: the caller owns the spec; describe only supplies the
        // factors, catalog and offers the controls read.
        if (designRef.current) {
          replaceSpec(designRef.current.spec as unknown as Spec)
          designRef.current.onDescribe?.(response)
          if (response.capabilities) setCapabilities(response.capabilities)
          return
        }
        // A location handed over from the canvas is applied to the OPENING
        // spec, not as a later edit: seeding it here means the first resolve
        // already draws that location, rather than drawing everything once and
        // then narrowing — which reads as a flicker and costs a full resolve of
        // the whole dataset on the way past.
        if (response.spec) {
          const keys = (response.table?.factors ?? [])
            .filter(f => f.is_schema_key)
            .map(f => f.name)
          replaceSpec(
            initialLocation?.length
              ? applyPickedLocation(response.spec, initialLocation, keys)
              : response.spec
          )
          captureBaseline.current = true
        }
        if (response.capabilities) setCapabilities(response.capabilities)
      })
      .catch(err => !cancelled && setLoadError((err as Error).message))
    return () => { cancelled = true }
    // `initialLocation` is deliberately NOT a dependency: it is what the
    // panel OPENED on, and re-running describe because it changed would
    // throw away every edit made since. It never does change — every plot
    // opens its own tab, so a second location from the canvas arrives as a
    // new webview rather than as a new prop.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [variable, plotNode, sourceParams])

  // A plot's named pins over variant space are STATEMENTS about the plotted
  // variable (`variant_selection` in the intent store), so they outlive the
  // panel. Persisted on every change after the opening spec has landed —
  // the opening spec already carries whatever was stored, so re-saving it
  // is a no-op, and a CSV source has no database to state anything in.
  const variantSetsJson = variantSetsKey(spec?.variant_sets)
  const openedVariantSets = useRef<string | null>(null)
  // Kept keyed on `variable` for the browser build, where this component
  // is a modal that really can be reopened on another variable in place. In
  // the extension each plot is its own tab, so this fires once.
  useEffect(() => { openedVariantSets.current = null }, [variable])
  useEffect(() => {
    if (!spec || !variable || csvPath) return
    if (openedVariantSets.current === null) {
      openedVariantSets.current = variantSetsJson
      return
    }
    if (variantSetsJson === openedVariantSets.current) return
    openedVariantSets.current = variantSetsJson
    callBackend('plot_variant_sets_save', {
      variable,
      variant_sets: spec.variant_sets ?? [],
    }).catch(err => console.warn('[PlotStudio] variant pins not saved:', err))
  }, [variantSetsJson, spec, variable, csvPath])

  // Which factors fan the figure set out. Changing THAT is what invalidates a
  // cursor into the fan-out; changing a colour or a plot kind does not, and
  // resetting to figure 1 on every spec edit would make the panel unusable
  // while browsing. A fan-out that merely got shorter is handled by the
  // backend's clamp instead.
  const iterateSignature = useMemo(() => iterateSignatureOf(spec?.roles), [spec?.roles])
  // A saved plot restores its own figure index along with its roles. The
  // signature it arrives with is recorded here, and the ONE reset that
  // signature causes is skipped, so the restored index survives. Any later
  // change of the fan-out resets as usual.
  const restoredSignature = useRef<string | null>(null)
  useEffect(() => {
    if (restoredSignature.current === iterateSignature) {
      restoredSignature.current = null
      return
    }
    restoredSignature.current = null
    setFigureIndex(0)
  }, [iterateSignature])

  // --- resolve on every spec change (debounced) ---------------------------
  const timer = useRef<number | null>(null)
  // The spec the capability report was last fetched for. Stepping the
  // navigator re-resolves (that is the point — only the shown figure is
  // serialized) but cannot change which plot kinds are legal, so it must not
  // cost a second RPC per arrow press.
  const capsSpecRef = useRef<string | null>(null)
  const specKey = useMemo(() => JSON.stringify(spec ?? null), [spec])
  // Design-only: the caller's spec comes in, edits go out. Compared as JSON
  // so the round trip (edit, caller, prop) settles instead of looping.
  const designKey = useMemo(() => (design ? JSON.stringify(design.spec) : null), [design])
  useEffect(() => {
    if (designKey === null || !describe) return
    if (designKey !== specKey) replaceSpec(designRef.current!.spec as unknown as Spec)
    // Only when the CALLER's spec changes.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [designKey, describe])
  useEffect(() => {
    if (designKey === null || !spec) return
    if (specKey !== designKey) designRef.current?.onChange(spec as unknown as Record<string, unknown>)
    // Only on this rail's own edits.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [specKey])
  // Monotonic id of the newest request. A reply whose id is not the current one
  // is stale and is DROPPED — see below for why that matters so much here.
  const generation = useRef(0)
  // Data warnings already shown as a popup in this panel (dataWarnings.ts).
  const shownWarnings = useRef(new Set<string>())
  // --- single-flight ------------------------------------------------------
  // At most ONE resolve is in flight, and at most one waiting behind it.
  //
  // The generation counter below drops stale REPLIES, which was never the whole
  // problem: the backend still ran every superseded request to completion. The
  // 180 ms debounce only coalesces keystrokes, so a drag across role dropdowns
  // fired a resolve every 180 ms while each took 4-16 s, and the server spawns a
  // thread per request and never cancels (server.py). Measured 2026-09-10 and
  // again 2026-09-11: four to six full resolves in flight at once, each
  // re-exploding millions of rows and slowing the others down, with resolve
  // times climbing 3.3s -> 7.6s -> 15.2s -> 27.7s as they piled up — until one
  // crossed the webview's fixed 30 s timeout (since removed — see webviewRpc.ts)
  // and surfaced as "Request plot_resolve timed out". Nothing had hung; the
  // panel was competing with its own abandoned work.
  //
  // Holding one back turns that drag into two resolves total: the one already
  // running, and the state the user actually stopped on. Intermediate specs are
  // never sent at all, which is the only way to not pay for them — the
  // transport has no cancel, so anything already dispatched runs to completion.
  // The preview size every resolve is decided at (see preview.ts). A ref,
  // read at send time, so launchResolve keeps its identity; `sentPreview`
  // is what the figures on screen were decided at.
  const preview = useMemo(
    () => previewRequest(previewMode, canvasWidth - 8, canvasHeight - 16),
    [previewMode, canvasWidth, canvasHeight]
  )
  const previewRef = useRef(preview)
  previewRef.current = preview
  const sentPreview = useRef<string | null>(null)
  const inFlight = useRef(false)
  const queued = useRef<{ spec: Spec; figureIndex: number; specKey: string } | null>(null)

  // Explicitly typed because it calls itself (to drain `queued`), and an
  // inferred self-referential const is a TS error rather than a cycle.
  const launchResolve: (spec: Spec, index: number, key: string) => void = useCallback(
    (nextSpec: Spec, nextIndex: number, nextKey: string) => {
      inFlight.current = true
      const mine = ++generation.current
      sentPreview.current = previewKey(previewRef.current)
      setBusy(true)
      const needCapabilities = capsSpecRef.current !== nextKey
      stepTimer.current.sent(nextIndex, performance.now())
      Promise.all([
        callBackend('plot_resolve', {
          spec: nextSpec,
          figure_index: nextIndex,
          preview: previewRef.current,
          ...resolveParams,
        }),
        needCapabilities
          ? callBackend('plot_capabilities', { spec: nextSpec, ...sourceParams }).catch(
              () => null
            )
          : Promise.resolve(null),
      ])
        .then(([resolved, caps]) => {
          if (mine !== generation.current) return
          const result = resolved as ResolveResponse
          if (result.ok) stepTimer.current.answered(nextIndex, performance.now())
          else stepTimer.current.reset()
          setSpecError(result.ok ? '' : (result.error ?? 'Could not resolve this plot.'))
          setFigures(result.figures ?? [])
          setFigureLabels(result.figure_labels ?? [])
          setNotPlotted(result.ok ? (result.not_plotted ?? null) : null)
          setFanoutNotes(result.notes ?? [])
          for (const message of unseenWarnings(shownWarnings.current, result.data_warnings)) {
            // Host-answered: only the extension can raise a VS Code popup.
            callBackend('show_warning', { message }).catch(() => console.warn('[Plot Studio]', message))
          }
          // Adopt the index the backend actually rendered — it clamps a stale
          // cursor against a fan-out that shrank — but ONLY if the user is
          // still on the figure this was sent for. Adopting an answer they had
          // clicked past dragged the cursor back and looped between two
          // figures forever (figureCursor.ts).
          const adopted = adoptedFigureIndex(
            nextIndex, figureIndexRef.current, result.figure_index
          )
          if (adopted !== null) setFigureIndex(adopted)
          else if (typeof result.figure_index === 'number') {
            console.info('[Plot Studio] figure_answer_superseded', {
              requested: nextIndex,
              rendered: result.figure_index,
              cursor: figureIndexRef.current,
            })
          }
          if (caps) {
            setCapabilities(caps as Capabilities)
            capsSpecRef.current = nextKey
          }
        })
        .catch(err => {
          if (mine !== generation.current) return
          stepTimer.current.reset()
          setSpecError((err as Error).message)
        })
        .finally(() => {
          inFlight.current = false
          const next = queued.current
          queued.current = null
          if (next) {
            // Someone changed the spec while this was running. Send the LAST
            // such state — everything before it is already superseded and
            // sending it would be exactly the pile-up this exists to stop.
            launchResolve(next.spec, next.figureIndex, next.specKey)
            return
          }
          // Only the newest request may say the panel is idle. A superseded one
          // finishing late would otherwise clear the spinner while the resolve
          // the user is actually waiting for is still running.
          if (mine === generation.current) setBusy(false)
        })
    },
    [resolveParams]
  )

  useEffect(() => {
    if (!spec) return
    if (designMode) {
      // No figure to draw: only the capability report the controls read.
      const capsTimer = window.setTimeout(() => {
        if (capsSpecRef.current === specKey) return
        callBackend('plot_capabilities', { spec, ...sourceParams })
          .then(caps => { setCapabilities(caps as Capabilities); capsSpecRef.current = specKey })
          .catch(err => console.warn('[Plot Studio] design rail: capabilities failed', err))
      }, 250)
      return () => window.clearTimeout(capsTimer)
    }
    if (timer.current) window.clearTimeout(timer.current)
    timer.current = window.setTimeout(() => {
      if (inFlight.current) {
        // Replaces any earlier waiter rather than queueing behind it.
        queued.current = { spec, figureIndex, specKey }
        // The panel is still working on the user's behalf, so it must still say
        // so — the running request may be for a spec they have already moved on
        // from, and clearing `busy` when it lands would be a lie.
        setBusy(true)
        return
      }
      launchResolve(spec, figureIndex, specKey)
    }, 180)
    return () => { if (timer.current) window.clearTimeout(timer.current) }
  }, [spec, specKey, figureIndex, sourceParams, launchResolve])

  // A resize or a preview-mode toggle changes only the SIZE the labels and
  // legend are decided at. Re-render the last resolve (`reuse_resolved`)
  // instead of resolving again — resolving is the 4-16 s part, re-rendering
  // is one matplotlib layout.
  const rerenderTimer = useRef<number | null>(null)
  useEffect(() => {
    if (!spec || sentPreview.current === null) return
    if (previewKey(preview) === sentPreview.current) return
    if (rerenderTimer.current) window.clearTimeout(rerenderTimer.current)
    rerenderTimer.current = window.setTimeout(() => {
      if (inFlight.current) {
        // The running resolve was sent at the old size; the queued one reads
        // the new size when it goes.
        queued.current = { spec, figureIndex, specKey }
        return
      }
      sentPreview.current = previewKey(previewRef.current)
      const mine = ++generation.current
      callBackend('plot_resolve', {
        spec,
        figure_index: figureIndex,
        preview: previewRef.current,
        reuse_resolved: true,
        ...resolveParams,
      })
        .then(resolved => {
          if (mine !== generation.current) return
          const result = resolved as ResolveResponse
          if (result.ok) setFigures(result.figures ?? [])
        })
        .catch(err => console.warn('[plot] preview re-render failed', err))
    }, 300)
    return () => { if (rerenderTimer.current) window.clearTimeout(rerenderTimer.current) }
  }, [preview, spec, specKey, figureIndex, resolveParams])

  // --- figure navigation --------------------------------------------------
  const figureCount = figureLabels.length
  // From the ref, not a state updater: the click is timed here, and StrictMode
  // runs an updater twice. A click or key event re-renders before the next
  // one is dispatched, so the ref is current for every press.
  const stepFigure = useCallback(
    (delta: number) => {
      const current = figureIndexRef.current
      const next = Math.max(0, Math.min(current + delta, Math.max(0, figureCount - 1)))
      if (next === current) return
      stepTimer.current.stepped(next, performance.now())
      setFigureIndex(next)
    },
    [figureCount]
  )

  // Plotly finished drawing `index` (onInitialized / onUpdate). The frame
  // after is when it is on screen; a finished ◀ ▶ step goes to scistack.log.
  const onFigureDrawn = useCallback((index: number) => {
    requestAnimationFrame(() => {
      const step = stepTimer.current.drawn(index, performance.now())
      if (step === null) return
      console.info('[Plot Studio] figure_step', step)
      callBackend('report_client_timing', {
        where: 'Plot Studio figure_step',
        label: figureLabelsRef.current[step.figure_index] ?? '',
        ...step,
      }).catch(() => undefined)
    })
  }, [])

  useEffect(() => {
    if (figureCount < 2) return
    const onKey = (event: KeyboardEvent) => {
      // Never steal the arrows from a field the user is typing in.
      const target = event.target as HTMLElement | null
      const tag = target?.tagName
      if (tag === 'INPUT' || tag === 'SELECT' || tag === 'TEXTAREA') return
      if (event.key === 'ArrowRight') stepFigure(1)
      else if (event.key === 'ArrowLeft') stepFigure(-1)
      else return
      event.preventDefault()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [figureCount, stepFigure])

  // --- spec edits ---------------------------------------------------------
  const setRole = useCallback((factor: string, role: Role) => {
    setSpec(prev => {
      if (!prev) return prev
      const roles = { ...prev.roles, [factor]: role }
      // Keep the grouping order in step with membership: a factor newly
      // grouped is placed by the data's own nesting, one leaving drops out —
      // and takes the colour with it, since colour names a grouping layer.
      //
      // PLACED, not appended. And because this writes `groups` on every
      // toggle, the backend's own depth rule (`ordered_groups`) never sees an
      // unplaced holder to place: this is the copy that decides what the user
      // gets, which is why it lives in a tested module (`groups.ts`) rather
      // than inline here.
      const current = prev.groups ?? []
      const groups =
        role === 'group'
          ? placeGroupLayer(current, factor, factorDepths)
          : current.filter(name => name !== factor)
      const color = role !== 'group' && prev.color === factor ? null : prev.color
      return { ...prev, roles, groups, color }
    })
  }, [factorDepths])

  return {
    openedVariable,
    csvPath,
    embedded,
    initialLocation,
    plotNode,
    design,
    openedFrom,
    onClose,
    designMode,
    designRef,
    sourceParams,
    derivedBars,
    setDerivedBars,
    resolveParams,
    variable,
    setVariable,
    describe,
    setDescribe,
    undo,
    spec,
    setSpec,
    replaceSpec,
    dispatchSpec,
    capabilities,
    setCapabilities,
    factors,
    factorDepths,
    grouping,
    theme,
    figures,
    setFigures,
    figureIndex,
    setFigureIndex,
    notPlotted,
    setNotPlotted,
    notPlottedOpen,
    setNotPlottedOpen,
    figureIndexRef,
    stepTimer,
    figureLabels,
    setFigureLabels,
    figureLabelsRef,
    fanoutNotes,
    setFanoutNotes,
    specError,
    setSpecError,
    loadError,
    setLoadError,
    busy,
    setBusy,
    code,
    setCode,
    notice,
    setNotice,
    controlsHidden,
    setControlsHidden,
    variantEditor,
    setVariantEditor,
    addingVariant,
    setAddingVariant,
    locationPickerOpen,
    setLocationPickerOpen,
    panelRef,
    canvasHeight,
    setCanvasHeight,
    canvasWidth,
    setCanvasWidth,
    previewMode,
    setPreviewMode,
    observerRef,
    openGroups,
    setOpenGroups,
    toggleGroup,
    railWidth,
    setRailWidth,
    railObserverRef,
    controlsRef,
    savedPlots,
    setSavedPlots,
    savedRailCollapsed,
    setSavedRailCollapsed,
    newerPrompt,
    setNewerPrompt,
    loadedPlot,
    setLoadedPlot,
    view,
    setView,
    plotGuide,
    setPlotGuide,
    plotGuidePayload,
    setPlotGuidePayload,
    switchView,
    applyLinkedSpec,
    restoreNotes,
    setRestoreNotes,
    savedBusy,
    setSavedBusy,
    savedError,
    setSavedError,
    plotPresets,
    setPlotPresets,
    appliedPreset,
    setAppliedPreset,
    presetNotes,
    setPresetNotes,
    presetBusy,
    setPresetBusy,
    presetError,
    setPresetError,
    baseline,
    setBaseline,
    captureBaseline,
    title,
    sourceName,
    canvasRef,
    variantSetsJson,
    openedVariantSets,
    iterateSignature,
    restoredSignature,
    timer,
    capsSpecRef,
    specKey,
    designKey,
    generation,
    shownWarnings,
    preview,
    previewRef,
    sentPreview,
    inFlight,
    queued,
    launchResolve,
    rerenderTimer,
    figureCount,
    stepFigure,
    onFigureDrawn,
    setRole,
  }
}

export type StudioCoreState = ReturnType<typeof useStudioCore>
