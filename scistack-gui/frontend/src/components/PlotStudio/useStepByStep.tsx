// PlotStudio state, part 2 of 8: the Step by step view (studio-unification Stage 5).
import { useState, useMemo, useEffect, useCallback, useRef } from 'react'
import { callBackend } from '../../api'
import { type GuidePayload, clampToBlocked } from '../StatStudio/Guided/guideSteps'
import { asGuideSpec, roleChanges } from '../Studio/plotGuide'
import { type StatsSpec } from '../StatStudio/statsSpec'
import { Role, KindInfo, Spec, applyPickedLocation, Filter } from './plotStudioTypes'
import { applyTile } from './kindTiles'
import { type Statistic, type Aggregation, type CategoricalOptions } from '../../generated/api-types'
import { type JoinChoice } from './showSample'
import { type HistogramOptions, type KdeOptions, type RegressionOptions } from './continuousKinds'
import { type FacetOptions, type YAxis, type StylePatch, type Matcher } from './plotSpecReducer'
import { type PanelOverride } from './panelOverrides'
import { type Comparison } from './compare'
import { type DifferenceBar, type Picking, IDLE, type TickRef, type DifferenceMeta, panelForAxis, targetAt, pickStep, addBar } from './differenceBars'
import { type WeightKey, withWeight } from './markWeights'
import { withTextSize, resetTextSizes, type TextTable, withTextTable, resetTextTables, withTextTarget } from './textSizes'
import { type TitleTexts, type ProjectAliasEdit } from './LabelsSection'
import { type SpecAliases } from './aliasEdit'
import { type SpecColors } from './colorEdit'
import { type ProjectColorEdit } from './ColorsSection'
import { type LocationSelection } from './locationSelection'
import { type PathStep } from './SchemaLocationPicker'
import type { StudioCoreState } from './useStudioCore'

/** PlotStudio: the Step by step view (studio-unification Stage 5). Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function useStepByStep(s: StudioCoreState) {
  const {
    plotNode,
    designMode,
    sourceParams,
    undo,
    spec,
    setSpec,
    dispatchSpec,
    figureIndex,
    view,
    setPlotGuide,
    plotGuidePayload,
    setPlotGuidePayload,
    timer,
    capsSpecRef,
    specKey,
    inFlight,
    queued,
    launchResolve,
    setRole,
  } = s

  // --- step by step (studio-unification Stage 5) ---------------------------
  // The screens for THIS spec, from the backend (no test runs). A plot_
  // node's own figure and the design-only rail have no step-by-step view.
  const guided = view === 'guided' && !designMode && !plotNode
  const [guideError, setGuideError] = useState('')
  const guideSpecKey = useMemo(() => JSON.stringify(spec ?? null), [spec])
  useEffect(() => {
    if (!guided || !spec) return
    const timer = window.setTimeout(() => {
      const t0 = performance.now()
      callBackend('stats_plot_guide', { spec, ...sourceParams }).then(raw => {
        const r = raw as { ok: boolean; guide?: GuidePayload; error?: string }
        console.info('[Plot Studio] step by step', { ms: Math.round(performance.now() - t0), ok: r.ok })
        if (r.ok && r.guide) { setPlotGuidePayload(r.guide); setGuideError('') }
        else setGuideError(r.error ?? 'The step-by-step view is not available for this figure.')
      }).catch(err => setGuideError(String(err)))
    }, 350)
    return () => window.clearTimeout(timer)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [guided, guideSpecKey, sourceParams])
  // A fresh guide that blocks a step before the one shown pulls the view back.
  useEffect(() => {
    if (!plotGuidePayload) return
    setPlotGuide(prev => clampToBlocked(asGuideSpec({}, prev), plotGuidePayload.steps).guide ?? prev)
  }, [plotGuidePayload])
  // A wizard choice: its role edits go through setRole (so a new grouping
  // layer is placed by depth), its navigation into the view state.
  const onGuideSpec = useCallback((next: StatsSpec) => {
    if (next.guide) setPlotGuide(next.guide)
    for (const change of roleChanges(spec?.roles as Record<string, string> | undefined, next.design.roles)) {
      setRole(change.factor, change.role as Role)
    }
  }, [spec?.roles, setRole])

  /** Move a grouping layer one step inward (-1, towards the first entry) or
   *  outward (+1). */
  const moveGroupLayer = useCallback((factor: string, delta: number) => {
    setSpec(prev => {
      if (!prev) return prev
      const layers = [...(prev.groups ?? [])]
      const from = layers.indexOf(factor)
      const to = from + delta
      if (from < 0 || to < 0 || to >= layers.length) return prev
      layers.splice(to, 0, ...layers.splice(from, 1))
      return { ...prev, groups: layers }
    })
  }, [])

  /** Colour one grouping layer (null: none). Colour is a tag on a layer,
   *  not a role — it never splits data, it labels a split by legend. */
  const setColor = useCallback((factor: string | null) => {
    dispatchSpec({ type: 'setColor', factor })
  }, [])

  const setKind = useCallback((tile: KindInfo) => {
    // A kind change can also change what the measure IS: a scalar kind on a
    // 1-D variable collapses it, and the roles a 1-D variable opens with (every
    // key on "separate figures") leave no replicates, so box and violin would
    // stay greyed out and this click would appear to do nothing. The backend
    // decides whether to re-default and what to (`roles.roles_for_kind`); this
    // applies the answer it already sent.
    //
    // Only when that answer describes the spec on screen. The report is a
    // debounced echo, and a suggestion computed against an older spec could
    // overwrite a role the user has set since — the one thing the rule promises
    // never to do.
    // A text measure's tile also carries its count / proportion stat
    // (`kindTiles.applyTile`, which owns the merge).
    const reportIsForThisSpec = capsSpecRef.current === specKey
    console.info('[Plot Studio] kind_set', tile.id ?? tile.kind, reportIsForThisSpec ? '(fresh report)' : '(stale report: roles kept)')
    setSpec(prev => (prev ? applyTile(prev, tile, reportIsForThisSpec) : prev))
  }, [specKey])

  /** Which of a plot_ function's inputs arrive as a table (`CustomPlot.as_table`):
   *  one checkbox per input, stored as the list, or `true` when all are on. */
  const setInputAsTable = useCallback((param: string, on: boolean, all: string[]) => {
    setSpec(prev => {
      if (!prev?.custom) return prev
      // The backend always writes as_table; absent would mean its default (all).
      const current = prev.custom.as_table ?? true
      const tables = new Set(
        current === true ? all : current === false ? [] : current.filter(p => all.includes(p))
      )
      if (on) tables.add(param)
      else tables.delete(param)
      const next = tables.size === all.length ? true : tables.size === 0 ? false : all.filter(p => tables.has(p))
      return { ...prev, custom: { ...prev.custom, as_table: next } }
    })
  }, [])

  const setCellStatistic = useCallback((statistic: Statistic) => {
    dispatchSpec({ type: 'setCellStatistic', statistic })
  }, [])

  const setAggregate = useCallback((patch: Aggregation) => {
    dispatchSpec({ type: 'setAggregate', patch })
  }, [])

  const setShowSample = useCallback((name: string, on: boolean) => {
    dispatchSpec({ type: 'setShowSample', name, on })
  }, [])

  const setJoinSample = useCallback((choice: JoinChoice) => {
    dispatchSpec({ type: 'setJoinSample', choice })
  }, [])

  /** Colour the overlay by one of its shown keys, independently of the
   *  marks' colour; Python says whether the choice is active right now. */
  const setSampleColor = useCallback((choice: string) => {
    dispatchSpec({ type: 'setSampleColor', choice })
  }, [])

  /** Unticked, the legend reads as though no key were shown. */
  const setSampleInLegend = useCallback((on: boolean) => {
    dispatchSpec({ type: 'setSampleInLegend', on })
  }, [])

  /** Unticked, the x axis holds only this figure's own levels. */
  const setShowMissingLevels = useCallback((on: boolean) => {
    dispatchSpec({ type: 'setShowMissingLevels', on })
  }, [])

  /** Data > Time zone: "local" or a zone to convert every aware value to. */
  const setTimeZone = useCallback((mode: string) => {
    dispatchSpec({ type: 'setTimeZone', mode })
  }, [])

  /** Data > X variable. With one, only scatter / line / regression draw, so
   *  the kind follows (continuousKinds.kindForXMeasure). */
  const setXMeasure = useCallback((name: string | null) => {
    console.info('[Plot Studio] x_measure_set', { x_measure: name })
    dispatchSpec({ type: 'setXMeasure', name })
  }, [])

  /** Chart > Categories: merge into `spec.categorical` (a text measure). */
  const setCategorical = useCallback((patch: CategoricalOptions) => {
    console.info('[Plot Studio] categorical_set', patch)
    dispatchSpec({ type: 'setCategorical', patch })
  }, [])

  /** Chart > Bars: stack the colour levels, or not. */
  /** The "Not plotted" footnote (StyleOptions.not_plotted_note); absent = off. */
  const setNotPlottedNote = useCallback((on: boolean) => {
    console.info('[Plot Studio] not_plotted_note_set', on)
    dispatchSpec({ type: 'setNotPlottedNote', on })
  }, [])

  const setBarStack = useCallback((stack: boolean) => {
    console.info('[Plot Studio] bar_stack_set', stack)
    dispatchSpec({ type: 'setBarStack', stack })
  }, [])

  /** Chart > Histogram: merge into `spec.histogram`. */
  const setHistogram = useCallback((patch: HistogramOptions) => {
    console.info('[Plot Studio] histogram_set', patch)
    dispatchSpec({ type: 'setHistogram', patch })
  }, [])

  /** Chart > Density curve: merge into `spec.kde`. */
  const setKde = useCallback((patch: KdeOptions) => {
    console.info('[Plot Studio] kde_set', patch)
    dispatchSpec({ type: 'setKde', patch })
  }, [])

  /** Chart > Regression: merge into `spec.regression`. */
  const setRegression = useCallback((patch: RegressionOptions) => {
    console.info('[Plot Studio] regression_set', patch)
    dispatchSpec({ type: 'setRegression', patch })
  }, [])

  const setFacet = useCallback((patch: Partial<FacetOptions>) => {
    dispatchSpec({ type: 'setFacet', patch })
  }, [])

  const setYAxis = useCallback((patch: Partial<YAxis>) => {
    dispatchSpec({ type: 'setYAxis', patch })
  }, [])

  const setStyle = useCallback((patch: StylePatch) => {
    dispatchSpec({ type: 'setStyle', patch })
  }, [])

  /** Replace `spec.panel_overrides` (built by panelOverrides.ts). */
  const setPanelOverrides = useCallback((next: PanelOverride[]) => {
    console.info('[Plot Studio] panel_overrides_set', { count: next.length, overrides: next })
    dispatchSpec({ type: 'setPanelOverrides', overrides: next })
  }, [])

  /** Replace `spec.comparison` (built by compare.ts). */
  const setComparison = useCallback((next: Comparison | null | undefined) => {
    console.info('[Plot Studio] comparison_set', { comparison: next })
    dispatchSpec({ type: 'setComparison', comparison: next })
  }, [])

  /** Replace `spec.difference_bars` (built by differenceBars.ts). */
  const setDifferenceBars = useCallback((next: DifferenceBar[]) => {
    console.info('[Plot Studio] difference_bars_set', { count: next.length, bars: next })
    dispatchSpec({ type: 'setDifferenceBars', bars: next })
  }, [])

  // "+ Add difference bar": two clicks on the preview. `picking` is the step,
  // `hoverTick` the tick under the pointer (figure index + panel + slot) for
  // the highlight band, `diffPanel` the panel the section lists.
  const [picking, setPicking] = useState<Picking>(IDLE)
  const [hoverTick, setHoverTick] = useState<(TickRef & { figure: number }) | null>(null)
  const [diffPanel, setDiffPanel] = useState(0)
  const [pickedFigure, setPickedFigure] = useState<number | null>(null)
  const pickingRef = useRef<Picking>(IDLE)
  pickingRef.current = picking

  const cancelPicking = useCallback(() => {
    console.info('[Plot Studio] difference_bar_picking_cancelled')
    setPicking(IDLE)
    setHoverTick(null)
  }, [])

  useEffect(() => {
    if (picking.phase === 'idle') return
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') cancelPicking()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [picking.phase, cancelPicking])

  /** The tick a plotly click/hover event points at, in that figure's meta. */
  const tickFromEvent = useCallback(
    (meta: DifferenceMeta | null | undefined, event: unknown) => {
      const point = (event as { points?: { x?: unknown; xaxis?: { _id?: string } }[] })?.points?.[0]
      if (!point) return null
      const panel = panelForAxis(meta, point.xaxis?._id)
      const target = targetAt(panel, point.x as number | string | undefined)
      return panel && target ? { panel, target } : null
    },
    [],
  )

  const onFigureClick = useCallback(
    (figureIndex: number, meta: DifferenceMeta | null | undefined, event: unknown) => {
      if (pickingRef.current.phase === 'idle') return
      const hit = tickFromEvent(meta, event)
      const step = pickStep(pickingRef.current, hit?.panel, hit?.target)
      console.info('[Plot Studio] difference_bar_pick', {
        phase: pickingRef.current.phase,
        panel: hit?.panel.index ?? null,
        tick: hit?.target.label ?? null,
        completed: Boolean(step.add),
      })
      if (hit) {
        setDiffPanel(hit.panel.index)
        setPickedFigure(figureIndex)
      }
      if (step.add) {
        const add = step.add
        setSpec(prev =>
          prev ? { ...prev, difference_bars: addBar(prev.difference_bars, add.match, add.a, add.b) } : prev,
        )
        console.info('[Plot Studio] difference_bar_added', add)
        setHoverTick(null)
      }
      setPicking(step.state)
    },
    [tickFromEvent],
  )

  const onFigureHover = useCallback(
    (figureIndex: number, meta: DifferenceMeta | null | undefined, event: unknown) => {
      if (pickingRef.current.phase === 'idle') return
      const hit = tickFromEvent(meta, event)
      setHoverTick(prev => {
        const next = hit ? { figure: figureIndex, panelIndex: hit.panel.index, slot: hit.target.slot } : null
        return prev && next && prev.figure === next.figure && prev.panelIndex === next.panelIndex && prev.slot === next.slot
          ? prev
          : next
      })
    },
    [tickFromEvent],
  )

  /** Set a mark weight (`style.sample_weight` / `line_weight`); 1 or
   *  cleared deletes the key (markWeights.withWeight). */
  const setWeight = useCallback((key: WeightKey, value: number | null) => {
    setSpec(prev => (prev ? { ...prev, style: withWeight(prev.style, key, value) } : prev))
  }, [])

  /** Set one `style.text` size, or clear it (null) back to derived. */
  const setTextSize = useCallback((key: string, value: number | null) => {
    setSpec(prev => {
      if (!prev) return prev
      const style = prev.style ?? {}
      return { ...prev, style: { ...style, text: withTextSize(style.text, key, value) } }
    })
  }, [])

  /** Clear every per-element size; `base` and `target` stay. */
  const resetText = useCallback(() => {
    setSpec(prev => {
      if (!prev) return prev
      const style = prev.style ?? {}
      return { ...prev, style: { ...style, text: resetTextSizes(style.text) } }
    })
  }, [])

  /** Set one element's ratio, floor or ceiling (`style.text.ratios` / `floors` /
   *  `ceilings`), or clear it (null) back to the default. */
  const setTextTable = useCallback((table: TextTable, element: string, value: number | null) => {
    setSpec(prev => {
      if (!prev) return prev
      const style = prev.style ?? {}
      return { ...prev, style: { ...style, text: withTextTable(style.text, table, element, value) } }
    })
  }, [])

  /** Every ratio, floor and ceiling back to the defaults; sizes stay. */
  const resetProportions = useCallback(() => {
    setSpec(prev => {
      if (!prev) return prev
      const style = prev.style ?? {}
      return { ...prev, style: { ...style, text: resetTextTables(style.text) } }
    })
  }, [])

  /** Where the figure is going (`style.text.target`): picks the auto band. */
  const setTextTarget = useCallback((target: string) => {
    setSpec(prev => {
      if (!prev) return prev
      const style = prev.style ?? {}
      return { ...prev, style: { ...style, text: withTextTarget(style.text, target) } }
    })
  }, [])

  /** `style.title` / `x_label` / `y_label`: blank DELETES the key (the saved
   *  spec drops nulls, so a stored null would read as "modified"). */
  const setTitleText = useCallback((key: keyof TitleTexts, value: string | null) => {
    setSpec(prev => {
      if (!prev) return prev
      const style: Record<string, unknown> = { ...(prev.style ?? {}) }
      if (value === null) delete style[key]
      else style[key] = value
      return { ...prev, style: style as Spec['style'] }
    })
  }, [])

  const setAliases = useCallback((next: SpecAliases) => {
    dispatchSpec({ type: 'setAliases', aliases: next })
  }, [])

  const setColors = useCallback((next: SpecColors) => {
    dispatchSpec({ type: 'setColors', colors: next })
  }, [])

  /** The plot's one mark colour (`style.mark_color`); null deletes the key,
   *  so a reopened saved plot does not read as modified. */
  const setMarkColor = useCallback((value: string | null) => {
    setSpec(prev => {
      if (!prev) return prev
      const style: Record<string, unknown> = { ...(prev.style ?? {}) }
      if (value === null) delete style.mark_color
      else style.mark_color = value
      return { ...prev, style: style as Spec['style'] }
    })
  }, [])

  /** Resolve the SAME spec again: after a project alias is written the spec
   *  has not changed, so nothing else would redraw the figure. Queues behind
   *  a running resolve like any other. */
  const refreshResolve = useCallback(() => {
    if (!spec) return
    if (inFlight.current) {
      queued.current = { spec, figureIndex, specKey }
      return
    }
    launchResolve(spec, figureIndex, specKey)
  }, [spec, figureIndex, specKey, launchResolve])

  // A server undo/redo from this panel (an alias or colour) changed the
  // config the figure was drawn with, and no broadcast reaches a plot tab:
  // draw it again. Through a ref so the stack keeps one stable callback.
  const refreshResolveRef = useRef(refreshResolve)
  refreshResolveRef.current = refreshResolve
  useEffect(() => {
    undo.onServerApplied = () => refreshResolveRef.current()
    return () => { undo.onServerApplied = null }
  }, [undo])

  /** One edit to the project's [aliases]; resolves to an error or null. */
  const writeProjectAlias = useCallback(
    (edit: ProjectAliasEdit): Promise<string | null> =>
      callBackend('plot_project_alias_set', { ...edit }, { stack: undo }).then(reply => {
        const result = reply as { ok: boolean; error?: string }
        if (!result.ok) return result.error ?? 'The project alias was not written.'
        refreshResolve()
        return null
      }),
    [refreshResolve, undo]
  )

  /** One edit to the project's [colors]; resolves to an error or null. */
  const writeProjectColor = useCallback(
    (edit: ProjectColorEdit): Promise<string | null> =>
      callBackend('plot_project_color_set', { ...edit }, { stack: undo }).then(reply => {
        const result = reply as { ok: boolean; error?: string }
        if (!result.ok) return result.error ?? 'The project colour was not written.'
        refreshResolve()
        return null
      }),
    [refreshResolve, undo]
  )

  /** Add or remove one factor from the y-limit scope, keeping panel order. */
  const toggleYScope = useCallback((name: string, on: boolean) => {
    setSpec(prev => {
      if (!prev) return prev
      const current = prev.y_axis?.scope ?? []
      const next = on
        ? [...current.filter(f => f !== name), name]
        : current.filter(f => f !== name)
      return { ...prev, y_axis: { ...(prev.y_axis ?? {}), scope: next } }
    })
  }, [])

  /**
   * Edit the rule in one grid slot, padding the array out to reach it.
   *
   * The slots ARE the grid — there is no add/remove, because the number of rows
   * and columns is the thing the user set. A slot left blank claims nothing and
   * takes whatever is left over, in order (scistackplot's Matcher.is_blank).
   */
  const setRuleAt = useCallback(
    (axis: 'rows' | 'cols', index: number, patch: Partial<Matcher>) => {
      setSpec(prev => {
        if (!prev) return prev
        const rules = [...(prev.facet?.[axis] ?? [])]
        while (rules.length <= index) rules.push({ op: 'contains', value: '' })
        rules[index] = { ...rules[index], ...patch }
        return { ...prev, facet: { ...(prev.facet ?? {}), [axis]: rules } }
      })
    },
    []
  )

  /**
   * Keep only `levels` of `column`, or drop the filter when `levels` is null.
   *
   * "Every level selected" is stored as NO filter rather than as a list of all
   * of them: a list would freeze today's levels into the spec, and the figure
   * would silently stop showing subject 13 the day it arrives.
   */
  const setLevelFilter = useCallback(
    (column: string, levels: (string | number)[] | null) => {
      setSpec(prev => {
        if (!prev) return prev
        const others = (prev.filters ?? []).filter(f => f.column !== column)
        const filters = levels === null ? others : [...others, { column, include: levels }]
        return { ...prev, filters }
      })
    },
    []
  )

  /**
   * Which schema locations to draw — the location picker's checkbox edits.
   *
   * `include: []` is inert (everything drawn), so an untouched picker and a
   * "select all" click store the same thing. `keys` travels with it as display
   * order; matching reads the keys named inside each prefix.
   */
  const setLocationSelection = useCallback(
    (selection: LocationSelection, keys: string[]) => {
      setSpec(prev =>
        prev
          ? {
              ...prev,
              location_filter: {
                keys,
                include: selection.include,
                exclude_levels: selection.exclude_levels,
              },
            }
          : prev
      )
    },
    []
  )

  /**
   * A row was clicked: show that location and nothing else.
   *
   * Two things happen together, and the second is the one worth explaining.
   * Picking `subject=01` leaves `trial` unanswered, and a trial factor sitting
   * on AGGREGATE would collapse the very thing the user opened the picker to
   * look at — every trial's own values. So any schema key the selection does
   * NOT name is moved off AGGREGATE onto FREE (replicates), and one with no
   * role at all gets FREE too.
   *
   * Deliberately NOT a blanket reassignment: a key already on X, COLOR or FACET
   * is drawing its levels individually, which is what was asked for, and
   * stamping over a chosen x axis because a location was clicked would be
   * astonishing.
   */
  const pickLocation = useCallback(
    (path: PathStep[], keys: string[]) => {
      setSpec(prev => (prev ? applyPickedLocation(prev, path, keys) : prev))
    },
    []
  )

  /** Numeric bounds on a measure. Either end may be null (open). */
  const setRangeFilter = useCallback(
    (column: string, minimum: number | null, maximum: number | null) => {
      setSpec(prev => {
        if (!prev) return prev
        const others = (prev.filters ?? []).filter(f => f.column !== column)
        if (minimum === null && maximum === null) return { ...prev, filters: others }
        const filter: Filter = { column }
        if (minimum !== null) filter.minimum = minimum
        if (maximum !== null) filter.maximum = maximum
        return { ...prev, filters: [...others, filter] }
      })
    },
    []
  )

  return {
    guided,
    guideError,
    setGuideError,
    guideSpecKey,
    onGuideSpec,
    moveGroupLayer,
    setColor,
    setKind,
    setInputAsTable,
    setCellStatistic,
    setAggregate,
    setShowSample,
    setJoinSample,
    setSampleColor,
    setSampleInLegend,
    setShowMissingLevels,
    setTimeZone,
    setXMeasure,
    setCategorical,
    setNotPlottedNote,
    setBarStack,
    setHistogram,
    setKde,
    setRegression,
    setFacet,
    setYAxis,
    setStyle,
    setPanelOverrides,
    setComparison,
    setDifferenceBars,
    picking,
    setPicking,
    hoverTick,
    setHoverTick,
    diffPanel,
    setDiffPanel,
    pickedFigure,
    setPickedFigure,
    pickingRef,
    cancelPicking,
    tickFromEvent,
    onFigureClick,
    onFigureHover,
    setWeight,
    setTextSize,
    resetText,
    setTextTable,
    resetProportions,
    setTextTarget,
    setTitleText,
    setAliases,
    setColors,
    setMarkColor,
    refreshResolve,
    refreshResolveRef,
    writeProjectAlias,
    writeProjectColor,
    toggleYScope,
    setRuleAt,
    setLevelFilter,
    setLocationSelection,
    pickLocation,
    setRangeFilter,
  }
}

export type StepByStepState = ReturnType<typeof useStepByStep>
