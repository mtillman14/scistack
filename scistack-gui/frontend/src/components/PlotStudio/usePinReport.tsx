// PlotStudio state, part 4 of 8: what the canvas has to report about the pin.
import { useMemo } from 'react'
import { GridMeta, FigurePayload } from './plotStudioTypes'
import { type MarkWeightsMeta } from './markWeights'
import { type PanelOverrideMeta, overrideCount } from './panelOverrides'
import { type DifferenceMeta, highlightShapes } from './differenceBars'
import { type CompareMeta } from './compare'
import { type TemporalMeta } from './timeZone'
import { type BinsMeta, type PanelFitsMeta } from './continuousKinds'
import { type RugMeta } from './showSample'
import { PLOT_THEMES } from './plotTheme'
import type { StudioCoreState } from './useStudioCore'
import type { StepByStepState } from './useStepByStep'
import type { CombinesState } from './useCombines'

/** PlotStudio: what the canvas has to report about the pin. Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function usePinReport(s: StudioCoreState & StepByStepState & CombinesState) {
  const {
    describe,
    spec,
    capabilities,
    theme,
    figures,
    picking,
    hoverTick,
    pickedFigure,
    variantRows,
  } = s

  // --- what the canvas has to report about the pin ------------------------
  // Both of these come off `capabilities.variants.sets`, measured by the same
  // mask the renderer applied, so the figure and the message about it cannot
  // disagree.
  // The index is carried, not looked up later: `variant_summary` reports one
  // entry per `spec.variant_sets` IN ORDER, so position is the only reliable
  // identity a row has — two rows can share a name, and a name can change while
  // a debounced capability report is still in flight.
  const definedSets = useMemo(
    () =>
      (capabilities?.variants?.sets ?? [])
        .map((set, index) => ({ set, index }))
        .filter(({ set }) => set.defined),
    [capabilities]
  )

  // Rows whose data was built by more than one version of some function. This
  // is now reported for the ordinary per-location "latest" state too (the
  // 2026-09-11 reversal), which is only tolerable because the message names
  // WHICH locations hold WHICH version — see SpanInfo.
  const spanningSets = useMemo(
    () => definedSets.filter(({ set }) => Object.keys(set.spans ?? {}).length > 0),
    [definedSets]
  )

  // The figure is empty AND the variants are why. Not "some row is empty": with
  // two rows and one of them matching, the figure is drawn and the per-row "no
  // data" tag is the right weight. Replacing a real figure with an explanation
  // would be worse than the tag.
  const emptyPin = useMemo(
    () =>
      definedSets.length > 0 &&
      definedSets.every(({ set }) => set.row_count === 0)
        ? definedSets
        : [],
    [definedSets]
  )

  const stackable = describe?.stackable_with ?? []
  // The section is reachable whenever there is anything for it to say: this
  // source has pipeline variants, the spec already holds rows, OR another
  // variable could be plotted alongside this one.
  //
  // That last clause was missing, and it made the whole multi-variable feature
  // unreachable: a project that never edited a function and swept no parameters
  // has no variant factors and no default row, so the section stayed hidden and
  // there was no way to add the second series.
  const hasVariants = Boolean(
    (capabilities?.variants?.factors ?? []).length > 0 ||
      variantRows.length > 0 ||
      stackable.length > 0
  )
  const summarizing = spec?.kind === 'bar' || spec?.kind === 'hbar' || spec?.kind === 'band'
  const faceted = Object.values(spec?.roles ?? {}).includes('facet')

  // The grid the backend actually built. Read back rather than recomputed: the
  // "set one, the other follows" arithmetic is grid_shape_for's, in
  // scistackplot, and a second copy of it here would be the thing that drifts.
  const gridMeta = (figures[0]?.figure?.layout?.meta ?? {}) as GridMeta
  // Which weight knob applies and what it resolves to (scistackplot.weights).
  const markWeights = (figures[0]?.figure?.layout?.meta as { mark_weights?: MarkWeightsMeta } | undefined)
    ?.mark_weights
  const effRows = Math.max(1, gridMeta.rows ?? 1)
  const effCols = Math.max(1, gridMeta.cols ?? 1)
  // One panel (nothing separates panels, or one level between the factors
  // that do): the backend lays out 1 x 1 whatever is pinned
  // (reduce.plan_layout), and the boxes say so instead of showing stale pins.
  const singlePanel = gridMeta.panels !== undefined && gridMeta.panels <= 1
  const layoutNotes = gridMeta.layout_notes ?? []
  // Every faceted panel, its drawn title and range, and its matched override
  // (render.base.panel_override_meta) — the Panels section lists these.
  const panelMeta = (figures[0]?.figure?.layout?.meta as
    | { panel_overrides?: PanelOverrideMeta }
    | undefined)?.panel_overrides
  const panelOverrideCount = overrideCount(spec?.panel_overrides)
  // What Statistics > Difference bars lists and what a click on the preview
  // picks from (scistackplot.diffbars.difference_meta).
  const diffMeta =
    (figures[0]?.figure?.layout?.meta as { difference_bars?: DifferenceMeta | null } | undefined)
      ?.difference_bars ?? null
  // What Structure > Compare says about the drawn figure: paired or not, and
  // what was dropped (scistackplot.compare.comparison_meta).
  const compareMeta =
    (figures[0]?.figure?.layout?.meta as { comparison?: CompareMeta | null } | undefined)
      ?.comparison ?? null
  // What Data > Time zone says about the drawn figure: its datetime axis and
  // notes (scistackplot.temporal.TemporalAxis); null for a number axis.
  const temporalMeta =
    (figures[0]?.figure?.layout?.meta as { temporal?: TemporalMeta | null } | undefined)
      ?.temporal ?? null
  // What Chart > Histogram / Regression say about the drawn figure: its bins
  // (bins.BinEdges) and fits (fits.PanelFit), never computed here.
  const binsMeta =
    (figures[0]?.figure?.layout?.meta as { bins?: BinsMeta | null } | undefined)?.bins ?? null
  // A histogram / KDE rug's shading scale (bins.rug_tables), never computed here.
  const rugMeta =
    (figures[0]?.figure?.layout?.meta as { rug?: RugMeta | null } | undefined)?.rug ?? null
  const fitsMeta =
    (figures[0]?.figure?.layout?.meta as { fits?: PanelFitsMeta[] | null } | undefined)?.fits ??
    null
  const figureDiffMeta = (figure: FigurePayload) =>
    (figure.figure?.layout?.meta as { difference_bars?: DifferenceMeta | null } | undefined)
      ?.difference_bars ?? null
  /** A figure's displayed layout, with the picking highlight added while
   *  "+ Add difference bar" is active: view-only, never in the figure. */
  const withPickingBands = (figure: FigurePayload, layout: Record<string, unknown>) => {
    if (picking.phase === 'idle') return layout
    const hover = hoverTick && hoverTick.figure === figure.index ? hoverTick : null
    const picked =
      picking.phase === 'second' && pickedFigure === figure.index
        ? { panelIndex: picking.panelIndex, slot: picking.first.slot }
        : null
    const bands = highlightShapes(figureDiffMeta(figure), hover, picked, PLOT_THEMES[theme].accent)
    return bands.length
      ? { ...layout, shapes: [...((layout.shapes as unknown[] | undefined) ?? []), ...bands] }
      : layout
  }

  // Only factors that separate PANELS can separate y limits — a colour or
  // replicate factor lives inside one panel, so splitting on it would ask one
  // axis for two ranges. Offering only these is how the control cannot be put
  // into a state the backend has to refuse (it drops them with a warning).
  //
  // Read back off the figure, not derived from `spec.roles`: the backend
  // completes roles the spec never states (a schema key promoted to ITERATE
  // because a nested key iterates, a `Variable` facet by default), and a
  // panel factor with no checkbox was one the user could never scale apart.
  // `spec.roles` is only the fallback before the first figure arrives.
  const yScopeFactors = gridMeta.panel_factors ?? Object.entries(spec?.roles ?? {})
    .filter(([, role]) => role === 'iterate' || role === 'facet')
    .map(([name]) => name)
  const yScope = (spec?.y_axis?.scope ?? []).filter(n => yScopeFactors.includes(n))
  // The limits the backend actually applied, read back off the figure so the
  // number on screen and the number in the box cannot disagree.
  // On a log axis plotly's `range` is in log10 units (render.base.axis_range);
  // the box shows data units, as the backend computed them.
  const plotlyYRange = (
    figures[0]?.figure?.layout?.yaxis as { range?: [number, number]; type?: string } | undefined
  )
  const appliedYLimits: [number, number] | undefined =
    plotlyYRange?.range && plotlyYRange.type === 'log'
      ? [10 ** plotlyYRange.range[0], 10 ** plotlyYRange.range[1]]
      : plotlyYRange?.range

  return {
    definedSets,
    spanningSets,
    emptyPin,
    stackable,
    hasVariants,
    summarizing,
    faceted,
    gridMeta,
    markWeights,
    effRows,
    effCols,
    singlePanel,
    layoutNotes,
    panelMeta,
    panelOverrideCount,
    diffMeta,
    compareMeta,
    temporalMeta,
    binsMeta,
    rugMeta,
    fitsMeta,
    figureDiffMeta,
    withPickingBands,
    yScopeFactors,
    yScope,
    plotlyYRange,
    appliedYLimits,
  }
}

export type PinReportState = ReturnType<typeof usePinReport>
