/**
 * Plot Studio — interactive plotting for a scidb variable.
 *
 * The panel is deliberately thin. It renders whatever `plot_describe` and
 * `plot_capabilities` return and sends back a spec; every decision about which
 * plot kinds are legal, what the defaults are, and how data is reduced lives in
 * scistackplot (CLAUDE.md NOTE 3). Adding a plot kind or a role should require
 * no change here beyond a label.
 *
 * The controls replace the four mutually-dependent checkbox groups of the
 * original R/Shiny app with one rule: every factor carries exactly one role.
 * That is a single <select> per factor, and the invariant is enforced by the
 * backend rather than by widgets updating each other's options.
 *
 * See docs/claude/plotting-library-design.md.
 */

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import createPlotlyComponent from 'react-plotly.js/factory'
import Plotly from 'plotly.js-cartesian-dist-min'
import CustomFigure, { customPreviewBlob, customPreviewSize, type CustomPreviewPayload } from './CustomFigure'
import ResizeHandle, { useResizablePanel } from '../ResizeHandle'
import type {
  Aggregation,
  CategoricalOptions,
  CustomPlot,
  ErrorBand,
  FactorVariable as GeneratedFactorVariable,
  Filter as GeneratedFilter,
  PlotKind,
  PlotSpec,
  Role as PlotSpecRole,
  Statistic,
  VariantSet as GeneratedVariantSet,
} from '../../generated/api-types'
import { callBackend, isVSCodeMode } from '../../api'
import { UndoStack } from '../../undo'
import { useHistoryState, useUndoShortcuts } from '../../hooks/useUndo'
import UndoButtons from '../UndoButtons'
import { injectedDbName } from '../../session'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import VariantDagPopup from './VariantDagPopup'
import GroupingDagPopup from './GroupingDagPopup'
import SchemaLocationPicker, { type PathStep } from './SchemaLocationPicker'
import { reportStudioOpen } from '../Studio/studioOpen'
// The rail's Group and Section are shared with StatStudio (one look, one owner).
import { Group, Section } from '../Studio/RailGroup'
import { NEW_GUIDE, PLOT_VIEW_STORAGE_KEY, asGuideSpec, initialView, roleChanges, type StudioView } from '../Studio/plotGuide'
import GuidedStudio from '../StatStudio/Guided/GuidedStudio'
import { clampToBlocked, type GuidePayload } from '../StatStudio/Guided/guideSteps'
import type { GuideState, StatsSpec } from '../StatStudio/statsSpec'
import {
  type LocationSelection,
  asSelection,
  describeSelection,
  rolesAfterPick,
} from './locationSelection'
import { orderGroups, placeGroupLayer } from './groups'
import SavedPlotsRail, { type SaveResult } from './SavedPlotsRail'
import PresetsSection, { type PresetSaveResult } from './PresetsSection'
import type { AppliedPreset, PresetInfo } from './presets'
import {
  isModified,
  iterateSignature as iterateSignatureOf,
  DEFAULT_VIEW,
  modifiedKey,
  newerVersionsText,
  readView,
  variantSetsKey,
  viewState,
  type NewerVersion,
  type RestoreNote,
  type SavedPlotInfo,
} from './savedPlots'
import {
  isLocked,
  isTicked,
  joinChoice,
  sampleColorChoice,
  MARK_COLOR,
  type JoinChoice,
  type SampleOverlay,
  joinTooltip,
  rugScaleNote,
  type RugMeta,
} from './showSample'
import {
  type AspectPreset,
  CUSTOM_ASPECT,
  ASPECT_LOCK_STORAGE_KEY,
  FALLBACK_PRESETS,
  aspectName,
  heightFor,
  parseAspectLocked,
  resizeFigure,
} from './figureSize'
import {
  SIZE_UNITS,
  SIZE_UNIT_STORAGE_KEY,
  type SizeUnit,
  fromInches,
  parseSizeUnit,
  toInches,
  unitInfo,
} from './figureUnits'
import { roundTo, stepValue } from './stepper'
import { adoptedFigureIndex } from './figureCursor'
import { unseenWarnings } from './dataWarnings'
import { FigureStepTimer } from './figureStepTiming'
import {
  figureOutputSize,
  previewKey,
  previewRequest,
  type PreviewMeta,
  type PreviewMode,
} from './preview'
import {
  autoSummary,
  baseTitle,
  basePlaceholder,
  drawnLabel,
  fixedTickNote,
  formatPt,
  hasFixedSizes,
  hasTextTables,
  placeholderFor,
  proportionRows,
  resetTextSizes,
  resetTextTables,
  rowTitle,
  textSizeRows,
  textTarget,
  textTargets,
  withTextSize,
  withTextTable,
  withTextTarget,
  type ResolvedTextSizes,
  textSizeOf,
  type TextTable,
} from './textSizes'
import LabelsSection, { type ProjectAliasEdit, type TitleTexts } from './LabelsSection'
import ColorsSection, { type ProjectColorEdit } from './ColorsSection'
import PanelsSection from './PanelsSection'
import DifferenceBarsSection from './DifferenceBarsSection'
import CompareSection from './CompareSection'
import TimeZoneSection from './TimeZoneSection'
import { HistogramSection, KdeSection, RegressionSection, XMeasureSection } from './ContinuousSections'
import NotPlottedPopup from './NotPlottedPopup'
import { NOT_PLOTTED_DEPTH, type NotPlottedTotal, type OmissionReport } from './notPlotted'
import {
  type BinsMeta,
  type HistogramCapability,
  type HistogramOptions,
  type KdeCapability,
  type KdeOptions,
  type PanelFitsMeta,
  type RegressionCapability,
  type RegressionOptions,
} from './continuousKinds'
import {
  formatLimit,
  parseLimitText,
  type TemporalMeta,
  type TimeZoneCapability,
} from './timeZone'
import { compareSummary, writtenHeader, type CompareCapability, type CompareMeta, type Comparison } from './compare'
import {
  IDLE,
  addBar,
  barCount,
  highlightShapes,
  panelForAxis,
  pickStep,
  setMalformedReporter,
  targetAt,
  type DifferenceBar,
  type DifferenceMeta,
  type Picking,
  type TickRef,
} from './differenceBars'
import {
  offersPanels,
  overrideCount,
  type PanelOverride,
  type PanelOverrideMeta,
  type YTitles,
} from './panelOverrides'
import { type Labelable, type SpecAliases } from './aliasEdit'
import {
  plotSpecReducer,
  type FacetOptions,
  type MatchOp,
  type Matcher,
  type PlotSpecAction,
  type StylePatch,
  type YAxis,
} from './plotSpecReducer'
import { plotMarkColor, type Colorable, type SpecColors } from './colorEdit'
import { type MarkWeightsMeta, type WeightKey, weightTitle, withWeight } from './markWeights'
import { copyFigurePng } from './clipboardPng'
import {
  addCombine,
  assignLevels,
  combineSummary,
  levelRange,
  readBuckets,
  removeCombine,
  renameBucket,
  renameCombine,
  switchSlot,
  unusedLevels,
  writeBuckets,
  type LevelGroup,
} from './combine'
import { PLOT_THEMES, otherPlotTheme, plotThemeVars, screenFigure } from './plotTheme'
import { blockedClickNotice, toolButtonStyle } from './toolButton'
import { applyTile, isTileSelected, tileKey, type KindTile } from './kindTiles'
import { setPlotTheme, usePlotTheme } from './usePlotTheme'
import StatsLink from './StatsLink'
import {
  OPEN_GROUPS_STORAGE_KEY,
  appearanceSummary,
  chartSummary,
  dataSummary,
  parseOpenGroups,
  railWidthClass,
  serializeOpenGroups,
  statisticsSummary,
  structureSummary,
  textSizeColumns,
  type GroupId,
  type RailWidth,
} from './sidebarGroups'
import { SAVE_DPI, Capabilities, DescribeResponse, FigurePayload, Filter, GridMeta, KIND_LABELS, KindInfo, ResolveResponse, Role, Spec, VariantSet, applyPickedLocation, describeSpan, fallbackRoles, shapeBadge } from './plotStudioTypes'
import { BucketAdder, CombineEditor, FigureNavigator, FigureSizeInput, GridSizeInput, GroupingList, LevelPicker, LimitInput, RangeFilter, RuleSlots, Shell, SizeInput, SlotName, StepperInput, VariantReadout, VariantRows, WeightInput, groupingPlacement, sampleNote } from './PlotStudioControls'
import { styles } from './plotStudioStyles'
import { usePlotStudio } from './usePlotStudio'
import { DataGroup, ChartGroup, StructureGroup, StatisticsGroup, AppearanceGroup } from './PlotStudioRail'

// A difference bar or panel that arrives without one of its dicts goes to
// scistack.log (2026-10-17: one crashed the tab and the log could not say which).
setMalformedReporter((what, owner) => {
  console.warn('[Plot Studio] difference bars: missing', what, owner)
  let shape = ''
  try {
    shape = JSON.stringify(owner) ?? String(owner)
  } catch {
    shape = String(owner)
  }
  callBackend('report_client_error', {
    where: 'Plot Studio difference bars',
    message: `${what} is missing; drawn as {} instead`,
    stack: '',
    component_stack: shape.slice(0, 2000),
  }).catch(() => undefined)
})

const Plot = createPlotlyComponent(Plotly)

export interface Props {
  /** Variable type (scidb) or column name (CSV). Empty picks the default. */
  variable: string
  /** Set to plot a CSV file instead of the project database. */
  csvPath?: string
  /**
   * True when this IS the whole webview (its own VS Code tab) rather than a
   * modal over the DAG: no backdrop, no rounded card, no close button — the
   * tab's own chrome does that.
   */
  embedded?: boolean
  /**
   * One schema location to open on — the canvas picker's row click, arriving
   * through the extension. Applied once, to the spec `describe` returns, so
   * "click a location, see that location" holds from the canvas exactly as it
   * does from the picker inside this panel.
   */
  initialLocation?: PathStep[]
  /**
   * A plot_ node to open on instead of a variable: the user's own function
   * draws the figure, and the node's edges bind its inputs.
   */
  plotNode?: string
  /**
   * Design-only: just the Data and Structure groups, editing a spec the CALLER
   * owns (StatStudio's rail; docs/claude/studio-unification.md). No figure,
   * no toolbar, no saved-plots rail, and no resolve: only the capability
   * report the controls read. One set of design controls for both studios.
   */
  design?: DesignBinding
  /** Which way in opened this studio (Studio/studioOpen.ts), logged once. */
  openedFrom?: string
  onClose: () => void
}

export interface DesignBinding {
  /** The design carrier (a PlotSpec dict). The caller is its owner. */
  spec: Record<string, unknown>
  onChange: (spec: Record<string, unknown>) => void
  /** The caller's role words, keyed by role id, from the backend. */
  roleLabels?: Record<string, string>
  /** Rendered between Data and Structure, where Plot Studio has Chart. */
  between?: React.ReactNode
  /** The describe payload, once: the caller reads the catalog from it rather
   *  than asking for a second describe. */
  onDescribe?: (describe: { catalog?: { measures?: { name: string; columns?: string[] }[] } }) => void
}

export default function PlotStudio(props: Props) {
  // State, effects and handlers: usePlotStudio.tsx. Destructured under
  // their own names, so the render below reads exactly as before.
  const s = usePlotStudio(props)
  const {
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
    openCombine,
    setOpenCombine,
    addLevelGroup,
    editLevelGroup,
    renameLevelGroup,
    switchCombineSlot,
    removeLevelGroup,
    addVariantFromPicker,
    removeVariantSet,
    adoptCombination,
    editVariantSet,
    schemaKeys,
    otherFilterable,
    schemaKeyNames,
    locationSummary,
    pickerSelection,
    groupLayers,
    groupableVariables,
    groupPickerOpen,
    setGroupPickerOpen,
    applyGroupings,
    derivedNames,
    bucketable,
    listedFactors,
    sourceLevels,
    variantRows,
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
    presets,
    specWidth,
    specHeight,
    previewMeta,
    figWidth,
    figHeight,
    outputSpec,
    fontSize,
    textMeta,
    drawnFont,
    aspect,
    aspectLocked,
    setAspectLocked,
    commitSize,
    onAspect,
    onResize,
    onWidth,
    onHeight,
    sizeUnit,
    setSizeUnit,
    handleExport,
    saveJob,
    saving,
    setSaving,
    saveKind,
    dataChooser,
    setDataChooser,
    dataDepth,
    setDataDepth,
    dataWide,
    setDataWide,
    imageFormat,
    setImageFormat,
    allFormats,
    formatChoices,
    copyFigure,
    saveFigures,
    saveData,
    openDataSave,
    handleAddToPipeline,
    savedVariable,
    currentKey,
    modified,
    saveCurrent,
    applyOpened,
    openSaved,
    switchSavedToLatest,
    keepSavedVersion,
    renameSaved,
    removeSaved,
    rawShape,
    savePreset,
    applyPreset,
    renamePreset,
    removePreset,
    savedRail,
  } = s

  // --- render -------------------------------------------------------------
  if (designMode && (loadError || !describe || describe.eligible === false)) {
    return (
      <div style={styles.note}>
        {loadError ? `Could not read the design: ${loadError}` : !describe ? 'Loading…' : describe.reason}
      </div>
    )
  }
  if (loadError) {
    return (
      <Shell variable={title} shape={shapeBadge(capabilities)} source={sourceName} onClose={onClose} embedded={embedded}>
        <div style={styles.error}>Could not open the plot panel: {loadError}</div>
      </Shell>
    )
  }
  if (!describe) {
    return (
      <Shell variable={title} shape={shapeBadge(capabilities)} source={sourceName} onClose={onClose} embedded={embedded}>
        <div style={styles.note}>Loading…</div>
      </Shell>
    )
  }
  if (describe.eligible === false) {
    // The empty state the design doc insists on: say why, never draw blank axes.
    return (
      <Shell variable={title} shape={shapeBadge(capabilities)} source={sourceName} onClose={onClose} embedded={embedded}>
        <div style={styles.note}>{describe.reason}</div>
      </Shell>
    )
  }

  const gridRows = (figures[0]?.figure?.layout?.meta as { rows?: number } | undefined)?.rows ?? 1
  // The navigator strip and any fan-out note sit above the figure and take
  // their height out of it.
  const chrome = (figureCount > 1 ? 34 : 0) + fanoutNotes.length * 30
  const available = figures.length > 1
    ? Math.round((canvasHeight - chrome) / 2) - 28
    : canvasHeight - chrome - 16
  // Fill the pane, but never squeeze a tall facet grid: each row needs room for
  // its own tick labels and the next row's title.
  const figureHeight = Math.max(320, gridRows * 240, available)

  // The two views of one figure, as in StatStudio: Step by step and Advanced.
  const viewSwitch = !designMode && !plotNode && (
    <div style={styles.viewSwitch} role="group" aria-label="View">
      <button type="button" style={view === 'guided' ? styles.viewOn : styles.viewOff}
              onClick={() => switchView('guided')} title="One decision at a time, each explained">Step by step</button>
      <button type="button" style={view === 'advanced' ? styles.viewOn : styles.viewOff}
              onClick={() => switchView('advanced')} title="Every setting at once">Advanced</button>
    </div>
  )
  // Step by step: the screens a figure shares with a StatStudio table, in
  // StatStudio's own GuidedStudio, with the live figure beside them.
  const guidedRail = (
    <div ref={controlsRef} style={{ ...styles.controls, ...(controlsHidden ? styles.controlsHidden : null) }}>
      {viewSwitch}
      {guideError
        ? <div style={styles.note}>{guideError}</div>
        : spec && (
          <GuidedStudio
            spec={asGuideSpec(spec as unknown as Record<string, unknown>, plotGuide)}
            onSpec={onGuideSpec}
            guide={plotGuidePayload}
            busy={busy}
            header={null}
            surface="figure"
          />
        )}
    </div>
  )
  // The controls rail, as one value: the Shell mounts it beside the figure,
  // and design-only mode (StatStudio's rail, docs/claude/studio-unification.md)
  // mounts its Data and Structure groups on their own.
  const sidebarRail = (
        <div
          ref={controlsRef}
          style={{
            ...styles.controls,
            ...(controlsHidden ? styles.controlsHidden : null),
          }}
        >
          {viewSwitch}
          {/* Five groups, in the order a figure is built
              (docs/claude/plot-studio-controls.md). A collapsed group says
              what it holds in one line, so the whole rail fits on a screen.
              Hidden with display:none, not unmounted: an input mid-edit
              keeps its text across a collapse. */}
          <DataGroup s={s} />

          {/* Plot type is the first real decision: it decides what Grouping,
              Statistics and the mark weights offer, so it heads the rail's
              questions about the figure rather than sitting sixth. */}
          {/* Design-only: the caller's own group sits where Chart would
              (StatStudio's Measures), so both studios read Data, then the
              output's first choice, then Structure. */}
          {designMode && design?.between}
          {!designMode && (
          <ChartGroup s={s} />
          )}

          {/* Every role in one group: what groups the marks, what separates
              panels and figures, what collapses — and the facet grid right
              under the Subplot role that creates it. */}
          <StructureGroup s={s} />

          {/* "What is averaged, and how the average is shown": the centre and
              spread, and the collapsed keys' own data drawn over them. The
              collapse ROLE itself stays with the other roles in Structure. */}
          {!designMode && (summarizing || capabilities?.sample_overlay || capabilities?.difference_bars?.available) && (
            <StatisticsGroup s={s} />
          )}

          {/* How the figure LOOKS, and nothing about what it shows. Size,
              axes, marks, text and labels — the old "Figure size" section held
              typography and tick behaviour too, and Preview at is a view
              setting, so it went to the toolbar over the figure. */}
          {!designMode && (
          <AppearanceGroup s={s} />
          )}
        </div>
  )
  // The pickers the Data and Structure groups open. Both modes need them.
  const designPopups = (
    <>
      {variantEditor !== null && !csvPath && (
        <VariantDagPopup
          // The ROW's variable, not the panel's: a row plotting RawEMG must
          // edit RawEMG's axes, or the popup offers versions from a variable
          // this row does not draw.
          variable={
            variantRows[variantEditor]?.variable ?? describe?.variable ?? variable
          }
          selection={(spec?.variant_sets?.[variantEditor]?.selection ?? {}) as Record<string, unknown>}
          name={variantRows[variantEditor]?.explicitName ?? ''}
          placeholder={variantRows[variantEditor]?.autoLabel ?? ''}
          onCancel={() => setVariantEditor(null)}
          onApply={({ selection, name }) => {
            // An empty name stays null so the label keeps following the
            // selection; typing one pins it.
            editVariantSet(variantEditor, { selection, name: name || null })
            setVariantEditor(null)
          }}
        />
      )}

      {/* "+ Add variant": pick a variable on the canvas, then a variant of it.
          Any plottable variable, not only the one being plotted — that is what
          makes overlaying RawEMG on FilteredEMG reachable at all. */}
      {addingVariant && !csvPath && (
        <VariantDagPopup
          pick
          variable={describe?.variable ?? variable}
          pickable={stackable}
          refusals={describe?.stackable_refused ?? {}}
          selection={{}}
          name=""
          onCancel={() => setAddingVariant(false)}
          onApply={addVariantFromPicker}
        />
      )}

      {/* "Group by…": pick a variable on the canvas, then its columns and the
          variant of it that supplies the labels. Not offered on the CSV path —
          a flat table has no separately-recorded variable to join in. */}
      {groupPickerOpen && !csvPath && (
        <GroupingDagPopup
          measure={describe?.variable ?? variable}
          candidates={groupableVariables}
          refusals={describe?.groupable_refused ?? {}}
          selected={(spec?.factor_variables ?? []).map(f => ({
            variable: f.variable,
            column: f.column ?? null,
          }))}
          sourceParams={sourceParams}
          onCancel={() => setGroupPickerOpen(false)}
          onApply={applyGroupings}
        />
      )}

      {locationPickerOpen && (
        <SchemaLocationPicker
          variable={describe?.variable ?? variable}
          selection={pickerSelection}
          value={asSelection(spec?.location_filter)}
          onChange={selection => setLocationSelection(selection, schemaKeyNames)}
          onPick={path => {
            pickLocation(path, schemaKeyNames)
            setLocationPickerOpen(false)
          }}
          onClose={() => setLocationPickerOpen(false)}
          csvPath={csvPath}
        />
      )}
    </>
  )
  if (designMode) {
    return (
      <div style={styles.designRail}>
        {sidebarRail}
        {designPopups}
      </div>
    )
  }

  return (
    <Shell
      variable={title}
      // The measure's shape used to head its own sidebar section, which spent a
      // whole block restating the title. It belongs to the title.
      shape={shapeBadge(capabilities)}
      source={sourceName}
      onClose={onClose}
      embedded={embedded}
      panelRef={panelRef}
      controlsHidden={controlsHidden}
      onToggleControls={() => setControlsHidden(v => !v)}
      rightRail={savedRail}
      sidebar={guided ? guidedRail : sidebarRail}
    >
      <div style={styles.figureColumn}>
      {/* The figure toolbar: how the preview is drawn, and everything that
          takes the figure out of the panel. Out of the rail so it is never a
          scroll away, and still there with the controls hidden. */}
      <div style={styles.toolbar}>
        {/* This panel's undo/redo (Cmd/Ctrl+Z works too, outside text fields). */}
        <UndoButtons stack={undo} />
        {/* Which size the preview's labels and legend are decided at: the
            file's (and drawn at it), or the pane's. A view setting. */}
        <label
          style={styles.gridSizeField}
          title="Export size: the preview is drawn at Width x Height, with exactly the label and legend decisions Save makes. Fit pane: fills the pane, decided at the pane's size."
        >
          <span style={styles.gridSizeLabel}>Preview at</span>
          <select
            value={previewMode}
            onChange={e => setPreviewMode(e.target.value as PreviewMode)}
            style={styles.select}
          >
            <option value="export">Export size</option>
            <option value="pane">Fit pane</option>
          </select>
        </label>
        {/* The size the file is written at (preview.figureOutputSize): the
            spec's in Export size, the pane's in Fit pane. Editing it in Fit
            pane fixes that size and switches to Export size (commitSize). */}
        {(() => {
          const unit = unitInfo(sizeUnit)
          const paneNote = previewMode === 'pane'
            ? ' Fit pane: the size of the view on screen, which is what Save writes. Change it to fix the size (switches to Export size).'
            : ''
          return (
            <>
              <FigureSizeInput
                label="W"
                inches={figWidth}
                unit={sizeUnit}
                onChange={onWidth}
                title={`Figure width.${aspectLocked ? ' Locked: the height follows at the current ratio.' : ''}${paneNote}`}
              />
              <FigureSizeInput
                label="H"
                inches={figHeight}
                unit={sizeUnit}
                onChange={onHeight}
                title={
                  (aspectLocked
                    ? 'Figure height. Locked: the width follows at the current ratio.'
                    : 'Figure height. Unlocked: the ratio becomes whatever W and H make.') + paneNote
                }
              />
              <label
                style={styles.gridSizeField}
                title={
                  'Lock aspect: typing or stepping W moves H (and H moves W) so the ratio stays ' +
                  (aspect === CUSTOM_ASPECT ? 'what it is now.' : `${aspect}.`) +
                  ' Off: W and H are independent. Picking a ratio in Appearance > Size always keeps W and sets H.'
                }
              >
                <input
                  type="checkbox"
                  checked={aspectLocked}
                  onChange={e => setAspectLocked(e.target.checked)}
                />
                <span style={styles.gridSizeLabel}>Lock aspect</span>
              </label>
              <select
                value={sizeUnit}
                onChange={e => setSizeUnit(e.target.value as SizeUnit)}
                style={{ ...styles.select, width: 52 }}
                title={`Units for Width and Height; px are the saved raster's pixels at ${SAVE_DPI} dpi. ▲▼ step ${unit.step} ${unit.label}.`}
              >
                {SIZE_UNITS.map(u => (
                  <option key={u.unit} value={u.unit}>{u.label}</option>
                ))}
              </select>
            </>
          )
        })()}
        <span style={styles.toolbarNotice} title={notice}>{notice}</span>
        {/* Beside the save buttons, not in a menu: the format is part of
            the save, and the two buttons share it. The list comes from the
            backend (what this matplotlib can write), so a build without a
            PDF writer cannot be asked for one. */}
        <label style={styles.gridSizeField}>
          <span style={styles.gridSizeLabel}>Format</span>
          <select
            value={imageFormat}
            onChange={e => setImageFormat(e.target.value)}
            disabled={saving}
            style={{ ...styles.select, width: 70 }}
          >
            {formatChoices.map(fmt => (
              <option key={fmt} value={fmt}>{fmt.toUpperCase()}</option>
            ))}
          </select>
        </label>
        <button
          type="button"
          style={toolButtonStyle(styles.toolButton, saving)}
          onClick={() => saveFigures('current')}
          // Same one-job-at-a-time rule as the button beside it: both are
          // background jobs now, and the progress readout follows one id.
          disabled={saving}
          title={
            saving
              ? 'A save is already running'
              : 'Render with matplotlib at full resolution — the same figure ' +
                'the pipeline would produce. Runs in the background.'
          }
        >
          {saving
            ? 'Saving…'
            : figureCount > 1
              ? 'Save current figure'
              : 'Save image'}
        </button>
        {/* The preview, not the export: instant, and ignores the Format
            dropdown (a clipboard takes PNG). */}
        <button
          type="button"
          style={toolButtonStyle(styles.toolButton, figures.length === 0)}
          onClick={copyFigure}
          disabled={figures.length === 0}
          title={
            'Copy the figure on screen to the clipboard as a 300 dpi PNG ' +
            '(light colours). Uses the preview, so a reduced view is copied ' +
            'reduced — use Save image for every point.'
          }
        >
          Copy PNG
        </button>
        {/* Only worth offering when there is more than one figure — and
            worth keeping separate, because it costs figureCount times as
            much as the button beside it. */}
        {figureCount > 1 && (
          <button
            type="button"
            style={toolButtonStyle(styles.toolButton, saving)}
            onClick={() => saveFigures('all')}
            // One job at a time. A second would write to the same files
            // from a second thread, and the progress readout can only
            // follow one id.
            disabled={saving}
            title={
              saving
                ? 'A save is already running'
                : `Render all ${figureCount} figures at full resolution, ` +
                  `one file each — runs in the background`
            }
          >
            {saving ? 'Saving…' : `Save all ${figureCount}`}
          </button>
        )}
        {/* What the figure leaves out (scistackplot.omissions): shown only
            when something is, with Python's own words. */}
        {notPlotted?.button && (
          <button
            type="button"
            style={styles.toolButton}
            onClick={() => {
              console.info('[Plot Studio] not_plotted_open', notPlotted.counts)
              setNotPlottedOpen(true)
            }}
            title={notPlotted.sentence}
          >
            {notPlotted.button}
          </button>
        )}
        {/* The rows the figure is drawn from, for statistics that must
            match the plot. Unavailable, with the backend's reason, for a
            raw 1-D / 2-D measure (scalar plots only). `aria-disabled`, not
            `disabled`, for that case: a click then puts the reason in the
            status line instead of doing nothing (toolButton.ts). */}
        <button
          type="button"
          style={toolButtonStyle(
            styles.toolButton,
            saving || !capabilities?.data_export?.available
          )}
          onClick={() => {
            const blocked = blockedClickNotice(
              capabilities?.data_export?.available,
              capabilities?.data_export?.reason
            )
            if (blocked) {
              console.info('[Plot Studio] save_data blocked', { reason: blocked })
              setNotice(`Save data: ${blocked}`)
              return
            }
            openDataSave()
          }}
          disabled={saving}
          aria-disabled={saving || !capabilities?.data_export?.available}
          title={
            saving
              ? 'A save is already running'
              : capabilities?.data_export?.available
                ? 'Save the long table this plot is drawn from as CSV — every ' +
                  'figure in one file, the figure keys as columns'
                : capabilities?.data_export?.reason ?? 'Not available for this plot'
          }
        >
          Save data (CSV)
        </button>
        {!capabilities?.custom && (
          <button type="button" style={styles.toolButton} onClick={handleExport}>
            Export code
          </button>
        )}
        {/* A CSV tab has no project to write an endpoint into — and on
            the database-less plot-only server the call is refused
            outright, so offering the button would only produce an
            error. "Export code" stays: plot_export is db_optional and
            works from a CSV. */}
        {!csvPath && !capabilities?.custom && (
          <button type="button" style={styles.toolPrimaryButton} onClick={handleAddToPipeline}>
            Add to pipeline
          </button>
        )}
        {!capabilities?.custom && (
          <StatsLink
            spec={spec}
            plotId={loadedPlot?.plot_id ?? null}
            variable={describe?.variable ?? variable}
            csvPath={csvPath}
            onApplySpec={applyLinkedSpec}
            onDerivedBars={setDerivedBars}
            buttonStyle={styles.toolButton}
          />
        )}
        {/* Floats over the figure rather than pushing it down: a taller
            toolbar would shrink the canvas and re-request a pane preview. */}
        {dataChooser && capabilities?.data_export?.available && (
          <div style={styles.dataChooser}>
            {capabilities.data_export.depths.length > 1 && (
              <div style={styles.hint}>
                Which rows? The default is exactly what the plot is drawn from;
                deeper choices keep the lower levels instead of averaging them.
              </div>
            )}
            {capabilities.data_export.depths.length > 1 &&
              capabilities.data_export.depths.map(depth => (
              <label key={depth.key ?? '__plotted'} style={styles.dataDepthOption}>
                <input
                  type="radio"
                  name="data-depth"
                  checked={dataDepth === depth.key}
                  onChange={() => setDataDepth(depth.key)}
                />
                <span>
                  {depth.label}
                  {depth.key === capabilities.data_export?.default ? ' (default)' : ''}
                  <span style={styles.dataColumns}>
                    {writtenHeader(depth, dataWide).join(', ')}
                  </span>
                </span>
              </label>
            ))}
            {(() => {
              // The checkbox only means something at a depth that keeps the
              // struct's fields; elsewhere it is hidden, not greyed out.
              const chosen = capabilities.data_export.depths.find(d => d.key === dataDepth)
              if (!capabilities.data_export.field_factor || !chosen?.wide_columns) return null
              return (
                <label style={styles.dataDepthOption}>
                  <input
                    type="checkbox"
                    checked={dataWide}
                    onChange={e => setDataWide(e.target.checked)}
                  />
                  <span>
                    One column per field ({capabilities.data_export.field_factor})
                    <span style={styles.dataColumns}>
                      {writtenHeader(chosen, dataWide).join(', ')}
                    </span>
                  </span>
                </label>
              )
            })()}
            <div style={styles.actions}>
              <button
                type="button"
                style={styles.primaryButton}
                onClick={() => void saveData(dataDepth, dataWide)}
                disabled={saving}
              >
                Save CSV…
              </button>
              <button type="button" style={styles.button} onClick={() => setDataChooser(false)}>
                Cancel
              </button>
            </div>
          </div>
        )}
      </div>
      <div style={styles.canvas} ref={canvasRef}>
        {specError && <div style={styles.specError}>{specError}</div>}
        {/* Say what the fan-out did that the spec did not literally ask for —
            iterating a nested key iterates its ancestors, so the figure count
            is larger than "one per trial" would suggest. */}
        {fanoutNotes.map((note, index) => (
          <div key={index} style={styles.layoutNote}>{note}</div>
        ))}
        {figureCount > 1 && (
          <FigureNavigator
            index={figureIndex}
            count={figureCount}
            label={figureLabels[figureIndex] ?? ''}
            onStep={stepFigure}
          />
        )}
        {/* The figure IS drawn, and its points were computed by more than one
            version of the same function. On the canvas rather than only on the
            sidebar row because that was the explicit requirement: a tag in a
            section the user may have collapsed is not prominent.
            Informational, not an error — per-location "latest" legitimately
            produces this, and it is the figure that cannot show it. The
            sentence names the locations so it can be acted on or dismissed at a
            glance.
            Deliberately says nothing about the pipeline canvas: a partially
            re-run PathInput loader reads GREEN there while producing exactly
            this state, so pointing at the canvas would mislead in the one case
            where this banner is the only signal
            (.claude/plan-pathinput-loader-staleness-gap.md). */}
        {notPlottedOpen && notPlotted && (
          <NotPlottedPopup
            total={notPlotted}
            figures={figures}
            noteOn={spec?.style?.not_plotted_note === true}
            saving={saving}
            onToggleNote={setNotPlottedNote}
            onSave={() => void saveData(NOT_PLOTTED_DEPTH, true)}
            onClose={() => setNotPlottedOpen(false)}
          />
        )}
        {/* A reopened saved plot whose data now has newer code. The figure
            below already shows the version it was saved with; this asks
            whether to move the saved plot to latest for good. Modal because
            it is a decision about the saved plot, not a passing notice. */}
        {newerPrompt && (
          <div style={styles.newerOverlay} role="dialog" aria-modal="true">
            <div style={styles.newerDialog}>
              <div style={styles.newerTitle}>Newer data exists for “{newerPrompt.name}”</div>
              <div style={styles.newerText}>{newerPrompt.text}</div>
              <div style={styles.newerText}>
                Switch this saved plot to the latest version? It will then follow
                the latest code automatically.
              </div>
              <div style={styles.newerButtons}>
                <button
                  type="button"
                  style={styles.newerPrimary}
                  disabled={savedBusy}
                  onClick={() => switchSavedToLatest(newerPrompt.plotId)}
                >
                  Switch to latest
                </button>
                <button type="button" style={styles.newerSecondary} onClick={keepSavedVersion}>
                  Keep saved version
                </button>
              </div>
            </div>
          </div>
        )}
        {spanningSets.map(({ set, index }) => (
          <div key={`span-${index}`} style={styles.spanBanner}>
            <strong>{set.name}</strong> mixes{' '}
            {Object.values(set.spans).map(describeSpan).join(' | ')}. Pin a
            version (or run options) on the row, or split it into one variant per
            version.
          </div>
        ))}
        {busy && <div style={styles.note}>Resolving…</div>}
        {/* Why the figure is empty, where the figure would have been.
            Reachable by design: the opening pin is applied blindly (latest
            code, first parameter value) because a rule that quietly picks a
            different value to avoid an empty figure is no longer predictable.
            The cost of that choice is paid here — the panel has to name what
            was attempted AND what exists, or the controls look correctly filled
            in and simply broken. */}
        {!specError && !busy && emptyPin.length > 0 && (
          <div style={styles.emptyPin}>
            <div style={styles.emptyPinTitle}>
              No records match {emptyPin.length > 1 ? 'these variants' : 'this variant'}.
            </div>
            {emptyPin.map(({ set, index }) => (
              <div key={index} style={styles.emptyPinBlock}>
                <div style={styles.emptyPinAttempt}>
                  <strong>{set.name}</strong> is asking for{' '}
                  <code style={styles.emptyPinCode}>
                    {Object.entries(set.resolved ?? {})
                      .map(([key, value]) => `${key}=${String(value)}`)
                      .join(', ') || 'everything'}
                  </code>
                </div>
                {set.available.length > 0 ? (
                  <>
                    <div style={styles.emptyPinHint}>
                      These combinations have records — click one to plot it:
                    </div>
                    <div style={styles.emptyPinOptions}>
                      {set.available.map((combination, position) => (
                        <button
                          key={position}
                          type="button"
                          style={styles.emptyPinOption}
                          onClick={() => adoptCombination(index, combination)}
                        >
                          {Object.entries(combination)
                            .map(([key, value]) => `${key}=${value}`)
                            .join(', ')}
                        </button>
                      ))}
                    </div>
                  </>
                ) : (
                  <div style={styles.emptyPinHint}>
                    This variable has no records at all yet — run the pipeline
                    that produces it.
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
        {!specError && figures.length === 0 && !busy && emptyPin.length === 0 && (
          <div style={styles.note}>Nothing to plot with these settings.</div>
        )}
        {figures.map((figure, index) => (
          <div
            key={figure.label || index}
            // Found by `copyFigure`, for the drawn size of a pane-filling figure.
            data-ps-figure={figure.index}
            style={{
              ...styles.figureBlock,
              ...(figures.length === 1 ? { height: '100%' } : null),
            }}
          >
            {/* The navigator already names the figure it is showing. */}
            {figure.label && figureCount < 2 && (
              <div style={styles.figureLabel}>{figure.label}</div>
            )}
            {figure.downsampled_from && (
              <div style={styles.downsampleNote}>
                Showing a reduced view of {figure.downsampled_from.toLocaleString()} points —
                the exported figure uses every point.
              </div>
            )}
            {figure.custom_preview && (
              <CustomFigure
                payload={figure.custom_preview}
                fillHeight={figureHeight}
                label={figure.label}
              />
            )}
            {(() => {
              // Export-size preview: drawn at the file's size (meta.fixed_size,
              // 1 pt = 1 px), scrolling if the pane is smaller. Otherwise the
              // figure fills the pane as it always has.
              if (!figure.figure) return null
              const fixed = (figure.figure.layout?.meta as { fixed_size?: [number, number] } | undefined)
                ?.fixed_size
              if (!fixed) return null
              // Light: the renderer's figure untouched, which is the saved
              // file. Dark: recoloured for the screen (plotTheme.screenFigure).
              const shown = screenFigure(figure.figure, theme)
              return (
                <div style={{ overflow: 'auto', maxWidth: '100%' }}>
                  <Plot
                    data={shown.data}
                    layout={withPickingBands(figure, {
                      ...shown.layout,
                      autosize: false,
                      width: fixed[0],
                      height: fixed[1],
                    })}
                    onClick={event => onFigureClick(figure.index, figureDiffMeta(figure), event)}
                    onHover={event => onFigureHover(figure.index, figureDiffMeta(figure), event)}
                    onUnhover={() => setHoverTick(null)}
                    onInitialized={() => onFigureDrawn(figure.index)}
                    onUpdate={() => onFigureDrawn(figure.index)}
                    config={{
                      displaylogo: false,
                      responsive: false,
                      modeBarButtonsToRemove: isVSCodeMode ? ['toImage'] : [],
                    }}
                    style={{ width: fixed[0], height: fixed[1] }}
                  />
                </div>
              )
            })()}
            {figure.figure && !(figure.figure.layout?.meta as { fixed_size?: unknown } | undefined)?.fixed_size && (
            <Plot
              data={screenFigure(figure.figure, theme).data}
              layout={withPickingBands(figure, {
                // As above: untouched in light mode, screen colours in dark.
                // Sizes are always the renderer's (`layout.font.size` =
                // `TextSizes.base`), so a text setting shows before a save.
                ...screenFigure(figure.figure, theme).layout,
                autosize: true,
                height: figureHeight,
              })}
              onClick={event => onFigureClick(figure.index, figureDiffMeta(figure), event)}
              onHover={event => onFigureHover(figure.index, figureDiffMeta(figure), event)}
              onUnhover={() => setHoverTick(null)}
              onInitialized={() => onFigureDrawn(figure.index)}
              onUpdate={() => onFigureDrawn(figure.index)}
              config={{
                displaylogo: false,
                responsive: true,
                // A webview cannot download; "Save image" does it server-side.
                modeBarButtonsToRemove: isVSCodeMode ? ['toImage'] : [],
              }}
              style={{ width: '100%' }}
              useResizeHandler
            />
            )}
          </div>
        ))}
      </div>
      </div>

      {code !== null && (
        <div style={styles.codeOverlay} onClick={() => setCode(null)}>
          <pre style={styles.code} onClick={e => e.stopPropagation()}>{code}</pre>
        </div>
      )}

      {designPopups}
    </Shell>
  )
}
