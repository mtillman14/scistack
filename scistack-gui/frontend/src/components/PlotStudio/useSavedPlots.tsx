// PlotStudio state, part 7 of 8: saved plots.
import { useEffect, useMemo, useCallback } from 'react'
import { callBackend } from '../../api'
import { type SavedPlotInfo, modifiedKey, viewState, isModified, type RestoreNote, type NewerVersion, readView, iterateSignature as iterateSignatureOf, variantSetsKey, newerVersionsText } from './savedPlots'
import { type SaveResult } from './SavedPlotsRail'
import { Spec, Capabilities } from './plotStudioTypes'
import type { StudioCoreState } from './useStudioCore'
import type { StepByStepState } from './useStepByStep'
import type { CombinesState } from './useCombines'
import type { PinReportState } from './usePinReport'
import type { FigureSizeState } from './useFigureSize'
import type { ExportState } from './useExport'

/** PlotStudio: saved plots. Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function useSavedPlots(s: StudioCoreState & StepByStepState & CombinesState & PinReportState & FigureSizeState & ExportState) {
  const {
    csvPath,
    describe,
    spec,
    setSpec,
    capabilities,
    setCapabilities,
    figureIndex,
    setFigureIndex,
    previewMode,
    setPreviewMode,
    setSavedPlots,
    setSavedRailCollapsed,
    setNewerPrompt,
    loadedPlot,
    setLoadedPlot,
    view,
    setView,
    setRestoreNotes,
    setSavedBusy,
    setSavedError,
    baseline,
    setBaseline,
    captureBaseline,
    openedVariantSets,
    restoredSignature,
    capsSpecRef,
  } = s

  // --- saved plots ----------------------------------------------------------
  // Keyed by the variable `describe` resolved (the prop may be empty, meaning
  // "the first plottable measure"). Plots are saved against that variable.
  // A plot_ node's figures are saved under its FUNCTION, apart from any
  // variable's (custom plot kinds): the key is a label, not a variable.
  const savedVariable = csvPath
    ? null
    : describe?.plot_node?.function ?? describe?.variable ?? null

  useEffect(() => {
    setSavedPlots([])
    if (!savedVariable) return
    let cancelled = false
    callBackend('plot_saved_list', { variable: savedVariable })
      .then(raw => {
        if (!cancelled) setSavedPlots((raw as { plots: SavedPlotInfo[] }).plots ?? [])
      })
      .catch(err => !cancelled && setSavedError(`Could not list saved plots: ${(err as Error).message}`))
    return () => { cancelled = true }
  }, [savedVariable])

  const currentKey = useMemo(
    () => modifiedKey(spec, viewState(previewMode, figureIndex)),
    [spec, previewMode, figureIndex]
  )
  // `baseline` is a dependency so that a save of UNCHANGED settings (the
  // key does not move) still captures: the save resets it to null, and that
  // change alone re-runs this.
  useEffect(() => {
    if (!captureBaseline.current || !spec) return
    captureBaseline.current = false
    setBaseline(currentKey)
  }, [currentKey, spec, baseline])
  const modified = isModified(baseline, currentKey)

  const saveCurrent = useCallback(
    async (name: string, overwrite: boolean): Promise<SaveResult> => {
      if (!spec || !savedVariable) return { ok: false }
      setSavedBusy(true)
      setSavedError('')
      try {
        const result = (await callBackend('plot_saved_save', {
          variable: savedVariable,
          name,
          spec,
          view: viewState(previewMode, figureIndex),
          overwrite,
          current_plot_id: loadedPlot?.plot_id ?? null,
        })) as { ok: boolean; plot?: SavedPlotInfo; plots?: SavedPlotInfo[]; exists?: SavedPlotInfo }
        if (!result.ok) return { ok: false, exists: result.exists }
        setSavedPlots(result.plots ?? [])
        setLoadedPlot(result.plot ?? null)
        setRestoreNotes([])
        captureBaseline.current = true
        setBaseline(null)
        return { ok: true }
      } catch (err) {
        setSavedError(`Could not save: ${(err as Error).message}`)
        return { ok: false }
      } finally {
        setSavedBusy(false)
      }
    },
    [spec, savedVariable, previewMode, figureIndex, loadedPlot]
  )

  /** Put an opened saved plot on screen — shared by Open and by answering
   *  "switch to latest", which replies with the reopened plot. */
  const applyOpened = useCallback((raw: unknown) => {
        const result = raw as {
          plot: SavedPlotInfo & {
            spec: Spec
            view: unknown
            notes: RestoreNote[]
            newer?: NewerVersion[]
          }
          capabilities: Capabilities | null
          plots?: SavedPlotInfo[]
        }
        const { spec: restored, view, notes, newer = [], ...info } = result.plot
        const shown = readView(view)
        console.info(
          `[PlotStudio] opened saved plot "${info.name}" v${info.version}: ` +
            `${notes.length} note(s), ${newer.length} newer code version(s)`, notes, newer
        )
        // Keep the restored figure index through the fan-out reset its roles
        // would otherwise trigger (see `restoredSignature`).
        restoredSignature.current = iterateSignatureOf(restored.roles)
        // Opening is not an edit of the Variants section: re-baseline the pin
        // effect so these rows (which may carry a FROZEN code version) are
        // not written as this variable's default pins.
        openedVariantSets.current = variantSetsKey(restored.variant_sets)
        setSpec(restored)
        if (result.plots) setSavedPlots(result.plots)
        // Newer code than the plot was saved with: ask, never switch silently.
        setNewerPrompt(
          newer.length > 0
            ? { plotId: info.plot_id, name: info.name, text: newerVersionsText(newer) }
            : null
        )
        setPreviewMode(shown.previewMode)
        setFigureIndex(shown.figureIndex)
        if (result.capabilities) {
          setCapabilities(result.capabilities)
          capsSpecRef.current = JSON.stringify(restored)
        }
        setLoadedPlot(info as SavedPlotInfo)
        setRestoreNotes(notes)
        // A saved figure reopens on every setting (as a saved table does).
        setView('advanced')
        if (notes.length) {
          // Not in the current format until saved again: never clean.
          captureBaseline.current = false
          setBaseline(null)
          setSavedRailCollapsed(false)
        } else {
          captureBaseline.current = true
          setBaseline(null)
        }
  }, [])

  const openSaved = useCallback((plotId: string) => {
    setSavedBusy(true)
    setSavedError('')
    callBackend('plot_saved_open', { plot_id: plotId })
      .then(applyOpened)
      .catch(err => setSavedError(`Could not open: ${(err as Error).message}`))
      .finally(() => setSavedBusy(false))
  }, [applyOpened])

  // "Newer data exists — switch this saved plot to the latest version?"
  // Yes appends a saved version that follows latest from now on and reopens
  // it (scistackplotdb.follow_latest_saved_plot); No keeps the frozen version
  // on screen and writes nothing.
  const switchSavedToLatest = useCallback((plotId: string) => {
    setSavedBusy(true)
    setSavedError('')
    setNewerPrompt(null)
    console.info(`[PlotStudio] saved plot ${plotId}: switching to follow latest`)
    callBackend('plot_saved_follow_latest', { plot_id: plotId })
      .then(applyOpened)
      .catch(err => setSavedError(`Could not switch to latest: ${(err as Error).message}`))
      .finally(() => setSavedBusy(false))
  }, [applyOpened])
  const keepSavedVersion = useCallback(() => {
    console.info('[PlotStudio] kept the saved plot on its frozen code version')
    setNewerPrompt(null)
  }, [])

  const renameSaved =useCallback(async (plotId: string, name: string): Promise<boolean> => {
    setSavedError('')
    try {
      const result = (await callBackend('plot_saved_rename', { plot_id: plotId, name })) as {
        plot: SavedPlotInfo
        plots: SavedPlotInfo[]
      }
      setSavedPlots(result.plots ?? [])
      setLoadedPlot(current => (current?.plot_id === plotId ? result.plot : current))
      return true
    } catch (err) {
      setSavedError(`Could not rename: ${(err as Error).message}`)
      return false
    }
  }, [])

  const removeSaved = useCallback(async (plotId: string) => {
    setSavedError('')
    try {
      const result = (await callBackend('plot_saved_hide', { plot_id: plotId })) as {
        plots: SavedPlotInfo[]
      }
      setSavedPlots(result.plots ?? [])
      if (loadedPlot?.plot_id === plotId) {
        // The settings on screen are no longer saved anywhere visible.
        captureBaseline.current = false
        setLoadedPlot(null)
        setBaseline(null)
      }
    } catch (err) {
      setSavedError(`Could not remove: ${(err as Error).message}`)
    }
  }, [loadedPlot])

  return {
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
  }
}

export type SavedPlotsState = ReturnType<typeof useSavedPlots>
