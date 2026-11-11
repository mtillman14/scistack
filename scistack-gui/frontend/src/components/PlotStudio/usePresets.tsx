// PlotStudio state, part 8 of 8: presets.
import { useEffect, useCallback } from 'react'
import { callBackend } from '../../api'
import { type PresetInfo, type AppliedPreset } from './presets'
import PresetsSection, { type PresetSaveResult } from './PresetsSection'
import { Spec, Capabilities } from './plotStudioTypes'
import SavedPlotsRail from './SavedPlotsRail'
import type { StudioCoreState } from './useStudioCore'
import type { StepByStepState } from './useStepByStep'
import type { CombinesState } from './useCombines'
import type { PinReportState } from './usePinReport'
import type { FigureSizeState } from './useFigureSize'
import type { ExportState } from './useExport'
import type { SavedPlotsState } from './useSavedPlots'

/** PlotStudio: presets. Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function usePresets(s: StudioCoreState & StepByStepState & CombinesState & PinReportState & FigureSizeState & ExportState & SavedPlotsState) {
  const {
    spec,
    setSpec,
    capabilities,
    setCapabilities,
    savedPlots,
    savedRailCollapsed,
    setSavedRailCollapsed,
    loadedPlot,
    restoreNotes,
    setRestoreNotes,
    savedBusy,
    savedError,
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
    capsSpecRef,
    presets,
    savedVariable,
    modified,
    saveCurrent,
    openSaved,
    renameSaved,
    removeSaved,
  } = s

  // --- presets --------------------------------------------------------------
  // The variable's raw shape, so the list can warn about a preset made on a
  // differently shaped variable (`scistackplot.shape_warning` decides).
  const rawShape = capabilities?.raw_shape ?? null

  useEffect(() => {
    setAppliedPreset(null)
    setPresetNotes([])
  }, [savedVariable])

  useEffect(() => {
    if (!savedVariable) { setPlotPresets([]); return }
    let cancelled = false
    callBackend('plot_preset_list', { shape: rawShape })
      .then(raw => {
        if (!cancelled) setPlotPresets((raw as { presets: PresetInfo[] }).presets ?? [])
      })
      .catch(err => !cancelled && setPresetError(`Could not list presets: ${(err as Error).message}`))
    return () => { cancelled = true }
  }, [savedVariable, rawShape])

  const savePreset = useCallback(
    async (name: string, overwrite: boolean): Promise<PresetSaveResult> => {
      if (!spec || !savedVariable) return { ok: false }
      setPresetBusy(true)
      setPresetError('')
      try {
        const result = (await callBackend('plot_preset_save', {
          name,
          spec,
          made_on_shape: rawShape,
          overwrite,
          current_preset_id: appliedPreset?.preset_id ?? null,
        })) as { ok: boolean; preset?: PresetInfo; presets?: PresetInfo[]; exists?: PresetInfo }
        if (!result.ok) return { ok: false, exists: result.exists }
        setPlotPresets(result.presets ?? [])
        console.info(`[PlotStudio] saved preset "${name}" v${result.preset?.version}`)
        return { ok: true }
      } catch (err) {
        setPresetError(`Could not save the preset: ${(err as Error).message}`)
        return { ok: false }
      } finally {
        setPresetBusy(false)
      }
    },
    [spec, savedVariable, rawShape, appliedPreset]
  )

  const applyPreset = useCallback((presetId: string) => {
    if (!spec) return
    setPresetBusy(true)
    setPresetError('')
    callBackend('plot_preset_apply', { preset_id: presetId, spec })
      .then(raw => {
        const result = raw as AppliedPreset
        const { spec: applied, notes, ...info } = result.preset
        console.info(
          `[PlotStudio] applied preset "${info.name}" v${info.version}: ${notes.length} note(s)`,
          notes
        )
        // A user edit, so one undo step restores the settings from before.
        setSpec(applied as Spec)
        if (result.capabilities) {
          setCapabilities(result.capabilities as Capabilities)
          capsSpecRef.current = JSON.stringify(applied)
        }
        setAppliedPreset(info as PresetInfo)
        setPresetNotes(notes)
      })
      .catch(err => setPresetError(`Could not apply: ${(err as Error).message}`))
      .finally(() => setPresetBusy(false))
  }, [spec, setSpec])

  const renamePreset = useCallback(async (presetId: string, name: string): Promise<boolean> => {
    setPresetError('')
    try {
      const result = (await callBackend('plot_preset_rename', {
        preset_id: presetId, name, shape: rawShape,
      })) as { preset: PresetInfo; presets: PresetInfo[] }
      setPlotPresets(result.presets ?? [])
      setAppliedPreset(current => (current?.preset_id === presetId ? result.preset : current))
      return true
    } catch (err) {
      setPresetError(`Could not rename: ${(err as Error).message}`)
      return false
    }
  }, [rawShape])

  const removePreset = useCallback(async (presetId: string) => {
    setPresetError('')
    try {
      const result = (await callBackend('plot_preset_hide', {
        preset_id: presetId, shape: rawShape,
      })) as { presets: PresetInfo[] }
      setPlotPresets(result.presets ?? [])
    } catch (err) {
      setPresetError(`Could not remove: ${(err as Error).message}`)
    }
  }, [rawShape])

  const savedRail = savedVariable ? (
    <SavedPlotsRail
      collapsed={savedRailCollapsed}
      onToggle={() => setSavedRailCollapsed(v => !v)}
      plots={savedPlots}
      loaded={loadedPlot}
      modified={modified}
      notes={restoreNotes}
      busy={savedBusy}
      error={savedError}
      onSave={saveCurrent}
      onOpen={openSaved}
      onRename={renameSaved}
      onRemove={removeSaved}
      onDismissNotes={() => setRestoreNotes([])}
    >
      <PresetsSection
        presets={plotPresets}
        applied={appliedPreset}
        notes={presetNotes}
        busy={presetBusy}
        error={presetError}
        onSave={savePreset}
        onApply={applyPreset}
        onRename={renamePreset}
        onRemove={removePreset}
        onDismissNotes={() => { setAppliedPreset(null); setPresetNotes([]) }}
      />
    </SavedPlotsRail>
  ) : null

  return {
    rawShape,
    savePreset,
    applyPreset,
    renamePreset,
    removePreset,
    savedRail,
  }
}

export type PresetsState = ReturnType<typeof usePresets>
