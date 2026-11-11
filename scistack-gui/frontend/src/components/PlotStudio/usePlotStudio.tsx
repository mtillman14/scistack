// PlotStudio's state, effects and handlers, composed from one hook per
// concern. Each takes what the earlier ones returned, so they run in the
// order the single component body ran them (grug stage plotstudio_split).
import type { Props } from './PlotStudio'
import { useStudioCore } from './useStudioCore'
import { useStepByStep } from './useStepByStep'
import { useCombines } from './useCombines'
import { usePinReport } from './usePinReport'
import { useFigureSize } from './useFigureSize'
import { useExport } from './useExport'
import { useSavedPlots } from './useSavedPlots'
import { usePresets } from './usePresets'

export function usePlotStudio(props: Props) {
  const core = useStudioCore(props)
  const stepByStepPart = useStepByStep(core)
  const combinesPart = useCombines({ ...core, ...stepByStepPart })
  const pinReportPart = usePinReport({ ...core, ...stepByStepPart, ...combinesPart })
  const figureSizePart = useFigureSize({ ...core, ...stepByStepPart, ...combinesPart, ...pinReportPart })
  const exportPart = useExport({ ...core, ...stepByStepPart, ...combinesPart, ...pinReportPart, ...figureSizePart })
  const savedPlotsPart = useSavedPlots({ ...core, ...stepByStepPart, ...combinesPart, ...pinReportPart, ...figureSizePart, ...exportPart })
  const presetsPart = usePresets({ ...core, ...stepByStepPart, ...combinesPart, ...pinReportPart, ...figureSizePart, ...exportPart, ...savedPlotsPart })
  return { ...core, ...stepByStepPart, ...combinesPart, ...pinReportPart, ...figureSizePart, ...exportPart, ...savedPlotsPart, ...presetsPart }
}

/** Everything the PlotStudio render reads: its props, state, effects'
 *  results and handlers. Rail groups take it as `s`. */
export type PlotStudioState = ReturnType<typeof usePlotStudio>
