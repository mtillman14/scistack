// PlotStudio state, part 5 of 8: the figure size toolbar.
import { FALLBACK_PRESETS, aspectName, parseAspectLocked, ASPECT_LOCK_STORAGE_KEY, heightFor, resizeFigure } from './figureSize'
import { type PreviewMeta, figureOutputSize } from './preview'
import { useMemo, useState, useEffect, useCallback } from 'react'
import { type ResolvedTextSizes } from './textSizes'
import { type SizeUnit, parseSizeUnit, SIZE_UNIT_STORAGE_KEY } from './figureUnits'
import type { StudioCoreState } from './useStudioCore'
import type { StepByStepState } from './useStepByStep'
import type { CombinesState } from './useCombines'
import type { PinReportState } from './usePinReport'

/** PlotStudio: the figure size toolbar. Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function useFigureSize(s: StudioCoreState & StepByStepState & CombinesState & PinReportState) {
  const {
    describe,
    spec,
    figures,
    previewMode,
    setPreviewMode,
    preview,
    setStyle,
  } = s

  // --- figure size ----------------------------------------------------------
  // The spec's own size, in inches (StyleOptions.width/height).
  const presets = describe?.figure_presets ?? FALLBACK_PRESETS
  const specWidth = typeof spec?.style?.width === 'number' ? spec.style.width : 8
  const specHeight = typeof spec?.style?.height === 'number' ? spec.style.height : 6
  // The size the file is WRITTEN at, and what the toolbar's Width and Height
  // show: the spec's in Export size, the pane's in Fit pane
  // (preview.figureOutputSize owns the rule).
  const previewMeta = (figures[0]?.figure?.layout?.meta as { preview?: PreviewMeta } | undefined)
    ?.preview
  const { width: figWidth, height: figHeight } = figureOutputSize(previewMode, previewMeta, {
    width: specWidth,
    height: specHeight,
  })
  // Save, Export code and Add to pipeline take this spec, so in Fit pane the
  // file is the view on screen. Saved plots keep `spec`: their view records
  // Fit pane, and reopening fits the pane again.
  const outputSpec = useMemo(() => {
    if (!spec || (figWidth === specWidth && figHeight === specHeight)) return spec
    return { ...spec, style: { ...(spec.style ?? {}), width: figWidth, height: figHeight } }
  }, [spec, figWidth, figHeight, specWidth, specHeight])
  // Points in the export, px in the preview — one number, so a change is
  // visible before anything is saved. null = AUTO (`TextSizes.base` None):
  // Python chooses every size (scistackplot.autosize) and reports them in
  // layout.meta.text_sizes, read below; nothing here guesses a number.
  const fontSize = typeof spec?.style?.text?.base === 'number' ? spec.style.text.base : null
  const textMeta = (figures[0]?.figure?.layout?.meta as { text_sizes?: ResolvedTextSizes } | undefined)
    ?.text_sizes
  const drawnFont = fontSize ?? (typeof textMeta?.base === 'number' ? textMeta.base : null)
  // What the size IS: the dropdown only reads it. Custom is a readout (it
  // cannot be picked); whether W and H move together is the Lock checkbox.
  const aspect = aspectName(figWidth, figHeight, presets)
  // Per viewer, like the unit: an editing mode, not part of the plot.
  const [aspectLocked, setAspectLocked] = useState<boolean>(() => {
    try {
      return parseAspectLocked(window.localStorage.getItem(ASPECT_LOCK_STORAGE_KEY))
    } catch {
      return parseAspectLocked(null)
    }
  })
  useEffect(() => {
    try {
      window.localStorage.setItem(ASPECT_LOCK_STORAGE_KEY, String(aspectLocked))
    } catch {
      // Not remembered; the checkbox still works.
    }
  }, [aspectLocked])
  // Editing the size in Fit pane fixes it: the pane size shown becomes the
  // spec's, then the edit applies, and the preview follows at Export size.
  const commitSize = useCallback(
    (patch: { width?: number; height?: number }, why: string) => {
      if (previewMode === 'pane') {
        console.info('[Plot Studio] size_fixed_from_pane', { why, pane: { width: figWidth, height: figHeight }, patch })
        setPreviewMode('export')
      }
      setStyle({ width: figWidth, height: figHeight, ...patch })
    },
    [previewMode, figWidth, figHeight, setStyle]
  )
  const onAspect = useCallback(
    (name: string) => {
      // A ratio keeps the width and moves the height, locked or not.
      if (presets.find(p => p.name === name)?.ratio == null) return
      const height = heightFor(figWidth, name, presets, figHeight)
      console.info('[Plot Studio] aspect_picked', { name, from: aspect, width: figWidth, height })
      commitSize({ height }, 'aspect')
    },
    [presets, aspect, figWidth, figHeight, commitSize]
  )
  // The Lock rule has one owner: figureSize.resizeFigure.
  const onResize = useCallback(
    (edit: { width: number } | { height: number }, why: 'width' | 'height') => {
      const size = resizeFigure({ width: figWidth, height: figHeight }, edit, aspectLocked, presets)
      console.info('[Plot Studio] figure_resized', { why, locked: aspectLocked, from: { width: figWidth, height: figHeight }, to: size })
      commitSize(size, why)
    },
    [figWidth, figHeight, aspectLocked, presets, commitSize]
  )
  const onWidth = useCallback((width: number) => onResize({ width }, 'width'), [onResize])
  const onHeight = useCallback((height: number) => onResize({ height }, 'height'), [onResize])
  // The unit the Width and Height boxes are shown in (figureUnits.ts); the
  // spec stays in inches. Per viewer; storage may be missing or throw.
  const [sizeUnit, setSizeUnit] = useState<SizeUnit>(() => {
    try {
      return parseSizeUnit(window.localStorage.getItem(SIZE_UNIT_STORAGE_KEY))
    } catch {
      return parseSizeUnit(null)
    }
  })
  useEffect(() => {
    try {
      window.localStorage.setItem(SIZE_UNIT_STORAGE_KEY, sizeUnit)
    } catch {
      // Not remembered; the boxes still work.
    }
  }, [sizeUnit])

  return {
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
  }
}

export type FigureSizeState = ReturnType<typeof useFigureSize>
