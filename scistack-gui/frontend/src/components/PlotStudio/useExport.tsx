// PlotStudio state, part 6 of 8: saving and exporting figures and data.
import { useCallback, useRef, useState } from 'react'
import { callBackend, isVSCodeMode } from '../../api'
import { customPreviewBlob, customPreviewSize } from './CustomFigure'
import { copyFigurePng } from './clipboardPng'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import type { StudioCoreState } from './useStudioCore'
import type { StepByStepState } from './useStepByStep'
import type { CombinesState } from './useCombines'
import type { PinReportState } from './usePinReport'
import type { FigureSizeState } from './useFigureSize'

/** PlotStudio: saving and exporting figures and data. Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function useExport(s: StudioCoreState & StepByStepState & CombinesState & PinReportState & FigureSizeState) {
  const {
    sourceParams,
    derivedBars,
    resolveParams,
    describe,
    undo,
    spec,
    capabilities,
    figures,
    figureIndex,
    setCode,
    setNotice,
    previewMode,
    title,
    figureCount,
    figWidth,
    figHeight,
    outputSpec,
  } = s

  // --- export -------------------------------------------------------------
  const handleExport = useCallback(() => {
    if (!spec || !outputSpec) return
    setNotice('')
    callBackend('plot_export', { spec: outputSpec, ...resolveParams })
      .then(raw => setCode((raw as { source: string }).source))
      .catch(err => setNotice(`Export failed: ${(err as Error).message}`))
  }, [spec, outputSpec, resolveParams])

  // The background save's job id. A ref, not state: the first figure can
  // report progress before the render that set it has committed, and the
  // message handler has to be able to match that id immediately.
  const saveJob = useRef<string | null>(null)
  // The same fact as state, purely to re-render the button. The ref is the
  // one the handler reads; this only ever follows it.
  const [saving, setSaving] = useState(false)
  // Which kind of save the running job is — the progress wording differs
  // ("Resolving figure 1 of 2…" means nothing for a CSV).
  const saveKind = useRef<'image' | 'data'>('image')
  // "Save data": the inline depth chooser, and the depth picked in it.
  const [dataChooser, setDataChooser] = useState(false)
  const [dataDepth, setDataDepth] = useState<string | null>(null)
  // "One column per field" for a struct variable — on by default (user,
  // 2026-09-19); applies to the field factor (ColName) only.
  const [dataWide, setDataWide] = useState(true)
  const [imageFormat, setImageFormat] = useState('png')
  // What the backend says it can write. A short, ordered shortlist first —
  // these are the ones a paper needs — then whatever else is available.
  const allFormats = describe?.image_formats ?? ['png', 'svg', 'pdf', 'eps']
  const formatChoices = [
    ...['png', 'svg', 'pdf', 'eps'].filter(f => allFormats.includes(f)),
    ...allFormats.filter(f => !['png', 'svg', 'pdf', 'eps'].includes(f)),
  ]


  /** Copy the figure on screen to the clipboard as a high-dpi PNG
   *  (`copyImage.ts`). The PREVIEW, in light colours whatever the theme, at
   *  its export size when it has one, else at the size it is drawn in the
   *  pane. Instant, unlike a save — and so reduced when the preview is. */
  const copyFigure = useCallback(async () => {
    const shown = figures.find(f => f.index === figureIndex) ?? figures[0]
    if (!shown) return
    if (shown.custom_preview) {
      // A plot_ function's figure: its PNG as drawn, or its plotly figure.
      const custom = shown.custom_preview
      const blob = customPreviewBlob(custom)
      try {
        if (blob) {
          await navigator.clipboard.write([new ClipboardItem({ [blob.type]: blob })])
          setNotice('Copied the figure as PNG')
        } else if (custom.kind === 'plotly') {
          const size = customPreviewSize(custom) ?? { width: 800, height: 600 }
          const result = await copyFigurePng(custom.figure, size.width, size.height)
          setNotice(`Copied ${result.widthPx} × ${result.heightPx} px PNG`)
        }
      } catch (err) {
        setNotice(`Could not copy: ${(err as Error).message}`)
      }
      return
    }
    if (!shown.figure) return
    const fixed = (shown.figure.layout?.meta as { fixed_size?: [number, number] } | undefined)
      ?.fixed_size
    const drawn = document.querySelector(
      `[data-ps-figure="${shown.index}"] .js-plotly-plot`
    ) as HTMLElement | null
    const width = fixed?.[0] ?? drawn?.clientWidth ?? 800
    const height = fixed?.[1] ?? drawn?.clientHeight ?? 600
    setNotice('Copying…')
    try {
      const result = await copyFigurePng(shown.figure, width, height)
      console.info('[Plot Studio] copy_png', { index: shown.index, width, height, ...result })
      setNotice(
        `Copied ${result.widthPx} × ${result.heightPx} px PNG (${result.dpi} dpi)` +
          (shown.downsampled_from ? ' — reduced view, as previewed' : '')
      )
    } catch (err) {
      const message = (err as Error).message
      setNotice(`Could not copy: ${message}`)
      // To scistack.log: a clipboard refusal sends no RPC otherwise.
      callBackend('report_client_error', {
        where: 'Plot Studio copy_png',
        message,
        stack: (err as Error).stack ?? '',
        component_stack: `figure ${shown.index}, ${width} x ${height} px`,
      }).catch(() => undefined)
    }
  }, [figures, figureIndex])

  /** Save one figure, or the whole fan-out.
   *
   *  Two buttons because the costs differ, but ONE shape: both are background
   *  jobs that report progress (`saveJob` below). A save renders at FULL
   *  resolution with no downsampling, and that is minutes per figure — 1543 s
   *  for a two-figure fan-out (scistack.log 2026-09-11) — against a fixed 30 s
   *  transport timeout. "Save current figure" was a request/response on the
   *  theory that one figure is cheap; it timed out just as reliably as saving
   *  everything did, and left the backend rendering a file nobody was waiting
   *  for. No timeout value fixes that, so neither one asks for an answer. */
  const saveFigures = useCallback(
    async (scope: 'current' | 'all') => {
      if (!spec || !outputSpec) return
      setNotice('')
      // A fan-out asks for a FOLDER. Its N filenames are the figure labels —
      // not the user's to choose — so asking for a name only raised the
      // question "which of the thirty is that?". One figure is one file, and
      // there the name is the whole point.
      const savingAll = scope === 'all'
      const defaultName = `${title || 'figure'}.${imageFormat}`.replace(
        /[^\w.-]+/g,
        '_'
      )
      try {
        let path: string | null = null
        if (isVSCodeMode) {
          const picked = await callBackend(
            savingAll ? 'pick_save_folder' : 'pick_save_path',
            // Only the CHOSEN format, not every one available: the dialog's
            // filter would otherwise let the user pick a second, different
            // answer to a question the dropdown already asked, and the backend
            // honours the dropdown.
            savingAll ? {} : { defaultName, formats: [imageFormat] }
          )
          path = (picked as { path: string | null }).path
          if (!path) return  // dialog cancelled
        } else {
          path = window.prompt(
            savingAll
              ? `Folder to save ${figureCount} figures into:`
              : 'Save the figure as:',
            savingAll ? '' : defaultName
          )
          if (!path) return
        }

        // The id is OURS, and it is adopted before the request leaves. The job
        // starts on a thread the moment the backend receives it, so a quick
        // figure can report progress — or complete — while this promise is
        // still settling; a handler that learned the id only from the response
        // would drop those messages and leave the panel saving forever.
        const job = `ps-${Math.random().toString(36).slice(2, 10)}`
        saveJob.current = job
        saveKind.current = 'image'
        // Which size the file is written at, and why (Fit pane = the pane's).
        console.info('[Plot Studio] save_size', { previewMode, width: figWidth, height: figHeight })
        setSaving(true)
        setNotice(
          savingAll
            ? `Saving ${figureCount} figures at full resolution…`
            : 'Saving this figure at full resolution…'
        )
        await callBackend('plot_save_start', {
          spec: outputSpec,
          path,
          job_id: job,
          image_format: imageFormat,
          // null, not omitted: the backend reads "which figure" from this one
          // field, and `undefined` would be dropped by JSON. It also decides
          // whether `path` is a file or the folder to fill.
          figure_index: savingAll ? null : figureIndex,
          ...resolveParams,
        })
      } catch (err) {
        // The id was adopted before the request; a request that never reached
        // the backend has no job to report, so release the button here or it
        // stays disabled until the panel is reopened.
        saveJob.current = null
        setSaving(false)
        setNotice(`Could not save: ${(err as Error).message}`)
      }
    },
    [spec, outputSpec, previewMode, figWidth, figHeight, resolveParams, title, figureIndex, figureCount, imageFormat]
  )

  /** Save the plot's long table as CSV — the rows the figure is drawn from
   *  (`scistackplot.plot_data`), every figure of the fan-out in one file.
   *  `depth` is a key from `capabilities.data_export.depths`; null is the
   *  default (the plotted sample). Same job path and messages as an image
   *  save: the load alone can outlast the transport timeout. */
  const saveData = useCallback(
    async (depth: string | null, fieldsAsColumns: boolean) => {
      if (!spec) return
      setDataChooser(false)
      setNotice('')
      const defaultName = `${title || 'plot'}_data.csv`.replace(/[^\w.-]+/g, '_')
      try {
        let path: string | null = null
        if (isVSCodeMode) {
          const picked = await callBackend('pick_save_path', {
            defaultName,
            formats: ['csv'],
            filterName: 'CSV',
          })
          path = (picked as { path: string | null }).path
          if (!path) return  // dialog cancelled
        } else {
          path = window.prompt('Save the plot data as:', defaultName)
          if (!path) return
        }
        const job = `ps-${Math.random().toString(36).slice(2, 10)}`
        saveJob.current = job
        saveKind.current = 'data'
        setSaving(true)
        setNotice("Collecting the plot's data…")
        await callBackend('plot_save_start', {
          spec,
          path,
          job_id: job,
          what: 'data',
          // null, not omitted, for the same reason as figure_index above.
          depth,
          fields_as_columns: fieldsAsColumns,
          ...sourceParams,
        })
      } catch (err) {
        saveJob.current = null
        setSaving(false)
        setNotice(`Could not save the data: ${(err as Error).message}`)
      }
    },
    [spec, sourceParams, title]
  )

  const openDataSave = useCallback(() => {
    const info = capabilities?.data_export
    if (!info?.available) return
    // Nothing to ask — one depth and no struct fields to spread — so go
    // straight to the file dialog.
    if (info.depths.length <= 1 && !info.field_factor) {
      void saveData(info.default, true)
      return
    }
    setDataDepth(info.default)
    setDataChooser(open => !open)
  }, [capabilities, saveData])

  useBackendMessage(
    useCallback((msg) => {
      // Both transports, one handler: the WebSocket path delivers the dict as
      // sent (`type`, fields at the top level) and the JSON-RPC path wraps it
      // (`method` + `params`). Same normalization every other consumer does.
      const kind = (msg.type ?? msg.method) as string
      const params = (msg.params ?? msg) as Record<string, unknown>
      if (!kind?.startsWith('plot_save_')) return
      // Ignore another panel's job — two Plot Studio tabs can save at once.
      if (params.job_id !== saveJob.current) return

      if (kind === 'plot_save_progress') {
        // Two stages, reported apart, because they are not the same size:
        // resolving a figure at full resolution is minutes and writing it is
        // seconds. A panel that said "0 of 2 saved" for twelve minutes was
        // counting the cheap half and looked hung.
        if (saveKind.current === 'data') {
          setNotice(
            params.stage === 'resolving' ? "Collecting the plot's data…" : 'Writing the CSV…'
          )
          return
        }
        setNotice(
          params.stage === 'resolving'
            ? `Resolving figure ${params.done} of ${params.total} at full resolution…`
            : `Saving ${params.done} of ${params.total}…`
        )
      } else if (kind === 'plot_save_complete') {
        const files = (params.files ?? []) as string[]
        const directory = params.directory as string | undefined
        const elapsed = params.elapsed as number | undefined
        saveJob.current = null
        setSaving(false)
        // One file: name it. Many: name the FOLDER and the count — thirty full
        // paths sharing one directory is not a message anyone reads, and it
        // pushed the one fact that matters (it finished) off the end.
        const where =
          files.length === 1
            ? files[0]
            : `${files.length} files in ${directory ?? 'the chosen folder'}`
        const rows = params.rows as number | undefined
        setNotice(
          `Saved ${where}` +
            (typeof rows === 'number' ? ` — ${rows} row(s)` : '') +
            (elapsed ? ` in ${elapsed.toFixed(1)}s` : '')
        )
      } else if (kind === 'plot_save_failed') {
        saveJob.current = null
        setSaving(false)
        setNotice(`Could not save: ${params.error}`)
      }
    }, [])
  )

  const handleAddToPipeline = useCallback(() => {
    if (!spec || !outputSpec) return
    setNotice('Writing endpoint…')
    callBackend('plot_add_to_pipeline', { spec: outputSpec, ...(derivedBars.length ? { derived_bars: derivedBars } : {}) }, { stack: undo })
      .then(raw => {
        const result = raw as { ok?: boolean; error?: string; function_name?: string; file?: string }
        if (result.error) setNotice(`Could not add: ${result.error}`)
        else setNotice(`Added ${result.function_name} to ${result.file}. Wire it up on the canvas.`)
      })
      .catch(err => setNotice(`Could not add: ${(err as Error).message}`))
  }, [spec, outputSpec, undo, derivedBars])

  return {
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
  }
}

export type ExportState = ReturnType<typeof useExport>
