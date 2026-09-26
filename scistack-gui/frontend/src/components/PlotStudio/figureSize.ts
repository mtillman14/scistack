/**
 * Figure size — the TypeScript half of `scistackplot/figsize.py`.
 *
 * The presets themselves come from the backend (`describe.figure_presets`), so
 * there is no second list here to drift. What IS duplicated is the arithmetic:
 * "height for this width at this ratio" and "which preset is this size".
 * Those must run on every keystroke, and the round trip through a resolve is
 * 180 ms plus a render — a dropdown that snapped back after the fact would be
 * worse than no dropdown. The tolerance is the same number as
 * `figsize.RATIO_TOLERANCE`, and `figureSize.test.ts` pins the same cases as
 * `test_figsize.py` so the two cannot disagree about what 8 x 6 is called.
 */

export interface AspectPreset {
  name: string
  /** width / height; null only for `custom`. */
  ratio: number | null
  label: string
  hint: string
}

export const CUSTOM_ASPECT = 'custom'

/** The default the spec opens with, for a describe that shipped no presets. */
export const FALLBACK_PRESETS: AspectPreset[] = [
  { name: '4:3', ratio: 4 / 3, label: '4:3', hint: '' },
  { name: '16:9', ratio: 16 / 9, label: '16:9', hint: '' },
  { name: '1:1', ratio: 1, label: '1:1', hint: '' },
  { name: CUSTOM_ASPECT, ratio: null, label: 'Custom', hint: '' },
]

/** Same value as `scistackplot.figsize.RATIO_TOLERANCE`. */
export const RATIO_TOLERANCE = 0.01

/** Height in inches for `width` at the named preset, to two decimals — the
 *  same rounding as the Python side, so a size written here reads back as the
 *  same preset there. Returns `fallback` for custom or an unknown name. */
export function heightFor(
  width: number,
  name: string,
  presets: AspectPreset[],
  fallback: number
): number {
  const ratio = presets.find(p => p.name === name)?.ratio ?? null
  if (ratio === null || !(width > 0)) return fallback
  return Math.round((width / ratio) * 100) / 100
}

/** Which preset `width x height` is, or `custom`. */
export function aspectName(width: number, height: number, presets: AspectPreset[]): string {
  if (!(width > 0) || !(height > 0)) return CUSTOM_ASPECT
  const ratio = width / height
  for (const preset of presets) {
    if (preset.ratio !== null && Math.abs(preset.ratio - ratio) <= RATIO_TOLERANCE) {
      return preset.name
    }
  }
  return CUSTOM_ASPECT
}

/** `1600 x 900 px` — exactly what the saved raster will be: the file is the
 *  requested size (scistackplot.write_figure; no whitespace trim since
 *  2026-09-23). */
export function pixelReadout(width: number, height: number, dpi: number): string {
  return `${Math.round(width * dpi)} × ${Math.round(height * dpi)} px`
}

/**
 * The figure size after the user types a width OR a height. The one owner of
 * the "Lock aspect" rule (the toolbar checkbox):
 *
 * - unlocked: only the edited side moves; the ratio becomes whatever that
 *   makes (the dropdown then reads it, often Custom);
 * - locked: the other side follows the CURRENT ratio. At a preset the
 *   preset's exact ratio is used, so repeated edits cannot drift off it
 *   through two-decimal rounding; at a custom size, the current w / h.
 *
 * Picking a preset in the dropdown is not an edit here: it always keeps the
 * width and moves the height (`heightFor`), locked or not.
 */
export function resizeFigure(
  current: { width: number; height: number },
  edit: { width: number } | { height: number },
  locked: boolean,
  presets: AspectPreset[]
): { width: number; height: number } {
  const next = { ...current, ...edit }
  const valid = current.width > 0 && current.height > 0
  if (!locked || !valid) return next
  const preset = presets.find(p => p.name === aspectName(current.width, current.height, presets))
  const ratio = preset?.ratio ?? current.width / current.height
  const round2 = (x: number) => Math.round(x * 100) / 100
  return 'width' in edit
    ? { width: edit.width, height: round2(edit.width / ratio) }
    : { width: round2(edit.height * ratio), height: edit.height }
}

/** Per viewer, in localStorage: locking is an editing mode, not part of the plot. */
export const ASPECT_LOCK_STORAGE_KEY = 'scistack.plotStudio.aspectLocked'

/** A remembered lock, locked for anything unreadable (the old default behaviour
 *  at a preset: the height followed the width). */
export function parseAspectLocked(raw: string | null | undefined): boolean {
  return raw !== 'false'
}
