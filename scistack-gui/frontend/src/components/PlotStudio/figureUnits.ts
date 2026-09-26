/**
 * The unit the figure's Width and Height boxes are shown in.
 *
 * Only a display: the spec always stores inches (`StyleOptions.width/height`,
 * what matplotlib's figsize takes). Switching the unit converts what the
 * boxes show and writes nothing. Pixels are the SAVED raster's pixels, at
 * the dpi `plot_save` uses, so "1600 px" is the width of the PNG you get.
 *
 * React-free so `npm test` can pin the conversions.
 */

export type SizeUnit = 'in' | 'mm' | 'px'

export interface SizeUnitInfo {
  unit: SizeUnit
  label: string
  /** One ▲▼ click (user, 2026-09-26: 0.1 in / 1 mm / 10 px). */
  step: number
  /** Decimals the box shows. */
  decimals: number
}

export const SIZE_UNITS: SizeUnitInfo[] = [
  { unit: 'in', label: 'in', step: 0.1, decimals: 2 },
  { unit: 'mm', label: 'mm', step: 1, decimals: 1 },
  { unit: 'px', label: 'px', step: 10, decimals: 0 },
]

export const DEFAULT_SIZE_UNIT: SizeUnit = 'in'

export const MM_PER_IN = 25.4

/** Inches are stored to this many places: fine enough that a typed pixel or
 *  millimetre reads back as itself (1 px at 200 dpi is 0.005 in). */
const STORED_DECIMALS = 4

export function unitInfo(unit: SizeUnit): SizeUnitInfo {
  return SIZE_UNITS.find(u => u.unit === unit) ?? SIZE_UNITS[0]
}

function round(value: number, decimals: number): number {
  const scale = 10 ** decimals
  return Math.round(value * scale) / scale
}

/** A stored size (inches) as the box shows it in `unit`. */
export function fromInches(inches: number, unit: SizeUnit, dpi: number): number {
  const raw = unit === 'mm' ? inches * MM_PER_IN : unit === 'px' ? inches * dpi : inches
  return round(raw, unitInfo(unit).decimals)
}

/** A value typed or stepped in `unit`, as the inches the spec stores. */
export function toInches(value: number, unit: SizeUnit, dpi: number): number {
  const raw = unit === 'mm' ? value / MM_PER_IN : unit === 'px' ? value / dpi : value
  return round(raw, STORED_DECIMALS)
}

/** A remembered unit, or the default for anything unreadable. */
export function parseSizeUnit(raw: string | null | undefined): SizeUnit {
  return SIZE_UNITS.some(u => u.unit === raw) ? (raw as SizeUnit) : DEFAULT_SIZE_UNIT
}

/** Per viewer, in localStorage: the unit is a convenience, not part of the plot. */
export const SIZE_UNIT_STORAGE_KEY = 'scistack.plotStudio.sizeUnit'
