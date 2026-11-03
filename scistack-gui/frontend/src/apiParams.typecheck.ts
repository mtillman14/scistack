/**
 * Compile-time checks of `callBackend`'s params (P5 stage 5c). Never run and
 * never bundled: `tsc` (the first step of `npm run build`) compiles it, and
 * each `@ts-expect-error` FAILS the build if its line stops being an error,
 * i.e. if a method's params stop being checked against the backend's
 * generated request model (`ApiParams`, from scistack_gui.api.wire_schema).
 */

import type { BackendArgs } from './api'

/** A real request type-checks. */
export const resolve: BackendArgs<'plot_resolve'> = [{ spec: { measures: ['Force'], kind: 'bar' } }]

/** A method whose request model has only optional fields may omit params. */
export const noParams: BackendArgs<'get_glue_columns'> = []

/** A method with no request model at all takes no typed params. */
export const bodiless: BackendArgs<'history_status'> = []

/** A method outside the table takes an untyped object, as before. */
export const untyped: BackendArgs<string> = [{ anything: 1 }]

// @ts-expect-error a plot spec needs its measures (PlotSpec.from_dict requires them)
export const noMeasures: BackendArgs<'plot_resolve'> = [{ spec: {} }]

// @ts-expect-error the kind is the backend's PlotKind vocabulary
export const badKind: BackendArgs<'plot_resolve'> = [{ spec: { measures: ['Force'], kind: 'pie' } }]

// @ts-expect-error a misspelled request field
export const typo: BackendArgs<'stats_compute'> = [{ spek: {} }]

// @ts-expect-error stats_compute takes a TABLE's spec, not a figure's
export const wrongSpec: BackendArgs<'stats_compute'> = [{ spec: { measures: ['Force'] } }]

// @ts-expect-error a method with required params cannot omit them
export const missing: BackendArgs<'plot_resolve'> = []
