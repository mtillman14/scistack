/**
 * VariantSelectionContext — what a DAG node's controls mean *right now*.
 *
 * The pipeline canvas and Plot Studio's variant popup draw the same nodes with
 * the same widgets, and they mean two completely different things:
 *
 *   - On the canvas, a ParameterNode checkbox is EXECUTION state. Unchecking a
 *     value excludes it from future for_each fan-outs (pipeline_store's
 *     hide_constant_value, and execution_service's filtering).
 *   - In the popup, the same checkbox is DISPLAY state. It selects which
 *     already-computed records a figure draws, and must never change what a run
 *     would do.
 *
 * Binding the popup to the canvas's handlers would make looking at a plot
 * quietly rewrite the run configuration — far worse than any duplicated widget.
 * So the mode is carried here rather than in node data: when this context is
 * present a node is in *selection* mode and the execution calls are unreachable
 * from it; when it is absent (the canvas, always) nothing about today's
 * behaviour changes.
 *
 * See docs/claude/variant-selection.md §5 and .claude/plan-plot-variant-rows.md.
 */

import { createContext, useContext } from 'react'

/** One variant axis, as `plot_variant_graph` describes it. */
export interface VariantAxis {
  column: string
  kind: 'code' | 'param'
  function: string | null
  param: string | null
  levels: string[]
}

export interface FunctionVersion {
  version: string
  function_hash: string
  first_saved: string | null
}

export interface VariantSelectionValue {
  /** The selection being edited: `{column: level | level[] | 'latest'}`. */
  selection: Record<string, unknown>
  /** The axis a node supplies, by the column the popup already bound to it.
   *
   *  Binding happens ONCE, in the popup, from the pipeline EDGES — an edge into
   *  a function carries `targetHandle = "param__<the function's argument
   *  name>"`, which is exactly `VariantAxis.param`. The node then only has to
   *  read back what it was given.
   *
   *  This replaced `axisForParameter(label, consumers)`, which compared
   *  `axis.param` against the node's LABEL. Those are two different namespaces:
   *  `axis.param` is the producing function's ARGUMENT name (scidb's `fn.param`
   *  branch-param key) while a Parameter node is labelled with the Parameter
   *  ENTITY's name. They coincide only while nobody has renamed a Parameter or
   *  fed a function port from a glue node — after which the axis silently fell
   *  out of the graph, its node dimmed to "not a variant here", and the axis
   *  was reported as living in a nested pipeline. Measured on a real project
   *  2026-09-11; see .claude/plan-plot-studio-variant-axis-fixes.md Finding 2.
   *
   *  Binding by port also means the node TYPE stops mattering: whatever feeds
   *  the port holds the axis, whether that is a Parameter, a glue node, or
   *  anything else wired there later. */
  axisForColumn: (column: string | null | undefined) => VariantAxis | null
  /** Code axes bind by function NAME, which is what a function node is labelled
   *  with — one namespace, no port involved. */
  axisForFunction: (functionLabel: string) => VariantAxis | null
  /** Every recorded version of a function, newest last. Empty = never run. */
  versionsFor: (functionLabel: string) => FunctionVersion[]
  /** Check/uncheck one level of a parameter axis. */
  toggleLevel: (column: string, level: string) => void
  /** Pick a version (or 'latest') for a function axis. */
  setVersion: (column: string, version: string) => void
  /** True when a level is currently selected. An axis with nothing chosen means
   *  "every level" — an unedited axis must not filter anything away. */
  isLevelSelected: (column: string, level: string) => boolean
  /** The version chosen for a function, defaulting to 'latest'. */
  versionFor: (column: string) => string
}

const VariantSelectionContext = createContext<VariantSelectionValue | null>(null)

export const VariantSelectionProvider = VariantSelectionContext.Provider

/** Non-null only inside the variant popup. Nodes branch on it. */
export function useVariantSelection(): VariantSelectionValue | null {
  return useContext(VariantSelectionContext)
}
