/**
 * VariantDagPopup — pick a variant on the pipeline graph.
 *
 * The same DAG the SciStack Pipeline canvas draws, in a modal, wired to a
 * PlotSpec instead of to execution state. Parameter nodes offer their recorded
 * levels as checkboxes; function nodes offer their recorded versions as a
 * dropdown defaulting to "latest".
 *
 * **Why the whole pipeline and not just the relevant part.** The induced
 * subgraph (ancestors of the measure that contribute >1 level) is smaller and
 * was the original design, but it is a graph the user has never seen. Drawing
 * the canvas they already know — with the nodes that cannot matter dimmed —
 * keeps "where am I" free, and makes the absence of a control informative:
 * a greyed node is telling you it does not distinguish these records.
 *
 * **Why a modal and not a tab.** Selection is a decision you come back from,
 * and the state being edited belongs to the panel underneath. Covering it is
 * the point: the popup is doing something different from the canvas of the same
 * shape sitting in another tab, and looking different is how a user knows.
 *
 * Node components with two meanings are reused (VariantSelectionContext
 * switches their mode) so this stays a mirror of the canvas as the canvas
 * changes, rather than a copy that slowly drifts from it. Node types that can
 * only ever be inert here — a Variable, a PathInput — are overridden in
 * `nodeTypes` instead, which keeps the variant-only rendering in this file and
 * leaves the canvas components untouched.
 *
 * **Axes bind to nodes by PORT, not by name** (`axisByNode`). An edge into a
 * function carries `targetHandle = "param__<the function's argument name>"`,
 * which is exactly `VariantAxis.param`; the node feeding that port is the node
 * that holds the axis, whatever it is called and whatever type it is. Matching
 * `axis.param` against a node's LABEL — the Parameter ENTITY's name — is a
 * different namespace and silently loses the axis the moment anyone renames a
 * Parameter or feeds a port from a glue node.
 */

import { useCallback, useEffect, useMemo, useState } from 'react'
import { Handle, Position, type Node } from '@xyflow/react'

import FunctionNode from '../DAG/FunctionNode'
import ParameterNode, { VariantParameterNode } from '../DAG/ParameterNode'
import { callBackend } from '../../api'
import {
  PickerDialog,
  pickerStyles as styles,
  usePipelineCanvas,
} from './DagPicker'
import {
  useVariantSelection,
  type FunctionVersion,
  type VariantAxis,
  type VariantSelectionValue,
} from '../../context/VariantSelectionContext'

/**
 * A nested pipeline, drawn but not enterable.
 *
 * The real `PipelineNode` reaches for `usePlanRun` and `useScope` so it can run
 * and descend — neither of which exists here (the Plot Studio tab mounts no
 * providers), and neither of which means anything in a selection dialog. Its
 * insides are a different scope; anything selectable in there reaches the user
 * through the "not on this canvas" list below the graph instead.
 */
function InertPipelineNode({ data }: { data: { label?: string } }) {
  return (
    <div style={styles.inertNode} title="A nested pipeline — open it on the canvas to see inside">
      <Handle type="target" position={Position.Left} />
      <div style={styles.inertNodeLabel}>{data.label ?? 'pipeline'}</div>
      <div style={styles.inertNodeHint}>nested pipeline</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

/**
 * A Variable node during the "which variable?" step of adding a variant.
 *
 * Its own component rather than a mode inside `VariableNode` for the reason the
 * whole popup is built around: the branch lives at the component boundary, so
 * the canvas keeps mounting execution state and the popup keeps mounting
 * selection state, and neither grows a conditional hook.
 *
 * A refused variable is drawn, not hidden. "FilteredEMG is here and RawEMG is
 * not" leaves the user to guess whether the missing one is unsupported, broken,
 * or simply not loaded; the reason on the node answers it in place. The reasons
 * are the backend's (`stackable_report`), so the dialog cannot invent a rule the
 * renderer does not apply.
 */
function PickableVariableNode({
  data,
}: {
  data: { label?: string; pickable?: boolean; refusal?: string; onPick?: () => void }
}) {
  const pickable = Boolean(data.pickable)
  return (
    <div
      style={{
        ...styles.pickNode,
        ...(pickable ? styles.pickNodeReady : styles.pickNodeRefused),
      }}
      onClick={pickable ? data.onPick : undefined}
      title={
        pickable
          ? 'Plot this variable — click to choose which of its variants'
          : `Cannot be plotted alongside: ${data.refusal ?? 'not compatible'}`
      }
    >
      <Handle type="target" position={Position.Left} />
      <div style={styles.pickNodeLabel}>{data.label ?? 'variable'}</div>
      <div style={styles.pickNodeHint}>
        {pickable ? 'click to plot' : (data.refusal ?? 'not compatible')}
      </div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

/**
 * Any non-parameter node that feeds a function port — a glue node, most often.
 *
 * Binding by PORT rather than by name means the node type stops mattering:
 * whatever is wired into `filterDelsys`'s `config` argument supplies that axis,
 * whether that is a Parameter, a glue node reshaping one, or something wired
 * there later. Before this, only ParameterNode was ever consulted, so a
 * glue-fed axis had no node to click at all.
 */
function VariantSupplierNode({
  data,
}: {
  data: { label?: string; variantAxisColumn?: string | null }
}) {
  const selection = useVariantSelection()
  if (!selection) return null
  return (
    <VariantParameterNode
      label={data.label ?? ''}
      axis={selection.axisForColumn(data.variantAxisColumn)}
      selection={selection}
    />
  )
}

/**
 * A node that can never be a variant axis: a Variable, a PathInput.
 *
 * Drawn inert rather than as its canvas self. These used to render verbatim —
 * undimmed and looking interactive — while every node that COULD hold an axis
 * was dimmed, which is exactly backwards: the informative thing about a dimmed
 * node is "I do not distinguish these records", and that is precisely what a
 * Variable node is.
 */
function InertVariantNode({ data, hint }: { data: { label?: string }; hint: string }) {
  return (
    <div style={styles.inertNode} title={`${data.label ?? ''} — ${hint}`}>
      <Handle type="target" position={Position.Left} />
      <div style={styles.inertNodeLabel}>{data.label ?? ''}</div>
      <div style={styles.inertNodeHint}>{hint}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

const InertVariableNode = (props: { data: { label?: string } }) => (
  <InertVariantNode {...props} hint="a variable, not a variant axis" />
)
const InertPathInputNode = (props: { data: { label?: string } }) => (
  <InertVariantNode {...props} hint="a file path, not a variant axis" />
)

const nodeTypes = {
  variableNode: InertVariableNode,
  functionNode: FunctionNode,
  glueNode: VariantSupplierNode,
  parameterNode: ParameterNode,
  pathInputNode: InertPathInputNode,
  pipelineNode: InertPipelineNode,
}

/** Step one of "+ Add variant": only Variable nodes do anything. */
const pickNodeTypes = { ...nodeTypes, variableNode: PickableVariableNode }


interface VariantGraph {
  axes: VariantAxis[]
  versions: Record<string, FunctionVersion[]>
  chain_functions: string[]
  /** Name of the per-row "newest at my own schema location" flag. The opening
   *  variant selects on it, and any explicit choice here has to clear it. */
  latest_column: string
  /** What a new row for this variable should open on — one variant, from
   *  `variants.default_selection`. Same rule the panel opens on, so a row added
   *  later and the row that was already there mean the same thing by "one
   *  variant". Empty for a source with no variant axes. */
  default_selection?: Record<string, unknown>
  /** `{axis column: canvas node id}` — which node on the pipeline canvas
   *  supplies each branch-param axis, bound by PORT in
   *  `plot_service.axis_node_bindings`. An axis absent from this mapping feeds
   *  no port on this canvas and falls into the list below the graph. */
  node_bindings?: Record<string, string>
}

interface Props {
  /** The measure being plotted — the axes are its. In `pick` mode this is only
   *  the DEFAULT offer; the user may choose another variable entirely. */
  variable: string
  /** Selection being edited, `{column: level | level[] | 'latest'}`. */
  selection: Record<string, unknown>
  /** Row label, editable here too so the popup is self-contained. Empty means
   *  "still following the selection" — `placeholder` is what that resolves to.
   *
   *  A row label is a PlotSpec idea. A second consumer (the Provenance panel)
   *  wants the same canvas to answer "which variant?" and has no rows, so it
   *  passes `showName={false}` and the field disappears — see `showName`. */
  name: string
  placeholder?: string
  /** False for a consumer with no row to name. The three chrome props below
   *  exist for the same reason: this component owns the *selection*, and the
   *  words around it belong to whoever is asking. Extracting the selection half
   *  from its PlotSpec wiring is exactly this — one component, a second
   *  consumer, no copy of the canvas. */
  showName?: boolean
  title?: string
  subtitle?: string
  applyLabel?: string
  /** Adding a NEW row rather than editing one: the popup opens on a variable
   *  pick first, and only then on that variable's variants. Two steps because
   *  they are two different questions — "what am I plotting" precedes "which
   *  version of it" — and the canvas is the right picture for both. */
  pick?: boolean
  /** Variables that may be picked in step one, besides `variable` itself. */
  pickable?: string[]
  /** Why each other variable may not be picked, from `stackable_report`. */
  refusals?: Record<string, string>
  onApply: (next: {
    selection: Record<string, unknown>
    name: string
    variable?: string
  }) => void
  onCancel: () => void
}

export default function VariantDagPopup({
  variable,
  selection: initial,
  name: initialName,
  placeholder,
  showName = true,
  title,
  subtitle,
  applyLabel = 'Apply',
  pick = false,
  pickable = [],
  refusals = {},
  onApply,
  onCancel,
}: Props) {
  // The pipeline canvas, independent of which variable is chosen — and shared
  // with the grouping picker, which draws the same graph to ask a different
  // question (see `DagPicker`).
  const { nodes, edges, loading, error: canvasError } = usePipelineCanvas()
  const [graph, setGraph] = useState<VariantGraph | null>(null)
  const [selection, setSelection] = useState<Record<string, unknown>>(initial)
  const [name, setName] = useState(initialName)
  const [axesError, setAxesError] = useState('')
  // null while step one is still open. In edit mode there is no step one, so it
  // is the variable being edited from the start and nothing else can set it.
  const [chosen, setChosen] = useState<string | null>(pick ? null : variable)
  const error = canvasError || axesError

  // --- the chosen variable's axes -----------------------------------------
  // Separate from the canvas load so step one can draw immediately, and so
  // picking a variable costs one RPC rather than re-fetching the pipeline.
  useEffect(() => {
    if (!chosen || nodes.length === 0) return
    let cancelled = false
    ;(async () => {
      try {
        const functions = nodes
          .filter(n => n.type === 'functionNode')
          .map(n => (n.data as { label: string }).label)
        const variantGraph = (await callBackend('plot_variant_graph', {
          variable: chosen,
          functions,
        })) as VariantGraph
        if (cancelled) return
        setGraph(variantGraph)
        // A NEW row opens on one variant, by the same rule the panel itself
        // opens on (`variants.default_selection`, sent with the graph). Left
        // empty it would mean "every variant of this variable", so clicking
        // "+ Add variant" would quietly add all of them to the figure.
        if (pick) setSelection(variantGraph.default_selection ?? {})
      } catch (err) {
        if (!cancelled) setAxesError((err as Error).message)
      }
    })()
    return () => { cancelled = true }
  }, [chosen, nodes, pick])

  // --- selection ----------------------------------------------------------
  const axes = graph?.axes ?? []

  const value: VariantSelectionValue = useMemo(() => {
    const byColumn = new Map(axes.map(a => [a.column, a]))

    /**
     * Drop the "current results" flag as soon as the user chooses anything.
     *
     * A variant opens on `{CodeIsLatest: true}` — a per-row flag, NOT an axis.
     * Leaving it in place while adding `Code:f = v1` asks for rows that are
     * simultaneously the newest and the old version, which is nothing at all:
     * the figure empties and the control looks broken. An explicit choice
     * supersedes the shortcut, which is also what the user means by making it.
     */
    const withoutLatestFlag = (current: Record<string, unknown>) => {
      const flag = graph?.latest_column
      if (!flag || !(flag in current)) return current
      const next = { ...current }
      delete next[flag]
      return next
    }

    return {
      selection,
      axisForColumn: (column: string | null | undefined) =>
        (column && byColumn.get(column)) || null,
      axisForFunction: (label: string) =>
        axes.find(a => a.kind === 'code' && a.function === label) ?? null,
      runAxisForFunction: (label: string) =>
        axes.find(a => a.kind === 'run' && a.function === label) ?? null,
      versionsFor: (label: string) => graph?.versions?.[label] ?? [],
      isLevelSelected: (column, level) => {
        const current = selection[column]
        // An axis nobody has touched selects everything: an untouched control
        // must never quietly filter data away.
        if (current === undefined) return true
        if (Array.isArray(current)) return current.map(String).includes(level)
        return String(current) === level
      },
      versionFor: column => {
        const current = selection[column]
        if (current === undefined) return 'latest'
        return Array.isArray(current) ? String(current[0] ?? 'latest') : String(current)
      },
      toggleLevel: (column, level) => {
        setSelection(previous => {
          const prev = withoutLatestFlag(previous)
          const axis = byColumn.get(column)
          const all = axis?.levels ?? []
          const current = prev[column]
          const chosen = current === undefined
            ? [...all]
            : Array.isArray(current)
              ? current.map(String)
              : [String(current)]
          const next = chosen.includes(level)
            ? chosen.filter(l => l !== level)
            : [...chosen, level]
          // Declared level order, not click order, so the legend stays stable.
          const ordered = all.filter(l => next.includes(l))
          const updated = { ...prev }
          if (ordered.length === all.length) {
            // Everything selected is the same as no constraint — and saying it
            // that way keeps the auto label ("all variants") honest.
            delete updated[column]
          } else {
            updated[column] = ordered
          }
          return updated
        })
      },
      setVersion: (column, version) => {
        setSelection(prev => ({ ...withoutLatestFlag(prev), [column]: version }))
      },
    }
  }, [axes, graph, selection])

  /** Step one is still open: no variable chosen, so there are no axes to show. */
  const choosing = pick && chosen === null

  /**
   * Which node supplies which axis, inverted from the backend's mapping.
   *
   * The binding itself is `plot_service.axis_node_bindings`, deliberately NOT
   * computed here: it is a rule about what scidb's namespacing means (an edge's
   * `targetHandle` carries the function's own argument name, which is exactly
   * `VariantAxis.param`), and a rule written in TSX has no test. This popup used
   * to derive it by comparing `axis.param` against a node's LABEL — the
   * Parameter ENTITY's name, a different namespace — and silently lost every
   * axis the moment a Parameter was renamed or a port was fed from a glue node.
   */
  const axisByNode = useMemo(() => {
    const bound = new Map<string, string>()
    for (const [column, nodeId] of Object.entries(graph?.node_bindings ?? {})) {
      if (nodeId) bound.set(nodeId, column)
    }
    return bound
  }, [graph])

  // Each node told which axis it supplies, so the node components only read
  // back what was bound here rather than re-deriving it from names.
  const boundNodes = useMemo(
    () =>
      nodes.map(node => ({
        ...node,
        data: { ...node.data, variantAxisColumn: axisByNode.get(node.id) ?? null },
      })),
    [nodes, axisByNode]
  )

  // Axes that bound to no node at all. A nested pipeline is one reason — the
  // popup opens at the root scope — but no longer the only one it can assume,
  // so the list says what it knows rather than asserting a cause.
  const unmapped = useMemo(() => {
    const placed = new Set(axisByNode.values())
    const functions = new Set(
      nodes
        .filter(n => n.type === 'functionNode')
        .map(n => (n.data as { label?: string }).label ?? '')
    )
    return axes.filter(axis =>
      axis.kind === 'code' || axis.kind === 'run'
        ? !functions.has(axis.function ?? '')
        : !placed.has(axis.column)
    )
  }, [axes, axisByNode, nodes])

  // Step one only: which variables may be clicked, and why the rest may not.
  // Every variable node is drawn either way — a refused one says its reason in
  // place rather than being quietly inert.
  const pickingNodes = useMemo(() => {
    if (!choosing) return boundNodes
    const offered = new Set([variable, ...pickable])
    return boundNodes.map(node =>
      node.type === 'variableNode'
        ? {
            ...node,
            data: {
              ...node.data,
              pickable: offered.has((node.data as { label?: string }).label ?? ''),
              refusal: refusals[(node.data as { label?: string }).label ?? ''],
              onPick: () => setChosen((node.data as { label?: string }).label ?? null),
            },
          }
        : node
    )
  }, [choosing, boundNodes, variable, pickable, refusals])

  return (
    <PickerDialog
      title={choosing ? 'Which variable?' : (title ?? 'Select variant')}
      subtitle={
        choosing
          ? 'Click the variable to plot. Anything that cannot be drawn alongside this figure says why.'
          : (subtitle ??
            'Checkboxes and versions here choose what the FIGURE shows. Nothing on this graph changes what a run does.')
      }
      headerExtra={
        !choosing && (
          <>
            {/* The chosen variable, once there is one. A row names a variable
                AND a variant, and after step one the second question is
                meaningless without the answer to the first on screen. */}
            <span style={styles.chosenVariable} title="The variable this variant plots">
              {chosen}
            </span>
            {showName && (
              <input
                value={name}
                onChange={e => setName(e.target.value)}
                placeholder={placeholder || 'variant name'}
                style={localStyles.nameInput}
                title="What this variant is called in the figure"
              />
            )}
          </>
        )
      }
      nodes={pickingNodes}
      edges={edges}
      nodeTypes={choosing ? pickNodeTypes : nodeTypes}
      loading={loading}
      error={error}
      selection={value}
      below={
        !choosing && unmapped.length > 0 ? (
          <div style={localStyles.unmapped}>
            <div style={localStyles.unmappedTitle}>No node on this canvas</div>
            {unmapped.map(axis => (
              <div key={axis.column} style={localStyles.unmappedRow}>
                <span style={localStyles.unmappedName}>{axis.column}</span>
                <div style={localStyles.unmappedLevels}>
                  {axis.levels.map(level => (
                    <label key={level} style={localStyles.unmappedLevel}>
                      <input
                        type="checkbox"
                        checked={value.isLevelSelected(axis.column, level)}
                        onChange={() => value.toggleLevel(axis.column, level)}
                      />
                      <span>{level}</span>
                    </label>
                  ))}
                </div>
              </div>
            ))}
          </div>
        ) : null
      }
      footer={
        <>
          {/* Step one back to nothing: leaving the popup open on the variable
              pick is cheaper than cancelling and re-opening when the wrong
              node was clicked. Only in `pick` mode — editing an existing row
              has no step to go back to, and its variable is the row's. */}
          {pick && !choosing && (
            <button
              type="button"
              style={styles.button}
              onClick={() => {
                setChosen(null)
                setGraph(null)
                setSelection({})
              }}
            >
              ← Variable
            </button>
          )}
          {/* Nothing to apply until a variable is chosen: a row with no
              variable and no selection is the inert row that clicking "+" used
              to create, and the whole point of this dialog is that "+" no
              longer creates one. */}
          {!choosing && (
            <button
              type="button"
              style={styles.primaryButton}
              onClick={() =>
                onApply({ selection, name, variable: chosen ?? undefined })
              }
            >
              {applyLabel}
            </button>
          )}
        </>
      }
      onCancel={onCancel}
    />
  )
}

/** Only what the shared shell does not provide (`DagPicker.pickerStyles`). */
const localStyles: Record<string, React.CSSProperties> = {
  nameInput: {
    background: 'var(--ps-control)', color: 'var(--ps-text-body)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 4, fontSize: 12, padding: '3px 6px', minWidth: 200,
  },
  unmapped: {
    borderTop: '1px solid var(--ps-border)', padding: '8px 14px', maxHeight: 120,
    overflowY: 'auto', flexShrink: 0,
  },
  unmappedTitle: { fontSize: 10, color: 'var(--ps-caution)', marginBottom: 4 },
  unmappedRow: { display: 'flex', alignItems: 'center', gap: 10, marginBottom: 3 },
  unmappedName: { fontSize: 11, fontFamily: 'monospace', color: 'var(--ps-text-secondary)' },
  unmappedLevels: { display: 'flex', gap: 8, flexWrap: 'wrap' },
  unmappedLevel: {
    display: 'flex', alignItems: 'center', gap: 3, fontSize: 11, color: 'var(--ps-text-body)',
  },
}
