/**
 * GroupingDagPopup — choose what stratifies a figure, on the pipeline graph.
 *
 * Two steps, on the same canvas, because they are two questions:
 *
 *  1. **Which variable?** Click a Variable node. One that cannot group this
 *     figure is drawn with its reason rather than omitted — a node that is
 *     simply absent is indistinguishable from a broken dialog.
 *  2. **Which columns, and which variant?** The canvas becomes the VARIANT
 *     canvas for the chosen grouping variable (parameter checkboxes, function
 *     version dropdowns, opening on latest), and the sidebar lists that
 *     variable's columns as checkboxes.
 *
 * Step two is the variant picker's machinery reused wholesale —
 * `VariantSelectionProvider` and the same node components — because it is
 * literally the same question asked about a different variable. What is new is
 * the sidebar.
 *
 * **Why the columns are fetched here and not sent with `describe`.** Listing
 * them means one `SELECT DISTINCT` per column of every wide variable at or
 * above the measure's level. On the user's project that was ~8 s added to every
 * panel open (scistack.log 2026-09-15) by two EMG variables that offer no
 * groupings at all. `plot_grouping_columns` asks for one variable, when the
 * user opens it.
 *
 * **This popup DOES call the backend**, unlike the variant popup's node
 * components — it has to, to fetch those columns. The property that matters is
 * narrower and is asserted in `test_plot_service.py`: it calls only READ RPCs,
 * so looking at a figure still cannot change what a run does.
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
  INERT_VARIANT_SELECTION,
  useVariantSelection,
  type FunctionVersion,
  type VariantAxis,
  type VariantSelectionValue,
} from '../../context/VariantSelectionContext'

/** One grouping the backend offers: a variable, or one column of one. */
export interface GroupingOffer {
  variable: string
  column: string | null
  label: string
  name: string
  levels: string[]
  level_count: number
}

/** A variable node's answer to "can I group by this?" — see `plot_grouping_graph`. */
export interface GroupingCandidate {
  variable: string
  kind: 'categorical' | 'numeric' | 'columns'
  label: string
  column_count: number
  offer?: GroupingOffer
  /** A column of the PLOTTED variable itself (`Gait.Side`): each row carries
   *  its own label, so there is no variant to choose (`FactorVariable.
   *  is_own_column`, decided by the backend and only read here). */
  own?: boolean
}

/** What the picked variables are for: Plot Studio's grouping (text labels)
 *  or StatStudio's model-formula covariates (numbers; the backend's mirror
 *  offer, `purpose` on plot_grouping_graph / plot_grouping_columns). */
export type PickPurpose = 'group' | 'covariate'

interface ColumnReport {
  offered: GroupingOffer[]
  rejected: Record<string, string>
}

interface VariantGraph {
  axes: VariantAxis[]
  versions: Record<string, FunctionVersion[]>
  latest_column?: string | null
  default_selection?: Record<string, unknown>
  node_bindings?: Record<string, string>
}

/** A Variable node in step one: clickable when it can group, explained when not. */
function PickableGroupingNode({
  data,
}: {
  data: {
    label?: string
    candidate?: GroupingCandidate
    refusal?: string
    onPick?: () => void
    purpose?: PickPurpose
  }
}) {
  const candidate = data.candidate
  const covariate = data.purpose === 'covariate'
  const hint = candidate
    ? candidate.kind === 'columns'
      ? candidate.own
        ? `${candidate.column_count} column(s) of the plotted data — click to choose`
        : `${candidate.column_count} column(s) — click to choose`
      : covariate
        ? 'numeric — click to use'
        : `${candidate.offer?.level_count ?? 0} level(s) — click to use`
    : (data.refusal ?? (covariate ? 'cannot be a covariate' : 'cannot group this figure'))
  return (
    <div
      style={{
        ...styles.pickNode,
        ...(candidate ? styles.pickNodeReady : styles.pickNodeRefused),
      }}
      onClick={candidate ? data.onPick : undefined}
      title={
        candidate
          ? (covariate ? 'Use this variable as a covariate' : 'Group this figure by this variable')
          : `${covariate ? 'Cannot be a covariate' : 'Cannot group this figure'}: ${data.refusal ?? 'not eligible'}`
      }
    >
      <Handle type="target" position={Position.Left} />
      <div style={styles.pickNodeLabel}>{data.label ?? 'variable'}</div>
      <div style={styles.pickNodeHint}>{hint}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

/** Any node that supplies a variant axis of the chosen grouping variable. */
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

function InertNode({ data, hint }: { data: { label?: string }; hint: string }) {
  return (
    <div style={styles.inertNode} title={`${data.label ?? ''} — ${hint}`}>
      <Handle type="target" position={Position.Left} />
      <div style={styles.inertNodeLabel}>{data.label ?? ''}</div>
      <div style={styles.inertNodeHint}>{hint}</div>
      <Handle type="source" position={Position.Right} />
    </div>
  )
}

const InertVariable = (p: { data: { label?: string } }) => (
  <InertNode {...p} hint="a variable, not a variant axis" />
)
const InertPathInput = (p: { data: { label?: string } }) => (
  <InertNode {...p} hint="a file path, not a variant axis" />
)
const InertPipeline = (p: { data: { label?: string } }) => (
  <InertNode {...p} hint="nested pipeline" />
)

/** Step two: the chosen variable's variants, exactly as the variant picker. */
const variantNodeTypes = {
  variableNode: InertVariable,
  functionNode: FunctionNode,
  glueNode: VariantSupplierNode,
  parameterNode: ParameterNode,
  pathInputNode: InertPathInput,
  pipelineNode: InertPipeline,
}

/** Step one: only Variable nodes do anything. */
const pickNodeTypes = { ...variantNodeTypes, variableNode: PickableGroupingNode }

interface Props {
  /** The figure's measure — what is being grouped. */
  measure: string
  /** Which variables may group it, and why the rest may not. */
  candidates: GroupingCandidate[]
  refusals: Record<string, string>
  /** Groupings already on the spec, so their checkboxes open ticked. */
  selected: { variable: string; column: string | null }[]
  onApply: (
    groups: { variable: string; column: string | null; variant: Record<string, unknown> }[]
  ) => void
  onCancel: () => void
  sourceParams: Record<string, unknown>
  /** Default 'group'. 'covariate': the same two steps, offering numeric
   *  variables and columns (StatStudio's Add covariate…). */
  purpose?: PickPurpose
}

export default function GroupingDagPopup({
  measure,
  candidates,
  refusals,
  selected,
  onApply,
  onCancel,
  sourceParams,
  purpose = 'group',
}: Props) {
  const covariate = purpose === 'covariate'
  const { nodes, edges, loading, error: canvasError } = usePipelineCanvas()
  const [chosen, setChosen] = useState<GroupingCandidate | null>(null)
  const [columns, setColumns] = useState<ColumnReport | null>(null)
  const [graph, setGraph] = useState<VariantGraph | null>(null)
  const [variant, setVariant] = useState<Record<string, unknown>>({})
  const [error, setError] = useState('')
  // Accumulated across variables in one visit: picking a demographics column
  // and a per-session Condition is one trip, not two.
  const [picked, setPicked] = useState<
    Record<string, { variable: string; column: string | null; variant: Record<string, unknown> }>
  >(() =>
    Object.fromEntries(
      selected.map(g => [
        `${g.variable}\u0000${g.column ?? ''}`,
        { ...g, variant: {} as Record<string, unknown> },
      ])
    )
  )

  const byVariable = useMemo(
    () => new Map(candidates.map(c => [c.variable, c])),
    [candidates]
  )

  // --- step two's data: the chosen variable's columns and its variants -----
  useEffect(() => {
    if (!chosen) return
    let cancelled = false
    ;(async () => {
      try {
        const functions = nodes
          .filter(n => n.type === 'functionNode')
          .map(n => (n.data as { label: string }).label)
        // Columns only for a wide table; a single-column variable IS the
        // grouping and has nothing to choose between.
        const columnsPromise =
          chosen.kind === 'columns'
            ? (callBackend('plot_grouping_columns', {
                variable: measure,
                group_variable: chosen.variable,
                purpose,
                ...sourceParams,
              }) as Promise<ColumnReport>)
            : Promise.resolve(null)
        // The same RPC the variant picker uses, over the GROUPING variable:
        // "which version of the sheet" is the same question as "which version
        // of the measure", asked about something else.
        // Not for the plotted variable's own columns: the label comes from the
        // very row it groups, so the measure's variant already chose it and a
        // second pin could only disagree (the backend ignores one, with a WARN).
        const graphPromise: Promise<VariantGraph | null> = chosen.own
          ? Promise.resolve(null)
          : (callBackend('plot_variant_graph', {
              variable: chosen.variable,
              functions,
              ...sourceParams,
            }) as Promise<VariantGraph>)

        const [report, variantGraph] = await Promise.all([
          columnsPromise,
          graphPromise,
        ])
        if (cancelled) return
        setColumns(report)
        setGraph(variantGraph)
        // Opens on LATEST, by the same rule the Variants section opens on —
        // `variants.default_selection`, sent with the graph. Left empty, every
        // variant of the sheet would contribute and the join would take
        // whichever record came first.
        setVariant(variantGraph?.default_selection ?? {})
      } catch (err) {
        if (!cancelled) setError((err as Error).message)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [chosen, nodes, measure, sourceParams, purpose])

  // --- the variant half, identical in meaning to the variant picker's ------
  const axes = graph?.axes ?? []
  const selectionValue: VariantSelectionValue = useMemo(() => {
    const byColumn = new Map(axes.map(a => [a.column, a]))
    const withoutLatestFlag = (current: Record<string, unknown>) => {
      const flag = graph?.latest_column
      if (!flag || !(flag in current)) return current
      const next = { ...current }
      delete next[flag]
      return next
    }
    return {
      selection: variant,
      axisForColumn: (column: string | null | undefined) =>
        (column && byColumn.get(column)) || null,
      axisForFunction: (label: string) =>
        axes.find(a => a.kind === 'code' && a.function === label) ?? null,
      runAxisForFunction: (label: string) =>
        axes.find(a => a.kind === 'run' && a.function === label) ?? null,
      versionsFor: (label: string) => graph?.versions?.[label] ?? [],
      isLevelSelected: (column, level) => {
        const current = variant[column]
        if (current === undefined) return true
        if (Array.isArray(current)) return current.map(String).includes(level)
        return String(current) === level
      },
      versionFor: column => {
        const current = variant[column]
        if (current === undefined) return 'latest'
        return Array.isArray(current) ? String(current[0] ?? 'latest') : String(current)
      },
      toggleLevel: (column, level) => {
        setVariant(previous => {
          const prev = withoutLatestFlag(previous)
          const all = byColumn.get(column)?.levels ?? []
          const current = prev[column]
          const chosenLevels =
            current === undefined
              ? [...all]
              : Array.isArray(current)
                ? current.map(String)
                : [String(current)]
          const next = chosenLevels.includes(level)
            ? chosenLevels.filter(l => l !== level)
            : [...chosenLevels, level]
          const ordered = all.filter(l => next.includes(l))
          const updated = { ...prev }
          if (ordered.length === all.length) delete updated[column]
          else updated[column] = ordered
          return updated
        })
      },
      setVersion: (column, version) => {
        setVariant(prev => ({ ...withoutLatestFlag(prev), [column]: version }))
      },
    }
  }, [axes, graph, variant])

  const axisByNode = useMemo(() => {
    const bound = new Map<string, string>()
    for (const [column, nodeId] of Object.entries(graph?.node_bindings ?? {})) {
      if (nodeId) bound.set(nodeId, column)
    }
    return bound
  }, [graph])

  const choosing = chosen === null

  const decorated = useMemo(() => {
    if (choosing) {
      return nodes.map(node =>
        node.type === 'variableNode'
          ? {
              ...node,
              data: {
                ...node.data,
                candidate: byVariable.get(
                  (node.data as { label?: string }).label ?? ''
                ),
                refusal: refusals[(node.data as { label?: string }).label ?? ''],
                purpose,
                onPick: () => {
                  const found = byVariable.get(
                    (node.data as { label?: string }).label ?? ''
                  )
                  if (found) setChosen(found)
                },
              },
            }
          : node
      )
    }
    return nodes.map(node => ({
      ...node,
      data: { ...node.data, variantAxisColumn: axisByNode.get(node.id) ?? null },
    }))
  }, [choosing, nodes, byVariable, refusals, axisByNode, purpose])

  // --- the sidebar's checkboxes -------------------------------------------
  const key = (variable: string, column: string | null) =>
    `${variable}\u0000${column ?? ''}`

  const toggle = useCallback(
    (offer: GroupingOffer, on: boolean) => {
      setPicked(current => {
        const next = { ...current }
        const id = key(offer.variable, offer.column)
        if (on) {
          next[id] = { variable: offer.variable, column: offer.column, variant }
        } else {
          delete next[id]
        }
        return next
      })
    },
    [variant]
  )

  // The variant is edited AFTER the columns are ticked as often as before, so
  // it is written through to everything picked from this variable rather than
  // captured once at tick time — otherwise changing the version would silently
  // apply to nothing.
  useEffect(() => {
    if (!chosen) return
    setPicked(current => {
      let changed = false
      const next = { ...current }
      for (const [id, group] of Object.entries(current)) {
        if (group.variable === chosen.variable && group.variant !== variant) {
          next[id] = { ...group, variant }
          changed = true
        }
      }
      return changed ? next : current
    })
  }, [variant, chosen])

  const offers: GroupingOffer[] = useMemo(() => {
    if (!chosen) return []
    if (chosen.kind !== 'columns') return chosen.offer ? [chosen.offer] : []
    return columns?.offered ?? []
  }, [chosen, columns])

  const sidebar = choosing ? null : (
    <>
      <div style={local.sidebarTitle}>{chosen?.label}</div>
      {chosen?.kind === 'columns' && !columns && (
        <div style={styles.note}>Reading its columns…</div>
      )}
      {offers.map(offer => (
        <label key={offer.label} style={local.columnRow}>
          <input
            type="checkbox"
            checked={Boolean(picked[key(offer.variable, offer.column)])}
            onChange={e => toggle(offer, e.target.checked)}
            style={{ marginRight: 6 }}
          />
          <span style={local.columnName}>{offer.column ?? offer.variable}</span>
          {!covariate && <span style={local.columnLevels}>{offer.level_count}</span>}
        </label>
      ))}
      {/* Refused columns in place. A column the user can see in their own
          spreadsheet and not in this list is otherwise a bug to them. */}
      {Object.entries(columns?.rejected ?? {}).map(([label, reason]) => (
        <div key={label} style={local.refused} title={reason}>
          {label.split('.').pop()} — {reason}
        </div>
      ))}
      {chosen?.kind === 'columns' && columns && offers.length === 0 && (
        <div style={styles.note}>
          {covariate ? 'No column of this variable is numeric.' : 'No column of this variable can group a figure.'}
        </div>
      )}
    </>
  )

  const count = Object.keys(picked).length

  return (
    <PickerDialog
      title={
        choosing
          ? covariate ? 'Which variable is the covariate?' : 'Group by which variable?'
          : 'Which columns, and which variant?'
      }
      subtitle={
        covariate
          ? choosing
            ? 'Click a numeric variable recorded at or above the analysis rows’ level (e.g. a per-subject age). Anything that cannot be a covariate says why.'
            : 'Tick the numeric columns to use as covariates. The graph chooses WHICH version of this variable supplies the values — it changes nothing about a run.'
          : choosing
          ? 'Click a variable recorded at or above this data’s level — or the plotted variable itself, to group by one of its own columns. Anything that cannot group this figure says why.'
          : chosen?.own
            ? 'Tick the columns to group by. These are columns of the variable being plotted, so each row carries its own label — there is no version to choose.'
            : 'Tick the columns to group by. The graph chooses WHICH version of this variable supplies the labels — it changes nothing about a run.'
      }
      headerExtra={
        !choosing && (
          <span style={styles.chosenVariable} title="The variable being grouped by">
            {chosen?.variable}
          </span>
        )
      }
      nodes={decorated}
      edges={edges}
      nodeTypes={choosing ? pickNodeTypes : variantNodeTypes}
      loading={loading}
      error={canvasError || error}
      // Step one selects a VARIABLE, not a variant, so there is nothing to
      // select on the canvas — but the provider must still be there, or the
      // function nodes mount their pipeline bodies and throw (see PickerDialog).
      selection={choosing ? INERT_VARIANT_SELECTION : selectionValue}
      sidebar={sidebar}
      footer={
        <>
          {!choosing && (
            <button
              type="button"
              style={styles.button}
              onClick={() => {
                setChosen(null)
                setColumns(null)
                setGraph(null)
                setVariant({})
              }}
            >
              ← Variable
            </button>
          )}
          <button
            type="button"
            style={styles.primaryButton}
            onClick={() => onApply(Object.values(picked))}
          >
            Apply{count ? ` (${count})` : ''}
          </button>
        </>
      }
      onCancel={onCancel}
    />
  )
}

const local: Record<string, React.CSSProperties> = {
  sidebarTitle: {
    fontSize: 11, color: 'var(--ps-accent-text)', fontFamily: 'monospace',
    marginBottom: 8, paddingBottom: 6, borderBottom: '1px solid var(--ps-border)',
  },
  columnRow: {
    display: 'flex', alignItems: 'center', fontSize: 11, color: 'var(--ps-text-body)',
    padding: '3px 0', cursor: 'pointer',
  },
  columnName: { flex: 1, minWidth: 0 },
  columnLevels: {
    fontSize: 9, color: 'var(--ps-text-faint-grey)', background: 'var(--ps-control)',
    borderRadius: 8, padding: '0 5px',
  },
  refused: {
    fontSize: 10, color: 'var(--ps-text-faint-indigo)', fontStyle: 'italic', padding: '2px 0',
  },
}
