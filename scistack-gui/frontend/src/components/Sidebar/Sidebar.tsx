/**
 * Sidebar — right-panel, no tab bar. Shows the Edit palette by default;
 * as soon as a function/constant/variable/path-input/sweep/pipeline node
 * is selected, the palette is replaced by that node's settings panel, and
 * reverts automatically on deselect. There's nothing left to click to
 * switch views — Hypothesis (statement + Research Question) and the old
 * Runs/Project tabs have all moved out (Research Question lives in
 * HypothesisTabs above the canvas; Runs is RunsDock docked on the canvas;
 * Project is the header's Paths popup) — so a manual tab bar had nothing
 * left to arbitrate between.
 *
 * When a function node is selected, the settings panel shows a read-only
 * list of all pipeline variants — the Cartesian product of every constant
 * node's values on the canvas.
 */

import { useMemo } from 'react'
import { useStore } from '@xyflow/react'
import EditTab from './EditTab'
import FunctionSettingsPanel from './FunctionSettingsPanel'
import GlueSettingsPanel from './GlueSettingsPanel'
import type { SchemaSelection, RunOptions, WhereFilter, ColumnSelectionMap } from './FunctionSettingsPanel'
import ParameterSettingsPanel from './ParameterSettingsPanel'
import VariableSettingsPanel from './VariableSettingsPanel'
import PathInputSettingsPanel from './PathInputSettingsPanel'
import PipelineSettingsPanel from './PipelineSettingsPanel'
import EndpointPanel from './EndpointPanel'
import type { PipelineNodeData } from '../DAG/PipelineNode'
import { useSelectedNode } from '../../context/SelectedNodeContext'
import type { Node } from '@xyflow/react'
import type { ParameterValue } from '../DAG/ParameterNode'

interface FnNodeData {
  label: string
  endpoint_kind?: 'plot' | 'stat'
  schemaSelection?: SchemaSelection | null
  schemaLevel?: string[] | null
  whereFilters?: WhereFilter[]
  runOptions?: RunOptions
  // param_name -> wired variable type. Also the Inputs section's row list:
  // only a VARIABLE-bound parameter can have columns picked for it.
  input_params?: Record<string, string>
  columnSelections?: ColumnSelectionMap
}

interface ParameterNodeData {
  label: string
  values: ParameterValue[]
  source_kind?: 'constant' | 'sweep'
}

function isFunctionNode(node: Node | null): node is Node & { data: FnNodeData } {
  return node?.type === 'functionNode'
}

interface GlueNodeData {
  label: string
  input_params?: Record<string, string>
}

function isGlueNode(node: Node | null): node is Node & { data: GlueNodeData } {
  return node?.type === 'glueNode'
}

function isParameterNode(node: Node | null): node is Node & { data: ParameterNodeData } {
  return node?.type === 'parameterNode'
}

function isVariableNode(node: Node | null): node is Node & { data: { label: string } } {
  return node?.type === 'variableNode'
}

interface PathInputAlternate {
  template: string
  root_folder: string | null
}

interface PathInputNodeData {
  label: string
  template: string
  root_folder: string | null
  alternate_templates: PathInputAlternate[]
}

function isPathInputNode(node: Node | null): node is Node & { data: PathInputNodeData } {
  return node?.type === 'pathInputNode'
}

function isPipelineNode(node: Node | null): node is Node & { data: PipelineNodeData } {
  return node?.type === 'pipelineNode'
}

/** Compute the Cartesian product of value arrays. */
function cartesian(arrays: string[][]): string[][] {
  if (arrays.length === 0) return []
  return arrays.reduce<string[][]>(
    (acc, arr) => acc.flatMap(row => arr.map(v => [...row, v])),
    [[]]
  )
}

export default function Sidebar() {
  const { selectedNode } = useSelectedNode()

  // Subscribe directly to the React Flow store so we re-render when node/edge data changes.
  const nodes = useStore(s => s.nodes)
  const edges = useStore(s => s.edges)

  const hasNodeSelection = isFunctionNode(selectedNode) || isGlueNode(selectedNode) || isParameterNode(selectedNode) || isVariableNode(selectedNode) || isPathInputNode(selectedNode) || isPipelineNode(selectedNode)

  // Compute variant combinations from constant nodes and multi-wired variable inputs
  // connected to the selected function node.
  // Re-derived whenever nodes or edges change (value edits, new connections, etc.).
  const { constantNames, inputTypeNames, variants } = useMemo(() => {
    const empty = { constantNames: [] as string[], inputTypeNames: [] as string[], variants: [] as Record<string, string>[] }
    if (!isFunctionNode(selectedNode)) return empty

    // BFS upstream: walk edges in reverse to find all ancestor node IDs.
    const visited = new Set<string>()
    const queue = [selectedNode.id]
    while (queue.length > 0) {
      const current = queue.shift()!
      for (const e of edges) {
        if (e.target === current && !visited.has(e.source)) {
          visited.add(e.source)
          queue.push(e.source)
        }
      }
    }

    // Constant variant axes
    const constantNodes = nodes.filter(
      n => n.type === 'parameterNode' && visited.has(n.id)
    ) as Array<Node & { data: ParameterNodeData }>

    const cNames = constantNodes.map(n => n.data.label)
    const cValueLists = constantNodes.map(n =>
      (n.data.values ?? []).map((v: ParameterValue) => v.value)
    )

    // Multi-variable input axes: find in__ handles with >1 variable source
    const inputHandleTypes: Record<string, string[]> = {}
    for (const e of edges) {
      if (e.target !== selectedNode.id) continue
      const th = e.targetHandle ?? ''
      if (!th.startsWith('in__')) continue
      const sourceNode = nodes.find(n => n.id === e.source)
      if (!sourceNode || sourceNode.type !== 'variableNode') continue
      const param = th.replace('in__', '')
      const label = (sourceNode.data as { label: string }).label
      if (!inputHandleTypes[param]) inputHandleTypes[param] = []
      if (!inputHandleTypes[param].includes(label)) {
        inputHandleTypes[param].push(label)
      }
    }

    // Only include params with >1 type as variant axes
    const itNames: string[] = []
    const itValueLists: string[][] = []
    for (const [param, types] of Object.entries(inputHandleTypes)) {
      if (types.length > 1) {
        itNames.push(param)
        itValueLists.push(types)
      }
    }

    const allNames = [...cNames, ...itNames]
    const allValueLists = [...cValueLists, ...itValueLists]

    if (allNames.length === 0) return empty
    if (allValueLists.some(vals => vals.length === 0)) {
      return { constantNames: cNames, inputTypeNames: itNames, variants: [] }
    }

    const combos = cartesian(allValueLists)
    const variantRows = combos.map(combo =>
      Object.fromEntries(allNames.map((name, i) => [name, combo[i]]))
    )

    return { constantNames: cNames, inputTypeNames: itNames, variants: variantRows }
  }, [nodes, edges, selectedNode])

  return (
    <div style={styles.root}>
      <div style={styles.content}>
        {!hasNodeSelection && <EditTab />}
        {isFunctionNode(selectedNode)
          && (selectedNode.data as FnNodeData).endpoint_kind && (
          <EndpointPanel
            fnName={(selectedNode.data as FnNodeData).label}
            kind={(selectedNode.data as FnNodeData).endpoint_kind!}
          />
        )}
        {isFunctionNode(selectedNode) && (
          <FunctionSettingsPanel
            id={selectedNode.id}
            label={(selectedNode.data as FnNodeData).label}
            variants={variants}
            constantNames={constantNames}
            inputTypeNames={inputTypeNames}
            schemaSelection={(selectedNode.data as FnNodeData).schemaSelection ?? null}
            schemaLevel={(selectedNode.data as FnNodeData).schemaLevel ?? null}
            whereFilters={(selectedNode.data as FnNodeData).whereFilters ?? []}
            runOptions={(selectedNode.data as FnNodeData).runOptions ?? { dry_run: false, save: true, distribute: false, as_table: false }}
            inputParams={(selectedNode.data as FnNodeData).input_params ?? {}}
            columnSelections={(selectedNode.data as FnNodeData).columnSelections ?? {}}
          />
        )}
        {isGlueNode(selectedNode) && (
          <GlueSettingsPanel
            // Remount per node: the panel seeds a draft buffer from the
            // file, so reusing the instance would carry one node's
            // half-typed body onto another.
            key={selectedNode.id}
            label={(selectedNode.data as GlueNodeData).label}
            wiredType={
              Object.values((selectedNode.data as GlueNodeData).input_params ?? {})
                .find(t => !!t)
            }
          />
        )}
        {isParameterNode(selectedNode) && (
          <ParameterSettingsPanel
            // Remount per node: the panel seeds local draft state from props,
            // so reusing the instance across a selection change would carry
            // one node's half-typed edit onto another.
            key={selectedNode.id}
            id={selectedNode.id}
            label={(selectedNode.data as ParameterNodeData).label}
            values={(selectedNode.data as ParameterNodeData).values}
          />
        )}
        {isVariableNode(selectedNode) && (
          <VariableSettingsPanel
            label={(selectedNode.data as { label: string }).label}
          />
        )}
        {isPathInputNode(selectedNode) && (
          <PathInputSettingsPanel
            key={selectedNode.id}
            id={selectedNode.id}
            label={(selectedNode.data as PathInputNodeData).label}
            template={(selectedNode.data as PathInputNodeData).template}
            root_folder={(selectedNode.data as PathInputNodeData).root_folder}
            alternate_templates={(selectedNode.data as PathInputNodeData).alternate_templates ?? []}
          />
        )}
        {isPipelineNode(selectedNode) && (
          <PipelineSettingsPanel
            useId={selectedNode.id}
            data={selectedNode.data as PipelineNodeData}
          />
        )}
      </div>
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    overflow: 'hidden',
  },
  content: {
    flex: 1,
    overflowY: 'auto',
    padding: '8px 0',
  },
}
