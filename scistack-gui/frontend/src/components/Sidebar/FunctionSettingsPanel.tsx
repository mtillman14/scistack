/**
 * FunctionSettingsPanel — shown in the sidebar when a function node is selected.
 *
 * Sections:
 *   1. Variants — read-only Cartesian product of constant node values and multi-type inputs.
 *   2. Data Filters — where= filter definitions (structured form).
 *   3. Schema Selection — which schema locations the node runs at, chosen in
 *      the same two-pane picker the plotting tab uses.
 *   4. Run Options — dry_run, save, distribute toggles.
 *
 * Schema selection, where filters, and run options are stored on the function
 * node's data so they persist across selection changes and are available to
 * handleRun.
 */

import { useEffect, useState, useCallback } from 'react'
import { useReactFlow } from '@xyflow/react'
import { callBackend } from '../../api'
import { useCommittedInput } from '../../hooks/useCommittedInput'
import { useScope } from '../../context/ScopeContext'
import SchemaLocationPicker from '../PlotStudio/SchemaLocationPicker'
import {
  type LocationSelection,
  asSelection,
  describeSelection,
  isInert,
} from '../PlotStudio/locationSelection'

interface VariantRow {
  [constantName: string]: string
}

interface HiddenCombo {
  node_id: string
  variant_key: Record<string, string>
}

/**
 * Which schema locations this node runs at: ragged `include` prefixes plus a
 * standing `exclude_levels` rule, the same pair the plotting tab stores.
 *
 * Replaces the old per-key `SchemaFilter`, which could only express a
 * Cartesian product — so "all of subject 01, plus trial 3 of subject 02" was
 * not sayable, and unticking one trial of one subject dropped that trial from
 * every subject. See docs/claude/location-filter-semantics.md.
 */
export type SchemaSelection = LocationSelection

export interface RunOptions {
  dry_run: boolean
  save: boolean
  distribute: boolean
  as_table: boolean
}

export interface WhereFilter {
  variable: string
  op: string
  value: string
}

/**
 * One parameter's column pick — `MyVar["col"]` / `MyVar[["a","b"]]` when
 * `iterate` is false, `MyVar.for_columns([...])` when it is true.
 *
 * Empty `columns` with `iterate: false` means "the whole variable" and is
 * stored as no entry at all, so an untouched parameter and one whose columns
 * were all unticked cannot differ (the backend's
 * `domain/column_selection.normalize` drops it too, from the other side).
 *
 * Empty `columns` WITH `iterate` is meaningful and IS stored: it means every
 * data column, one call each, resolved at for_each time.
 */
export interface ColumnSelection {
  columns: string[]
  iterate: boolean
}

export type ColumnSelectionMap = Record<string, ColumnSelection>

/**
 * A saved column selection the node's LAST RUN did not use, per parameter.
 *
 * Decision A of docs/claude/intent-and-fact.md: a script run ignores GUI
 * intent, so the canvas can hold a statement the data does not reflect. The
 * backend (graph_builder.mark_unused_intent) says which, and why:
 *   script_run    — the last run read source only;
 *   changed_since — stated after the last run, not yet run;
 *   never_run     — this function has no recorded run at all.
 * Silence here is the bug class this model exists to kill, so it is shown
 * beside the selection, not hidden in a log.
 */
export interface UnusedIntent {
  stated: ColumnSelection
  recorded: ColumnSelection | null
  origin: string | null
  reason: 'script_run' | 'changed_since' | 'never_run'
}

export type UnusedIntentMap = Record<string, UnusedIntent>

/** The one spelling of a selection, matching the backend's
 *  `column_selection.describe` — two spellings is how a user ends up reading
 *  a log line that disagrees with what the node shows. */
export function describeColumnSelection(sel: ColumnSelection | undefined): string {
  if (!sel) return '(whole variable)'
  const n = sel.columns.length
  if (sel.iterate) return n === 0 ? 'per column' : `per column (${n})`
  if (n === 1) return `"${sel.columns[0]}"`
  return `${n} columns`
}

interface VariableColumns {
  ok: boolean
  error?: string
  data_columns?: string[]
  schema_keys?: string[]
  note?: string
}

const OPERATORS = ['==', '!=', '<', '<=', '>', '>=', 'IN'] as const

interface Props {
  id: string
  label: string
  variants: VariantRow[]
  constantNames: string[]
  inputTypeNames: string[]
  schemaSelection: SchemaSelection | null
  schemaLevel: string[] | null    // which schema keys to iterate over; null = all
  whereFilters: WhereFilter[]
  runOptions: RunOptions
  inputParams: Record<string, string>
  columnSelections: ColumnSelectionMap
  unusedIntent?: UnusedIntentMap
}

interface SchemaInfo {
  keys: string[]
  values: Record<string, unknown[]>
}

interface VariableInfo {
  variable_name: string
}

interface WhereFilterRowProps {
  nodeId: string
  index: number
  filter: WhereFilter
  variableNames: string[]
  onUpdateCanvas: (index: number, patch: Partial<WhereFilter>) => void
  onUpdate: (index: number, patch: Partial<WhereFilter>) => void
  onRemove: (index: number) => void
  isValidVar: boolean
}

function WhereFilterRow({ nodeId, index, filter, variableNames, onUpdateCanvas, onUpdate, onRemove, isValidVar }: WhereFilterRowProps) {
  const varInput = useCommittedInput({
    initialValue: filter.variable,
    resetKey: `${nodeId}-${index}-var`,
    onLiveChange: val => onUpdateCanvas(index, { variable: val }),
    onSave: val => onUpdate(index, { variable: val }),
  })

  const valInput = useCommittedInput({
    initialValue: filter.value,
    resetKey: `${nodeId}-${index}-val`,
    onLiveChange: val => onUpdateCanvas(index, { value: val }),
    onSave: val => onUpdate(index, { value: val }),
  })

  return (
    <div style={filterRowStyle}>
      <input
        list="where-filter-variables"
        style={{
          ...filterVarInputStyle,
          ...(filter.variable !== '' && !isValidVar ? { borderColor: '#dc2626' } : {}),
        }}
        placeholder="Variable"
        {...varInput}
      />
      <select
        style={filterOpSelectStyle}
        value={filter.op}
        onChange={e => onUpdate(index, { op: e.target.value })}
      >
        {OPERATORS.map(op => (
          <option key={op} value={op}>{op}</option>
        ))}
      </select>
      <input
        style={filterValueInputStyle}
        placeholder="value"
        {...valInput}
      />
      <button
        style={filterRemoveBtnStyle}
        onClick={() => onRemove(index)}
        title="Remove filter"
      >&times;</button>
    </div>
  )
}

interface ColumnSelectRowProps {
  param: string
  variableType: string
  columns: VariableColumns | undefined
  selection: ColumnSelection | undefined
  onChange: (param: string, next: ColumnSelection | null) => void
}

/**
 * One row of the Inputs section: which columns of `variableType` this
 * parameter receives.
 *
 * The column list is whatever the backend read live from `_variables.dtype`
 * (`variable_service.input_columns`) — never a list this component guessed.
 * A scalar/array-stored variable reports its single class-named column plus
 * the note saying why, which is why an "empty" picker still shows one row
 * instead of looking broken.
 */
function ColumnSelectRow({ param, variableType, columns, selection, onChange }: ColumnSelectRowProps) {
  const [open, setOpen] = useState(false)
  const available = columns?.data_columns ?? []
  const loaded = columns !== undefined
  const resolvable = loaded && columns.ok && available.length > 0
  const picked = selection?.columns ?? []
  const iterate = selection?.iterate ?? false
  // No variable type means the graph has no record of a variable feeding this
  // parameter — it is unwired, or it is fed by a PathInput/Parameter, which
  // the aggregate partitions out of `input_params` and the fill-in pass adds
  // back as an empty string. Either way there is no table to pick columns
  // from, so the row says so rather than spinning on a fetch it can't make.
  const unbound = !variableType

  // Emit through one funnel so the "empty + not iterating = no entry" rule
  // lives in exactly one place on this side, matching the backend's.
  const emit = useCallback((cols: string[], it: boolean) => {
    onChange(param, cols.length === 0 && !it ? null : { columns: cols, iterate: it })
  }, [param, onChange])

  const toggleColumn = useCallback((col: string) => {
    const next = picked.includes(col)
      ? picked.filter(c => c !== col)
      : available.filter(c => picked.includes(c) || c === col)  // keep source order
    emit(next, iterate)
  }, [picked, available, iterate, emit])

  return (
    <div style={styles.inputRow}>
      <div style={styles.inputRowHead}>
        <span style={styles.inputParamName} title={`${param}: ${variableType || 'not wired'}`}>
          {param}
        </span>
        <span style={styles.inputTypeName}>{variableType || '—'}</span>
        <button
          type="button"
          style={{ ...styles.columnPickBtn, ...(unbound ? styles.columnPickBtnInert : {}) }}
          disabled={unbound}
          onClick={() => setOpen(o => !o)}
          title={
            unbound
              ? 'No variable feeds this parameter, so there are no columns to '
                + 'pick. Only a variable-fed parameter can have a column selection.'
              : resolvable
                ? 'Choose which columns of this variable the function receives'
                : loaded
                  ? (columns?.error ?? 'No columns could be read for this variable.')
                  : 'Reading columns…'
          }
        >
          {unbound ? 'n/a' : describeColumnSelection(selection)}
          {unbound ? '' : ` ${open ? '▴' : '▾'}`}
        </button>
      </div>

      {open && !unbound && (
        <div style={styles.columnPanel}>
          {!loaded && <div style={styles.empty}>Reading columns…</div>}
          {loaded && !columns.ok && (
            <div style={styles.empty}>{columns.error ?? 'No columns available.'}</div>
          )}
          {resolvable && (
            <>
              <div style={styles.columnPanelActions}>
                <button
                  type="button"
                  style={styles.columnActionBtn}
                  onClick={() => emit([...available], iterate)}
                >Select all</button>
                <button
                  type="button"
                  style={styles.columnActionBtn}
                  onClick={() => emit([], iterate)}
                >Deselect all</button>
              </div>
              <div style={styles.columnList}>
                {available.map(col => (
                  <label key={col} style={styles.checkboxLabel}>
                    <input
                      type="checkbox"
                      checked={picked.includes(col)}
                      onChange={() => toggleColumn(col)}
                      style={styles.checkbox}
                    />
                    <span style={styles.checkboxText}>{col}</span>
                  </label>
                ))}
              </div>
              <label
                style={styles.optionLabel}
                title={
                  'Run the function once per column and reassemble the results '
                  + 'into ONE output variable whose data, per schema combo, is a '
                  + 'one-row table with the same column names as the source. '
                  + 'With no columns ticked this iterates every data column.'
                }
              >
                <input
                  type="checkbox"
                  checked={iterate}
                  onChange={() => emit(picked, !iterate)}
                  style={styles.checkbox}
                />
                <span style={styles.optionText}>Run once per column</span>
                <span style={styles.optionHint}>for_columns</span>
              </label>
              {columns.note && <div style={styles.columnNote}>{columns.note}</div>}
            </>
          )}
        </div>
      )}
    </div>
  )
}

/**
 * "Stated here, not used by the last run." One line under a parameter whose
 * saved selection is not what the data reflects — the two facts and the
 * reason, never a silent chip. Wording per reason:
 *   script_run    — the last run came from a script, which reads source
 *                   only (decision A); the selection applies to GUI runs.
 *   changed_since — the selection was changed after the last run.
 *   never_run     — nothing has run yet.
 */
function UnusedIntentNote({ param, note }: { param: string; note: UnusedIntent }) {
  const stated = describeColumnSelection(note.stated)
  const recorded = note.recorded ? describeColumnSelection(note.recorded) : 'the whole variable'
  let text: string
  if (note.reason === 'script_run') {
    text = `Not used by the last run: it ran from a script, which reads source only, and bound ${recorded}. This selection (${stated}) applies when ${param} is run from here.`
  } else if (note.reason === 'changed_since') {
    text = `Changed since the last run, which bound ${recorded}. Run again to apply ${stated}.`
  } else {
    text = `Not run yet — ${stated} will apply on the first run from here.`
  }
  return (
    <div
      style={unusedIntentStyle}
      title={`stated: ${JSON.stringify(note.stated)} · last run (${note.origin ?? 'none'}) recorded: ${JSON.stringify(note.recorded)}`}
    >
      <span style={unusedIntentBadgeStyle}>not reflected</span> {text}
    </div>
  )
}

const unusedIntentStyle: React.CSSProperties = {
  fontSize: 10,
  color: '#92400e',
  background: '#fffbeb',
  border: '1px solid #fcd34d',
  borderRadius: 3,
  padding: '3px 6px',
  margin: '2px 0 6px 0',
  lineHeight: 1.35,
}

const unusedIntentBadgeStyle: React.CSSProperties = {
  fontWeight: 600,
  textTransform: 'uppercase',
  letterSpacing: 0.3,
  marginRight: 4,
}

export default function FunctionSettingsPanel({ id, label, variants, constantNames, inputTypeNames, schemaSelection, schemaLevel, whereFilters, runOptions, inputParams, columnSelections, unusedIntent }: Props) {
  const { setNodes } = useReactFlow()
  const { markNodeDirty, clearNodeDirty } = useScope()
  const [schema, setSchema] = useState<SchemaInfo | null>(null)
  const [variableNames, setVariableNames] = useState<string[]>([])
  const [hiddenCombos, setHiddenCombos] = useState<HiddenCombo[]>([])
  const [showHidden, setShowHidden] = useState(false)
  const [pickerOpen, setPickerOpen] = useState(false)
  const [columnsByType, setColumnsByType] = useState<Record<string, VariableColumns>>({})

  useEffect(() => {
    callBackend('get_schema')
      .then(d => setSchema(d as SchemaInfo))
      .catch(console.error)
    callBackend('get_variables_list')
      .then(d => {
        const vars = d as VariableInfo[]
        setVariableNames(vars.map(v => v.variable_name).sort())
      })
      .catch(console.error)
  }, [])

  // The distinct variable types this node's parameters are bound to. Joined
  // into a string so the effect below re-runs when the SET changes, not on
  // every render (a fresh array literal is a new identity every time).
  const boundTypes = Object.values(inputParams).filter(Boolean)
  const boundTypesKey = Array.from(new Set(boundTypes)).sort().join(' ')

  // Columns are read live, once per distinct variable type, when the panel
  // opens on a node — mirroring the glue panel. No scaffolding and no cache
  // across nodes: re-saving a variable with a different shape changes this
  // answer, and a stale list would silently offer columns that no longer
  // exist (the run then fails at load with scifor's KeyError).
  useEffect(() => {
    const types = boundTypesKey ? boundTypesKey.split(' ') : []
    if (types.length === 0) {
      setColumnsByType({})
      return
    }
    let cancelled = false
    Promise.all(types.map(t =>
      callBackend('get_variable_columns', { variable_type: t })
        .then(d => [t, d as VariableColumns] as const)
        .catch(err => [t, { ok: false, error: String(err) }] as const)
    )).then(entries => {
      if (!cancelled) setColumnsByType(Object.fromEntries(entries))
    })
    return () => { cancelled = true }
  }, [boundTypesKey])

  const refetchHidden = useCallback(() => {
    callBackend('list_hidden_combos', { function_name: label })
      .then(d => setHiddenCombos((d as { combos: HiddenCombo[] }).combos))
      .catch(console.error)
  }, [label])

  useEffect(() => {
    refetchHidden()
  }, [refetchHidden])

  // Only the constant axes matter for hide/restore matching — a hidden
  // combo is scoped to constant values, never to multi-type input axes
  // (see plan-combo-hiding.md's v1 scope cut).
  const rowConstants = useCallback(
    (row: VariantRow) => Object.fromEntries(constantNames.map(n => [n, row[n]])),
    [constantNames]
  )
  const matchingHidden = useCallback(
    (row: VariantRow) => {
      const rc = rowConstants(row)
      return hiddenCombos.find(h =>
        constantNames.every(n => String(h.variant_key[n]) === String(rc[n]))
      )
    },
    [hiddenCombos, constantNames, rowConstants]
  )

  const hideRow = useCallback((row: VariantRow) => {
    callBackend('hide_combo', { function_name: label, node_id: id, variant_key: rowConstants(row) })
      .then(refetchHidden)
      .catch(console.error)
  }, [label, id, rowConstants, refetchHidden])

  const unhideCombo = useCallback((nodeId: string) => {
    callBackend('unhide_combo', { node_id: nodeId })
      .then(refetchHidden)
      .catch(console.error)
  }, [refetchHidden])

  // Helper to update function node data and persist config to backend.
  const updateNodeData = useCallback((patch: Record<string, unknown>) => {
    setNodes(nds => {
      const updated = nds.map(node =>
        node.id === id
          ? { ...node, data: { ...node.data, ...patch } }
          : node
      )
      // Persist config to backend.
      const node = updated.find(n => n.id === id)
      if (node) {
        const config: Record<string, unknown> = {}
        const d = node.data as Record<string, unknown>
        if (d.schemaSelection) config.schemaSelection = d.schemaSelection
        if (d.schemaLevel) config.schemaLevel = d.schemaLevel
        if (d.whereFilters) config.whereFilters = d.whereFilters
        if (d.runOptions) config.runOptions = d.runOptions
        if (d.columnSelections) config.columnSelections = d.columnSelections
        callBackend('put_node_config', { node_id: id, config })
          .then(() => clearNodeDirty(id))
          .catch(err => console.error('[FunctionSettings] save error:', err))
      }
      return updated
    })
  }, [id, setNodes, clearNodeDirty])

  // Canvas-only variant — updates node data without saving to backend.
  // Used by useCommittedInput's onLiveChange for text fields. Marks the
  // node dirty so an unrelated dag_updated refetch (e.g. another node's
  // put_layout from dragging a new node onto the canvas) can't revert this
  // not-yet-saved draft -- see ScopeContext's markNodeDirty doc.
  const updateNodeDataCanvas = useCallback((patch: Record<string, unknown>) => {
    setNodes(nds => nds.map(node =>
      node.id === id ? { ...node, data: { ...node.data, ...patch } } : node
    ))
    markNodeDirty(id, patch)
  }, [id, setNodes, markNodeDirty])

  // The picker writes the whole pair at once; an inert selection is stored as
  // null so an untouched node and a "select all" click cannot differ.
  const setSchemaSelection = useCallback((next: LocationSelection) => {
    updateNodeData({ schemaSelection: isInert(next) ? null : next })
  }, [updateNodeData])

  // Toggle a schema key in the iteration level.
  const toggleSchemaLevel = useCallback((key: string) => {
    if (!schema) return
    const allKeys = schema.keys
    const current = schemaLevel ?? allKeys
    const isSelected = current.includes(key)

    let updated: string[]
    if (isSelected) {
      updated = current.filter(k => k !== key)
    } else {
      // Maintain the original order from schema.keys.
      updated = allKeys.filter(k => current.includes(k) || k === key)
    }

    // If all keys selected, store null (means "all").
    const isAll = updated.length === allKeys.length
    updateNodeData({ schemaLevel: isAll ? null : updated })
  }, [schema, schemaLevel, updateNodeData])

  // Set (or clear) one parameter's column selection. Saved exactly like
  // runOptions — updateNodeData + put_node_config. Unlike run options there
  // is deliberately NO second, live-canvas route: the backend reads only the
  // stored config, so the two cannot disagree (the disagreement
  // docs/claude/gui-run-options-flow.md warns about).
  const setColumnSelection = useCallback((param: string, next: ColumnSelection | null) => {
    const updated: ColumnSelectionMap = { ...columnSelections }
    if (next === null) delete updated[param]
    else updated[param] = next
    updateNodeData({ columnSelections: updated })
  }, [columnSelections, updateNodeData])

  // Toggle a run option.
  const toggleRunOption = useCallback((opt: keyof RunOptions) => {
    const updated = { ...runOptions, [opt]: !runOptions[opt] }
    updateNodeData({ runOptions: updated })
  }, [runOptions, updateNodeData])

  // --- Where filter management ---
  const addWhereFilter = useCallback(() => {
    const newFilter: WhereFilter = { variable: '', op: '==', value: '' }
    updateNodeData({ whereFilters: [...whereFilters, newFilter] })
  }, [whereFilters, updateNodeData])

  const removeWhereFilter = useCallback((index: number) => {
    const updated = whereFilters.filter((_, i) => i !== index)
    updateNodeData({ whereFilters: updated })
  }, [whereFilters, updateNodeData])

  const updateWhereFilter = useCallback((index: number, patch: Partial<WhereFilter>) => {
    const updated = whereFilters.map((f, i) => i === index ? { ...f, ...patch } : f)
    updateNodeData({ whereFilters: updated })
  }, [whereFilters, updateNodeData])

  const updateWhereFilterCanvas = useCallback((index: number, patch: Partial<WhereFilter>) => {
    const updated = whereFilters.map((f, i) => i === index ? { ...f, ...patch } : f)
    updateNodeDataCanvas({ whereFilters: updated })
  }, [whereFilters, updateNodeDataCanvas])

  // All variant column names (constants + multi-type inputs)
  const allVariantNames = [...constantNames, ...inputTypeNames]
  // Hiding a specific combo only supports constant-value axes (see
  // plan-combo-hiding.md) \u2014 a row whose uniqueness comes from an
  // input-type choice has no computable call_id to hide by yet.
  const hideDisabledReason = inputTypeNames.length > 0
    ? 'Hiding one combo isn\u2019t supported for multi-type input rows yet'
    : undefined
  const visibleVariants = showHidden ? variants : variants.filter(row => !matchingHidden(row))

  return (
    <div style={styles.root}>
      <div style={styles.fnName}>{label}</div>

      {/* ---- Variants ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Variants</div>

        {variants.length === 0 && (
          <div style={styles.empty}>
            {allVariantNames.length === 0
              ? 'No variant axes (no constants or multi-type inputs).'
              : 'No values defined on variant axes.'}
          </div>
        )}

        {variants.length > 0 && (
          <table style={styles.table}>
            <thead>
              <tr>
                {allVariantNames.map(name => (
                  <th key={name} style={{
                    ...styles.th,
                    ...(inputTypeNames.includes(name) ? { color: '#6bb5f0' } : {}),
                  }}>{name}</th>
                ))}
                <th style={styles.th} />
              </tr>
            </thead>
            <tbody>
              {visibleVariants.map((row, i) => {
                const hidden = matchingHidden(row)
                return (
                  <tr key={i} style={styles.variantRow}>
                    {allVariantNames.map(name => (
                      <td key={name} style={styles.td}>
                        <span style={{
                          ...styles.pill,
                          ...(inputTypeNames.includes(name) ? { color: '#6bb5f0', background: '#1a2a3a' } : {}),
                          ...(hidden ? { opacity: 0.5 } : {}),
                        }}>{row[name] ?? '\u2014'}</span>
                      </td>
                    ))}
                    <td style={styles.td}>
                      {hidden ? (
                        <button
                          style={styles.hideBtn}
                          onClick={() => unhideCombo(hidden.node_id)}
                          title="Restore this combo"
                        >restore</button>
                      ) : (
                        <button
                          style={{
                            ...styles.hideBtn,
                            ...(hideDisabledReason ? { opacity: 0.4, cursor: 'default', textDecoration: 'none' } : {}),
                          }}
                          disabled={!!hideDisabledReason}
                          onClick={() => hideRow(row)}
                          title={hideDisabledReason ?? 'Hide this combo (never deletes data \u2014 excludes it from runs and can be restored)'}
                        >hide</button>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}

        {hiddenCombos.length > 0 && (
          <button style={styles.showHiddenToggle} onClick={() => setShowHidden(s => !s)}>
            {showHidden ? 'hide restored rows' : `${hiddenCombos.length} hidden \u2014 show`}
          </button>
        )}
      </section>

      {/* ---- Inputs (column selection) ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Inputs</div>

        {Object.keys(inputParams).length === 0 ? (
          <div style={styles.empty}>
            No variable inputs wired. Connect a variable to a parameter&rsquo;s
            handle to choose its columns.
          </div>
        ) : (
          <>
            <div style={styles.hint}>
              Which columns of each wired variable this function receives.
              Nothing ticked means the whole variable.
            </div>
            {Object.entries(inputParams).map(([param, type]) => (
              <div key={param}>
                <ColumnSelectRow
                  param={param}
                  variableType={type}
                  columns={type ? columnsByType[type] : undefined}
                  selection={columnSelections[param]}
                  onChange={setColumnSelection}
                />
                {unusedIntent?.[param] && (
                  <UnusedIntentNote param={param} note={unusedIntent[param]} />
                )}
              </div>
            ))}
          </>
        )}
      </section>

      {/* ---- Data Filters (where=) ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Data Filters</div>

        {whereFilters.length === 0 && (
          <div style={styles.empty}>No data filters. All records will be used.</div>
        )}

        {whereFilters.map((f, idx) => {
          const isValidVar = f.variable === '' || variableNames.includes(f.variable)
          return (
            <WhereFilterRow
              key={idx}
              nodeId={id}
              index={idx}
              filter={f}
              variableNames={variableNames}
              onUpdateCanvas={updateWhereFilterCanvas}
              onUpdate={updateWhereFilter}
              onRemove={removeWhereFilter}
              isValidVar={isValidVar}
            />
          )
        })}
        {/* Shared datalist for variable name autocomplete */}
        <datalist id="where-filter-variables">
          {variableNames.map(v => (
            <option key={v} value={v} />
          ))}
        </datalist>

        <button style={styles.addFilterBtn} onClick={addWhereFilter}>
          + Add Filter
        </button>

        {whereFilters.length > 1 && (
          <div style={styles.filterHint}>
            Each filter runs as a separate variant (EachOf).
          </div>
        )}
      </section>

      {/* ---- Schema Selection ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Schema Selection</div>
        <div style={styles.hint}>
          Which schema locations this node runs at. Omit a level everywhere
          (by key), or pick locations one by one. Everything runs until you
          say otherwise.
        </div>
        <button
          style={styles.locationBtn}
          onClick={() => setPickerOpen(true)}
          type="button"
        >
          {describeSelection(asSelection(schemaSelection))}
        </button>
      </section>

      {/* ---- Schema Level ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Schema Level</div>

        {!schema && <div style={styles.empty}>Loading schema...</div>}

        {schema && schema.keys.length === 0 && (
          <div style={styles.empty}>No schema keys configured.</div>
        )}

        {schema && schema.keys.length > 0 && (
          <>
            <div style={styles.schemaLevelHint}>
              Which schema keys to iterate over
            </div>
            <div style={styles.checkboxGrid}>
              {schema.keys.map(key => {
                const checked = schemaLevel === null || schemaLevel.includes(key)
                return (
                  <label key={key} style={styles.checkboxLabel}>
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggleSchemaLevel(key)}
                      style={styles.checkbox}
                    />
                    <span style={styles.checkboxText}>{key}</span>
                  </label>
                )
              })}
            </div>
          </>
        )}
      </section>

      {/* ---- Run Options ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Run Options</div>

        <label style={styles.optionLabel}>
          <input
            type="checkbox"
            checked={runOptions.dry_run}
            onChange={() => toggleRunOption('dry_run')}
            style={styles.checkbox}
          />
          <span style={styles.optionText}>Dry run</span>
          <span style={styles.optionHint}>Preview without executing</span>
        </label>

        <label style={styles.optionLabel}>
          <input
            type="checkbox"
            checked={runOptions.save}
            onChange={() => toggleRunOption('save')}
            style={styles.checkbox}
          />
          <span style={styles.optionText}>Save results</span>
          <span style={styles.optionHint}>Persist outputs to DB</span>
        </label>

        <label style={styles.optionLabel}>
          <input
            type="checkbox"
            checked={runOptions.distribute}
            onChange={() => toggleRunOption('distribute')}
            style={styles.checkbox}
          />
          <span style={styles.optionText}>Distribute</span>
          <span style={styles.optionHint}>Save at lower schema level</span>
        </label>

        <label style={styles.optionLabel}>
          <input
            type="checkbox"
            checked={runOptions.as_table}
            onChange={() => toggleRunOption('as_table')}
            style={styles.checkbox}
          />
          <span style={styles.optionText}>As table</span>
          <span style={styles.optionHint}>Keep schema columns in DataFrames</span>
        </label>

        {/* `for_columns` is an execution MODE, exactly like its two siblings
            above — "call the function once per column and reassemble" — but
            it is SET per input, on the Inputs section, because it names which
            input iterates. Stating it here is what makes it visible where a
            user looks for modes; it stayed invisible for as long as it lived
            only inside a column list, which is how it got silently dropped
            (docs/claude/intent-and-fact.md, plan Stage 4). Read-only on
            purpose: one place to change it, one place to see it. */}
        {Object.entries(columnSelections).some(([, sel]) => sel?.iterate) && (
          <div style={styles.optionLabel}>
            <span style={{ ...styles.checkbox, visibility: 'hidden' }} />
            <span style={styles.optionText}>Run once per column</span>
            <span style={styles.optionHint}>
              {Object.entries(columnSelections)
                .filter(([, sel]) => sel?.iterate)
                .map(([param]) => param)
                .join(', ')}
              {' — set on the Inputs section'}
            </span>
          </div>
        )}
      </section>

      {/* The same picker the plotting tab opens. `nodeId` switches its tree to
          the INNER JOIN of this node's input variables — the locations it can
          actually run — and `onPick` is omitted because "plot this location
          alone" means nothing here. */}
      {pickerOpen && (
        <SchemaLocationPicker
          variable={label}
          nodeId={id}
          value={asSelection(schemaSelection)}
          onChange={setSchemaSelection}
          onClose={() => setPickerOpen(false)}
        />
      )}
    </div>
  )
}

// These are module-level so WhereFilterRow (defined above) can reference them.
const filterRowStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 3,
  marginBottom: 4,
}
const filterVarInputStyle: React.CSSProperties = {
  flex: '3 1 0',
  minWidth: 0,
  background: '#1e1e3a',
  border: '1px solid #2a2a4a',
  borderRadius: 3,
  color: '#b2ded9',
  fontSize: 11,
  fontFamily: 'monospace',
  padding: '2px 4px',
}
const filterOpSelectStyle: React.CSSProperties = {
  flex: '0 0 auto',
  width: 42,
  background: '#1e1e3a',
  border: '1px solid #2a2a4a',
  borderRadius: 3,
  color: '#ccc',
  fontSize: 11,
  fontFamily: 'monospace',
  padding: '2px 2px',
  textAlign: 'center',
}
const filterValueInputStyle: React.CSSProperties = {
  flex: '4 1 0',
  minWidth: 0,
  background: '#1e1e3a',
  border: '1px solid #2a2a4a',
  borderRadius: 3,
  color: '#b2ded9',
  fontSize: 11,
  fontFamily: 'monospace',
  padding: '2px 4px',
}
const filterRemoveBtnStyle: React.CSSProperties = {
  background: 'none',
  border: 'none',
  color: '#dc2626',
  fontSize: 14,
  cursor: 'pointer',
  padding: '0 2px',
  lineHeight: 1,
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  fnName: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#a89cf0',
    marginBottom: 12,
    wordBreak: 'break-all',
  },
  section: {
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 10,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
    marginBottom: 6,
  },
  empty: {
    color: '#555',
    fontStyle: 'italic',
    fontSize: 11,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  th: {
    textAlign: 'left',
    fontSize: 10,
    color: '#888',
    fontWeight: 600,
    padding: '2px 4px 4px 0',
    borderBottom: '1px solid #2a2a4a',
    fontFamily: 'monospace',
  },
  variantRow: {
    borderBottom: '1px solid #1e1e3a',
  },
  td: {
    padding: '4px 4px 4px 0',
    verticalAlign: 'middle',
  },
  pill: {
    display: 'inline-block',
    background: '#1e1e3a',
    borderRadius: 3,
    padding: '1px 5px',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
  hideBtn: {
    background: 'none',
    border: 'none',
    color: '#7b68ee',
    fontSize: 10,
    cursor: 'pointer',
    padding: '1px 4px',
    textDecoration: 'underline',
  },
  showHiddenToggle: {
    background: 'none',
    border: 'none',
    color: '#666',
    fontSize: 10,
    fontStyle: 'italic',
    cursor: 'pointer',
    padding: '4px 0 0 0',
  },
  addFilterBtn: {
    background: 'none',
    border: '1px dashed #444',
    borderRadius: 3,
    color: '#7b68ee',
    fontSize: 11,
    cursor: 'pointer',
    padding: '3px 8px',
    marginTop: 4,
    width: '100%',
  },
  filterHint: {
    fontSize: 10,
    color: '#555',
    fontStyle: 'italic',
    marginTop: 4,
  },
  // Schema selection styles
  hint: {
    fontSize: 10,
    color: '#777',
    marginBottom: 6,
    lineHeight: 1.4,
  },
  locationBtn: {
    width: '100%',
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    padding: '4px 8px',
    cursor: 'pointer',
    textAlign: 'left',
  },
  checkboxGrid: {
    display: 'flex',
    flexWrap: 'wrap',
    gap: 4,
  },
  checkboxLabel: {
    display: 'flex',
    alignItems: 'center',
    gap: 3,
    background: '#1e1e3a',
    borderRadius: 3,
    padding: '2px 6px',
    cursor: 'pointer',
  },
  checkbox: {
    margin: 0,
    accentColor: '#7b68ee',
  },
  checkboxText: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
  schemaLevelHint: {
    fontSize: 10,
    color: '#555',
    fontStyle: 'italic',
    marginBottom: 4,
  },
  // Run options styles
  optionLabel: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    marginBottom: 6,
    cursor: 'pointer',
  },
  optionText: {
    fontSize: 11,
    color: '#ccc',
    fontWeight: 600,
  },
  optionHint: {
    fontSize: 10,
    color: '#555',
    fontStyle: 'italic',
  },
  // Inputs / column selection
  inputRow: {
    marginBottom: 6,
  },
  inputRowHead: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
  },
  inputParamName: {
    flex: '0 0 auto',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#ccc',
    maxWidth: 90,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  inputTypeName: {
    flex: '0 0 auto',
    fontFamily: 'monospace',
    fontSize: 10,
    color: '#6bb5f0',
    maxWidth: 80,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  columnPickBtn: {
    flex: '1 1 0',
    minWidth: 0,
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    padding: '2px 6px',
    cursor: 'pointer',
    textAlign: 'left',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  columnPickBtnInert: {
    opacity: 0.45,
    cursor: 'default',
    color: '#777',
  },
  columnPanel: {
    margin: '4px 0 0 8px',
    padding: '6px 8px',
    background: '#16162a',
    border: '1px solid #2a2a4a',
    borderRadius: 3,
  },
  columnPanelActions: {
    display: 'flex',
    gap: 8,
    marginBottom: 4,
  },
  columnActionBtn: {
    background: 'none',
    border: 'none',
    color: '#6bb5f0',
    fontSize: 10,
    padding: 0,
    cursor: 'pointer',
    textDecoration: 'underline',
  },
  columnList: {
    maxHeight: 160,
    overflowY: 'auto',
    marginBottom: 6,
  },
  columnNote: {
    fontSize: 10,
    color: '#777',
    fontStyle: 'italic',
    lineHeight: 1.4,
    marginTop: 4,
  },
}
