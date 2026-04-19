/**
 * FunctionSettingsPanel — shown in the sidebar when a function node is selected.
 *
 * Sections:
 *   1. Variants — read-only Cartesian product of constant node values and multi-type inputs.
 *   2. Data Filters — where= filter definitions (structured form).
 *   3. Schema Filter — checkboxes per schema key to restrict which combos run.
 *   4. Run Options — dry_run, save, distribute toggles.
 *
 * Schema filter, where filters, and run options are stored on the function node's
 * data so they persist across selection changes and are available to handleRun.
 */

import { useEffect, useState, useCallback } from 'react'
import { useReactFlow } from '@xyflow/react'
import { callBackend } from '../../api'
import { useCommittedInput } from '../../hooks/useCommittedInput'

interface VariantRow {
  [constantName: string]: string
}

export interface SchemaFilter {
  [key: string]: unknown[]  // schema key → selected values
}

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

const OPERATORS = ['==', '!=', '<', '<=', '>', '>=', 'IN'] as const

interface Props {
  id: string
  label: string
  variants: VariantRow[]
  constantNames: string[]
  inputTypeNames: string[]
  schemaFilter: SchemaFilter | null
  schemaLevel: string[] | null    // which schema keys to iterate over; null = all
  whereFilters: WhereFilter[]
  runOptions: RunOptions
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

export default function FunctionSettingsPanel({ id, label, variants, constantNames, inputTypeNames, schemaFilter, schemaLevel, whereFilters, runOptions }: Props) {
  const { setNodes } = useReactFlow()
  const [schema, setSchema] = useState<SchemaInfo | null>(null)
  const [variableNames, setVariableNames] = useState<string[]>([])

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
        if (d.schemaFilter) config.schemaFilter = d.schemaFilter
        if (d.schemaLevel) config.schemaLevel = d.schemaLevel
        if (d.whereFilters) config.whereFilters = d.whereFilters
        if (d.runOptions) config.runOptions = d.runOptions
        callBackend('put_node_config', { node_id: id, config })
      }
      return updated
    })
  }, [id, setNodes])

  // Canvas-only variant — updates node data without saving to backend.
  // Used by useCommittedInput's onLiveChange for text fields.
  const updateNodeDataCanvas = useCallback((patch: Record<string, unknown>) => {
    setNodes(nds => nds.map(node =>
      node.id === id ? { ...node, data: { ...node.data, ...patch } } : node
    ))
  }, [id, setNodes])

  // Toggle a single value in the schema filter.
  const toggleSchemaValue = useCallback((key: string, value: unknown) => {
    if (!schema) return
    const allValues = schema.values[key] ?? []
    const current = schemaFilter?.[key] ?? allValues
    const valStr = String(value)
    const isSelected = current.some(v => String(v) === valStr)

    let updated: unknown[]
    if (isSelected) {
      updated = current.filter(v => String(v) !== valStr)
    } else {
      updated = [...current, value]
    }

    const newFilter: SchemaFilter = { ...(schemaFilter ?? {}), [key]: updated }

    // If all values selected for this key, remove the key from filter (means "all").
    if (updated.length === allValues.length) {
      delete newFilter[key]
    }

    // If filter is empty (all keys have all values), store null.
    const hasFilter = Object.keys(newFilter).length > 0
    updateNodeData({ schemaFilter: hasFilter ? newFilter : null })
  }, [schema, schemaFilter, updateNodeData])

  // Select all / none helpers.
  const selectAll = useCallback((key: string) => {
    const newFilter = { ...(schemaFilter ?? {}) }
    delete newFilter[key]
    const hasFilter = Object.keys(newFilter).length > 0
    updateNodeData({ schemaFilter: hasFilter ? newFilter : null })
  }, [schemaFilter, updateNodeData])

  const selectNone = useCallback((key: string) => {
    const newFilter: SchemaFilter = { ...(schemaFilter ?? {}), [key]: [] }
    updateNodeData({ schemaFilter: newFilter })
  }, [schemaFilter, updateNodeData])

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
              </tr>
            </thead>
            <tbody>
              {variants.map((row, i) => (
                <tr key={i} style={styles.variantRow}>
                  {allVariantNames.map(name => (
                    <td key={name} style={styles.td}>
                      <span style={{
                        ...styles.pill,
                        ...(inputTypeNames.includes(name) ? { color: '#6bb5f0', background: '#1a2a3a' } : {}),
                      }}>{row[name] ?? '\u2014'}</span>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
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

      {/* ---- Schema Filter ---- */}
      <section style={styles.section}>
        <div style={styles.sectionTitle}>Schema Filter</div>

        {!schema && <div style={styles.empty}>Loading schema...</div>}

        {schema && schema.keys.length === 0 && (
          <div style={styles.empty}>No schema keys configured.</div>
        )}

        {schema && schema.keys.map(key => {
          const allValues = schema.values[key] ?? []
          const selected = schemaFilter?.[key] ?? allValues

          return (
            <div key={key} style={styles.schemaKey}>
              <div style={styles.schemaKeyHeader}>
                <span style={styles.schemaKeyName}>{key}</span>
                <span style={styles.schemaKeyActions}>
                  <button
                    style={styles.linkBtn}
                    onClick={() => selectAll(key)}
                    title="Select all"
                  >all</button>
                  <span style={styles.separator}>/</span>
                  <button
                    style={styles.linkBtn}
                    onClick={() => selectNone(key)}
                    title="Select none"
                  >none</button>
                </span>
              </div>
              <div style={styles.checkboxGrid}>
                {allValues.map(value => {
                  const valStr = String(value)
                  const checked = selected.some(v => String(v) === valStr)
                  return (
                    <label key={valStr} style={styles.checkboxLabel}>
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={() => toggleSchemaValue(key, value)}
                        style={styles.checkbox}
                      />
                      <span style={styles.checkboxText}>{valStr}</span>
                    </label>
                  )
                })}
              </div>
            </div>
          )
        })}
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
      </section>
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
  // Schema filter styles
  schemaKey: {
    marginBottom: 10,
  },
  schemaKeyHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 4,
  },
  schemaKeyName: {
    fontFamily: 'monospace',
    fontSize: 11,
    fontWeight: 600,
    color: '#a89cf0',
  },
  schemaKeyActions: {
    fontSize: 10,
    color: '#555',
  },
  linkBtn: {
    background: 'none',
    border: 'none',
    color: '#7b68ee',
    fontSize: 10,
    cursor: 'pointer',
    padding: 0,
    textDecoration: 'underline',
  },
  separator: {
    margin: '0 2px',
    color: '#444',
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
}
