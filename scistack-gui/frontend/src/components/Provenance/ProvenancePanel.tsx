/**
 * ProvenancePanel — pick a variable, pin a variant, see what produced it.
 *
 * The GUI half of `.claude/plan-variant-provenance-introspection.md`. It asks
 * the backend ONE question (`variable_provenance`), which is
 * `Inspector.provenance` — the same method `scidb trace --variant … --runs`
 * calls. Nothing about provenance is computed here: a rule written in TSX has
 * no test, and two implementations of "which run produced this" would be two
 * answers (CLAUDE.md NOTE 3).
 *
 * **The picker is `VariantDagPopup`, not a copy of it.** Selection on the
 * pipeline canvas already exists and already means "which already-computed
 * records", which is exactly this question; it was merely wired to a PlotSpec.
 * It now takes `showName`/`title`/`applyLabel`, so this is a second consumer
 * rather than a second canvas that drifts from the first.
 *
 * **Everything here has a terminal equivalent**, by construction: the panel
 * shows a `scidb trace` line for the current selection, so whatever is on
 * screen can be reproduced — and checked — at a prompt.
 */

import { useCallback, useMemo, useState } from 'react'

import VariantDagPopup from '../PlotStudio/VariantDagPopup'
import { callBackend } from '../../api'
import * as modalStyles from '../modalStyles'

interface RunRef {
  run_id: string
  timestamp: string
  user_id: string | null
  where_clause: string | null
  invocation_id: string
  function_hash: string | null
  run_options: string | null
  /** Added by `provenance_service` on the flat list — which node's run it is. */
  function_name?: string | null
}

interface TraceInput {
  param: string
  record_id: string
  variable: string
}

interface TraceNode {
  record_id: string
  variable: string
  schema: Record<string, string>
  depth: number
  function_name: string | null
  function_hash: string | null
  constants: Record<string, string>
  path_inputs: Record<string, string>
  inputs: TraceInput[]
  saved: string | null
  saved_by: string | null
  run_count: number
  last_run: string | null
  invocation_id: string | null
  invocation_ids: string[]
  call_id: string | null
  run_options: string | null
  code_version: string | null
  runs: RunRef[]
}

interface ProvenanceReply {
  variable?: string
  root_record_id: string | null
  nodes: TraceNode[]
  selection: Record<string, string>
  matched_record_ids: string[]
  runs: RunRef[]
  /** Set instead of a tree when the pin matched nothing, or matched
   *  ambiguously — an answer to render in place, not a failure. */
  error?: string
}

const short = (id: string | null | undefined, n = 8) =>
  id ? id.slice(0, n) : '—'

/** The `scidb` command that reproduces what is on screen. */
function traceCommand(
  variable: string,
  selection: Record<string, unknown>
): string {
  const pins = Object.entries(selection)
    .map(([key, value]) => {
      const one = Array.isArray(value) ? value[0] : value
      return `--variant ${key}=${String(one)}`
    })
    .join(' ')
  return `scidb trace ${variable} ${pins} --runs`.replace(/\s+/g, ' ').trim()
}

export default function ProvenancePanel({ onClose }: { onClose: () => void }) {
  const [variables, setVariables] = useState<string[]>([])
  const [picking, setPicking] = useState(false)
  const [variable, setVariable] = useState<string | null>(null)
  const [selection, setSelection] = useState<Record<string, unknown>>({})
  const [reply, setReply] = useState<ProvenanceReply | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  const openPicker = useCallback(async () => {
    setError('')
    try {
      const rows = (await callBackend('get_variables_list')) as {
        variable_name: string
      }[]
      setVariables(rows.map(r => r.variable_name))
      setPicking(true)
    } catch (err) {
      setError((err as Error).message)
    }
  }, [])

  const apply = useCallback(
    async (next: {
      selection: Record<string, unknown>
      variable?: string
    }) => {
      const chosen = next.variable ?? variable
      setPicking(false)
      if (!chosen) return
      setVariable(chosen)
      setSelection(next.selection)
      setLoading(true)
      setError('')
      try {
        const payload = (await callBackend('variable_provenance', {
          variable: chosen,
          selection: next.selection,
        })) as ProvenanceReply
        setReply(payload)
      } catch (err) {
        setError((err as Error).message)
        setReply(null)
      } finally {
        setLoading(false)
      }
    },
    [variable]
  )

  // Depth-first from the root, so the list reads as the pipeline reads: the
  // record you asked about, then what it was made from.
  const ordered = useMemo(() => {
    if (!reply) return []
    return [...reply.nodes].sort((a, b) => a.depth - b.depth)
  }, [reply])

  return (
    <div style={modalStyles.overlay} onClick={onClose}>
      <div
        style={{ ...modalStyles.dialog, width: '80vw', maxWidth: 1100 }}
        onClick={e => e.stopPropagation()}
      >
        <div style={modalStyles.dialogTitle}>Provenance</div>
        <div style={styles.intro}>
          Pick a variable and pin a variant on the pipeline graph, exactly as
          Plot Studio does — then see the functions, versions and runs that
          produced those records.
        </div>

        <div style={styles.controls}>
          <button type="button" style={styles.primary} onClick={openPicker}>
            {variable ? 'Change variable / variant' : 'Pick a variable…'}
          </button>
          {variable && <span style={styles.chosen}>{variable}</span>}
          {variable && Object.keys(selection).length > 0 && (
            <span style={styles.pin}>
              {Object.entries(selection)
                .map(([k, v]) => `${k}=${Array.isArray(v) ? v.join('|') : String(v)}`)
                .join('  ')}
            </span>
          )}
        </div>

        {variable && (
          // Every panel maps to a CLI command: that is what makes "the CLI
          // powers the GUI" checkable rather than a claim.
          <code style={styles.command}>{traceCommand(variable, selection)}</code>
        )}

        {loading && <div style={styles.note}>Reading the graph…</div>}
        {error && <div style={styles.error}>{error}</div>}
        {reply?.error && <div style={styles.error}>{reply.error}</div>}

        {reply && !reply.error && (
          <div style={styles.body}>
            <div style={styles.summary}>
              <span>
                root record <code>{short(reply.root_record_id)}</code>
              </span>
              {reply.matched_record_ids.length > 1 && (
                <span
                  title="A variant spans schema locations; the tree is rooted at the most recently saved match."
                >
                  {reply.matched_record_ids.length} records match this variant
                </span>
              )}
              <span>{reply.runs.length} run(s) produced it</span>
            </div>

            <div style={styles.columns}>
              <div style={styles.column}>
                <div style={styles.columnTitle}>Upstream</div>
                {ordered.map(node => (
                  <NodeCard key={node.record_id} node={node} />
                ))}
              </div>

              <div style={styles.column}>
                <div style={styles.columnTitle}>Runs, newest first</div>
                {reply.runs.length === 0 && (
                  <div style={styles.note}>
                    No tracked run — these records were produced outside a
                    for_each execution (a terminal MATLAB run, or a direct save).
                  </div>
                )}
                {reply.runs.map(run => (
                  <div key={`${run.run_id}-${run.invocation_id}`} style={styles.run}>
                    <div style={styles.runHead}>
                      <code>{short(run.run_id)}</code>
                      <span>{run.timestamp}</span>
                      {run.user_id && <span style={styles.dim}>{run.user_id}</span>}
                    </div>
                    <div style={styles.dim}>
                      {run.function_name ?? 'unknown fn'} · invocation{' '}
                      <code>{short(run.invocation_id)}</code>
                      {run.run_options ? ` · ${run.run_options}` : ''}
                    </div>
                    {run.where_clause && (
                      <div style={styles.dim} title="Audit text — never parsed">
                        where {run.where_clause}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        <div style={styles.footer}>
          <button type="button" style={styles.button} onClick={onClose}>
            Close
          </button>
        </div>
      </div>

      {picking && (
        <VariantDagPopup
          variable={variable ?? variables[0] ?? ''}
          selection={selection}
          name=""
          showName={false}
          pick
          pickable={variables}
          title="Which variant?"
          subtitle="Pick a variable, then pin the variant you want the provenance of. Nothing here changes what a run does."
          applyLabel="Show provenance"
          onApply={apply}
          onCancel={() => setPicking(false)}
        />
      )}
    </div>
  )
}

/** One record and the call that produced it. */
function NodeCard({ node }: { node: TraceNode }) {
  const schema = Object.entries(node.schema)
    .map(([k, v]) => `${k}=${v}`)
    .join(' ')
  return (
    <div style={styles.node}>
      <div style={styles.nodeHead}>
        <span style={styles.nodeVar}>{node.variable}</span>
        {schema && <span style={styles.dim}>{schema}</span>}
        <code style={styles.dim}>{short(node.record_id)}</code>
      </div>
      {node.function_name === null ? (
        <div style={styles.dim}>raw save</div>
      ) : (
        <>
          <div style={styles.dim}>
            {node.function_name}
            {node.code_version ? ` · code ${node.code_version}` : ''}
            {node.call_id ? ` · call ${short(node.call_id)}` : ''}
            {node.run_options ? ` · ${node.run_options}` : ''}
          </div>
          {node.invocation_ids.length > 1 && (
            <div
              style={styles.reproduced}
              title="More than one invocation claims this record. The for_each save path never writes that (a changed body or flag is a new record), so another writer put it here — each run below says which invocation it belongs to."
            >
              reproduced: {node.invocation_ids.length} producing invocations
            </div>
          )}
        </>
      )}
      {Object.keys(node.constants).length > 0 && (
        <div style={styles.dim}>
          {Object.entries(node.constants)
            .map(([k, v]) => `${k}=${v}`)
            .join('  ')}
        </div>
      )}
      {node.saved && (
        <div style={styles.dim}>
          saved {node.saved}
          {node.saved_by ? ` by ${node.saved_by}` : ''}
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  intro: { fontSize: 12, color: '#aaa', marginBottom: 12, lineHeight: 1.5 },
  controls: { display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' },
  chosen: {
    fontFamily: 'monospace', fontSize: 12, color: '#a5f3fc',
    background: '#164e63', border: '1px solid #0891b2',
    borderRadius: 4, padding: '2px 8px',
  },
  pin: { fontFamily: 'monospace', fontSize: 11, color: '#e0b050' },
  command: {
    display: 'block', marginTop: 10, padding: '6px 10px',
    background: '#0f0f1e', border: '1px solid #2a2a4a', borderRadius: 4,
    fontFamily: 'monospace', fontSize: 11, color: '#9ad', overflowX: 'auto',
  },
  body: { marginTop: 14 },
  summary: {
    display: 'flex', gap: 18, flexWrap: 'wrap', fontSize: 12, color: '#ccc',
    marginBottom: 10,
  },
  columns: { display: 'flex', gap: 16, alignItems: 'flex-start' },
  column: { flex: 1, minWidth: 0, maxHeight: '48vh', overflowY: 'auto' },
  columnTitle: {
    fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.6,
    color: '#888', marginBottom: 6,
  },
  node: {
    border: '1px solid #2a2a4a', borderRadius: 4, padding: '7px 9px',
    marginBottom: 6, background: '#16162c',
  },
  nodeHead: { display: 'flex', gap: 8, alignItems: 'baseline', flexWrap: 'wrap' },
  nodeVar: { fontSize: 12, fontWeight: 600, color: '#ddd' },
  reproduced: { fontSize: 11, color: '#e0b050' },
  run: {
    border: '1px solid #2a2a4a', borderRadius: 4, padding: '7px 9px',
    marginBottom: 6, background: '#16162c',
  },
  runHead: {
    display: 'flex', gap: 8, alignItems: 'baseline', flexWrap: 'wrap',
    fontSize: 12, color: '#ddd',
  },
  dim: { fontSize: 11, color: '#9a9ab0', wordBreak: 'break-word' },
  note: { fontSize: 12, color: '#888', padding: '8px 0' },
  error: { fontSize: 12, color: '#ff8a8a', padding: '8px 0' },
  footer: { marginTop: 14, display: 'flex', justifyContent: 'flex-end', gap: 8 },
  button: {
    padding: '5px 14px', background: '#2a2a4a', color: '#ccc',
    border: '1px solid #3a3a5a', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  primary: {
    padding: '5px 14px', background: '#164e63', color: '#a5f3fc',
    border: '1px solid #0891b2', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
}
