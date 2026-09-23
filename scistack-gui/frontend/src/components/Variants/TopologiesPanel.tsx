/**
 * TopologiesPanel — "why does this variable have more variants than I
 * expected?"
 *
 * The GUI half of `.claude/plan-topologies-panel.md`. It asks the backend ONE
 * question (`variable_topologies`), which is `Inspector.topologies` — the same
 * method `scidb variants <name>` renders. Nothing about variants is computed
 * here: a rule written in TSX has no test, and two implementations of "is this
 * variant still live" would be two answers (CLAUDE.md NOTE 3). Even the
 * verdict wording arrives from the backend; what this file owns is layout.
 *
 * **Bottom-up, where `ProvenancePanel` is top-down.** That panel pins a
 * variant and traces where it came from; you cannot pin a variant you do not
 * know exists. This one says what is in here. They are kept as two entries in
 * two places for that reason — collapsing them into one would hide the one
 * that answers "what is in here".
 *
 * **The `load:` line is the reason this exists.** Everything above it is
 * context the user would want anyway; that line is the only place in the GUI
 * where "this variant is not what a run will read" is visible. Two variants
 * that look equally alive is the shape of the 2026-09-22 bug.
 */

import { useCallback, useEffect, useState } from 'react'

import { callBackend } from '../../api'
import * as modalStyles from '../modalStyles'
import {
  constantsLabel,
  isNotCurrent,
  locationsLabel,
  topologiesSummary,
  topologyHeading,
  variantsCommand,
} from './topologies'
import type { TopologiesReply, TopologyGroup, VariantRow } from './topologies'

const short = (id: string | null | undefined, n = 8) => (id ? id.slice(0, n) : '—')

export default function TopologiesPanel({
  variable: initialVariable,
  onClose,
}: {
  variable?: string | null
  onClose: () => void
}) {
  const [variables, setVariables] = useState<string[]>([])
  const [variable, setVariable] = useState<string | null>(initialVariable ?? null)
  const [allLocations, setAllLocations] = useState(false)
  const [reply, setReply] = useState<TopologiesReply | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  // The variable list is a plain dropdown, not `VariantDagPopup`: that picker
  // selects a VARIANT, and needing to pick one to see which exist is the
  // circularity this panel removes.
  useEffect(() => {
    let live = true
    callBackend('get_variables_list')
      .then(rows => {
        if (!live) return
        const names = (rows as { variable_name: string }[]).map(r => r.variable_name)
        setVariables(names)
        setVariable(current => current ?? names[0] ?? null)
      })
      .catch(err => live && setError((err as Error).message))
    return () => {
      live = false
    }
  }, [])

  const load = useCallback(
    async (name: string, everyLocation: boolean) => {
      setLoading(true)
      setError('')
      try {
        const payload = (await callBackend('variable_topologies', {
          variable: name,
          all_locations: everyLocation,
        })) as TopologiesReply
        setReply(payload)
      } catch (err) {
        setError((err as Error).message)
        setReply(null)
      } finally {
        setLoading(false)
      }
    },
    []
  )

  useEffect(() => {
    if (variable) void load(variable, allLocations)
  }, [variable, allLocations, load])

  return (
    <div style={modalStyles.overlay} onClick={onClose}>
      <div
        style={{ ...modalStyles.dialog, width: '80vw', maxWidth: 1100 }}
        onClick={e => e.stopPropagation()}
      >
        <div style={modalStyles.dialogTitle}>Variants</div>
        <div style={styles.intro}>
          Every shape that has produced this variable, and every run of each
          shape — with whether a load would still return its records. Read-only:
          nothing here changes what a run does.
        </div>

        <div style={styles.controls}>
          <label style={styles.dim} htmlFor="topologies-variable">
            Variable
          </label>
          <select
            id="topologies-variable"
            style={styles.select}
            value={variable ?? ''}
            onChange={e => setVariable(e.target.value || null)}
          >
            {variables.map(name => (
              <option key={name} value={name}>
                {name}
              </option>
            ))}
          </select>
          <label style={styles.checkbox}>
            <input
              type="checkbox"
              checked={allLocations}
              onChange={e => setAllLocations(e.target.checked)}
            />
            Show every location
          </label>
        </div>

        {variable && (
          // Every panel maps to a CLI command: that is what makes "the CLI
          // powers the GUI" checkable rather than a claim.
          <code style={styles.command}>{variantsCommand(variable, allLocations)}</code>
        )}

        {loading && <div style={styles.note}>Reading the variants…</div>}
        {error && <div style={styles.error}>{error}</div>}

        {reply && !loading && !error && (
          <div style={styles.body}>
            <div style={styles.summary}>{topologiesSummary(reply)}</div>
            {reply.topologies.length === 0 && (
              <div style={styles.note}>
                No pipeline step has produced <code>{reply.variable}</code> —
                its records were saved directly, so there is no topology to
                show.
              </div>
            )}
            {reply.topologies.map(topology => (
              <TopologyCard
                key={`${topology.function_name}:${topology.output_type}:${topology.input_types
                  .map(p => p.join('='))
                  .join(',')}`}
                topology={topology}
              />
            ))}
          </div>
        )}

        <div style={styles.footer}>
          <button type="button" style={styles.button} onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  )
}

/** One DAG shape — one node on the canvas — and the runs of it. */
function TopologyCard({ topology }: { topology: TopologyGroup }) {
  return (
    <div style={styles.topology}>
      <div style={styles.topologyHead}>{topologyHeading(topology)}</div>
      {topology.variants.map((variant, index) => (
        <VariantRowView
          key={variant.call_id || index}
          variant={variant}
          index={index + 1}
        />
      ))}
    </div>
  )
}

function VariantRowView({ variant, index }: { variant: VariantRow; index: number }) {
  const [expanded, setExpanded] = useState(false)
  const stale = isNotCurrent(variant.verdict)
  const verdictStyle =
    variant.verdict === 'current'
      ? styles.verdictCurrent
      : variant.verdict === 'partially_superseded'
        ? styles.verdictPartial
        : styles.verdictSuperseded
  const locations = variant.locations ?? { total: 0, keys: [], sample: [] }
  const hasMore = locations.total > (locations.sample?.length ?? 0)

  return (
    <div style={{ ...styles.variant, ...(stale ? styles.variantStale : null) }}>
      <div style={styles.variantHead}>
        <span style={styles.ordinal}>[{index}]</span>
        <span style={stale ? styles.constantsStale : styles.constants}>
          {constantsLabel(variant)}
        </span>
        {variant.run_options && (
          <span style={styles.chip} title="Run options are a variant axis of their own: two rows identical everywhere else and different here are two genuinely different variants.">
            run {variant.run_options}
          </span>
        )}
        {variant.function_hash && (
          <span style={styles.chip} title="The producing source. The only thing that tells two variants apart when constants, inputs and run options are all identical — a body edit.">
            code {short(variant.function_hash)}
          </span>
        )}
      </div>

      <div style={styles.dim}>
        {variant.record_count} record(s)
        {variant.first_saved
          ? ` · first ${variant.first_saved} · last ${variant.last_saved}`
          : ' · never saved'}
      </div>

      <div style={verdictStyle} title="Whether a `latest` load would still return these records.">
        {variant.verdict_label}
      </div>

      <div style={styles.dim}>
        {locations.total} location(s)
        {locations.total > 0 && (
          <>
            {' · '}
            <span>{locationsLabel(locations)}</span>
            {hasMore && (
              <button
                type="button"
                style={styles.linkButton}
                onClick={() => setExpanded(v => !v)}
                title="Tick “Show every location” above to fetch them all; this expands what has already been fetched."
              >
                {expanded ? 'less' : 'more'}
              </button>
            )}
          </>
        )}
      </div>

      {expanded && locations.sample && locations.sample.length > 0 && (
        <div style={styles.locationList}>
          {locations.sample.map((combo, i) => (
            <code key={i} style={styles.locationChip}>
              {Object.entries(combo)
                .map(([k, v]) => `${k}=${v}`)
                .join(' ')}
            </code>
          ))}
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  intro: { fontSize: 12, color: '#aaa', marginBottom: 12, lineHeight: 1.5 },
  controls: { display: 'flex', alignItems: 'center', gap: 10, flexWrap: 'wrap' },
  select: {
    background: '#16162c', color: '#ddd', border: '1px solid #3a3a5a',
    borderRadius: 4, padding: '4px 8px', fontSize: 12, minWidth: 200,
  },
  checkbox: {
    display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#aaa',
  },
  command: {
    display: 'block', marginTop: 10, padding: '6px 10px',
    background: '#0f0f1e', border: '1px solid #2a2a4a', borderRadius: 4,
    fontFamily: 'monospace', fontSize: 11, color: '#9ad', overflowX: 'auto',
  },
  body: { marginTop: 14, maxHeight: '58vh', overflowY: 'auto' },
  summary: { fontSize: 12, color: '#ccc', marginBottom: 10 },
  topology: {
    border: '1px solid #2a2a4a', borderRadius: 4, padding: '8px 10px',
    marginBottom: 10, background: '#141428',
  },
  topologyHead: {
    fontFamily: 'monospace', fontSize: 12, color: '#a5f3fc', marginBottom: 8,
    wordBreak: 'break-word',
  },
  variant: {
    border: '1px solid #2a2a4a', borderRadius: 4, padding: '6px 8px',
    marginBottom: 6, background: '#16162c',
  },
  variantStale: { background: '#141422', borderColor: '#3a3a4a' },
  variantHead: {
    display: 'flex', gap: 8, alignItems: 'baseline', flexWrap: 'wrap',
    marginBottom: 3,
  },
  ordinal: { fontFamily: 'monospace', fontSize: 11, color: '#666' },
  constants: { fontFamily: 'monospace', fontSize: 12, color: '#ddd' },
  constantsStale: {
    fontFamily: 'monospace', fontSize: 12, color: '#7a7a90',
    textDecoration: 'line-through',
  },
  chip: {
    fontFamily: 'monospace', fontSize: 10, color: '#9ad',
    background: '#0f0f1e', border: '1px solid #2a2a4a', borderRadius: 3,
    padding: '1px 5px',
  },
  verdictCurrent: { fontSize: 11, color: '#8fd48f', marginTop: 2 },
  verdictPartial: { fontSize: 11, color: '#e0b050', marginTop: 2 },
  verdictSuperseded: { fontSize: 11, color: '#8a8a9a', marginTop: 2 },
  locationList: {
    display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 5,
    maxHeight: 140, overflowY: 'auto',
  },
  locationChip: {
    fontSize: 10, color: '#9a9ab0', background: '#0f0f1e',
    border: '1px solid #2a2a4a', borderRadius: 3, padding: '1px 5px',
  },
  linkButton: {
    background: 'none', border: 'none', color: '#9ad', cursor: 'pointer',
    fontSize: 11, padding: '0 4px', textDecoration: 'underline',
  },
  dim: { fontSize: 11, color: '#9a9ab0', wordBreak: 'break-word' },
  note: { fontSize: 12, color: '#888', padding: '8px 0' },
  error: { fontSize: 12, color: '#ff8a8a', padding: '8px 0' },
  footer: { marginTop: 14, display: 'flex', justifyContent: 'flex-end', gap: 8 },
  button: {
    padding: '5px 14px', background: '#2a2a4a', color: '#ccc',
    border: '1px solid #3a3a5a', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
}
