/**
 * ParameterSettingsPanel — shown in the sidebar when a Parameter node is
 * selected. Add, remove and edit its values.
 *
 * Every change here **rewrites the declaration in source** via
 * `update_parameter` — the file is the single source of truth, and the GUI
 * edits it the way an IDE would (docs/claude/entity-editability-model.md).
 * Adding a value is literally adding an argument:
 * `scidb.Parameter(10)` -> `scidb.Parameter(10, 20)`. There is no second
 * kind to convert to, so no conversion prompt (D6).
 *
 * Values that have run but are no longer declared in source stay listed
 * (the DB is the record of what actually ran) and are marked "history" —
 * removing one of those is a no-op against source, so the row has no
 * remove button.
 *
 * Removing the LAST declared value is allowed. A Parameter with no values is
 * a real state — it is what "New parameter" creates, since that form collects
 * only a name — and anything wired to an empty one fails loudly at run rather
 * than running with a value nobody chose.
 *
 * A Parameter declared outside the entities file is read-only: every edit
 * control is greyed out up front with a banner pointing at the declaration
 * (useEntityEditability), rather than taking input the write would refuse.
 *
 * A refused write is SHOWN, never silently reverted — see useSourceEdit for
 * why that matters.
 *
 * The Generate section restores the vector-valued controls the old
 * SweepSettingsPanel had before the Sweep/Constant merge (D6) dropped them:
 * a whole list at once, either typed as a list or generated as a range from
 * start/end plus a step size or a target number of steps. Generation is
 * purely a frontend concern — the backend only ever receives the final flat
 * list, through the same `update_parameter` every other control here uses.
 *
 * What "Replace values" additionally sends is a `group`, and that is the
 * whole of how a generated set is told apart from values added one at a time:
 * the button IS the signal. The backend records it as display state (never in
 * source, so the declaration stays a flat list in all three languages) and
 * ships the set back as ONE row with a compact label, which both this panel
 * and the canvas node render — the same string, computed once, in
 * `graph_builder.render_value_group_label`. Individually added values are
 * untouched by any of this and render exactly as they always have.
 */

import { useMemo, useState } from 'react'
import type { ParameterValue } from '../DAG/ParameterNode'
import ReadOnlyDeclarationBanner from './ReadOnlyDeclarationBanner'
import { formatLocation, isLockedForEditing } from './sourceLocation'
import { useEntityEditability, useSourceEdit } from './useSourceEdit'

interface Props {
  id: string
  label: string
  values: ParameterValue[]
}

type GenMode = 'list' | 'range'
type RangeKind = 'step' | 'count'

/** The values source currently declares, in node order.
 *
 *  A generated row stands for its `members`, not for its own label — the
 *  label (`0:2:20 — 11 values`) is display text and must never be written
 *  back to source as a value. Flattening here is what lets every write below
 *  keep sending the plain flat list the backend has always received. */
function declaredValues(values: ParameterValue[]): string[] {
  return values
    .filter(v => v.is_current_source_value)
    .flatMap(v => (v.kind === 'generated' ? v.members ?? [] : [v.value]))
}

/** `"20"` -> 20, `"abc"` -> `"abc"` — the panel's inputs are text, but a
 *  Parameter should hold real numbers so version_keys match a bare literal. */
function coerce(raw: string): number | string | boolean {
  const t = raw.trim()
  if (t === 'true') return true
  if (t === 'false') return false
  if (t !== '' && !Number.isNaN(Number(t))) return Number(t)
  return t
}

function parseListDraft(text: string): number[] {
  return text
    .split(/[,\s]+/)
    .map(s => s.trim())
    .filter(s => s.length > 0)
    .map(Number)
    .filter(n => !Number.isNaN(n))
}

/** Round away float noise (0.1 + 0.2 -> 0.30000000000000004) without
 *  clobbering intentionally precise values — 10 significant decimals is far
 *  past anything a parameter step size would legitimately need. */
function clean(n: number): number {
  return Math.round(n * 1e10) / 1e10
}

function generateRange(start: number, end: number, third: number, kind: RangeKind): number[] {
  if (Number.isNaN(start) || Number.isNaN(end) || Number.isNaN(third)) return []
  if (kind === 'count') {
    const count = Math.floor(third)
    if (count < 1) return []
    if (count === 1) return [clean(start)]
    const step = (end - start) / (count - 1)
    return Array.from({ length: count }, (_, i) => clean(start + i * step))
  }
  // step-size mode
  const step = third
  if (step === 0) return start === end ? [clean(start)] : []
  const count = Math.floor((end - start) / step + 1e-9) + 1
  if (count < 1) return []
  return Array.from({ length: count }, (_, i) => clean(start + i * step))
}

export default function ParameterSettingsPanel({ label, values }: Props) {
  const [draft, setDraft] = useState('')
  const { submit, error, readOnlyAt, saving, clearError } = useSourceEdit()
  // Declared outside the entities file: every edit control is locked up
  // front rather than taking input the write would refuse.
  const editability = useEntityEditability('parameter', label)
  const locked = isLockedForEditing(editability)
  const inputsDisabled = saving || locked

  const declared = declaredValues(values)

  // --- Generate section ---
  // Seeded from the generation that produced the values currently on screen,
  // when there was one: reopening Generate should show the range you last
  // applied, not 0/10/1 again.
  const genSpec = values.find(v => v.kind === 'generated')?.spec
  const isRangeSpec = genSpec?.step !== undefined
  const [genOpen, setGenOpen] = useState(false)
  const [genMode, setGenMode] = useState<GenMode>(isRangeSpec ? 'range' : 'list')
  const [listDraft, setListDraft] = useState(declared.join(', '))
  const [start, setStart] = useState(isRangeSpec ? String(genSpec!.start) : '0')
  const [end, setEnd] = useState(isRangeSpec ? String(genSpec!.end) : '10')
  const [third, setThird] = useState(isRangeSpec ? String(genSpec!.step) : '1')
  // Always 'step': a stored spec records the step it worked out to, whether
  // the user originally asked for a step size or a number of steps.
  const [rangeKind, setRangeKind] = useState<RangeKind>('step')

  // No re-seeding effect: Sidebar remounts this panel per node (key={id}),
  // so the useState initializer above already runs on every selection
  // change — and NOT on the dag_updated refetch that follows a save, which
  // would otherwise clobber an in-flight edit.

  const preview = useMemo(() => {
    if (genMode === 'list') return parseListDraft(listDraft)
    return generateRange(Number(start), Number(end), Number(third), rangeKind)
  }, [genMode, listDraft, start, end, third, rangeKind])

  const write = (next: string[]) =>
    submit('update_parameter', { name: label, values: next.map(coerce) })

  const addValue = async () => {
    const v = draft.trim()
    if (!v || declared.includes(v)) {
      setDraft('')
      return
    }
    if (await write([...declared, v])) setDraft('')
  }

  /** Remove one value, or — for a generated row — the whole set it stands
   *  for. Emptying a Parameter completely is allowed: declared-with-no-value
   *  is a legal state, and anything wired to it then fails loudly at run
   *  rather than running with a value nobody chose. */
  const removeValue = async (v: ParameterValue) => {
    const gone = new Set(v.kind === 'generated' ? v.members ?? [] : [v.value])
    await write(declared.filter(d => !gone.has(d)))
  }

  const applyGenerated = async () => {
    if (preview.length === 0) return
    // The `group` is the ONLY thing marking these values as one set — same
    // endpoint, same flat list of values, different button. `addValue` above
    // deliberately sends none.
    await submit('update_parameter', {
      name: label,
      values: preview,
      group: {
        kind: genMode === 'range' ? 'range' : 'list',
        spec:
          genMode === 'range'
            ? {
                start: Number(start),
                // The LAST GENERATED VALUE, not the typed end: 0/7/step 2
                // produces 0,2,4,6, and a label reading "0:2:7" would name
                // a value that is not in the set.
                end: preview.length ? preview[preview.length - 1] : Number(end),
                // The label states a step, so a count-mode range reports the
                // step it worked out to — "0:2:20" is what the values ARE,
                // whichever way the user asked for them.
                step:
                  rangeKind === 'step'
                    ? Number(third)
                    : preview.length > 1
                    ? clean(preview[1] - preview[0])
                    : 0,
              }
            : { members: preview },
      },
    })
  }

  return (
    <div style={styles.root}>
      <div style={styles.constName}>{label}</div>

      {locked && editability?.file && (
        <ReadOnlyDeclarationBanner file={editability.file} line={editability.line} />
      )}

      <section style={styles.section}>
        <div style={styles.sectionTitle}>
          Values{declared.length > 1 ? ` — runs ${declared.length}×` : ''}
        </div>

        {values.length === 0 && <div style={styles.empty}>No values yet</div>}

        {values.map((v, i) => {
          const isDeclared = !!v.is_current_source_value
          const isGroup = v.kind === 'generated'
          return (
            <div key={i} style={styles.valueRow}>
              <span style={isGroup ? styles.groupPill : styles.valuePill}>
                {v.value}
              </span>
              {v.record_count > 0 && (
                <span style={styles.recCount}>{v.record_count} rec</span>
              )}
              {!isDeclared && <span style={styles.historyTag}>history</span>}
              {isDeclared && !locked && (
                <button
                  style={styles.removeBtn}
                  onClick={() => removeValue(v)}
                  disabled={inputsDisabled}
                  title={
                    isGroup
                      ? 'Remove the whole generated set from the declaration in source'
                      : 'Remove from the declaration in source'
                  }
                >
                  ×
                </button>
              )}
            </div>
          )
        })}
      </section>

      <section style={styles.section}>
        <div style={styles.sectionTitle}>Add value</div>
        <div style={styles.addRow}>
          <input
            style={locked ? styles.inputLocked : styles.input}
            placeholder="value…"
            value={draft}
            disabled={inputsDisabled}
            onChange={e => { setDraft(e.target.value); clearError() }}
            onKeyDown={e => {
              if (e.key === 'Enter') addValue()
              if (e.key === 'Escape') { setDraft(''); clearError() }
            }}
          />
          <button
            style={locked ? styles.btnLocked : styles.addBtn}
            onClick={addValue}
            disabled={inputsDisabled}
          >
            {saving ? '…' : 'Add'}
          </button>
        </div>
        {declared.length > 1 && (
          <div style={styles.hint}>
            Each value runs as its own for_each call.
          </div>
        )}
      </section>

      <section style={styles.section}>
        <button
          style={styles.disclosure}
          onClick={() => setGenOpen(o => !o)}
          type="button"
          title="Set every value at once — paste a list, or generate a range"
        >
          {genOpen ? '▾' : '▸'} Generate
        </button>

        {genOpen && (
          <div style={styles.genBody}>
            <div style={styles.modeRow}>
              <button
                style={genMode === 'list' ? styles.modeBtnActive : styles.modeBtn}
                onClick={() => setGenMode('list')}
                type="button"
              >
                List
              </button>
              <button
                style={genMode === 'range' ? styles.modeBtnActive : styles.modeBtn}
                onClick={() => setGenMode('range')}
                type="button"
              >
                Range
              </button>
            </div>

            {genMode === 'list' ? (
              <>
                <input
                  style={styles.genInput}
                  placeholder="1, 2, 5, 10"
                  value={listDraft}
                  disabled={inputsDisabled}
                  onChange={e => { setListDraft(e.target.value); clearError() }}
                />
                <div style={styles.hint}>Comma- or space-separated numbers.</div>
              </>
            ) : (
              <>
                <div style={styles.rangeGrid}>
                  <label style={styles.rangeField}>
                    <span style={styles.rangeLabel}>Start</span>
                    <input
                      style={styles.genInput}
                      type="number"
                      value={start}
                      disabled={inputsDisabled}
                      onChange={e => { setStart(e.target.value); clearError() }}
                    />
                  </label>
                  <label style={styles.rangeField}>
                    <span style={styles.rangeLabel}>End</span>
                    <input
                      style={styles.genInput}
                      type="number"
                      value={end}
                      disabled={inputsDisabled}
                      onChange={e => { setEnd(e.target.value); clearError() }}
                    />
                  </label>
                  <label style={styles.rangeField}>
                    <span style={styles.rangeLabel}>
                      {rangeKind === 'step' ? 'Step size' : '# of steps'}
                    </span>
                    <input
                      style={styles.genInput}
                      type="number"
                      value={third}
                      disabled={inputsDisabled}
                      onChange={e => { setThird(e.target.value); clearError() }}
                    />
                  </label>
                </div>
                <div style={styles.modeRow}>
                  <button
                    style={rangeKind === 'step' ? styles.modeBtnActive : styles.modeBtn}
                    onClick={() => setRangeKind('step')}
                    type="button"
                  >
                    By step size
                  </button>
                  <button
                    style={rangeKind === 'count' ? styles.modeBtnActive : styles.modeBtn}
                    onClick={() => setRangeKind('count')}
                    type="button"
                  >
                    By # of steps
                  </button>
                </div>
              </>
            )}

            <div style={styles.sectionTitle}>Preview</div>
            <div style={styles.previewText}>
              {preview.length === 0
                ? 'No values — check your input.'
                : preview.length <= 8
                ? preview.join(', ')
                : `${preview.slice(0, 6).join(', ')}, … (${preview.length} total)`}
            </div>
            {preview.length > 1 && (
              <div style={styles.hint}>
                Runs {preview.length}× — one for_each call per value.
              </div>
            )}

            <button
              style={preview.length === 0 || locked ? styles.replaceBtnDisabled : styles.replaceBtn}
              onClick={applyGenerated}
              disabled={inputsDisabled || preview.length === 0}
              type="button"
              title="Rewrite the declaration in source with exactly these values"
            >
              {saving ? '…' : 'Replace values'}
            </button>
          </div>
        )}
      </section>

      {error && (
        <div style={styles.error}>
          {error}
          {readOnlyAt && (
            <div style={styles.errorDetail}>
              Declared in <span style={styles.mono}>{formatLocation(readOnlyAt)}</span> —
              edit it there and hit 🔄 Refresh Code.
            </div>
          )}
        </div>
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  root: {
    padding: '12px',
    color: '#ccc',
    fontSize: 12,
  },
  constName: {
    fontFamily: 'monospace',
    fontWeight: 700,
    fontSize: 13,
    color: '#4ecdc4',
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
  valueRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    marginBottom: 4,
    borderBottom: '1px solid #1e1e3a',
    paddingBottom: 4,
  },
  valuePill: {
    flex: 1,
    background: '#1e3a2f',
    borderRadius: 3,
    padding: '2px 6px',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
  // A generated set reads as one thing, so it gets its own pill: same family,
  // dashed edge to say "this stands for several values".
  groupPill: {
    flex: 1,
    background: '#1e3a2f',
    border: '1px dashed #2a9d8f',
    borderRadius: 3,
    padding: '1px 6px',
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#b2ded9',
  },
  recCount: {
    color: '#555',
    fontSize: 10,
    whiteSpace: 'nowrap',
  },
  historyTag: {
    color: '#666',
    fontSize: 9,
    fontStyle: 'italic',
    whiteSpace: 'nowrap',
  },
  removeBtn: {
    background: 'transparent',
    border: 'none',
    color: '#666',
    cursor: 'pointer',
    fontSize: 14,
    padding: '0 2px',
    lineHeight: 1,
  },
  addRow: {
    display: 'flex',
    gap: 6,
  },
  input: {
    flex: 1,
    background: '#1a1a2e',
    border: '1px solid #333',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    padding: '3px 6px',
    minWidth: 0,
  },
  addBtn: {
    background: '#2a9d8f',
    border: 'none',
    borderRadius: 3,
    color: '#fff',
    fontSize: 11,
    padding: '3px 8px',
    cursor: 'pointer',
    fontWeight: 600,
  },
  // Greyed, not hidden: the values stay readable, only writing is refused.
  inputLocked: {
    flex: 1,
    background: '#15151f',
    border: '1px solid #2a2a2a',
    borderRadius: 3,
    color: '#777',
    fontSize: 11,
    padding: '3px 6px',
    minWidth: 0,
    cursor: 'not-allowed',
  },
  btnLocked: {
    background: '#3a3a3a',
    border: 'none',
    borderRadius: 3,
    color: '#777',
    fontSize: 11,
    padding: '3px 8px',
    cursor: 'not-allowed',
    fontWeight: 600,
  },
  hint: {
    marginTop: 5,
    fontSize: 10,
    color: '#666',
  },
  disclosure: {
    background: 'transparent',
    border: 'none',
    padding: 0,
    cursor: 'pointer',
    fontSize: 10,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.8,
  },
  genBody: {
    marginTop: 8,
  },
  modeRow: {
    display: 'flex',
    gap: 4,
    marginBottom: 8,
  },
  modeBtn: {
    flex: 1,
    padding: '4px 0',
    background: 'transparent',
    color: '#888',
    border: '1px solid #333',
    borderRadius: 3,
    cursor: 'pointer',
    fontSize: 10,
    fontWeight: 600,
  },
  modeBtnActive: {
    flex: 1,
    padding: '4px 0',
    background: '#1e3a2f',
    color: '#4ecdc4',
    border: '1px solid #2a9d8f',
    borderRadius: 3,
    cursor: 'pointer',
    fontSize: 10,
    fontWeight: 600,
  },
  genInput: {
    display: 'block',
    width: '100%',
    background: '#1a1a2e',
    border: '1px solid #333',
    borderRadius: 3,
    color: '#b2ded9',
    fontSize: 11,
    fontFamily: 'monospace',
    padding: '4px 6px',
    outline: 'none',
    boxSizing: 'border-box',
  },
  rangeGrid: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr 1fr',
    gap: 5,
    marginBottom: 8,
  },
  rangeField: {
    display: 'flex',
    flexDirection: 'column',
    gap: 3,
    minWidth: 0,
  },
  rangeLabel: {
    fontSize: 9,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.4,
    whiteSpace: 'nowrap',
  },
  previewText: {
    fontFamily: 'monospace',
    fontSize: 11,
    color: '#e5e5e5',
    background: '#12122a',
    border: '1px solid #333',
    borderRadius: 3,
    padding: '4px 6px',
    wordBreak: 'break-all',
  },
  replaceBtn: {
    width: '100%',
    marginTop: 10,
    padding: '5px 0',
    background: '#2a9d8f',
    color: '#fff',
    border: 'none',
    borderRadius: 3,
    cursor: 'pointer',
    fontSize: 11,
    fontWeight: 700,
  },
  replaceBtnDisabled: {
    width: '100%',
    marginTop: 10,
    padding: '5px 0',
    background: '#2a2a3e',
    color: '#666',
    border: 'none',
    borderRadius: 3,
    cursor: 'not-allowed',
    fontSize: 11,
    fontWeight: 700,
  },
  error: {
    background: 'rgba(255, 77, 79, 0.12)',
    border: '1px solid #ff4d4f',
    borderRadius: 4,
    padding: '6px 8px',
    fontSize: 11,
    color: '#ff9a9c',
    whiteSpace: 'pre-wrap',
  },
  errorDetail: {
    marginTop: 4,
    color: '#c98a8b',
  },
  mono: {
    fontFamily: 'monospace',
  },
}
