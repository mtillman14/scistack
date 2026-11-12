/**
 * ExpectedLocationsPopup — "which locations should this variable have, and
 * does each one match?" Opened from a Variable's right-click (Expected
 * locations…), from its canvas badges, and from the Coverage popup.
 *
 * Every status, label and sentence comes from scidb
 * (scidb.provenance.expectation through `variable_expectation`); this
 * component draws them. The first look is two colours (matches / does not
 * match); the exact status is never hidden: an icon per status, the label
 * and the "why" sentence are columns, and the row's tooltip repeats them
 * (user decision D8, docs/claude/expected-locations.md).
 *
 * Writes: a key's setting (Shared / Per parent / Auto, declared values),
 * Expected / Not expected marks with a reason, Unmark. Marks never change
 * what runs; "Exclude" (Inventory) is a different act.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { callBackend } from '../../api'
import * as modalStyles from '../modalStyles'
import {
  STATUS_ICON,
  STATUS_ORDER,
  TONE_COLOR,
  concreteKeys,
  keyText,
  type ExpectationResult,
  type ExpectationRow,
  type FollowResult,
  type KeySetting,
  type Status,
} from './expectationTypes'
import { styles } from './expectationStyles'

export type ExpectedFilter = 'problems' | 'marks' | null

interface Props {
  variable: string
  /** Open filtered: the badge that was clicked. */
  initialFilter?: ExpectedFilter
  /** Open on these key values only (a Coverage cell). */
  initialValues?: Record<string, string>
  onClose: () => void
}

interface MarkDraft {
  row: ExpectationRow
  /** Keys of the row the mark names; unticked keys become every value. */
  include: Record<string, boolean>
  expected: boolean
  reason: string
  busy: boolean
  error: string
}

export default function ExpectedLocationsPopup({ variable, initialFilter = null, initialValues, onClose }: Props) {
  const [result, setResult] = useState<ExpectationResult | null>(null)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')
  const [loading, setLoading] = useState(false)
  const [statuses, setStatuses] = useState<Set<Status>>(
    () => new Set(initialFilter === 'problems' ? ['missing_unexpected', 'present_unexpected'] : []),
  )
  const [marksOnly, setMarksOnly] = useState(initialFilter === 'marks')
  const [values, setValues] = useState<Record<string, string> | undefined>(initialValues)
  const [draft, setDraft] = useState<MarkDraft | null>(null)
  const [follow, setFollow] = useState<FollowResult | null>(null)
  const [followError, setFollowError] = useState('')
  const queryId = useRef(0)

  const load = useCallback(() => {
    const id = ++queryId.current
    setLoading(true)
    setError('')
    callBackend('variable_expectation', { variable })
      .then(d => { if (id === queryId.current) setResult(d as ExpectationResult) })
      .catch(err => { if (id === queryId.current) { setResult(null); setError((err as Error).message) } })
      .finally(() => { if (id === queryId.current) setLoading(false) })
  }, [variable])

  useEffect(() => { load() }, [load])

  const rows = useMemo(() => {
    if (!result) return []
    return result.rows.filter(r =>
      (statuses.size === 0 || statuses.has(r.status))
      && (!marksOnly || r.mark != null)
      && (!values || Object.entries(values).every(([k, v]) => r.values[k] === v || r.values[k] === null)),
    )
  }, [result, statuses, marksOnly, values])

  const toggleStatus = (s: Status) =>
    setStatuses(prev => {
      const next = new Set(prev)
      if (next.has(s)) next.delete(s)
      else next.add(s)
      return next
    })

  const startMark = (row: ExpectationRow) => {
    setMessage('')
    const include = Object.fromEntries(Object.keys(concreteKeys(row.values)).map(k => [k, true]))
    // Offer the opposite of what the row is now.
    setDraft({ row, include, expected: !row.expected, reason: '', busy: false, error: '' })
  }

  const draftKeys = draft
    ? Object.fromEntries(Object.entries(concreteKeys(draft.row.values)).filter(([k]) => draft.include[k]))
    : {}

  const saveMark = async () => {
    if (!draft || !draft.reason.trim() || Object.keys(draftKeys).length === 0) return
    setDraft({ ...draft, busy: true, error: '' })
    try {
      await callBackend('mark_expectation', {
        variable, keys: draftKeys, expected: draft.expected, reason: draft.reason,
      })
      setMessage(`Marked ${keyText(draftKeys)} ${draft.expected ? 'expected' : 'not expected'}.`)
      setDraft(null)
      load()
    } catch (err) {
      setDraft(d => (d ? { ...d, busy: false, error: (err as Error).message } : d))
    }
  }

  const unmark = async (keys: Record<string, string>) => {
    if (!window.confirm(`Take back the mark on ${keyText(keys)}? (Its history is kept.)`)) return
    setError('')
    try {
      await callBackend('unmark_expectation', { variable, keys })
      setMessage(`Unmarked ${keyText(keys)}.`)
      load()
    } catch (err) {
      setError((err as Error).message)
    }
  }

  const startFollow = (row: ExpectationRow) => {
    setFollow(null)
    setFollowError('')
    callBackend('follow_location', { location: concreteKeys(row.values) })
      .then(d => setFollow(d as FollowResult))
      .catch(err => setFollowError((err as Error).message))
  }

  const tones = result?.tone_counts
  const allMarker = result?.all_marker ?? '(all)'

  return (
    <div style={modalStyles.overlay} onClick={onClose}>
      <div style={styles.dialog} data-testid="expected-locations-popup" onClick={e => e.stopPropagation()}>
        <div style={styles.header}>
          <span style={modalStyles.dialogTitle}>Expected locations: {variable}</span>
          <button style={styles.closeBtn} onClick={onClose} title="Close" type="button">×</button>
        </div>
        <div style={styles.note}>
          Which locations this variable should have, and whether each matches. Green: as
          expected. Red: missing unexpectedly, or present unexpectedly. Grey: excluded from
          analysis. Marks say what is expected; they never change what runs.
        </div>

        {loading && !result && <div style={styles.hint}>Loading…</div>}
        {error && <div style={styles.error}>{error}</div>}
        {message && <div style={styles.message}>{message}</div>}

        {result && tones && (
          <>
            <div style={styles.controls}>
              <span style={{ color: TONE_COLOR.ok }}>{tones.ok.toLocaleString()} as expected</span>
              <span style={{ color: TONE_COLOR.problem }}>{tones.problem.toLocaleString()} not</span>
              <span style={{ color: TONE_COLOR.excluded }}>{tones.excluded.toLocaleString()} excluded</span>
              <span style={styles.hint}>
                keys {result.keys.join('/') || '—'} ({result.keys_rule})
              </span>
            </div>

            <KeySettings settings={result.settings} onSaved={detail => { setMessage(detail); load() }} onError={setError} />

            {result.notes.map(n => <div key={n} style={styles.hint}>Note: {n}</div>)}

            <div style={styles.controls}>
              <span style={styles.label}>Show:</span>
              <button
                type="button"
                style={statuses.size === 0 && !marksOnly ? styles.chipOn : styles.chip}
                onClick={() => { setStatuses(new Set()); setMarksOnly(false) }}
              >
                All {result.total_rows.toLocaleString()}
              </button>
              {STATUS_ORDER.map(s => (
                <button
                  key={s}
                  type="button"
                  style={statuses.has(s) ? styles.chipOn : styles.chip}
                  title={`Show only: ${result.labels[s]} (click again to show all)`}
                  onClick={() => toggleStatus(s)}
                >
                  <span style={{ color: TONE_COLOR[s === 'excluded' ? 'excluded' : (s === 'present' || s === 'missing_expected') ? 'ok' : 'problem'] }}>
                    {STATUS_ICON[s]}
                  </span>{' '}
                  {result.labels[s]} {result.counts[s].toLocaleString()}
                </button>
              ))}
              <button
                type="button"
                style={marksOnly ? styles.chipOn : styles.chip}
                onClick={() => setMarksOnly(m => !m)}
                title="Only rows a mark (own or inherited) decides"
              >
                Marked
              </button>
              {values && (
                <button type="button" style={styles.chipOn} onClick={() => setValues(undefined)} title="Show every location">
                  {keyText(values)} ×
                </button>
              )}
            </div>

            <div style={styles.tableWrap}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.th} />
                    {result.columns.map(c => <th key={c} style={styles.th}>{c}</th>)}
                    <th style={styles.th}>status</th>
                    <th style={styles.th}>why</th>
                    <th style={styles.th} />
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => {
                    const own = r.mark && r.inherited_from == null ? r.mark : null
                    return (
                      <tr key={i} data-testid="expected-row" data-status={r.status} title={`${r.label}: ${r.why}`} style={draft?.row === r ? { background: '#2a2a48' } : undefined}>
                        <td style={{ ...styles.td, color: TONE_COLOR[r.tone] }}>{STATUS_ICON[r.status]}</td>
                        {result.columns.map(c => (
                          <td key={c} style={styles.td}>
                            {c in r.values ? (r.values[c] ?? allMarker) : ''}
                          </td>
                        ))}
                        <td style={{ ...styles.td, color: TONE_COLOR[r.tone] }}>{r.label}</td>
                        <td style={styles.whyTd}>{r.why}</td>
                        <td style={styles.actionTd}>
                          <button type="button" style={styles.rowBtn} onClick={() => startMark(r)}
                            title="Mark this location (or a wider one) Expected or Not expected, with a reason">
                            Mark…
                          </button>
                          {own && (
                            <button type="button" style={styles.rowBtn} onClick={() => unmark(own.keys)}
                              title={`Take back the mark on ${keyText(own.keys)}`}>
                              Unmark
                            </button>
                          )}
                          {Object.keys(concreteKeys(r.values)).length > 0 && (
                            <button type="button" style={styles.rowBtn} onClick={() => startFollow(r)}
                              title="This location through every variable of the pipeline">
                              Follow
                            </button>
                          )}
                        </td>
                      </tr>
                    )
                  })}
                  {rows.length === 0 && (
                    <tr><td style={styles.td} colSpan={result.columns.length + 4}>(no rows)</td></tr>
                  )}
                </tbody>
              </table>
            </div>
            {result.truncated && (
              <div style={styles.hint}>
                Showing {result.rows.length.toLocaleString()} of {result.total_rows.toLocaleString()} rows.
                For all of them: <code>scidb expected {variable} --csv out.csv</code>
              </div>
            )}

            {draft && (
              <div style={styles.panel}>
                <div style={styles.controls}>
                  <span>Mark</span>
                  {Object.entries(concreteKeys(draft.row.values)).map(([k, v]) => (
                    <label key={k} style={{ display: 'flex', gap: 3, alignItems: 'center' }}
                      title="Untick to mark every value of this key">
                      <input
                        type="checkbox"
                        checked={draft.include[k]}
                        onChange={e => setDraft({ ...draft, include: { ...draft.include, [k]: e.target.checked } })}
                      />
                      <code>{k}={v}</code>
                    </label>
                  ))}
                  <span style={styles.segment}>
                    <button type="button" style={draft.expected ? styles.segOn : styles.segOff}
                      onClick={() => setDraft({ ...draft, expected: true })}>Expected</button>
                    <button type="button" style={!draft.expected ? styles.segOn : styles.segOff}
                      onClick={() => setDraft({ ...draft, expected: false })}>Not expected</button>
                  </span>
                </div>
                <div style={styles.hint}>
                  {Object.keys(draftKeys).length === 0
                    ? 'Tick at least one key.'
                    : `${keyText(draftKeys)} of ${variable} — and every variable downstream of it — will be ${draft.expected ? 'expected' : 'not expected'}.`}
                </div>
                <div style={styles.controls}>
                  <input
                    style={{ ...styles.input, flex: 1, minWidth: 240 }}
                    placeholder="Reason (required), e.g. file never recorded"
                    value={draft.reason}
                    onChange={e => setDraft({ ...draft, reason: e.target.value })}
                    onKeyDown={e => { if (e.key === 'Enter') saveMark() }}
                    autoFocus
                  />
                  <button type="button" style={styles.primary} onClick={saveMark}
                    disabled={draft.busy || !draft.reason.trim() || Object.keys(draftKeys).length === 0}>
                    {draft.busy ? 'Saving…' : 'Save mark'}
                  </button>
                  <button type="button" style={styles.btn} onClick={() => setDraft(null)}>Cancel</button>
                </div>
                {draft.error && <div style={styles.error}>{draft.error}</div>}
              </div>
            )}

            {(follow || followError) && (
              <FollowPanel result={follow} error={followError} onClose={() => { setFollow(null); setFollowError('') }} />
            )}

            {result.marks.length > 0 && (
              <details style={{ marginTop: 8 }}>
                <summary style={{ cursor: 'pointer', color: '#ccc' }}>
                  This variable's own marks ({result.marks.length})
                </summary>
                {result.marks.map(m => (
                  <div key={keyText(m.keys)} style={{ ...styles.controls, color: '#bbb' }}>
                    <code>{keyText(m.keys)}</code>
                    <span>{m.expected ? 'expected' : 'not expected'}</span>
                    <span style={styles.hint}>“{m.reason}” {m.changed_at ? m.changed_at.slice(0, 10) : ''}</span>
                    <button type="button" style={styles.rowBtn} onClick={() => unmark(m.keys)}>Unmark</button>
                  </div>
                ))}
              </details>
            )}
          </>
        )}
      </div>
    </div>
  )
}

/** One row per key of the variable: how its values are expected. Changing
 *  one changes it for the whole project (it is a fact about the study). */
function KeySettings({
  settings,
  onSaved,
  onError,
}: {
  settings: KeySetting[]
  onSaved: (detail: string) => void
  onError: (msg: string) => void
}) {
  const [texts, setTexts] = useState<Record<string, string>>({})
  const [busy, setBusy] = useState('')

  const save = async (key: string, sharing: 'shared' | 'per_parent' | null, valuesText: string | null) => {
    setBusy(key)
    try {
      const r = await callBackend('set_key_expectation', { key, sharing, values_text: valuesText }) as { detail: string }
      setTexts(t => { const n = { ...t }; delete n[key]; return n })
      onSaved(`Schema key ${r.detail} (for every variable).`)
    } catch (err) {
      onError((err as Error).message)
    } finally {
      setBusy('')
    }
  }

  return (
    <div style={{ ...styles.panel, marginTop: 4 }}>
      <div style={styles.hint}>
        Does every parent share the same values of a key? Shared: every value is expected
        under every parent (e.g. trials 1–5 in every session). Per parent: only the values
        found under each parent count (e.g. gait cycles). Applies to the whole project.
      </div>
      {settings.map(s => {
        const stated = s.stated_sharing
        const text = texts[s.key] ?? (s.values ? s.values.join(', ') : '')
        const edited = s.key in texts
        return (
          <div key={s.key} style={styles.controls} title={s.description}>
            <code style={{ minWidth: 70 }}>{s.key}</code>
            <span style={styles.segment}>
              {([['shared', 'Shared'], ['per_parent', 'Per parent'], [null, 'Auto']] as const).map(([value, label]) => (
                <button
                  key={label}
                  type="button"
                  disabled={busy === s.key}
                  style={stated === value ? styles.segOn : styles.segOff}
                  title={value === null ? `Automatic: now ${s.sharing === 'shared' ? 'shared' : 'per parent'}` : undefined}
                  onClick={() => save(s.key, value, value === 'shared' && s.values ? s.values.join(', ') : null)}
                >
                  {label}
                </button>
              ))}
            </span>
            {s.sharing === 'shared' ? (
              <>
                <input
                  style={{ ...styles.input, width: 220 }}
                  placeholder="every value in the database"
                  title="Declared values: comma-separated, ranges like 1..5 or 01..12 (a leading zero keeps the width)"
                  value={text}
                  onChange={e => setTexts(t => ({ ...t, [s.key]: e.target.value }))}
                />
                {edited && (
                  <button type="button" style={styles.primary} disabled={busy === s.key}
                    onClick={() => save(s.key, 'shared', text.trim() ? text : null)}>
                    Save values
                  </button>
                )}
              </>
            ) : (
              <span style={styles.hint}>only the values found under each parent</span>
            )}
            <span style={styles.hint}>{s.description}</span>
          </div>
        )
      })}
    </div>
  )
}

function FollowPanel({ result, error, onClose }: { result: FollowResult | null; error: string; onClose: () => void }) {
  return (
    <div style={styles.panel}>
      <div style={styles.header}>
        <span>
          Follow {result ? <code>{keyText(result.location)}</code> : '…'} through the pipeline
        </span>
        <button style={styles.closeBtn} onClick={onClose} title="Close" type="button">×</button>
      </div>
      {error && <div style={styles.error}>{error}</div>}
      {result && (
        <>
          {result.steps.map(s => (
            <div key={s.variable} style={{ display: 'flex', gap: 8, margin: '2px 0' }} title={s.summary}>
              <span style={{ color: TONE_COLOR[s.tone], width: 12 }}>
                {s.tone === 'problem' ? '✕' : s.tone === 'excluded' ? '·' : s.tone === 'empty' ? '' : '●'}
              </span>
              <code style={{ minWidth: 140 }}>{s.variable}</code>
              <span style={{ color: '#bbb' }}>
                {s.ignored_keys.length > 0 && <span style={styles.hint}>(no {s.ignored_keys.join(', ')}) </span>}
                {s.summary}
              </span>
            </div>
          ))}
          <div style={result.lost_at.length ? styles.error : styles.message}>
            {result.lost_at.length
              ? `First missing although an input has it: ${result.lost_at.join(', ')}`
              : 'Not lost between variables.'}
          </div>
        </>
      )}
    </div>
  )
}
