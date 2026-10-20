/**
 * ModelFormulaDialog — the mixed model as an lme4 formula
 * (.claude/plan-statstudio-model-formula.md, stage 5).
 *
 * The TEXT is the single source of truth: the helpers (Add covariate…, Add
 * random term…, Reset) only edit it, and Apply sends it. The webview knows no
 * grammar and no statistics: every check (what each name means here, the
 * problems and their spans, the factors the formula adds or drops) comes from
 * `stats_formula_check` (scistackstats.model over csv-stats' grammar), and
 * Apply is `stats_formula_apply`, which returns the new spec. A factor change
 * is written to the design there, so the plot-divergence banner shows it.
 */

import { useEffect, useMemo, useRef, useState } from 'react'
import { callBackend } from '../../api'
import GroupingDagPopup, { type GroupingCandidate } from '../PlotStudio/GroupingDagPopup'
import { insertCovariate } from './modelFormulaText'
import type { CovariateBinding, StatsSpec } from './statsSpec'

interface CheckError { message: string; position?: number; length?: number }

interface Check {
  dv: string
  text?: string
  default?: string
  is_default?: boolean
  errors: CheckError[]
  warnings?: string[]
  terms?: { name: string; role: string; detail: string }[]
  add?: string[]
  drop?: string[]
  needs_mixed_model?: boolean
  offers?: { groups: string[]; factors: string[]; addable: string[]; covariates: string[] }
}

interface Props {
  spec: StatsSpec
  analysisId: string
  /** The first DV's variable: what covariates are offered against. */
  measure: string
  sourceParams: Record<string, unknown>
  /** No covariate picker on the CSV path: a flat table has nothing to join. */
  canPickCovariates: boolean
  onApply: (spec: StatsSpec) => void
  onCancel: () => void
}

/** The text with each error span marked, for the underline under the editor. */
function Marked({ text, errors }: { text: string; errors: CheckError[] }) {
  const spans = errors
    .filter(e => e.position != null)
    .map(e => [e.position!, e.position! + Math.max(1, e.length ?? 1)] as [number, number])
    .sort((a, b) => a[0] - b[0])
  if (!spans.length) return null
  const parts: React.ReactNode[] = []
  let at = 0
  spans.forEach(([s, e], i) => {
    if (s < at) return
    parts.push(text.slice(at, s))
    parts.push(<mark key={i} style={styles.mark}>{text.slice(s, e) || ' '}</mark>)
    at = e
  })
  parts.push(text.slice(at))
  return <div style={styles.marked}>{parts}</div>
}

export default function ModelFormulaDialog({
  spec, analysisId, measure, sourceParams, canPickCovariates, onApply, onCancel,
}: Props) {
  const analysis = spec.comparisons.find(a => a.id === analysisId) ?? spec.comparisons[0]
  const [text, setText] = useState<string>(analysis?.formula ?? '')
  const [covariates, setCovariates] = useState<CovariateBinding[]>(analysis?.covariates ?? [])
  const [check, setCheck] = useState<Check | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [picker, setPicker] = useState<{ candidates: GroupingCandidate[]; refusals: Record<string, string> } | null>(null)
  const [pendingNames, setPendingNames] = useState<string[]>([])
  const [termForm, setTermForm] = useState<{ group: string; intercept: boolean; slopes: string[]; correlated: boolean } | null>(null)
  const generation = useRef(0)

  // --- the live check (debounced, latest wins) ------------------------------
  useEffect(() => {
    const mine = ++generation.current
    const timer = window.setTimeout(() => {
      setBusy(true)
      callBackend('stats_formula_check', {
        spec, analysis_id: analysisId, text: text.trim() || null, covariates, ...sourceParams,
      }).then(raw => {
        if (mine !== generation.current) return
        const r = raw as { ok: boolean; checks: Check[] }
        const first = r.checks[0] ?? null
        setCheck(first)
        setError('')
        // A blank editor shows the model StatStudio fits by default, so the
        // user starts from it rather than from nothing.
        if (!text.trim() && first?.text) setText(first.text)
        console.info('[ModelFormula] check', { text, errors: first?.errors.length ?? 0, add: first?.add, drop: first?.drop })
      }).catch(err => { if (mine === generation.current) setError(String(err)) })
        .finally(() => { if (mine === generation.current) setBusy(false) })
    }, 300)
    return () => window.clearTimeout(timer)
  }, [text, covariates, spec, analysisId, sourceParams])

  // Names of newly picked covariates come back from the backend (one owner of
  // a covariate's name); then they are written into the fixed part, before
  // the first random term.
  useEffect(() => {
    if (!pendingNames.length) return
    setText(current => pendingNames.reduce(insertCovariate, current))
    setPendingNames([])
  }, [pendingNames])

  // --- Add covariate… (the grouping picker, purpose "covariate") ------------
  const openPicker = async () => {
    try {
      const r = await callBackend('plot_grouping_graph', { variable: measure, purpose: 'covariate', ...sourceParams }) as
        { offered: GroupingCandidate[]; rejected: Record<string, string> }
      setPicker({ candidates: r.offered, refusals: r.rejected })
    } catch (err) {
      setError(String(err))
    }
  }

  const applyPicked = async (picked: { variable: string; column: string | null; variant: Record<string, unknown> }[]) => {
    setPicker(null)
    const key = (c: { variable: string; column: string | null }) => `${c.variable}\u0000${c.column ?? ''}`
    const have = new Set(covariates.map(key))
    const added = picked.filter(p => !have.has(key(p)))
    if (!added.length) return
    const next = [...covariates, ...added.map(p => ({ variable: p.variable, column: p.column, variant: p.variant }))]
    setCovariates(next)
    // The backend names them; ask once with the new bindings, then insert.
    try {
      const r = await callBackend('stats_formula_check', {
        spec, analysis_id: analysisId, text: text.trim() || null, covariates: next, ...sourceParams,
      }) as { checks: Check[] }
      const names = r.checks[0]?.offers?.covariates ?? []
      const before = new Set(check?.offers?.covariates ?? [])
      setPendingNames(names.filter(n => !before.has(n)))
    } catch (err) {
      setError(String(err))
    }
  }

  // --- Add random term… --------------------------------------------------------
  const groupChoices = useMemo(() => {
    const groups = check?.offers?.groups ?? []
    const factors = check?.offers?.factors ?? []
    const unit = groups[0]
    return [...groups, ...(unit ? factors.map(f => `${unit}:${f}`) : [])]
  }, [check])
  const slopeChoices = useMemo(
    () => [...(check?.offers?.factors ?? []), ...(check?.offers?.covariates ?? [])],
    [check],
  )
  const insertTerm = () => {
    if (!termForm || !termForm.group) return
    const lhs = [termForm.intercept ? '1' : '0', ...termForm.slopes].join(' + ')
    setText(current => `${current.trim()} + (${lhs} ${termForm.correlated ? '|' : '||'} ${termForm.group})`)
    setTermForm(null)
  }

  const apply = async () => {
    try {
      const r = await callBackend('stats_formula_apply', {
        spec, analysis_id: analysisId, text: text.trim() || null, covariates, ...sourceParams,
      }) as { ok: boolean; spec?: StatsSpec; errors?: CheckError[]; design_changed?: boolean }
      if (!r.ok || !r.spec) {
        setError((r.errors ?? []).map(e => e.message).join('; ') || 'Not applied.')
        return
      }
      console.info('[ModelFormula] applied', { analysisId, designChanged: r.design_changed })
      onApply(r.spec)
    } catch (err) {
      setError(String(err))
    }
  }

  const errors = check?.errors ?? []
  return (
    <div style={styles.overlay} role="dialog" aria-label="Model formula">
      <div style={styles.dialog}>
        <div style={styles.title}>Model formula{busy ? ' · checking…' : ''}</div>
        <div style={styles.muted}>
          lme4 spelling. The fixed part is one crossing of factors and covariates (<code>group * time * age</code>);
          random terms are <code>(1 | subject)</code>, <code>(1 + time | subject)</code>, <code>(1 + x || subject)</code>.
          The text is what runs; the buttons below only edit it.
        </div>
        <div style={styles.editorRow}>
          <span style={styles.lhs} title="The left side is each measure">{measure} ~</span>
          <div style={{ flex: 1 }}>
            <textarea value={text} onChange={e => setText(e.target.value)} spellCheck={false}
                      rows={3} style={styles.editor} aria-invalid={errors.length > 0} />
            <Marked text={text} errors={errors} />
          </div>
        </div>
        {errors.map((e, i) => <div key={i} style={styles.error}>✕ {e.message}</div>)}
        {(check?.warnings ?? []).map((w, i) => <div key={i} style={styles.warn}>{w}</div>)}
        {error && <div style={styles.error}>{error}</div>}

        {check?.terms && check.terms.length > 0 && (
          <table style={styles.terms}>
            <tbody>
              {check.terms.map((t, i) => (
                <tr key={i}>
                  <td style={styles.termName}>{t.name}</td>
                  <td style={styles.termRole}>{t.role}</td>
                  <td style={styles.muted}>{t.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {((check?.add?.length ?? 0) > 0 || (check?.drop?.length ?? 0) > 0) && (
          <div style={styles.note}>
            Apply changes the table's design
            {check?.add?.length ? `: adds ${check.add.join(', ')} as grouping layer(s)` : ''}
            {check?.drop?.length ? `${check?.add?.length ? ';' : ':'} averages over ${check.drop.join(', ')} (Collapse)` : ''}.
            A linked figure then shows the design-differs banner, with Sync to PlotStudio.
          </div>
        )}
        {check?.needs_mixed_model && (
          <div style={styles.note}>Covariates or custom random effects: this analysis will use the linear mixed model.</div>
        )}

        <div style={styles.helpers}>
          <button type="button" style={styles.button} onClick={openPicker} disabled={!canPickCovariates}
                  title={canPickCovariates ? 'Pick a numeric variable on the pipeline graph' : 'A CSV file has no variables to join'}>
            Add covariate…
          </button>
          <button type="button" style={styles.button}
                  onClick={() => setTermForm({ group: groupChoices[0] ?? '', intercept: true, slopes: [], correlated: true })}>
            Add random term…
          </button>
          <button type="button" style={styles.button} onClick={() => { setText(check?.default ?? ''); setCovariates([]) }}
                  disabled={!check?.default}>
            Reset to default
          </button>
          {covariates.length > 0 && (
            <span style={styles.muted}>
              Covariates: {covariates.map(c => (c.column ? `${c.variable}.${c.column}` : c.variable)).join(', ')}
            </span>
          )}
        </div>

        {termForm && (
          <div style={styles.termForm}>
            <label style={styles.field}>Group
              <select value={termForm.group} onChange={e => setTermForm({ ...termForm, group: e.target.value })} style={styles.select}>
                {groupChoices.map(g => <option key={g} value={g}>{g}</option>)}
              </select>
            </label>
            <label style={styles.check}>
              <input type="checkbox" checked={termForm.intercept}
                     onChange={e => setTermForm({ ...termForm, intercept: e.target.checked })} /> intercept
            </label>
            {slopeChoices.map(s => (
              <label key={s} style={styles.check}>
                <input type="checkbox" checked={termForm.slopes.includes(s)}
                       onChange={e => setTermForm({
                         ...termForm,
                         slopes: e.target.checked ? [...termForm.slopes, s] : termForm.slopes.filter(x => x !== s),
                       })} /> slope {s}
              </label>
            ))}
            <label style={styles.check}>
              <input type="checkbox" checked={termForm.correlated}
                     onChange={e => setTermForm({ ...termForm, correlated: e.target.checked })} /> correlated (|, not ||)
            </label>
            <button type="button" style={styles.button} onClick={insertTerm} disabled={!termForm.group}>Insert</button>
            <button type="button" style={styles.button} onClick={() => setTermForm(null)}>✕</button>
          </div>
        )}

        <div style={styles.footer}>
          <button type="button" style={styles.button} onClick={onCancel}>Cancel</button>
          <button type="button" style={styles.primary} onClick={apply} disabled={errors.length > 0 || busy || !check}>
            Apply
          </button>
        </div>
      </div>

      {picker && (
        <GroupingDagPopup
          purpose="covariate"
          measure={measure}
          candidates={picker.candidates}
          refusals={picker.refusals}
          selected={covariates.map(c => ({ variable: c.variable, column: c.column }))}
          sourceParams={sourceParams}
          onCancel={() => setPicker(null)}
          onApply={applyPicked}
        />
      )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  overlay: { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50 },
  dialog: { width: 'min(760px, 92vw)', maxHeight: '88vh', overflow: 'auto', background: 'var(--ps-surface)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 6, padding: 14, display: 'flex', flexDirection: 'column', gap: 8, fontSize: 13 },
  title: { fontSize: 15, fontWeight: 600, color: 'var(--ps-text-strong)' },
  muted: { color: 'var(--ps-text-muted)', fontSize: 12 },
  editorRow: { display: 'flex', gap: 8, alignItems: 'flex-start' },
  lhs: { fontFamily: 'monospace', paddingTop: 6, whiteSpace: 'nowrap', color: 'var(--ps-text-secondary)' },
  editor: { width: '100%', boxSizing: 'border-box', fontFamily: 'monospace', fontSize: 13, background: 'var(--ps-field)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: 6, resize: 'vertical' },
  marked: { fontFamily: 'monospace', fontSize: 13, whiteSpace: 'pre-wrap', color: 'transparent', padding: '0 7px' },
  mark: { background: 'none', color: 'var(--ps-error)', textDecoration: 'underline wavy var(--ps-error)' },
  error: { color: 'var(--ps-error)', fontSize: 12 },
  warn: { color: 'var(--ps-warn)', fontSize: 12 },
  note: { background: 'var(--ps-note-bg)', border: '1px solid var(--ps-note-border)', color: 'var(--ps-note-text)', borderRadius: 4, padding: 6, fontSize: 12 },
  terms: { borderCollapse: 'collapse', fontSize: 12 },
  termName: { fontFamily: 'monospace', padding: '1px 10px 1px 0' },
  termRole: { padding: '1px 10px 1px 0', color: 'var(--ps-accent-text)' },
  helpers: { display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' },
  termForm: { display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap', border: '1px solid var(--ps-border-faint)', borderRadius: 4, padding: 6 },
  field: { display: 'flex', alignItems: 'center', gap: 4, fontSize: 12 },
  check: { display: 'flex', alignItems: 'center', gap: 3, fontSize: 12 },
  select: { background: 'var(--ps-field)', color: 'var(--ps-text)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '2px 4px', fontSize: 12 },
  footer: { display: 'flex', justifyContent: 'flex-end', gap: 6, marginTop: 4 },
  button: { background: 'var(--ps-control)', color: 'var(--ps-text-control)', border: '1px solid var(--ps-border)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer', fontSize: 12 },
  primary: { background: 'var(--ps-accent)', color: 'var(--ps-accent-fg)', border: '1px solid var(--ps-accent-border)', borderRadius: 3, padding: '3px 12px', cursor: 'pointer', fontSize: 12 },
}
