/**
 * StatStudio's step-by-step view (plan-statstudio-guided).
 *
 * One decision per screen: the step's question, choice cards with the
 * recommended answer marked, the facts about this data, and a prominent
 * panel explaining every idea the step names. All of that text comes from
 * the backend (scistackstats.explain via `guide`); this component lays it
 * out and applies a choice's action through `guideSteps.applyAction`. It
 * holds no statistics vocabulary (a test in guideSteps.test.ts checks).
 *
 * Navigation rules (user decision 2026-10-20) live in guideSteps.ts: jump
 * back to any finished step, never forward past a blocked one.
 */

import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { callBackend } from '../../../api'
import type { StatsSpec } from '../statsSpec'
import {
  applyAction, canGoTo, currentStep, goTo, next, previous, stepStatus,
  type GuideChoice, type GuideConcept, type GuidePayload, type GuideStep, type StepStatus,
} from './guideSteps'

interface Props {
  spec: StatsSpec
  onSpec: (next: StatsSpec) => void
  guide: GuidePayload | null
  busy: boolean
  /** Title bar, view switch, link banner and messages. */
  header: ReactNode
  /** The list of measures with "add" (the first step). */
  measureEditor: ReactNode
  /** The tables, sentences, export and save (the last step). */
  results: ReactNode
}

function log(event: string, step: string | null, detail?: string) {
  callBackend('stats_guide_log', { event, step, detail: detail ?? null }).catch(() => undefined)
}

const STATUS_MARK: Record<StepStatus, string> = { done: '✓', current: '●', blocked: '!', open: '○', locked: '·' }

export default function GuidedStudio({ spec, onSpec, guide, busy, header, measureEditor, results }: Props) {
  const steps = useMemo(() => guide?.steps ?? [], [guide])
  const stepId = currentStep(spec, steps)
  const step = steps.find(s => s.id === stepId) ?? null
  const index = steps.findIndex(s => s.id === stepId)
  // The idea behind the choice the pointer is on, shown first in the panel.
  const [focusConcept, setFocusConcept] = useState<string | null>(null)
  useEffect(() => { setFocusConcept(null) }, [stepId])
  useEffect(() => { if (stepId) log('show', stepId) }, [stepId])

  if (!guide || !step) {
    return (
      <div style={styles.shell}>
        {header}
        <div style={styles.muted}>{busy ? 'Reading your data…' : 'Preparing the steps…'}</div>
      </div>
    )
  }

  const choose = (choice: GuideChoice) => {
    if (choice.disabled || choice.selected) return
    log('choose', step.id, choice.id)
    onSpec(applyAction(spec, choice.action))
  }
  const waiting = step.needs_results && !guide.has_results

  return (
    <div style={styles.shell}>
      {header}
      <div style={styles.layout}>
        <nav style={styles.rail} aria-label="Steps">
          {steps.map((s, i) => {
            const status = stepStatus(spec, steps, s.id)
            const reachable = canGoTo(spec, steps, s.id)
            return (
              <button key={s.id} type="button" disabled={!reachable}
                      style={{ ...styles.railItem, ...(s.id === stepId ? styles.railItemOn : {}),
                               ...(status === 'locked' ? styles.railItemLocked : {}),
                               ...(status === 'blocked' ? styles.railItemBlocked : {}) }}
                      title={s.blocked ?? (reachable ? undefined : 'Finish the steps before this one first')}
                      onClick={() => { log('jump', s.id); onSpec(goTo(spec, steps, s.id)) }}>
                <span style={styles.railMark}>{STATUS_MARK[status]}</span>
                <span>{i + 1}. {s.title}</span>
              </button>
            )
          })}
        </nav>

        <main style={styles.main}>
          <div style={styles.stepCount}>Step {index + 1} of {steps.length}</div>
          <h2 style={styles.question}>{step.question}</h2>

          {step.blocked && <div style={styles.blocked} role="alert"><b>Before going on:</b> {step.blocked}</div>}
          {step.warnings.map((w, i) => <div key={i} style={styles.warning}>{w}</div>)}

          {step.facts.length > 0 && (
            <section style={styles.facts} aria-label="Your data">
              <div style={styles.boxTitle}>Your data</div>
              <ul style={styles.list}>{step.facts.map((f, i) => <li key={i}>{f}</li>)}</ul>
            </section>
          )}

          {step.table && <CellTable table={step.table} />}

          {step.recommendation && (
            <div style={styles.recommendation}><b>Recommended.</b> {step.recommendation}</div>
          )}

          {step.id === 'measure' && <div style={styles.card}>{measureEditor}</div>}

          {waiting && <div style={styles.muted}>Running the tests on your data…</div>}

          {step.cards.map(card => (
            <section key={card.id} style={styles.card}>
              <div style={styles.cardTitle}>{card.title}</div>
              {card.lay && <div style={styles.cardLay}>{card.lay}</div>}
              <div style={styles.choices} role="radiogroup" aria-label={card.title}>
                {card.choices.map(choice => (
                  <button key={choice.id} type="button" role="radio" aria-checked={choice.selected}
                          disabled={!!choice.disabled}
                          title={choice.disabled ?? undefined}
                          style={{ ...styles.choice, ...(choice.selected ? styles.choiceOn : {}),
                                   ...(choice.disabled ? styles.choiceOff : {}) }}
                          onMouseEnter={() => setFocusConcept(choice.concept)}
                          onFocus={() => setFocusConcept(choice.concept)}
                          onClick={() => choose(choice)}>
                    <span style={styles.choiceHead}>
                      <span style={styles.radio}>{choice.selected ? '◉' : '○'}</span>
                      <span style={styles.choiceLabel}>{choice.label}</span>
                      {choice.recommended && <span style={styles.badge}>Recommended</span>}
                    </span>
                    {choice.lay && <span style={styles.choiceLay}>{choice.lay}</span>}
                    {choice.disabled && <span style={styles.choiceLay}>{choice.disabled}</span>}
                  </button>
                ))}
              </div>
            </section>
          ))}

          {step.id === 'results' && guide.has_results && results}

          <div style={styles.nav}>
            <button type="button" style={styles.button} disabled={index <= 0}
                    onClick={() => { log('back', step.id); onSpec(previous(spec, steps)) }}>← Back</button>
            <span style={{ flex: 1 }} />
            {index < steps.length - 1 && (
              <button type="button" style={styles.primary} disabled={!!step.blocked || waiting}
                      title={step.blocked ?? undefined}
                      onClick={() => { log('next', step.id); onSpec(next(spec, steps)) }}>Next →</button>
            )}
          </div>
        </main>

        <ExplainPanel step={step} concepts={guide.concepts} focus={focusConcept} />
      </div>
    </div>
  )
}

function CellTable({ table }: { table: NonNullable<GuideStep['table']> }) {
  const empty = new Set(table.empty_rows ?? [])
  return (
    <div style={styles.tableBox}>
      <table style={styles.table}>
        <thead><tr>{table.columns.map(c => <th key={c} style={styles.th}>{c}</th>)}</tr></thead>
        <tbody>
          {table.rows.map((row, i) => (
            <tr key={i} style={empty.has(i) ? styles.emptyRow : undefined}>
              {row.map((cell, j) => <td key={j} style={styles.td}>{cell ?? ''}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ConceptBlock({ concept, highlight }: { concept: GuideConcept; highlight?: boolean }) {
  const [open, setOpen] = useState(false)
  return (
    <div style={{ ...styles.concept, ...(highlight ? styles.conceptOn : {}) }}>
      <div style={styles.conceptTitle}>{concept.title}</div>
      <div style={styles.technical}>{concept.technical}</div>
      <div>{concept.lay}</div>
      {concept.more && (
        <>
          <button type="button" style={styles.link} onClick={() => setOpen(!open)} aria-expanded={open}>
            {open ? 'Less' : 'Learn more'}
          </button>
          {open && <div style={styles.more}>{concept.more}</div>}
        </>
      )}
    </div>
  )
}

function ExplainPanel({ step, concepts, focus }: { step: GuideStep; concepts: Record<string, GuideConcept>; focus: string | null }) {
  const ids = step.concepts.filter(id => concepts[id])
  const focused = focus && concepts[focus] ? concepts[focus] : null
  return (
    <aside style={styles.explain} aria-label="What does this mean?">
      <div style={styles.explainTitle}>What does this mean?</div>
      {focused && !ids.includes(focused.id) && <ConceptBlock concept={focused} highlight />}
      {ids.map(id => <ConceptBlock key={id} concept={concepts[id]} highlight={id === focus} />)}
    </aside>
  )
}

const styles: Record<string, React.CSSProperties> = {
  shell: { height: '100%', overflow: 'auto', padding: 12, boxSizing: 'border-box' },
  layout: { display: 'flex', flexWrap: 'wrap', gap: 16, alignItems: 'flex-start' },
  rail: { display: 'flex', flexDirection: 'column', gap: 2, width: 190, flex: '0 0 190px' },
  railItem: { display: 'flex', gap: 8, alignItems: 'center', textAlign: 'left', background: 'none', border: '1px solid transparent', borderRadius: 4, padding: '5px 8px', color: 'var(--ps-text)', cursor: 'pointer', fontSize: 13 },
  railItemOn: { background: 'var(--ps-accent-bg-strong)', border: '1px solid var(--ps-accent-border)', color: 'var(--ps-text-strong)', fontWeight: 600 },
  railItemLocked: { color: 'var(--ps-text-muted)', cursor: 'default' },
  railItemBlocked: { color: 'var(--ps-warn)' },
  railMark: { width: 14, textAlign: 'center' },
  main: { flex: '1 1 420px', minWidth: 300, maxWidth: 820, display: 'flex', flexDirection: 'column', gap: 10 },
  stepCount: { color: 'var(--ps-text-muted)', fontSize: 12 },
  question: { fontSize: 22, fontWeight: 600, margin: '0 0 4px', color: 'var(--ps-text-strong)' },
  blocked: { border: '1px solid var(--ps-warn)', borderLeft: '4px solid var(--ps-warn)', borderRadius: 4, padding: '8px 10px' },
  warning: { background: 'var(--ps-warn-bg)', border: '1px solid var(--ps-warn-border)', borderRadius: 4, padding: '6px 10px', fontSize: 13 },
  facts: { background: 'var(--ps-surface)', border: '1px solid var(--ps-border-faint)', borderRadius: 6, padding: '8px 12px' },
  boxTitle: { fontWeight: 600, marginBottom: 4, color: 'var(--ps-text-strong)' },
  list: { margin: 0, paddingLeft: 18, display: 'flex', flexDirection: 'column', gap: 3 },
  recommendation: { background: 'var(--ps-note-bg)', border: '1px solid var(--ps-note-border)', color: 'var(--ps-note-text)', borderRadius: 6, padding: '8px 12px' },
  card: { border: '1px solid var(--ps-border)', borderRadius: 6, padding: 12, display: 'flex', flexDirection: 'column', gap: 8 },
  cardTitle: { fontWeight: 600, fontSize: 15, color: 'var(--ps-text-strong)' },
  cardLay: { color: 'var(--ps-text-secondary)', fontSize: 13 },
  choices: { display: 'flex', flexDirection: 'column', gap: 6 },
  choice: { display: 'flex', flexDirection: 'column', gap: 3, textAlign: 'left', background: 'var(--ps-surface)', border: '1px solid var(--ps-border-soft)', borderRadius: 6, padding: '8px 10px', color: 'var(--ps-text)', cursor: 'pointer', fontSize: 13 },
  choiceOn: { border: '2px solid var(--ps-accent-border)', background: 'var(--ps-accent-bg-strong)' },
  choiceOff: { opacity: 0.55, cursor: 'not-allowed' },
  choiceHead: { display: 'flex', alignItems: 'center', gap: 8 },
  radio: { width: 14 },
  choiceLabel: { fontWeight: 600 },
  choiceLay: { color: 'var(--ps-text-secondary)', paddingLeft: 22 },
  badge: { marginLeft: 'auto', background: 'var(--ps-accent)', color: 'var(--ps-accent-fg)', borderRadius: 10, padding: '1px 8px', fontSize: 11 },
  tableBox: { overflowX: 'auto' },
  table: { borderCollapse: 'collapse', fontSize: 12 },
  th: { textAlign: 'left', padding: '3px 10px', borderBottom: '1px solid var(--ps-border-strong)' },
  td: { padding: '2px 10px', fontVariantNumeric: 'tabular-nums' },
  emptyRow: { background: 'var(--ps-warn-bg)', fontWeight: 600 },
  nav: { display: 'flex', alignItems: 'center', gap: 8, marginTop: 8 },
  button: { background: 'var(--ps-control)', color: 'var(--ps-text-control)', border: '1px solid var(--ps-border)', borderRadius: 4, padding: '6px 14px', cursor: 'pointer', fontSize: 13 },
  primary: { background: 'var(--ps-accent)', color: 'var(--ps-accent-fg)', border: '1px solid var(--ps-accent-border)', borderRadius: 4, padding: '6px 18px', cursor: 'pointer', fontSize: 13, fontWeight: 600 },
  explain: { flex: '1 1 280px', maxWidth: 380, minWidth: 260, background: 'var(--ps-note-bg)', border: '1px solid var(--ps-note-border)', borderRadius: 8, padding: 12, display: 'flex', flexDirection: 'column', gap: 10, position: 'sticky', top: 0 },
  explainTitle: { fontSize: 16, fontWeight: 600, color: 'var(--ps-note-text)' },
  concept: { display: 'flex', flexDirection: 'column', gap: 3, fontSize: 13, lineHeight: 1.45, borderLeft: '3px solid transparent', paddingLeft: 8 },
  conceptOn: { borderLeft: '3px solid var(--ps-accent-border)' },
  conceptTitle: { fontWeight: 600, color: 'var(--ps-text-strong)' },
  technical: { fontStyle: 'italic', color: 'var(--ps-text-muted)', fontSize: 12 },
  more: { color: 'var(--ps-text-secondary)' },
  link: { background: 'none', border: 'none', padding: 0, color: 'var(--ps-accent-text)', cursor: 'pointer', textAlign: 'left', fontSize: 12 },
  muted: { color: 'var(--ps-text-muted)', fontSize: 13, margin: '6px 0' },
}
