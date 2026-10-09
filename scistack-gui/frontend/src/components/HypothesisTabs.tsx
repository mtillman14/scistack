/**
 * HypothesisTabs — top-level tab strip, one tab per hypothesis pipeline.
 *
 * A hypothesis is not a separate structure — it's a pipeline (see
 * ScopeContext/pipeline_store.py's nested-pipeline scopes) tagged with
 * research-question/evidence metadata. 'main' is simply the default
 * hypothesis, a sibling to every other one, not a special scratch scope.
 *
 * Switching tabs uses ScopeContext's `jumpToRoot`, which resets the
 * breadcrumb to a single crumb naming the hypothesis itself — hypotheses
 * are true top-level siblings, not scopes nested under 'main' (unlike the
 * Submodules sidebar's `jumpTo`, which does prepend the root crumb).
 * A tab reads as "current" whenever the breadcrumb's ROOT is that
 * hypothesis, so it stays highlighted while the user has descended into
 * one of its submodules.
 *
 * "Delete" hides the hypothesis rather than deleting its data (project
 * ethos — see pipeline_store.py's hide_pipeline); the toggle at the end of
 * the tab strip restores hidden ones, same pattern as PipelineDAG's
 * hidden-edges panel.
 *
 * Below the tab strip, an always-visible Research Question row shows/edits
 * the CURRENT hypothesis's research_question — the point being you
 * shouldn't have to hunt through a sidebar tab just to remember what
 * question you're answering. This row was the last consumer of
 * hypothesis_statement/evidence_for/evidence_against's UI (formerly the
 * sidebar's Hypothesis tab, `Sidebar/HypothesisPanel.tsx` — deleted once
 * Research Question moved here, since the panel had nothing left to show);
 * those backend fields still exist (HypothesisInfo), just with no editing
 * UI anywhere in the GUI today.
 */

import { useState, useEffect, useCallback, useRef } from 'react'
import { callBackend, isVSCodeMode } from '../api'
import { useBackendMessage } from '../hooks/useBackendMessage'
import { useScope } from '../context/ScopeContext'
import ProjectExportButton from './ProjectExportButton'
import VerifyButton from './VerifyButton'

export interface HypothesisInfo {
  pipeline_id: string
  name: string
  research_question: string
  hypothesis_statement: string
  evidence_for: string[]
  evidence_against: string[]
}

interface HiddenPipelineInfo {
  pipeline_id: string
  name: string
  is_hypothesis: boolean
}

export default function HypothesisTabs() {
  const { breadcrumb, jumpToRoot, renameInPath, bumpGraph } = useScope()
  const [hypotheses, setHypotheses] = useState<HypothesisInfo[]>([])
  const [hiddenPipelines, setHiddenPipelines] = useState<HiddenPipelineInfo[]>([])
  const [showHidden, setShowHidden] = useState(false)
  const [adding, setAdding] = useState(false)
  const [draft, setDraft] = useState('')
  const [error, setError] = useState('')
  const [renamingPid, setRenamingPid] = useState<string | null>(null)
  const [renameDraft, setRenameDraft] = useState('')
  const [duplicatingPid, setDuplicatingPid] = useState<string | null>(null)
  const [duplicateDraft, setDuplicateDraft] = useState('')
  const addInputRef = useRef<HTMLInputElement>(null)
  const renameInputRef = useRef<HTMLInputElement>(null)
  const duplicateInputRef = useRef<HTMLInputElement>(null)
  const importFileRef = useRef<HTMLInputElement>(null)

  const fetchHypotheses = useCallback(() => {
    callBackend('list_hypotheses')
      .then(d => setHypotheses((d as { hypotheses: HypothesisInfo[] }).hypotheses))
      .catch(console.error)
  }, [])

  const fetchHidden = useCallback(() => {
    callBackend('get_hidden_pipelines')
      .then(d => setHiddenPipelines((d as { pipelines: HiddenPipelineInfo[] }).pipelines))
      .catch(console.error)
  }, [])

  useEffect(() => { fetchHypotheses(); fetchHidden() }, [fetchHypotheses, fetchHidden])

  useBackendMessage(useCallback((msg) => {
    if (msg.type === 'dag_updated' || msg.method === 'dag_updated') { fetchHypotheses(); fetchHidden() }
  }, [fetchHypotheses, fetchHidden]))

  // If the breadcrumb's root was hidden in a prior session (e.g. 'main'
  // itself), the page reloads pointed at a scope that's no longer visible
  // — land on whatever hypothesis is actually there instead.
  useEffect(() => {
    if (hypotheses.length === 0) return
    const rootId = breadcrumb[0].pipeline_id
    if (!hypotheses.some(h => h.pipeline_id === rootId)) {
      jumpToRoot(hypotheses[0].pipeline_id, hypotheses[0].name)
    }
    // Only re-check when the hypothesis list changes, not on every breadcrumb move.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hypotheses])

  const handleRestore = (pid: string) => {
    callBackend('unhide_pipeline', { pipeline_id: pid })
      .then(() => { setError(''); fetchHypotheses(); fetchHidden() })
      .catch(err => setError((err as Error).message))
  }

  useEffect(() => {
    if (adding) addInputRef.current?.focus()
  }, [adding])

  useEffect(() => {
    if (renamingPid) renameInputRef.current?.focus()
  }, [renamingPid])

  useEffect(() => {
    if (duplicatingPid) duplicateInputRef.current?.focus()
  }, [duplicatingPid])

  const commitDraft = () => {
    const name = draft.trim()
    if (name) {
      callBackend('create_hypothesis', { name })
        .then((d) => {
          setError('')
          fetchHypotheses()
          const r = d as { pipeline_id: string; name: string }
          jumpToRoot(r.pipeline_id, r.name)
        })
        .catch(err => setError((err as Error).message))
    }
    setDraft('')
    setAdding(false)
  }

  const commitRename = () => {
    const pid = renamingPid
    const name = renameDraft.trim()
    setRenamingPid(null)
    setRenameDraft('')
    if (!pid || !name) return
    callBackend('rename_pipeline', { pipeline_id: pid, name })
      .then(() => {
        setError('')
        fetchHypotheses()
        renameInPath(pid, name)
        bumpGraph()
      })
      .catch(err => setError((err as Error).message))
  }

  const commitDuplicate = () => {
    const pid = duplicatingPid
    const name = duplicateDraft.trim()
    setDuplicatingPid(null)
    setDuplicateDraft('')
    if (!pid || !name) return
    callBackend('duplicate_hypothesis', { pipeline_id: pid, name })
      .then((d) => {
        setError('')
        fetchHypotheses()
        const r = d as { pipeline_id: string }
        jumpToRoot(r.pipeline_id, name)
      })
      .catch(err => setError((err as Error).message))
  }

  // Export/import between SciStack users (to-do #7) — the document only
  // (wiring/layout/config), never data/records. Export writes into the
  // project's exports/ dir (same "write into project dir, return the
  // path" pattern as the existing Report button) and, in standalone mode,
  // also triggers a browser download of the same JSON; VS Code webview
  // mode has no filesystem download, so it just shows the written path
  // (same limitation the Report button already has for opening tabs).
  const handleExport = (pid: string, name: string) => {
    callBackend('export_pipeline', { pipeline_id: pid })
      .then(d => {
        const r = d as { path: string; document: unknown }
        setError('')
        if (!isVSCodeMode) {
          const blob = new Blob([JSON.stringify(r.document, null, 2)], { type: 'application/json' })
          const url = URL.createObjectURL(blob)
          const a = document.createElement('a')
          a.href = url
          a.download = `${name.replace(/[^\w.-]+/g, '_')}.json`
          a.click()
          URL.revokeObjectURL(url)
        }
        window.alert(`Exported '${name}' to:\n${r.path}`)
      })
      .catch(err => setError((err as Error).message))
  }

  // Translate to a standalone script (to-do #6) — Python or MATLAB,
  // detected automatically from the closure's function languages; a
  // mixed-language closure comes back as a 400 (not supported yet).
  const handleExportCode = (pid: string, name: string) => {
    callBackend('export_pipeline_code', { pipeline_id: pid })
      .then(d => {
        const r = d as { path: string; language: 'python' | 'matlab'; script: string; warnings: string[] }
        setError('')
        if (!isVSCodeMode) {
          const ext = r.language === 'matlab' ? 'm' : 'py'
          const blob = new Blob([r.script], { type: 'text/plain' })
          const url = URL.createObjectURL(blob)
          const a = document.createElement('a')
          a.href = url
          a.download = `${name.replace(/[^\w.-]+/g, '_')}.${ext}`
          a.click()
          URL.revokeObjectURL(url)
        }
        const warningText = r.warnings.length > 0
          ? `\n\n${r.warnings.length} step(s) skipped (see comments in the script):\n${r.warnings.join('\n')}`
          : ''
        window.alert(`Exported '${name}' (${r.language}) to:\n${r.path}${warningText}`)
      })
      .catch(err => setError((err as Error).message))
  }

  // File API read is pure browser/JS (no filesystem-path dependency), so
  // this works identically in standalone and VS Code webview modes.
  const handleImportFile = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    e.target.value = ''  // allow re-selecting the same file again later
    if (!file) return
    const reader = new FileReader()
    reader.onload = () => {
      let parsed: unknown
      try {
        parsed = JSON.parse(reader.result as string)
      } catch {
        setError('Not a valid pipeline export file (invalid JSON)')
        return
      }
      callBackend('import_pipeline', { document: parsed })
        .then(async d => {
          const r = d as {
            pipeline_id: string
            reused: { pipelines: string[]; constants: string[]; path_inputs: string[]; sweeps: string[] }
            unresolved_labels: string[]
          }
          setError('')
          const listRes = await callBackend('list_hypotheses') as { hypotheses: HypothesisInfo[] }
          setHypotheses(listRes.hypotheses)
          fetchHidden()
          const imported = listRes.hypotheses.find(h => h.pipeline_id === r.pipeline_id)
          jumpToRoot(r.pipeline_id, imported?.name ?? r.pipeline_id)

          const reusedTotal = r.reused.pipelines.length + r.reused.constants.length + r.reused.path_inputs.length + r.reused.sweeps.length
          if (reusedTotal > 0 || r.unresolved_labels.length > 0) {
            const parts: string[] = []
            if (r.reused.pipelines.length > 0) {
              parts.push(`Reused ${r.reused.pipelines.length} existing submodule(s) with identical content (kept the shared local copy instead of duplicating): ${r.reused.pipelines.join(', ')}`)
            }
            const reusedGlobals = [...r.reused.constants, ...r.reused.path_inputs, ...r.reused.sweeps]
            if (reusedGlobals.length > 0) {
              parts.push(`Reused ${reusedGlobals.length} existing local definition(s) by name (kept as-is): ${reusedGlobals.join(', ')}`)
            }
            if (r.unresolved_labels.length > 0) {
              parts.push(`${r.unresolved_labels.length} function/variable name(s) not found locally yet: ${r.unresolved_labels.join(', ')}`)
            }
            window.alert(parts.join('\n\n'))
          }
        })
        .catch(err => setError((err as Error).message))
    }
    reader.readAsText(file)
  }

  const handleDelete = (pid: string) => {
    callBackend('delete_hypothesis', { pipeline_id: pid })
      .then(() => {
        setError('')
        fetchHypotheses()
        fetchHidden()
        if (pid === rootPipelineId) {
          // Land on whatever hypothesis is left, not a hardcoded 'main' —
          // 'main' itself may be the one just hidden.
          const remaining = hypotheses.find(h => h.pipeline_id !== pid)
          if (remaining) jumpToRoot(remaining.pipeline_id, remaining.name)
        } else {
          bumpGraph()
        }
      })
      .catch(err => setError((err as Error).message))
  }

  // A tab reads as "current" whenever the breadcrumb's root is that
  // hypothesis — stays highlighted while descended into a submodule.
  const rootPipelineId = breadcrumb[0].pipeline_id
  const currentHyp = hypotheses.find(h => h.pipeline_id === rootPipelineId) ?? null

  const [rqDraft, setRqDraft] = useState('')
  useEffect(() => {
    setRqDraft(currentHyp?.research_question ?? '')
    // Only re-seed when the CURRENT hypothesis changes — depending on the
    // question text itself would clobber in-progress typing every time a
    // dag_updated notification triggers fetchHypotheses.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentHyp?.pipeline_id])

  const saveRq = () => {
    if (!currentHyp || rqDraft === currentHyp.research_question) return
    callBackend('update_hypothesis', { pipeline_id: currentHyp.pipeline_id, research_question: rqDraft })
      .then(() => { setError(''); fetchHypotheses() })
      .catch(err => setError((err as Error).message))
  }

  return (
    <div>
    <div style={styles.root}>
      {hypotheses.map(h => (
        renamingPid === h.pipeline_id ? (
          <input
            key={h.pipeline_id}
            ref={renameInputRef}
            style={styles.renameInput}
            value={renameDraft}
            onChange={e => setRenameDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') commitRename()
              if (e.key === 'Escape') { setRenamingPid(null); setRenameDraft('') }
            }}
            onBlur={commitRename}
          />
        ) : duplicatingPid === h.pipeline_id ? (
          <input
            key={h.pipeline_id}
            ref={duplicateInputRef}
            style={styles.renameInput}
            value={duplicateDraft}
            placeholder={`${h.name} copy…`}
            onChange={e => setDuplicateDraft(e.target.value)}
            onKeyDown={e => {
              if (e.key === 'Enter') commitDuplicate()
              if (e.key === 'Escape') { setDuplicatingPid(null); setDuplicateDraft('') }
            }}
            onBlur={commitDuplicate}
          />
        ) : (
          <div
            key={h.pipeline_id}
            style={h.pipeline_id === rootPipelineId ? styles.tabActive : styles.tab}
            title={h.research_question || h.name}
          >
            <span onClick={() => jumpToRoot(h.pipeline_id, h.name)} style={styles.tabLabel}>
              {h.name}
            </span>
            <span style={styles.tabActions}>
              <button
                style={styles.rowBtn}
                title="Duplicate this hypothesis into a new tab (e.g. gait symmetry -> gait speed)"
                onClick={() => { setDuplicatingPid(h.pipeline_id); setDuplicateDraft(`${h.name} copy`) }}
              >
                ⎘
              </button>
              <button
                style={styles.rowBtn}
                title="Export this hypothesis (+ everything it uses) as a portable file to share with another SciStack user"
                onClick={() => handleExport(h.pipeline_id, h.name)}
              >
                ⇩
              </button>
              <button
                style={styles.rowBtn}
                title="Translate this hypothesis to a standalone Python/MATLAB script"
                onClick={() => handleExportCode(h.pipeline_id, h.name)}
              >
                {'</>'}
              </button>
              <button
                style={styles.rowBtn}
                title="Rename hypothesis"
                onClick={() => { setRenamingPid(h.pipeline_id); setRenameDraft(h.name) }}
              >
                ✎
              </button>
              {hypotheses.length >= 2 && (
                <button
                  style={styles.rowBtn}
                  title="Delete hypothesis (hides it — never deletes data; restore below)"
                  onClick={() => handleDelete(h.pipeline_id)}
                >
                  ×
                </button>
              )}
            </span>
          </div>
        )
      ))}
      {adding ? (
        <input
          ref={addInputRef}
          style={styles.renameInput}
          value={draft}
          placeholder="hypothesis name…"
          onChange={e => setDraft(e.target.value)}
          onKeyDown={e => {
            if (e.key === 'Enter') commitDraft()
            if (e.key === 'Escape') { setDraft(''); setAdding(false) }
          }}
          onBlur={commitDraft}
        />
      ) : (
        <button style={styles.addTab} onClick={() => setAdding(true)} title="New hypothesis">
          + new hypothesis
        </button>
      )}
      <input
        ref={importFileRef}
        type="file"
        accept=".json"
        style={{ display: 'none' }}
        onChange={handleImportFile}
      />
      <button
        style={styles.addTab}
        onClick={() => importFileRef.current?.click()}
        title="Import a pipeline exported by another SciStack user"
      >
        ⇧ import
      </button>
      <ProjectExportButton />
      <VerifyButton />
      {(() => {
        const hiddenHypotheses = hiddenPipelines.filter(p => p.is_hypothesis)
        if (hiddenHypotheses.length === 0) return null
        return (
          <span style={styles.hiddenWrap}>
            <button
              style={styles.hiddenToggle}
              onClick={() => setShowHidden(v => !v)}
              type="button"
            >
              {showHidden ? 'hide' : `${hiddenHypotheses.length} hidden — show`}
            </button>
            {showHidden && hiddenHypotheses.map(p => (
              <span key={p.pipeline_id} style={styles.hiddenRow}>
                {p.name}
                <button
                  style={styles.hiddenRestoreBtn}
                  onClick={() => handleRestore(p.pipeline_id)}
                  type="button"
                  title="Restore this hypothesis"
                >
                  restore
                </button>
              </span>
            ))}
          </span>
        )
      })()}
      {error && <span style={styles.errorText}>{error}</span>}
    </div>
    {currentHyp && (
      <div style={styles.rqRow}>
        <span style={styles.rqLabel}>Research Question</span>
        <input
          style={styles.rqInput}
          value={rqDraft}
          placeholder="What are we trying to find out?"
          onChange={e => setRqDraft(e.target.value)}
          onBlur={saveRq}
          onKeyDown={e => {
            if (e.key === 'Enter') (e.target as HTMLInputElement).blur()
          }}
        />
      </div>
    )}
    </div>
  )
}

const styles: Record<string, React.CSSProperties> = {
  rqRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '5px 12px',
    background: '#101024',
    borderBottom: '1px solid #2a2a4a',
    flexShrink: 0,
  },
  rqLabel: {
    fontSize: 10,
    fontWeight: 700,
    color: '#666',
    textTransform: 'uppercase',
    letterSpacing: 0.6,
    whiteSpace: 'nowrap',
    flexShrink: 0,
  },
  rqInput: {
    flex: 1,
    minWidth: 0,
    background: 'transparent',
    border: '1px solid transparent',
    borderRadius: 3,
    color: '#ddd',
    fontSize: 12,
    fontFamily: 'inherit',
    padding: '3px 6px',
    outline: 'none',
  },
  root: {
    display: 'flex',
    alignItems: 'center',
    gap: 2,
    padding: '0 12px',
    background: '#15152e',
    borderBottom: '1px solid #2a2a4a',
    flexShrink: 0,
    overflowX: 'auto',
  },
  tab: {
    display: 'flex',
    alignItems: 'center',
    padding: '0 4px 0 14px',
    borderBottom: '2px solid transparent',
    color: '#888',
    fontSize: 13,
    fontWeight: 500,
    whiteSpace: 'nowrap',
  },
  tabActive: {
    display: 'flex',
    alignItems: 'center',
    padding: '0 4px 0 14px',
    borderBottom: '2px solid #7b68ee',
    color: '#fff',
    fontSize: 13,
    fontWeight: 600,
    whiteSpace: 'nowrap',
  },
  tabLabel: {
    padding: '8px 6px 8px 0',
    cursor: 'pointer',
  },
  tabActions: {
    display: 'flex',
    alignItems: 'center',
  },
  addTab: {
    padding: '8px 10px',
    background: 'transparent',
    border: 'none',
    color: '#666',
    fontSize: 12,
    cursor: 'pointer',
    whiteSpace: 'nowrap',
  },
  renameInput: {
    background: '#1a1a2e',
    border: '1px solid #7b68ee',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 12,
    fontFamily: 'monospace',
    padding: '4px 6px',
    outline: 'none',
    margin: '4px 0',
  },
  errorText: {
    fontSize: 11,
    color: '#f87171',
    marginLeft: 8,
  },
  rowBtn: {
    flexShrink: 0,
    background: 'transparent',
    border: 'none',
    color: '#888',
    fontSize: 12,
    lineHeight: 1,
    cursor: 'pointer',
    padding: '4px 2px',
  },
  hiddenWrap: {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    marginLeft: 8,
  },
  hiddenToggle: {
    background: 'transparent',
    border: 'none',
    color: '#666',
    fontSize: 11,
    cursor: 'pointer',
    whiteSpace: 'nowrap',
    padding: '4px 0',
  },
  hiddenRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 4,
    fontSize: 11,
    color: '#aaa',
    whiteSpace: 'nowrap',
  },
  hiddenRestoreBtn: {
    background: 'transparent',
    border: '1px solid #3a3a5a',
    borderRadius: 3,
    color: '#9a8ff0',
    fontSize: 10,
    cursor: 'pointer',
    padding: '1px 5px',
  },
}
