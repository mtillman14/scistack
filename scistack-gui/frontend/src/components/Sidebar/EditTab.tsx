/**
 * EditTab — palette of draggable function, variable, parameter and
 * path-input nodes.
 *
 * Drag an item onto the canvas to place a new node.
 * The drag payload is JSON in the 'application/scistack-node' dataTransfer key:
 *   { nodeType: 'functionNode' | 'variableNode' | 'parameterNode' | 'pathInputNode', label: string }
 *
 * The five categories (Submodules, Functions, Variables, Parameters, Path
 * Inputs) are shown one at a time behind an icon tab strip rather than
 * stacked. Constants and Sweeps are ONE category — a Parameter is a named
 * thing with one or more values, and which source form declares it is not a
 * distinction the user makes (D6, docs/claude/entity-editability-model.md).
 * Clicking (not dragging) a list item selects it and opens the info panel
 * docked to the bottom of the sidebar: a read-only signature+docstring for
 * functions, or a free-text notes textarea (persisted server-side, see
 * layout.py's read_notes/write_note) for everything else. The selection
 * lives in SidebarSelectionContext so the canvas (PipelineDAG's
 * onPaneClick) can clear it too.
 */

import { useEffect, useState, useRef, useCallback } from 'react'
import { callBackend, isVSCodeMode } from '../../api'
import { useBackendMessage } from '../../hooks/useBackendMessage'
import { useScope } from '../../context/ScopeContext'
import { useSidebarSelection } from '../../context/SidebarSelectionContext'
import type { SidebarItemKind, SidebarSelectedItem } from '../../context/SidebarSelectionContext'
import { SourceLocationDialog } from '../SourceLocationDialog'
import type { SourceLocation } from '../SourceLocationDialog'
import { formatLocation } from './sourceLocation'

interface LoadError {
  source: string
  error: string
}

interface Registry {
  functions: string[]
  variables: string[]
  matlab_functions?: string[]
  matlab_functions_mismatched?: string[]
  /**
   * {function name -> 'process' | 'plot' | 'stat' | 'glue'} from
   * scidb.discover.function_role. The classifier deliberately lives in
   * scidb — the prefixes already drive execution behaviour there, so the GUI
   * must not hold a second copy of the strings (CLAUDE.md NOTE 3). The
   * dropdown below is pure display and belongs here; what a role IS does not.
   */
  function_roles?: Record<string, FunctionRole>
  load_errors?: LoadError[]
}

type FunctionRole = 'process' | 'plot' | 'stat' | 'glue'

/**
 * Roles in presentation order, plus the "All" escape hatch.
 *
 * Defaulting to Process is a behaviour change from the old
 * show-everything list, so each option carries a COUNT: nothing becomes
 * invisible, only collapsed. Without counts a user with existing plot_
 * functions would simply see them vanish.
 */
const ROLE_FILTERS: { id: FunctionRole | 'all'; label: string }[] = [
  { id: 'process', label: 'Process' },
  { id: 'plot', label: 'Plots' },
  { id: 'stat', label: 'Stats' },
  { id: 'glue', label: 'Glue' },
  { id: 'all', label: 'All' },
]

/** Per-project persistence for the last role selection (see write_note). */
const ROLE_NOTE_KEY = 'ui:function_role'

interface PipelineInfo {
  pipeline_id: string
  name: string
}

interface HiddenPipelineInfo {
  pipeline_id: string
  name: string
  is_hypothesis: boolean
}

interface ParameterListItem {
  name: string
  source_file?: string | null
  source_line?: number | null
  declared_in_entities_file?: boolean
}

interface TabDef {
  id: SidebarItemKind
  icon: string
  label: string
}

const TABS: TabDef[] = [
  { id: 'submodule', icon: '⧉', label: 'Submodules' },
  { id: 'function', icon: 'f(x)', label: 'Functions' },
  { id: 'variable', icon: 'x', label: 'Variables' },
  { id: 'parameter', icon: 'P', label: 'Parameters' },
  { id: 'pathInput', icon: '📁', label: 'Path Inputs' },
]

export default function EditTab() {
  const [activeTab, setActiveTab] = useState<TabDef['id']>('submodule')
  const { selectedItem, setSelectedItem } = useSidebarSelection()

  const selectTab = (tab: TabDef['id']) => {
    setActiveTab(tab)
    setSelectedItem(null)
  }

  const [registry, setRegistry] = useState<Registry>({ functions: [], variables: [] })
  // Which function role the Functions list is filtered to. Defaults to
  // Process — the ordinary pipeline step — so a project that accumulates
  // dozens of one-line glue reshapers doesn't drown its real steps.
  const [roleFilter, setRoleFilter] = useState<FunctionRole | 'all'>('process')
  // discoveryError is the request itself failing (network/RPC error) — a
  // real problem, shown here as a banner. registry.load_errors (some
  // module/file failing to import server-side) is NOT shown here: in
  // loose-script/folder-scan mode it's routinely full of framework/example
  // files that were never meant to be pipeline code, so surfacing it as an
  // always-on red banner reads as a process failure when it usually isn't.
  // It's still fully visible, per-module, in 📁 Paths → Discovered Code
  // (components/Sidebar/ProjectConfigPanel.tsx) for when it's worth digging into.
  const [discoveryError, setDiscoveryError] = useState('')
  const [parameters, setParameters] = useState<ParameterListItem[]>([])
  const [paramContextMenu, setParamContextMenu] = useState<{ x: number; y: number; item: ParameterListItem } | null>(null)
  const [paramSourceLoc, setParamSourceLoc] = useState<SourceLocation | null>(null)
  const [addingConst, setAddingConst] = useState(false)
  const [constDraft, setConstDraft] = useState('')
  const constInputRef = useRef<HTMLInputElement>(null)

  const [pathInputs, setPathInputs] = useState<string[]>([])
  const [addingPI, setAddingPI] = useState(false)
  const [piDraft, setPiDraft] = useState('')
  const [piError, setPiError] = useState('')
  const [piSubmitting, setPiSubmitting] = useState(false)
  const piInputRef = useRef<HTMLInputElement>(null)


  const [addingVar, setAddingVar] = useState(false)
  const [varDraft, setVarDraft] = useState('')
  const [varError, setVarError] = useState('')
  const [varSubmitting, setVarSubmitting] = useState(false)
  const varInputRef = useRef<HTMLInputElement>(null)

  // New glue node: a name and a language, nothing else. The body is the
  // two-line minimum that parses; the user writes the real thing in the
  // node's code panel.
  const [addingGlue, setAddingGlue] = useState(false)
  const [glueLang, setGlueLang] = useState<'python' | 'matlab'>('python')
  const [glueDraft, setGlueDraft] = useState('')
  const [glueError, setGlueError] = useState('')
  const [glueSubmitting, setGlueSubmitting] = useState(false)
  const glueInputRef = useRef<HTMLInputElement>(null)

  // Manual built-in/library function reference (numpy.mean, a MATLAB
  // builtin, ...) — distinct from auto-discovered functions above.
  const [addingBuiltin, setAddingBuiltin] = useState(false)
  const [builtinLang, setBuiltinLang] = useState<'python' | 'matlab'>('python')
  const [builtinDraft, setBuiltinDraft] = useState('')
  const [builtinError, setBuiltinError] = useState('')
  const [builtinSubmitting, setBuiltinSubmitting] = useState(false)
  const builtinInputRef = useRef<HTMLInputElement>(null)

  // Nested pipelines: the scopes list + navigation state. Hypothesis-tagged
  // pipelines get their own tab strip (HypothesisTabs) — this list is
  // submodules only, so drag-onto-canvas here always means "place a
  // reusable submodule," never "place a whole hypothesis."
  const { currentScope, jumpTo, renameInPath, bumpGraph, graphVersion } = useScope()
  const [pipelines, setPipelines] = useState<PipelineInfo[]>([])
  const [hypothesisIds, setHypothesisIds] = useState<Set<string>>(new Set())
  const [hiddenPipelines, setHiddenPipelines] = useState<HiddenPipelineInfo[]>([])
  const [showHiddenPipelines, setShowHiddenPipelines] = useState(false)
  const [addingPipe, setAddingPipe] = useState(false)
  const [pipeDraft, setPipeDraft] = useState('')
  const [pipeError, setPipeError] = useState('')
  const [renamingPid, setRenamingPid] = useState<string | null>(null)
  const [renameDraft, setRenameDraft] = useState('')
  const pipeInputRef = useRef<HTMLInputElement>(null)
  const renameInputRef = useRef<HTMLInputElement>(null)

  // Free-text notes (everything except functions) — fetched once, updated
  // locally after each successful save so the textarea doesn't flash.
  const [notes, setNotes] = useState<Record<string, string>>({})
  const fetchNotes = useCallback(() => {
    callBackend('get_notes')
      .then(d => setNotes(d as Record<string, string>))
      .catch(console.error)
  }, [])

  // Remember the last role per project (same scoping precedent as the
  // scoped hidden state): a glue-heavy session shouldn't re-filter itself on
  // every panel open. First open of a project defaults to Process.
  useEffect(() => {
    callBackend('get_notes')
      .then(d => {
        const saved = (d as Record<string, string>)[ROLE_NOTE_KEY]
        if (saved && ROLE_FILTERS.some(r => r.id === saved)) {
          setRoleFilter(saved as FunctionRole | 'all')
        }
      })
      .catch(() => { /* no saved preference is the normal first-open case */ })
  }, [])

  const selectRole = (role: FunctionRole | 'all') => {
    setRoleFilter(role)
    setSelectedItem(null)
    callBackend('set_note', { key: ROLE_NOTE_KEY, text: role }).catch(console.error)
  }

  function fetchRegistry() {
    callBackend('get_registry')
      .then(d => { setRegistry(d as Registry); setDiscoveryError('') })
      .catch(err => {
        console.error(err)
        setDiscoveryError(`Failed to load functions/variables: ${(err as Error).message}`)
      })
  }

  function fetchPipelines() {
    callBackend('list_pipelines')
      .then(d => setPipelines((d as { pipelines: PipelineInfo[] }).pipelines))
      .catch(console.error)
    callBackend('list_hypotheses')
      .then(d => setHypothesisIds(new Set(
        (d as { hypotheses: Array<{ pipeline_id: string }> }).hypotheses.map(h => h.pipeline_id)
      )))
      .catch(console.error)
  }

  function fetchHiddenPipelines() {
    callBackend('get_hidden_pipelines')
      .then(d => setHiddenPipelines((d as { pipelines: HiddenPipelineInfo[] }).pipelines))
      .catch(console.error)
  }

  const handleRestorePipeline = (pid: string) => {
    callBackend('unhide_pipeline', { pipeline_id: pid })
      .then(() => { setPipeError(''); fetchPipelines(); fetchHiddenPipelines() })
      .catch(err => setPipeError((err as Error).message))
  }

  // Every list this panel owns must be fetched here, not only on the
  // 'dag_updated' broadcast below: on reopening a project nothing has changed
  // yet, so no broadcast arrives and an omitted list stays empty until the user
  // clicks "Refresh code". That is exactly how PathInputs went missing from the
  // sidebar while still rendering on the canvas (which is fed by get_pipeline,
  // an independent path).
  useEffect(() => {
    fetchRegistry()
    fetchParameters()
    fetchPathInputs()
    fetchNotes()
  }, [fetchNotes])

  // Scope mutations elsewhere (e.g. a use placed on the canvas) bump
  // graphVersion — keep the pipelines list in sync.
  useEffect(() => {
    fetchPipelines()
    fetchHiddenPipelines()
  }, [graphVersion])

  // Re-fetch registry when the backend signals a refresh (e.g. module reload).
  useBackendMessage(useCallback((msg) => {
    if (msg.type === 'dag_updated' || msg.method === 'dag_updated') {
      fetchRegistry()
      fetchPathInputs()
      fetchParameters()
      fetchPipelines()
      fetchHiddenPipelines()
    }
  }, []))

  function fetchParameters() {
    callBackend('get_parameters')
      .then((items) => {
        setParameters(items as ParameterListItem[])
      })
      .catch(err => {
        console.error(err)
        setDiscoveryError(`Failed to load parameters: ${(err as Error).message}`)
      })
  }

  function fetchPathInputs() {
    callBackend('get_path_inputs')
      .then((items) => {
        const arr = items as Array<{ name: string }>
        setPathInputs(arr.map(i => i.name))
      })
      .catch(err => {
        console.error('[PathInputs] fetch error:', err)
        setDiscoveryError(`Failed to load path inputs: ${(err as Error).message}`)
      })
  }

  useEffect(() => {
    if (addingConst) constInputRef.current?.focus()
  }, [addingConst])

  useEffect(() => {
    if (addingPI) piInputRef.current?.focus()
  }, [addingPI])

  useEffect(() => {
    if (addingVar) varInputRef.current?.focus()
  }, [addingVar])

  useEffect(() => {
    if (addingBuiltin) builtinInputRef.current?.focus()
  }, [addingBuiltin])

  useEffect(() => {
    if (addingPipe) pipeInputRef.current?.focus()
  }, [addingPipe])

  useEffect(() => {
    if (renamingPid) renameInputRef.current?.focus()
  }, [renamingPid])

  const commitConstDraft = () => {
    const name = constDraft.trim()
    if (name) {
      callBackend('create_parameter', { name }).then(fetchParameters)
    }
    setConstDraft('')
    setAddingConst(false)
  }

  const commitVarDraft = () => {
    if (varSubmitting) return
    const name = varDraft.trim()
    if (!name) {
      setVarDraft('')
      setAddingVar(false)
      setVarError('')
      return
    }
    setVarSubmitting(true)
    callBackend('create_variable', { name })
      .then(data => {
        const d = data as { ok?: boolean; error?: string }
        if (d.ok) {
          // No dag_updated broadcast follows this (see api/variables.py) —
          // a bare type declaration can't change the canvas, so refresh
          // just this panel's own registry rather than waiting on one.
          fetchRegistry()
          setVarDraft('')
          setAddingVar(false)
          setVarError('')
        } else {
          setVarError(d.error || 'Failed')
          varInputRef.current?.focus()
        }
      })
      .catch(() => {
        setVarError('Request failed')
        varInputRef.current?.focus()
      })
      .finally(() => setVarSubmitting(false))
  }

  const commitBuiltinDraft = () => {
    if (builtinSubmitting) return
    const reference = builtinDraft.trim()
    if (!reference) {
      setBuiltinDraft('')
      setAddingBuiltin(false)
      setBuiltinError('')
      return
    }
    setBuiltinSubmitting(true)
    callBackend('create_builtin_function', { language: builtinLang, reference })
      .then(data => {
        const d = data as { ok?: boolean; error?: string }
        if (d.ok) {
          setBuiltinDraft('')
          setAddingBuiltin(false)
          setBuiltinError('')
          fetchRegistry()
        } else {
          setBuiltinError(d.error || 'Failed')
          builtinInputRef.current?.focus()
        }
      })
      .catch(() => {
        setBuiltinError('Request failed')
        builtinInputRef.current?.focus()
      })
      .finally(() => setBuiltinSubmitting(false))
  }

  const commitGlueDraft = () => {
    if (glueSubmitting) return
    const name = glueDraft.trim()
    if (!name) {
      setGlueDraft('')
      setAddingGlue(false)
      setGlueError('')
      return
    }
    setGlueSubmitting(true)
    callBackend('create_glue', { name, language: glueLang })
      .then(data => {
        const d = data as { ok?: boolean; error?: string }
        if (d.ok) {
          setGlueDraft('')
          setAddingGlue(false)
          setGlueError('')
          // Creating a function of a given role SELECTS that role, so a
          // just-created node is never filtered out of view.
          selectRole('glue')
          fetchRegistry()
        } else {
          setGlueError(d.error || 'Failed')
          glueInputRef.current?.focus()
        }
      })
      .catch(() => {
        setGlueError('Request failed')
        glueInputRef.current?.focus()
      })
      .finally(() => setGlueSubmitting(false))
  }

  const commitPiDraft = () => {
    if (piSubmitting) return
    const name = piDraft.trim()
    if (!name) {
      setPiDraft('')
      setAddingPI(false)
      setPiError('')
      return
    }
    setPiSubmitting(true)
    callBackend('create_path_input', { name })
      .then(data => {
        const d = data as { ok?: boolean; error?: string }
        if (d.ok !== false) {
          setPiDraft('')
          setAddingPI(false)
          setPiError('')
          fetchPathInputs()
        } else {
          setPiError(d.error || 'Failed')
          piInputRef.current?.focus()
        }
      })
      .catch(err => {
        setPiError((err as Error).message || 'Request failed')
        piInputRef.current?.focus()
      })
      .finally(() => setPiSubmitting(false))
  }


  // Backend 400s (duplicate names, still-used or last-remaining hides)
  // carry a clear message — surface it verbatim under the section.
  const commitPipeDraft = () => {
    const name = pipeDraft.trim()
    if (name) {
      callBackend('create_pipeline', { name })
        .then(() => { setPipeError(''); fetchPipelines() })
        .catch(err => setPipeError((err as Error).message))
    }
    setPipeDraft('')
    setAddingPipe(false)
  }

  const commitRename = () => {
    const pid = renamingPid
    const name = renameDraft.trim()
    setRenamingPid(null)
    setRenameDraft('')
    if (!pid || !name) return
    callBackend('rename_pipeline', { pipeline_id: pid, name })
      .then(() => {
        setPipeError('')
        fetchPipelines()
        renameInPath(pid, name)
        bumpGraph()  // pipelineNode labels on parent canvases change
      })
      .catch(err => setPipeError((err as Error).message))
  }

  const handleDeletePipeline = (pid: string) => {
    callBackend('delete_pipeline', { pipeline_id: pid })
      .then(() => {
        setPipeError('')
        fetchPipelines()
        fetchHiddenPipelines()
        if (pid === currentScope) {
          // Land on whatever pipeline is left, not a hardcoded 'main' —
          // 'main' itself may now be hidden.
          const remaining = pipelines.find(p => p.pipeline_id !== pid)
          if (remaining) jumpTo(remaining.pipeline_id, remaining.name)
        } else {
          bumpGraph()
        }
      })
      .catch(err => setPipeError((err as Error).message))
  }

  const onPipelineDragStart = (e: React.DragEvent, p: PipelineInfo) => {
    e.dataTransfer.setData('application/scistack-pipeline', JSON.stringify(p))
    e.dataTransfer.effectAllowed = 'move'
  }

  const onDragStart = (
    e: React.DragEvent,
    nodeType: 'functionNode' | 'glueNode' | 'variableNode' | 'parameterNode' | 'pathInputNode',
    label: string,
  ) => {
    e.dataTransfer.setData(
      'application/scistack-node',
      JSON.stringify({ nodeType, label }),
    )
    e.dataTransfer.effectAllowed = 'move'
  }

  const selectListItem = (kind: SidebarItemKind, name: string, displayLabel?: string) => {
    setSelectedItem({ kind, name, displayLabel })
  }

  // Right-click a Parameter row → "Refresh from file" / "Open source",
  // mirroring PipelineDAG.tsx's canvas context menu for the same node type
  // (see docs/claude/entity-editability-model.md — same actions, second
  // surface). Positioned viewport-fixed rather than canvas-relative, since
  // this list has no bounding-box math to do.
  const openParamContextMenu = (e: React.MouseEvent, item: ParameterListItem) => {
    e.preventDefault()
    setParamContextMenu({ x: e.clientX, y: e.clientY, item })
  }

  const handleRefreshParamFromSidebar = () => {
    const name = paramContextMenu?.item.name
    setParamContextMenu(null)
    if (!name) return
    callBackend('refresh_parameter_source', { name })
      .then(res => {
        const r = res as { ok: boolean; error?: string }
        if (!r.ok) window.alert(`Could not refresh '${name}' from file: ${r.error ?? 'unknown error'}`)
      })
      .catch(err => window.alert(`Could not refresh '${name}' from file: ${(err as Error).message}`))
  }

  const handleOpenParamSourceFromSidebar = () => {
    const item = paramContextMenu?.item
    setParamContextMenu(null)
    if (!item?.source_file) return
    if (isVSCodeMode) {
      callBackend('reveal_in_editor', { file: item.source_file, line: item.source_line })
        .catch(err => window.alert(`Failed to open source: ${(err as Error).message}`))
    } else {
      setParamSourceLoc({ name: item.name, file: item.source_file, line: item.source_line ?? 0 })
    }
  }

  return (
    <div style={styles.root}>
      <div style={styles.tabStrip}>
        {TABS.map(tab => (
          <button
            key={tab.id}
            type="button"
            onClick={() => selectTab(tab.id)}
            style={{
              ...styles.tabBtn,
              ...(activeTab === tab.id ? styles.tabBtnActive : {}),
            }}
          >
            <span style={styles.tabIcon}>{tab.icon}</span>
            <span style={styles.tabLabel}>{tab.label}</span>
          </button>
        ))}
      </div>
      <div style={styles.content}>
        {discoveryError && <div style={styles.errorBanner}>{discoveryError}</div>}
        {activeTab === 'submodule' && (
          <Section
            action={
              <button style={styles.addBtn} onClick={() => setAddingPipe(true)} title="New submodule">
                +
              </button>
            }
          >
            {pipelines.filter(p => !hypothesisIds.has(p.pipeline_id)).map(p => (
              renamingPid === p.pipeline_id ? (
                <input
                  key={p.pipeline_id}
                  ref={renameInputRef}
                  style={styles.draftInput}
                  value={renameDraft}
                  onChange={e => setRenameDraft(e.target.value)}
                  onKeyDown={e => {
                    if (e.key === 'Enter') commitRename()
                    if (e.key === 'Escape') { setRenamingPid(null); setRenameDraft('') }
                  }}
                  onBlur={commitRename}
                />
              ) : (
                <div
                  key={p.pipeline_id}
                  draggable
                  onDragStart={e => onPipelineDragStart(e, p)}
                  onClick={() => {
                    jumpTo(p.pipeline_id, p.name)
                    selectListItem('submodule', p.pipeline_id, p.name)
                  }}
                  style={{
                    ...styles.item,
                    borderLeftColor: '#a21caf',
                    display: 'flex',
                    alignItems: 'center',
                    gap: 4,
                    ...(p.pipeline_id === currentScope ? styles.pipelineCurrent : {}),
                    ...(selectedItem?.kind === 'submodule' && selectedItem.name === p.pipeline_id ? styles.itemSelected : {}),
                  }}
                  title={p.pipeline_id === currentScope
                    ? 'Current scope'
                    : 'Click to open; drag onto the canvas to place as a node'}
                >
                  <span style={{ flex: 1 }}>⧉ {p.name}</span>
                  {p.pipeline_id !== 'main' && (
                    <>
                      <button
                        style={styles.rowBtn}
                        title="Rename pipeline"
                        onClick={e => {
                          e.stopPropagation()
                          setRenamingPid(p.pipeline_id)
                          setRenameDraft(p.name)
                        }}
                      >
                        ✎
                      </button>
                      <button
                        style={styles.rowBtn}
                        title="Delete pipeline"
                        onClick={e => {
                          e.stopPropagation()
                          handleDeletePipeline(p.pipeline_id)
                        }}
                      >
                        ×
                      </button>
                    </>
                  )}
                </div>
              )
            ))}
            {addingPipe && (
              <input
                ref={pipeInputRef}
                style={styles.draftInput}
                value={pipeDraft}
                placeholder="submodule name…"
                onChange={e => setPipeDraft(e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Enter') commitPipeDraft()
                  if (e.key === 'Escape') { setPipeDraft(''); setAddingPipe(false) }
                }}
                onBlur={commitPipeDraft}
              />
            )}
            {pipeError && (
              <div style={styles.errorText}>{pipeError}</div>
            )}
            {(() => {
              const hiddenSubmodules = hiddenPipelines.filter(p => !p.is_hypothesis)
              if (hiddenSubmodules.length === 0) return null
              return (
                <div style={styles.hiddenWrap}>
                  <button
                    style={styles.hiddenToggle}
                    onClick={() => setShowHiddenPipelines(v => !v)}
                    type="button"
                  >
                    {showHiddenPipelines
                      ? 'hide'
                      : `${hiddenSubmodules.length} hidden — show`}
                  </button>
                  {showHiddenPipelines && hiddenSubmodules.map(p => (
                    <div key={p.pipeline_id} style={styles.hiddenRow}>
                      <span style={{ flex: 1 }}>{p.name}</span>
                      <button
                        style={styles.rowBtn}
                        onClick={() => handleRestorePipeline(p.pipeline_id)}
                        title="Restore this submodule"
                      >
                        restore
                      </button>
                    </div>
                  ))}
                </div>
              )
            })()}
          </Section>
        )}
        {activeTab === 'function' && (
          <Section
            action={
              // Two creations live under this category, and which one the +
              // means depends on the role you're looking at: a glue node is a
              // function with a role, not a separate kind of thing.
              roleFilter === 'glue' ? (
                <button
                  style={styles.addBtn}
                  onClick={() => setAddingGlue(true)}
                  title="New glue node — free-form code that reshapes an input in memory, between a variable and the function consuming it"
                >
                  +
                </button>
              ) : (
                <button
                  style={styles.addBtn}
                  onClick={() => setAddingBuiltin(true)}
                  title="Add a built-in/library function you didn't write yourself (e.g. numpy.mean, or a MATLAB command)"
                >
                  +
                </button>
              )
            }
          >
            {(() => {
              const allFns = [...registry.functions, ...(registry.matlab_functions ?? [])]
              const roles = registry.function_roles ?? {}
              const roleOf = (fn: string): FunctionRole => roles[fn] ?? 'process'
              const counts: Record<string, number> = { all: allFns.length }
              for (const fn of allFns) {
                counts[roleOf(fn)] = (counts[roleOf(fn)] ?? 0) + 1
              }
              const shown = roleFilter === 'all'
                ? allFns
                : allFns.filter(fn => roleOf(fn) === roleFilter)
              return (
                <>
                  <select
                    style={styles.roleFilter}
                    value={roleFilter}
                    onChange={e => selectRole(e.target.value as FunctionRole | 'all')}
                    title="Filter the list by function role. The canvas always shows whatever is wired."
                  >
                    {ROLE_FILTERS.map(r => (
                      <option key={r.id} value={r.id}>
                        {r.label} ({counts[r.id] ?? 0})
                      </option>
                    ))}
                  </select>
                  {shown.map(fn => {
                    const mismatch = registry.matlab_functions_mismatched?.includes(fn)
                    const displayLabel = mismatch ? `${fn} (function/file name mismatch)` : fn
                    const isGlue = roleOf(fn) === 'glue'
                    return (
                      <DragItem
                        key={fn}
                        label={displayLabel}
                        color={isGlue ? '#9ca3af' : '#7b68ee'}
                        selected={selectedItem?.kind === 'function' && selectedItem.name === fn}
                        onDragStart={e => onDragStart(e, isGlue ? 'glueNode' : 'functionNode', fn)}
                        onClick={() => selectListItem('function', fn)}
                      />
                    )
                  })}
                  {shown.length === 0 && (
                    <div style={styles.emptyRole}>
                      No {ROLE_FILTERS.find(r => r.id === roleFilter)?.label.toLowerCase()} functions yet.
                    </div>
                  )}
                </>
              )
            })()}
            {addingGlue && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <div style={{ display: 'flex', gap: 4 }}>
                  <select
                    value={glueLang}
                    onChange={e => {
                      setGlueLang(e.target.value as 'python' | 'matlab')
                      setGlueError('')
                    }}
                    style={{ fontSize: 12 }}
                    title="Glue runs in the language of the run — a MATLAB pipeline needs MATLAB glue"
                  >
                    <option value="python">Python</option>
                    <option value="matlab">MATLAB</option>
                  </select>
                  <input
                    ref={glueInputRef}
                    style={{ ...styles.draftInput, flex: 1 }}
                    value={glueDraft}
                    // The glue_ prefix is what makes it a glue node, so the
                    // backend applies it rather than making the user
                    // remember it.
                    placeholder="drop_baseline"
                    onChange={e => { setGlueDraft(e.target.value); setGlueError('') }}
                    onKeyDown={e => {
                      if (e.key === 'Enter') commitGlueDraft()
                      if (e.key === 'Escape') {
                        setGlueDraft('')
                        setAddingGlue(false)
                        setGlueError('')
                      }
                    }}
                  />
                </div>
                {glueSubmitting && (
                  <div style={{ ...styles.errorText, color: '#999' }}>Creating…</div>
                )}
                {!glueSubmitting && glueError && (
                  <div style={styles.errorText}>{glueError}</div>
                )}
              </div>
            )}
            {addingBuiltin && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                <div style={{ display: 'flex', gap: 4 }}>
                  <select
                    value={builtinLang}
                    onChange={e => {
                      setBuiltinLang(e.target.value as 'python' | 'matlab')
                      setBuiltinError('')
                    }}
                    style={{ fontSize: 12 }}
                  >
                    <option value="python">Python</option>
                    <option value="matlab">MATLAB</option>
                  </select>
                  <input
                    ref={builtinInputRef}
                    style={{ ...styles.draftInput, flex: 1 }}
                    value={builtinDraft}
                    placeholder={builtinLang === 'python' ? 'numpy.mean' : 'mean'}
                    onChange={e => { setBuiltinDraft(e.target.value); setBuiltinError('') }}
                    onKeyDown={e => {
                      if (e.key === 'Enter') commitBuiltinDraft()
                      if (e.key === 'Escape') {
                        setBuiltinDraft('')
                        setAddingBuiltin(false)
                        setBuiltinError('')
                      }
                    }}
                  />
                </div>
                {builtinSubmitting && (
                  <div style={{ ...styles.errorText, color: '#999' }}>
                    {builtinLang === 'matlab' ? 'Validating with MATLAB…' : 'Validating…'}
                  </div>
                )}
                {!builtinSubmitting && builtinError && (
                  <div style={styles.errorText}>{builtinError}</div>
                )}
              </div>
            )}
          </Section>
        )}
        {activeTab === 'variable' && (
          <Section
            action={
              <button style={styles.addBtn} onClick={() => setAddingVar(true)} title="New variable type">
                +
              </button>
            }
          >
            {registry.variables.map(v => (
              <DragItem
                key={v}
                label={v}
                color="#2a9d8f"
                selected={selectedItem?.kind === 'variable' && selectedItem.name === v}
                onDragStart={e => onDragStart(e, 'variableNode', v)}
                onClick={() => selectListItem('variable', v)}
              />
            ))}
            {addingVar && (
              <>
                <input
                  ref={varInputRef}
                  style={styles.draftInput}
                  value={varDraft}
                  placeholder="VariableName…"
                  onChange={e => { setVarDraft(e.target.value); setVarError('') }}
                  onKeyDown={e => {
                    if (e.key === 'Enter') commitVarDraft()
                    if (e.key === 'Escape') { setVarDraft(''); setAddingVar(false); setVarError('') }
                  }}
                  onBlur={commitVarDraft}
                />
                {varError && (
                  <div style={styles.errorText}>{varError}</div>
                )}
              </>
            )}
          </Section>
        )}
        {activeTab === 'parameter' && (
          <Section
            action={
              <button style={styles.addBtn} onClick={() => setAddingConst(true)} title="New parameter">
                +
              </button>
            }
          >
            {/* Constants and Sweeps are one concept here (D6): a Parameter
                is a named thing with one or more values, and which of the
                two source forms declares it is not a distinction the user
                makes. Both lists are unioned and de-duplicated by name. */}
            {parameters.map(c => (
              <DragItem
                key={c.name}
                label={c.name}
                color="#2a9d8f"
                selected={selectedItem?.kind === 'parameter' && selectedItem.name === c.name}
                onDragStart={e => onDragStart(e, 'parameterNode', c.name)}
                onClick={() => selectListItem('parameter', c.name)}
                onContextMenu={c.source_file ? (e) => openParamContextMenu(e, c) : undefined}
              />
            ))}
            {addingConst && (
              <input
                ref={constInputRef}
                style={styles.draftInput}
                value={constDraft}
                placeholder="constant name…"
                onChange={e => setConstDraft(e.target.value)}
                onKeyDown={e => {
                  if (e.key === 'Enter') commitConstDraft()
                  if (e.key === 'Escape') { setConstDraft(''); setAddingConst(false) }
                }}
                onBlur={commitConstDraft}
              />
            )}
          </Section>
        )}
        {activeTab === 'pathInput' && (
          <Section
            action={
              <button style={styles.addBtn} onClick={() => setAddingPI(true)} title="New path input">
                +
              </button>
            }
          >
            {pathInputs.map(p => (
              <DragItem
                key={p}
                label={p}
                color="#d97706"
                selected={selectedItem?.kind === 'pathInput' && selectedItem.name === p}
                onDragStart={e => onDragStart(e, 'pathInputNode', p)}
                onClick={() => selectListItem('pathInput', p)}
              />
            ))}
            {addingPI && (
              <>
                <input
                  ref={piInputRef}
                  style={styles.draftInput}
                  value={piDraft}
                  placeholder="param name…"
                  onChange={e => { setPiDraft(e.target.value); setPiError('') }}
                  onKeyDown={e => {
                    if (e.key === 'Enter') commitPiDraft()
                    if (e.key === 'Escape') { setPiDraft(''); setAddingPI(false); setPiError('') }
                  }}
                  onBlur={commitPiDraft}
                />
                {piError && (
                  <div style={styles.errorText}>{piError}</div>
                )}
              </>
            )}
          </Section>
        )}
      </div>
      {selectedItem && (
        <ItemInfoPanel
          item={selectedItem}
          notes={notes}
          onNoteSaved={(key, text) => setNotes(prev => ({ ...prev, [key]: text }))}
          onClose={() => setSelectedItem(null)}
        />
      )}
      {paramContextMenu && (
        <>
          {/* Transparent full-viewport backdrop: EditTab has no other
              click-away infra, unlike PipelineDAG's onPaneClick. */}
          <div style={styles.contextMenuBackdrop} onClick={() => setParamContextMenu(null)} onContextMenu={e => { e.preventDefault(); setParamContextMenu(null) }} />
          <div style={{ ...styles.contextMenu, left: paramContextMenu.x, top: paramContextMenu.y }}>
            {paramContextMenu.item.declared_in_entities_file && (
              <button style={styles.contextMenuItem} onClick={handleRefreshParamFromSidebar} type="button">
                🔄 Refresh from file
              </button>
            )}
            {paramContextMenu.item.source_file && (
              <button style={styles.contextMenuItem} onClick={handleOpenParamSourceFromSidebar} type="button">
                📝 Open source ({formatLocation({ file: paramContextMenu.item.source_file, line: paramContextMenu.item.source_line ?? null })})
              </button>
            )}
          </div>
        </>
      )}
      {paramSourceLoc && (
        <SourceLocationDialog location={paramSourceLoc} onClose={() => setParamSourceLoc(null)} />
      )}
    </div>
  )
}

/** Composite key into the notes dict — mirrors layout.py's write_note. */
function noteKey(item: SidebarSelectedItem): string {
  return `${item.kind}:${item.name}`
}

function ItemInfoPanel({
  item,
  notes,
  onNoteSaved,
  onClose,
}: {
  item: SidebarSelectedItem
  notes: Record<string, string>
  onNoteSaved: (key: string, text: string) => void
  onClose: () => void
}) {
  const displayName = item.displayLabel ?? item.name
  const key = noteKey(item)

  return (
    <div style={styles.infoPanel}>
      <div style={styles.infoPanelHeader}>
        <span style={styles.infoPanelTitle}>{displayName}</span>
        <button style={styles.rowBtn} title="Close" onClick={onClose}>×</button>
      </div>
      <div style={styles.infoPanelBody}>
        {item.kind === 'function'
          ? <FunctionDocView fnName={item.name} />
          : <NoteEditor itemKey={key} initialText={notes[key] ?? ''} onSaved={onNoteSaved} />}
      </div>
    </div>
  )
}

interface FunctionDoc {
  ok: boolean
  language?: 'python' | 'matlab'
  signature?: string
  docstring?: string | null
  error?: string
}

function FunctionDocView({ fnName }: { fnName: string }) {
  const [doc, setDoc] = useState<FunctionDoc | null>(null)

  useEffect(() => {
    let cancelled = false
    setDoc(null)
    callBackend('get_function_doc', { name: fnName })
      .then(d => { if (!cancelled) setDoc(d as FunctionDoc) })
      .catch(err => { if (!cancelled) setDoc({ ok: false, error: (err as Error).message }) })
    return () => { cancelled = true }
  }, [fnName])

  if (!doc) return <div style={styles.infoMuted}>Loading…</div>
  if (!doc.ok) return <div style={styles.errorText}>{doc.error}</div>
  return (
    <>
      <div style={styles.signature}>{doc.signature}</div>
      <div style={styles.docstring}>{doc.docstring || <span style={styles.infoMuted}>No docstring available.</span>}</div>
    </>
  )
}

function NoteEditor({
  itemKey,
  initialText,
  onSaved,
}: {
  itemKey: string
  initialText: string
  onSaved: (key: string, text: string) => void
}) {
  const [draft, setDraft] = useState(initialText)

  // Re-sync when a different item (different key) is selected.
  useEffect(() => {
    setDraft(initialText)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [itemKey])

  const commit = () => {
    if (draft === initialText) return
    callBackend('set_note', { key: itemKey, text: draft })
      .then(() => onSaved(itemKey, draft))
      .catch(console.error)
  }

  return (
    <textarea
      style={styles.noteTextarea}
      value={draft}
      placeholder="Notes…"
      onChange={e => setDraft(e.target.value)}
      onBlur={commit}
    />
  )
}

function Section({
  children,
  action,
}: {
  children: React.ReactNode
  action?: React.ReactNode
}) {
  return (
    <div style={styles.section}>
      {action && <div style={styles.sectionHeader}>{action}</div>}
      {children}
    </div>
  )
}

function DragItem({
  label,
  color,
  selected,
  onDragStart,
  onClick,
  onContextMenu,
}: {
  label: string
  color: string
  selected?: boolean
  onDragStart: (e: React.DragEvent) => void
  onClick?: () => void
  onContextMenu?: (e: React.MouseEvent) => void
}) {
  return (
    <div
      draggable
      onDragStart={onDragStart}
      onClick={onClick}
      onContextMenu={onContextMenu}
      style={{ ...styles.item, borderLeftColor: color, ...(selected ? styles.itemSelected : {}) }}
    >
      {label}
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
  contextMenuBackdrop: {
    position: 'fixed',
    inset: 0,
    zIndex: 999,
    background: 'transparent',
  },
  contextMenu: {
    // Fixed, not absolute: coordinates come from clientX/clientY (viewport),
    // unlike PipelineDAG's canvas-relative menu.
    position: 'fixed',
    zIndex: 1000,
    background: '#1a1a2e',
    border: '1px solid #3a3a5a',
    borderRadius: 4,
    boxShadow: '0 4px 12px rgba(0,0,0,0.4)',
    overflow: 'hidden',
  },
  contextMenuItem: {
    display: 'block',
    width: '100%',
    padding: '6px 14px',
    background: 'transparent',
    color: '#eee',
    border: 'none',
    fontSize: 12,
    textAlign: 'left',
    cursor: 'pointer',
    whiteSpace: 'nowrap',
  },
  tabStrip: {
    display: 'flex',
    flexShrink: 0,
    borderBottom: '1px solid #333',
  },
  tabBtn: {
    flex: 1,
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    gap: 2,
    background: 'transparent',
    border: 'none',
    borderBottom: '2px solid transparent',
    color: '#888',
    padding: '6px 2px',
    cursor: 'pointer',
  },
  tabBtnActive: {
    color: '#ddd',
    borderBottom: '2px solid #7b68ee',
  },
  tabIcon: {
    fontSize: 14,
    fontFamily: 'monospace',
    lineHeight: 1,
  },
  tabLabel: {
    fontSize: 9,
    textAlign: 'center',
    lineHeight: 1.1,
  },
  content: {
    flex: 1,
    overflowY: 'auto',
    padding: '4px 0',
    minHeight: 0,
  },
  section: {
    marginBottom: 8,
  },
  sectionHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'flex-end',
    padding: '4px 12px',
  },
  addBtn: {
    background: 'transparent',
    border: 'none',
    color: '#7b68ee',
    fontSize: 18,
    lineHeight: 1,
    cursor: 'pointer',
    padding: '0 2px',
  },
  roleFilter: {
    width: '100%',
    marginBottom: 6,
    fontSize: 12,
    padding: '2px 4px',
  },
  emptyRole: {
    color: '#999',
    fontSize: 11,
    fontStyle: 'italic',
    padding: '4px 2px',
  },
  draftInput: {
    display: 'block',
    width: 'calc(100% - 24px)',
    margin: '2px 12px',
    background: '#1a1a2e',
    border: '1px solid #7b68ee',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 12,
    fontFamily: 'monospace',
    padding: '4px 6px',
    outline: 'none',
    boxSizing: 'border-box',
  },
  item: {
    padding: '5px 12px',
    fontSize: 12,
    fontFamily: 'monospace',
    color: '#ccc',
    borderLeft: '3px solid',
    cursor: 'pointer',
    userSelect: 'none',
  },
  itemSelected: {
    background: '#2a2a4a',
    color: '#fff',
  },
  errorText: {
    padding: '2px 12px',
    fontSize: 11,
    color: '#f87171',
  },
  errorBanner: {
    background: '#442222',
    color: '#ff8888',
    padding: '6px 10px',
    margin: '8px 12px 0',
    borderRadius: 4,
    fontSize: 11,
  },
  errorBannerLine: {
    whiteSpace: 'nowrap',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    marginTop: 2,
  },
  pipelineCurrent: {
    background: '#2a2a4a',
    color: '#fff',
  },
  rowBtn: {
    flexShrink: 0,
    background: 'transparent',
    border: 'none',
    color: '#888',
    fontSize: 12,
    lineHeight: 1,
    cursor: 'pointer',
    padding: '0 2px',
  },
  hiddenWrap: {
    marginTop: 4,
  },
  hiddenToggle: {
    background: 'transparent',
    border: 'none',
    color: '#666',
    fontSize: 11,
    cursor: 'pointer',
    padding: '4px 0',
  },
  hiddenRow: {
    display: 'flex',
    alignItems: 'center',
    gap: 4,
    fontSize: 11,
    color: '#aaa',
    padding: '2px 0',
  },
  infoPanel: {
    flexShrink: 0,
    height: '20%',
    minHeight: 120,
    borderTop: '1px solid #333',
    display: 'flex',
    flexDirection: 'column',
    background: '#181828',
  },
  infoPanelHeader: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '4px 10px',
    borderBottom: '1px solid #2a2a3a',
    flexShrink: 0,
  },
  infoPanelTitle: {
    fontSize: 12,
    fontFamily: 'monospace',
    color: '#ddd',
    fontWeight: 700,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  infoPanelBody: {
    flex: 1,
    overflowY: 'auto',
    padding: '6px 10px',
    display: 'flex',
    flexDirection: 'column',
    gap: 6,
  },
  infoMuted: {
    fontSize: 11,
    color: '#666',
    fontStyle: 'italic',
  },
  signature: {
    fontSize: 11,
    fontFamily: 'monospace',
    color: '#7b68ee',
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
  },
  docstring: {
    fontSize: 11,
    color: '#ccc',
    whiteSpace: 'pre-wrap',
    wordBreak: 'break-word',
  },
  noteTextarea: {
    flex: 1,
    resize: 'none',
    background: '#1a1a2e',
    border: '1px solid #333',
    borderRadius: 3,
    color: '#ccc',
    fontSize: 11,
    fontFamily: 'inherit',
    padding: '6px 8px',
    outline: 'none',
    boxSizing: 'border-box',
  },
}
