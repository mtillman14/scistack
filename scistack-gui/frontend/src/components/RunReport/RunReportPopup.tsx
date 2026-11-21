/**
 * RunReportPopup — everything about a run of a function node, for verifying
 * it (GitHub issue #17). Opens by itself when a run ends and from a function
 * node's right-click (Run report…); RunReportContext decides which.
 *
 * One tab per function node a run covered. Each tab shows one run, newest by
 * default, with the node's earlier runs in a picker. Top to bottom: the
 * counts, what needs attention, the settings used, exclusions in force,
 * each input and output, and one row per schema-key combination.
 *
 * Every count, label and sentence is scidb's (scidb.provenance.run_report
 * through `run_report`); this component draws them. The summary part costs
 * queries only and paints first; the data part reads the run's records and
 * is fetched right away for a modest run, on request for a large one.
 *
 * The window does not block the canvas: there is no backdrop.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { callBackend } from '../../api'
import { useNodeFailures } from '../../context/NodeFailuresContext'
import { useRunReport, type RunReportTarget } from '../../context/RunReportContext'
import { openDataPreview } from '../DataPreview/dataPreview'
import {
  STATUS_COLOR,
  STATUS_ICON,
  STATUS_ORDER,
  VERDICT_ICON,
  attentionOf,
  bytesText,
  durationText,
  keysText,
  locationsText,
  shouldAutoRead,
  startedText,
  tabTone,
  type ReportRow,
  type ReportTab,
  type RowStatus,
  type RunReport,
  type RunReportData,
  type RunSummary,
  type Side,
  type VariableProfile,
} from './runReportTypes'
import { styles } from './runReportStyles'
import { openDataView } from '../DataView/openDataView'
import { statesForReason } from '../DAG/stepVerdict'
import { SHOW_EXPECTED_LOCATIONS } from '../Expectation/expectationTypes'

type DataState = RunReportData | 'loading' | { error: string }

export default function RunReportPopup() {
  const { target, close } = useRunReport()
  if (!target) return null
  // Keyed by openId: a new run's window starts from its own tabs and caches.
  return <RunReportWindow key={target.openId} target={target} onClose={close} />
}

function RunReportWindow({ target, onClose }: { target: RunReportTarget; onClose: () => void }) {
  const { autoOpen, setAutoOpen } = useRunReport()
  const [active, setActive] = useState(0)
  // Reports read so far, by run id: shared by the tabs so a tab's flag is
  // known as soon as its summary is.
  const [reports, setReports] = useState<Record<string, RunReport>>(target.prefetched)
  const [data, setData] = useState<Record<string, DataState>>({})
  // The run each tab shows (a tab opened by right-click starts with none).
  const [runIds, setRunIds] = useState<(string | null)[]>(() => target.tabs.map(t => t.runId))

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const noteReport = useCallback((r: RunReport) => {
    setReports(prev => (prev[r.summary.run_id] ? prev : { ...prev, [r.summary.run_id]: r }))
  }, [])
  const noteData = useCallback((runId: string, d: DataState) => {
    setData(prev => ({ ...prev, [runId]: d }))
  }, [])
  const pickRun = useCallback((index: number, runId: string) => {
    setRunIds(prev => prev.map((r, i) => (i === index ? runId : r)))
  }, [])

  const tab = target.tabs[active]
  return (
    <div style={styles.layer} data-testid="run-report">
      <div style={styles.dialog} role="dialog" aria-label="Run report">
        <div style={styles.header}>
          <span style={styles.title}>Run report</span>
          <span style={styles.headerRight}>
            <label style={styles.autoOpen} title="Open this window by itself when a run finishes. Right-click a function node → Run report… always works.">
              <input type="checkbox" checked={autoOpen} onChange={e => setAutoOpen(e.target.checked)} />
              {' '}Open after every run
            </label>
            <button style={styles.closeBtn} onClick={onClose} title="Close (Esc)" type="button">×</button>
          </span>
        </div>

        {target.tabs.length > 1 && (
          <div style={styles.tabs} role="tablist">
            {target.tabs.map((t, i) => {
              const runId = runIds[i]
              const report = runId ? reports[runId] : undefined
              const d = runId ? data[runId] : undefined
              const tone = tabTone(report, isData(d) ? d : undefined)
              return (
                <button
                  key={`${t.nodeId ?? ''}:${t.functionName}:${i}`}
                  type="button"
                  role="tab"
                  aria-selected={i === active}
                  style={i === active ? styles.tabOn : styles.tab}
                  onClick={() => setActive(i)}
                  title={report?.summary.status_line ?? t.functionName}
                >
                  <span style={{ color: tone === 'problem' ? '#f0c040' : tone === 'ok' ? '#6be16b' : '#666' }}>
                    {tone === 'problem' ? '⚠' : tone === 'ok' ? '✓' : '…'}
                  </span>{' '}
                  {t.functionName}
                </button>
              )
            })}
          </div>
        )}

        <ReportTabBody
          key={active}
          tab={tab}
          runId={runIds[active]}
          report={runIds[active] ? reports[runIds[active]!] : undefined}
          data={runIds[active] ? data[runIds[active]!] : undefined}
          onReport={noteReport}
          onData={noteData}
          onPickRun={runId => pickRun(active, runId)}
        />
      </div>
    </div>
  )
}

function isData(d: DataState | undefined): d is RunReportData {
  return typeof d === 'object' && d !== null && 'rows' in d
}

interface BodyProps {
  tab: ReportTab
  runId: string | null
  report: RunReport | undefined
  data: DataState | undefined
  onReport: (r: RunReport) => void
  onData: (runId: string, d: DataState) => void
  onPickRun: (runId: string) => void
}

function ReportTabBody({ tab, runId, report, data, onReport, onData, onPickRun }: BodyProps) {
  const [runs, setRuns] = useState<RunSummary[] | null>(null)
  const [error, setError] = useState('')
  const asked = useRef<Set<string>>(new Set())

  // The node's recorded runs: the picker, and the run to open on for a right-click.
  useEffect(() => {
    let cancelled = false
    callBackend('run_report', {
      part: 'list',
      function_name: tab.functionName,
      outputs: tab.outputs?.length ? tab.outputs : null,
    })
      .then(raw => {
        if (cancelled) return
        const list = (raw as { runs: RunSummary[] }).runs
        setRuns(list)
        if (!runId && list.length > 0) onPickRun(list[0].run_id)
      })
      .catch(err => { if (!cancelled) setError((err as Error).message) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab.functionName, tab.nodeId])

  useEffect(() => {
    if (!runId || report) return
    let cancelled = false
    setError('')
    callBackend('run_report', { part: 'summary', run_id: runId })
      .then(raw => { if (!cancelled) onReport(raw as RunReport) })
      .catch(err => { if (!cancelled) setError((err as Error).message) })
    return () => { cancelled = true }
  }, [runId, report, onReport])

  const readRecords = useCallback((id: string) => {
    if (asked.current.has(id)) return
    asked.current.add(id)
    onData(id, 'loading')
    const t0 = performance.now()
    callBackend('run_report', { part: 'data', run_id: id })
      .then(raw => {
        const d = raw as RunReportData
        // eslint-disable-next-line no-console
        console.debug(`[RunReport] data of ${id}: ${d.rows.length}/${d.total_rows} row(s) in ${Math.round(performance.now() - t0)} ms`)
        onData(id, d)
      })
      .catch(err => {
        asked.current.delete(id)
        onData(id, { error: (err as Error).message })
      })
  }, [onData])

  // A modest run's records are read right away; a large one waits for the button.
  useEffect(() => {
    if (report && runId && data === undefined && shouldAutoRead(report.summary)) readRecords(runId)
  }, [report, runId, data, readRecords])

  if (error) return <div style={styles.error}>{error}</div>
  if (runs !== null && runs.length === 0 && !runId) {
    return (
      <div style={styles.empty} data-testid="run-report-empty">
        No runs of <b>{tab.functionName}</b> are recorded yet. Run the node and its report opens here.
        (Runs made before this feature are not recorded.)
      </div>
    )
  }
  if (!report || !runId) return <div style={styles.hint}>Loading…</div>

  const loaded = isData(data) ? data : undefined
  return (
    <ReportView
      tab={tab}
      report={report}
      runs={runs ?? []}
      data={loaded}
      dataState={data}
      onPickRun={onPickRun}
      onReadRecords={() => readRecords(runId)}
    />
  )
}

interface ViewProps {
  tab: ReportTab
  report: RunReport
  runs: RunSummary[]
  data: RunReportData | undefined
  dataState: DataState | undefined
  onPickRun: (runId: string) => void
  onReadRecords: () => void
}

function ReportView({ tab, report, runs, data, dataState, onPickRun, onReadRecords }: ViewProps) {
  const s = report.summary
  const { openPanel, failures } = useNodeFailures()
  const attention = attentionOf(report, data)
  const dataPending = dataState === undefined || dataState === 'loading'
  const dataError = typeof dataState === 'object' && dataState !== null && 'error' in dataState ? dataState.error : ''
  const hasLiveFailures = tab.nodeId != null && failures[tab.nodeId] != null
  const picker = runs.some(r => r.run_id === s.run_id) ? runs : [s, ...runs]

  return (
    <div data-testid="run-report-body" data-run={s.run_id} data-verdict={s.verdict}>
      <div style={styles.runLine}>
        <span style={styles.fnName}>{s.function_name}</span>
        <select
          style={styles.select}
          value={s.run_id}
          onChange={e => onPickRun(e.target.value)}
          title="This node's recorded runs, newest first"
          aria-label="Run"
        >
          {picker.map((r, i) => (
            <option key={r.run_id} value={r.run_id}>
              {i === 0 && runs[0]?.run_id === r.run_id ? 'Latest: ' : ''}
              {startedText(r.started_at)} {VERDICT_ICON[r.verdict]} {r.computed} computed
              {r.failed ? `, ${r.failed} failed` : ''}
            </option>
          ))}
        </select>
        <span style={styles.dim}>
          took {durationText(s.duration_ms)}
          {report.run.origin ? ` · from ${report.run.origin}` : ''}
          {report.run.step ? ` · step ${report.run.step}` : ''}
          {' · run '}{s.run_id}
          {s.group_id ? ` · log run=${s.group_id}` : ''}
        </span>
      </div>
      <div style={styles.statusLine} data-testid="run-report-status">
        <span style={{ color: s.verdict === 'ok' ? '#6be16b' : '#f0c040' }}>{VERDICT_ICON[s.verdict]}</span>{' '}
        {s.status_line}
        {s.failed > 0 && hasLiveFailures && (
          <button type="button" style={styles.linkBtn} onClick={() => openPanel(tab.nodeId!)}>
            Show failures
          </button>
        )}
      </div>
      {[...report.notes, ...(data?.notes ?? [])].map((n, i) => <div key={i} style={styles.note}>{n}</div>)}

      {report.verdict && (
        <Section title="Did it work?">
          <div style={styles.verdictLine} data-testid="run-report-verdict" data-word={report.verdict.word}>
            {report.verdict.status_line}
          </div>
          {report.verdict.reasons.map((r, i) => (
            <div key={i} style={styles.kv}>
              <button
                type="button"
                style={styles.linkBtn}
                title="Open the data view on these locations"
                onClick={() => openDataView({
                  variables: s.output_types, states: statesForReason(r.kind), functionName: s.function_name,
                })}
              >
                {r.text}
              </button>
              {r.locations && r.locations.length > 0 && (
                <span style={styles.dim}>
                  {' '}e.g. {r.locations.slice(0, 3).map(l => keysText(l)).join('; ')}
                </span>
              )}
            </div>
          ))}
        </Section>
      )}

      {(report.whats_new ?? []).some(n => n.new_count > 0 || n.resaved_changed > 0 || n.changes.length > 0) && (
        <Section title="What's new?">
          {(report.whats_new ?? []).map(n => (
            <div key={n.variable} style={styles.card} data-testid="run-report-new">
              <div style={n.new_count > 0 ? styles.newHeadline : undefined}>{n.headline}</div>
              {n.new_compact.length > 3 && (
                <details><summary style={styles.dim}>all {n.new_count} new location(s)</summary>
                  {n.new_compact.map(line => <div key={line}><code>{line}</code></div>)}
                </details>
              )}
              <div style={styles.dim}>
                {n.locations_before} → {n.locations_after} location(s)
                {n.changes.length > 0 && ` · ${n.changes.join(' · ')}`}
              </div>
              {(n.resaved_changed > 0 || n.resaved_unchanged > 0) && (
                <div style={styles.dim}>
                  saved again: {n.resaved_changed} with a changed value, {n.resaved_unchanged} unchanged
                </div>
              )}
            </div>
          ))}
        </Section>
      )}

      <Section title="What looks odd?">
        {(report.findings ?? []).map((f, i) => (
          <div key={`f${i}`} style={styles.attnProblem} data-testid="run-report-finding">
            <span>⚠</span> {f.text} <span style={styles.dim}>({keysText(f.location)})</span>
          </div>
        ))}
        {attention.map((a, i) => (
          <div key={i} style={a.severity === 'problem' ? styles.attnProblem : styles.attnNote} data-testid="run-report-attention" data-severity={a.severity}>
            <span>{a.severity === 'problem' ? '⚠' : '•'}</span> {a.text}
          </div>
        ))}
        {attention.length === 0 && (report.findings ?? []).length === 0 && !dataPending && !dataError && <div style={styles.ok}>✓ Nothing flagged.</div>}
        {dataState === 'loading' && <div style={styles.hint}>Reading the run's records for NaNs and shapes…</div>}
        {dataState === undefined && s.computed > 0 && (
          <div style={styles.hint}>
            NaNs, shapes and sizes are found by reading the run's {s.saved_records.toLocaleString()} record(s).{' '}
            <button type="button" style={styles.btn} onClick={onReadRecords}>Read the records</button>
          </div>
        )}
        {dataError && <div style={styles.error}>The records could not be read: {dataError}</div>}
      </Section>

      <Section title="What was it run with?">
        <SettingsTable report={report} />
      </Section>

      <Section title="Inclusions and exclusions">
        <div style={styles.kv}>
          <span style={styles.k}>Locations selected</span>
          <span>{locationsText(report.settings.locations)}</span>
        </div>
        <div style={styles.kv}>
          <span style={styles.k}>Excluded when it ran</span>
          <span>
            {report.exclusions.length === 0 && 'nothing'}
            {report.exclusions.map((e, i) => (
              <div key={i}>
                <code>{keysText(e.keys)}</code> — {e.reason}
                {e.changed_at ? <span style={styles.dim}> ({e.changed_at.slice(0, 10)})</span> : null}
              </div>
            ))}
          </span>
        </div>
        {SHOW_EXPECTED_LOCATIONS && report.marks.length > 0 && (
          <div style={styles.kv}>
            <span style={styles.k} title="Marks say what should exist. They never change what runs.">Expected / Not expected marks</span>
            <span>
              {report.marks.map((m, i) => (
                <div key={i}>
                  {m.variable} <code>{keysText(m.keys)}</code>: {m.expected ? 'expected' : 'not expected'} — {m.reason}
                </div>
              ))}
            </span>
          </div>
        )}
      </Section>

      <Section title="Inputs">
        {report.inputs.length === 0 && <div style={styles.hint}>No variable inputs recorded for this run.</div>}
        {report.inputs.map(card => (
          <div key={card.param} style={styles.card} data-testid="run-report-input">
            <div>
              <b>{card.param}</b> ← <VariableLink name={card.variable} />
              <span style={styles.dim}>
                {' '}{card.records.toLocaleString()} record(s) at {card.locations.toLocaleString()} location(s)
                {card.through_glue ? ' · through glue' : ''}
              </span>
            </div>
            {(card.variants ?? []).map((v, i) => (
              'more' in v
                ? <div key={i} style={styles.dim}>… +{v.more} more variant(s)</div>
                : Object.keys(v.params).length > 0 && (
                  <div key={i} style={styles.variant}>
                    variant ({v.records} record(s)): {Object.entries(v.params).map(([k, val]) => `${k}=${val}`).join(', ')}
                  </div>
                )
            ))}
            <Profile profile={data?.variables[card.variable]} />
          </div>
        ))}
      </Section>

      <Section title="Outputs">
        {report.outputs.map(card => (
          <div key={card.variable} style={styles.card} data-testid="run-report-output">
            <div>
              <VariableLink name={card.variable} />
              <span style={styles.dim}>
                {' '}{card.records.toLocaleString()} record(s) at {card.locations.toLocaleString()} location(s)
                {card.records > 0 && ` · ${card.new_records.toLocaleString()} new, ${card.resaved_records.toLocaleString()} saved again unchanged`}
              </span>
            </div>
            <Profile profile={data?.variables[card.variable]} />
          </div>
        ))}
      </Section>

      <Section title={data ? `Combinations (${data.total_rows.toLocaleString()})` : 'Combinations'}>
        {data ? <RowsTable data={data} nodeId={tab.nodeId} /> : (
          <div style={styles.hint}>
            {dataState === 'loading' ? 'Reading the records…' : 'One row per schema-key combination, once the records are read.'}
          </div>
        )}
      </Section>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>{title}</div>
      {children}
    </div>
  )
}

function VariableLink({ name }: { name: string }) {
  return (
    <button
      type="button"
      style={styles.varLink}
      onClick={() => openDataPreview({ variable: name })}
      title={`Open the data preview of ${name}`}
    >
      {name}
    </button>
  )
}

function SettingsTable({ report }: { report: RunReport }) {
  const st = report.settings
  const [codeOpen, setCodeOpen] = useState<string | null>(null)
  const rows: [string, React.ReactNode][] = []
  for (const v of st.function_versions) {
    const units = Object.entries(v.source.units)
    rows.push([
      'Function version',
      <span key={v.hash}>
        <code>{v.hash.slice(0, 12)}</code> <span style={styles.dim}>({v.invocations} call(s))</span>{' '}
        {units.length > 0
          ? (
            <button type="button" style={styles.linkBtn} onClick={() => setCodeOpen(codeOpen === v.hash ? null : v.hash)}>
              {codeOpen === v.hash ? 'Hide the code' : 'Show the code as it ran'}
            </button>
          )
          : <span style={styles.dim}>code not stored</span>}
        {codeOpen === v.hash && units.map(([name, source]) => (
          <pre key={name} style={styles.code}>{source}</pre>
        ))}
      </span>,
    ])
  }
  for (const [param, values] of Object.entries(st.constants)) {
    const name = st.declared_names[param]
    rows.push([
      name ? `${param} (${name})` : param,
      <code key={param}>{values.slice(0, 8).join(', ')}{values.length > 8 ? `, … (+${values.length - 8})` : ''}</code>,
    ])
  }
  for (const [param, specs] of Object.entries(st.path_inputs)) {
    rows.push([`${param} (files)`, <code key={param}>{specs.join('; ')}</code>])
  }
  for (const [param, sels] of Object.entries(st.selectors)) {
    rows.push([`${param} selection`, <code key={param}>{sels.join('; ')}</code>])
  }
  for (const label of st.run_options) rows.push(['Run options', <code key={label}>{label}</code>])
  rows.push(['where', st.where ? <code key="w">{st.where}</code> : <span key="w" style={styles.dim}>none</span>])
  rows.push([
    'Other',
    <span key="o">
      on_error={st.on_error ?? '?'} · skip_computed={String(st.skip_computed)} · save={String(st.save)}
    </span>,
  ])
  return (
    <>
      {rows.map(([k, v], i) => (
        <div key={i} style={styles.kv}>
          <span style={styles.k}>{k}</span>
          <span style={styles.v}>{v}</span>
        </div>
      ))}
    </>
  )
}

function Profile({ profile }: { profile: VariableProfile | undefined }) {
  if (!profile) return null
  const columns = Object.entries(profile.columns)
  return (
    <div style={styles.profile}>
      {profile.nbytes != null && <div style={styles.dim}>total size {bytesText(profile.nbytes)}</div>}
      {columns.map(([col, info]) => (
        <div key={col} data-testid="run-report-column">
          <code>{col}</code>{' '}
          <span style={styles.dim}>[{info.dtypes.join('/') || '?'}]</span>{' '}
          shape{info.distinct_shapes === 1 ? '' : 's'}:{' '}
          <span style={info.distinct_shapes > 1 ? styles.warnText : undefined}>
            {info.shapes.map(sh => (info.distinct_shapes === 1 ? `all ${sh.shape}` : `${sh.shape} ×${sh.count}`)).join(', ')}
            {info.distinct_shapes > info.shapes.length ? ', …' : ''}
          </span>
          {' · '}
          <span style={info.cells.problems || info.cells.nan_elements ? styles.warnText : styles.dim}>
            {info.cells.description}
          </span>
        </div>
      ))}
      {profile.records_with_data === 0 && <div style={styles.dim}>no stored data to read</div>}
    </div>
  )
}

function SideCell({ side }: { side: Side | undefined }) {
  if (!side) return <td style={styles.td} />
  const bad = side.problems > 0 || side.nan_elements > 0
  return (
    <td style={styles.td} title={[side.dtype, bytesText(side.nbytes), side.findings].filter(Boolean).join(' · ')}>
      {side.value ? <>{side.value} <span style={styles.dim}>({side.shape_text})</span></> : side.shape_text}
      {bad && <span style={styles.warnText}> ⚠ {side.findings}</span>}
    </td>
  )
}

function RowsTable({ data, nodeId }: { data: RunReportData; nodeId: string | null }) {
  const { openPanel, failures } = useNodeFailures()
  const [statuses, setStatuses] = useState<Set<RowStatus>>(new Set())
  const [suspectOnly, setSuspectOnly] = useState(false)
  const [filter, setFilter] = useState('')
  const params = useMemo(() => [...new Set(data.rows.flatMap(r => Object.keys(r.inputs)))].sort(), [data])
  const outs = useMemo(() => [...new Set(data.rows.flatMap(r => Object.keys(r.outputs)))].sort(), [data])
  const suspect = (r: ReportRow) =>
    [...Object.values(r.inputs), ...Object.values(r.outputs)].some(sd => sd.problems > 0 || sd.nan_elements > 0)
  const rows = useMemo(() => {
    const needle = filter.trim().toLowerCase()
    return data.rows.filter(r =>
      (statuses.size === 0 || statuses.has(r.status))
      && (!suspectOnly || suspect(r))
      && (!needle || Object.entries(r.location).some(([k, v]) => `${k}=${v}`.toLowerCase().includes(needle))),
    )
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, statuses, suspectOnly, filter])
  const toggle = (st: RowStatus) =>
    setStatuses(prev => {
      const next = new Set(prev)
      if (next.has(st)) next.delete(st)
      else next.add(st)
      return next
    })
  const canShowFailures = nodeId != null && failures[nodeId] != null

  return (
    <>
      <div style={styles.controls}>
        {STATUS_ORDER.filter(st => data.status_counts[st] > 0).map(st => (
          <button key={st} type="button" style={statuses.has(st) ? styles.chipOn : styles.chip} onClick={() => toggle(st)}>
            <span style={{ color: STATUS_COLOR[st] }}>{STATUS_ICON[st]}</span>{' '}
            {data.status_labels[st]} {data.status_counts[st].toLocaleString()}
          </button>
        ))}
        <button type="button" style={suspectOnly ? styles.chipOn : styles.chip} onClick={() => setSuspectOnly(v => !v)}
          title="Only combinations where an input or an output has NaN, blank, empty or constant cells">
          ⚠ With findings
        </button>
        <input
          style={styles.input}
          value={filter}
          onChange={e => setFilter(e.target.value)}
          placeholder="filter, e.g. subject=S01"
          aria-label="Filter combinations"
        />
        <span style={styles.dim}>{rows.length.toLocaleString()} shown</span>
      </div>
      {data.truncated && (
        <div style={styles.note}>
          Showing the first {data.rows.length.toLocaleString()} of {data.total_rows.toLocaleString()} combinations.
          For all of them: <code>scidb run-report {'<function>'} --run {data.run_id} --csv rows.csv</code>
        </div>
      )}
      <div style={styles.tableWrap}>
        <table style={styles.table}>
          <thead>
            <tr>
              <th style={styles.th} />
              {data.schema_keys.map(k => <th key={k} style={styles.th}>{k}</th>)}
              {params.map(p => <th key={`i:${p}`} style={styles.th}>in: {p}</th>)}
              {outs.map(o => <th key={`o:${o}`} style={styles.th}>out: {o}</th>)}
              <th style={styles.th}>why</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} data-testid="run-report-row" data-status={r.status}>
                <td style={{ ...styles.td, color: STATUS_COLOR[r.status] }} title={r.status_label}>
                  {STATUS_ICON[r.status]} {r.status_label}
                </td>
                {data.schema_keys.map(k => <td key={k} style={styles.td}>{r.location[k] ?? ''}</td>)}
                {params.map(p => <SideCell key={`i:${p}`} side={r.inputs[p]} />)}
                {outs.map(o => <SideCell key={`o:${o}`} side={r.outputs[o]} />)}
                <td style={styles.whyTd}>
                  {r.reason}
                  {r.status === 'failed' && canShowFailures && (
                    <button type="button" style={styles.linkBtn} onClick={() => openPanel(nodeId!)}>Show failure</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}
