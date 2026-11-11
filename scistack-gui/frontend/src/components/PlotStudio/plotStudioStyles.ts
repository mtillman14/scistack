// PlotStudio: the one inline-style table every PlotStudio file reads.
// Moved verbatim out of PlotStudio.tsx (grug stage plotstudio_split,
// docs/claude/grugbrain-principles.md); PlotStudio.tsx is the consumer.

/** Inline styles, keyed by role. One table for every PlotStudio file, so a
 *  colour or spacing change lands everywhere at once. */
export const styles: Record<string, React.CSSProperties> = {
  overlay: {
    position: 'fixed', inset: 0, background: 'var(--ps-scrim)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000,
  },
  // Own-tab mode: fill the webview exactly, no card, no backdrop.
  embeddedRoot: { position: 'absolute', inset: 0, background: 'var(--ps-bg)' },
  embeddedPanel: {
    width: '100%', height: '100%', background: 'var(--ps-bg)',
    display: 'flex', flexDirection: 'row', overflow: 'hidden',
  },
  panel: {
    width: '96vw', height: '94vh', background: 'var(--ps-bg)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 8,
    display: 'flex', flexDirection: 'row', overflow: 'hidden',
  },
  // The title bar and the controls share one column, so the figure column
  // starts at the very top of the panel.
  // 280 px until dragged (Shell's ResizeHandle); nothing inside may assume it
  // (railWidthClass reads the measured width).
  rail: {
    width: 280, flexShrink: 0, display: 'flex', flexDirection: 'column',
    minHeight: 0, borderRight: '1px solid var(--ps-border)',
  },
  // Collapsed: only as wide as the toggle that brings it back.
  railCollapsed: { width: 'auto' },
  header: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    gap: 6, padding: '8px 12px', borderBottom: '1px solid var(--ps-border)',
    background: 'var(--ps-surface)', flexShrink: 0,
  },
  title: {
    color: 'var(--ps-text)', fontSize: 13, fontWeight: 600,
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
  },
  close: {
    background: 'transparent', border: 'none', color: 'var(--ps-text-muted-neutral)',
    cursor: 'pointer', fontSize: 14,
  },
  // LONGHANDS only, and the same keys in both states — see controlsHidden.
  controls: {
    padding: 12, flex: 1, minHeight: 0, overflowX: 'hidden', overflowY: 'auto',
  },
  // Step by step | Advanced: StatStudio's switch, in the same place.
  viewSwitch: { display: 'flex', gap: 4, margin: '0 0 8px' },
  viewOff: { background: 'none', border: '1px solid var(--ps-border-faint)', color: 'var(--ps-text-secondary)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer', fontSize: 12 },
  viewOn: { background: 'var(--ps-accent-bg-strong)', border: '1px solid var(--ps-accent-border)', color: 'var(--ps-text-strong)', borderRadius: 3, padding: '3px 10px', cursor: 'pointer', fontSize: 12 },
  // Design-only mode (StatStudio's rail): the groups flow in the host's column.
  designRail: { display: 'flex', flexDirection: 'column' },
  // Collapsed rather than unmounted: the control state survives the toggle.
  //
  // These two objects must name exactly the same overflow properties. React
  // removes whatever a re-render drops, so a collapsed state that added the
  // `overflow` SHORTHAND left React clearing `overflow` on reopen — and
  // clearing the shorthand clears overflow-y with it, while the unchanged
  // `overflowY: 'auto'` was skipped as "nothing to re-apply". The rail came
  // back unscrollable, and only after a collapse/expand cycle.
  controlsHidden: {
    width: 0, flex: '0 0 0', padding: 0, overflowX: 'hidden', overflowY: 'hidden',
  },
  headerActions: { display: 'flex', alignItems: 'center', gap: 6, flexShrink: 0 },
  headerLeft: { display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 },
  ruleList: { marginTop: 6 },
  ruleTitle: { fontSize: 10, color: 'var(--ps-text-muted)', marginBottom: 3 },
  ruleRow: { display: 'flex', alignItems: 'center', gap: 4, marginBottom: 4 },
  slotIndex: { fontSize: 10, color: 'var(--ps-text-muted-neutral)', flex: '0 0 52px' },
  gridSizeRow: { display: 'flex', gap: 8, marginBottom: 4 },
  gridSizeField: { display: 'flex', alignItems: 'center', gap: 4 },
  gridSizeLabel: { fontSize: 11, color: 'var(--ps-text-secondary)' },
  // An unpinned dimension reads as derived, not as something the user typed.
  gridSizeAuto: { color: 'var(--ps-text-muted-purple)', fontStyle: 'italic' },
  // Text sizes: two columns of label + box, labels right-aligned so the boxes
  // line up.
  textSizesHeader: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    marginTop: 8, marginBottom: 4,
  },
  textSizesGrid: {
    display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '4px 10px', marginBottom: 6,
  },
  // The Proportions grid: element | ratio | floor | ceiling | drawn.
  textProportionsGrid: {
    display: 'grid', gridTemplateColumns: 'auto repeat(3, minmax(0, 1fr)) auto',
    gap: '3px 6px', alignItems: 'center', marginBottom: 6,
  },
  textProportionsTitle: { fontSize: 11, color: 'var(--ps-text-secondary)', fontWeight: 600 },
  textProportionsHead: { fontSize: 10, color: 'var(--ps-text-muted)', textAlign: 'center' as const },
  textProportionsDrawn: { fontSize: 10, color: 'var(--ps-text-secondary)', whiteSpace: 'nowrap' as const },
  textProportionsClamped: { fontSize: 10, color: 'var(--ps-caution)', whiteSpace: 'nowrap' as const },
  textAutoSummary: { fontSize: 10, color: 'var(--ps-text-muted)', lineHeight: 1.4, marginBottom: 6 },
  textSizeLabel: {
    fontSize: 11, color: 'var(--ps-text-secondary)', flex: 1, textAlign: 'right' as const, whiteSpace: 'nowrap' as const,
  },
  inlineButton: {
    padding: '1px 8px', background: 'var(--ps-control)', color: 'var(--ps-text-control)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer', fontSize: 10,
  },
  layoutNote: {
    fontSize: 10, color: 'var(--ps-caution)', marginTop: 6, lineHeight: 1.4,
  },
  headerButton: {
    background: 'var(--ps-control)', color: 'var(--ps-text-control)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 4, cursor: 'pointer', fontSize: 11, padding: '2px 8px',
  },
  // minWidth 0: a flex item defaults to its content's width, and a wide plotly
  // figure would then push the rail off the panel instead of scrolling.
  // The figure side: toolbar over the canvas. The canvas alone is measured
  // (canvasRef), so the toolbar's height is already out of the figure's.
  figureColumn: {
    flex: 1, minWidth: 0, minHeight: 0, display: 'flex', flexDirection: 'column',
  },
  toolbar: {
    position: 'relative', display: 'flex', flexWrap: 'wrap', alignItems: 'center',
    gap: 6, padding: '6px 12px', borderBottom: '1px solid var(--ps-border)',
    background: 'var(--ps-surface)', flexShrink: 0,
  },
  toolbarNote: { fontSize: 10, color: 'var(--ps-text-secondary)', whiteSpace: 'nowrap' },
  // Save progress and results. Takes the slack between the view controls and
  // the buttons; the full text is its tooltip.
  toolbarNotice: {
    flex: '1 1 80px', minWidth: 0, fontSize: 10, color: 'var(--ps-cyan)',
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  toolButton: {
    padding: '4px 8px', background: 'var(--ps-control)', color: 'var(--ps-text-control)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer', fontSize: 11,
    whiteSpace: 'nowrap',
  },
  toolPrimaryButton: {
    padding: '4px 8px', background: 'var(--ps-accent)', color: 'var(--ps-accent-fg)',
    border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 600,
    whiteSpace: 'nowrap',
  },
  canvas: { flex: 1, minWidth: 0, minHeight: 0, padding: 12, overflowY: 'auto' },
  group: { borderBottom: '1px solid var(--ps-border)', marginBottom: 4 },
  groupHeader: {
    display: 'flex', alignItems: 'baseline', gap: 6, width: '100%', minWidth: 0,
    padding: '6px 0', background: 'none', border: 'none', cursor: 'pointer',
    textAlign: 'left', color: 'var(--ps-text-control)',
  },
  groupChevron: { fontSize: 10, color: 'var(--ps-text-muted)', flexShrink: 0, width: 10 },
  groupTitle: {
    fontSize: 11, textTransform: 'uppercase', letterSpacing: 0.8, fontWeight: 700,
    color: 'var(--ps-text-control)', flexShrink: 0,
  },
  groupSummary: {
    fontSize: 10, color: 'var(--ps-text-muted)', minWidth: 0,
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  groupBody: { padding: '4px 0 4px 16px' },
  section: { marginBottom: 14 },
  sectionTitleRow: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 6,
    marginBottom: 6,
  },
  sectionTitle: {
    fontSize: 10, textTransform: 'uppercase', letterSpacing: 0.6,
    color: 'var(--ps-accent)', fontWeight: 700,
  },
  hintToggle: {
    background: 'none', border: 'none', padding: 0, cursor: 'pointer', fontSize: 11,
    lineHeight: 1, color: 'var(--ps-text-muted)',
  },
  hintToggleOn: { color: 'var(--ps-accent)' },
  kindGrid: { display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0 8px', marginBottom: 4 },
  // A boolean with a long label: box first, label wraps beside it.
  checkRow: {
    display: 'flex', alignItems: 'flex-start', gap: 6, marginBottom: 5, cursor: 'pointer',
  },
  checkLabel: { fontSize: 11, color: 'var(--ps-text-control)', lineHeight: 1.3 },
  hint: { fontSize: 10, color: 'var(--ps-text-faint)', marginBottom: 6, fontStyle: 'italic' },
  /** Text that switches the panel to another measure — a link, not a button. */
  linkButton: {
    background: 'none',
    border: 'none',
    padding: 0,
    color: 'var(--ps-accent-text)',
    textDecoration: 'underline',
    cursor: 'pointer',
    fontSize: 'inherit',
    fontStyle: 'normal',
  },
  refusedRow: {
    fontSize: 10, color: 'var(--ps-text-faint-slate)', marginBottom: 4, paddingLeft: 18,
    fontFamily: 'monospace',
  },
  locationButton: {
    width: '100%',
    textAlign: 'left',
    background: 'var(--ps-field)',
    border: '1px solid var(--ps-border)',
    color: 'var(--ps-text-body)',
    borderRadius: 4,
    padding: '5px 8px',
    fontSize: 12,
    cursor: 'pointer',
  },
  pinNote: {
    fontSize: 10, lineHeight: 1.45, color: 'var(--ps-note-text)', marginBottom: 6,
    padding: '5px 7px', background: 'var(--ps-note-bg)', borderLeft: '2px solid var(--ps-note-border)',
  },
  factorRow: {
    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
    gap: 6, marginBottom: 5,
  },
  factorName: {
    fontSize: 11, fontFamily: 'monospace', color: 'var(--ps-text-control)',
    display: 'flex', alignItems: 'center', gap: 4, minWidth: 0,
  },
  levelCount: {
    fontSize: 9, color: 'var(--ps-text-faint-grey)', background: 'var(--ps-control)',
    borderRadius: 8, padding: '0 5px',
  },
  // Where a ticked grouping ended up. Muted: it is a confirmation, not a
  // warning — the one reading that IS a warning ("pooled") says so in words.
  groupRole: { fontSize: 9, color: 'var(--ps-text-faint-indigo)', marginLeft: 6 },
  colorTag: { fontSize: 10, color: 'var(--ps-text-faint-indigo)', marginLeft: 6, marginRight: 4, whiteSpace: 'nowrap' as const },
  // A wide variable, collapsed. Reads as a row rather than a button so the
  // section still scans as one list of groupings.
  variantTag: {
    fontSize: 8, color: 'var(--ps-warn)', border: '1px solid var(--ps-warn-border)',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  fieldTag: {
    fontSize: 8, color: 'var(--ps-cyan)', border: '1px solid var(--ps-cyan-border)',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  codeTag: {
    fontSize: 8, color: 'var(--ps-accent-text-light)', border: '1px solid var(--ps-accent-border)',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
    marginLeft: 6,
  },
  variantPicker: {
    display: 'flex', flexDirection: 'column', gap: 6, marginTop: 8,
  },
  variantSetRow: {
    display: 'flex', alignItems: 'center', gap: 4, flexWrap: 'wrap',
  },
  variantNameInput: {
    flex: 1, minWidth: 0,
    background: 'var(--ps-control)', color: 'var(--ps-text-body)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 4, fontSize: 11, padding: '3px 5px',
  },
  variantSelectButton: {
    flex: '0 0 auto', padding: '3px 8px', background: 'var(--ps-control)', color: 'var(--ps-accent-text-light)',
    border: '1px solid var(--ps-accent-border)', borderRadius: 4, cursor: 'pointer', fontSize: 11,
  },
  variantRemove: {
    flex: '0 0 auto', padding: '2px 5px', background: 'transparent', color: 'var(--ps-text-muted-neutral)',
    border: 'none', cursor: 'pointer', fontSize: 11,
  },
  variantEmptyTag: {
    fontSize: 9, color: 'var(--ps-warn)', border: '1px solid var(--ps-warn-border)',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  // Grey, not amber: "not yet said" is a state, not a problem.
  variantUnsetTag: {
    fontSize: 9, color: 'var(--ps-text-muted-purple)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 3, padding: '0 3px', textTransform: 'uppercase',
  },
  variantAdd: {
    alignSelf: 'flex-start', padding: '3px 10px', background: 'transparent',
    color: 'var(--ps-accent-text)', border: '1px dashed var(--ps-accent-border)', borderRadius: 4,
    cursor: 'pointer', fontSize: 11,
  },
  variantRow: { display: 'flex', flexDirection: 'column', gap: 2 },
  variantName: {
    fontSize: 10, color: 'var(--ps-text-muted-grey)', fontFamily: 'monospace',
    display: 'flex', alignItems: 'center',
  },
  variantLevels: { display: 'flex', flexWrap: 'wrap', gap: 8 },
  variantLevel: {
    display: 'flex', alignItems: 'center', gap: 3,
    fontSize: 11, color: 'var(--ps-text-body)', cursor: 'pointer',
  },
  variantCount: {
    fontSize: 10, color: 'var(--ps-text-muted-grey)', marginTop: 4,
    borderTop: '1px solid var(--ps-border-neutral)', paddingTop: 4,
  },
  customWarn: { fontSize: 10, color: 'var(--ps-warn)', marginTop: 4 },
  variantCountEmpty: {
    fontSize: 10, color: 'var(--ps-warn)', marginTop: 4,
    borderTop: '1px solid var(--ps-border-neutral)', paddingTop: 4,
  },
  shapeTag: { fontSize: 9, color: 'var(--ps-cyan)', marginLeft: 6 },
  sourceTag: { fontSize: 9, color: 'var(--ps-text-muted-violet)', marginLeft: 6 },
  readonlyValue: { fontSize: 12, fontFamily: 'monospace', color: 'var(--ps-text)' },
  select: {
    background: 'var(--ps-control)', color: 'var(--ps-text-body)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 4, fontSize: 11, padding: '2px 4px', maxWidth: 130,
  },
  // A box with ▲▼ beside it (StepperInput): one bordered control, the
  // arrows stacked at its right edge.
  stepper: {
    display: 'inline-flex', alignItems: 'stretch', flexShrink: 0,
    background: 'var(--ps-control)', border: '1px solid var(--ps-border-strong)', borderRadius: 4,
  },
  stepperInput: {
    background: 'transparent', color: 'var(--ps-text-body)', border: 'none', outline: 'none',
    fontSize: 11, padding: '2px 4px', minWidth: 0,
  },
  stepperArrows: {
    display: 'flex', flexDirection: 'column', borderLeft: '1px solid var(--ps-border-strong)',
  },
  stepperArrow: {
    flex: 1, padding: '0 3px', background: 'none', border: 'none', cursor: 'pointer',
    color: 'var(--ps-text-secondary)', fontSize: 7, lineHeight: 1,
  },
  kindRow: {
    display: 'flex', alignItems: 'center', fontSize: 11,
    color: 'var(--ps-text-control)', marginBottom: 3, cursor: 'pointer',
  },
  actions: { display: 'flex', gap: 6, marginTop: 8 },
  button: {
    flex: 1, padding: '5px 8px', background: 'var(--ps-control)', color: 'var(--ps-text-control)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer', fontSize: 11,
  },
  primaryButton: {
    flex: 1, padding: '5px 8px', background: 'var(--ps-accent)', color: 'var(--ps-accent-fg)',
    border: 'none', borderRadius: 4, cursor: 'pointer', fontSize: 11, fontWeight: 600,
  },
  notice: { fontSize: 10, color: 'var(--ps-cyan)', marginTop: 6 },
  // Drops down from the toolbar, over the figure.
  dataChooser: {
    position: 'absolute', top: '100%', right: 12, zIndex: 20, width: 340,
    maxWidth: 'calc(100% - 24px)', maxHeight: '60vh', overflowY: 'auto',
    padding: 8, background: 'var(--ps-surface)',
    border: '1px solid var(--ps-border-neutral-strong)',
    borderRadius: 4, boxShadow: '0 4px 12px var(--ps-shadow)',
  },
  dataDepthOption: {
    display: 'flex',
    alignItems: 'flex-start',
    gap: 6,
    fontSize: 11,
    marginBottom: 6,
    cursor: 'pointer',
  },
  dataColumns: { display: 'block', fontSize: 10, color: 'var(--ps-text-muted-neutral)', fontFamily: 'monospace' },
  note: { fontSize: 12, color: 'var(--ps-text-faint)', fontStyle: 'italic', padding: 8 },
  error: { fontSize: 12, color: 'var(--ps-error)', padding: 12 },
  specError: {
    fontSize: 11, color: 'var(--ps-warn)', background: 'var(--ps-warn-bg)',
    border: '1px solid var(--ps-warn-border)', borderRadius: 4, padding: 8, marginBottom: 8,
  },
  figureBlock: { marginBottom: 14 },
  figureLabel: {
    fontSize: 11, fontFamily: 'monospace', color: 'var(--ps-text-secondary-alt)', marginBottom: 2,
  },
  navigator: {
    display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6,
    padding: '3px 6px', background: 'var(--ps-bg)', border: '1px solid var(--ps-border-soft)',
    borderRadius: 4,
  },
  navButton: {
    background: 'var(--ps-control-alt)', color: 'var(--ps-text-body)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 3, cursor: 'pointer', fontSize: 12, lineHeight: 1,
    padding: '3px 8px',
  },
  navButtonOff: { opacity: 0.35, cursor: 'default' },
  navLabel: {
    flex: 1, fontSize: 11, fontFamily: 'monospace', color: 'var(--ps-text-control)',
    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
  },
  navCount: { fontSize: 11, color: 'var(--ps-text-muted-neutral)', whiteSpace: 'nowrap' },
  levelPicker: { marginBottom: 4 },
  levelPickerHead: {
    display: 'flex', alignItems: 'center', gap: 6, width: '100%',
    background: 'transparent', border: 'none', color: 'var(--ps-text-body)', cursor: 'pointer',
    fontSize: 11, padding: '3px 0', textAlign: 'left',
  },
  levelCountFiltered: {
    fontSize: 10, color: 'var(--ps-warn)', background: 'var(--ps-warn-bg)',
    border: '1px solid var(--ps-warn-border)', borderRadius: 8, padding: '0 6px',
  },
  levelChevron: { fontSize: 9, color: 'var(--ps-text-faint)' },
  levelList: {
    maxHeight: 160, overflowY: 'auto', padding: '2px 0 4px 10px',
    borderLeft: '1px solid var(--ps-border-soft)', marginLeft: 2,
  },
  levelRow: {
    display: 'flex', alignItems: 'center', fontSize: 11, color: 'var(--ps-text-secondary)',
    padding: '1px 0', cursor: 'pointer',
  },
  levelReset: {
    background: 'transparent', border: 'none', color: 'var(--ps-link)',
    cursor: 'pointer', fontSize: 10, padding: '2px 0',
  },
  levelBulkRow: { display: 'flex', gap: 10 },
  levelEmpty: { fontSize: 10, color: 'var(--ps-warn)', paddingTop: 2 },
  xLayerRow: { display: 'flex', alignItems: 'center', gap: 4, padding: '2px 0' },
  xLayerDepth: {
    fontSize: 9, color: 'var(--ps-text-muted-neutral)', background: 'var(--ps-control-alt)', borderRadius: 8,
    padding: '0 5px', minWidth: 14, textAlign: 'center',
  },
  bucketRow: { display: 'flex', alignItems: 'center', gap: 4, padding: '1px 0' },
  bucketLevel: {
    fontSize: 10, color: 'var(--ps-text-secondary)', fontFamily: 'monospace',
    minWidth: 54, overflow: 'hidden', textOverflow: 'ellipsis',
  },
  bucketArrow: { fontSize: 9, color: 'var(--ps-text-faint-grey)' },
  bucketInput: {
    flex: 1, minWidth: 40, background: 'var(--ps-surface)', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 3, fontSize: 10, padding: '1px 4px',
  },
  // Structure > Combine. The collapsed row reads as one line of text; the
  // expanded editor is buckets, then one grid of the source's levels.
  combineHead: {
    flex: 1, minWidth: 0, display: 'flex', alignItems: 'center', gap: 6,
    background: 'transparent', border: 'none', cursor: 'pointer', padding: '3px 0',
    textAlign: 'left', color: 'var(--ps-text-body)',
  },
  combineFrom: {
    fontSize: 10, color: 'var(--ps-text-muted-violet)', fontFamily: 'monospace',
    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
  },
  combineUse: {
    flex: '0 0 auto', padding: '1px 6px', background: 'transparent', color: 'var(--ps-accent-text)',
    border: '1px solid var(--ps-accent-border)', borderRadius: 4, cursor: 'pointer', fontSize: 10,
  },
  combineLine: { display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 },
  combineLabel: { fontSize: 10, color: 'var(--ps-text-faint)', margin: '4px 0 2px' },
  bucketTarget: { background: 'var(--ps-control)', borderRadius: 3 },
  chipGrid: { display: 'flex', flexWrap: 'wrap', gap: 3, margin: '2px 0 4px' },
  chip: {
    display: 'inline-flex', alignItems: 'center', gap: 3, fontSize: 10, fontFamily: 'monospace',
    borderRadius: 3, padding: '1px 5px', cursor: 'pointer', userSelect: 'none' as const,
  },
  // In the target bucket.
  chipOn: {
    background: 'var(--ps-accent-bg-strong)', color: 'var(--ps-accent-text)',
    border: '1px solid var(--ps-accent-border)',
  },
  // In another bucket: clicking moves it to the target.
  chipOther: {
    background: 'transparent', color: 'var(--ps-text-secondary)',
    border: '1px solid var(--ps-border-strong)',
  },
  // In no bucket: dropped (or the catch-all).
  chipUnused: {
    background: 'transparent', color: 'var(--ps-text-faint)',
    border: '1px dashed var(--ps-border-strong)',
  },
  chipOwner: { fontSize: 8, color: 'var(--ps-text-faint-indigo)' },
  rangeRow: { display: 'flex', alignItems: 'center', gap: 6, marginTop: 4 },
  rangeInput: {
    width: 62, background: 'var(--ps-surface)', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 3, fontSize: 10, padding: '2px 4px',
  },
  variantVariableSelect: {
    background: 'var(--ps-surface)', color: 'var(--ps-text-body)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 3, fontSize: 10, padding: '2px 4px', maxWidth: 110,
  },
  // The same information as the dropdown, when there is nothing to switch to.
  // Styled as text rather than as a disabled control: a greyed-out select
  // invites clicking and then refuses, which is worse than a plain label.
  variantVariableLabel: {
    fontSize: 10, color: 'var(--ps-text-muted-indigo)', fontFamily: 'monospace',
    maxWidth: 110, overflow: 'hidden', textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  // Informational blue, NOT the amber of specError/downsampleNote. Per-location
  // "latest" legitimately produces a spanning figure; this is a statement of
  // fact about what is drawn, not a warning that something broke. Colouring it
  // like an error would train the user to dismiss the one message that says
  // their subjects were computed by different code.
  // "Newer data exists — switch to latest?" (switchSavedToLatest).
  newerOverlay: {
    position: 'fixed', inset: 0, background: 'var(--ps-scrim)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1100,
  },
  newerDialog: {
    maxWidth: 460, background: 'var(--ps-bg)', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 8, padding: 16,
    display: 'flex', flexDirection: 'column', gap: 10, lineHeight: 1.5,
  },
  newerTitle: { fontSize: 13, fontWeight: 600 },
  newerText: { fontSize: 12 },
  newerButtons: { display: 'flex', gap: 8, justifyContent: 'flex-end', marginTop: 4 },
  newerPrimary: {
    padding: '4px 12px', background: 'var(--ps-control)', color: 'var(--ps-accent-text-light)',
    border: '1px solid var(--ps-accent-border)', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  newerSecondary: {
    padding: '4px 12px', background: 'transparent', color: 'var(--ps-text-body)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 4, cursor: 'pointer', fontSize: 12,
  },
  spanBanner: {
    fontSize: 11, color: 'var(--ps-info-text)', background: 'var(--ps-info-bg)',
    border: '1px solid var(--ps-info-border)', borderRadius: 4, padding: 8, marginBottom: 8,
    lineHeight: 1.5,
  },
  emptyPin: {
    fontSize: 11, color: 'var(--ps-text-body)', background: 'var(--ps-bg)',
    border: '1px solid var(--ps-border-strong)', borderRadius: 6, padding: 14, marginBottom: 8,
    lineHeight: 1.6,
  },
  emptyPinTitle: { fontSize: 12, color: 'var(--ps-text-strong)', marginBottom: 8 },
  emptyPinBlock: { marginTop: 10 },
  emptyPinAttempt: { color: 'var(--ps-text-secondary)' },
  emptyPinCode: {
    fontFamily: 'monospace', fontSize: 10, color: 'var(--ps-warn)',
    background: 'var(--ps-code-bg)', borderRadius: 3, padding: '1px 4px',
  },
  emptyPinHint: { color: 'var(--ps-text-muted-purple)', fontSize: 10, marginTop: 6 },
  emptyPinOptions: {
    display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 6,
  },
  emptyPinOption: {
    background: 'var(--ps-surface)', color: 'var(--ps-accent-text)', border: '1px solid var(--ps-accent-border)',
    borderRadius: 4, cursor: 'pointer', fontSize: 10, padding: '3px 8px',
    fontFamily: 'monospace',
  },
  downsampleNote: { fontSize: 10, color: 'var(--ps-warn)', marginBottom: 4 },
  codeOverlay: {
    position: 'absolute', inset: 0, background: 'var(--ps-scrim-code)',
    display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 40,
  },
  code: {
    background: 'var(--ps-code-bg)', color: 'var(--ps-text-body)', border: '1px solid var(--ps-border-strong)',
    borderRadius: 6, padding: 16, fontSize: 11, fontFamily: 'monospace',
    maxHeight: '80%', maxWidth: '80%', overflow: 'auto', whiteSpace: 'pre',
  },
}

