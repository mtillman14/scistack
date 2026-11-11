// The five groups of PlotStudio's controls rail, one component each, moved
// verbatim out of PlotStudio.tsx (grug stage plotstudio_split). Each reads
// what it needs from the one PlotStudioState (usePlotStudio).
import type { PlotStudioState } from './usePlotStudio'
import { Group, Section } from '../Studio/RailGroup'
import { dataSummary, chartSummary, structureSummary, statisticsSummary, appearanceSummary, textSizeColumns } from './sidebarGroups'
import { styles } from './plotStudioStyles'
import { VariantRows, VariantReadout, LevelPicker, RangeFilter, CombineEditor, BucketAdder, groupingPlacement, GroupingList, SlotName, GridSizeInput, RuleSlots, sampleNote, LimitInput, WeightInput, SizeInput, StepperInput } from './PlotStudioControls'
import { XMeasureSection, HistogramSection, KdeSection, RegressionSection } from './ContinuousSections'
import TimeZoneSection from './TimeZoneSection'
import { KIND_LABELS, Role, fallbackRoles } from './plotStudioTypes'
import { tileKey, isTileSelected } from './kindTiles'
import { type Statistic, type ErrorBand } from '../../generated/api-types'
import { compareSummary } from './compare'
import CompareSection from './CompareSection'
import { barCount } from './differenceBars'
import { isLocked, isTicked, rugScaleNote, joinTooltip, joinChoice, type JoinChoice, sampleColorChoice, MARK_COLOR } from './showSample'
import DifferenceBarsSection from './DifferenceBarsSection'
import { textTarget, fixedTickNote, basePlaceholder, baseTitle, textTargets, hasFixedSizes, textSizeRows, textSizeOf, rowTitle, placeholderFor, autoSummary, hasTextTables, proportionRows, formatPt, drawnLabel } from './textSizes'
import { formatLimit } from './timeZone'
import { offersPanels, type YTitles } from './panelOverrides'
import PanelsSection from './PanelsSection'
import { weightTitle } from './markWeights'
import { Fragment } from 'react'
import LabelsSection from './LabelsSection'
import { type Labelable } from './aliasEdit'
import ColorsSection from './ColorsSection'
import { type Colorable, plotMarkColor } from './colorEdit'

/** The rail's Data group, moved verbatim out of PlotStudio's render. */
export function DataGroup({ s }: { s: PlotStudioState }) {
  const {
    csvPath,
    variable,
    setVariable,
    describe,
    spec,
    capabilities,
    setVariantEditor,
    setAddingVariant,
    setLocationPickerOpen,
    openGroups,
    toggleGroup,
    title,
    setTimeZone,
    setXMeasure,
    setLevelFilter,
    setRangeFilter,
    removeVariantSet,
    editVariantSet,
    schemaKeys,
    otherFilterable,
    locationSummary,
    variantRows,
    stackable,
    hasVariants,
    temporalMeta,
  } = s
  return (
    <Group
      title="Data"
      open={openGroups.data}
      onToggle={() => toggleGroup('data')}
      summary={dataSummary({
        variantCount: variantRows.length,
        locations: schemaKeys.length > 0 ? locationSummary : null,
        filterCount: (spec?.filters ?? []).length,
      })}
    >
      {(() => {
        // A wide variable with text AND other fields is two measures
        // (the catalog's `text_of`): one link to the other half, so no
        // field is out of reach from the panel.
        const measures = describe?.catalog?.measures ?? []
        const current = describe?.variable ?? variable
        const textHalf = measures.find(m => m.text_of === current)
        const numericHalf = measures.find(m => m.name === current)?.text_of
        const other = textHalf?.name ?? numericHalf
        if (!other || csvPath) return null
        return (
          <div style={styles.hint}>
            {textHalf
              ? `Its text fields (${(textHalf.columns ?? []).join(', ')}) are plotted as `
              : 'Its other fields are plotted as '}
            <button
              type="button"
              style={styles.linkButton}
              onClick={() => {
                console.info('[Plot Studio] switch_measure', current, '->', other)
                setVariable(other)
              }}
            >
              {other}
            </button>
          </div>
        )
      })()}
      {hasVariants && (
        <Section
          title="Variants"
          hint="Each row is one variant: a variable, narrowed to one version of the pipeline that produced it. Two or more become a factor you can colour or facet by."
        >
          <VariantRows
            rows={variantRows}
            primary={spec?.measures?.[0] ?? title}
            stackable={stackable}
            onRename={(index, name) => editVariantSet(index, { name })}
            onSetVariable={(index, variable) =>
              editVariantSet(index, { variable })
            }
            onEdit={index => setVariantEditor(index)}
            onRemove={removeVariantSet}
            onAdd={() => setAddingVariant(true)}
          />
          <VariantReadout summary={capabilities?.variants} />
        </Section>
      )}

      {schemaKeys.length > 0 && (
        <Section
          title="Schema keys"
          hint="Which records to plot, and whether each location's data is sound. Omit a level everywhere (by key), or pick locations one by one. Everything is included until you say otherwise."
        >
          {/* One button, and nothing else. The flat per-key LevelPickers
              that used to live here are gone, not hidden: they could only
              express a Cartesian product, they said nothing about whether
              the data at a location was any good, and keeping them beside
              the picker would be two controls answering one question — the
              way the Variants/Factors duplication went wrong. */}
          <button
            type="button"
            style={styles.locationButton}
            onClick={() => setLocationPickerOpen(true)}
          >
            {locationSummary}
          </button>
        </Section>
      )}

      <Section title="Filters" hint="Narrow what is drawn without changing what anything means.">
        {otherFilterable.map(factor => (
          <LevelPicker
            key={factor.name}
            factor={factor}
            onChange={levels => setLevelFilter(factor.name, levels)}
          />
        ))}
        {/* A range on a datetime measure would be typed as numbers of
            ms; not offered (plan-plot-datetimes). */}
        {spec && capabilities?.shape === 'scalar' && !capabilities?.time_zone?.measure && (
          <RangeFilter
            measure={spec.measures[0]}
            filter={(spec.filters ?? []).find(f => f.column === spec.measures[0])}
            onChange={(minimum, maximum) =>
              setRangeFilter(spec.measures[0], minimum, maximum)
            }
          />
        )}
      </Section>

      {/* An x-y plot: the variable on x (joined, one value per row).
          Offered only from what the source can line up
          (`DataSource.joinable_with`). */}
      {spec && !capabilities?.custom && (
        <Section
          title="X variable"
          hint="Plot this variable against another one, record for record: scatter, line or a regression with its line of best fit. Numbers and datetimes both work."
        >
          <XMeasureSection
            current={spec.x_measure}
            options={describe?.joinable_with ?? []}
            temporal={Boolean(capabilities?.x_measure?.temporal)}
            onChange={setXMeasure}
          />
        </Section>
      )}

      {/* Only when a datetime is in play: where values holding several
          zones sit (`PlotSpec.time_zone`, scistackplot.temporal). */}
      {capabilities?.time_zone?.applies && (
        <Section
          title="Time zone"
          hint="Datetimes recorded in different zones (a subject in Berlin, another in New York) can be placed at each one's own local clock time, so times of day compare, or converted to one zone, so instants line up. Values with no zone are placed as recorded."
        >
          <TimeZoneSection
            mode={spec?.time_zone}
            capability={capabilities.time_zone}
            meta={temporalMeta}
            onChange={setTimeZone}
          />
        </Section>
      )}
    </Group>
  )
}

/** The rail's Chart group, moved verbatim out of PlotStudio's render. */
export function ChartGroup({ s }: { s: PlotStudioState }) {
  const {
    describe,
    spec,
    capabilities,
    openGroups,
    toggleGroup,
    setKind,
    setInputAsTable,
    setCellStatistic,
    setCategorical,
    setBarStack,
    setHistogram,
    setKde,
    setRegression,
    binsMeta,
    fitsMeta,
  } = s
  return (
    <Group
      title="Chart"
      open={openGroups.chart}
      onToggle={() => toggleGroup('chart')}
      summary={chartSummary({
        kindLabel: spec?.kind ? KIND_LABELS[spec.kind] ?? spec.kind : null,
        perRecord: capabilities?.cell_collapse?.active ? spec?.cell_statistic ?? 'mean' : null,
      })}
    >
      <Section title="Plot type">
        <div style={styles.kindGrid}>
          {(capabilities?.kinds ?? []).map(info => (
            <label
              key={tileKey(info)}
              style={{ ...styles.kindRow, opacity: info.available ? 1 : 0.45 }}
              title={info.reason ?? ''}
            >
              <input
                type="radio"
                name="plot-kind"
                checked={isTileSelected(info, spec)}
                disabled={!info.available}
                onChange={() => setKind(info)}
                style={{ marginRight: 6 }}
              />
              {info.label ?? KIND_LABELS[info.kind] ?? info.kind}
            </label>
          ))}
        </div>
        {capabilities?.custom && (() => {
          // A plot_ function draws this figure (custom plot kinds). What
          // it is called with, and why it cannot be drawn, come from
          // scistackplot.custom; the panel only displays them.
          const custom = capabilities.custom
          const all = custom.inputs.map(i => i.param)
          const notes = [...(describe?.plot_node?.notes ?? []), ...(custom.notes ?? [])]
          return (
            <div>
              <div style={styles.hint}>
                {custom.level === 'panel'
                  ? `Draws one panel per call onto 'ax'; the panels, ticks, legend and text are laid out from ${custom.layout_input}.`
                  : `Returns a whole figure per call (no 'ax' parameter): separate panels are not available.`}
              </div>
              {custom.inputs.map(input => (
                <label key={input.param} style={styles.factorRow} title={`${input.variable} — ${input.shape}, keys ${input.keys.join(', ') || 'none'}`}>
                  <span style={styles.factorName}>{input.param} ← {input.variable}</span>
                  <input
                    type="checkbox"
                    checked={input.as_table}
                    onChange={e => setInputAsTable(input.param, e.target.checked, all)}
                  />
                  <span style={{ marginLeft: 4 }}>as table</span>
                </label>
              ))}
              {custom.reason && <div style={styles.customWarn}>{custom.reason}</div>}
              {[...new Set(notes)].map(note => (
                <div key={note} style={styles.hint}>{note}</div>
              ))}
            </div>
          )
        })()}
        {/* Shown only for a 1-D measure, because only there is there
            anything to reduce. No on/off switch beside it: the KIND says
            whether the vectors are drawn or summarized, so "collapse on,
            kind = line" is a state that cannot be expressed rather than one
            the panel has to adjudicate. */}
        {capabilities?.cell_collapse?.applies && (
          <>
            <label style={styles.factorRow}>
              <span style={styles.factorName}>Per-record value</span>
              <select
                value={spec?.cell_statistic ?? 'mean'}
                onChange={e => setCellStatistic(e.target.value as Statistic)}
                style={styles.select}
              >
                <option value="mean">Mean</option>
                <option value="median">Median</option>
              </select>
            </label>
            <div style={styles.hint}>
              {capabilities.cell_collapse.active
                ? `Each vector is reduced to its ${
                    spec?.cell_statistic ?? 'mean'
                  } — one value per record — and plotted like any scalar.`
                : 'Applies to scatter, strip, spaghetti, box, violin, bar, horizontal bar and histogram: each vector becomes one value per record.'}
            </div>
          </>
        )}
      </Section>
      {capabilities?.categorical?.applies && (
        <Section
          title="Categories"
          hint="This variable holds text. Count bars count its records per category; proportion bars average each unit's % (the last collapsed key is the unit). Collapse POOLS records for text — nothing is averaged before the %."
        >
          {capabilities.categorical.problem && (
            <div style={styles.note}>{capabilities.categorical.problem}</div>
          )}
          {capabilities.categorical.level_applies && (
            <label style={styles.factorRow}>
              <span style={styles.factorName}>Show</span>
              <select
                value={spec?.categorical?.level ?? ''}
                onChange={e => setCategorical({ level: e.target.value || null })}
                style={styles.select}
              >
                <option value="">Every category</option>
                {(capabilities.categorical.levels ?? []).map(level => (
                  <option key={level} value={level}>Only {level}</option>
                ))}
              </select>
            </label>
          )}
        </Section>
      )}
      {capabilities?.bar?.applies && (
        <Section title="Bars" hint="Stack the coloured layer's bars on one tick instead of one tick each — with a text measure's % on colour, the 100% stacked bar.">
          <label style={styles.factorRow}>
            <input
              type="checkbox"
              checked={!!spec?.bar?.stack}
              onChange={e => setBarStack(e.target.checked)}
              style={{ marginRight: 6 }}
            />
            <span>Stack the colour levels</span>
          </label>
          {capabilities.bar.note && <div style={styles.hint}>{capabilities.bar.note}</div>}
        </Section>
      )}
      {spec?.kind === 'histogram' && (
        <Section
          title="Histogram"
          hint="The measure's values binned along x. Bins are shared by every panel of the figure. Percent and density are within each colour, so groups of different sizes compare by shape."
        >
          <HistogramSection
            options={spec.histogram ?? undefined}
            capability={capabilities?.histogram}
            meta={binsMeta}
            onChange={setHistogram}
          />
        </Section>
      )}
      {spec?.kind === 'kde' && (
        <Section
          title="Density curve"
          hint="A smoothed histogram: a Gaussian kernel density estimate of the measure, one curve per colour, each with area 1 so groups of different sizes compare by shape. Finer than bins, but it smooths over gaps and spikes (a pile of zeros reads as a bump)."
        >
          <KdeSection options={spec.kde ?? undefined} capability={capabilities?.kde} onChange={setKde} />
        </Section>
      )}
      {spec?.kind === 'regression' && (
        <Section
          title="Regression"
          hint="A least-squares line through each colour's points (or all of a panel's), with R², p (slope ≠ 0) and n on the plot."
        >
          <RegressionSection
            options={spec.regression ?? undefined}
            capability={capabilities?.regression}
            fits={fitsMeta}
            onChange={setRegression}
          />
        </Section>
      )}
    </Group>
  )
}

/** The rail's Structure group, moved verbatim out of PlotStudio's render. */
export function StructureGroup({ s }: { s: PlotStudioState }) {
  const {
    design,
    describe,
    spec,
    capabilities,
    factors,
    grouping,
    openGroups,
    toggleGroup,
    setRole,
    moveGroupLayer,
    setColor,
    setFacet,
    setComparison,
    setRuleAt,
    openCombine,
    setOpenCombine,
    addLevelGroup,
    editLevelGroup,
    renameLevelGroup,
    switchCombineSlot,
    removeLevelGroup,
    groupLayers,
    groupableVariables,
    setGroupPickerOpen,
    bucketable,
    listedFactors,
    sourceLevels,
    faceted,
    effRows,
    effCols,
    singlePanel,
    layoutNotes,
    compareMeta,
  } = s
  return (
    <Group
      title="Structure"
      open={openGroups.structure}
      onToggle={() => toggleGroup('structure')}
      summary={structureSummary({
        grouped: groupLayers,
        color: spec?.color ?? null,
        roles: (spec?.roles ?? {}) as Record<string, string>,
        compare: compareSummary(spec?.comparison),
      })}
    >
      {/* Grouping: what groups EXIST, then which of them group the x axis.
          These were two sections with near-identical names ("Groups" and
          an "X grouping" list that only appeared once two factors already
          held X, making it unreachable until the user found the role
          dropdown). One question, one place, read top to bottom. */}
      {/* Combine: make a factor with fewer levels, which REPLACES its
          source everywhere a role is chosen. Above Grouping because
          Grouping and Factors list its result — and switch back to the
          source from the result's name there. */}
      {(bucketable.length > 0 || (spec?.level_groups ?? []).length > 0) && (
        <Section
          title="Combine"
          hint="Combine a factor's levels into fewer, averaging each combined group — e.g. four stim conditions into one STIM. The result replaces the factor everywhere; switch back from its name in Grouping or Factors. To keep the original levels and group them by a label, record the label as a variable and use Group by…."
        >
          {(spec?.level_groups ?? []).map((group, index) => (
            <CombineEditor
              key={index}
              group={group}
              levels={sourceLevels(group.source)}
              open={openCombine === index}
              onToggle={() => setOpenCombine(open => (open === index ? null : index))}
              onEdit={patch => editLevelGroup(index, patch)}
              onRename={name => renameLevelGroup(index, name)}
              onUse={() => switchCombineSlot(group.source, group.name)}
              onRemove={() => removeLevelGroup(index)}
            />
          ))}
          <BucketAdder factors={bucketable} onAdd={addLevelGroup} />
        </Section>
      )}

      <Section
        title="Grouping"
        hint="Grouping the data already records (Group by…) and the factors that group the marks. A combined factor's name is a dropdown: switch back to the factor it replaced."
      >
        {/* Refusals count towards showing this: if every column of a sheet
            was rejected, the reasons are the only thing that explains an
            otherwise empty section. */}
        {(groupableVariables.length > 0 ||
          Object.keys(describe?.groupable_refused ?? {}).length > 0) && (
          <>
            {/* One button, and the groupings it produced. The flat list of
                every column of every wide variable that used to live here
                did not scale — a demographics sheet is 18 checkboxes and
                ~100 queries on every panel open — and it could not express
                WHICH version of the sheet supplies the labels. Both are the
                picker's job now, on the canvas the user already knows. */}
            <button
              type="button"
              style={styles.locationButton}
              onClick={() => setGroupPickerOpen(true)}
              disabled={groupableVariables.length === 0}
              title={
                groupableVariables.length === 0
                  ? 'No variable is recorded at or above this data’s level.'
                  : 'Choose what stratifies this figure'
              }
            >
              {(spec?.factor_variables ?? []).length === 0
                ? 'Group by…'
                : `Group by: ${(spec?.factor_variables ?? [])
                    .map(f => f.column ?? f.variable)
                    .join(', ')}`}
            </button>
            {(spec?.factor_variables ?? []).map(group => {
              const name = group.column ?? group.variable
              return (
                <div key={name} style={styles.kindRow}>
                  <span style={styles.factorName}>{name}</span>
                  <span style={styles.groupRole}>
                    {groupingPlacement(spec, groupLayers, factors, name)}
                  </span>
                </div>
              )
            })}
            {/* Variables refused in place, for the reason the variant
                picker shows them: one a user expected to group by, absent
                with no explanation, is indistinguishable from a bug. */}
            {Object.entries(describe?.groupable_refused ?? {}).map(
              ([label, reason]) => (
                <div key={label} style={styles.refusedRow} title={reason}>
                  {label} — {reason}
                </div>
              )
            )}
          </>
        )}

        {/* The grouping list: one mark per level combination, innermost
            first, one layer optionally coloured. What a "mark" is — a
            bar, a line, a spaghetti line — is the kind's reading, and the
            backend says it in `hint`. A 2-D measure refuses with a reason. */}
        {grouping?.available ? (
          <GroupingList
            factors={listedFactors}
            layers={groupLayers}
            onSwitchSlot={switchCombineSlot}
            color={spec?.color ?? null}
            hint={grouping.hint}
            labelled={grouping.labelled_layers ?? groupLayers.length}
            maxLabelled={grouping.max_labelled_layers}
            onToggle={(name, on) => setRole(name, on ? 'group' : 'iterate')}
            onMove={moveGroupLayer}
            onColor={setColor}
          />
        ) : (
          grouping?.reason && (
            <div style={styles.hint}>
              <strong>Grouping:</strong> {grouping.reason}
            </div>
          )
        )}
      </Section>

      {/* Compare: a grouping layer's levels relative to one of them, as a
          difference or a % change. Right under Grouping because the
          reference is a level of a grouping layer. */}
      {capabilities?.comparison && (
        <Section
          title="Compare"
          hint="Show each level of a grouping layer relative to a reference level: the difference, or the % change. The reference sits at 0. Paired when each unit (a subject) has its own reference value, the rule Show sample uses to join points; otherwise each value is compared with the reference's centre. Every other control still applies."
        >
          <CompareSection
            comparison={spec?.comparison}
            capability={capabilities.comparison}
            meta={compareMeta}
            onChange={setComparison}
          />
        </Section>
      )}

      <Section
        title="Factors"
        hint="Everything not grouped: separate figures, separate panels, or collapsed (averaged). The last collapsed key is the sample the error bars are drawn over."
      >
        {listedFactors.filter(factor => spec?.roles?.[factor.name] !== 'group').map(factor => (
          <label key={factor.name} style={styles.factorRow}>
            <span style={styles.factorName}>
              <SlotName factor={factor} factors={factors} onSwitch={switchCombineSlot} />
              {factor.is_variant && <span style={styles.variantTag} title="A pipeline variant, not a replicate">variant</span>}
              {factor.is_field && <span style={styles.fieldTag} title="The fields of this struct/dict variable — one subplot each by default">fields</span>}
              <span style={styles.levelCount}>{factor.level_count}</span>
            </span>
            <select
              value={spec?.roles?.[factor.name] ?? 'iterate'}
              onChange={e => setRole(factor.name, e.target.value as Role)}
              style={styles.select}
            >
              {/* `factors` falls back to describe's raw table, which
                  carries no role report — an empty <select> would be a
                  dead control, so show at least what it is set to. */}
              {(factor.roles ?? fallbackRoles(spec?.roles?.[factor.name])).map(option => (
                <option
                  key={option.role}
                  value={option.role}
                  // Disabled rather than hidden: a role that is unavailable
                  // BECAUSE of another choice ("something else already has
                  // Color") has to stay visible, or the control silently
                  // shrinks and the user cannot see what to undo.
                  disabled={!option.available}
                  title={option.reason ?? option.hint}
                >
                  {design?.roleLabels?.[option.role] ?? option.label}
                  {option.available ? '' : ' —'}
                </option>
              ))}
            </select>
          </label>
        ))}
      </Section>

      {faceted && (
        <Section
          title="Layout"
          hint="Set rows or columns — the other follows from the number of subplots. Then name what belongs in each one; blank takes whatever is left, in order."
        >
          <div style={styles.gridSizeRow}>
            <GridSizeInput
              label="N rows"
              value={spec?.facet?.n_rows ?? null}
              effective={effRows}
              locked={singlePanel}
              onChange={n => setFacet({ n_rows: n })}
            />
            <GridSizeInput
              label="N columns"
              value={spec?.facet?.n_cols ?? null}
              effective={effCols}
              locked={singlePanel}
              onChange={n => setFacet({ n_cols: n })}
            />
          </div>

          {singlePanel ? (
            <div style={styles.hint}>
              One panel, so the grid is 1 × 1. Rows, columns and slots you
              set apply again once there is more than one panel.
            </div>
          ) : (
            <>
              <RuleSlots
                title="Rows"
                count={effRows}
                rules={spec?.facet?.rows ?? []}
                onEdit={(i, patch) => setRuleAt('rows', i, patch)}
              />
              <RuleSlots
                title="Columns"
                count={effCols}
                rules={spec?.facet?.cols ?? []}
                onEdit={(i, patch) => setRuleAt('cols', i, patch)}
              />
            </>
          )}

          {/* Never let the layout quietly disobey a rule: if a subplot had
              to move, or the grid had to grow, say which and why. */}
          {layoutNotes.map((note, index) => (
            <div key={index} style={styles.layoutNote}>{note}</div>
          ))}
        </Section>
      )}
    </Group>
  )
}

/** The rail's Statistics group, moved verbatim out of PlotStudio's render. */
export function StatisticsGroup({ s }: { s: PlotStudioState }) {
  const {
    spec,
    capabilities,
    openGroups,
    toggleGroup,
    setAggregate,
    setShowSample,
    setJoinSample,
    setSampleColor,
    setSampleInLegend,
    setDifferenceBars,
    picking,
    setPicking,
    diffPanel,
    setDiffPanel,
    cancelPicking,
    summarizing,
    diffMeta,
    rugMeta,
  } = s
  return (
    <Group
        title="Statistics"
        open={openGroups.statistics}
        onToggle={() => toggleGroup('statistics')}
        summary={statisticsSummary({
          summarizing,
          centre: spec?.aggregate?.statistic ?? 'mean',
          spread: spec?.aggregate?.error ?? 'sd',
          pooled: spec?.aggregate?.pooled ?? false,
          shown: capabilities?.sample_overlay?.shown ?? [],
          differenceBars: barCount(spec?.difference_bars),
        })}
      >
        {summarizing && (
          <Section title="Summary">
            <label style={styles.factorRow}>
              <span style={styles.factorName}>Centre</span>
              <select
                value={spec?.aggregate?.statistic ?? 'mean'}
                onChange={e => setAggregate({ statistic: e.target.value as Statistic })}
                style={styles.select}
              >
                <option value="mean">Mean</option>
                <option value="median">Median</option>
              </select>
            </label>
            <label style={styles.factorRow}>
              <span style={styles.factorName}>Spread</span>
              <select
                value={spec?.aggregate?.error ?? 'sd'}
                onChange={e => setAggregate({ error: e.target.value as ErrorBand })}
                style={styles.select}
              >
                <option value="sd">SD</option>
                <option value="sem">SEM</option>
                <option value="ci95" title="t-based: centre ± t(0.975, n−1)·SD/√n">95% CI (t-based)</option>
                <option value="iqr">IQR</option>
                <option value="none">None</option>
              </select>
            </label>
            {/* The chain, as the figure runs it: nested and unweighted by
                default, each subject counting once however many trials it
                has; pooled is the deliberate alternative (weight by N). */}
            <label style={styles.factorRow} title="Drop every collapsed factor in one step, so a subject with more trials weighs more">
              <span style={styles.factorName}>Weight by N</span>
              <input
                type="checkbox"
                checked={spec?.aggregate?.pooled ?? false}
                onChange={e => setAggregate({ pooled: e.target.checked })}
              />
            </label>
            <div style={styles.hint}>{sampleNote(capabilities, spec)}</div>
          </Section>
        )}

        {capabilities?.sample_overlay && (
          <Section
            title="Show sample"
            hint={
              capabilities.sample_overlay.available
                ? "Overlay the collapsed keys' data as points: tick a key to see one point per level of it. Deeper keys imply the shallower ones."
                : undefined
            }
          >
            {/* The collapsed keys' data drawn on top of the marks — for the
                distribution behind a bar, and for the raw data behind it.
                Python decides what a tick means (a deeper key implies the
                shallower ones: a trial is a trial OF a subject), whether the
                points are joined and why; this section displays the answer.
                Its Weight lives with the other mark weights, in Appearance. */}
            {!capabilities.sample_overlay.available ? (
              <div style={styles.hint}>{capabilities.sample_overlay.reason}</div>
            ) : (
              <>
                {capabilities.sample_overlay.factors.map(factor => (
                  <label
                    key={factor.name}
                    style={styles.factorRow}
                    title={isLocked(factor) ? `Implied by a deeper key — part of every point's identity` : undefined}
                  >
                    <span style={styles.factorName}>{factor.name}</span>
                    <input
                      type="checkbox"
                      checked={isTicked(factor)}
                      disabled={isLocked(factor)}
                      onChange={e => setShowSample(factor.name, e.target.checked)}
                    />
                  </label>
                ))}
                {capabilities.sample_overlay.shown.length > 0 &&
                  capabilities.sample_overlay.form === 'rug' && (
                    <div style={styles.hint}>{rugScaleNote(rugMeta)}</div>
                  )}
                {capabilities.sample_overlay.shown.length > 0 &&
                  capabilities.sample_overlay.form !== 'rug' && (
                  <>
                    <label
                      style={styles.factorRow}
                      title={joinTooltip(capabilities.sample_overlay.join)}
                    >
                      <span style={styles.factorName}>Join points</span>
                      <select
                        value={joinChoice(spec?.join_sample)}
                        onChange={e => setJoinSample(e.target.value as JoinChoice)}
                        style={styles.select}
                      >
                        <option value="auto">
                          Auto ({capabilities.sample_overlay.join.automatic
                            ? capabilities.sample_overlay.join.join ? 'lines' : 'points'
                            : '…'})
                        </option>
                        <option value="lines">Lines</option>
                        <option value="points">Points</option>
                      </select>
                    </label>
                    {/* The overlay's own colour — one per level of a shown
                        key, independent of the Grouping colour (a shown key
                        is collapsed, a coloured layer groups; no factor is
                        both). With it, a joined line runs across the marks'
                        colours: pre → post inside one group. */}
                    <label
                      style={styles.factorRow}
                      title="Colour the points (and their lines) by one of the shown keys, separately from the marks' colour. Lines then join a subject across the coloured bars."
                    >
                      <span style={styles.factorName}>Colour points by</span>
                      <select
                        value={sampleColorChoice(spec?.sample_color)}
                        onChange={e => setSampleColor(e.target.value)}
                        style={styles.select}
                      >
                        <option value={MARK_COLOR}>Mark's colour</option>
                        {capabilities.sample_overlay.color.options.map(name => (
                          <option key={name} value={name}>{name}</option>
                        ))}
                        {spec?.sample_color &&
                          !capabilities.sample_overlay.color.options.includes(spec.sample_color) && (
                            <option value={spec.sample_color}>{spec.sample_color} (not shown)</option>
                          )}
                      </select>
                    </label>
                    {/* Listing the overlay's levels can take half the
                        figure's width (14 subjects); unticked, the legend
                        reads as though nothing were shown. Only an
                        overlay with its OWN colour is ever listed. */}
                    <label
                      style={styles.factorRow}
                      title={
                        capabilities.sample_overlay.color.active
                          ? "List the points' colours in the legend. Unticked, the legend reads as though nothing were shown here; the points keep their colours."
                          : "The points take their mark's colour, so the legend has nothing to list for them. Colour them by a shown key to use this."
                      }
                    >
                      <span style={styles.factorName}>Show in legend</span>
                      <input
                        type="checkbox"
                        checked={spec?.sample_in_legend !== false}
                        disabled={!capabilities.sample_overlay.color.active}
                        onChange={e => setSampleInLegend(e.target.checked)}
                      />
                    </label>
                    <div style={styles.hint}>{capabilities.sample_overlay.granularity}</div>
                  </>
                )}
                {capabilities.sample_overlay.ignored.length > 0 && (
                  <div style={styles.hint}>
                    Not collapsed right now, so not shown: {capabilities.sample_overlay.ignored.join(', ')}
                  </div>
                )}
              </>
            )}
          </Section>
        )}
        {/* Bars joining two marks of a panel, labelled ("*"): a test's
            result. Picked by clicking marks in the preview; placed by
            Python (scistackplot.diffbars), listed from layout.meta. */}
        {capabilities?.difference_bars && (
          <Section
            title="Difference bars"
            hint="Mark two groups as different: + Add difference bar, then click a mark in each (a bar, a box or any of its sample points). Bars are placed above everything drawn, and the y axis grows to hold them. A bar belongs to this figure's panel only."
          >
            <DifferenceBarsSection
              meta={diffMeta}
              bars={spec?.difference_bars}
              capability={capabilities.difference_bars}
              picking={picking}
              panelIndex={diffPanel}
              onPanel={setDiffPanel}
              onStartPicking={() => {
                console.info('[Plot Studio] difference_bar_picking_started')
                setPicking({ phase: 'first' })
              }}
              onCancelPicking={cancelPicking}
              onBars={setDifferenceBars}
            />
          </Section>
        )}
      </Group>
  )
}

/** The rail's Appearance group, moved verbatim out of PlotStudio's render. */
export function AppearanceGroup({ s }: { s: PlotStudioState }) {
  const {
    csvPath,
    spec,
    capabilities,
    figures,
    openGroups,
    toggleGroup,
    railWidth,
    setShowMissingLevels,
    setYAxis,
    setStyle,
    setPanelOverrides,
    setWeight,
    setTextSize,
    resetText,
    setTextTable,
    resetProportions,
    setTextTarget,
    setTitleText,
    setAliases,
    setColors,
    setMarkColor,
    writeProjectAlias,
    writeProjectColor,
    toggleYScope,
    gridMeta,
    markWeights,
    panelMeta,
    panelOverrideCount,
    temporalMeta,
    yScopeFactors,
    yScope,
    appliedYLimits,
    presets,
    figWidth,
    figHeight,
    fontSize,
    textMeta,
    drawnFont,
    aspect,
    onAspect,
  } = s
  return (
    <Group
      title="Appearance"
      open={openGroups.appearance}
      onToggle={() => toggleGroup('appearance')}
      summary={appearanceSummary({
        width: figWidth,
        height: figHeight,
        font: fontSize,
        fontTarget: textTarget(spec?.style?.text),
        yMin: spec?.y_axis?.minimum ?? null,
        yMax: spec?.y_axis?.maximum ?? null,
        panelOverrides: panelOverrideCount,
      })}
    >
      <Section
        title="Size"
        hint="Applies to saved images, exported code and the pipeline step. Pick a ratio here and set the width in the toolbar over the figure (journal columns: 3.5 in single, 7.2 in double); the height follows."
      >
        <label style={styles.factorRow}>
          <span style={styles.factorName}>Aspect</span>
          <select
            value={aspect}
            onChange={e => onAspect(e.target.value)}
            style={styles.select}
          >
            {presets.map(preset => (
              <option
                key={preset.name}
                value={preset.name}
                title={preset.ratio == null ? 'The size matches no ratio. Pick a ratio to set the height from the width.' : preset.hint}
                disabled={preset.ratio == null}
              >
                {preset.label}
              </option>
            ))}
          </select>
        </label>
      </Section>

      <Section
        title={gridMeta.axis_sides?.y === 'bottom' ? 'Y axis (values, along the bottom)' : 'Y axis'}
        hint="Which plots share one y scale. Nothing ticked means every plot in the dataset gets the same limits; ticking everything autoscales each subplot to its own data."
      >
        {yScopeFactors.length === 0 ? (
          <div style={styles.hint}>
            One plot, so there is nothing to separate — give a factor the
            <em> separate figures</em> or <em>subplot</em> role to scale
            them apart.
          </div>
        ) : (
          yScopeFactors.map(name => (
            <label key={name} style={styles.factorRow}>
              <input
                type="checkbox"
                checked={yScope.includes(name)}
                onChange={e => toggleYScope(name, e.target.checked)}
              />
              <span style={styles.factorName}>{name}</span>
            </label>
          ))
        )}
        <div style={styles.gridSizeRow}>
          <LimitInput
            label="Min"
            value={spec?.y_axis?.minimum ?? null}
            onChange={v => setYAxis({ minimum: v })}
            temporal={temporalMeta !== null}
            onDate={v => setYAxis({ minimum: v })}
          />
          <LimitInput
            label="Max"
            value={spec?.y_axis?.maximum ?? null}
            onChange={v => setYAxis({ maximum: v })}
            temporal={temporalMeta !== null}
            onDate={v => setYAxis({ maximum: v })}
          />
        </div>
        {/* The rule AND the numbers it produced: "why is this 0.61?" has to
            have an answer the user can read off the panel. */}
        <div style={styles.layoutNote}>
          {yScope.length === 0
            ? 'Same limits everywhere'
            : `Separate limits per ${yScope.join(', ')}`}
          {appliedYLimits
            ? ` — ${formatLimit(appliedYLimits[0], temporalMeta !== null)} to ${formatLimit(appliedYLimits[1], temporalMeta !== null)}`
            : ''}
          {panelOverrideCount > 0
            ? ` (${panelOverrideCount} panel${panelOverrideCount === 1 ? '' : 's'} set in Panels)`
            : ''}
        </div>
      </Section>

      {/* One faceted panel's own y limits and y title, and which panels
          draw a y title at all. Only for a grid of faceted panels. */}
      {offersPanels(panelMeta) && panelMeta && (
        <Section
          title="Panels"
          hint="Settings for one subplot, over the figure's. A panel is named by its facet values, so a setting applies to that panel in every separate figure. Hiding y titles gives their room back to the panels."
        >
          <PanelsSection
            meta={panelMeta}
            overrides={spec?.panel_overrides}
            yTitles={(spec?.style?.y_titles as YTitles | undefined) ?? 'every_panel'}
            onOverrides={setPanelOverrides}
            onYTitles={value => setStyle({ y_titles: value })}
            renderLimit={(label, value, placeholder, onChange) => (
              <LimitInput
                label={label}
                value={value}
                placeholder={placeholder}
                onChange={onChange}
              />
            )}
          />
        </Section>
      )}

      {/* x tick labels: fitted by default (strip a shared ID prefix,
          wrap, shrink, rotate, every k-th). Any of these may be fixed;
          a fixed value is kept even where it overlaps, and the notice
          below says so. */}
      <Section title="X tick labels">
        {(() => {
          // Offered only when it would change something: the plot holds
          // x levels this figure has no data for (ResolvedPlot.missing_x).
          const missing = gridMeta.missing_x ?? []
          if (missing.length === 0) return null
          const shown = missing.slice(0, 6).join(', ') + (missing.length > 6 ? ', …' : '')
          return (
            <label
              style={styles.checkRow}
              title={`Keep an empty tick for each level the rest of the plot has but this figure does not (${shown}), so the gap reads as missing data. Unticked, the axis holds only this figure's levels and the marks get wider.`}
            >
              <input
                type="checkbox"
                checked={spec?.show_missing_levels !== false}
                onChange={e => setShowMissingLevels(e.target.checked)}
              />
              <span style={styles.checkLabel}>
                Show missing levels ({missing.length}: {shown})
              </span>
            </label>
          )
        })()}
        <label
          style={styles.factorRow}
          title="Auto rotates only when the labels would overlap upright"
        >
          <span style={styles.factorName}>Rotation</span>
          <select
            value={spec?.style?.tick_rotation == null ? 'auto' : String(spec.style.tick_rotation)}
            onChange={e =>
              setStyle({ tick_rotation: e.target.value === 'auto' ? null : Number(e.target.value) })
            }
            style={styles.select}
          >
            <option value="auto">Auto</option>
            <option value="0">0°</option>
            <option value="45">45°</option>
            <option value="90">90°</option>
          </select>
        </label>
        <label
          style={styles.factorRow}
          title="Auto shows every label when they fit; numbered labels (01, SS02…) are thinned when they cannot"
        >
          <span style={styles.factorName}>Show</span>
          <select
            value={spec?.style?.tick_every == null ? 'auto' : String(spec.style.tick_every)}
            onChange={e =>
              setStyle({ tick_every: e.target.value === 'auto' ? null : Number(e.target.value) })
            }
            style={styles.select}
          >
            <option value="auto">Auto</option>
            <option value="1">Every label</option>
            {[2, 3, 4, 5, 10].map(k => (
              <option key={k} value={String(k)}>Every {k}{k === 2 ? 'nd' : k === 3 ? 'rd' : 'th'}</option>
            ))}
          </select>
        </label>
        {/* Checkbox first: the label is long and has to wrap. */}
        <label
          style={styles.checkRow}
          title="Blank the labels of a layer that is also the colour, while the legend lists the same levels in the same colours. The ticks stay; a bracket row whose labels are hidden loses its lines too."
        >
          <input
            type="checkbox"
            checked={spec?.style?.hide_legend_ticks === true}
            onChange={e => setStyle({ hide_legend_ticks: e.target.checked })}
          />
          <span style={styles.checkLabel}>Hide labels the legend repeats</span>
        </label>
        {(() => {
          // The fit's own verdict, from the figure on screen.
          const fit = (figures[0]?.figure?.layout?.meta as
            | { label_fit?: { fits?: boolean; ticks?: string | null } }
            | undefined)?.label_fit
          if (!fit || fit.fits !== false) return null
          return (
            <div style={styles.layoutNote}>
              The x labels still overlap at this size ({fit.ticks}). Widen the
              figure, lower the font, or show fewer labels.
              {fixedTickNote(spec?.style?.text) && ` ${fixedTickNote(spec?.style?.text)}`}
            </div>
          )
        })()}
      </Section>

      {/* Both mark weights in one place: a spaghetti's own points + lines
          (StyleOptions.line_weight) and the sample overlay's
          (StyleOptions.sample_weight). Python says when each applies
          (layout.meta.mark_weights, capabilities.sample_overlay). */}
      {(markWeights?.lines?.applies ||
        (capabilities?.sample_overlay?.available &&
          capabilities.sample_overlay.form !== 'rug' &&
          capabilities.sample_overlay.shown.length > 0)) && (
        <Section title="Marks">
          {markWeights?.lines?.applies && (
            <WeightInput
              label="Line weight"
              value={typeof spec?.style?.line_weight === 'number' ? spec.style.line_weight : null}
              title={weightTitle(markWeights.lines, 'lines')}
              onChange={value => setWeight('line_weight', value)}
            />
          )}
          {capabilities?.sample_overlay?.available &&
            capabilities.sample_overlay.form !== 'rug' &&
            capabilities.sample_overlay.shown.length > 0 && (
              <WeightInput
                label="Sample weight"
                value={typeof spec?.style?.sample_weight === 'number' ? spec.style.sample_weight : null}
                title={weightTitle(markWeights?.sample, 'sample points')}
                onChange={value => setWeight('sample_weight', value)}
              />
            )}
        </Section>
      )}

      {/* One box per text element (StyleOptions.text). Font empty =
          AUTO: Python sizes every empty element as large as the figure
          lays it out cleanly, inside the Print / Slide band
          (scistackplot.autosize). Every placeholder is the size Python
          chose or derived (layout.meta.text_sizes), never computed
          here. A typed size is fixed: auto and the label and legend
          fits never shrink it. */}
      <Section title="Text">
        <div style={styles.textSizesHeader}>
          <SizeInput
            label="Font (pt)"
            value={fontSize}
            placeholder={basePlaceholder(textMeta)}
            resolved={drawnFont}
            onChange={base => setTextSize('base', base)}
            title={baseTitle(textMeta)}
          />
          <label
            style={styles.factorRow}
            title={
              'Where the figure is going: the band auto sizes within. ' +
              'Print 8–12 pt, Slide 14–28 pt, at the saved size — place the ' +
              'figure at its saved size for the band to hold. Ignored when Font is fixed'
            }
          >
            <select
              value={textTarget(spec?.style?.text)}
              onChange={e => setTextTarget(e.target.value)}
              style={styles.select}
              disabled={fontSize !== null}
            >
              {textTargets(textMeta).map(target => (
                <option key={target} value={target}>
                  {target === 'print' ? 'Print' : target === 'slide' ? 'Slide' : target}
                </option>
              ))}
            </select>
          </label>
          {hasFixedSizes(spec?.style?.text) && (
            <button
              type="button"
              style={styles.inlineButton}
              title="Clear every size below so they all follow Font (or auto) again"
              onClick={resetText}
            >
              Reset
            </button>
          )}
        </div>
        <div
          style={{
            ...styles.textSizesGrid,
            gridTemplateColumns: `repeat(${textSizeColumns(railWidth)}, 1fr)`,
          }}
        >
          {textSizeRows(textMeta, spec?.style?.text).map(row => {
            const value = textSizeOf(spec?.style?.text, row.key)
            return (
              <SizeInput
                key={row.key}
                label={row.label}
                title={rowTitle(row)}
                value={typeof value === 'number' ? value : null}
                placeholder={placeholderFor(row)}
                resolved={row.resolved ?? drawnFont}
                onChange={next => setTextSize(row.key, next)}
              />
            )
          })}
        </div>
        {/* What auto decided (autosize.AutoTextSize): the reference and
            the rule that stopped it one step larger. */}
        {autoSummary(textMeta) && (
          <div style={styles.textAutoSummary} title={baseTitle(textMeta)}>
            {autoSummary(textMeta)}
          </div>
        )}
        {/* The intended proportions (TextSizes.ratios / floors /
            ceilings), stored as typed and never altered by sizing.
            Placeholders are Python's effective values; Drawn is what
            this layout drew, marked when held at a bound. */}
        <div style={styles.textSizesHeader}>
          <span style={styles.textProportionsTitle}>Proportions</span>
          {hasTextTables(spec?.style?.text) && (
            <button
              type="button"
              style={styles.inlineButton}
              title="Every ratio, floor and ceiling back to the defaults"
              onClick={resetProportions}
            >
              Reset
            </button>
          )}
        </div>
        <div style={styles.textProportionsGrid}>
          <span />
          <span style={styles.textProportionsHead} title="Size over the reference (Font)">Ratio</span>
          <span style={styles.textProportionsHead} title="Auto never draws it smaller (pt)">Floor</span>
          <span style={styles.textProportionsHead} title="Auto never draws it larger (pt)">Ceiling</span>
          <span style={styles.textProportionsHead} title="What this layout drew (pt)">Drawn</span>
          {proportionRows(textMeta, spec?.style?.text).map(row => (
            <Fragment key={row.key}>
              <span style={styles.textSizeLabel}>{row.label}</span>
              {(['ratios', 'floors', 'ceilings'] as const).map(table => {
                const stored = table === 'ratios' ? row.ratio : table === 'floors' ? row.floor : row.ceiling
                const effective =
                  table === 'ratios'
                    ? row.effectiveRatio
                    : table === 'floors'
                      ? row.effectiveFloor
                      : row.effectiveCeiling
                return (
                  <StepperInput
                    key={table}
                    value={stored}
                    fallback={effective}
                    step={table === 'ratios' ? 0.1 : 0.5}
                    min={table === 'ratios' ? 0.1 : 1}
                    allowBlank
                    decimals={2}
                    width={44}
                    placeholder={effective === null ? '—' : formatPt(effective)}
                    title={
                      row.fixed
                        ? `${row.label} has a fixed size: its ratio and bounds are not used`
                        : table !== 'ratios' && fontSize !== null
                          ? 'Font is fixed: bounds apply only to auto'
                          : undefined
                    }
                    onChange={next => setTextTable(table, row.key, next)}
                  />
                )
              })}
              <span
                style={row.clamped ? styles.textProportionsClamped : styles.textProportionsDrawn}
                title={row.clamped ? `Held at its ${row.clamped}: the ratio is kept for a larger canvas` : undefined}
              >
                {drawnLabel(row)}
              </span>
            </Fragment>
          ))}
        </div>
      </Section>

      {/* Titles and display aliases. A CSV plot has no project, so it gets
          the plot's own aliases without the project buttons. */}
      <Section title="Labels">
        <LabelsSection
          labelable={
            (figures[0]?.figure?.layout?.meta as { labelable?: Labelable[] } | undefined)
              ?.labelable ?? []
          }
          aliases={spec?.aliases}
          titles={{
            title: (spec?.style?.title as string | null | undefined) ?? null,
            x_label: (spec?.style?.x_label as string | null | undefined) ?? null,
            y_label: (spec?.style?.y_label as string | null | undefined) ?? null,
          }}
          onTitle={setTitleText}
          onAliases={setAliases}
          onProject={writeProjectAlias}
          projectEnabled={!csvPath}
          axisSides={gridMeta.axis_sides}
        />
      </Section>

      {/* Mark colours per painted level, and the single mark colour.
          A CSV plot has no project: the plot's own colours only. */}
      <Section title="Colours">
        <ColorsSection
          colorable={
            (figures[0]?.figure?.layout?.meta as { colorable?: Colorable[] } | undefined)
              ?.colorable ?? []
          }
          colors={spec?.colors}
          markColor={plotMarkColor(spec?.style)}
          onColors={setColors}
          onMarkColor={setMarkColor}
          onProject={writeProjectColor}
          projectEnabled={!csvPath}
        />
      </Section>
    </Group>
  )
}

