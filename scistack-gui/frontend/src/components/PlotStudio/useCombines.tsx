// PlotStudio state, part 3 of 8: Structure > Combine (the edits themselves live in combine.ts).
import { useState, useCallback, useMemo } from 'react'
import { addCombine, type LevelGroup, renameCombine, switchSlot, removeCombine } from './combine'
import { VariantSet } from './plotStudioTypes'
import { describeSelection, asSelection } from './locationSelection'
import { orderGroups, placeGroupLayer } from './groups'
import type { StudioCoreState } from './useStudioCore'
import type { StepByStepState } from './useStepByStep'

/** PlotStudio: Structure > Combine (the edits themselves live in combine.ts). Moved verbatim out of usePlotStudio (grug
 *  plotstudio_split); called in order by usePlotStudio. */
export function useCombines(s: StudioCoreState & StepByStepState) {
  const {
    variable,
    describe,
    spec,
    setSpec,
    capabilities,
    factors,
    factorDepths,
    grouping,
    setAddingVariant,
    capsSpecRef,
    specKey,
  } = s

  // --- combines (Structure > Combine; the edits live in `combine.ts`) ------
  //
  // Which combine row is expanded. ONE at a time, held here rather than in
  // each row: a row's own state was lost whenever it remounted, which is how
  // an edited combine kept springing back open. A new combine opens; every
  // other row shows its one-line summary.
  const [openCombine, setOpenCombine] = useState<number | null>(null)

  const addLevelGroup = useCallback((source: string, levels: (string | number)[]) => {
    setSpec(prev => {
      if (!prev) return prev
      const taken = factors.map(f => f.name)
      const next = addCombine(prev, source, levels, taken)
      setOpenCombine((next.level_groups ?? []).length - 1)
      console.debug('[combine] add', source, '->', (next.level_groups ?? [])[(next.level_groups ?? []).length - 1]?.name)
      return next
    })
  }, [factors])

  /** Mapping / catch-all edits. The name has its own edit, `renameLevelGroup`,
   *  because the slot (role, grouping position, colour) is keyed by it. */
  const editLevelGroup = useCallback((index: number, patch: Partial<LevelGroup>) => {
    setSpec(prev => {
      if (!prev) return prev
      const groups = [...(prev.level_groups ?? [])]
      if (!groups[index]) return prev
      groups[index] = { ...groups[index], ...patch }
      return { ...prev, level_groups: groups }
    })
  }, [])

  const renameLevelGroup = useCallback((index: number, name: string) => {
    setSpec(prev => (prev ? renameCombine(prev, index, name) : prev))
  }, [])

  /** The slot dropdown in Grouping / Factors: hand `source`'s slot to `choice`. */
  const switchCombineSlot = useCallback((source: string, choice: string) => {
    console.debug('[combine] slot', source, '->', choice)
    setSpec(prev => (prev ? switchSlot(prev, source, choice) : prev))
  }, [])

  const removeLevelGroup = useCallback((index: number) => {
    setOpenCombine(open => (open === index ? null : open !== null && open > index ? open - 1 : open))
    setSpec(prev => (prev ? removeCombine(prev, index) : prev))
  }, [])

  /** Commit the row the "+ Add variant" picker built.
   *
   *  The row is created HERE, on apply — not by the "+" click. Clicking "+"
   *  used to append an empty row immediately, which meant the list grew before
   *  the user had said anything and a cancelled thought left "(not set)"
   *  behind. Now "+" asks two questions (which variable, which variant) and
   *  nothing enters the spec until both are answered.
   *
   *  The row arrives already pinned, because the picker seeded its selection
   *  from `variants.default_selection` — so a new row is ONE variant, the same
   *  way the panel opened on one. An empty selection would mean "every variant
   *  of this variable", which is never what clicking "+" meant. */
  const addVariantFromPicker = useCallback(
    (next: {
      selection: Record<string, unknown>
      name: string
      variable?: string
    }) => {
      setSpec(prev => {
        if (!prev) return prev
        // Row 0 already exists: `roles.default_spec` seeds it for every table,
        // variants or not. This used to seed it HERE, lazily, on the way to
        // adding the second — which meant the panel opened with an empty
        // Variants list saying nothing about the variable on screen, and put a
        // rule about what a figure opens on in TypeScript (CLAUDE.md NOTE 3).
        return {
          ...prev,
          variant_sets: [
            ...(prev.variant_sets ?? []),
            {
              name: next.name || null,
              selection: next.selection,
              variable: next.variable ?? null,
            },
          ],
        }
      })
      setAddingVariant(false)
    },
    []
  )

  const removeVariantSet = useCallback((index: number) => {
    setSpec(prev =>
      prev
        ? { ...prev, variant_sets: (prev.variant_sets ?? []).filter((_, i) => i !== index) }
        : prev
    )
  }, [])

  /** Adopt a combination the data actually holds, from the empty-state panel.
   *
   *  Replaces the selection outright rather than merging into it. The pin that
   *  produced the empty figure is exactly what has to go, and merging would
   *  keep the latest flag beside an explicit version — rows that are
   *  simultaneously the newest and an old version, which is nothing at all.
   *  Same rule as the popup's `withoutLatestFlag`: an explicit choice
   *  supersedes the shortcut. */
  const adoptCombination = useCallback(
    (index: number, combination: Record<string, string>) => {
      setSpec(prev => {
        if (!prev) return prev
        const sets = [...(prev.variant_sets ?? [])]
        if (!sets[index]) return prev
        // An explicit choice also replaces a reopened plot's frozen version.
        sets[index] = { ...sets[index], selection: { ...combination }, frozen: undefined }
        return { ...prev, variant_sets: sets }
      })
    },
    []
  )

  const editVariantSet = useCallback((index: number, patch: Partial<VariantSet>) => {
    setSpec(prev => {
      if (!prev) return prev
      const sets = [...(prev.variant_sets ?? [])]
      if (!sets[index]) return prev
      sets[index] = {
        ...sets[index],
        ...patch,
        // Editing the SELECTION is an explicit choice of what to draw, so a
        // reopened plot's frozen version (which would override it,
        // variants.effective_selection) goes. Renaming the row keeps it.
        ...('selection' in patch ? { frozen: undefined } : {}),
      }
      return { ...prev, variant_sets: sets }
    })
  }, [])

  // Schema keys get their own section: "plot subject 01, trial 2" is a
  // different question from "narrow this factor", asked far more often, and the
  // keys are the same few every time. Everything else filterable — a struct's
  // fields, a joined group variable — lands in Filters. Variant factors are
  // excluded: selecting variants is the Variants section's job, and offering it
  // twice is how the two answers start disagreeing.
  const schemaKeys = useMemo(
    () => factors.filter(f => f.is_schema_key),
    [factors]
  )
  const otherFilterable = useMemo(
    () => factors.filter(f => !f.is_schema_key && !f.is_variant),
    [factors]
  )
  const schemaKeyNames = useMemo(() => schemaKeys.map(f => f.name), [schemaKeys])

  const locationSummary = useMemo(
    () => describeSelection(asSelection(spec?.location_filter)),
    [spec?.location_filter]
  )

  // Which variant the picker is describing. One row is a pin (the panel opens
  // on exactly one), so its selection IS the variant in view; with several rows
  // the figure is a comparison and no single variant is "the" one, so the
  // picker falls back to the source's default the same way the canvas does.
  const pickerSelection = useMemo(() => {
    const sets = spec?.variant_sets ?? []
    return sets.length === 1 ? (sets[0].selection ?? null) : null
  }, [spec?.variant_sets])
  // Membership from roles, order from `groups` — the same reconciliation the
  // backend does, so the control shows what the figure will draw.
  //
  // Derived from the SPEC rather than read off `capabilities.grouping.layers`,
  // for the reason the Variants rows are: the spec updates on the click and
  // the capability report is a debounced echo, so a checkbox reading the echo
  // would visibly lag the tick. The backend's copy is what the FIGURE uses;
  // this one only has to agree with it, and `ordered_groups` is the shared
  // definition both are written from.
  const groupLayers = useMemo(() => {
    // A source its combine replaced never groups — the backend collapses it
    // whatever the spec says — so it never shows as a layer either.
    const replaced = new Set(factors.filter(f => f.combined_into).map(f => f.name))
    const holders = Object.entries(spec?.roles ?? {})
      .filter(([name, role]) => role === 'group' && !replaced.has(name))
      .map(([name]) => name)
    return orderGroups(spec?.groups ?? [], holders, factorDepths)
  }, [spec?.roles, spec?.groups, factorDepths, factors])

  const groupableVariables = describe?.groupable_variables ?? []
  const [groupPickerOpen, setGroupPickerOpen] = useState(false)

  /** Apply the picker's selection: replace `factor_variables` wholesale.
   *
   *  Wholesale because the popup shows every grouping at once and opens with
   *  the current ones ticked — so what comes back IS the answer, unticking
   *  included. Merging would make unticking impossible.
   *
   *  Each grouping still arrives with a role (`roles.role_for_new_grouping`,
   *  published on the capability report): a grouping ticked is a request for
   *  one mark per level, so it joins the grouping list — or becomes a facet
   *  when the tick layers are full. Never left to default. */
  const applyGroupings = useCallback(
    (groups: { variable: string; column: string | null; variant: Record<string, unknown> }[]) => {
      setGroupPickerOpen(false)
      setSpec(prev => {
        if (!prev) return prev
        const names = new Set(groups.map(g => g.column ?? g.variable))
        const gone = (prev.factor_variables ?? [])
          .map(f => f.column ?? f.variable)
          .filter(name => !names.has(name))
        const fresh = (capsSpecRef.current === specKey && grouping?.new_grouping_role) || 'group'
        const roles = { ...prev.roles }
        for (const name of gone) delete roles[name]
        let layers = (prev.groups ?? []).filter(name => !gone.includes(name))
        const color = prev.color && gone.includes(prev.color) ? null : prev.color
        for (const group of groups) {
          const name = group.column ?? group.variable
          if (roles[name]) continue // a decision already made is never overwritten
          roles[name] = fresh
          if (fresh === 'group') layers = placeGroupLayer(layers, name, factorDepths)
        }
        return { ...prev, factor_variables: groups, roles, groups: layers, color }
      })
    },
    [factorDepths, grouping?.new_grouping_role, specKey]
  )
  // Only real factors can be bucketed — not a factor this spec already derived
  // (bucketing a bucket answers nothing) and not a variant axis.
  const derivedNames = useMemo(
    () => new Set((spec?.level_groups ?? []).map(g => g.name)),
    [spec?.level_groups]
  )
  const bucketable = useMemo(
    () =>
      factors.filter(
        f => !f.is_variant && !f.combined_from && !derivedNames.has(f.name)
      ),
    [factors, derivedNames]
  )
  // What the Grouping and Factors lists show: every factor but a source a
  // combine has replaced — that one is reached through the combine's name
  // dropdown, and the backend keeps it collapsed (`roles.complete_assignment`).
  const listedFactors = useMemo(
    () => factors.filter(f => !f.combined_into),
    [factors]
  )
  // Source levels per combine source, for the Combine section's editor and
  // summaries. A replaced source is still in `factors` with every level.
  const sourceLevels = useCallback(
    (source: string) => factors.find(f => f.name === source)?.levels ?? [],
    [factors]
  )

  // Rows come from the SPEC, annotations from the backend. The spec is the
  // source of truth and updates on the keystroke; `capabilities` is a debounced
  // echo, so rendering rows from it would make "+ Add variant" appear to do
  // nothing for a moment — and, worse, would let a row index mean different
  // things in the list and in the popup during that window.
  const variantRows = useMemo(
    () =>
      (spec?.variant_sets ?? []).map((set, index) => {
        const info = capabilities?.variants?.sets?.[index]
        return {
          explicitName: set.name ?? null,
          autoLabel: info?.auto_label ?? '(not set)',
          // Naming a variable is saying something, so such a row is defined
          // even with nothing selected: "also plot FilteredEMG, all of it".
          // A frozen version is a statement too (variants.says_something).
          defined:
            Object.keys(set.selection ?? {}).length > 0 ||
            Boolean(set.variable) ||
            Object.keys(set.frozen ?? {}).length > 0,
          rowCount: info?.row_count,
          spans: info?.spans ?? {},
          variable: set.variable ?? null,
          frozen: set.follow_latest ? {} : set.frozen ?? {},
          followLatest: Boolean(set.follow_latest),
        }
      }),
    [spec?.variant_sets, capabilities]
  )

  return {
    openCombine,
    setOpenCombine,
    addLevelGroup,
    editLevelGroup,
    renameLevelGroup,
    switchCombineSlot,
    removeLevelGroup,
    addVariantFromPicker,
    removeVariantSet,
    adoptCombination,
    editVariantSet,
    schemaKeys,
    otherFilterable,
    schemaKeyNames,
    locationSummary,
    pickerSelection,
    groupLayers,
    groupableVariables,
    groupPickerOpen,
    setGroupPickerOpen,
    applyGroupings,
    derivedNames,
    bucketable,
    listedFactors,
    sourceLevels,
    variantRows,
  }
}

export type CombinesState = ReturnType<typeof useCombines>
