/**
 * Presentation helpers for the Variants (topologies) panel.
 *
 * React-free on purpose, so `npm test` can execute the rules (see
 * `tsconfig.test.json` — the include list is explicit and a file added there
 * must reach neither React nor the DOM).
 *
 * **Nothing here decides anything about variants.** The verdict, the location
 * sample and the chronology all arrive already decided from
 * `services/variants_service.py`, which imports them from `scidb.inspect`
 * (CLAUDE.md NOTE 3). What lives here is strictly how they are WORDED and
 * ORDERED on screen — the part that has no terminal equivalent to disagree
 * with.
 */

/** The closed verdict vocabulary — `scidb.inspect.api.VERDICT_*`. */
export type Verdict = 'current' | 'partially_superseded' | 'superseded'

export interface LocationSample {
  total: number
  keys: string[]
  sample: Record<string, string | number>[]
}

export interface VariantRow {
  function_name: string
  call_id: string
  output_type: string
  output_num: number | null
  input_types: Record<string, string>
  constants: Record<string, string>
  record_count: number
  current: boolean
  current_record_count: number
  run_options: string | null
  function_hash: string | null
  first_saved: string | null
  last_saved: string | null
  verdict: Verdict
  verdict_label: string
  locations: LocationSample
}

export interface TopologyGroup {
  function_name: string
  input_types: [string, string][]
  output_type: string
  variants: VariantRow[]
}

export interface TopologiesReply {
  variable: string
  command: string
  topology_count: number
  variant_count: number
  topologies: TopologyGroup[]
}

/** `loadGaitRiteOneFile(gaitRitePath: …, gaitRiteConfig: …) → GAITRiteLoaded` */
export function topologyHeading(topology: TopologyGroup): string {
  const inputs =
    topology.input_types.map(([param, type]) => `${param}: ${type}`).join(', ') ||
    '(no inputs)'
  return `${topology.function_name}(${inputs}) → ${topology.output_type}`
}

/** `gaitRiteConfig={…}`, or `no constants` — the line that names the variant. */
export function constantsLabel(variant: VariantRow): string {
  const entries = Object.entries(variant.constants ?? {})
  if (entries.length === 0) return 'no constants'
  return entries
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join(', ')
}

/**
 * `subject/session/speed — SS01/BL/SSV, SS01/BL/FV, … (+417)`.
 *
 * A count is not a place: "450 records" and "450 locations over
 * subject × session × speed × trial" answer different questions, which is
 * why the keys are named and not just counted.
 */
export function locationsLabel(locations: LocationSample): string {
  if (!locations || locations.total === 0) return 'no locations'
  const shown = locations.sample ?? []
  if (shown.length === 0) return `${locations.total} location(s)`
  const keys = (locations.keys ?? []).join('/')
  const values = shown
    .map(combo => Object.values(combo).map(v => String(v)).join('/'))
    .join(', ')
  const more = locations.total - shown.length
  return `${keys} — ${values}${more > 0 ? `, … (+${more})` : ''}`
}

/**
 * Whether the `load:` verdict means "a run will not read this".
 *
 * `superseded` and `partially_superseded` are BOTH true here and must still
 * read differently on screen: partial is not a rounding of the other two —
 * run options are judged per function, globally, so a variant can lose some
 * locations and keep others, and that case is the trial-4 orphan the whole
 * investigation started from.
 */
export function isNotCurrent(verdict: Verdict): boolean {
  return verdict !== 'current'
}

/** The `scidb` command that reproduces what is on screen. */
export function variantsCommand(variable: string, allLocations = false): string {
  return `scidb variants ${variable}${allLocations ? ' --locations' : ''}`
}

/**
 * A one-line summary for the panel header: how much is here and how much of
 * it a load would skip. Written so the ALARMING number is the one a reader
 * lands on — two variants that look equally alive is the shape of the bug
 * this panel exists for.
 */
export function topologiesSummary(reply: TopologiesReply): string {
  const stale = reply.topologies
    .flatMap(t => t.variants)
    .filter(v => isNotCurrent(v.verdict)).length
  const head = `${reply.topology_count} topology/ies, ${reply.variant_count} variant(s)`
  if (stale === 0) return `${head} — all current`
  return `${head} — ${stale} a load would not fully return`
}
