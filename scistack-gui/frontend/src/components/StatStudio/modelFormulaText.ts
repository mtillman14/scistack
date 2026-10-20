/**
 * The Model formula popup's only text edit of its own: inserting a picked
 * covariate's name into the fixed part. The grammar is csv-stats' and every
 * check is the backend's (`stats_formula_check`); this only places a name the
 * backend gave (`offers.covariates`) where a person would type it.
 */

/** An R identifier; anything else is written in backticks. */
const PLAIN_NAME = /^[A-Za-z_.][A-Za-z0-9_.]*$/

/** The names already written in ``text``, bare or backticked. */
export function namesIn(text: string): Set<string> {
  const out = new Set<string>()
  for (const m of text.matchAll(/`([^`]*)`|[A-Za-z_.][A-Za-z0-9_.]*/g)) out.add(m[1] ?? m[0])
  return out
}

/** ``text`` with ``name`` crossed into the fixed part, before the first
 *  random term; unchanged when the name is already there. */
export function insertCovariate(text: string, name: string): string {
  if (namesIn(text).has(name)) return text
  const spelled = PLAIN_NAME.test(name) ? name : `\`${name}\``
  const firstRandom = text.search(/\s*\+\s*\(/)
  return firstRandom >= 0
    ? `${text.slice(0, firstRandom)} * ${spelled}${text.slice(firstRandom)}`
    : `${text} * ${spelled}`
}
