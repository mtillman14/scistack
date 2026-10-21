/**
 * PathInput key patterns — what a `{key}` placeholder matches on disk
 * (`scifor.PathInput(key_regex=)`), e.g. `subject = 'R2_\d{3}'` so a subject
 * holding the `_` delimiter is captured whole, or `speed = '[A-Za-z]+'` to
 * split an adjacent `{speed}{trial}`. Shape, exactly as the backend serves and
 * accepts it: `{key: pattern}`. An aliased key also matches its declared
 * spellings, then canonicalizes (scifor `_key_pattern`).
 *
 * Pure edits only. VALIDATION is not here: a pattern that does not compile or
 * has a capturing group is refused by scifor's `key_regex_pattern_error` when
 * the declaration is written, and the panel shows that refusal — one owner
 * for the rule (CLAUDE.md NOTE 4), as with pathInputAliases.ts.
 */

export type PathInputKeyRegex = Record<string, string>

/** *keyRegex* with *key* set to *pattern* (replacing any previous one).
 * Never mutates the input. */
export function withPattern(
  keyRegex: PathInputKeyRegex,
  key: string,
  pattern: string,
): PathInputKeyRegex {
  return { ...(keyRegex ?? {}), [key.trim()]: pattern }
}

/** *keyRegex* without *key*, so removing the last pattern writes a plain
 * template again. */
export function withoutPattern(keyRegex: PathInputKeyRegex, key: string): PathInputKeyRegex {
  const next = { ...(keyRegex ?? {}) }
  delete next[key]
  return next
}
