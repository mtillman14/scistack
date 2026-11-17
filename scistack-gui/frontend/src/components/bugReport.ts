/**
 * The "🐞 Report bug" button's link (issue #20): a prefilled GitHub new-issue
 * URL. No backend and no token — the user lands on GitHub's form, signed in,
 * and clicks Submit themselves.
 *
 * This module is the one owner of the repository's issue URL and of what the
 * prefilled body says. The extension host only checks that what it is asked
 * to open is a GitHub https link (extension/src/openExternal.ts).
 *
 * No `template=` parameter: the repository has no issue templates, and an
 * issue *form* (bug.yml) ignores `body=`, which would drop what the user typed.
 *
 * React-free so `npm test` can run it.
 */

import type { BundleBuild } from '../bundleBuild.js'

export const NEW_ISSUE_URL = 'https://github.com/mtillman14/scistack/issues/new'

/**
 * GitHub answers 414 above roughly 8 KB of URL. The body is cut to keep the
 * whole link under this, with a note saying it was cut, so a long paste never
 * turns into a broken page.
 */
export const MAX_URL_LENGTH = 7500

export const TRUNCATION_NOTE =
  '\n\n_(Report shortened to fit in a link — please paste the rest here.)_'

/** The environment footer: which build the user was running. */
export function environmentSection(build: BundleBuild): string {
  return [
    '',
    '---',
    '**Environment** (filled in by the SciStack GUI)',
    `- GUI version: ${build.version}`,
    `- Commit: ${build.commit}`,
    `- Built: ${build.built_at}`,
    `- Target: ${build.target}`,
  ].join('\n')
}

/**
 * GitHub's limit for a file attached to an issue (non-image files, 25 MB).
 * Above it the reporter is told to share the project file another way.
 */
export const GITHUB_ATTACHMENT_LIMIT_BYTES = 25 * 1024 * 1024

/**
 * The bug report's project file name, from the export dialog's default
 * (`<database>.scistack`, owned by the backend's get_export_options) and the
 * attachable suffix (`.scistack.zip`): `<database>-bug-report.scistack.zip`.
 */
export function bugReportFileName(defaultName: string, extension: string): string {
  const stem = defaultName.endsWith(extension) ? defaultName.slice(0, -extension.length) : defaultName
  return `${stem}-bug-report${extension}.zip`
}

/** The last path component, for either separator (the path comes from the OS). */
export function fileNameOf(path: string): string {
  return path.split(/[\\/]/).pop() ?? path
}

/** The line that tells the reporter, in GitHub's editor, where the file goes. */
export function attachmentSection(fileName: string): string {
  return [
    '',
    `**Project file:** _drag \`${fileName}\` into this box to attach it_`,
  ].join('\n')
}

function issueUrl(title: string, body: string): string {
  const q = new URLSearchParams({ title, body, labels: 'bug' })
  return `${NEW_ISSUE_URL}?${q.toString()}`
}

export interface BugReportLink {
  url: string
  /** True when the user's text was shortened to fit the URL limit. */
  truncated: boolean
}

/**
 * The new-issue link for `title` + `body`, with the environment footer and,
 * when a project file was saved, the line saying where to drop it.
 * The title is never cut (GitHub limits it to 256 chars itself); the user's
 * body is cut from the end until the link fits.
 */
export function bugReportLink(
  title: string,
  body: string,
  build: BundleBuild,
  attachmentName?: string,
): BugReportLink {
  const footer = (attachmentName ? attachmentSection(attachmentName) + '\n' : '') + environmentSection(build)
  const full = issueUrl(title.trim(), `${body.trim()}\n${footer}`)
  if (full.length <= MAX_URL_LENGTH) return { url: full, truncated: false }

  // Binary search for the longest prefix of the body that fits: encoded
  // length is not proportional to character count (é, emoji, newlines).
  const text = body.trim()
  let lo = 0
  let hi = text.length
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2)
    const candidate = issueUrl(title.trim(), `${text.slice(0, mid)}${TRUNCATION_NOTE}\n${footer}`)
    if (candidate.length <= MAX_URL_LENGTH) lo = mid
    else hi = mid - 1
  }
  return {
    url: issueUrl(title.trim(), `${text.slice(0, lo)}${TRUNCATION_NOTE}\n${footer}`),
    truncated: true,
  }
}
