/**
 * Which build of the frontend this is, as vite stamped it (vite.config.ts,
 * `define.__SCISTACK_BUNDLE__`), and the one report of it to the backend.
 *
 * Without this nothing said which bundle a panel was running: on 2026-10-16
 * Plot Studio had no Copy PNG under scistack-gui 0.1.37 because VS Code ran a
 * hand-built 0.1.0 extension, and finding that took an evening. The webview
 * now reports itself once on load; the extension host adds who loaded it and
 * judges the version (extension/src/webviewBuild.ts), and Python writes the
 * line to scistack.log (scistack_gui/services/client_build.py).
 *
 * React-free so `npm test` can run it.
 */

export interface BundleBuild {
  /** extension/package.json's version at build time; "0.0.0" = unstamped dev build. */
  version: string
  /** Short git SHA, `-dirty` when the frontend tree had local edits. */
  commit: string
  /** ISO time the bundle was built. */
  built_at: string
  /** The vite target: "webview" (VS Code) or "standalone" (FastAPI). */
  target: string
}

declare const __SCISTACK_BUNDLE__: BundleBuild | undefined

/** Outside a vite build (node tests) the define is absent; say so, do not throw. */
export const BUNDLE_BUILD: BundleBuild =
  typeof __SCISTACK_BUNDLE__ !== 'undefined'
    ? __SCISTACK_BUNDLE__
    : { version: 'unknown', commit: 'unknown', built_at: 'unknown', target: 'unbuilt' }

/** The `report_client_build` params: the build plus which root is running. */
export function buildReportParams(
  view: 'plot' | 'pipeline',
  build: BundleBuild = BUNDLE_BUILD,
): Record<string, unknown> {
  return { ...build, view }
}

/**
 * Send the report. Never throws and never blocks the render: a server older
 * than the method answers "Method not found", which is itself worth a
 * console line and nothing more.
 */
export function reportBundleBuild(
  view: 'plot' | 'pipeline',
  call: (method: string, params: Record<string, unknown>) => Promise<unknown>,
): void {
  const params = buildReportParams(view)
  console.info('[SciStack] webview bundle', params)
  call('report_client_build', params).catch(err => {
    console.warn('[SciStack] report_client_build failed', err)
  })
}
