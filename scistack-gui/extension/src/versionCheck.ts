/**
 * Does the installed `scistack-gui` Python package match this extension?
 *
 * Both are released from one git tag (`vX.Y.Z`): hatch-vcs stamps the Python
 * package, `scripts/stamp-version.js` stamps `package.json`. But users update
 * them separately — the Marketplace auto-updates the extension, pip does not
 * touch the venv — so a mismatch is the expected failure, and its symptoms
 * (unknown RPC methods, missing fields) say nothing about versions.
 *
 * Kept free of `vscode` so it runs under `npm test`.
 */

/**
 * "Dev build" on both sides: package.json's committed placeholder (only CI
 * stamps a real version), and the Python packages' `__version__` when the
 * source tree was never installed.
 */
export const DEV_VERSION = '0.0.0';

export type VersionVerdict =
  | { kind: 'match' }
  /** A dev build of either side — no claim to check. */
  | { kind: 'skipped'; reason: string }
  | { kind: 'mismatch'; message: string };

/**
 * The ONE rule for "are these two halves from the same release": a client
 * (the extension, or the webview bundle it loaded) against the Python server.
 * The callers only word the warning; neither restates when to warn.
 */
type ReleaseComparison =
  | { kind: 'match' }
  | { kind: 'skipped'; reason: string }
  | { kind: 'server-unreported' }
  | { kind: 'differ' };

function compareRelease(
  clientLabel: string,
  clientVersion: string,
  serverVersion: string | null | undefined,
): ReleaseComparison {
  if (clientVersion === DEV_VERSION) {
    return { kind: 'skipped', reason: `${clientLabel} is an unstamped dev build` };
  }
  // A server older than the version handshake — itself a mismatch.
  if (!serverVersion) return { kind: 'server-unreported' };
  if (serverVersion === DEV_VERSION || /\.dev\d|\+/.test(serverVersion)) {
    // hatch-vcs's spelling for a checkout past the last tag (editable install).
    return { kind: 'skipped', reason: `scistack-gui ${serverVersion} is a dev build` };
  }
  return serverVersion === clientVersion ? { kind: 'match' } : { kind: 'differ' };
}

export function checkServerVersion(
  extensionVersion: string,
  serverVersion: string | null | undefined,
): VersionVerdict {
  const cmp = compareRelease('extension', extensionVersion, serverVersion);
  if (cmp.kind === 'server-unreported') {
    return {
      kind: 'mismatch',
      message:
        `SciStack extension ${extensionVersion} is talking to a scistack-gui Python ` +
        `package that did not report its version (older than this extension). ` +
        `Run: pip install scistack-gui==${extensionVersion}`,
    };
  }
  if (cmp.kind === 'differ') {
    return {
      kind: 'mismatch',
      message:
        `SciStack extension ${extensionVersion} does not match the installed ` +
        `scistack-gui Python package ${serverVersion}. Things may break. ` +
        `Run: pip install scistack-gui==${extensionVersion}`,
    };
  }
  return cmp;
}

/**
 * Is the webview bundle a panel actually loaded from the same release as the
 * Python server? Separate from {@link checkServerVersion} because the bundle
 * is a file on disk the extension merely points at: a development copy of the
 * extension, or a `dist/webview/` nobody rebuilt, serves an old frontend under
 * a current server (2026-10-16: Plot Studio had no Copy PNG button under
 * scistack-gui 0.1.37 because VS Code ran a hand-built 0.1.0 copy).
 *
 * `bundleVersion` null/undefined = a bundle built before it stamped itself;
 * such a bundle never sends the report, so this only sees a malformed one.
 */
export function checkWebviewVersion(
  bundleVersion: string | null | undefined,
  serverVersion: string | null | undefined,
): VersionVerdict {
  if (!bundleVersion) {
    return { kind: 'skipped', reason: 'webview bundle did not report a version' };
  }
  const cmp = compareRelease('webview bundle', bundleVersion, serverVersion);
  if (cmp.kind === 'server-unreported' || cmp.kind === 'differ') {
    return {
      kind: 'mismatch',
      message:
        `The SciStack view is running a webview bundle from release ${bundleVersion}, ` +
        `but the scistack-gui Python package is ${serverVersion ?? '(older than the version handshake)'}. ` +
        `Check "Developer: Show Running Extensions" for a second SciStack copy, ` +
        `or rebuild both frontend bundles in a dev checkout.`,
    };
  }
  return cmp;
}
