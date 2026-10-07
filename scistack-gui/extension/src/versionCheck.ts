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

export function checkServerVersion(
  extensionVersion: string,
  serverVersion: string | null | undefined,
): VersionVerdict {
  if (extensionVersion === DEV_VERSION) {
    return { kind: 'skipped', reason: 'extension is an unstamped dev build' };
  }
  if (!serverVersion) {
    // A server older than this handshake — itself a mismatch.
    return {
      kind: 'mismatch',
      message:
        `SciStack extension ${extensionVersion} is talking to a scistack-gui Python ` +
        `package that did not report its version (older than this extension). ` +
        `Run: pip install scistack-gui==${extensionVersion}`,
    };
  }
  if (serverVersion === DEV_VERSION || /\.dev\d|\+/.test(serverVersion)) {
    // hatch-vcs's spelling for a checkout past the last tag (editable install).
    return { kind: 'skipped', reason: `scistack-gui ${serverVersion} is a dev build` };
  }
  if (serverVersion === extensionVersion) return { kind: 'match' };
  return {
    kind: 'mismatch',
    message:
      `SciStack extension ${extensionVersion} does not match the installed ` +
      `scistack-gui Python package ${serverVersion}. Things may break. ` +
      `Run: pip install scistack-gui==${extensionVersion}`,
  };
}
