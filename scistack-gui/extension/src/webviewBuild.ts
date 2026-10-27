/**
 * What a webview reports about the bundle it is running, and what the host
 * adds before the report reaches scistack.log.
 *
 * Nothing used to say which frontend a panel was running. The extension
 * reports its own version and the server reports its own, but the bundle is
 * a file the extension merely points at (`<extensionPath>/dist/webview/`),
 * so a development copy of the extension, or a `dist/` nobody rebuilt, served
 * an old frontend with no symptom but a missing feature (2026-10-16: no Copy
 * PNG under scistack-gui 0.1.37, caused by a hand-built 0.1.0 extension).
 *
 * The webview sends `report_client_build` once on load
 * (`frontend/src/bundleBuild.ts`). The panel calls {@link prepareBuildReport}
 * and forwards the RESULT to Python, so the one scistack.log line carries the
 * bundle, the extension that loaded it, and the verdict. The verdict is
 * decided here, by `versionCheck.ts` — Python logs it, never re-decides it.
 *
 * Kept free of `vscode` so it runs under `npm test`.
 */

import { checkWebviewVersion, VersionVerdict } from './versionCheck';

/** The extension that loaded the bundle — what only the host knows. */
export interface ExtensionIdentity {
  version: string;
  /** `context.extensionPath`: the folder `dist/webview/` was loaded from. */
  path: string;
  /** `vscode.ExtensionMode` as a word: Production / Development / Test. */
  mode: string;
}

export interface BuildReport {
  /** The params to forward to Python's `report_client_build`. */
  params: Record<string, unknown>;
  /** One line for the SciStack output channel. */
  line: string;
  verdict: VersionVerdict;
}

function text(value: unknown, fallback: string): string {
  return typeof value === 'string' && value ? value : fallback;
}

export function prepareBuildReport(
  raw: Record<string, unknown> | undefined,
  extension: ExtensionIdentity,
  serverVersion: string | null | undefined,
): BuildReport {
  const bundle = raw ?? {};
  const bundleVersion = typeof bundle.version === 'string' ? bundle.version : null;
  const verdict = checkWebviewVersion(bundleVersion, serverVersion);
  const line =
    `[webview] ${text(bundle.view, 'unknown')} view: bundle ${bundleVersion ?? '(not reported)'} ` +
    `(commit ${text(bundle.commit, 'unknown')}, built ${text(bundle.built_at, 'unknown')}) ` +
    `from ${extension.path} [extension ${extension.version}, ${extension.mode}]; ` +
    `scistack-gui ${serverVersion ?? '(not reported)'} — ${verdict.kind}` +
    (verdict.kind === 'skipped' ? ` (${verdict.reason})` : '');
  return {
    params: {
      ...bundle,
      extension_version: extension.version,
      extension_path: extension.path,
      extension_mode: extension.mode,
      verdict: verdict.kind,
      verdict_detail:
        verdict.kind === 'mismatch' ? verdict.message
          : verdict.kind === 'skipped' ? verdict.reason
            : '',
    },
    line,
    verdict,
  };
}
