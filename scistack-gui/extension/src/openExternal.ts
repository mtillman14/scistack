/**
 * The host's guard for the webview's `open_external` request (issue #20, the
 * bug reporter). A webview cannot open a browser tab; the host can, through
 * vscode.env.openExternal — so the host must not open whatever it is handed.
 * Only https links to github.com pass; the frontend owns which GitHub page
 * (frontend/src/components/bugReport.ts).
 *
 * vscode-free so `npm test` can run it.
 */

export function isAllowedExternalUrl(raw: unknown): raw is string {
  if (typeof raw !== 'string') return false;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return false;
  }
  return url.protocol === 'https:' && url.hostname === 'github.com' && url.username === '' && url.password === '';
}
