# Release process: one tag → PyPI + VS Code Marketplace + Open VSX

## The one rule

**The git tag `vX.Y.Z` is the only owner of the version.** No file in the repo
contains a release version string. Everything that needs one reads it from the
tag:

| Consumer | How it gets the version |
| --- | --- |
| Every Python package (13 of them) | `hatch-vcs` (`[tool.hatch.version] source = "vcs"`) reads the tag at build time |
| VS Code extension | `extension/scripts/stamp-version.js` writes the tag's `X.Y.Z` into `package.json` **in CI only** |
| Running server | `scistack_gui.server.package_version()` → `importlib.metadata.version("scistack-gui")` |

`extension/package.json` is committed as `"version": "0.0.0"`, a placeholder
that means "dev build". `scripts/stamp-version.test.js` asserts it, so a real
version committed there (which would be a second owner of the version) fails `npm test`.

Known exception not yet fixed: `scidb/src/scidb/__init__.py` hardcodes
`__version__ = "0.1.0"`, which does not follow the tag.

## Cutting a release

1. Make sure `main` is green and committed. If the standalone frontend changed,
   rebuild and commit `scistack_gui/static/` first. `publish.yml` does not
   rebuild it. The extension's webview bundle *is* rebuilt in CI.
2. `git tag v0.1.30 && git push origin v0.1.30`.
3. Nothing else. `publish.yml` runs:

```
tag push
  └─ ci            (ci.yml via workflow_call: pytest matrix, build, extension)
      └─ publish   (PyPI: build 13 packages, refuse .dev/rc/+local, trusted publishing)
          └─ publish-vscode
               npm ci (extension + frontend)
               npm run stamp-version     ← GITHUB_REF_NAME → package.json
               npm run build:all         ← esbuild bundle + vite "webview" target
               vsce package → scistack-gui-X.Y.Z.vsix
               vsce publish --packagePath   (VSCE_PAT)
               ovsx publish                 (OVSX_PAT)
               upload .vsix as a workflow artifact
```

`publish-vscode` has `needs: publish` **on purpose**. The extension tells
users to `pip install scistack-gui==X.Y.Z`, so that version must already be on
PyPI before the extension can auto-update anyone.

### Tags that are refused

`stamp-version.js` accepts only `^v\d+\.\d+\.\d+$`. The Marketplace has no
pre-release version spelling (pre-releases use a separate `--pre-release` flag
and still need a plain `X.Y.Z`). The PyPI job independently refuses
`.dev`/`rc`/`+local` artifacts, so a bad tag fails both sides.

### Partial failure

If PyPI succeeds and the Marketplace step fails (an expired token, for
example), re-run only the `publish-vscode` job from the Actions UI. It
re-stamps from the same tag. Do not push a new tag just to retry the
extension: the new tag would also publish new versions of all 13 Python
packages.

## Secrets and accounts

| Where | What |
| --- | --- |
| GitHub environment `pypi` | none; uses PyPI trusted publishing (OIDC). Each PyPI project lists `publish.yml` as a trusted publisher |
| GitHub environment `vscode-marketplace` | `VSCE_PAT`: Azure DevOps token, Organization = *All accessible organizations*, scope *Marketplace → Manage*. `OVSX_PAT`: open-vsx.org access token |
| Marketplace | publisher id `scistack` (display name "SciStack"), managed at marketplace.visualstudio.com/manage |
| Open VSX | namespace `scistack`, created once with `npx ovsx create-namespace scistack -p <token>` |

Azure DevOps tokens expire, and expiry is the most likely cause of a red
`publish-vscode` job.

## Runtime version handshake

A release keeps the published versions equal. It cannot keep a *user's*
install equal: the Marketplace auto-updates the extension, and pip never
updates their venv. So on every server start:

1. `server._send_ready(params)` is the **only** builder of the `ready`
   notification, used by both the database server and the plot-only server.
   It adds `"version": package_version()` and logs `[startup] scistack-gui version X`.
   `tests/test_server_ready_version.py` asserts that the literal
   `"method": "ready"` appears once in `server.py`, so a second hand-built
   frame cannot come back.
2. `extension/src/session.ts` `reportVersionMismatch` reads
   `context.extension.packageJSON.version` and calls
   `versionCheck.checkServerVersion` (no `vscode` import, so it is unit-tested under `npm test`).
3. The verdict is one of:
   - `match`: both versions are equal.
   - `skipped`: the extension is `0.0.0` (local build), or the server version
     contains `.devN` or `+` (an editable install past the last tag). Dev
     setups never see a warning.
   - `mismatch`: the versions differ, or the server reported no version (a
     server older than this handshake). This shows one warning per extension
     host with `pip install scistack-gui==<extension version>`.
4. Every start logs
   `Versions: extension A, scistack-gui B — <verdict>` to the SciStack Output
   Channel. Check this line first when a user reports "unknown method" or
   missing-field errors.

The fix the warning suggests always pins the Python package to the extension's
version, not the other way round. The user cannot pick an extension version as
easily as a pip version.

## What ships in the .vsix

`.vscodeignore` whitelists by exclusion. The package should contain exactly
`package.json`, `README.md`, `LICENSE`, `dist/extension.js`, and
`dist/webview/{index.js,index.css,index.html}`. Verify with:

```
cd scistack-gui/extension && npx vsce ls
```

The CI `extension` job runs `vsce package` on every PR, so a manifest or
ignore-file problem fails the PR rather than the release. `README.md` is the
Marketplace listing page. Its Install section is the user-facing statement of
the version-matching rule.

## Local / manual publish (fallback)

```
cd scistack-gui/extension
npm ci && npm ci --prefix ../frontend
node scripts/stamp-version.js v0.1.30      # DO NOT commit the resulting package.json
npm run build:all
npx vsce package
npx vsce publish --packagePath scistack-gui-0.1.30.vsix   # prompts for / uses VSCE_PAT
npx ovsx publish scistack-gui-0.1.30.vsix -p <OVSX_TOKEN>
git checkout package.json                  # back to 0.0.0
```

## Related

- `gui-extension-startup-path.md`: interpreter resolution, readiness, and startup diagnostics.
- `gui-vscode-extension.md`: the original extension architecture plan.
- `.github/workflows/publish.yml`, `.github/workflows/ci.yml`.
