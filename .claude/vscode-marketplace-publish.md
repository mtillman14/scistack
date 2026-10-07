# Plan: publish scistack-gui to the VS Code Marketplace, version-locked to PyPI

## Principle
The git tag `vX.Y.Z` is the ONE owner of the version. hatch-vcs already reads
it for every Python package; the extension becomes a second consumer of the
same tag. No hand-maintained version string anywhere.

## Changes

### 1. Version stamping (one owner = the tag)
- `extension/package.json`: `"version": "0.0.0"` placeholder, never edited.
- New `extension/scripts/stamp-version.mjs`: reads the version from
  `GITHUB_REF_NAME` (or `git describe --tags --exact-match`), strips `v`,
  refuses anything not `X.Y.Z` (marketplace has no `.devN`/`rcN`), writes it
  into package.json in the CI workspace only (never committed).
- Unit test (`node --test`) for the tag -> version parsing and the refusal.

### 2. Runtime handshake (catches drift on users' machines)
- Server reports `scistack_gui.__version__` (importlib.metadata) in its
  ready message; extension compares with its own `package.json` version.
- On mismatch: warning notification naming both versions plus the
  `pip install scistack-gui==X.Y.Z` fix; logged to the SciStack output channel.
  Extension version `0.0.0` (dev build) skips the check.
- Tests: TS unit test for the compare function; pytest that the ready
  payload carries the version.

### 3. Marketplace-ready manifest
- package.json: `repository`, `license: MIT`, `icon` (128x128 PNG),
  `homepage`, `bugs`, keywords; upgrade `@vscode/vsce` 2.x -> 3.x.
- Add `LICENSE` (MIT) — repo currently has none; vsce prompts without it.
- `.vscodeignore`: exclude `dist/test/**`, `*.map`, `build-on-windows.ps1`,
  `to_run_on_linux_from_windows.md`, `tsconfig*.json`, `.DS_Store`, `.gitignore`.
- README: install section (`pip install scistack-gui` matching the version).

### 4. CI
- `ci.yml`: new `extension` job: `npm ci`, `npm test`, `vsce package` (smoke).
- `publish.yml`: new `publish-vscode` job after the PyPI job: stamp version,
  `npm run build:all`, `vsce package`, `vsce publish --packagePath`, optional
  `ovsx publish` (Open VSX), attach .vsix to the GitHub release.
  Secret: `VSCE_PAT` (and `OVSX_PAT` if Open VSX).

## User-side (manual, one-time)
1. Create a publisher at https://marketplace.visualstudio.com/manage
   (Microsoft account). ID must match package.json `publisher`.
2. Create Azure DevOps PAT (Marketplace: Manage, All accessible orgs) ->
   GitHub secret `VSCE_PAT` in the `pypi`-style environment.
3. First publish can be manual: `npx vsce login <publisher>` then
   `npx vsce publish --packagePath scistack-gui-X.Y.Z.vsix`.
