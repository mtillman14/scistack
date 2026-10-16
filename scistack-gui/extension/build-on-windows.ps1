# Build the scistack-gui extension on Windows local disk, then copy the
# compiled output back to the original location.
#
# This works around esbuild failing with EIO errors when building directly
# on a network mount. The resulting extension.js is platform-independent,
# so building on Windows and running on Linux (via Remote-SSH) is fine.
#
# Usage (from this script's directory):
#   powershell -ExecutionPolicy Bypass -File .\build-on-windows.ps1

$ErrorActionPreference = "Stop"

# Source = the directory this script lives in (extension/).
$SourceDir = $PSScriptRoot
if (-not $SourceDir) { $SourceDir = (Get-Location).Path }

# The frontend/ sibling directory.
$FrontendSourceDir = Join-Path (Split-Path $SourceDir -Parent) "frontend"

# Build in local %TEMP% to avoid the network mount.
$BuildDir = Join-Path $env:TEMP "scistack-ext-build"
$FrontendBuildDir = Join-Path $env:TEMP "scistack-frontend-build"

Write-Host "Extension source: $SourceDir"
Write-Host "Frontend source:  $FrontendSourceDir"
Write-Host "Build dir:        $BuildDir"
Write-Host ""

# ---- clean the local build dirs -----------------------------------------------
foreach ($dir in @($BuildDir, $FrontendBuildDir)) {
    if (Test-Path $dir) {
        Write-Host "Removing old build directory: $dir"
        Remove-Item -Recurse -Force $dir
    }
}

# ---- copy extension source to local disk (skip node_modules + dist) -----------
Write-Host "Copying extension source to local disk..."
New-Item -ItemType Directory -Path $BuildDir | Out-Null

# robocopy is the fastest reliable way on Windows; /XD excludes directories.
robocopy $SourceDir $BuildDir /E /XD node_modules dist /NFL /NDL /NJH /NJS /NP | Out-Null
# robocopy exit codes 0-7 are success; 8+ are failure.
if ($LASTEXITCODE -ge 8) { throw "robocopy (extension) failed with exit code $LASTEXITCODE" }

# ---- copy frontend source to local disk (skip node_modules + dist) ------------
Write-Host "Copying frontend source to local disk..."
New-Item -ItemType Directory -Path $FrontendBuildDir | Out-Null

robocopy $FrontendSourceDir $FrontendBuildDir /E /XD node_modules dist /NFL /NDL /NJH /NJS /NP | Out-Null
if ($LASTEXITCODE -ge 8) { throw "robocopy (frontend) failed with exit code $LASTEXITCODE" }

# ---- build the extension (esbuild) -------------------------------------------
Push-Location $BuildDir
try {
    Write-Host ""
    Write-Host "Running npm install (extension)..."
    npm install
    if ($LASTEXITCODE -ne 0) { throw "npm install (extension) failed" }

    Write-Host ""
    Write-Host "Running npm run build (extension)..."
    npm run build
    if ($LASTEXITCODE -ne 0) { throw "npm run build (extension) failed" }
} finally {
    Pop-Location
}

# ---- build the frontend webview (vite) ----------------------------------------
Push-Location $FrontendBuildDir
try {
    Write-Host ""
    Write-Host "Running npm install (frontend)..."
    npm install
    if ($LASTEXITCODE -ne 0) { throw "npm install (frontend) failed" }

    Write-Host ""
    Write-Host "Building webview bundle..."
    $env:VITE_BUILD_TARGET = "webview"
    # The %TEMP% copy has no .git and no sibling extension/, so hand vite the
    # bundle stamp it would otherwise read itself (frontend/vite.config.ts).
    $env:SCISTACK_BUNDLE_VERSION = (Get-Content (Join-Path $SourceDir "package.json") -Raw | ConvertFrom-Json).version
    try {
        $sha = (git -C $SourceDir rev-parse --short HEAD 2>$null)
        $dirty = (git -C $FrontendSourceDir status --porcelain -- . 2>$null)
        if ($sha) { $env:SCISTACK_BUNDLE_COMMIT = if ($dirty) { "$sha-dirty" } else { "$sha" } }
    } catch { }
    Write-Host "Bundle stamp: version $env:SCISTACK_BUNDLE_VERSION, commit $env:SCISTACK_BUNDLE_COMMIT"
    # Override outDir to write into the extension build's dist/webview.
    $WebviewOutDir = Join-Path (Join-Path $BuildDir "dist") "webview"
    npx vite build --outDir $WebviewOutDir --emptyOutDir
    if ($LASTEXITCODE -ne 0) { throw "vite build (webview) failed" }
} finally {
    $env:VITE_BUILD_TARGET = $null
    $env:SCISTACK_BUNDLE_VERSION = $null
    $env:SCISTACK_BUNDLE_COMMIT = $null
    Pop-Location
}

# ---- copy dist/ back to the network mount ------------------------------------
$SourceDist = Join-Path $SourceDir "dist"
$BuiltDist  = Join-Path $BuildDir  "dist"

if (-not (Test-Path $SourceDist)) {
    New-Item -ItemType Directory -Path $SourceDist | Out-Null
}

Write-Host ""
Write-Host "Copying dist/ back to source..."

# Extension host JS
Copy-Item -Force (Join-Path $BuiltDist "extension.js") (Join-Path $SourceDist "extension.js")

$MapFile = Join-Path $BuiltDist "extension.js.map"
if (Test-Path $MapFile) {
    Copy-Item -Force $MapFile (Join-Path $SourceDist "extension.js.map")
}

# Webview bundle
$WebviewBuilt  = Join-Path $BuiltDist "webview"
$WebviewTarget = Join-Path $SourceDist "webview"
if (Test-Path $WebviewBuilt) {
    if (-not (Test-Path $WebviewTarget)) {
        New-Item -ItemType Directory -Path $WebviewTarget | Out-Null
    }
    Copy-Item -Force -Recurse (Join-Path $WebviewBuilt "*") $WebviewTarget
    Write-Host "  Copied webview bundle to dist/webview/"
}

Write-Host ""
Write-Host "Done. Reload the VS Code window to pick up changes."
