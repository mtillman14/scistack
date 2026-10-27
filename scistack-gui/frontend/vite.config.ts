import { execSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// What this bundle is, stamped in at build time and reported by the running
// webview (src/bundleBuild.ts → report_client_build → scistack.log). The version
// is extension/package.json's — the field CI stamps from the release tag
// before building, so it CONSUMES the tag rather than restating it ("0.0.0"
// = a local, unstamped build). build-on-windows.ps1 builds a copy in %TEMP%
// with neither the sibling extension/ nor .git, so it passes both in the
// SCISTACK_BUNDLE_* env vars; anything unreadable is "unknown", never fatal.
function bundleVersion(): string {
  if (process.env.SCISTACK_BUNDLE_VERSION) return process.env.SCISTACK_BUNDLE_VERSION
  try {
    const pkg = fileURLToPath(new URL('../extension/package.json', import.meta.url))
    return JSON.parse(readFileSync(pkg, 'utf8')).version ?? 'unknown'
  } catch {
    return 'unknown'
  }
}

function bundleCommit(): string {
  if (process.env.SCISTACK_BUNDLE_COMMIT) return process.env.SCISTACK_BUNDLE_COMMIT
  try {
    const sha = execSync('git rev-parse --short HEAD', { stdio: ['ignore', 'pipe', 'ignore'] })
      .toString().trim()
    const dirty = execSync('git status --porcelain -- .', { stdio: ['ignore', 'pipe', 'ignore'] })
      .toString().trim()
    return dirty ? `${sha}-dirty` : sha
  } catch {
    return 'unknown'
  }
}

// Build target is controlled by the VITE_BUILD_TARGET env var:
//   "standalone" (default) → ../scistack_gui/static/ (served by FastAPI)
//   "webview"              → ../extension/dist/webview/ (loaded by VS Code Webview)
const target = process.env.VITE_BUILD_TARGET ?? 'standalone'

const buildConfigs = {
  standalone: {
    outDir: '../scistack_gui/static',
    emptyOutDir: true,
  },
  webview: {
    outDir: '../extension/dist/webview',
    emptyOutDir: true,
    // Webview must be a single bundle (no code splitting) because VS Code
    // Webviews can only load scripts with a nonce from the extension's directory.
    rollupOptions: {
      output: {
        entryFileNames: 'index.js',
        chunkFileNames: 'index.js',
        assetFileNames: 'index.[ext]',
        // Disable code splitting — everything in one chunk
        manualChunks: undefined,
      },
    },
  },
}

// During development, the Vite dev server runs on port 5173 and the FastAPI
// backend runs on port 8765. The proxy below forwards any request starting
// with /api to the backend, so the frontend code can just call "/api/pipeline"
// without worrying about the different port.
export default defineConfig({
  plugins: [react()],
  define: {
    __SCISTACK_BUNDLE__: JSON.stringify({
      version: bundleVersion(),
      commit: bundleCommit(),
      built_at: new Date().toISOString(),
      target,
    }),
  },
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8765',
    },
  },
  build: buildConfigs[target as keyof typeof buildConfigs],
})
