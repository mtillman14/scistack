import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// During development, the Vite dev server runs on port 5173 and the FastAPI
// backend runs on port 8765. The proxy below forwards any request starting
// with /api to the backend, so the frontend code can just call "/api/pipeline"
// without worrying about the different port.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': 'http://localhost:8765',
    },
  },
  // In production, Vite builds static files into ../scistack_gui/static/
  // which FastAPI then serves directly.
  build: {
    outDir: '../scistack_gui/static',
    emptyOutDir: true,
  },
})
