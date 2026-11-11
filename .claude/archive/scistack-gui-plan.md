# SciStack GUI Plan

## Overview

A cross-platform desktop-style GUI for SciStack, built as a local web app:
- **FastAPI** backend (Python) — wraps all SciStack Python APIs, manages DB connection, hosts Jupyter kernel
- **React + React Flow** frontend — pipeline DAG, code drawer, schema filter
- Launched via CLI: `scistack-gui experiment.duckdb`
- Browser opens automatically; Ctrl+C to stop

---

## Source of Truth Principle

**The `.py` and `.duckdb` files are always the single source of truth** for pipeline structure, data, and computation. The GUI is a read/execute/visualize layer — it never writes Python code or modifies the database schema on the user's behalf. The only GUI-owned state is node positioning (`layout.json`). This means:
- The DAG is derived from the DB, not defined in the GUI
- Running a pipeline step executes real Python (`for_each`) against the real DB
- If the user changes their `.py` file and restarts the GUI, the DAG reflects the new state automatically

---

## Phases

### Phase 1 — MVP (current focus)
- Read the DB and render the pipeline DAG (variable + function nodes, variant listboxes)
- Persistent node positioning
- Select variants, set schema filter, run a pipeline step via `POST /run`
- Jupyter code drawer for ad-hoc execution
- stdout streaming via WebSocket

### Phase 2 — Sub-analyses (future)
See below.

### Phase 3 — Direct DAG editing (future)
Allow users to define new variable types and `for_each` calls from within the GUI. This is a significant scope expansion and must not compromise the source-of-truth principle — any GUI-authored pipeline steps would need to be written back to a `.py` file, not stored only in the GUI.

---

---

## Package Structure

Everything lives under `./scistack-gui/` in the monorepo root.

```
scistack-gui/
├── scistack_gui/
│   ├── __main__.py          # CLI entry point: parse arg, start uvicorn, open browser
│   ├── app.py               # FastAPI app factory
│   ├── db.py                # DB connection singleton (shared by API + kernel)
│   ├── kernel.py            # Jupyter kernel lifecycle manager
│   ├── layout.py            # Node position persistence (read/write layout.json)
│   └── api/
│       ├── pipeline.py      # GET /pipeline, GET /variants
│       ├── schema.py        # GET /schema/keys, GET /schema/values
│       ├── run.py           # POST /run (triggers for_each in background thread)
│       ├── layout.py        # GET /layout, PUT /layout/{node_id}
│       └── ws.py            # WebSocket: kernel I/O + DAG refresh events
├── frontend/
│   ├── src/
│   │   ├── App.tsx
│   │   ├── components/
│   │   │   ├── DAG/
│   │   │   │   ├── PipelineDAG.tsx     # React Flow canvas
│   │   │   │   ├── VariableNode.tsx    # Node with scrollable listbox of variants
│   │   │   │   └── FunctionNode.tsx    # Node with Run button
│   │   │   └── CodeDrawer.tsx          # Jupyter code panel (slides up from bottom)
│   │   └── hooks/
│   │       ├── usePipeline.ts          # Fetches + refreshes DAG data
│   │       └── useKernel.ts            # WebSocket connection to kernel
│   └── package.json
└── pyproject.toml
```

---

## Backend

### Startup (`__main__.py`)
1. Parse `experiment.duckdb` from CLI arg
2. Initialize DB connection via `configure_database()`
3. Start Jupyter kernel, inject `db` into kernel namespace
4. Start uvicorn on `localhost:8765`
5. `webbrowser.open("http://localhost:8765")`

### REST Endpoints

| Method | Path | Returns |
|---|---|---|
| GET | `/pipeline` | Nodes + edges for React Flow: variable types, function nodes, connections |
| GET | `/variants` | `db.list_pipeline_variants()` |
| GET | `/schema/keys` | `db.dataset_schema_keys` |
| GET | `/schema/values/{key}` | `db.distinct_schema_values(key)` |
| POST | `/run` | Trigger `for_each(fn, inputs, outputs, **schema_filter)` in background thread |

### WebSocket (`/ws`)
Single connection for two event types:
- **Kernel I/O:** stream stdout/stderr/results from Jupyter kernel to frontend
- **DAG refresh:** after `/run` completes (or `for_each` runs in a cell), push a `dag_updated` event so the frontend re-fetches `/pipeline`

### Jupyter Kernel (`kernel.py`)
- Start an `InProcessKernelClient` (or `KernelManager` for isolation) on app startup
- Pre-populate namespace: `db`, all variable classes registered in the DB, `for_each`, `Fixed`
- Expose `execute(code) -> stream of output messages` called via WebSocket
- Kernel shares the same DuckDB connection as the FastAPI backend (same `db` object)

---

## Frontend

### DAG

Two custom React Flow node types:

**VariableNode**
- Label: variable type name (e.g. `FilteredEMG`)
- Scrollable listbox below the label showing variants: e.g. `☑ low_hz=20`, `☑ low_hz=30`
- Constant-type variable nodes additionally show `[+ new]` at the bottom of the listbox
- Variants come from `branch_params` via `/variants` endpoint
- Checked state drives what the downstream Run will execute

**FunctionNode**
- Label: function name
- `[▶ Run]` button — fires `POST /run` with the currently selected variants from connected input nodes
- Shows a spinner while run is in progress

Edges: variable node → function node → variable node (bipartite graph)

### Node Position Persistence

Node positions are **stable** — once placed, a node never moves unless the user drags it. This is essential so users can build a spatial mental map of their pipeline.

**Mechanism:**
- Positions stored in `layout.json` alongside the `.duckdb` file (e.g. `experiment.layout.json`)
- `GET /layout` — returns `{node_id: {x, y}}` map for all known nodes
- `PUT /layout/{node_id}` — called on React Flow's `onNodeDragStop`, persists the new position immediately
- On DAG refresh (`dag_updated`): new nodes not present in `layout.json` get initial positions via dagre (factoring in existing node positions to avoid overlap); all existing nodes restore their saved positions
- The frontend never lets React Flow auto-layout the full graph after the first load

### Code Drawer

- Slides up from the bottom of the screen (like browser DevTools)
- When opened from a function node, the DAG shrinks to show only that function + its immediate inputs and outputs (the rest fades/collapses)
- Contains one or more code cells; each cell sends code to the Jupyter kernel via WebSocket and streams output below
- Kernel namespace always has `db` in scope, so `RawEMG.load(...)`, `for_each(...)` etc. work directly

### Global Schema Filter Bar

- Sits at the top of the DAG view
- Dropdowns for each schema key: `subject: [all ▾]`, `session: [all ▾]`
- Default: "all missing" — run only schema combos not yet computed
- Selection is passed as part of `POST /run` payload

---

## Data Flow: Run Action

1. User selects variants in listboxes on input nodes
2. User sets schema filter in top bar
3. User clicks `[▶ Run]` on a function node
4. Frontend sends `POST /run` with `{function, selected_variants, schema_filter}`
5. Backend reconstructs `for_each(fn, inputs={...}, outputs=[...], **schema_filter)` call and runs in a background thread
6. stdout from `for_each` streams to frontend via WebSocket
7. On completion, backend emits `dag_updated` via WebSocket
8. Frontend re-fetches `/pipeline` and `/variants`, DAG updates

---

## Installation

```
pip install scistack-gui
scistack-gui experiment.duckdb
```

Frontend is pre-built and bundled as static files served by FastAPI (`StaticFiles`). No Node.js required at runtime.

---

## Build / Dev Workflow

- Backend: `uvicorn scistack_gui.app:app --reload`
- Frontend: `cd frontend && npm run dev` (Vite dev server proxies API calls to backend)
- Production: `npm run build` → output copied to `scistack_gui/static/` → served by FastAPI

---

## Phase 2 — Sub-analyses (Future)

Scientific pipelines are rarely linear in practice. Three-quarters of the way through a pipeline, a methodological question arises, a new metric seems worth testing, or a hypothesis emerges about a variable that isn't central to the main analysis. Today these side inquiries live in scattered one-off scripts that are hard to organize, hard to revisit, and easy to lose.

**The goal:** make sub-analyses first-class citizens — easy to spin off from any point in the main pipeline, clearly scoped, and easy to return to.

**Concept:**
- Any variable node in the DAG can be the root of a sub-analysis
- A sub-analysis is a mini-pipeline that branches off from a main-pipeline variable, exists in its own named scope, and doesn't pollute the main pipeline's namespace
- Sub-analyses appear visually distinct in the DAG (e.g. a collapsible sub-graph, or a separate tab/lane with a link back to its branch point)
- The code drawer is the natural authoring surface: open it at a variable node, write a few cells, and "promote" that cell sequence to a named sub-analysis
- Sub-analyses are still backed by `.py` files and the same `.duckdb` — the source-of-truth principle holds

**Why this matters:** Most pipeline tools are designed for one clean linear (or branching) pipeline. But real scientific work is messier — it's full of "let me just quickly check..." moments that deserve to be organized, not buried. Supporting sub-analyses positions SciStack as a tool for the full scientific thought process, not just the final polished pipeline.

---

## Key Dependencies

| Package | Purpose |
|---|---|
| `fastapi` + `uvicorn` | Backend server |
| `jupyter_client` | Jupyter kernel management |
| `react-flow` | DAG canvas |
| `dagre` | Initial placement of new nodes only |
| `vite` | Frontend build tool |
| `typescript` | Frontend language |
