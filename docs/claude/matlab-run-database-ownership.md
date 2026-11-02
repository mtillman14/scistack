# Who owns the DuckDB file during a MATLAB run

MATLAB pipelines are the one place in SciStack where **two processes want
the same `.duckdb` file**. DuckDB allows exactly one writer process, so
ownership has to be handed back and forth explicitly. Getting this wrong
does not produce an error — it produced a *hang* (todos #5 and #13), which
is why it is worth writing down.

## The three ways MATLAB gets driven

| Tier | Who runs MATLAB | Where the decision is made |
| --- | --- | --- |
| 2 — MathWorks terminal | the user's own MATLAB session, via `matlabTerminal.ts` | `dagPanel.ts`, after Python says `host_execution_required` |
| 3 — sidecar | a `matlab -nodesktop` child of the Python server (`matlab_sidecar.py`) | `api/run.py`, in-process |
| 4 — clipboard | the user, by pasting | `dagPanel.ts`, last resort |

In **every** tier a MATLAB process opens the same database the GUI has, and
holds it for the whole run (`scihist.configure_database` keeps its
connection open).

## Rule 1 — the GUI's lock is not permanently held

`scistack_gui/runtime/db.py` refcounts the connection and closes it when the count
reaches zero. The JSON-RPC server acquires per request
(`server.py::_handle_request`) and releases after, so between requests the
file is free for MATLAB.

**Browser/standalone mode does not do this.** FastAPI has no equivalent
acquire/release, so the connection stays open for the life of the process.
That is why sidecar runs must hand the file over explicitly:

```python
with external_db_access("MATLAB"):
    success = _drive_sidecar(matlab_sidecar, command, emit)
```

`external_db_access` closes our connection, marks the database externally
owned, and restores it on exit. Both sidecar paths (single node and whole
pipeline) go through it, via the shared `_drive_sidecar` helper.

## Rule 2 — don't take the lock back mid-run

While `_external_holder` is set, `acquire_db_connection` refuses
immediately with a **non-retryable** `DatabaseLockedError`. Winning that
race would be worse than losing it: reopening would break the MATLAB run
we just dispatched. A request that arrives mid-run gets a truthful "MATLAB
has the database" instead.

A conflict we did *not* create (MATLAB opened it on its own) is retryable
— `acquire_db_connection` backs off for `ACQUIRE_RETRY_TIMEOUT` (5 s),
which absorbs MATLAB's short single-write locks, then reports.

`DatabaseLockedError` is classified from DuckDB's message text
(`"Conflicting lock is held in … (PID n)"`), not the exception type —
DuckDB reuses plain `IOException` for unrelated I/O failures, and those
must not be retried or blamed on MATLAB.

## Rule 3 — a request must always get a response

`_handle_request` runs on a per-request thread and the extension's
`pythonProcess.request()` holds a promise until a frame with the matching
id arrives. **An exception escaping `_handle_request` hangs the GUI
permanently** — no error, no log the user would look at, nothing.

This is exactly what happened: `acquire_db_connection()` was called
*outside* the `try`, so a MATLAB-held database killed the thread silently.
`start_run` → `generate_matlab_command` never returned, and the GUI froze
with `handleMatlabRun: requesting generate_matlab_command` as its last log
line.

The dispatcher now: acquires inside the `try`, releases only if the acquire
succeeded, maps `DatabaseLockedError` to error code `-32010`, and has an
outer net that answers *anything* that escapes — with a `responded` flag so
it can never emit a second frame for an id already answered. The client has
a matching backstop (`scistack.rpcTimeoutMs`, default 5 min).

## Rule 4 — don't refresh into a database MATLAB owns

`extension.ts`'s DB file-watcher fires on `.duckdb.wal` changes, and MATLAB
writes the WAL *throughout* a run, not just at the end. Refreshing then
means graph RPCs against a database we cannot open — repeatedly.

`MatlabRunTracker` (vscode-free, unit-tested) tracks which run_ids are in
flight. The watcher calls `noteDbChange()`, which returns false while
MATLAB owns the database and remembers that a refresh is due; one refresh
is replayed via `takeDeferredRefresh()` when the last run ends. Sidecar
runs clear through the Python `run_done` notification; terminal/clipboard
runs clear inside `DagPanel` (they emit no notification), which is why the
"all finished" callback exists rather than doing this in the notification
handler.

## Where the MATLAB/Python decision lives

`api/run.py::route_matlab_single_run` — the backend, asking
`matlab_registry.is_matlab_function`. It used to live only in
`dagPanel.ts`, keyed on a `language` field the webview happened to send;
browser clients never passed through that host, so a MATLAB node clicked in
a browser fell through to the Python registry and failed with
`"Function '…' not found in registry"`. The frontend still sends
`language`, but only as a hint for pre-attaching the debugger.

The whole-pipeline equivalent is `start_pipeline_run` +
`pipeline_has_matlab_steps`; both use the same
`host_can_dispatch_matlab` → `host_execution_required` handshake, and both
emit `run_output`/`run_done` on the caller's own `run_id` so the frontend
never needs to know which tier served it.
