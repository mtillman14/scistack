"""
The ``plots`` section of a project bundle (``scidb.bundle``): saved plots and
plot presets, copied row for row.

Both stores key nothing by canvas ids or database record ids -- a saved plot
is named per VARIABLE, a preset per project -- so a verbatim copy into the
new project is the whole story: ids, versions and hidden rows kept, exactly
as the exporting project had them (``VersionedStore.dump_rows`` /
``load_rows``, the one owner of the table's columns).
"""

from __future__ import annotations

import json

from scistacklog import Log

from . import presets, saved

LAYER = "scistackplotdb"

_STORES = {"saved_plots.json": saved.STORE, "presets.json": presets.STORE}


class PlotsSection:
    name = "plots"

    def export(self, ctx) -> "dict[str, bytes]":
        files: dict[str, bytes] = {}
        for filename, store in _STORES.items():
            rows = store.dump_rows(ctx.db)
            if rows:
                files[filename] = json.dumps(rows, indent=2, default=str).encode("utf-8")
        Log.info("[bundle_section] plots export: %s", sorted(files), layer=LAYER)
        return files

    def import_(self, ctx, files: "dict[str, bytes]") -> dict:
        report = {}
        for filename, store in _STORES.items():
            if filename in files:
                rows = json.loads(files[filename].decode("utf-8"))
                report[store.table] = store.load_rows(ctx.db, rows)
        Log.info("[bundle_section] plots import: %s", report, layer=LAYER)
        return report
