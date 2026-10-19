"""The ``stats`` section of a project bundle: saved stats tables and stats
presets, copied row for row (``VersionedStore.dump_rows`` / ``load_rows``),
like scistackplotdb's ``plots`` section. A schema-key rename is applied with
the same exact-string remap the plots section uses."""

from __future__ import annotations

import json

from scistacklog import Log
from scistackplotdb.bundle_section import _remap_row

from .saved import PRESET_STORE, STORE

LAYER = "scistackstatsdb"

_STORES = {"saved_stats.json": STORE, "stats_presets.json": PRESET_STORE}


class StatsSection:
    name = "stats"

    def export(self, ctx) -> "dict[str, bytes]":
        files: dict[str, bytes] = {}
        for filename, store in _STORES.items():
            rows = store.dump_rows(ctx.db)
            if rows:
                files[filename] = json.dumps(rows, indent=2, default=str).encode("utf-8")
        Log.info("[bundle_section] stats export: %s", sorted(files), layer=LAYER)
        return files

    def import_(self, ctx, files: "dict[str, bytes]") -> dict:
        report = {}
        key_map = getattr(ctx, "key_map", None)
        for filename, store in _STORES.items():
            if filename in files:
                rows = json.loads(files[filename].decode("utf-8"))
                if key_map is not None and not key_map.is_identity:
                    rows = [_remap_row(row, store, key_map, ctx.map_report) for row in rows]
                report[store.table] = store.load_rows(ctx.db, rows)
        Log.info("[bundle_section] stats import: %s", report, layer=LAYER)
        return report
