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
        key_map = getattr(ctx, "key_map", None)
        for filename, store in _STORES.items():
            if filename in files:
                rows = json.loads(files[filename].decode("utf-8"))
                if key_map is not None and not key_map.is_identity:
                    rows = [_remap_row(row, store, key_map, ctx.map_report) for row in rows]
                report[store.table] = store.load_rows(ctx.db, rows)
        Log.info("[bundle_section] plots import: %s", report, layer=LAYER)
        return report


def _remap_row(row: dict, store, key_map, map_report) -> dict:
    """A saved plot or preset carried into another schema (portability Stage
    6). A spec names factors in many fields (roles, groups, facets, colours,
    filters, difference bars, ...), so every string EQUAL to an exporter key
    is renamed (``KeyMap.exact_strings``) and the item is flagged for review
    -- a factor that merely equals a key name cannot be told apart."""
    envelope = json.loads(row["envelope_json"])
    where = f"{store.noun} {row.get('name')!r}"
    mapped = key_map.exact_strings(envelope, where, map_report)
    return {**row, "envelope_json": json.dumps(mapped)}
