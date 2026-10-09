"""
Copying whole tables between project databases (portability Stage 7).

The one owner of "move these tables to another database, exactly": each
table's DDL (from DuckDB's own catalog, so primary keys and defaults survive
-- a copied variable table must still accept ``INSERT ... ON CONFLICT``),
its rows as Parquet, and the views over them. Used by the bundle's history
and data sections and by the GUI section's verbatim mode.

Loading is for a NEW project: a table the target already has (scidb creates
its own tables on open, the GUI seeds the root pipeline) keeps the target's
DDL, is EMPTIED, and receives the columns both sides have -- so the result
is the exported rows, no more, no fewer. A column only the target has keeps
its default; a column only the bundle has is reported and dropped.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from scistacklog import Log

TABLES_FILE = "tables.json"


def _ddl(duck, name: str) -> "str | None":
    row = duck._fetchone(
        "SELECT sql FROM duckdb_tables() WHERE schema_name = 'main' AND table_name = ?",
        [name],
    )
    return row[0] if row else None


def _view_ddl(duck, name: str) -> "str | None":
    row = duck._fetchone(
        "SELECT sql FROM duckdb_views() WHERE schema_name = 'main' AND view_name = ?",
        [name],
    )
    return row[0] if row else None


def _columns(duck, name: str) -> list[str]:
    return [
        r[0]
        for r in duck._fetchall(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [name],
        )
    ]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def dump(duck, tables: list[str], views: "list[str] | None" = None) -> "dict[str, bytes]":
    """``{relpath: bytes}``: ``tables.json`` (DDL per table, view DDL, row
    counts) and ``<table>.parquet`` per table. A table that does not exist is
    skipped (an untouched store never created it)."""
    spec = {"tables": {}, "views": {}}
    files: dict[str, bytes] = {}
    with tempfile.TemporaryDirectory() as tmp:
        for name in tables:
            ddl = _ddl(duck, name)
            if ddl is None:
                continue
            path = Path(tmp) / f"{name}.parquet"
            duck._execute(f"COPY (SELECT * FROM {_q(name)}) TO '{path.as_posix()}' (FORMAT PARQUET)")
            files[f"{name}.parquet"] = path.read_bytes()
            count = duck._fetchone(f"SELECT count(*) FROM {_q(name)}")[0]
            spec["tables"][name] = {"ddl": ddl, "rows": count}
    for name in views or []:
        ddl = _view_ddl(duck, name)
        if ddl is not None:
            spec["views"][name] = ddl
    files[TABLES_FILE] = json.dumps(spec, indent=2).encode("utf-8")
    Log.info(
        f"[table_copy] dump: {len(spec['tables'])} table(s), "
        f"{sum(t['rows'] for t in spec['tables'].values())} row(s), "
        f"{len(spec['views'])} view(s)"
    )
    return files


def load(duck, files: "dict[str, bytes]") -> dict:
    """Recreate the dumped tables in *duck* (a NEW project's database).
    Returns ``{"tables": {name: rows}, "views": [...], "dropped_columns":
    {table: [...]}}``."""
    spec = json.loads(files[TABLES_FILE].decode("utf-8"))
    out = {"tables": {}, "views": [], "dropped_columns": {}}
    with tempfile.TemporaryDirectory() as tmp:
        for name, info in spec.get("tables", {}).items():
            if _ddl(duck, name) is None:
                duck._execute(info["ddl"])
            else:
                duck._execute(f"DELETE FROM {_q(name)}")
            path = Path(tmp) / f"{name}.parquet"
            path.write_bytes(files[f"{name}.parquet"])
            src = path.as_posix()
            # Top-level columns only (parquet_schema() would also list nested
            # list/struct fields, which can share a name with a real column).
            bundle_cols = [
                r[0] for r in duck._fetchall(f"DESCRIBE SELECT * FROM read_parquet('{src}')")
            ]
            target_cols = _columns(duck, name)
            shared = [c for c in bundle_cols if c in target_cols]
            dropped = [c for c in bundle_cols if c not in target_cols]
            if dropped:
                out["dropped_columns"][name] = dropped
            cols = ", ".join(_q(c) for c in shared)
            duck._execute(
                f"INSERT INTO {_q(name)} ({cols}) SELECT {cols} FROM read_parquet('{src}')"
            )
            out["tables"][name] = duck._fetchone(f"SELECT count(*) FROM {_q(name)}")[0]
    for name, ddl in spec.get("views", {}).items():
        duck._execute(ddl.replace("CREATE VIEW", "CREATE OR REPLACE VIEW", 1))
        out["views"].append(name)
    Log.info(
        f"[table_copy] load: {len(out['tables'])} table(s), "
        f"{sum(out['tables'].values())} row(s), {len(out['views'])} view(s); "
        f"dropped columns {out['dropped_columns'] or 'none'}"
    )
    return out
