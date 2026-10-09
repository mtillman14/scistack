"""
Did your re-run reproduce the exporter's results? (portability Stage 9;
``.claude/plan-portability.md``; ``docs/claude/portability.md`` I17.)

``verify`` COMPARES -- it never runs code (user decision 2026-10-08): you run
the pipeline as usual on your own copy of the raw files, then compare your
live history with the exporter's.

**The exporter's side** is either the newest archived history under
``.scistack/archive/`` (an import without data, or into another schema), or
a ``.scistack`` bundle file (``against=``). It is loaded from its Parquet
files into a separate in-memory DuckDB, never into the live database.

**Matching -- the structural key.** Records line up by WHAT produced them,
not by their content: a variable record's key is (type, schema_version,
location, the key of its producing call); a call's key is (function name,
function hash, run options, its bindings), and a binding names its input by
the input's own key (a constant or PathInput record by its id, which is
content- and name-addressed already). So when an upstream value differs,
the records below it still find their counterparts, and only their content
differs. The exporter's locations are renamed through the import's key map
(``.scistack/import.json``); a location naming a key the map dropped is
"not comparable".

**Classes** for every exporter variable record (not excluded):

* ``reproduced`` -- same key, same content hash;
* ``within_tolerance`` -- same key, different hash, but both sides' data
  rows agree within ``rtol``/``atol`` (only when the exporter's DATA is
  available: a bundle exported with data);
* ``differs_at_source`` -- different content, every variable input matched;
* ``differs_downstream`` -- different content below a difference (the report
  names the first divergences upstream);
* ``code_changed`` -- the key matches except for the function's hash;
* ``not_run`` -- no counterpart in your history;
* ``not_comparable`` -- its location names a key your schema dropped.

Your records with no exporter counterpart are counted as ``new``. When a side
has several records for one key, the newest (``created_at``) is compared.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

from scistacklog import Log

CLASSES = (
    "reproduced", "within_tolerance", "differs_at_source", "differs_downstream",
    "code_changed", "not_run", "not_comparable",
)
DEFAULT_RTOL = 1e-6
DEFAULT_ATOL = 1e-12
#: How many entries each list in the report keeps (counts are always complete).
REPORT_LIMIT = 50

HISTORY_TABLES = ("_record", "_invocation", "_invocation_input", "_invocation_output", "_schema")


class VerifyError(ValueError):
    """Nothing to verify against, or the exporter's side cannot be read."""


# ---------------------------------------------------------------------------
# The two sides
# ---------------------------------------------------------------------------


class _Side:
    """A source of rows: ``table(sql, params) -> (columns, rows)``."""

    def __init__(self, label: str, table: Callable, has: Callable[[str], bool]):
        self.label = label
        self.table = table
        self.has = has


def _live_side(db) -> _Side:
    duck = db._duck

    def table(sql, params=None):
        cols, rows = duck._fetch_table(sql, params) if params else duck._fetch_table(sql)
        return list(cols), [tuple(r) for r in rows]

    def has(name):
        return bool(duck._fetchall(
            "SELECT 1 FROM duckdb_tables() WHERE schema_name = 'main' AND table_name = ?", [name]
        ))

    return _Side("yours", table, has)


def _memory_side(label: str, sections: "list[dict[str, bytes]]", tmp: Path) -> _Side:
    """Load ``table_copy`` dumps (``tables.json`` + Parquet) into an
    in-memory DuckDB of their own."""
    import duckdb

    con = duckdb.connect()
    loaded: set[str] = set()
    for i, files in enumerate(sections):
        folder = tmp / f"s{i}"
        folder.mkdir(parents=True, exist_ok=True)
        spec = json.loads(files.get("tables.json", b"{}"))
        for name in (spec.get("tables") or {}):
            blob = files.get(f"{name}.parquet")
            if blob is None:
                continue
            path = folder / f"{name}.parquet"
            path.write_bytes(blob)
            con.execute(f'CREATE TABLE "{name}" AS SELECT * FROM read_parquet(\'{path.as_posix()}\')')
            loaded.add(name)

    def table(sql, params=None):
        cur = con.execute(sql, params or [])
        return [d[0] for d in cur.description], [tuple(r) for r in cur.fetchall()]

    return _Side(label, table, lambda name: name in loaded)


def _archive_sections(folder: Path) -> "dict[str, bytes]":
    return {p.name: p.read_bytes() for p in folder.iterdir() if p.is_file()}


def _exporter_side(root: Path, against: "Path | None", tmp: Path) -> "tuple[_Side, str, bool]":
    """``(side, description, has_data)``."""
    from .bundle import ARCHIVE_DIR, DATA_SECTION, HISTORY_SECTION, read_bundle

    if against is None:
        archives = sorted((root / ARCHIVE_DIR).glob("*/tables.json")) if (root / ARCHIVE_DIR).is_dir() else []
        if not archives:
            raise VerifyError(
                f"no archived history under {root / ARCHIVE_DIR}. An import with data loads the "
                "exporter's history LIVE, so there is nothing to compare in place: import the "
                "bundle into a fresh project with --no-history, run the pipeline, then verify "
                "--against the bundle file."
            )
        folder = archives[-1].parent
        return _memory_side("exporter", [_archive_sections(folder)], tmp), str(folder), False
    against = Path(against)
    if against.is_dir():
        if not (against / "tables.json").is_file():
            raise VerifyError(f"{against} is not an archived history (no tables.json)")
        return _memory_side("exporter", [_archive_sections(against)], tmp), str(against), False
    bundle = read_bundle(against)
    history = bundle.sections.get(HISTORY_SECTION)
    if history is None:
        raise VerifyError(f"{against} carries no history to verify against")
    data = bundle.sections.get(DATA_SECTION)
    side = _memory_side("exporter", [history] + ([data] if data else []), tmp)
    return side, str(against), data is not None


# ---------------------------------------------------------------------------
# The graph and its structural keys
# ---------------------------------------------------------------------------


@dataclass
class _Rec:
    rid: str
    type: str
    schema_version: Any
    schema_id: Any
    content_hash: str
    excluded: bool
    created_at: Any


class _Graph:
    def __init__(self, side: _Side, key_map: "dict[str, str | None] | None" = None):
        self.side = side
        self.key_map = key_map
        t0 = time.monotonic()
        _, rows = side.table(
            "SELECT record_id, type, schema_version, schema_id, content_hash, excluded, created_at FROM _record"
        )
        self.records = {r[0]: _Rec(*r[:5], bool(r[5]), r[6]) for r in rows}
        _, rows = side.table(
            "SELECT invocation_id, function_name, function_hash, as_table, distribute, across_variants "
            "FROM _invocation"
        )
        self.invocations = {r[0]: r[1:] for r in rows}
        self.inputs: dict[str, list] = {}
        for inv, param, rid, selector in side.table(
            "SELECT invocation_id, param_name, input_record_id, selector FROM _invocation_input"
        )[1]:
            self.inputs.setdefault(inv, []).append((param, rid, selector or ""))
        self.producers: dict[str, list] = {}
        for inv, num, rid in side.table(
            "SELECT invocation_id, output_num, output_record_id FROM _invocation_output"
        )[1]:
            self.producers.setdefault(rid, []).append((inv, num))
        cols, rows = side.table("SELECT * FROM _schema")
        keys = [c for c in cols if c not in ("schema_id", "schema_level")]
        self.locations: dict[Any, "tuple | None"] = {}
        for r in rows:
            row = dict(zip(cols, r))
            items, comparable = [], True
            for k in keys:
                v = row.get(k)
                if v is None:
                    continue
                name = k
                if key_map is not None and k in key_map:
                    name = key_map[k]
                    if name is None:
                        comparable = False
                        break
                items.append((name, str(v)))
            self.locations[row["schema_id"]] = tuple(sorted(items)) if comparable else None
        self._memo: dict[tuple, "str | None"] = {}
        Log.info(
            f"[verify] {side.label}: {len(self.records)} record(s), {len(self.invocations)} call(s) "
            f"loaded in {time.monotonic() - t0:.2f}s"
        )

    def is_variable(self, rec: _Rec) -> bool:
        return not str(rec.type).startswith("__")

    def key(self, rid: str, strict: bool = True) -> "str | None":
        """The structural key of *rid*, or ``None`` when not comparable."""
        memo = (rid, strict)
        if memo in self._memo:
            return self._memo[memo]
        rec = self.records.get(rid)
        if rec is None:
            out = f"missing:{rid}"
        elif not self.is_variable(rec):
            out = f"{rec.type}:{rid}"  # constants / PathInput specs are content/name addressed
        else:
            location = self.locations.get(rec.schema_id, ()) if rec.schema_id is not None else ()
            if location is None:
                out = None
            else:
                producers = []
                comparable = True
                for inv, num in self.producers.get(rid, []):
                    pk = self._call_key(inv, strict)
                    if pk is None:
                        comparable = False
                        break
                    producers.append((pk, num))
                out = None if not comparable else _digest(
                    (rec.type, rec.schema_version, location, min(producers) if producers else None)
                )
        self._memo[memo] = out
        return out

    def _call_key(self, inv: str, strict: bool) -> "str | None":
        fn, fn_hash, as_table, distribute, across = self.invocations.get(inv, (None,) * 5)
        bindings = []
        for param, rid, selector in self.inputs.get(inv, []):
            k = self.key(rid, strict)
            if k is None:
                return None
            bindings.append((param, selector, k))
        return _digest((fn, fn_hash if strict else None, _norm(as_table), bool(distribute),
                        _norm(across), tuple(sorted(bindings))))

    def function_of(self, rid: str) -> "str | None":
        for inv, _ in self.producers.get(rid, []):
            return self.invocations.get(inv, (None,))[0]
        return None

    def variable_inputs(self, rid: str) -> list[str]:
        out = []
        for inv, _ in self.producers.get(rid, []):
            for _, input_rid, _ in self.inputs.get(inv, []):
                rec = self.records.get(input_rid)
                if rec is not None and self.is_variable(rec):
                    out.append(input_rid)
        return out

    def location_dict(self, rid: str) -> dict:
        rec = self.records[rid]
        loc = self.locations.get(rec.schema_id, ()) if rec.schema_id is not None else ()
        return dict(loc or ())


def _norm(v):
    if v is None:
        return ()
    if isinstance(v, (list, tuple)):
        return tuple(sorted(str(x) for x in v))
    return (str(v),)


def _digest(value) -> str:
    return hashlib.sha256(repr(value).encode("utf-8")).hexdigest()[:20]


def _newest_by_key(graph: _Graph) -> "tuple[dict[str, str], dict[str, int], int]":
    """``({key: newest rid}, {key: older count}, not_comparable count)`` over
    the variable records that are not excluded."""
    best: dict[str, str] = {}
    older: dict[str, int] = {}
    not_comparable = 0
    for rid, rec in graph.records.items():
        if not graph.is_variable(rec) or rec.excluded:
            continue
        k = graph.key(rid)
        if k is None:
            not_comparable += 1
            continue
        if k in best:
            older[k] = older.get(k, 0) + 1
            if str(rec.created_at) > str(graph.records[best[k]].created_at):
                best[k] = rid
        else:
            best[k] = rid
    return best, older, not_comparable


# ---------------------------------------------------------------------------
# Data within tolerance
# ---------------------------------------------------------------------------


def _data_rows(side: _Side, type_name: str, rid: str) -> "tuple[list[str], list[tuple]] | None":
    if not side.has("_registered_types"):
        return None
    _, rows = side.table("SELECT table_name FROM _registered_types WHERE type_name = ?", [type_name])
    if not rows or not side.has(rows[0][0]):
        return None
    table = rows[0][0]
    cols, data = side.table(f'SELECT * FROM "{table}" WHERE record_id = ? ORDER BY rowid', [rid])
    keep = [i for i, c in enumerate(cols) if c != "record_id"]
    return [cols[i] for i in keep], [tuple(r[i] for i in keep) for r in data]


def _close(a, b, rtol: float, atol: float) -> "tuple[bool, float]":
    """``(equal within tolerance, the largest absolute difference seen)``."""
    import math

    import numpy as np

    if a is None or b is None:
        return a is None and b is None, 0.0
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b, 0.0
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        if math.isnan(float(a)) and math.isnan(float(b)):
            return True, 0.0
        return math.isclose(float(a), float(b), rel_tol=rtol, abs_tol=atol), abs(float(a) - float(b))
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        try:
            x, y = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
        except (TypeError, ValueError):
            return list(a) == list(b), 0.0
        if x.shape != y.shape:
            return False, float("inf")
        diff = float(np.nanmax(np.abs(x - y))) if x.size else 0.0
        return bool(np.allclose(x, y, rtol=rtol, atol=atol, equal_nan=True)), diff
    return a == b, 0.0


def _within_tolerance(exp: _Side, live: _Side, type_name: str, exp_rid: str, live_rid: str,
                      rtol: float, atol: float) -> "tuple[bool | None, float]":
    """``(True/False, max abs diff)``, or ``(None, 0)`` when a side has no
    data rows to compare."""
    a, b = _data_rows(exp, type_name, exp_rid), _data_rows(live, type_name, live_rid)
    if not a or not b or not a[1] or not b[1]:
        return None, 0.0
    if a[0] != b[0] or len(a[1]) != len(b[1]):
        return False, float("inf")
    worst = 0.0
    for ra, rb in zip(a[1], b[1]):
        for va, vb in zip(ra, rb):
            ok, d = _close(va, vb, rtol, atol)
            worst = max(worst, d)
            if not ok:
                return False, worst
    return True, worst


# ---------------------------------------------------------------------------
# The comparison
# ---------------------------------------------------------------------------


@dataclass
class VerifyReport:
    against: str
    data: bool
    rtol: float
    atol: float
    counts: dict = field(default_factory=dict)
    new: int = 0
    by_function: dict = field(default_factory=dict)
    first_divergences: list = field(default_factory=list)
    code_changed: list = field(default_factory=list)
    not_run: list = field(default_factory=list)
    duplicates: int = 0
    seconds: float = 0.0

    @property
    def ok(self) -> bool:
        bad = sum(v for k, v in self.counts.items() if k not in ("reproduced", "within_tolerance"))
        return bad == 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["ok"] = self.ok
        return d


def _key_map(root: Path) -> "dict[str, str | None] | None":
    from .bundle import IMPORT_RECORD
    from .environment import STATE_DIR

    path = root / STATE_DIR / IMPORT_RECORD
    if not path.is_file():
        return None
    km = json.loads(path.read_text(encoding="utf-8")).get("key_map") or {}
    return None if all(k == v for k, v in km.items()) else km


def verify(db, *, root: "Path | str | None" = None, against: "Path | str | None" = None,
           rtol: float = DEFAULT_RTOL, atol: float = DEFAULT_ATOL) -> VerifyReport:
    """Compare *db*'s history with the exporter's. See the module docstring."""
    from scifor.pathinput import project_root

    t0 = time.monotonic()
    root = Path(root) if root is not None else project_root()
    with tempfile.TemporaryDirectory() as tmp:
        exp_side, described, has_data = _exporter_side(root, Path(against) if against else None, Path(tmp))
        exp = _Graph(exp_side, _key_map(root))
        live = _Graph(_live_side(db))
        exp_best, exp_older, not_comparable = _newest_by_key(exp)
        live_best, live_older, _ = _newest_by_key(live)
        live_loose = {live.key(rid, strict=False) for rid in live_best.values()}

        report = VerifyReport(against=described, data=has_data, rtol=rtol, atol=atol)
        report.duplicates = sum(exp_older.values()) + sum(live_older.values())
        cls: dict[str, str] = {}
        detail: dict[str, dict] = {}
        for k, rid in exp_best.items():
            rec = exp.records[rid]
            other = live_best.get(k)
            if other is None:
                cls[rid] = "code_changed" if exp.key(rid, strict=False) in live_loose else "not_run"
                continue
            theirs = live.records[other]
            if theirs.content_hash == rec.content_hash:
                cls[rid] = "reproduced"
                continue
            within, worst = (None, 0.0)
            if has_data:
                within, worst = _within_tolerance(exp_side, live.side, rec.type, rid, other, rtol, atol)
            detail[rid] = {"recipient_hash": theirs.content_hash, "max_abs_diff": worst,
                           "compared_values": within is not None}
            cls[rid] = "within_tolerance" if within else "differs"

        for rid, c in list(cls.items()):
            if c != "differs":
                continue
            upstream = [i for i in exp.variable_inputs(rid) if cls.get(i) not in (None, "reproduced", "within_tolerance")]
            cls[rid] = "differs_downstream" if upstream else "differs_at_source"

        counts = {c: 0 for c in CLASSES}
        for rid, c in cls.items():
            counts[c] += 1
            rec = exp.records[rid]
            fn = exp.function_of(rid) or "?"
            per = report.by_function.setdefault(f"{fn} -> {rec.type}", {})
            per[c] = per.get(c, 0) + 1
            entry = {"type": rec.type, "function": fn, "location": exp.location_dict(rid),
                     "exporter_hash": rec.content_hash, **detail.get(rid, {})}
            if c == "differs_at_source" and len(report.first_divergences) < REPORT_LIMIT:
                report.first_divergences.append(entry)
            elif c == "not_run" and len(report.not_run) < REPORT_LIMIT:
                report.not_run.append(entry)
            elif c == "code_changed" and fn not in report.code_changed:
                report.code_changed.append(fn)
        counts["not_comparable"] = not_comparable
        report.counts = counts
        report.new = sum(1 for k in live_best if k not in exp_best)
    report.seconds = round(time.monotonic() - t0, 3)
    Log.info(
        f"[verify] against {described}: {report.counts} new={report.new} "
        f"duplicates={report.duplicates} data={has_data} in {report.seconds}s"
    )
    return report
