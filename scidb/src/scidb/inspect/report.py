"""Endpoint report surface: collect finalized plot_/stat_ records into a
self-contained HTML page + stats tables (stage 5 of
docs/claude/endpoints-viz-and-stats-design.md).

Discovery needs no bookkeeping beyond the provenance graph: an endpoint
record is one whose PRODUCING invocation's function_name starts with
``plot_`` (stored value = the artifact path) or ``stat_`` (stored value =
the canonical result JSON, whose optional ``report_path`` names a PDF
artifact). Drafts never appear by construction — they have no records; the
report is explicitly the FINALIZED surface.

Rendering is dependency-free string-template HTML (inline CSS, zero external
requests): the page must open unchanged from an archive folder years later.
"""

from __future__ import annotations

import base64
import html
import json
import shutil
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from ..roles import ROLE_PREFIX, function_role
from ..log import Log

# Embedded images above this size fall back to file links (keeps index.html
# manageable; the artifact copy still lands in artifacts/).
EMBED_SIZE_CAP = 2 * 1024 * 1024


# ---------------------------------------------------------------------------
# Manifest dataclasses (ReportData is what --json emits)
# ---------------------------------------------------------------------------


@dataclass
class FigureEntry:
    record_id: str
    fn: str
    variable: str
    schema: dict
    branch_params: dict
    artifact_path: str
    artifact_exists: bool
    stamp_ok: bool | None  # None = no stamp found; False = record mismatch (STALE)
    timestamp: str | None


@dataclass
class StatEntry:
    record_id: str
    fn: str
    variable: str
    schema: dict
    branch_params: dict
    result: Any  # parsed JSON dict, or the raw string if unparseable
    result_parsed: bool
    report_path: str | None
    report_exists: bool | None
    timestamp: str | None


@dataclass
class ReportData:
    db_name: str
    generated_at: str
    filters: dict
    figures: list[FigureEntry] = field(default_factory=list)
    stats: list[StatEntry] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def collect_report(
    inspector,
    fn: str | None = None,
    variable: str | None = None,
    all_versions: bool = False,
) -> ReportData:
    """Collect every finalized endpoint record into a ReportData manifest.

    fn / variable narrow to one producing function / one output type.
    Latest-per-variant by default (the same collapse ``records()`` uses);
    ``all_versions=True`` includes superseded records.
    """
    from .. import provenance_query

    db = inspector._db
    duck = inspector._duck
    schema_keys = list(db.dataset_schema_keys)

    variable = getattr(variable, "__name__", variable)

    # One pass over the graph: endpoint-producing invocations and their
    # output records. LEFT JOIN _schema — root-level (grand aggregation)
    # outputs have a NULL schema_id and MUST still appear (the same trap the
    # skip gate hit in stage 2).
    schema_cols = "".join(f', s."{k}"' for k in schema_keys)
    # Prefixes come from the shared role table, not literals — the report is
    # the endpoint (plot/stat) surface, and a fifth role must not silently
    # start or stop appearing here.
    endpoint_like = " OR ".join(
        "inv.function_name LIKE '"
        + ROLE_PREFIX[role].replace("_", "\\_")
        + "%' ESCAPE '\\'"
        for role in ("plot", "stat")
    )
    conds = [
        f"({endpoint_like})",
        "COALESCE(r.excluded, FALSE) = FALSE",
    ]
    params: list = []
    if fn:
        conds.append("inv.function_name = ?")
        params.append(str(fn))
    if variable:
        conds.append("r.type = ?")
        params.append(str(variable))
    rows = duck._fetchall(
        "SELECT io.output_record_id, inv.function_name, r.type, "
        "rs.timestamp" + schema_cols + " "
        "FROM _invocation inv "
        "JOIN _invocation_output io ON io.invocation_id = inv.invocation_id "
        "JOIN _record r ON r.record_id = io.output_record_id "
        "LEFT JOIN _record_save rs ON rs.record_id = io.output_record_id "
        "LEFT JOIN _schema s ON r.schema_id = s.schema_id "
        f"WHERE {' AND '.join(conds)} "
        "ORDER BY rs.timestamp DESC",
        params,
    )

    data = ReportData(
        db_name=Path(str(db.dataset_db_path)).name,
        generated_at=time.strftime("%Y-%m-%dT%H:%M:%S"),
        filters={"fn": fn, "variable": variable, "all_versions": all_versions},
    )
    if not rows:
        return data

    # Latest-per-variant collapse: records() / _find_record already implement
    # the authoritative rule per type; intersect with the endpoint rid set.
    seen_rids: set = set()
    entries: list = []  # (rid, fn_name, type_name, timestamp, schema_dict)
    for row in rows:
        rid, fn_name, type_name, ts = str(row[0]), str(row[1]), str(row[2]), row[3]
        if rid in seen_rids:
            continue  # multiple _record_save rows: keep newest (ORDER BY)
        seen_rids.add(rid)
        schema = {}
        for i, k in enumerate(schema_keys):
            v = row[4 + i]
            if v is not None:
                schema[k] = str(v)
        entries.append((rid, fn_name, type_name, ts, schema))

    if not all_versions:
        latest_rids: set = set()
        for type_name in {e[2] for e in entries}:
            try:
                df = db._find_record(
                    type_name,
                    nested_metadata=db._split_metadata({}),
                    version_id="latest",
                )
                latest_rids |= {str(r) for r in df["record_id"]}
            except Exception as e:
                data.warnings.append(
                    f"latest-version collapse unavailable for {type_name}: {e}"
                )
                latest_rids |= {e2[0] for e2 in entries if e2[2] == type_name}
        entries = [e for e in entries if e[0] in latest_rids]

    # Batched enrichment (N+1 rule): branch params + stored values.
    rids = [e[0] for e in entries]
    bp_map = provenance_query.branch_params_batch(duck, rids)
    values = _stored_values(inspector, entries)

    from ..artifact_stamp import read_artifact_stamp

    for rid, fn_name, type_name, ts, schema in entries:
        value = values.get(rid)
        bp = bp_map.get(rid, {}) or {}
        ts_str = str(ts) if ts is not None else None
        if function_role(fn_name) == "plot":
            apath = str(value) if value is not None else ""
            exists = bool(apath) and Path(apath).is_file()
            stamp_ok = None
            if exists:
                blob = read_artifact_stamp(apath)
                if blob is not None and "record_id" in blob:
                    stamp_ok = blob["record_id"] == rid
            if not exists:
                data.warnings.append(
                    f"figure file not found: {apath or '<empty path>'} "
                    f"(record {rid[:12]}, {fn_name})"
                )
            elif stamp_ok is False:
                data.warnings.append(
                    f"STALE ARTIFACT: {apath} carries a different record's "
                    f"stamp — the file was overwritten by another run since "
                    f"record {rid[:12]} ({fn_name}) was saved"
                )
            data.figures.append(
                FigureEntry(
                    record_id=rid,
                    fn=fn_name,
                    variable=type_name,
                    schema=schema,
                    branch_params=bp,
                    artifact_path=apath,
                    artifact_exists=exists,
                    stamp_ok=stamp_ok,
                    timestamp=ts_str,
                )
            )
        else:
            parsed: Any = value
            ok = False
            if isinstance(value, str):
                try:
                    parsed = json.loads(value)
                    ok = isinstance(parsed, dict)
                except (ValueError, TypeError):
                    parsed = value
            if not ok:
                data.warnings.append(
                    f"stat result is not a JSON object (record {rid[:12]}, "
                    f"{fn_name}) — rendered raw"
                )
            rp = parsed.get("report_path") if ok else None
            rp_exists = Path(rp).is_file() if rp else None
            if rp and not rp_exists:
                data.warnings.append(
                    f"stat report file not found: {rp} (record {rid[:12]})"
                )
            data.stats.append(
                StatEntry(
                    record_id=rid,
                    fn=fn_name,
                    variable=type_name,
                    schema=schema,
                    branch_params=bp,
                    result=parsed,
                    result_parsed=ok,
                    report_path=rp,
                    report_exists=rp_exists,
                    timestamp=ts_str,
                )
            )

    Log.info(
        f"[report] collected {len(data.figures)} figure(s), "
        f"{len(data.stats)} stat(s), {len(data.warnings)} warning(s) "
        f"from {data.db_name}"
    )
    return data


def _stored_values(inspector, entries) -> dict:
    """{record_id: stored VARCHAR value} per endpoint record, batched per type."""
    out: dict = {}
    by_type: dict = {}
    for rid, _fn, type_name, _ts, _schema in entries:
        by_type.setdefault(type_name, []).append(rid)
    for type_name, rids in by_type.items():
        table = inspector._scalar(
            "SELECT table_name FROM _registered_types WHERE type_name = ?",
            [type_name],
            default=type_name,
        )
        if not inspector._duck._table_exists(table):
            continue
        placeholders = ", ".join(["?"] * len(rids))
        try:
            df = inspector._duck._fetchdf(
                f'SELECT * FROM "{table}" WHERE record_id IN ({placeholders})',
                list(rids),
            )
        except Exception as e:
            Log.debug(f"[report] values unavailable for {type_name}: {e}")
            continue
        if "record_id" not in df.columns:
            continue
        data_cols = [c for c in df.columns if c not in ("record_id", "schema_id")]
        if not data_cols:
            continue
        for rid, group in df.groupby("record_id"):
            out[str(rid)] = group.iloc[0][data_cols[0]]
    return out


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


def write_report(
    inspector,
    out_dir: str | Path,
    fn: str | None = None,
    variable: str | None = None,
    all_versions: bool = False,
    copy_artifacts: bool = True,
    embed: bool = True,
) -> Path:
    """Write index.html + manifest.json + stats.csv (+ artifacts/ copies).

    Returns the path to index.html. Missing/stale artifacts render their
    metadata with a warning banner rather than failing the report.
    """
    data = collect_report(
        inspector, fn=fn, variable=variable, all_versions=all_versions
    )
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Copy artifacts (record_id-prefixed: collision-proof, portable folder).
    art_dir = out / "artifacts"
    local_paths: dict = {}  # record_id -> relative path within out/
    if copy_artifacts:
        art_dir.mkdir(exist_ok=True)
        for entry in list(data.figures) + list(data.stats):
            src = (
                entry.artifact_path
                if isinstance(entry, FigureEntry)
                else entry.report_path
            )
            src_exists = (
                entry.artifact_exists
                if isinstance(entry, FigureEntry)
                else entry.report_exists
            )
            if not src or not src_exists:
                continue
            dest = art_dir / f"{entry.record_id[:12]}_{Path(src).name}"
            try:
                shutil.copy2(src, dest)
                local_paths[entry.record_id] = f"artifacts/{dest.name}"
            except OSError as e:
                data.warnings.append(f"could not copy {src}: {e}")

    (out / "manifest.json").write_text(json.dumps(asdict(data), indent=2, default=str))
    _write_stats_csv(data, out / "stats.csv")
    index = out / "index.html"
    index.write_text(_render_html(data, local_paths, embed=embed))
    Log.info(
        f"[report] wrote {index} ({len(data.figures)} figure(s), "
        f"{len(data.stats)} stat(s))"
    )
    return index


def _write_stats_csv(data: ReportData, path: Path) -> None:
    """Flat stats table: one row per stat record, scalar top-level keys as
    columns, plus test_family / schema / branch_params identity columns."""
    import pandas as pd

    rows = []
    for e in data.stats:
        row: dict = {
            "test_family": e.fn,
            "variable": e.variable,
            "record_id": e.record_id,
        }
        for k, v in e.schema.items():
            row[k] = v
        for k, v in sorted(e.branch_params.items()):
            row[k] = v
        if e.result_parsed:
            for k, v in e.result.items():
                if isinstance(v, (int, float, str, bool)) or v is None:
                    row[k] = v
        rows.append(row)
    pd.DataFrame(rows).to_csv(path, index=False)


# ---------------------------------------------------------------------------
# HTML rendering (dependency-free, self-contained)
# ---------------------------------------------------------------------------

_CSS = """
body { font-family: -apple-system, 'Segoe UI', Helvetica, Arial, sans-serif;
       margin: 2rem auto; max-width: 70rem; padding: 0 1rem; color: #1a1a1a; }
h1 { border-bottom: 2px solid #444; padding-bottom: .3rem; }
h2 { margin-top: 2.5rem; border-bottom: 1px solid #bbb; padding-bottom: .2rem; }
h3 { margin-top: 1.8rem; color: #333; }
.meta { color: #555; font-size: .9rem; }
.warn { background: #fff3cd; border: 1px solid #e0c25a; border-radius: 4px;
        padding: .5rem .8rem; margin: .4rem 0; font-size: .9rem; }
.stale { background: #f8d7da; border-color: #d9534f; }
figure { margin: 1.2rem 0; padding: .8rem; border: 1px solid #ddd;
         border-radius: 6px; }
figure img { max-width: 100%; height: auto; }
figcaption { font-size: .85rem; color: #444; margin-top: .5rem; }
.rid { font-family: monospace; font-size: .8rem; color: #777; }
.bp { font-family: monospace; font-size: .85rem; color: #0a5; }
table { border-collapse: collapse; margin: .8rem 0; font-size: .9rem; }
th, td { border: 1px solid #ccc; padding: .3rem .6rem; text-align: left; }
th { background: #f2f2f2; }
details pre { background: #f7f7f7; padding: .5rem; border-radius: 4px;
              overflow-x: auto; font-size: .8rem; }
"""


def _esc(v) -> str:
    return html.escape(str(v))


def _identity_caption(entry) -> str:
    parts = []
    if entry.schema:
        parts.append(", ".join(f"{_esc(k)}={_esc(v)}" for k, v in entry.schema.items()))
    if entry.branch_params:
        bp = ", ".join(
            f"{_esc(k)}={_esc(v)}" for k, v in sorted(entry.branch_params.items())
        )
        parts.append(f'<span class="bp">{bp}</span>')
    parts.append(f'<span class="rid">record {_esc(entry.record_id[:12])}</span>')
    return " · ".join(parts)


def _figure_html(e: FigureEntry, local_paths: dict, embed: bool) -> str:
    body = ""
    if not e.artifact_exists:
        body = (
            f'<div class="warn">figure file not found at '
            f"<code>{_esc(e.artifact_path)}</code></div>"
        )
    else:
        if e.stamp_ok is False:
            body += (
                '<div class="warn stale">STALE: this file carries a '
                "different record&#39;s provenance stamp — it was "
                "overwritten by another run</div>"
            )
        src = local_paths.get(e.record_id, e.artifact_path)
        suffix = Path(e.artifact_path).suffix.lower()
        size = Path(e.artifact_path).stat().st_size
        if embed and suffix in (".png", ".svg") and size <= EMBED_SIZE_CAP:
            raw = Path(e.artifact_path).read_bytes()
            mime = "image/png" if suffix == ".png" else "image/svg+xml"
            b64 = base64.b64encode(raw).decode("ascii")
            body += f'<img src="data:{mime};base64,{b64}" alt="{_esc(e.fn)}">'
        elif suffix in (".png", ".svg", ".jpg", ".jpeg"):
            body += f'<img src="{_esc(src)}" alt="{_esc(e.fn)}">'
        else:
            body += (
                f'<a href="{_esc(src)}">{_esc(Path(e.artifact_path).name)}'
                f"</a> ({suffix or 'file'})"
            )
    return f"<figure>{body}<figcaption>{_identity_caption(e)}</figcaption></figure>"


def _stats_family_html(fn_name: str, entries: list) -> str:
    """One table per (fn, result-key-set) family — per-test-type tables,
    consistent with D5's no-universal-schema decision."""
    families: dict = {}
    for e in entries:
        keys = (
            tuple(
                sorted(
                    k
                    for k, v in e.result.items()
                    if isinstance(v, (int, float, str, bool))
                )
            )
            if e.result_parsed
            else ("<raw>",)
        )
        families.setdefault(keys, []).append(e)

    chunks = [f"<h3>{_esc(fn_name)}</h3>"]
    for keys, group in families.items():
        if keys == ("<raw>",):
            for e in group:
                chunks.append(
                    f'<div class="warn">unparseable result for record '
                    f"{_esc(e.record_id[:12])}</div><details><summary>raw"
                    f"</summary><pre>{_esc(e.result)}</pre></details>"
                )
            continue
        schema_cols = sorted({k for e in group for k in e.schema})
        bp_cols = sorted({k for e in group for k in e.branch_params})
        scalar_cols = [k for k in keys if k != "report_path"]
        header = "".join(
            f"<th>{_esc(c)}</th>" for c in schema_cols + bp_cols + scalar_cols
        )
        rows_html = []
        for e in group:
            cells = [e.schema.get(c, "") for c in schema_cols]
            cells += [e.branch_params.get(c, "") for c in bp_cols]
            cells += [e.result.get(c, "") for c in scalar_cols]
            row = "".join(f"<td>{_esc(c)}</td>" for c in cells)
            rows_html.append(f"<tr>{row}</tr>")
            nested = {
                k: v
                for k, v in e.result.items()
                if not isinstance(v, (int, float, str, bool)) and v is not None
            }
            if nested:
                pretty = _esc(json.dumps(nested, indent=2, default=str))
                rows_html.append(
                    f'<tr><td colspan="{len(cells)}"><details>'
                    f"<summary>nested fields (record "
                    f"{_esc(e.record_id[:12])})</summary>"
                    f"<pre>{pretty}</pre></details></td></tr>"
                )
        chunks.append(f"<table><tr>{header}</tr>{''.join(rows_html)}</table>")
    return "\n".join(chunks)


def _render_html(data: ReportData, local_paths: dict, embed: bool) -> str:
    filters = ", ".join(f"{k}={v}" for k, v in data.filters.items() if v)
    head = (
        f"<h1>scidb report — {_esc(data.db_name)}</h1>"
        f'<p class="meta">generated {_esc(data.generated_at)}'
        + (f" · filters: {_esc(filters)}" if filters else "")
        + f" · {len(data.figures)} figure(s), {len(data.stats)} stat result(s)."
        f" Drafts (finalized=False) leave no records and do not appear.</p>"
    )
    warn_html = "".join(f'<div class="warn">{_esc(w)}</div>' for w in data.warnings)

    fig_chunks = []
    by_fn: dict = {}
    for e in data.figures:
        by_fn.setdefault(e.fn, []).append(e)
    for fn_name in sorted(by_fn):
        group = sorted(
            by_fn[fn_name],
            key=lambda e: (
                tuple(sorted(e.schema.items())),
                tuple(sorted(str(i) for i in e.branch_params.items())),
            ),
        )
        fig_chunks.append(f"<h3>{_esc(fn_name)}</h3>")
        fig_chunks.extend(_figure_html(e, local_paths, embed) for e in group)

    stat_chunks = []
    by_fn_s: dict = {}
    for e in data.stats:
        by_fn_s.setdefault(e.fn, []).append(e)
    for fn_name in sorted(by_fn_s):
        stat_chunks.append(_stats_family_html(fn_name, by_fn_s[fn_name]))

    return (
        "<!DOCTYPE html>\n<html><head><meta charset='utf-8'>"
        f"<title>scidb report — {_esc(data.db_name)}</title>"
        f"<style>{_CSS}</style></head><body>"
        + head
        + warn_html
        + ("<h2>Figures</h2>" + "".join(fig_chunks) if fig_chunks else "")
        + ("<h2>Statistics</h2>" + "".join(stat_chunks) if stat_chunks else "")
        + (
            "<p class='meta'>No endpoint records found. Endpoint functions "
            "(plot_*/stat_*) must run with finalized=True to record.</p>"
            if not fig_chunks and not stat_chunks
            else ""
        )
        + "</body></html>"
    )
