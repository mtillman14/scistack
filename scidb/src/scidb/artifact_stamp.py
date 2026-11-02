"""Artifact provenance stamping (D4, endpoints-viz-and-stats-design.md).

Embeds a JSON provenance blob INSIDE endpoint artifacts — ``plot_`` figures
and ``stat_`` PDF reports — so a file found in a paper draft years later
traces back to its exact DB record. Embedded metadata travels with the file
(unlike a sidecar, which a copy/move/email can leave behind); the sidecar
``<artifact>.provenance.json`` is strictly the fallback for formats that
cannot be embedded safely.

All stamping is dependency-free (stdlib only) and operates on the FILE, not
the renderer (``savefig(metadata=)`` cannot include the record_id — it only
exists after the save — and file-level stamping works identically for
MATLAB-rendered figures whose paths cross the bridge).

Formats:

- **PNG** — a ``tEXt`` chunk (keyword ``scidb:provenance``) inserted after
  ``IHDR``; CRC via ``zlib.crc32``.
- **SVG** — a ``<metadata id="scidb-provenance">`` element inserted right
  after the opening ``<svg …>`` tag.
- **PDF** — an *incremental update*: a new object carrying
  ``/scidb_provenance <hex-encoded JSON>`` is appended together with a new
  xref subsection and a trailer chaining to the previous one via ``/Prev``.
  Every original byte stays untouched. Works for classic-xref producers
  (reportlab — csv-stats' writer — and matplotlib both qualify); PDFs whose
  trailer cannot be parsed (e.g. xref-stream producers) fall back to the
  sidecar. The READER never depends on the update being well-formed: it is a
  backwards byte-scan for the marker.

Failures never raise out of ``stamp_artifact`` — an artifact that renders but
cannot be stamped must not fail a pipeline. Every stamp, fallback, and
failure is logged.
"""

import json
import re
import zlib
from pathlib import Path

from scidb.model.log import Log

STAMP_VERSION = 1
_PNG_SIG = b"\x89PNG\r\n\x1a\n"
_PNG_KEYWORD = b"scidb:provenance"
_SVG_MARKER = '<metadata id="scidb-provenance">'
_PDF_KEY = b"/scidb_provenance"
_SIDECAR_SUFFIX = ".provenance.json"


# ---------------------------------------------------------------------------
# PNG
# ---------------------------------------------------------------------------


def _png_chunks(data: bytes):
    """Yield (offset, length, type, data_bytes) for each chunk."""
    pos = len(_PNG_SIG)
    while pos + 8 <= len(data):
        length = int.from_bytes(data[pos : pos + 4], "big")
        ctype = data[pos + 4 : pos + 8]
        yield pos, length, ctype, data[pos + 8 : pos + 8 + length]
        pos += 12 + length


def _strip_png_stamp(data: bytes) -> bytes:
    """Remove any existing scidb tEXt chunk (idempotent re-stamping)."""
    for pos, length, ctype, cdata in _png_chunks(data):
        if ctype == b"tEXt" and cdata.startswith(_PNG_KEYWORD + b"\x00"):
            return data[:pos] + data[pos + 12 + length :]
    return data


def _stamp_png(data: bytes, payload: bytes) -> "bytes | None":
    if not data.startswith(_PNG_SIG) or data[12:16] != b"IHDR":
        return None
    data = _strip_png_stamp(data)
    ihdr_len = int.from_bytes(data[8:12], "big")
    insert_at = len(_PNG_SIG) + 12 + ihdr_len  # after IHDR incl. its CRC
    chunk_data = _PNG_KEYWORD + b"\x00" + payload
    chunk = (
        len(chunk_data).to_bytes(4, "big")
        + b"tEXt"
        + chunk_data
        + zlib.crc32(b"tEXt" + chunk_data).to_bytes(4, "big")
    )
    return data[:insert_at] + chunk + data[insert_at:]


def _read_png(data: bytes) -> "str | None":
    if not data.startswith(_PNG_SIG):
        return None
    for _pos, _length, ctype, cdata in _png_chunks(data):
        if ctype == b"tEXt" and cdata.startswith(_PNG_KEYWORD + b"\x00"):
            return cdata[len(_PNG_KEYWORD) + 1 :].decode("latin-1")
    return None


# ---------------------------------------------------------------------------
# SVG
# ---------------------------------------------------------------------------


def _xml_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _xml_unescape(s: str) -> str:
    return s.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def _stamp_svg(data: bytes, payload: bytes) -> "bytes | None":
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    # Strip an existing stamp element (idempotent re-stamping).
    start = text.find(_SVG_MARKER)
    if start != -1:
        end = text.find("</metadata>", start)
        if end != -1:
            text = text[:start] + text[end + len("</metadata>") :]
    svg_open = text.find("<svg")
    if svg_open == -1:
        return None
    tag_end = text.find(">", svg_open)
    if tag_end == -1 or text[tag_end - 1] == "/":  # self-closing root: no content
        return None
    element = _SVG_MARKER + _xml_escape(payload.decode("ascii")) + "</metadata>"
    return (text[: tag_end + 1] + element + text[tag_end + 1 :]).encode("utf-8")


def _read_svg(data: bytes) -> "str | None":
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None
    start = text.find(_SVG_MARKER)
    if start == -1:
        return None
    start += len(_SVG_MARKER)
    end = text.find("</metadata>", start)
    if end == -1:
        return None
    return _xml_unescape(text[start:end])


# ---------------------------------------------------------------------------
# PDF (incremental update; classic-xref producers)
# ---------------------------------------------------------------------------


def _stamp_pdf(data: bytes, payload: bytes) -> "bytes | None":
    if not data.startswith(b"%PDF"):
        return None
    sx = data.rfind(b"startxref")
    if sx == -1:
        return None
    m = re.match(rb"\s*(\d+)", data[sx + len(b"startxref") :])
    if m is None:
        return None
    prev_xref = int(m.group(1))
    # Classic trailer dict (reportlab / matplotlib). No trailer keyword →
    # xref-stream PDF → caller falls back to the sidecar.
    tidx = data.rfind(b"trailer", 0, sx)
    if tidx == -1:
        return None
    tdict = data[tidx:sx]
    root_m = re.search(rb"/Root\s+(\d+)\s+(\d+)\s+R", tdict)
    size_m = re.search(rb"/Size\s+(\d+)", tdict)
    if root_m is None or size_m is None:
        return None
    new_obj = int(size_m.group(1))  # /Size == next free object number

    out = data if data.endswith(b"\n") else data + b"\n"
    obj_offset = len(out)
    out += b"%d 0 obj\n<< %s <%s> >>\nendobj\n" % (
        new_obj,
        _PDF_KEY,
        payload.hex().encode("ascii"),
    )
    xref_offset = len(out)
    out += b"xref\n%d 1\n%010d 00000 n \n" % (new_obj, obj_offset)
    trailer = b"trailer\n<< /Size %d /Root %s %s R /Prev %d /ScidbProv %d 0 R >>\n" % (
        new_obj + 1,
        root_m.group(1),
        root_m.group(2),
        prev_xref,
        new_obj,
    )
    out += trailer
    out += b"startxref\n%d\n%%%%EOF\n" % xref_offset
    return out


def _read_pdf(data: bytes) -> "str | None":
    # Backwards byte-scan: independent of xref flavor and update validity.
    idx = data.rfind(_PDF_KEY + b" <")
    if idx == -1:
        return None
    start = idx + len(_PDF_KEY) + 2
    end = data.find(b">", start)
    if end == -1:
        return None
    try:
        return bytes.fromhex(data[start:end].decode("ascii")).decode("ascii")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_STAMPERS = {".png": _stamp_png, ".svg": _stamp_svg, ".pdf": _stamp_pdf}
_READERS = {".png": _read_png, ".svg": _read_svg, ".pdf": _read_pdf}


def _sidecar_path(path: Path) -> Path:
    return path.with_name(path.name + _SIDECAR_SUFFIX)


def stamp_artifact(path: "str | Path", blob: dict) -> bool:
    """Embed ``blob`` (JSON) into the artifact at ``path``.

    Returns True when embedded IN the file; False when the sidecar fallback
    was used or the file is missing. Never raises: a figure that renders but
    cannot be stamped must not fail the pipeline.
    """
    p = Path(path)
    payload = json.dumps(blob, sort_keys=True, ensure_ascii=True).encode("ascii")
    if not p.is_file():
        Log.warn(f"[artifact-stamp] {p}: file not found — nothing stamped")
        return False

    suffix = p.suffix.lower()
    stamper = _STAMPERS.get(suffix)
    reason = f"unsupported format {suffix!r}"
    if stamper is not None:
        try:
            data = p.read_bytes()
            new = stamper(data, payload)
            if new is not None:
                p.write_bytes(new)
                Log.info(
                    f"[artifact-stamp] {p.name}: embedded provenance "
                    f"({suffix}, {len(payload)} bytes)"
                )
                return True
            reason = f"{suffix} structure not recognized (e.g. xref-stream PDF)"
        except Exception as exc:  # never fail the pipeline over a stamp
            reason = f"{type(exc).__name__}: {exc}"

    try:
        _sidecar_path(p).write_text(payload.decode("ascii"))
        Log.warn(
            f"[artifact-stamp] {p.name}: could not embed ({reason}) — "
            f"wrote sidecar {_sidecar_path(p).name}"
        )
    except Exception as exc:
        Log.warn(
            f"[artifact-stamp] {p.name}: could not embed ({reason}) and "
            f"sidecar write failed ({type(exc).__name__}: {exc})"
        )
    return False


def read_artifact_stamp(path: "str | Path") -> "dict | None":
    """Read the provenance blob from an artifact (embedded or sidecar).

    Returns the parsed dict, or None when no stamp is found.
    """
    p = Path(path)
    raw: str | None = None
    if p.is_file():
        reader = _READERS.get(p.suffix.lower())
        try:
            data = p.read_bytes()
            if reader is not None:
                raw = reader(data)
            else:
                # Unknown extension: probe all formats by content.
                for probe in (_read_png, _read_svg, _read_pdf):
                    raw = probe(data)
                    if raw is not None:
                        break
        except Exception:
            raw = None
    if raw is None:
        sidecar = _sidecar_path(p)
        if sidecar.is_file():
            try:
                raw = sidecar.read_text()
            except Exception:
                return None
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None
