"""The ONE owner of a MATLAB function's source identity: the hash of its ``.m``.

Two readers need this number and they must never disagree:

- **MATLAB at run time** (``+scidb/+internal/hash_function.m``) — the hash a
  run records in ``_invocation.function_hash``.
- **The GUI at state-check / dispatch time** (``matlab_parser`` via
  ``matlab_registry``) — the hash it compares the recorded ones against.

They used to compute it two ways: the GUI hashed the file's raw bytes, MATLAB
hashed ``fileread(...)`` re-encoded as UTF-8. Those agree only when MATLAB
decodes the file exactly as UTF-8 — a BOM, a non-ASCII character read under a
Windows code page, or non-UTF-8 bytes made every MATLAB node permanently red
("the function's source has changed since it last ran") with nothing edited.

**The recipe: SHA-256 of the file's raw bytes**, read here, in Python, by both.
MATLAB no longer reads the file for hashing at all — it resolves the path with
``which`` and calls :func:`matlab_function_source`. Bytes are the only form
both sides see identically: no decoding, no line-ending normalisation.

:func:`text_hashes_to` is the same recipe in text form, for the one caller
that holds text rather than a path (the bridge, deciding whether the source
MATLAB forwarded can be filed under the digest). It is true exactly when the
text is the faithful UTF-8 decoding of the hashed bytes.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

@dataclass(frozen=True)
class MatlabSource:
    """One ``.m`` file's bytes, read once: its hash and its text."""

    path: str
    #: 64-hex SHA-256 of the raw bytes — the stored ``function_hash``.
    hash: str
    #: The bytes decoded as UTF-8 (``errors="replace"``: for display and
    #: parsing; :attr:`faithful` says whether it re-encodes to the bytes).
    text: str
    size: int
    #: The text re-encodes to exactly the hashed bytes, so it may be filed
    #: under :attr:`hash` (source capture).
    faithful: bool
    crlf: bool
    bom: bool
    non_ascii: bool

    def describe(self) -> str:
        """The facts that decide whether two readers could disagree."""
        return (
            f"{self.hash[:12]} ({self.size} bytes, crlf={self.crlf}, bom={self.bom}, "
            f"non_ascii={self.non_ascii}, utf8={self.faithful})"
        )


def hash_source_bytes(raw: bytes) -> str:
    """THE recipe: SHA-256 hex of a MATLAB source file's raw bytes."""
    return sha256(raw).hexdigest()


def text_hashes_to(text: str, digest: str) -> bool:
    """Whether *text* is the UTF-8 form of bytes hashing to *digest*."""
    try:
        raw = text.encode("utf-8")
    except UnicodeEncodeError:  # lone surrogates from a lossy round trip
        return False
    return hash_source_bytes(raw) == str(digest)


def source_from_bytes(raw: bytes, path: "str | Path" = "") -> MatlabSource:
    """:class:`MatlabSource` for bytes already read (the GUI parser reads the
    file once for parsing and hashing both)."""
    try:
        text = raw.decode("utf-8")
        faithful = True
    except UnicodeDecodeError:
        text = raw.decode("utf-8", errors="replace")
        faithful = False
    return MatlabSource(
        path=str(path),
        hash=hash_source_bytes(raw),
        text=text,
        size=len(raw),
        faithful=faithful,
        crlf=b"\r\n" in raw,
        bom=raw.startswith(b"\xef\xbb\xbf"),
        non_ascii=any(b > 127 for b in raw),
    )


def read_matlab_source(path: "str | Path") -> MatlabSource:
    """Read *path* and hash it with the one recipe. Raises ``OSError``."""
    return source_from_bytes(Path(path).read_bytes(), path)


def matlab_function_source(path: str, name: str = "") -> tuple[str, str]:
    """``(hash, text)`` for MATLAB's ``hash_function.m``.

    *text* is ``""`` when it would not re-hash to *hash* (the file is not
    UTF-8): the run still records the right hash, it just captures no source.
    Logged at INFO with the encoding facts, so a run's log names the exact
    file and digest MATLAB recorded — the run-time half of the check whose
    graph-time half is the GUI's ``matlab_registry.current_matlab_function``.
    """
    from scidb.log import Log

    src = read_matlab_source(str(path))
    Log.info(
        f"[source_hash] {name or Path(str(path)).stem}: run hashes {src.path} -> "
        f"{src.describe()}",
    )
    if not src.faithful:
        Log.warn(
            f"[source_hash] {name}: {src.path} is not valid UTF-8; the run records "
            f"its hash but captures no source text",
        )
        return src.hash, ""
    return src.hash, src.text
