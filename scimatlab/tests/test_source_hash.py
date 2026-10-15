"""``scimatlab.source_hash`` is the ONE owner of a MATLAB function's hash.

C-Brace, 2026-10-14: a clean MATLAB run recorded one hash and the GUI judged
the node by another, so the node read "the function's source has changed since
it last ran" right after running. MATLAB hashed ``fileread(...)`` re-encoded as
UTF-8, the GUI hashed raw bytes — two recipes for one concept. Both now ask
this module, and MATLAB no longer reads the file for hashing at all.
"""

import sys
from hashlib import sha256
from pathlib import Path

_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_root / "scimatlab" / "src"))

from scimatlab.source_hash import (  # noqa: E402
    hash_source_bytes,
    matlab_function_source,
    read_matlab_source,
    text_hashes_to,
)

_HASH_FUNCTION_M = (
    _root / "scimatlab" / "src" / "scimatlab" / "matlab" / "+scidb" / "+internal"
    / "hash_function.m"
)

#: The shapes on which a decode-then-re-encode recipe diverges from raw bytes.
_AWKWARD = {
    "crlf": b"function out = f(x)\r\n    out = x;\r\nend\r\n",
    "bom": b"\xef\xbb\xbffunction out = f(x)\n    out = x;\nend\n",
    "non_ascii": "function out = f(x)\n    % 5 °C, µV\n    out = x;\nend\n".encode(),
    "latin1": b"function out = f(x)\n    % 5 \xb0C\n    out = x;\nend\n",
}


def test_the_recipe_is_sha256_of_the_raw_bytes(tmp_path):
    for name, raw in _AWKWARD.items():
        path = tmp_path / f"{name}.m"
        path.write_bytes(raw)
        assert read_matlab_source(path).hash == sha256(raw).hexdigest(), name
        assert hash_source_bytes(raw) == sha256(raw).hexdigest(), name


def test_what_matlab_records_is_what_the_file_bytes_hash_to(tmp_path):
    """MATLAB's entry point returns the raw-bytes hash for every shape, so it
    cannot drift from the GUI's reading of the same file."""
    for name, raw in _AWKWARD.items():
        path = tmp_path / f"{name}.m"
        path.write_bytes(raw)
        digest, _text = matlab_function_source(str(path), "f")
        assert digest == hash_source_bytes(raw), name


def test_utf8_text_round_trips_to_its_hash_and_is_captured(tmp_path):
    for name in ("crlf", "bom", "non_ascii"):
        path = tmp_path / f"{name}.m"
        path.write_bytes(_AWKWARD[name])
        digest, text = matlab_function_source(str(path), "f")
        assert text, name
        assert text_hashes_to(text, digest), name


def test_a_non_utf8_file_records_its_hash_but_captures_no_text(tmp_path):
    path = tmp_path / "latin1.m"
    path.write_bytes(_AWKWARD["latin1"])
    src = read_matlab_source(path)
    assert not src.faithful
    digest, text = matlab_function_source(str(path), "f")
    assert digest == src.hash
    assert text == ""


def test_text_that_is_not_the_files_text_does_not_match():
    raw = _AWKWARD["crlf"]
    assert not text_hashes_to(raw.decode().replace("\r\n", "\n"), hash_source_bytes(raw))


def test_matlab_does_not_hash_on_its_own():
    """``hash_function.m`` resolves the file and delegates; it must not read
    or hash the text itself (that was the second recipe)."""
    lines = [
        line
        for line in _HASH_FUNCTION_M.read_text().splitlines()
        if not line.lstrip().startswith("%")
    ]
    code = "\n".join(lines)
    assert "py.scimatlab.source_hash.matlab_function_source(" in code
    assert "fileread" not in code
    assert "sha256" not in code
