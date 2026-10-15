"""The GUI judges a MATLAB node by the ``.m`` file as it is NOW.

C-Brace, 2026-10-14: the registry held the hash from a scan at 15:33; the user
edited ``load_cbrace.m`` and ran it at 18:10. MATLAB recorded the new file's
hash, the state check compared against the scan's, and the node read "the
function's source has changed since it last ran" straight after a clean run.
Every content-dependent read now goes through
``matlab_registry.current_matlab_function``, and the hash itself has one owner
(``scimatlab.source_hash``) shared with MATLAB.
"""

import logging
import os

import pytest

from scimatlab.source_hash import matlab_function_source, read_matlab_source

V1 = b"function out = load_cbrace(filePath)\r\n    out = filePath;\r\nend\r\n"
V2 = b"function tableOut = load_cbrace(filePath, sheet)\r\n    % 5 \xc2\xb0C\r\n    tableOut = filePath;\r\nend\r\n"


@pytest.fixture
def registry_with(tmp_path):
    from scistack_gui import matlab_registry

    before = dict(matlab_registry._matlab_functions)
    path = tmp_path / "load_cbrace.m"
    path.write_bytes(V1)
    matlab_registry._matlab_functions.clear()
    matlab_registry.load_from_sources([path])
    yield matlab_registry, path
    matlab_registry._matlab_functions.clear()
    matlab_registry._matlab_functions.update(before)


def _edit(path, raw):
    st = path.stat()
    path.write_bytes(raw)
    # A coarse-mtime filesystem (SMB, FAT) can leave the stamp unchanged
    # within one tick; the size differs anyway, but bump mtime to be sure.
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 2_000_000_000))


def test_gui_and_matlab_hash_the_same_file_identically(tmp_path):
    """One owner: the parser's hash is what MATLAB's entry point records, on a
    CRLF + non-ASCII file (where the old fileread recipe could diverge)."""
    from scistack_gui.matlab_parser import parse_matlab_function

    path = tmp_path / "load_cbrace.m"
    path.write_bytes(V2)
    info = parse_matlab_function(path)
    digest, _text = matlab_function_source(str(path), "load_cbrace")
    assert info.source_hash == digest == read_matlab_source(path).hash


def test_an_edit_after_the_scan_is_seen_by_the_state_check(registry_with, caplog):
    matlab_registry, path = registry_with
    scanned = matlab_registry.get_matlab_function("load_cbrace").source_hash
    _edit(path, V2)

    with caplog.at_level(logging.INFO, logger="scistack_gui.matlab_registry"):
        from scistack_gui.api.pipeline import _build_matlab_fn_proxy

        proxy = _build_matlab_fn_proxy("load_cbrace")

    assert proxy.source_hash == read_matlab_source(path).hash != scanned
    assert "changed on disk since the last scan" in caplog.text
    # The registry entry itself is replaced, so plain reads agree.
    info = matlab_registry.get_matlab_function("load_cbrace")
    assert info.source_hash == proxy.source_hash
    assert info.params == ["filePath", "sheet"]


def test_run_dispatch_rechecks_the_file_and_logs_the_expected_hash(registry_with, caplog):
    matlab_registry, path = registry_with
    _edit(path, V2)
    from scistack_gui.api.matlab_command import _check_source_at_dispatch

    with caplog.at_level(logging.INFO):
        expected = _check_source_at_dispatch("load_cbrace")

    assert expected == read_matlab_source(path).hash
    assert "[source_hash] load_cbrace" in caplog.text
    assert matlab_registry.get_matlab_function("load_cbrace").params == ["filePath", "sheet"]


def test_an_unchanged_file_costs_no_reparse(registry_with):
    matlab_registry, _path = registry_with
    first = matlab_registry.get_matlab_function("load_cbrace")
    assert matlab_registry.current_matlab_function("load_cbrace", purpose="test") is first


def test_a_vanished_file_keeps_the_scans_entry(registry_with, caplog):
    matlab_registry, path = registry_with
    first = matlab_registry.get_matlab_function("load_cbrace")
    path.unlink()
    with caplog.at_level(logging.WARNING, logger="scistack_gui.matlab_registry"):
        assert matlab_registry.current_matlab_function("load_cbrace", purpose="test") is first
    assert "no longer defines function" in caplog.text
