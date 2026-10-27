"""The log file's name has one owner: ``scidb.log.LOG_FILE_NAME``.

The file was renamed ``scidb.log`` -> ``scistack.log`` (2026-10-26). Python
reaches the name through ``scidb.log.log_path_for``; two other languages need
it too and cannot import it:

- MATLAB's ``+scidb/configure_database.m`` calls ``attach_log_file`` through
  ``py.`` rather than spelling the names, so it cannot drift. Pinned here.
- The extension's ``failureReporter.ts`` opens the log from TypeScript, so it
  keeps a copy. Pinned here to the owner's value.

If either drifts, MATLAB or the Failures panel reads a file nobody writes.
"""

import re
from pathlib import Path

from scidb.log import LOG_FILE_NAME, log_path_for

_REPO = Path(__file__).resolve().parents[2]


def test_owner_names_scistack_log(tmp_path):
    assert LOG_FILE_NAME == "scistack.log"
    assert log_path_for(tmp_path / "proj.duckdb") == tmp_path / LOG_FILE_NAME


def test_extension_copy_matches_owner():
    src = (_REPO / "scistack-gui" / "extension" / "src" / "failureReporter.ts").read_text(
        encoding="utf-8"
    )
    m = re.search(r"export const LOG_FILE_NAME = '([^']+)';", src)
    assert m, "failureReporter.ts no longer declares LOG_FILE_NAME"
    assert m.group(1) == LOG_FILE_NAME


def test_matlab_configure_database_uses_owner():
    src = (
        _REPO / "scimatlab" / "src" / "scimatlab" / "matlab" / "+scidb" / "configure_database.m"
    ).read_text(encoding="utf-8")
    code = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith("%"))
    assert "py.scidb.log.attach_log_file(" in code
    assert ".log'" not in code, "configure_database.m spells a log file name itself"
