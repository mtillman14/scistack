"""SciStack never records who did anything — guarded like any other fact.

Removed 2026-10-08 (docs/claude/decisions.md, "No user identity"): the
``SCIDB_USER_ID`` reader and the ``_run.user_id``, ``_record_save.user_id``,
``_variant_pin.pinned_by``, tombstone ``deleted_by`` and ``_exclusions.
changed_by`` columns, plus every display of them. History records what ran
and when, never who, so it is shareable by default (docs/claude/
portability.md) and two people's histories of one computation are identical.

The scan covers every package's shipped source in every language. Old
databases still carry the nullable columns (no migrations in beta); nothing
may read or write them again, which is what this test pins.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each

REPO = Path(__file__).resolve().parents[2]

#: Shipped source only — tests may name the concept to assert its absence.
SOURCE_GLOBS = (
    "*/src/**/*.py",
    "*/src/**/*.m",
    "scistack-gui/scistack_gui/**/*.py",
    "scistack-gui/frontend/src/**/*.ts",
    "scistack-gui/frontend/src/**/*.tsx",
    "scistack-gui/extension/src/**/*.ts",
)

FORBIDDEN = re.compile(
    r"\b(user_id|userId|SCIDB_USER\w*|getpass|getuser|pinned_by|deleted_by"
    r"|changed_by|saved_by)\b"
)


def _source_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in SOURCE_GLOBS:
        for p in REPO.glob(pattern):
            if "node_modules" in p.parts or ".test." in p.name:
                continue
            files.add(p)
    return sorted(files)


def test_scan_finds_the_source_tree():
    """The guard is only as good as its reach: an empty scan would pass
    vacuously, so prove it sees the files it is meant to police."""
    files = _source_files()
    names = {p.name for p in files}
    print(f"[test_no_user_identity] scanning {len(files)} source file(s)")
    if not (REPO / "scistack-gui").is_dir():
        pytest.skip("not running from the monorepo checkout")
    for expected in ("provenance_save.py", "variant_pins.py", "VariantsPopup.tsx"):
        assert expected in names, f"scan missed {expected}; globs out of date"


def test_no_user_identity_in_any_source():
    hits = []
    for p in _source_files():
        text = p.read_text(encoding="utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), 1):
            if FORBIDDEN.search(line):
                hits.append(f"{p.relative_to(REPO)}:{lineno}: {line.strip()}")
    assert not hits, (
        "User identity is not a SciStack concept (removed 2026-10-08, see "
        "docs/claude/decisions.md). Remove:\n" + "\n".join(hits)
    )


# ---------------------------------------------------------------------------
# Behaviour: a fresh database stores no identity, even when the old
# environment variable is still set on someone's machine.
# ---------------------------------------------------------------------------

SENTINEL = "alice-should-never-be-stored"
WHO_COLUMNS = {"user_id", "pinned_by", "deleted_by", "changed_by", "saved_by"}


class IdentityRaw(BaseVariable):
    pass


class IdentityOut(BaseVariable):
    pass


def _double(x):
    return x * 2


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setenv("SCIDB_USER_ID", SENTINEL)
    _scifor.set_schema([])
    db = configure_database(tmp_path / "identity.duckdb", ["subject"])
    yield db
    _scifor.set_schema([])
    db.close()


def test_fresh_database_has_no_who_columns_and_stores_no_identity(db):
    from scidb.exclusions import exclude_schema

    IdentityRaw.save(np.array([1.0, 2.0]), subject="S01")
    for_each(_double, {"x": IdentityRaw}, [IdentityOut], subject=["S01"])
    exclude_schema("identity test", db=db, subject="S02")

    columns = db._duck._fetchall(
        "SELECT table_name, column_name FROM information_schema.columns"
    )
    tables = sorted({t for t, _ in columns})
    print(f"[test_no_user_identity] {len(tables)} table(s): {tables}")
    who = sorted(f"{t}.{c}" for t, c in columns if c in WHO_COLUMNS)
    assert not who, f"who-columns created in a fresh database: {who}"

    # The sentinel must not have landed in any text cell of any table.
    leaked = []
    for table in tables:
        text_cols = [
            c
            for t, c, dtype in db._duck._fetchall(
                "SELECT table_name, column_name, data_type "
                "FROM information_schema.columns WHERE table_name = ?",
                [table],
            )
            if "CHAR" in dtype.upper() or dtype.upper() in ("TEXT", "STRING")
        ]
        for col in text_cols:
            n = db._duck._fetchone(
                f'SELECT COUNT(*) FROM "{table}" WHERE CAST("{col}" AS VARCHAR) LIKE ?',
                [f"%{SENTINEL}%"],
            )[0]
            if n:
                leaked.append(f"{table}.{col} ({n} row(s))")
    assert not leaked, f"SCIDB_USER_ID value was stored: {leaked}"
