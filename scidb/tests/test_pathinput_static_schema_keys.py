"""A fully static PathInput (no {key} placeholders) can't supply values for
ANY schema key. Requesting schema_keys that have no other source (no table,
no DB rows, no matching placeholder) should be dropped from iteration rather
than erroring or silently producing 0 iterations -- the caller gets a single
run against the literal path, as if those keys had never been requested.

Mirrors +scifor/for_each.m's pi_is_static handling (scimatlab) so a one-off
literal-path pipeline behaves the same in MATLAB and Python.
"""

import scifor as _scifor
import pytest

from scidb import configure_database, for_each


def read_content(filepath):
    from pathlib import Path

    return Path(str(filepath)).read_text().strip()


@pytest.fixture
def literal_file(tmp_path):
    """A single real file, unrelated to any schema-key template."""
    f = tmp_path / "6MWT_GR.xlsx"
    f.write_text("hello")
    return f


@pytest.fixture
def empty_db(tmp_path):
    db = configure_database(tmp_path / "test.duckdb", ["subject", "pass"])
    yield db
    _scifor.set_schema([])
    db.close()


class TestStaticPathInputDropsUnresolvedSchemaKeys:
    def test_schema_keys_dropped_runs_once(self, empty_db, literal_file):
        pi = _scifor.PathInput(str(literal_file), name=str(str(literal_file)))

        result = for_each(
            read_content,
            {"filepath": pi},
            [],
            save=False,
            schema_keys=["subject", "pass"],
        )

        assert len(result) == 1
        # outputs=[] -> scidb defaults the output column name to "result"
        # (its own convention), not scifor's "output".
        assert result["result"].iloc[0] == "hello"

    def test_not_dropped_when_pathinput_is_templated(self, empty_db, tmp_path):
        # Contrast case: a PathInput that DOES have placeholders is a real
        # candidate source for 'subject', so the new drop-unresolved-keys
        # leniency must not kick in -- 'pass' stays unresolved and the
        # pre-existing "0 iterations" behavior (not a hard error, but not a
        # drop-and-run-once either) is preserved.
        pi = _scifor.PathInput(
            "{subject}/6MWT_GR.xlsx", root_folder=str(tmp_path), name="{subject}/6MWT_GR.xlsx"
        )

        result = for_each(
            read_content,
            {"filepath": pi},
            [],
            save=False,
            schema_keys=["subject", "pass"],
        )

        assert len(result) == 0
