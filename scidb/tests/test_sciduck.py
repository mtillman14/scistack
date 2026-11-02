"""scidb's SciDuck: schema ids, the variable list and groups.

``SciDuck`` moved here from sciduckdb (plan P3 stage 6, D4): its three
tables are declared in ``scidb.store.tables`` and created on a writable open
(connect -> ``tables.ensure_core`` -> schema validation). The codec and the
connection it builds on are sciduckdb's, tested there
(``sciduckdb/tests/test_codec_storage.py``).
"""

import json

import pytest

from scidb.store.sciduck import SciDuck


@pytest.fixture
def duck():
    """A fresh in-memory SciDuck."""
    db = SciDuck(":memory:", dataset_schema=["subject", "session", "trial"])
    yield db
    db.close()


def register(duck: SciDuck, name: str) -> None:
    """A row in ``_variables``, as scidb's save writes one."""
    duck.execute(
        "INSERT INTO _variables (variable_name, schema_level, dtype, description) "
        "VALUES (?, ?, ?, '')",
        [name, duck.dataset_schema[-1], json.dumps({"mode": "single_column"})],
    )


# ---------------------------------------------------------------------------
# Initialization
# ---------------------------------------------------------------------------


class TestInit:
    """Test SciDuck initialization."""

    def test_creates_metadata_tables(self, duck):
        """Should create _schema, _variables, and _variable_groups tables."""
        tables = [
            row[0]
            for row in duck.fetchall(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            )
        ]
        assert "_schema" in tables
        assert "_variables" in tables
        assert "_variable_groups" in tables

    def test_stores_dataset_schema(self, duck):
        """Should store the dataset schema."""
        assert duck.dataset_schema == ["subject", "session", "trial"]

    def test_schema_mismatch_raises(self, tmp_path):
        """Reopening with different schema should raise ValueError."""
        db_path = tmp_path / "schema_test.duckdb"
        db1 = SciDuck(db_path, dataset_schema=["subject", "session"])
        # Populate _schema
        db1.batch_get_or_create_schema_ids(
            {("session", ("S01", "A")): {"subject": "S01", "session": "A"}}
        )
        db1.close()

        with pytest.raises(ValueError, match="schema mismatch"):
            SciDuck(db_path, dataset_schema=["subject", "trial"])

    def test_repr(self, duck):
        """Should return a readable repr."""
        r = repr(duck)
        assert "SciDuck" in r
        assert "variables=" in r

    def test_context_manager(self, tmp_path):
        """Should work as a context manager."""
        db_path = tmp_path / "ctx.duckdb"
        with SciDuck(db_path, dataset_schema=["subject"]) as db:
            register(db, "x")
        # Connection should be closed after exiting


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


class TestGroups:
    """Test variable grouping functionality."""

    def test_add_to_group(self, duck):
        """Should add variables to a group."""
        register(duck, "var_a")
        register(duck, "var_b")

        duck.add_to_group("my_group", ["var_a", "var_b"])

        members = duck.get_group("my_group")
        assert "var_a" in members
        assert "var_b" in members

    def test_add_single_string(self, duck):
        """Should accept a single string instead of list."""
        duck.add_to_group("single", "var_x")
        members = duck.get_group("single")
        assert "var_x" in members

    def test_remove_from_group(self, duck):
        """Should remove variable from group."""
        duck.add_to_group("grp", ["var_a", "var_b"])
        duck.remove_from_group("grp", "var_a")

        members = duck.get_group("grp")
        assert "var_a" not in members
        assert "var_b" in members

    def test_list_groups(self, duck):
        """Should list all group names."""
        duck.add_to_group("group_1", "a")
        duck.add_to_group("group_2", "b")

        groups = duck.list_groups()
        assert "group_1" in groups
        assert "group_2" in groups

    def test_add_duplicate_is_idempotent(self, duck):
        """Adding same variable to same group twice should not duplicate."""
        duck.add_to_group("grp", "var_a")
        duck.add_to_group("grp", "var_a")

        members = duck.get_group("grp")
        assert members.count("var_a") == 1


# ---------------------------------------------------------------------------
# List / Inspect
# ---------------------------------------------------------------------------


class TestListInspect:
    """Test listing and inspection features."""

    def test_list_variables(self, duck):
        """Should list all variables with metadata."""
        register(duck, "alpha")
        register(duck, "beta")

        var_list = duck.list_variables()
        assert len(var_list) == 2
        names = list(var_list["variable_name"])
        assert "alpha" in names
        assert "beta" in names



# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_variable_survives_reconnect(self, tmp_path):
        """Variable metadata should survive reconnection."""
        db_path = tmp_path / "persist_ver.duckdb"
        schema = ["subject"]

        db1 = SciDuck(db_path, dataset_schema=schema)
        register(db1, "registered")
        db1.close()

        db2 = SciDuck(db_path, dataset_schema=schema)
        names = list(db2.list_variables()["variable_name"])
        db2.close()

        assert names == ["registered"]


# ---------------------------------------------------------------------------
# Schema ids (non-contiguous keys)
# ---------------------------------------------------------------------------


@pytest.fixture
def wide_duck():
    """Provide a SciDuck with a wider schema for non-contiguous key tests."""
    db = SciDuck(
        ":memory:",
        dataset_schema=["subject", "intervention", "timepoint", "speed", "trial"],
    )
    yield db
    db.close()


def _speed_combo(subject, speed):
    keys = {"subject": subject, "intervention": "RMT30", "speed": speed}
    return ("speed", (subject, "RMT30", speed)), keys


class TestSchemaIds:
    """batch_get_or_create_schema_ids: the one owner of _schema rows."""

    def test_schema_row_has_nulls_for_skipped_keys(self, wide_duck):
        """Skipped schema keys should be NULL in _schema table."""
        wide_duck.batch_get_or_create_schema_ids(dict([_speed_combo("S01", "SSV")]))
        row = wide_duck.query(
            "SELECT * FROM _schema WHERE schema_level = ?",
            params=["speed"],
        )
        assert row["timepoint"].iloc[0] is None
        assert row["trial"].iloc[0] is None
        assert row["subject"].iloc[0] == "S01"
        assert row["intervention"].iloc[0] == "RMT30"
        assert row["speed"].iloc[0] == "SSV"

    def test_distinct_non_contiguous_locations_get_distinct_ids(self, wide_duck):
        ids = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S01", "FV")])
        )
        assert len(set(ids.values())) == 2
        schema_rows = wide_duck.query(
            "SELECT * FROM _schema WHERE schema_level = 'speed'"
        )
        assert set(schema_rows["speed"]) == {"SSV", "FV"}

    def test_second_batch_reuses_existing_and_allocates_new_without_collision(
        self, wide_duck
    ):
        """Regression: ids are allocated from MAX(schema_id)+1, so a later
        batch must reuse existing rows and never collide with them."""
        first = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S02", "SSV")])
        )
        second = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S02", "FV")])
        )
        assert second[("speed", ("S01", "RMT30", "SSV"))] == first[
            ("speed", ("S01", "RMT30", "SSV"))
        ]
        assert second[("speed", ("S02", "RMT30", "FV"))] not in first.values()

    def test_batch_then_single_schema_id_no_collision(self, wide_duck):
        """Regression: both paths allocate from MAX(schema_id)+1. The single
        path formerly used a DuckDB sequence, which could fall out of sync and
        hit a duplicate primary key on _schema."""
        batch_ids = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S01", "FV")])
        )
        _, new_keys = _speed_combo("S02", "FV")
        new_id = wide_duck.get_or_create_schema_id("speed", new_keys)
        assert new_id not in batch_ids.values()
        key, existing_keys = _speed_combo("S01", "SSV")
        existing_id = wide_duck.get_or_create_schema_id("speed", existing_keys)
        assert existing_id == batch_ids[key]

    def test_single_then_batch_schema_id_no_collision(self, wide_duck):
        key, keys = _speed_combo("S01", "SSV")
        id1 = wide_duck.get_or_create_schema_id("speed", keys)
        batch_ids = wide_duck.batch_get_or_create_schema_ids(
            dict([_speed_combo("S01", "SSV"), _speed_combo("S01", "FV")])
        )
        assert batch_ids[key] == id1
        assert batch_ids[("speed", ("S01", "RMT30", "FV"))] != id1


def test_list_variables_if_open_returns_none_when_closed_without_error(
    tmp_path, caplog
):
    """The best-effort read checks and queries as one step: closed -> None,
    and no ERROR traceback (scistack.log 2026-10-19 15:08:27)."""
    import logging

    from scidb.store.sciduck import SciDuck

    db = SciDuck(str(tmp_path / "ifopen.duckdb"), dataset_schema=["subject"])
    assert db.list_variables_if_open() is not None
    db.close()
    caplog.set_level(logging.DEBUG)
    assert db.list_variables_if_open() is None
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    db.reopen()
    assert list(db.list_variables_if_open().columns) == list(
        db.list_variables().columns
    )
    db.close()


