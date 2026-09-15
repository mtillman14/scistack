"""Declared level order: ``[schema_keys]`` in the project config.

Stage 7 of ``.claude/plan-schema-key-picker-and-level-order.md``. A schema key's
levels have no inherent order — ``session`` is chronological to the person who
ran the study and alphabetical to everything else — so the project says which.

The rule under test, in one line: **declared levels first, in the declared
order; everything else appended by whatever sorted them before.** The second
half is what keeps a session collected after the file was written visible
rather than dropped.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, configure_database, schema_order

SCHEMA = ["subject", "session"]


class Ordered(BaseVariable):
    pass


@pytest.fixture(autouse=True)
def _clear_cache():
    schema_order.clear_cache()
    yield
    schema_order.clear_cache()


def write_config(root, body: str, *, pyproject: bool = False):
    if pyproject:
        (root / "pyproject.toml").write_text(
            f"[project]\nname = 'x'\n\n[tool.scistack.schema_keys]\n{body}",
            encoding="utf-8",
        )
    else:
        (root / "scistack.toml").write_text(
            f"modules = []\n\n[schema_keys]\n{body}", encoding="utf-8"
        )


# --- reading the declaration ------------------------------------------------


class TestReading:
    def test_scistack_toml(self, tmp_path):
        write_config(tmp_path, 'session = ["BL", "POST", "FU"]\n')
        assert schema_order.declared_level_order(tmp_path) == {
            "session": ["BL", "POST", "FU"]
        }

    def test_pyproject_toml(self, tmp_path):
        write_config(tmp_path, 'session = ["BL", "POST"]\n', pyproject=True)
        assert schema_order.declared_level_order(tmp_path) == {
            "session": ["BL", "POST"]
        }

    def test_no_config_and_no_table_are_both_empty(self, tmp_path):
        assert schema_order.declared_level_order(tmp_path) == {}
        (tmp_path / "scistack.toml").write_text("modules = []\n", encoding="utf-8")
        schema_order.clear_cache()
        assert schema_order.declared_level_order(tmp_path) == {}

    def test_values_are_text_so_padding_survives(self, tmp_path):
        """"01" must stay "01" rather than becoming 1 — which spelling is
        identity is schema-key-types' decision, not this file's."""
        write_config(tmp_path, 'subject = ["01", "02", 3]\n')
        assert schema_order.declared_level_order(tmp_path) == {
            "subject": ["01", "02", "3"]
        }

    def test_a_repeated_level_is_collapsed(self, tmp_path):
        write_config(tmp_path, 'session = ["BL", "BL", "POST"]\n')
        assert schema_order.declared_level_order(tmp_path) == {
            "session": ["BL", "POST"]
        }

    def test_a_malformed_declaration_is_ignored_not_fatal(self, tmp_path, caplog):
        import logging

        write_config(tmp_path, 'session = "BL"\n')
        with caplog.at_level(logging.WARNING, logger="scidb"):
            assert schema_order.declared_level_order(tmp_path) == {}
        assert "not a list of levels" in caplog.text

    def test_an_edit_is_picked_up_without_manual_invalidation(self, tmp_path):
        write_config(tmp_path, 'session = ["BL"]\n')
        assert schema_order.declared_level_order(tmp_path)["session"] == ["BL"]

        import os
        import time

        time.sleep(0.01)
        write_config(tmp_path, 'session = ["POST", "BL"]\n')
        os.utime(tmp_path / "scistack.toml", None)
        assert schema_order.declared_level_order(tmp_path)["session"] == [
            "POST",
            "BL",
        ]

    def test_a_key_that_is_not_a_schema_key_is_reported(self, caplog):
        """A typo is otherwise completely silent: the declaration never
        matches and the default order comes out with nothing said."""
        import logging

        with caplog.at_level(logging.WARNING, logger="scidb"):
            schema_order.validate({"sesion": ["BL"]}, ["subject", "session"])
        assert "sesion" in caplog.text
        assert "not a schema key" in caplog.text


# --- applying it ------------------------------------------------------------


class TestOrderLevels:
    DECLARED = {"session": ["BL", "POST", "FU"]}

    def order(self, values, key="session"):
        return schema_order.order_levels(
            key, values, declared=self.DECLARED, fallback=sorted
        )

    def test_declared_levels_lead_in_declared_order(self):
        assert self.order(["FU", "POST", "BL"]) == ["BL", "POST", "FU"]

    def test_an_undeclared_level_is_appended_not_dropped(self):
        """The plan's own example: declare ["a","c"], observe "b", get
        ["a","c","b"] — visible at the end rather than silently missing."""
        assert schema_order.order_levels(
            "key1", ["b", "c", "a"], declared={"key1": ["a", "c"]}, fallback=sorted
        ) == ["a", "c", "b"]

    def test_several_undeclared_levels_keep_the_fallback_order(self):
        assert self.order(["z", "BL", "m"]) == ["BL", "m", "z"]

    def test_a_declared_level_with_no_data_is_skipped(self):
        assert self.order(["FU", "BL"]) == ["BL", "FU"]

    def test_an_undeclared_key_is_untouched(self):
        """Adding the file must change only the keys it names."""
        assert self.order(["b", "a"], key="subject") == ["a", "b"]

    def test_values_match_as_text_but_come_back_as_given(self):
        result = schema_order.order_levels(
            "trial", [3, 1, 2], declared={"trial": ["2", "1"]}, fallback=sorted
        )
        assert result == [2, 1, 3]


# --- through a real database ------------------------------------------------


class TestRowOrder:
    @pytest.fixture
    def db(self, tmp_path, monkeypatch):
        write_config(tmp_path, 'session = ["BL", "POST", "FU"]\n')
        # The manager reads the config of the project containing the CWD,
        # exactly as a user's script would.
        monkeypatch.chdir(tmp_path)
        _scifor.set_schema([])
        schema_order.clear_cache()
        database = configure_database(tmp_path / "order.duckdb", SCHEMA)
        for session in ["FU", "BL", "POST"]:
            Ordered.save(np.array([1.0]), subject="01", session=session)
        yield database
        _scifor.set_schema([])
        database.close()

    def test_the_manager_reads_the_declaration(self, db):
        assert db.dataset_schema_key_order == {"session": ["BL", "POST", "FU"]}

    def test_rows_come_back_in_the_declared_order(self, db):
        frame = db.load_all_as_df(Ordered, stringify_schema=True)
        assert frame["session"].tolist() == ["BL", "POST", "FU"]

    def test_an_undeclared_level_sorts_after_the_declared_ones(self, db):
        Ordered.save(np.array([2.0]), subject="01", session="EXTRA")
        frame = db.load_all_as_df(Ordered, stringify_schema=True)
        assert frame["session"].tolist() == ["BL", "POST", "FU", "EXTRA"]

    def test_an_undeclared_key_keeps_its_numeric_sort(self, db):
        """`subject` is undeclared, so the numeric-aware default still
        applies: 2 before 10, not "10" before "2"."""
        for subject in ["10", "2"]:
            Ordered.save(np.array([3.0]), subject=subject, session="BL")
        frame = db.load_all_as_df(Ordered, stringify_schema=True)
        subjects = [s for s in frame["subject"].tolist() if s != "01"]
        assert subjects.index("2") < subjects.index("10")

    def test_the_location_tree_draws_in_the_declared_order(self, db):
        """The picker's tree is a display surface like any other — it must not
        read BL, FU, POST beside an axis reading BL, POST, FU."""
        from scidb.locations import location_states

        tree = location_states(Ordered, db=db)
        subject = tree.roots[0]
        assert [child.value for child in subject.children] == ["BL", "POST", "FU"]
