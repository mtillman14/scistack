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


# --- read live, not snapshotted at open --------------------------------------


def _bump_mtime(path, seconds: float = 5.0):
    """Move the file's mtime forward: two writes inside one filesystem tick
    would otherwise look like one version of the file to the mtime cache."""
    import os

    stat = path.stat()
    os.utime(path, (stat.st_atime + seconds, stat.st_mtime + seconds))


class TestLiveReading:
    """The declaration used to be copied onto the DatabaseManager when it
    opened, so an edit to scistack.toml changed no table and no figure until
    a restart. It is asked for on every use now."""

    @pytest.fixture
    def db(self, tmp_path, monkeypatch):
        write_config(tmp_path, 'session = ["BL", "POST", "FU"]\n')
        monkeypatch.chdir(tmp_path)
        _scifor.set_schema([])
        database = configure_database(tmp_path / "live.duckdb", SCHEMA)
        for session in ["FU", "BL", "POST"]:
            Ordered.save(np.array([1.0]), subject="01", session=session)
        yield database
        _scifor.set_schema([])
        database.close()

    def test_an_edit_reaches_the_next_load_without_reopening(self, db, tmp_path):
        before = db.load_all_as_df(Ordered, stringify_schema=True)
        assert before["session"].tolist() == ["BL", "POST", "FU"]

        write_config(tmp_path, 'session = ["FU", "POST", "BL"]\n')
        _bump_mtime(tmp_path / "scistack.toml")

        after = db.load_all_as_df(Ordered, stringify_schema=True)
        assert after["session"].tolist() == ["FU", "POST", "BL"]
        assert db.dataset_schema_key_order == {"session": ["FU", "POST", "BL"]}

    def test_removing_the_table_restores_the_default_order(self, db, tmp_path):
        (tmp_path / "scistack.toml").write_text("modules = []\n", encoding="utf-8")
        _bump_mtime(tmp_path / "scistack.toml")
        frame = db.load_all_as_df(Ordered, stringify_schema=True)
        assert frame["session"].tolist() == ["BL", "FU", "POST"]

    def test_a_typo_added_mid_session_is_reported(self, db, tmp_path, caplog):
        import logging

        write_config(tmp_path, 'sesion = ["BL"]\n')
        _bump_mtime(tmp_path / "scistack.toml")
        with caplog.at_level(logging.WARNING, logger="scidb"):
            _ = db.dataset_schema_key_order
        assert "sesion" in caplog.text


class TestLocatingTheProject:
    """Which project's config applies: the one AT scifor.project_root (the
    pinned root, else the cwd) -- never above it, never next to the database
    -- and the answer is logged, including "none"."""

    @pytest.fixture(autouse=True)
    def _unpin(self):
        from scifor.pathinput import clear_project_root

        clear_project_root()
        yield
        clear_project_root()

    def test_the_pinned_project_root_wins_over_the_cwd(self, tmp_path, monkeypatch):
        from scifor.pathinput import set_project_root

        pinned = tmp_path / "pinned"
        elsewhere = tmp_path / "elsewhere"
        pinned.mkdir()
        elsewhere.mkdir()
        write_config(pinned, 'session = ["POST", "BL"]\n')
        write_config(elsewhere, 'session = ["BL", "POST"]\n')
        monkeypatch.chdir(elsewhere)
        set_project_root(pinned)
        assert schema_order.project_level_order() == {"session": ["POST", "BL"]}

    def test_a_config_above_the_root_is_not_this_projects(self, tmp_path, monkeypatch):
        """The upward walk is gone (one owner of the root: scifor.project_root)."""
        write_config(tmp_path, 'session = ["POST", "BL"]\n')
        below = tmp_path / "below"
        below.mkdir()
        monkeypatch.chdir(below)
        assert schema_order.project_level_order() == {}

    def test_the_database_folder_is_not_a_fallback(self, tmp_path, monkeypatch):
        """A config next to the database belongs to the project only when that
        folder IS the root; there is no second place to look."""
        project = tmp_path / "project"
        outside = tmp_path / "outside"
        project.mkdir()
        outside.mkdir()
        write_config(project, 'session = ["POST", "BL"]\n')
        monkeypatch.chdir(outside)
        assert schema_order.project_level_order() == {}

    def test_no_config_found_is_logged_once(self, tmp_path, monkeypatch, caplog):
        import logging

        monkeypatch.chdir(tmp_path)
        with caplog.at_level(logging.INFO, logger="scidb"):
            schema_order.project_level_order()
            schema_order.project_level_order()
        assert caplog.text.count("no project config found") == 1

    def test_the_config_in_use_is_logged(self, tmp_path, monkeypatch, caplog):
        import logging

        write_config(tmp_path, 'session = ["BL"]\n')
        monkeypatch.chdir(tmp_path)
        with caplog.at_level(logging.INFO, logger="scidb"):
            schema_order.project_level_order()
        assert "[schema_order] using" in caplog.text
        assert "working directory" in caplog.text

    def test_a_config_without_the_table_says_so(self, tmp_path, monkeypatch, caplog):
        import logging

        (tmp_path / "scistack.toml").write_text("modules = []\n", encoding="utf-8")
        monkeypatch.chdir(tmp_path)
        with caplog.at_level(logging.INFO, logger="scidb"):
            assert schema_order.project_level_order() == {}
        assert "has no [schema_keys] table" in caplog.text


# --- for_each: iteration order and the tables it hands out -------------------


class Summary(BaseVariable):
    pass


class TestForEachOrder:
    """``for_each(session=[])`` gets its levels from
    ``DatabaseManager.distinct_schema_values``, which used to be DuckDB's
    ``ORDER BY`` — alphabetical — so iteration, and every table assembled from
    it, ran BL, FU, POST whatever the project declared."""

    CODE = {"BL": 1.0, "POST": 2.0, "FU": 3.0}

    @pytest.fixture
    def db(self, tmp_path, monkeypatch):
        write_config(tmp_path, 'session = ["BL", "POST", "FU"]\n')
        monkeypatch.chdir(tmp_path)
        _scifor.set_schema([])
        database = configure_database(tmp_path / "foreach.duckdb", SCHEMA)
        # The value names the session, so the function can report which
        # location it was called for without asking for metadata.
        for session in ["FU", "BL", "POST"]:
            Ordered.save(self.CODE[session], subject="01", session=session)
        yield database
        _scifor.set_schema([])
        database.close()

    def test_distinct_values_follow_the_declaration(self, db):
        assert db.distinct_schema_values("session") == ["BL", "POST", "FU"]

    def test_iteration_runs_in_the_declared_order(self, db):
        from scidb import for_each

        seen: list[float] = []

        def record(value):
            seen.append(float(value))
            return value

        for_each(
            record,
            inputs={"value": Ordered},
            outputs=[Summary],
            subject=[],
            session=[],
        )
        assert seen == [1.0, 2.0, 3.0], "BL, POST, FU"

    def test_an_as_table_input_arrives_in_the_declared_order(self, db):
        from scidb import for_each

        frames = []

        def record(df):
            frames.append(df)
            return float(len(df))

        for_each(
            record,
            inputs={"df": Ordered},
            outputs=[Summary],
            as_table=["df"],
            subject=[],
        )
        assert len(frames) == 1
        assert [str(s) for s in frames[0]["session"]] == ["BL", "POST", "FU"]


class TestNewConfigMidSession:
    """A lookup that found NO config is trusted for ``LOCATE_TTL`` seconds —
    the lookup parses the candidate TOML, and a DatabaseManager
    asks on every sort. So a scistack.toml created after the database opened
    is found once that window passes, not on the very next call."""

    def test_a_new_config_is_found_after_the_ttl(self, tmp_path, monkeypatch):
        from scifor.pathinput import clear_project_root

        clear_project_root()
        monkeypatch.chdir(tmp_path)
        assert schema_order.project_level_order() == {}

        write_config(tmp_path, 'session = ["POST", "BL"]\n')
        # Within the window: still the cached "none found".
        assert schema_order.project_level_order() == {}

        import time as _time

        real = _time.monotonic
        monkeypatch.setattr(
            _time, "monotonic", lambda: real() + schema_order.LOCATE_TTL + 1
        )
        assert schema_order.project_level_order() == {"session": ["POST", "BL"]}
