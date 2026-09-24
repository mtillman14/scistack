"""Display aliases: ``[aliases]`` in the project config (scidb.aliases).

Stage 3 of ``.claude/plan-plot-text-sizes-and-aliases.md``. One entry per
thing — ``name`` for what the thing reads as, a ``levels`` sub-table for its
values. scidb owns the grammar: it reads, checks and renders it; plotting
applies it. See docs/claude/plot-text-and-labels.md.
"""

import logging

import numpy as np
import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

import scifor as _scifor
from scidb import BaseVariable, aliases, configure_database, schema_order

SCHEMA = ["subject", "session"]

EXAMPLE = """
[aliases.session]
name = "Session"

[aliases.session.levels]
"BL" = "Baseline"
"01" = "Visit 1"

[aliases."Demographics.Sex"]
name = "Sex"
levels = { "F" = "Female", "M" = "Male" }

[aliases.StepLength]
name = "Step length (cm)"
"""


class Aliased(BaseVariable):
    pass


@pytest.fixture(autouse=True)
def _clear_cache():
    aliases.clear_cache()
    schema_order.clear_cache()
    yield
    aliases.clear_cache()
    schema_order.clear_cache()


def write_config(root, body: str, *, pyproject: bool = False):
    if pyproject:
        body = body.replace("[aliases", "[tool.scistack.aliases")
        (root / "pyproject.toml").write_text(
            f"[project]\nname = 'x'\n{body}", encoding="utf-8"
        )
    else:
        (root / "scistack.toml").write_text(f"modules = []\n{body}", encoding="utf-8")
    return root / ("pyproject.toml" if pyproject else "scistack.toml")


def _bump_mtime(path, seconds: float = 5.0):
    import os

    stat = path.stat()
    os.utime(path, (stat.st_atime + seconds, stat.st_mtime + seconds))


EXPECTED = {
    "session": {"name": "Session", "levels": {"BL": "Baseline", "01": "Visit 1"}},
    "Demographics.Sex": {"name": "Sex", "levels": {"F": "Female", "M": "Male"}},
    "StepLength": {"name": "Step length (cm)"},
}


# --- reading -----------------------------------------------------------------


class TestReading:
    def test_scistack_toml(self, tmp_path):
        assert aliases.aliases_in(write_config(tmp_path, EXAMPLE)) == EXPECTED

    def test_pyproject_toml(self, tmp_path):
        assert aliases.aliases_in(write_config(tmp_path, EXAMPLE, pyproject=True)) == EXPECTED

    def test_no_table_is_empty(self, tmp_path):
        assert aliases.aliases_in(write_config(tmp_path, "")) == {}

    def test_padded_level_keys_stay_text(self, tmp_path):
        config = write_config(tmp_path, '[aliases.subject.levels]\n"01" = "P1"\n"1" = "Other"\n')
        assert aliases.aliases_in(config)["subject"]["levels"] == {"01": "P1", "1": "Other"}

    def test_a_number_where_text_is_expected_becomes_its_text(self, tmp_path):
        config = write_config(tmp_path, "[aliases.speed.levels]\nfast = 2\n")
        assert aliases.aliases_in(config) == {"speed": {"levels": {"fast": "2"}}}

    def test_one_bad_entry_does_not_cost_the_rest(self, tmp_path, caplog):
        body = (
            "[aliases]\n"
            'broken = "Session"\n'  # not a table
            "\n[aliases.session]\n"
            'name = "Session"\n'
            'lvls = { BL = "Baseline" }\n'  # not a setting
            "\n[aliases.StepLength]\n"
            'levels = "nope"\n'  # levels not a table -> entry left empty
        )
        with caplog.at_level(logging.WARNING, logger="scidb"):
            table = aliases.aliases_in(write_config(tmp_path, body))
        assert table == {"session": {"name": "Session"}}
        assert "aliases.broken" in caplog.text
        assert "aliases.session.lvls is not a setting" in caplog.text
        assert "aliases.StepLength.levels is not a table" in caplog.text

    def test_empty_aliases_are_dropped_quietly(self, tmp_path):
        config = write_config(tmp_path, '[aliases.session]\nname = ""\n[aliases.session.levels]\nBL = ""\n')
        assert aliases.aliases_in(config) == {}

    def test_an_edit_is_picked_up_without_manual_invalidation(self, tmp_path):
        config = write_config(tmp_path, '[aliases.session]\nname = "Session"\n')
        assert aliases.aliases_in(config)["session"]["name"] == "Session"
        write_config(tmp_path, '[aliases.session]\nname = "Visit"\n')
        _bump_mtime(config)
        assert aliases.aliases_in(config)["session"]["name"] == "Visit"

    def test_what_was_read_is_logged(self, tmp_path, caplog):
        with caplog.at_level(logging.INFO, logger="scidb"):
            aliases.aliases_in(write_config(tmp_path, EXAMPLE))
        assert "session (name, 2 level(s))" in caplog.text
        assert "StepLength (name)" in caplog.text


# --- checking ----------------------------------------------------------------


class TestValidate:
    def test_known_things_are_quiet(self):
        warnings = aliases.validate(
            EXPECTED, schema_keys=["subject", "session"], variables=["Demographics", "StepLength"]
        )
        assert warnings == []

    def test_synthetic_factors_are_known(self):
        table = {"ColName": {"levels": {"L_HAM": "Left hamstring"}}, "Variant": {"name": "Filter"}}
        assert aliases.validate(table, schema_keys=[], variables=[]) == []

    def test_a_typo_is_reported(self, caplog):
        with caplog.at_level(logging.WARNING, logger="scidb"):
            warnings = aliases.validate(
                {"sesion": {"name": "Session"}}, schema_keys=["session"], variables=[]
            )
        assert len(warnings) == 1
        assert "sesion" in caplog.text

    def test_a_column_of_an_unknown_variable_is_reported(self):
        warnings = aliases.validate(
            {"Demografics.Sex": {"name": "Sex"}}, schema_keys=[], variables=["Demographics"]
        )
        assert len(warnings) == 1

    def test_a_shared_name_is_reported(self):
        warnings = aliases.validate(
            {"session": {"name": "Session"}}, schema_keys=["session"], variables=["session"]
        )
        assert any("both a schema key and a variable" in w for w in warnings)

    def test_two_levels_with_one_alias_are_reported(self):
        warnings = aliases.validate(
            {"session": {"levels": {"BL": "Before", "PRE": "Before"}}},
            schema_keys=["session"],
            variables=[],
        )
        assert len(warnings) == 1
        assert "'BL' and 'PRE'" in warnings[0]


# --- writing -----------------------------------------------------------------


class TestRender:
    def test_round_trips_through_the_reader(self, tmp_path):
        text = aliases.render_aliases_table(EXPECTED)
        config = write_config(tmp_path, "\n" + text)
        assert aliases.aliases_in(config) == EXPECTED

    def test_keys_are_quoted_when_they_must_be(self):
        text = aliases.render_aliases_table(EXPECTED)
        assert '[aliases."Demographics.Sex"]' in text
        assert "[aliases.session]" in text
        # Level keys always quoted: "01" must stay text.
        assert '"01" = "Visit 1"' in text

    def test_awkward_text_is_escaped(self):
        table = {"x": {"name": 'say "hi" \\ there\n', "levels": {'a"b': "c\td"}}}
        parsed = tomllib.loads(aliases.render_aliases_table(table))
        assert parsed["aliases"]["x"] == {"name": 'say "hi" \\ there\n', "levels": {'a"b': "c\td"}}

    def test_nothing_renders_as_nothing(self):
        assert aliases.render_aliases_table({}) == ""
        assert aliases.render_aliases_table(None) == ""

    def test_a_pyproject_root(self):
        text = aliases.render_aliases_table(EXPECTED, root="tool.scistack.aliases")
        assert tomllib.loads(text)["tool"]["scistack"]["aliases"]["session"]["name"] == "Session"

    def test_what_the_reader_drops_is_never_written(self):
        text = aliases.render_aliases_table({"session": {"name": "S", "bogus": 1}})
        assert "bogus" not in text


class TestWithAlias:
    def test_set_and_clear_a_name(self):
        table = aliases.with_alias({}, "session", name="Session")
        assert table == {"session": {"name": "Session"}}
        assert aliases.with_alias(table, "session", name=None) == {}

    def test_set_and_clear_a_level(self):
        table = aliases.with_alias(EXPECTED, "session", level="POST", alias="After")
        assert table["session"]["levels"]["POST"] == "After"
        assert table["session"]["name"] == "Session"  # untouched
        cleared = aliases.with_alias(table, "session", level="POST", alias="")
        assert "POST" not in cleared["session"]["levels"]

    def test_the_input_is_not_mutated(self):
        before = {"session": {"name": "Session", "levels": {"BL": "Baseline"}}}
        aliases.with_alias(before, "session", level="BL", alias=None)
        assert before["session"]["levels"] == {"BL": "Baseline"}


# --- through a real database -------------------------------------------------


class TestThroughTheManager:
    @pytest.fixture
    def db(self, tmp_path, monkeypatch):
        write_config(tmp_path, EXAMPLE)
        monkeypatch.chdir(tmp_path)
        _scifor.set_schema([])
        database = configure_database(tmp_path / "aliases.duckdb", SCHEMA)
        Aliased.save(np.array([1.0]), subject="01", session="BL")
        yield database
        _scifor.set_schema([])
        database.close()

    def test_the_manager_reads_the_project_aliases(self, db):
        assert db.dataset_aliases == EXPECTED

    def test_an_edit_reaches_the_next_read_without_reopening(self, db, tmp_path):
        config = write_config(tmp_path, '[aliases.session]\nname = "Visit"\n')
        _bump_mtime(config)
        assert db.dataset_aliases == {"session": {"name": "Visit"}}

    def test_a_typo_is_reported_when_it_is_written(self, db, tmp_path, caplog):
        config = write_config(tmp_path, '[aliases.sesion]\nname = "Visit"\n')
        _bump_mtime(config)
        with caplog.at_level(logging.WARNING, logger="scidb"):
            _ = db.dataset_aliases
        assert "sesion" in caplog.text

    def test_a_variable_name_is_known(self, db, tmp_path, caplog):
        config = write_config(tmp_path, '[aliases.Aliased]\nname = "A"\n')
        _bump_mtime(config)
        with caplog.at_level(logging.WARNING, logger="scidb"):
            _ = db.dataset_aliases
        assert "Aliased" not in caplog.text
