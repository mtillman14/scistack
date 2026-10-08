"""Mark colours: ``[colors]`` in the project config (scidb.colors).

Stage 2 of ``.claude/plan-custom-mark-colors.md``. A string directly under
``[colors]`` is a setting (only ``default``); a table is a thing, keyed by
level TEXT. scidb owns the SHAPE; whether the text is a colour is
scistackplot's ``parse_color``. See docs/claude/plot-colors.md.
"""

import logging

import pytest

from scidb import colors, schema_order

EXAMPLE = """
[colors]
default = "#333333"

[colors.session]
"BL" = "#0072b2"
"01" = "#d55e00"

[colors."Demographics.Sex"]
"F" = "#cc79a7"
"""

EXPECTED = {
    "default": "#333333",
    "session": {"BL": "#0072b2", "01": "#d55e00"},
    "Demographics.Sex": {"F": "#cc79a7"},
}


@pytest.fixture(autouse=True)
def _clear_cache():
    colors.clear_cache()
    schema_order.clear_cache()
    yield
    colors.clear_cache()
    schema_order.clear_cache()


def write_config(root, body: str, *, pyproject: bool = False):
    if pyproject:
        body = body.replace("[colors", "[tool.scistack.colors")
        (root / "pyproject.toml").write_text(f"[project]\nname = 'x'\n{body}", encoding="utf-8")
    else:
        (root / "scistack.toml").write_text(f"modules = []\n{body}", encoding="utf-8")
    return root / ("pyproject.toml" if pyproject else "scistack.toml")


def _bump_mtime(path, seconds: float = 5.0):
    import os

    stat = path.stat()
    os.utime(path, (stat.st_atime + seconds, stat.st_mtime + seconds))


class TestReading:
    def test_scistack_toml(self, tmp_path):
        assert colors.colors_in(write_config(tmp_path, EXAMPLE)) == EXPECTED

    def test_a_pyproject_is_never_config(self, tmp_path):
        """2026-10-08: config lives only in scistack.toml; [tool.scistack]
        in a pyproject.toml is not read."""
        assert colors.colors_in(write_config(tmp_path, EXAMPLE, pyproject=True)) == {}

    def test_no_table_is_empty(self, tmp_path):
        assert colors.colors_in(write_config(tmp_path, "")) == {}

    def test_padded_level_keys_stay_text(self, tmp_path):
        config = write_config(tmp_path, '[colors.subject]\n"01" = "#111111"\n"1" = "#222222"\n')
        assert colors.colors_in(config)["subject"] == {"01": "#111111", "1": "#222222"}

    def test_a_thing_named_default_is_a_thing(self, tmp_path):
        config = write_config(tmp_path, '[colors.default]\n"a" = "#111111"\n')
        table = colors.colors_in(config)
        assert colors.things_of(table) == {"default": {"a": "#111111"}}
        assert colors.default_of(table) is None

    def test_one_bad_entry_does_not_cost_the_rest(self, tmp_path, caplog):
        body = (
            "[colors]\n"
            'palette = "viridis"\n'  # not a setting
            "\n[colors.session]\n"
            '"BL" = "#0072b2"\n'
            '"FU" = 3\n'  # not text
        )
        with caplog.at_level(logging.WARNING, logger="scidb"):
            table = colors.colors_in(write_config(tmp_path, body))
        assert table == {"session": {"BL": "#0072b2"}}
        assert "colors.palette is not a setting" in caplog.text
        assert "colors.session.FU is int" in caplog.text

    def test_colour_text_is_kept_as_written(self, tmp_path):
        # Parsing is scistackplot's: a bad colour is scidb's business only as text.
        config = write_config(tmp_path, '[colors.session]\n"BL" = "not-a-colour"\n')
        assert colors.colors_in(config) == {"session": {"BL": "not-a-colour"}}

    def test_empty_colours_are_dropped_quietly(self, tmp_path):
        config = write_config(tmp_path, '[colors]\ndefault = ""\n[colors.session]\nBL = ""\n')
        assert colors.colors_in(config) == {}

    def test_an_edit_is_picked_up_without_manual_invalidation(self, tmp_path):
        config = write_config(tmp_path, '[colors.session]\n"BL" = "#111111"\n')
        assert colors.colors_in(config)["session"]["BL"] == "#111111"
        write_config(tmp_path, '[colors.session]\n"BL" = "#222222"\n')
        _bump_mtime(config)
        assert colors.colors_in(config)["session"]["BL"] == "#222222"

    def test_what_was_read_is_logged(self, tmp_path, caplog):
        with caplog.at_level(logging.INFO, logger="scidb"):
            colors.colors_in(write_config(tmp_path, EXAMPLE))
        assert "default #333333" in caplog.text
        assert "session (2 level(s))" in caplog.text


class TestValidate:
    def test_known_things_are_quiet(self):
        warnings = colors.validate(
            EXPECTED, schema_keys=["session"], variables=["Demographics"]
        )
        assert warnings == []

    def test_a_typo_is_reported(self, caplog):
        with caplog.at_level(logging.WARNING, logger="scidb"):
            warnings = colors.validate(
                {"sesion": {"BL": "#111111"}}, schema_keys=["session"], variables=[]
            )
        assert len(warnings) == 1
        assert "sesion" in caplog.text

    def test_the_default_setting_is_not_a_thing(self):
        assert colors.validate({"default": "#111111"}, schema_keys=[], variables=[]) == []


class TestRender:
    def test_round_trips_through_the_reader(self, tmp_path):
        text = colors.render_colors_table(EXPECTED)
        config = write_config(tmp_path, "\n" + text)
        assert colors.colors_in(config) == EXPECTED

    def test_default_sits_under_the_section_header_first(self):
        text = colors.render_colors_table(EXPECTED)
        assert text.startswith('[colors]\ndefault = "#333333"')
        assert '[colors."Demographics.Sex"]' in text
        assert '"01" = "#d55e00"' in text

    def test_nothing_renders_as_nothing(self):
        assert colors.render_colors_table({}) == ""
        assert colors.render_colors_table(None) == ""


class TestWithColor:
    def test_set_and_clear_a_level(self):
        table = colors.with_color({}, "session", level="BL", color="#111111")
        assert table == {"session": {"BL": "#111111"}}
        table = colors.with_color(table, "session", level="BL", color=None)
        assert table == {}

    def test_set_and_clear_the_default(self):
        table = colors.with_color(EXPECTED, None, color="#abcdef")
        assert table["default"] == "#abcdef"
        assert table["session"] == EXPECTED["session"]
        assert "default" not in colors.with_color(table, None, color="")

    def test_a_level_is_required_for_a_thing(self):
        with pytest.raises(ValueError):
            colors.with_color({}, "session", color="#111111")
