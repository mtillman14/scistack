"""
The TOML entities file: parsing, construction, per-entry error isolation,
and the write half (span location + splice).

Ground truth for ``.claude/plan-entities-toml-26-08-31.md`` Stage 1 and for
the format documented in ``scidb/src/scidb/entities.py``. The properties
these lock down, in order of how much would silently break without them:

1. **Values are never re-parsed** -- ``"01"`` stays ``"01"``
   (``feedback_zero_padded_schema_keys``).
2. **One bad entry does not take the file down** -- the failure mode of the
   exec'd ``.py`` entities file this format replaces.
3. **An edit preserves everything outside the entry's own value** --
   comments, blank lines, neighbouring declarations.
"""

from __future__ import annotations

import os
import textwrap

import pytest

from scidb import entities
from scidb.parameter import Parameter
from scidb.variable import BaseVariable
from scifor import EachOf, PathInput


def write(tmp_path, text: str):
    """Write *text* as the entities file. The leading newline a triple-quoted
    literal starts with is stripped, so a declaration on the first written
    line really is line 1 -- these tests assert on reported line numbers."""
    path = tmp_path / "scistack_entities.toml"
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


class TestLoad:
    def test_missing_file_is_empty_not_an_error(self, tmp_path):
        """A project that has not created an entity yet is not a broken
        project."""
        result = entities.load(tmp_path / "nope.toml")
        assert result.names() == []
        assert result.errors == []

    def test_variables_become_basevariable_subclasses(self, tmp_path):
        path = write(tmp_path, """
            variables = ["StepLength", "EmgEnvelope"]
        """)
        result = entities.load(path)

        assert sorted(result.variables) == ["EmgEnvelope", "StepLength"]
        cls = result.variables["StepLength"]
        assert issubclass(cls, BaseVariable)
        assert cls.__name__ == "StepLength"
        # Registered exactly as `class StepLength(BaseVariable)` would be --
        # this is what makes the type usable for save/load.
        assert BaseVariable._all_subclasses["StepLength"] is cls

    def test_empty_array_is_a_parameter_with_no_values(self, tmp_path):
        """`NAME = []` is declared-but-not-yet-valued, not a broken entry.
        It is what the GUI writes for a new Parameter, which used to be a
        placeholder 0 indistinguishable from a real declared value."""
        path = write(tmp_path, """
            [parameters]
            CUTOFF_HZ = []
        """)
        result = entities.load(path)

        assert result.errors == []
        param = result.parameters["CUTOFF_HZ"]
        assert isinstance(param, Parameter)
        assert param.values == []

    def test_empty_parameter_round_trips(self, tmp_path):
        rendered = entities.render_parameter_value([])
        assert rendered == "[]"
        path = write(tmp_path, f"""
            [parameters]
            CUTOFF_HZ = {rendered}
        """)
        assert entities.load(path).parameters["CUTOFF_HZ"].values == []

    def test_variable_class_keeps_metaclass_behaviour(self, tmp_path):
        """Built through VariableMeta, so class-level comparison still
        builds a filter rather than returning a bool."""
        path = write(tmp_path, 'variables = ["Side"]\n')
        cls = entities.load(path).variables["Side"]

        assert (cls == cls) is True  # class identity preserved
        assert not isinstance(cls == "L", bool)  # ...but a filter otherwise

    def test_scalar_parameter_has_one_value(self, tmp_path):
        path = write(tmp_path, """
            [parameters]
            SAMPLING_RATE_HZ = 1000
        """)
        param = entities.load(path).parameters["SAMPLING_RATE_HZ"]

        assert isinstance(param, Parameter)
        assert param.values == [1000]
        assert param.value == 1000

    def test_array_parameter_fans_out(self, tmp_path):
        path = write(tmp_path, """
            [parameters]
            WINDOW_SECONDS = [10, 20, 30]
        """)
        assert entities.load(path).parameters["WINDOW_SECONDS"].values == [10, 20, 30]

    def test_zero_padded_values_stay_strings(self, tmp_path):
        """The property the format exists to protect: no eval, no literal
        re-parse, so "01" cannot become 1."""
        path = write(tmp_path, """
            [parameters]
            SUBJECT_IDS = ["01", "02"]
        """)
        assert entities.load(path).parameters["SUBJECT_IDS"].values == ["01", "02"]

    def test_inline_table_is_the_value_not_options(self, tmp_path):
        path = write(tmp_path, """
            [parameters]
            CONFIG = { fld1 = 1, fld2 = 2 }
        """)
        param = entities.load(path).parameters["CONFIG"]

        assert param.values == [{"fld1": 1, "fld2": 2}]

    def test_nested_array_is_one_list_valued_parameter(self, tmp_path):
        """The outer array is always alternatives, so a list-valued
        Parameter nests -- one value that happens to be a list, not three."""
        path = write(tmp_path, """
            [parameters]
            X = [[1, 2, 3]]
        """)
        assert entities.load(path).parameters["X"].values == [[1, 2, 3]]

    def test_string_path_input(self, tmp_path):
        path = write(tmp_path, """
            [path_inputs]
            EMG_FILE = "{subject}/{session}_emg.csv"
        """)
        pi = entities.load(path).path_inputs["EMG_FILE"]

        assert isinstance(pi, PathInput)
        assert pi.path_template == "{subject}/{session}_emg.csv"
        assert pi.root_folder is None

    def test_table_path_input_carries_root_folder(self, tmp_path):
        path = write(tmp_path, """
            [path_inputs]
            RAW = { template = "{subject}/raw.csv", root_folder = "/data/raw" }
        """)
        pi = entities.load(path).path_inputs["RAW"]

        assert pi.path_template == "{subject}/raw.csv"
        assert str(pi.root_folder) == "/data/raw"

    def test_array_path_input_is_an_eachof(self, tmp_path):
        """The alternate-template form portability's importer writes."""
        path = write(tmp_path, """
            [path_inputs]
            RAW = ["a/{s}.csv", { template = "b/{s}.csv", root_folder = "/data" }]
        """)
        obj = entities.load(path).path_inputs["RAW"]

        assert isinstance(obj, EachOf)
        assert [a.path_template for a in obj.alternatives] == ["a/{s}.csv", "b/{s}.csv"]

    def test_declaration_lines_are_recorded(self, tmp_path):
        """Feeds the GUI's "declared at file:line"."""
        path = write(tmp_path, """
            variables = ["A"]

            [parameters]
            B = 1
            C = 2
        """)
        result = entities.load(path)

        assert result.lines["B"] == 4
        assert result.lines["C"] == 5

    def test_parameter_records_its_source_location(self, tmp_path):
        path = write(tmp_path, """
            [parameters]
            B = 1
        """)
        param = entities.load(path).parameters["B"]

        assert param.source_file == str(path)
        assert param.source_line == 2


class TestErrorIsolation:
    """One bad entry is rejected with its name and line; every good entry
    in the same file still loads."""

    def test_bad_entry_does_not_take_down_the_file(self, tmp_path):
        path = write(tmp_path, """
            [path_inputs]
            GOOD = "a/{s}.csv"
            BAD = { template = "b.csv", nonsense = 1 }
            ALSO_GOOD = "c/{s}.csv"
        """)
        result = entities.load(path)

        assert sorted(result.path_inputs) == ["ALSO_GOOD", "GOOD"]
        assert [e.name for e in result.errors] == ["BAD"]
        assert "nonsense" in result.errors[0].message
        assert result.errors[0].line == 3

    def test_path_input_without_template_is_rejected(self, tmp_path):
        path = write(tmp_path, """
            [path_inputs]
            BAD = { root_folder = "/data" }
        """)
        result = entities.load(path)

        assert result.path_inputs == {}
        assert "template" in result.errors[0].message

    def test_invalid_identifier_is_rejected(self, tmp_path):
        path = write(tmp_path, """
            variables = ["not-an-identifier", "Fine"]
        """)
        result = entities.load(path)

        assert list(result.variables) == ["Fine"]
        assert "not a valid Python identifier" in result.errors[0].message

    def test_duplicate_name_across_sections_is_rejected(self, tmp_path):
        """The name is how every consumer refers to the entity, so one name
        meaning two things has no resolution."""
        path = write(tmp_path, """
            variables = ["X"]

            [parameters]
            X = 1
        """)
        result = entities.load(path)

        assert "X" in result.variables
        assert result.parameters == {}
        assert "duplicate" in result.errors[0].message

    def test_variables_below_a_section_header_is_reported(self, tmp_path):
        """TOML binds it to the preceding table, so every name in it would
        otherwise vanish with nothing logged."""
        path = write(tmp_path, """
            [parameters]
            A = 1
            variables = ["StepLength"]
        """)
        result = entities.load(path)

        assert result.variables == {}
        assert any("move it above" in e.message for e in result.errors)

    def test_unparseable_toml_yields_one_located_error(self, tmp_path):
        path = write(tmp_path, """
            [parameters
            A = 1
        """)
        result = entities.load(path)

        assert result.names() == []
        assert len(result.errors) == 1
        assert "invalid TOML" in result.errors[0].message


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


class TestRender:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (1000, "1000"),
            (True, "true"),
            (False, "false"),
            ("01", '"01"'),
            ("with \"quotes\"", '"with \\"quotes\\""'),
            (r"C:\data", '"C:\\\\data"'),
            ([1, "a"], '[1, "a"]'),
            ({"fld1": 1}, "{ fld1 = 1 }"),
        ],
    )
    def test_render_value(self, value, expected):
        assert entities.render_value(value) == expected

    def test_bool_is_not_written_as_int(self):
        """bool IS an int in Python; writing True as 1 would read back as a
        different type."""
        assert entities.render_value(True) == "true"

    def test_none_is_refused(self):
        with pytest.raises(ValueError, match="no TOML representation|no null"):
            entities.render_value(None)

    def test_single_parameter_value_renders_as_a_scalar(self):
        assert entities.render_parameter_value([1000]) == "1000"

    def test_multiple_parameter_values_render_as_an_array(self):
        assert entities.render_parameter_value([10, 20]) == "[10, 20]"

    def test_path_input_without_root_is_a_bare_string(self):
        assert entities.render_path_input_value("a/{s}.csv") == '"a/{s}.csv"'

    def test_path_input_with_root_is_a_table(self):
        assert entities.render_path_input_value("a.csv", "/data") == (
            '{ template = "a.csv", root_folder = "/data" }'
        )

    def test_alternates_render_as_an_array(self):
        rendered = entities.render_path_input_value(
            "a.csv", None, [{"template": "b.csv", "root_folder": "/d"}]
        )
        assert rendered == '["a.csv", { template = "b.csv", root_folder = "/d" }]'


# ---------------------------------------------------------------------------
# Locating and writing
# ---------------------------------------------------------------------------


class TestSpans:
    def test_finds_a_simple_value(self):
        text = "[parameters]\nA = 1000\n"
        span = entities.find_entry_span(text, "parameters", "A")

        assert text[span.start : span.end] == "1000"

    def test_span_excludes_a_trailing_comment(self):
        text = "[parameters]\nA = 1000  # the rate\n"
        span = entities.find_entry_span(text, "parameters", "A")

        assert text[span.start : span.end] == "1000"

    def test_span_covers_a_multi_line_array(self):
        text = "[parameters]\nA = [\n    1,\n    2,\n]\nB = 3\n"
        span = entities.find_entry_span(text, "parameters", "A")

        assert text[span.start : span.end] == "[\n    1,\n    2,\n]"

    def test_same_name_in_two_sections_is_not_confused(self):
        text = "[parameters]\nA = 1\n\n[path_inputs]\nA = \"x.csv\"\n"

        p = entities.find_entry_span(text, "parameters", "A")
        pi = entities.find_entry_span(text, "path_inputs", "A")

        assert text[p.start : p.end] == "1"
        assert text[pi.start : pi.end] == '"x.csv"'

    def test_missing_entry_is_none(self):
        assert entities.find_entry_span("[parameters]\n", "parameters", "A") is None


class TestUpsert:
    def test_replacing_a_value_preserves_everything_else(self):
        text = textwrap.dedent("""\
            # keep me
            [parameters]
            A = 1  # and me
            B = 2
            """)
        updated = entities.upsert_entry(text, "parameters", "A", "5")

        assert updated == textwrap.dedent("""\
            # keep me
            [parameters]
            A = 5  # and me
            B = 2
            """)

    def test_new_entry_lands_at_the_end_of_its_section(self):
        text = "[parameters]\nA = 1\n\n[path_inputs]\nP = \"x.csv\"\n"
        updated = entities.upsert_entry(text, "parameters", "B", "2")

        assert updated == "[parameters]\nA = 1\nB = 2\n\n[path_inputs]\nP = \"x.csv\"\n"

    def test_section_is_created_when_absent(self):
        text = 'variables = ["A"]\n'
        updated = entities.upsert_entry(text, "parameters", "B", "2")

        assert updated == 'variables = ["A"]\n\n[parameters]\nB = 2\n'

    def test_round_trip_through_the_loader(self, tmp_path):
        text = entities.initial_text()
        text = entities.upsert_entry(
            text, "parameters", "WINDOW", entities.render_parameter_value([10, 20])
        )
        text = entities.upsert_entry(
            text,
            "path_inputs",
            "RAW",
            entities.render_path_input_value("{s}/raw.csv", "/data"),
        )
        text = entities.add_variable(text, "StepLength")
        path = tmp_path / "scistack_entities.toml"
        path.write_text(text, encoding="utf-8")

        result = entities.load(path)

        assert result.errors == []
        assert result.parameters["WINDOW"].values == [10, 20]
        assert result.path_inputs["RAW"].path_template == "{s}/raw.csv"
        assert "StepLength" in result.variables

    def test_scalar_to_array_is_one_splice(self, tmp_path):
        """Adding a value changes the form but not the entry -- the whole
        RHS is rewritten in place (entity-editability-model.md D6)."""
        text = "[parameters]\nA = 10\n"
        updated = entities.upsert_entry(
            text, "parameters", "A", entities.render_parameter_value([10, 20])
        )

        assert updated == "[parameters]\nA = [10, 20]\n"


class TestAddVariable:
    def test_appends_to_a_multi_line_array_keeping_comments(self):
        text = 'variables = [\n    "A",  # first\n]\n'
        updated = entities.add_variable(text, "B")

        assert updated == 'variables = [\n    "A",  # first\n    "B",\n]\n'

    def test_single_line_array_is_rewritten_multi_line(self):
        text = 'variables = ["A"]\n'
        updated = entities.add_variable(text, "B")

        assert updated == 'variables = [\n    "A",\n    "B",\n]\n'

    def test_empty_array(self):
        text = "variables = []\n\n[parameters]\n"
        updated = entities.add_variable(text, "A")

        assert updated == 'variables = [\n    "A",\n]\n\n[parameters]\n'

    def test_already_declared_is_a_no_op(self):
        text = 'variables = ["A"]\n'
        assert entities.add_variable(text, "A") == text

    def test_missing_key_is_added_above_the_first_section(self):
        """Below a section header TOML would bind it to that table."""
        text = "[parameters]\nA = 1\n"
        updated = entities.add_variable(text, "X")

        assert updated == 'variables = [\n    "X",\n]\n\n[parameters]\nA = 1\n'


# ---------------------------------------------------------------------------
# Project resolution and the attribute namespace
# ---------------------------------------------------------------------------


class TestProjectResolution:
    @pytest.fixture(autouse=True)
    def _clear(self):
        entities.clear_cache()
        yield
        entities.clear_cache()

    def test_entities_file_key_is_honoured(self, tmp_path):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "src/things.toml"\n', encoding="utf-8"
        )
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "things.toml").write_text("[parameters]\nA = 1\n")

        assert entities.entities_path(tmp_path) == tmp_path / "src" / "things.toml"

    def test_conventional_path_used_when_key_absent_and_file_exists(self, tmp_path):
        (tmp_path / "scistack.toml").write_text("modules = []\n", encoding="utf-8")
        (tmp_path / "src").mkdir()
        conventional = tmp_path / "src" / "scistack_entities.toml"
        conventional.write_text("[parameters]\nA = 1\n", encoding="utf-8")

        assert entities.entities_path(tmp_path) == conventional

    def test_no_guessing_when_the_conventional_file_is_absent(self, tmp_path):
        """Guessing a path that doesn't exist would report a missing-file
        error against a file the user never asked for."""
        (tmp_path / "scistack.toml").write_text("modules = []\n", encoding="utf-8")

        assert entities.entities_path(tmp_path) is None

    def test_no_config_at_all(self, tmp_path):
        assert entities.entities_path(tmp_path) is None

    def test_empty_key_is_an_explicit_opt_out(self, tmp_path):
        """``entities_file = ""`` must NOT fall through to the conventional
        path -- it is what the GUI's "clear entities file" writes, and the
        whole point is to stop reading a file sitting at that very path."""
        (tmp_path / "scistack.toml").write_text(
            'entities_file = ""\n', encoding="utf-8"
        )
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "scistack_entities.toml").write_text(
            "[parameters]\nA = 1\n", encoding="utf-8"
        )

        assert entities.entities_path(tmp_path) is None
        assert entities.load_for_project(tmp_path).parameters == {}


class TestResolveEntitiesPath:
    """The rule on its own, for callers that already located the project
    config and parsed its section (``scistack_gui.config.load_config``)."""

    def test_declared_key_wins_and_resolves_against_the_root(self, tmp_path):
        resolved = entities.resolve_entities_path(
            tmp_path, {"entities_file": "src/things.toml"}
        )

        assert resolved == tmp_path / "src" / "things.toml"

    def test_conventional_fallback_only_when_the_file_exists(self, tmp_path):
        assert entities.resolve_entities_path(tmp_path, {}) is None

        (tmp_path / "src").mkdir()
        conventional = tmp_path / "src" / "scistack_entities.toml"
        conventional.write_text("[parameters]\nA = 1\n", encoding="utf-8")

        assert entities.resolve_entities_path(tmp_path, {}) == conventional

    def test_empty_key_opts_out_even_when_the_file_exists(self, tmp_path):
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "scistack_entities.toml").write_text(
            "[parameters]\nA = 1\n", encoding="utf-8"
        )

        assert entities.resolve_entities_path(tmp_path, {"entities_file": ""}) is None

    def test_missing_section_is_treated_as_empty(self, tmp_path):
        assert entities.resolve_entities_path(tmp_path, None) is None

    @pytest.mark.skipif(os.sep == "\\", reason="POSIX-only reinterpretation")
    def test_a_windows_written_key_still_finds_the_file(self, tmp_path):
        """``entities_file = "src\\scistack_entities.toml"`` -- written by a
        Windows session, read here. Naively joined it names a backslashed
        file in the project ROOT, which is where the MATLAB classdef stub
        directory then went too (it sits beside the entities file)."""
        (tmp_path / "src").mkdir()
        real = tmp_path / "src" / "scistack_entities.toml"
        real.write_text("[parameters]\nA = 1\n", encoding="utf-8")

        resolved = entities.resolve_entities_path(
            tmp_path, {"entities_file": "src\\scistack_entities.toml"}
        )

        assert resolved == real
        assert resolved.parent == tmp_path / "src"

    def test_opt_out_predicate_distinguishes_cleared_from_unset(self, tmp_path):
        assert entities.is_entities_opt_out({"entities_file": ""}) is True
        assert entities.is_entities_opt_out({}) is False
        assert entities.is_entities_opt_out(None) is False
        assert entities.is_entities_opt_out({"entities_file": "e.toml"}) is False

    def test_load_for_project_reads_through_the_config(self, tmp_path):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "e.toml"\n', encoding="utf-8"
        )
        (tmp_path / "e.toml").write_text("[parameters]\nRATE = 1000\n", encoding="utf-8")

        result = entities.load_for_project(tmp_path)

        assert result.parameters["RATE"].value == 1000

    def test_cache_is_invalidated_by_an_edit(self, tmp_path):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "e.toml"\n', encoding="utf-8"
        )
        target = tmp_path / "e.toml"
        target.write_text("[parameters]\nRATE = 1000\n", encoding="utf-8")
        assert entities.load_for_project(tmp_path).parameters["RATE"].value == 1000

        target.write_text("[parameters]\nRATE = 2000\n", encoding="utf-8")
        entities.clear_cache()  # mtime resolution is coarser than this test

        assert entities.load_for_project(tmp_path).parameters["RATE"].value == 2000

    def test_attribute_access_resolves_a_declared_entity(self, tmp_path, monkeypatch):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "e.toml"\n', encoding="utf-8"
        )
        (tmp_path / "e.toml").write_text(
            "[parameters]\nWINDOW_SECONDS = [10, 20]\n", encoding="utf-8"
        )
        monkeypatch.chdir(tmp_path)

        assert entities.WINDOW_SECONDS.values == [10, 20]

    def test_unknown_attribute_lists_what_is_declared(self, tmp_path, monkeypatch):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "e.toml"\n', encoding="utf-8"
        )
        (tmp_path / "e.toml").write_text("[parameters]\nA = 1\n", encoding="utf-8")
        monkeypatch.chdir(tmp_path)

        with pytest.raises(AttributeError, match="Declared: A"):
            entities.NOT_DECLARED
