"""The entities-file grammar, with no scidb: parse results, rendering,
spans, text edits and the write gate.

``scifor.project.entities_file`` parses TOML into plain frozen dataclasses
(``DeclaredEntities``); ``scidb.project.entities`` turns those into Variables,
Parameters and PathInputs. These tests pin the grammar half on its own, so a
parse regression shows up here without a database or a model in the way.
End-to-end tests (TOML text -> materialized objects) are scidb's
``tests/test_entities_toml.py`` and ``tests/test_entities_seam.py``.
"""

from __future__ import annotations

import textwrap

import pytest

from scifor.project import entities_file
from scifor.project.entities_file import (
    DeclaredParameter,
    DeclaredPathInput,
    DeclaredVariable,
    PathInputArm,
    parse_entities,
    read_entities_file,
)


def parse(text: str):
    return parse_entities(textwrap.dedent(text).lstrip("\n"), "scistack_entities.toml")


# ---------------------------------------------------------------------------
# Parse results
# ---------------------------------------------------------------------------


class TestParse:
    def test_missing_file_is_empty_not_an_error(self, tmp_path):
        declared = read_entities_file(tmp_path / "absent.toml")
        assert declared.names() == []
        assert declared.errors == ()

    def test_every_kind_is_plain_data(self):
        declared = parse(
            """
            variables = ["StepLength"]

            [parameters]
            RATE = 1000
            WINDOW = [10, 20]

            [path_inputs]
            RAW = "{subject}/raw.csv"
            ALT = ["{s}/a.csv", { template = "{s}/b.csv", root_folder = "/d" }]
            """
        )
        assert declared.errors == ()
        assert declared.variables == (DeclaredVariable("StepLength", 1),)
        assert declared.parameters == (
            DeclaredParameter("RATE", (1000,), 4),
            DeclaredParameter("WINDOW", (10, 20), 5),
        )
        assert declared.path_inputs == (
            DeclaredPathInput("RAW", (PathInputArm("{subject}/raw.csv"),), 8),
            DeclaredPathInput(
                "ALT",
                (PathInputArm("{s}/a.csv"), PathInputArm("{s}/b.csv", root_folder="/d")),
                9,
            ),
        )
        assert declared.names() == ["StepLength", "RATE", "WINDOW", "RAW", "ALT"]

    def test_zero_padded_values_stay_strings(self):
        declared = parse('[parameters]\nIDS = ["01", "02"]\n')
        assert declared.parameters[0].values == ("01", "02")

    def test_empty_array_is_a_parameter_with_no_values(self):
        declared = parse("[parameters]\nCUTOFF = []\n")
        assert declared.errors == ()
        assert declared.parameters[0].values == ()

    def test_lone_alias_spelling_is_normalized_to_a_list(self):
        declared = parse(
            """
            [path_inputs]
            X = { template = "{session}/x.csv", aliases = { session = { BL = "Baseline" } } }
            """
        )
        assert declared.path_inputs[0].arms[0].aliases == {"session": {"BL": ["Baseline"]}}

    def test_duplicate_name_across_sections_is_rejected(self):
        declared = parse(
            """
            variables = ["A"]

            [parameters]
            A = 1
            """
        )
        assert [v.name for v in declared.variables] == ["A"]
        assert declared.parameters == ()
        assert "duplicate" in declared.errors[0].message

    def test_unknown_path_input_key_rejects_only_that_entry(self):
        declared = parse(
            """
            [path_inputs]
            BAD = { template = "x.csv", regex = ".*" }
            GOOD = "y.csv"
            """
        )
        assert [p.name for p in declared.path_inputs] == ["GOOD"]
        assert declared.errors[0].name == "BAD"
        assert declared.errors[0].line == 2

    def test_unparseable_toml_yields_one_located_error(self):
        declared = parse('variables = ["A"\n')
        assert declared.names() == []
        assert len(declared.errors) == 1
        assert "invalid TOML" in declared.errors[0].message

    def test_library_schema_keys(self):
        declared = parse('[library]\nschema_keys = ["subject", "trial"]\n')
        assert declared.library_schema_keys == ("subject", "trial")
        assert parse("variables = []\n").library_schema_keys is None

    def test_parse_logs_under_the_entities_prefix(self, caplog):
        with caplog.at_level("DEBUG", logger="scidb"):
            parse('variables = ["A"]\n')
        assert "[entities] Parsed" in caplog.text
        assert "[entities] Declared variable A" in caplog.text


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
        assert entities_file.render_value(value) == expected

    def test_bool_is_not_written_as_int(self):
        """bool IS an int in Python; writing True as 1 would read back as a
        different type."""
        assert entities_file.render_value(True) == "true"

    def test_none_is_refused(self):
        with pytest.raises(ValueError, match="no TOML representation|no null"):
            entities_file.render_value(None)

    def test_single_parameter_value_renders_as_a_scalar(self):
        assert entities_file.render_parameter_value([1000]) == "1000"

    def test_multiple_parameter_values_render_as_an_array(self):
        assert entities_file.render_parameter_value([10, 20]) == "[10, 20]"

    def test_path_input_without_root_is_a_bare_string(self):
        assert entities_file.render_path_input_value("a/{s}.csv") == '"a/{s}.csv"'

    def test_path_input_with_root_is_a_table(self):
        assert entities_file.render_path_input_value("a.csv", "/data") == (
            '{ template = "a.csv", root_folder = "/data" }'
        )

    def test_alternates_render_as_an_array(self):
        rendered = entities_file.render_path_input_value(
            "a.csv", None, [{"template": "b.csv", "root_folder": "/d"}]
        )
        assert rendered == '["a.csv", { template = "b.csv", root_folder = "/d" }]'


# ---------------------------------------------------------------------------
# Locating and writing
# ---------------------------------------------------------------------------


class TestSpans:
    def test_finds_a_simple_value(self):
        text = "[parameters]\nA = 1000\n"
        span = entities_file.find_entry_span(text, "parameters", "A")

        assert text[span.start : span.end] == "1000"

    def test_span_excludes_a_trailing_comment(self):
        text = "[parameters]\nA = 1000  # the rate\n"
        span = entities_file.find_entry_span(text, "parameters", "A")

        assert text[span.start : span.end] == "1000"

    def test_span_covers_a_multi_line_array(self):
        text = "[parameters]\nA = [\n    1,\n    2,\n]\nB = 3\n"
        span = entities_file.find_entry_span(text, "parameters", "A")

        assert text[span.start : span.end] == "[\n    1,\n    2,\n]"

    def test_same_name_in_two_sections_is_not_confused(self):
        text = "[parameters]\nA = 1\n\n[path_inputs]\nA = \"x.csv\"\n"

        p = entities_file.find_entry_span(text, "parameters", "A")
        pi = entities_file.find_entry_span(text, "path_inputs", "A")

        assert text[p.start : p.end] == "1"
        assert text[pi.start : pi.end] == '"x.csv"'

    def test_missing_entry_is_none(self):
        assert entities_file.find_entry_span("[parameters]\n", "parameters", "A") is None




class TestAddVariable:
    def test_appends_to_a_multi_line_array_keeping_comments(self):
        text = 'variables = [\n    "A",  # first\n]\n'
        updated = entities_file.add_variable(text, "B")

        assert updated == 'variables = [\n    "A",  # first\n    "B",\n]\n'

    def test_single_line_array_is_rewritten_multi_line(self):
        text = 'variables = ["A"]\n'
        updated = entities_file.add_variable(text, "B")

        assert updated == 'variables = [\n    "A",\n    "B",\n]\n'

    def test_empty_array(self):
        text = "variables = []\n\n[parameters]\n"
        updated = entities_file.add_variable(text, "A")

        assert updated == 'variables = [\n    "A",\n]\n\n[parameters]\n'

    def test_already_declared_is_a_no_op(self):
        text = 'variables = ["A"]\n'
        assert entities_file.add_variable(text, "A") == text

    def test_missing_key_is_added_above_the_first_section(self):
        """Below a section header TOML would bind it to that table."""
        text = "[parameters]\nA = 1\n"
        updated = entities_file.add_variable(text, "X")

        assert updated == 'variables = [\n    "X",\n]\n\n[parameters]\nA = 1\n'


class TestRemoveEntry:
    """The palette's Delete: only the named entry's bytes go."""

    def test_removes_only_that_entry_keeping_neighbours_and_comments(self):
        text = (
            "[parameters]\n"
            "# rates\n"
            "A = 1\n"
            "B = [10, 20]  # windows\n"
            "C = 3\n"
        )
        updated = entities_file.remove_entry(text, "parameters", "B")

        assert updated == "[parameters]\n# rates\nA = 1\nC = 3\n"

    def test_multi_line_value_goes_whole(self):
        text = (
            "[path_inputs]\n"
            "RAW = {\n"
            '    template = "{s}/raw.csv",\n'
            '    root_folder = "/data",\n'
            "}\n"
            'EMG = "{s}/emg.csv"\n'
        )
        updated = entities_file.remove_entry(text, "path_inputs", "RAW")

        assert updated == '[path_inputs]\nEMG = "{s}/emg.csv"\n'

    def test_last_entry_without_trailing_newline(self):
        text = "[parameters]\nA = 1\nB = 2"
        assert entities_file.remove_entry(text, "parameters", "B") == "[parameters]\nA = 1\n"

    def test_only_entry_leaves_the_section_header(self):
        text = "[parameters]\nA = 1\n\n[path_inputs]\nP = \"x\"\n"
        updated = entities_file.remove_entry(text, "parameters", "A")

        assert updated == "[parameters]\n\n[path_inputs]\nP = \"x\"\n"
        entities_file.checked_text(updated)

    def test_same_name_in_another_section_is_untouched(self):
        text = "[parameters]\nX = 1\n\n[path_inputs]\nX = \"a\"\n"
        updated = entities_file.remove_entry(text, "path_inputs", "X")

        assert updated == "[parameters]\nX = 1\n\n[path_inputs]\n"

    def test_missing_entry_raises(self):
        with pytest.raises(ValueError, match="No entry 'Z'"):
            entities_file.remove_entry("[parameters]\nA = 1\n", "parameters", "Z")


class TestRemoveVariable:
    def test_multi_line_drops_the_elements_line_and_its_comment(self):
        text = 'variables = [\n    "A",  # first\n    "B",  # second\n    "C",\n]\n'
        updated = entities_file.remove_variable(text, "B")

        assert updated == 'variables = [\n    "A",  # first\n    "C",\n]\n'

    def test_multi_line_last_element_without_trailing_comma(self):
        text = 'variables = [\n    "A",\n    "B"\n]\n'
        updated = entities_file.remove_variable(text, "B")

        assert updated == 'variables = [\n    "A",\n]\n'
        entities_file.checked_text(updated)

    def test_single_line_first_middle_last(self):
        text = 'variables = ["A", "B", "C"]\n'
        assert entities_file.remove_variable(text, "A") == 'variables = ["B", "C"]\n'
        assert entities_file.remove_variable(text, "B") == 'variables = ["A", "C"]\n'
        assert entities_file.remove_variable(text, "C") == 'variables = ["A", "B"]\n'

    def test_only_element_leaves_an_empty_array(self):
        text = 'variables = ["A"]\n\n[parameters]\nX = 1\n'
        updated = entities_file.remove_variable(text, "A")

        assert updated == 'variables = []\n\n[parameters]\nX = 1\n'
        entities_file.checked_text(updated)

    def test_single_quoted_literal_matches(self):
        assert entities_file.remove_variable("variables = ['A', 'B']\n", "A") == "variables = ['B']\n"

    def test_name_inside_a_comment_is_not_an_element(self):
        text = 'variables = [\n    # "B" was renamed\n    "A",\n]\n'
        with pytest.raises(ValueError, match="No variable 'B'"):
            entities_file.remove_variable(text, "B")

    def test_missing_name_or_key_raises(self):
        with pytest.raises(ValueError):
            entities_file.remove_variable('variables = ["A"]\n', "Z")
        with pytest.raises(ValueError):
            entities_file.remove_variable("[parameters]\nX = 1\n", "Z")

    def test_round_trip_with_add_variable(self):
        text = 'variables = [\n    "A",\n]\n'
        assert entities_file.remove_variable(entities_file.add_variable(text, "B"), "B") == text



class TestCheckedText:
    """The gate every GUI write of the entities file passes before disk."""

    def test_valid_text_is_returned_unchanged(self):
        text = '[path_inputs]\nP = "x.csv"\n'
        assert entities_file.checked_text(text) is text

    def test_invalid_text_is_refused_and_the_line_is_logged(self, caplog):
        text = '[path_inputs]\nP = "x.csv"Q = ""\n'
        with caplog.at_level("ERROR", logger="scidb"):
            with pytest.raises(ValueError, match="nothing was written"):
                entities_file.checked_text(text, "ents.toml")

        assert "refusing to write ents.toml" in caplog.text
        assert 'P = "x.csv"Q = ""' in caplog.text

    def test_error_line_is_read_from_the_message_when_lineno_is_absent(self):
        # Python 3.11-3.13 tomllib and tomli 2.0 (3.10) set no `lineno`.
        exc = ValueError("Expected newline (at line 7, column 12)")
        assert entities_file._decode_error_line(exc) == 7
        assert entities_file._decode_error_line(ValueError("no location")) == 0

