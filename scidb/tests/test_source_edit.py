"""Tests for scidb.source_edit — locating and rewriting entity declarations.

Stage 1 of .claude/plan-gui-entity-editing-26-08-24.md. Everything here is
pure string/AST work; no files, no registry, no GUI.
"""

import ast

import pytest

from scidb.source_edit import (
    Span,
    find_binding_span,
    line_number,
    render_parameter,
    render_path_input,
    splice,
)


# ---------------------------------------------------------------------------
# find_binding_span
# ---------------------------------------------------------------------------


def _spliced(text, name, replacement):
    """Locate `name` and splice in `replacement` — the whole edit pipeline."""
    span = find_binding_span(text, name)
    assert span is not None, f"no binding found for {name!r}"
    return splice(text, span, replacement)


def test_finds_simple_binding():
    text = "import scidb\n\nWINDOW = scidb.constant(30, description='')\n"
    span = find_binding_span(text, "WINDOW")
    assert span is not None
    assert span.extract(text) == "scidb.constant(30, description='')"


def test_span_covers_whole_rhs_not_just_the_literal():
    """The span deliberately covers the entire call expression, which is what
    makes a Constant -> Sweep form change the same operation as a value
    change (D4/D5)."""
    text = "WINDOW = scidb.constant(30, description='')\n"
    out = _spliced(text, "WINDOW", render_parameter([30, 45]))
    assert out == "WINDOW = scidb.Parameter(30, 45, description='')\n"


def test_preserves_surrounding_comments_and_declarations():
    text = (
        '"""Entities."""\n'
        "import scidb\n"
        "\n"
        "# the sampling window, in seconds\n"
        "WINDOW = scidb.constant(30, description='')  # trailing note\n"
        "\n"
        "OTHER = scidb.constant(1, description='')\n"
    )
    out = _spliced(text, "WINDOW", "scidb.constant(45, description='')")
    assert "# the sampling window, in seconds\n" in out
    assert "# trailing note" in out
    assert "OTHER = scidb.constant(1, description='')" in out
    assert "scidb.constant(45, description='')" in out
    assert "constant(30" not in out


def test_multiline_call_span():
    text = (
        "RAW = scidb.PathInput(\n"
        "    '{subject}/{trial}.mat',\n"
        "    root_folder='/data',\n"
        ")\n"
        "AFTER = 1\n"
    )
    out = _spliced(text, "RAW", render_path_input("{subject}/x.csv", name="RAW"))
    assert out == "RAW = scidb.PathInput('{subject}/x.csv', name='RAW')\nAFTER = 1\n"


def test_annassign_supported():
    text = "WINDOW: int = scidb.constant(30, description='')\n"
    span = find_binding_span(text, "WINDOW")
    assert span is not None
    assert span.extract(text) == "scidb.constant(30, description='')"


def test_bare_annotation_has_no_value():
    assert find_binding_span("WINDOW: int\n", "WINDOW") is None


def test_last_binding_wins():
    """Module-level rebinding: discovery reads vars(module) after import, so
    the final binding is the one that is actually live."""
    text = "W = scidb.constant(1, description='')\nW = scidb.constant(2, description='')\n"
    span = find_binding_span(text, "W")
    assert span is not None
    assert span.extract(text) == "scidb.constant(2, description='')"


def test_nested_binding_is_not_top_level():
    text = "def f():\n    WINDOW = scidb.constant(30, description='')\n"
    assert find_binding_span(text, "WINDOW") is None


def test_chained_assignment_rejected():
    assert find_binding_span("A = B = scidb.constant(1, description='')\n", "A") is None


def test_tuple_unpacking_rejected():
    assert find_binding_span("A, B = 1, 2\n", "A") is None


def test_missing_name_returns_none():
    assert find_binding_span("OTHER = 1\n", "WINDOW") is None


def test_syntax_error_returns_none_rather_than_raising():
    assert find_binding_span("WINDOW = scidb.constant(\n", "WINDOW") is None


def test_non_ascii_column_offsets():
    """ast reports col_offset as a UTF-8 BYTE offset; a naive character-index
    conversion silently mislocates the span on any line containing non-ASCII
    text, corrupting the file on splice."""
    text = "RAW = scidb.PathInput('{sujet}/données.csv')\nAFTER = 1\n"
    span = find_binding_span(text, "RAW")
    assert span is not None
    assert span.extract(text) == "scidb.PathInput('{sujet}/données.csv')"
    out = splice(text, span, render_path_input("x.csv", name="RAW"))
    assert out == "RAW = scidb.PathInput('x.csv', name='RAW')\nAFTER = 1\n"


def test_non_ascii_on_an_earlier_line():
    text = (
        "# commentaire accentué — plusieurs caractères\n"
        "RAW = scidb.PathInput('a.csv')\n"
    )
    span = find_binding_span(text, "RAW")
    assert span is not None
    assert span.extract(text) == "scidb.PathInput('a.csv')"


# ---------------------------------------------------------------------------
# render_*
# ---------------------------------------------------------------------------


def test_render_parameter_single_value():
    assert render_parameter([2]) == "scidb.Parameter(2, description='')"
    assert (
        render_parameter([2.5], "how long")
        == "scidb.Parameter(2.5, description='how long')"
    )
    assert render_parameter(["a"]) == "scidb.Parameter('a', description='')"


def test_render_parameter_no_values():
    """A Parameter declared but not yet valued. The empty case has to build
    the argument list rather than interpolate a fixed comma, or it renders as
    the unparseable `scidb.Parameter(, description='')`."""
    assert render_parameter([]) == "scidb.Parameter(description='')"
    assert (
        render_parameter([], "filled in later")
        == "scidb.Parameter(description='filled in later')"
    )


def test_render_parameter_many_values():
    """One constructor whatever the count -- adding a value is adding an
    argument, never a change of form (D6)."""
    assert (
        render_parameter([0, 1, 2]) == "scidb.Parameter(0, 1, 2, description='')"
    )


def test_render_path_input_omits_falsy_root_folder():
    assert (
        render_path_input("{s}/x.csv", name="RAW")
        == "scidb.PathInput('{s}/x.csv', name='RAW')"
    )
    assert (
        render_path_input("{s}/x.csv", "", name="RAW")
        == "scidb.PathInput('{s}/x.csv', name='RAW')"
    )
    assert (
        render_path_input("{s}/x.csv", "/data", name="RAW")
        == "scidb.PathInput('{s}/x.csv', root_folder='/data', name='RAW')"
    )


def test_render_path_input_with_alternates_wraps_in_eachof():
    out = render_path_input(
        "a.csv", None, [{"template": "b.csv", "root_folder": "/d"}], name="RAW"
    )
    # Every alternative carries the ONE name: they are one PathInput.
    assert out == (
        "scidb.EachOf(scidb.PathInput('a.csv', name='RAW'), "
        "scidb.PathInput('b.csv', root_folder='/d', name='RAW'))"
    )


def test_qualifier_can_be_dropped_for_direct_import_contexts():
    assert render_parameter([1], qualifier="") == "Parameter(1, description='')"
    assert (
        render_path_input("a.csv", qualifier="", name="RAW")
        == "PathInput('a.csv', name='RAW')"
    )


@pytest.mark.parametrize(
    "expr",
    [
        render_parameter([30], "secs"),
        render_parameter([1, 2.5, "x"]),
        render_path_input("{s}/x.csv", "/data", name="NAME"),
        render_path_input("a.csv", None, [{"template": "b.csv"}], name="NAME"),
    ],
)
def test_rendered_expressions_are_parseable_and_relocatable(expr):
    """Anything render_* emits must be valid Python, and must be findable
    again by find_binding_span — otherwise a write-back could not be
    verified or subsequently re-edited."""
    text = f"NAME = {expr}\n"
    ast.parse(text)
    span = find_binding_span(text, "NAME")
    assert span is not None
    assert span.extract(text) == expr


def test_splice_is_pure_string_replacement():
    assert splice("abcdef", Span(2, 4), "XY") == "abXYef"


# ---------------------------------------------------------------------------
# line_number
# ---------------------------------------------------------------------------


def test_line_number_is_one_based():
    text = "a\nb\nc\n"
    assert line_number(text, 0) == 1
    assert line_number(text, 2) == 2
    assert line_number(text, 4) == 3


def test_line_number_of_a_located_binding():
    text = "import scidb\n\n# note\nWINDOW = scidb.constant(30, description='')\n"
    span = find_binding_span(text, "WINDOW")
    assert span is not None
    assert line_number(text, span.start) == 4
