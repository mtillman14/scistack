"""
Tests for MATLAB support: parser, registry, and command generation.
"""

import logging
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# matlab_parser tests
# ---------------------------------------------------------------------------


class TestParseMatlabFunction:
    def test_basic_function(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "bandpass_filter.m"
        f.write_text(
            textwrap.dedent("""\
            function [filtered] = bandpass_filter(signal, low_hz, high_hz)
            % BANDPASS_FILTER  Apply a bandpass filter.
                filtered = signal * low_hz;
            end
        """)
        )

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "bandpass_filter"
        assert info.params == ["signal", "low_hz", "high_hz"]
        assert info.language == "matlab"
        assert len(info.source_hash) == 64  # SHA-256 hex
        assert info.n_outputs == 1  # [filtered]

    def test_single_output(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "compute_vo2.m"
        f.write_text("function result = compute_vo2(breath_data)\n  result = 0;\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "compute_vo2"
        assert info.params == ["breath_data"]
        assert info.n_outputs == 1

    def test_no_output(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "plot_results.m"
        f.write_text("function plot_results(data, title_str)\n  plot(data);\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "plot_results"
        assert info.params == ["data", "title_str"]
        assert info.n_outputs == 0

    def test_no_params(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "setup.m"
        f.write_text("function setup()\n  disp('hi');\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "setup"
        assert info.params == []

    def test_not_a_function(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "script.m"
        f.write_text("% Just a script\nx = 5;\n")

        info = parse_matlab_function(f)
        assert info is None

    def test_missing_file(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        info = parse_matlab_function(tmp_path / "nonexistent.m")
        assert info is None

    def test_multiple_outputs(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "decompose.m"
        f.write_text("function [amp, phase, freq] = decompose(signal, fs)\nend\n")

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "decompose"
        assert info.params == ["signal", "fs"]
        assert info.n_outputs == 3  # [amp, phase, freq]

    def test_source_hash_changes(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "foo.m"
        f.write_text("function y = foo(x)\n  y = x;\nend\n")
        info1 = parse_matlab_function(f)

        f.write_text("function y = foo(x)\n  y = x * 2;\nend\n")
        info2 = parse_matlab_function(f)

        assert info1.source_hash != info2.source_hash

    def test_method_inside_non_basevariable_classdef_not_registered(self, tmp_path):
        """A `function` inside a methods block of a non-BaseVariable classdef
        (e.g. a matlab.unittest.TestCase's setup helper) must not be
        extracted as a standalone pipeline function — regression test for
        the 'shadows previous definition' bug where every unittest test
        class's identically-named setup method (e.g. resetSchema/addPaths)
        overwrote the last one registered."""
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "TestSomething.m"
        f.write_text(
            textwrap.dedent("""\
            classdef TestSomething < matlab.unittest.TestCase
                methods (TestMethodSetup)
                    function resetSchema(~)
                        scifor.set_schema(string.empty(1, 0));
                    end
                end
            end
        """)
        )

        info = parse_matlab_function(f)
        assert info is None

    def test_method_inside_basevariable_classdef_not_registered(self, tmp_path):
        """Same rule applies to a BaseVariable classdef's own methods (e.g.
        a constructor) -- parse_matlab_function must defer to
        parse_matlab_variable/classify_matlab_file for these, not extract
        the constructor as a standalone function."""
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "RawSignal.m"
        f.write_text(
            textwrap.dedent("""\
            classdef RawSignal < scidb.BaseVariable
                methods
                    function obj = RawSignal()
                    end
                end
            end
        """)
        )

        info = parse_matlab_function(f)
        assert info is None


class TestExtractDocstring:
    """Docstring extraction: the contiguous %-comment block immediately
    following the function declaration (MATLAB's own help/H1 convention).
    """

    def test_single_line(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "bandpass_filter.m"
        f.write_text(
            textwrap.dedent("""\
            function [filtered] = bandpass_filter(signal, low_hz, high_hz)
            % BANDPASS_FILTER  Apply a bandpass filter.
                filtered = signal * low_hz;
            end
        """)
        )

        info = parse_matlab_function(f)
        assert info.docstring == "BANDPASS_FILTER  Apply a bandpass filter."

    def test_multi_line(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "compute_vo2.m"
        f.write_text(
            textwrap.dedent("""\
            function result = compute_vo2(breath_data)
            %COMPUTE_VO2 Estimate VO2 from breath-by-breath data.
            %   result = COMPUTE_VO2(breath_data) returns the estimated VO2.
            %
            %   See also COMPUTE_VCO2.
              result = 0;
            end
        """)
        )

        info = parse_matlab_function(f)
        assert info.docstring == (
            "COMPUTE_VO2 Estimate VO2 from breath-by-breath data.\n"
            "  result = COMPUTE_VO2(breath_data) returns the estimated VO2.\n"
            "\n"
            "  See also COMPUTE_VCO2."
        )

    def test_none_when_blank_line_follows(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "setup.m"
        f.write_text("function setup()\n\n  disp('hi');\nend\n")

        info = parse_matlab_function(f)
        assert info.docstring is None

    def test_none_when_code_follows_immediately(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "plot_results.m"
        f.write_text("function plot_results(data)\n  plot(data);\nend\n")

        info = parse_matlab_function(f)
        assert info.docstring is None

    def test_stops_at_first_non_comment_line(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "decompose.m"
        f.write_text(
            textwrap.dedent("""\
            function [amp, phase] = decompose(signal)
            % DECOMPOSE  Split a signal into amplitude and phase.
            amp = abs(signal);
            % this trailing comment is not part of the docstring
            phase = angle(signal);
            end
        """)
        )

        info = parse_matlab_function(f)
        assert info.docstring == "DECOMPOSE  Split a signal into amplitude and phase."


class TestParseMatlabVariable:
    def test_basic_classdef(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "RawSignal.m"
        f.write_text(
            textwrap.dedent("""\
            classdef RawSignal < scidb.BaseVariable
                % Raw EMG signal data
            end
        """)
        )

        name = parse_matlab_variable(f)
        assert name == "RawSignal"

    def test_not_base_variable(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "MyClass.m"
        f.write_text("classdef MyClass < handle\nend\n")

        name = parse_matlab_variable(f)
        assert name is None

    def test_custom_base(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "Foo.m"
        f.write_text("classdef Foo < mylib.BaseVariable\nend\n")

        name = parse_matlab_variable(f)
        assert name == "Foo"

    def test_missing_file(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        name = parse_matlab_variable(tmp_path / "nonexistent.m")
        assert name is None

    def test_no_classdef(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "script.m"
        f.write_text("% Just a script\n")

        name = parse_matlab_variable(f)
        assert name is None


class TestBlockCommentStripping:
    """A %{ %} block comment must not false-positive as a real
    function/classdef declaration — a common way scientists leave
    "here's how to call this" example code in their scripts."""

    def test_function_example_inside_block_comment_ignored(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "real_fn.m"
        f.write_text(
            textwrap.dedent("""\
            %{
            Example usage:
            function y = fake_example(x)
                y = x * 2;
            end
            %}
            function y = real_fn(x)
                y = x;
            end
        """)
        )

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "real_fn"

    def test_classdef_example_inside_block_comment_ignored(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_variable

        f = tmp_path / "RealVar.m"
        f.write_text(
            textwrap.dedent("""\
            %{
            classdef FakeVar < scidb.BaseVariable
            end
            %}
            classdef RealVar < scidb.BaseVariable
            end
        """)
        )

        name = parse_matlab_variable(f)
        assert name == "RealVar"

    def test_entirely_commented_out_function_not_registered(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "commented.m"
        f.write_text(
            textwrap.dedent("""\
            %{
            function y = commented(x)
                y = x;
            end
            %}
        """)
        )

        info = parse_matlab_function(f)
        assert info is None


class TestLineContinuation:
    """A `...`-continued multi-line function signature must not leak the
    continuation marker or the next line's leading whitespace into a
    captured parameter name."""

    def test_multiline_signature_params_are_clean(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "many_args.m"
        f.write_text(
            "function y = many_args(alpha, beta, ...\n"
            "    gamma, delta)\n"
            "y = alpha;\n"
            "end\n"
        )

        info = parse_matlab_function(f)
        assert info is not None
        assert info.name == "many_args"
        assert info.params == ["alpha", "beta", "gamma", "delta"]
        assert not any("..." in p for p in info.params)
        assert not any("\n" in p for p in info.params)

    def test_multiline_signature_with_trailing_comment(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_function

        f = tmp_path / "commented_continuation.m"
        f.write_text(
            "function y = commented_continuation(a, ... this is a note\n"
            "    b)\n"
            "y = a;\n"
            "end\n"
        )

        info = parse_matlab_function(f)
        assert info is not None
        assert info.params == ["a", "b"]

    def test_source_hash_unaffected_by_preprocessing(self, tmp_path):
        """The source_hash must still be over the raw, unmodified file
        bytes — preprocessing is only applied to the text used for
        regex matching, never to what gets hashed."""
        from hashlib import sha256

        from scistack_gui.matlab_parser import parse_matlab_function

        raw = b"function y = foo(a, ...\n    b)\ny=a;\nend\n"
        f = tmp_path / "foo.m"
        f.write_bytes(raw)

        info = parse_matlab_function(f)
        assert info.source_hash == sha256(raw).hexdigest()


class TestPreprocessingIsLengthPreserving:
    """``_preprocess_for_parsing`` masks block comments and line
    continuations with spaces rather than deleting them, so an offset into
    the parsed text is also a valid offset into the original file. Without
    this, a span computed during parsing would land at the wrong place on
    write-back and corrupt the file (plan Stage 2)."""

    @pytest.mark.parametrize(
        "text",
        [
            "function y = f(a)\ny = a;\nend\n",
            "%{\nblock comment\n%}\nfunction y = f(a)\ny = a;\nend\n",
            "function y = f(a, ...\n    b)\ny = a;\nend\n",
            "%{\nblock\n%}\nfunction y = f(a, ... note\n    b)\ny = a;\nend\n",
            "%{\n%}\n",
            "",
        ],
    )
    def test_length_is_preserved(self, text):
        from scistack_gui.matlab_parser import _preprocess_for_parsing

        assert len(_preprocess_for_parsing(text)) == len(text)

    def test_block_comment_keeps_line_count(self):
        """Block comments become blank lines, so line numbers reported to
        the user still match the real file."""
        from scistack_gui.matlab_parser import _preprocess_for_parsing

        text = "%{\na\nb\n%}\nfunction y = f()\n"
        out = _preprocess_for_parsing(text)
        assert out.count("\n") == text.count("\n")
        assert "block" not in out
        assert out.index("function") == text.index("function")

    def test_masked_regions_carry_no_keywords(self):
        from scistack_gui.matlab_parser import _preprocess_for_parsing

        text = "%{\nfunction y = fake(x)\nclassdef Fake < scidb.BaseVariable\n%}\n"
        out = _preprocess_for_parsing(text)
        assert "function" not in out
        assert "classdef" not in out


class TestParseMatlabEntitiesScript:
    """The MATLAB entities script — a plain .m of top-level bindings, the
    analogue of src/scistack_entities.py (plan D1)."""

    def _write(self, tmp_path, body):
        f = tmp_path / "scistack_entities.m"
        f.write_text(textwrap.dedent(body))
        return f

    def test_parses_each_kind(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_entities_script

        f = self._write(
            tmp_path,
            """\
            raw_emg = scidb.PathInput('{subject}/{trial}.mat');
            window = scidb.Parameter(10, 20, 30);
            thresh = scidb.Parameter(2);
            """,
        )

        bindings = {b.name: b for b in parse_matlab_entities_script(f)}
        assert set(bindings) == {"raw_emg", "window", "thresh"}
        assert bindings["raw_emg"].kind == "path_input"
        assert bindings["window"].kind == "parameter"
        assert bindings["thresh"].kind == "parameter"

    def test_spans_locate_expression_and_arguments(self, tmp_path):
        from scistack_gui.matlab_parser import (
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(tmp_path, "window = scidb.Parameter(10, 20);\n")
        text = read_source_text(f)
        b = parse_matlab_entities_script(f)[0]

        assert b.expr_span.extract(text) == "scidb.Parameter(10, 20)"
        assert b.args_span.extract(text) == "10, 20"

    def test_expression_span_enables_a_form_change(self, tmp_path):
        """The RHS span covers the constructor too, so Constant -> Sweep is
        the same splice as a value edit (D4)."""
        from scidb.source_edit import splice

        from scistack_gui.matlab_parser import (
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(tmp_path, "thresh = scidb.Parameter(2);\nx = 1;\n")
        text = read_source_text(f)
        b = parse_matlab_entities_script(f)[0]

        assert (
            splice(text, b.expr_span, "scidb.Parameter(2, 5)")
            == "thresh = scidb.Parameter(2, 5);\nx = 1;\n"
        )

    def test_scifor_namespace_accepted(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_entities_script

        f = self._write(tmp_path, "window = scidb.Parameter(1);\n")
        assert parse_matlab_entities_script(f)[0].kind == "parameter"

    def test_bare_constructor_accepted(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_entities_script

        f = self._write(tmp_path, "window = Parameter(1);\n")
        assert parse_matlab_entities_script(f)[0].kind == "parameter"

    def test_non_entity_lines_are_skipped(self, tmp_path):
        """An entities script may contain ordinary MATLAB."""
        from scistack_gui.matlab_parser import parse_matlab_entities_script

        f = self._write(
            tmp_path,
            """\
            n = 5;
            label = 'hello';
            window = scidb.Parameter(1);
            """,
        )
        assert [b.name for b in parse_matlab_entities_script(f)] == ["window"]

    def test_comparison_is_not_a_binding(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_entities_script

        f = self._write(tmp_path, "if a == scidb.Parameter(1)\nend\n")
        assert parse_matlab_entities_script(f) == []

    def test_paren_inside_a_template_does_not_break_the_span(self, tmp_path):
        from scistack_gui.matlab_parser import (
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(tmp_path, "p = scidb.PathInput('{s}/a(1).mat');\n")
        text = read_source_text(f)
        b = parse_matlab_entities_script(f)[0]
        assert b.args_span.extract(text) == "'{s}/a(1).mat'"

    def test_semicolon_is_optional(self, tmp_path):
        from scistack_gui.matlab_parser import (
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(tmp_path, "window = scidb.Parameter(1)\nother = 2\n")
        text = read_source_text(f)
        b = parse_matlab_entities_script(f)[0]
        assert b.expr_span.extract(text) == "scidb.Parameter(1)"

    def test_continued_argument_list(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_entities_script

        f = self._write(tmp_path, "window = scidb.Parameter(1, ...\n    2);\n")
        b = parse_matlab_entities_script(f)[0]
        assert b.kind == "parameter"

    def test_constant_literal_extracted(self, tmp_path):
        from scistack_gui.matlab_parser import (
            binding_parameter_literal,
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(
            tmp_path, "w = scidb.Parameter(30, description='Analysis window');\n"
        )
        text = read_source_text(f)
        b = parse_matlab_entities_script(f)[0]
        assert binding_parameter_literal(b, text) == ([30], "Analysis window")

    def test_literals_extracted(self, tmp_path):
        from scistack_gui.matlab_parser import (
            binding_path_input_literal,
            binding_parameter_literal,
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(
            tmp_path,
            """\
            raw = scidb.PathInput('{s}/a.mat', root_folder='/data');
            window = scidb.Parameter(10, 20.5, 'x');
            """,
        )
        text = read_source_text(f)
        bindings = {b.name: b for b in parse_matlab_entities_script(f)}

        assert binding_path_input_literal(bindings["raw"], text) == (
            "{s}/a.mat",
            "/data",
        )
        assert binding_parameter_literal(bindings["window"], text) == ([10, 20.5, "x"], "")

    def test_root_folder_accepted_in_both_matlab_syntaxes(self, tmp_path):
        """+scifor/PathInput.m's `arguments` block accepts name=value
        (R2021b+) and name-value pairs alike, so extraction must too."""
        from scistack_gui.matlab_parser import (
            binding_path_input_literal,
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(
            tmp_path,
            """\
            a = scidb.PathInput('x.mat', root_folder='/data');
            b = scidb.PathInput('x.mat', 'root_folder', '/data');
            """,
        )
        text = read_source_text(f)
        bindings = {b.name: b for b in parse_matlab_entities_script(f)}

        assert binding_path_input_literal(bindings["a"], text) == ("x.mat", "/data")
        assert binding_path_input_literal(bindings["b"], text) == ("x.mat", "/data")

    # -- Static folding of templates built from variables ------------------
    #
    # A real project lost 5 of 8 MATLAB PathInputs to "not a literal
    # construction" because their templates were assembled from a base
    # directory rather than written inline, so they never appeared as canvas
    # nodes. Folding resolves the common assembly idioms without running
    # MATLAB. See matlab_parser.collect_matlab_literal_scope.

    def _fold(self, tmp_path, src, name="p"):
        from scistack_gui.matlab_parser import (
            binding_path_input_literal,
            collect_matlab_literal_scope,
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(tmp_path, src)
        text = read_source_text(f)
        scope = collect_matlab_literal_scope(text)
        bindings = {b.name: b for b in parse_matlab_entities_script(f)}
        return binding_path_input_literal(bindings[name], text, scope)

    def test_folds_bare_variable_reference(self, tmp_path):
        src = "tmpl = '6MWT-{pass}.xlsx';\np = scidb.PathInput(tmpl);\n"
        assert self._fold(tmp_path, src) == ("6MWT-{pass}.xlsx", None)

    def test_folds_bracket_concatenation_with_spaces(self, tmp_path):
        """``[a b]`` splits on whitespace, unlike an argument list."""
        src = (
            "baseDir = 'Y:/data';\n"
            "p = scidb.PathInput([baseDir '/6MWT-{pass}.xlsx']);\n"
        )
        assert self._fold(tmp_path, src) == ("Y:/data/6MWT-{pass}.xlsx", None)

    def test_folds_bracket_concatenation_with_commas(self, tmp_path):
        src = "baseDir = 'Y:/data';\np = scidb.PathInput([baseDir, '/x.xlsx']);\n"
        assert self._fold(tmp_path, src) == ("Y:/data/x.xlsx", None)

    def test_folds_fullfile(self, tmp_path):
        src = (
            "baseDir = 'Y:/data';\n"
            "p = scidb.PathInput(fullfile(baseDir, 'EMG', '6MWT.csv'));\n"
        )
        assert self._fold(tmp_path, src) == ("Y:/data/EMG/6MWT.csv", None)

    def test_folds_strcat_without_separator(self, tmp_path):
        src = "stem = 'trial';\np = scidb.PathInput(strcat(stem, '.mat'));\n"
        assert self._fold(tmp_path, src) == ("trial.mat", None)

    def test_folds_chained_variable_definitions(self, tmp_path):
        """A later helper may be built from an earlier one."""
        src = (
            "root = 'Y:/study';\n"
            "emgDir = [root '/EMG'];\n"
            "p = scidb.PathInput([emgDir '/{pass}.csv']);\n"
        )
        assert self._fold(tmp_path, src) == ("Y:/study/EMG/{pass}.csv", None)

    def test_folds_root_folder_argument_too(self, tmp_path):
        src = (
            "rootDir = '/data';\n"
            "p = scidb.PathInput('x.mat', 'root_folder', rootDir);\n"
        )
        assert self._fold(tmp_path, src) == ("x.mat", "/data")

    def test_last_binding_of_a_helper_wins(self, tmp_path):
        src = (
            "baseDir = 'Y:/old';\n"
            "baseDir = 'Y:/new';\n"
            "p = scidb.PathInput([baseDir '/x.csv']);\n"
        )
        assert self._fold(tmp_path, src) == ("Y:/new/x.csv", None)

    def test_unresolvable_expression_still_reports_non_literal(self, tmp_path):
        """Guessing a value would put a wrong path on the canvas -- worse
        than the honest 'cannot extract' warning."""
        src = "p = scidb.PathInput(sprintf('%s.csv', subject));\n"
        assert self._fold(tmp_path, src) is None

    def test_unknown_variable_is_not_invented(self, tmp_path):
        src = "p = scidb.PathInput([undefinedVar '/x.csv']);\n"
        assert self._fold(tmp_path, src) is None

    def test_partially_resolvable_concat_is_all_or_nothing(self, tmp_path):
        src = "baseDir = 'Y:/data';\np = scidb.PathInput([baseDir unknownTail]);\n"
        assert self._fold(tmp_path, src) is None

    def test_folding_is_opt_in_no_scope_behaves_as_before(self, tmp_path):
        """Callers that don't pass a scope keep the old literal-only rule."""
        from scistack_gui.matlab_parser import (
            binding_path_input_literal,
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(
            tmp_path, "tmpl = 'x.mat';\np = scidb.PathInput(tmpl);\n"
        )
        text = read_source_text(f)
        b = {x.name: x for x in parse_matlab_entities_script(f)}["p"]
        assert binding_path_input_literal(b, text) is None

    def test_equals_inside_a_template_is_not_a_named_argument(self, tmp_path):
        from scistack_gui.matlab_parser import (
            binding_path_input_literal,
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(tmp_path, "a = scidb.PathInput('{s}/a=b.mat');\n")
        text = read_source_text(f)
        b = parse_matlab_entities_script(f)[0]
        assert binding_path_input_literal(b, text) == ("{s}/a=b.mat", None)

    def test_non_literal_value_invalidates_extraction(self, tmp_path):
        from scistack_gui.matlab_parser import (
            binding_parameter_literal,
            parse_matlab_entities_script,
            read_source_text,
        )

        f = self._write(tmp_path, "window = scidb.Parameter(1, some_var);\n")
        text = read_source_text(f)
        b = parse_matlab_entities_script(f)[0]
        assert binding_parameter_literal(b, text) is None

    def test_missing_file_returns_empty(self, tmp_path):
        from scistack_gui.matlab_parser import parse_matlab_entities_script

        assert parse_matlab_entities_script(tmp_path / "nope.m") == []

    def test_is_entities_script_rejects_functions_and_classdefs(self, tmp_path):
        from scistack_gui.matlab_parser import is_matlab_entities_script

        script = self._write(tmp_path, "window = scidb.Parameter(1);\n")
        assert is_matlab_entities_script(script)

        fn = tmp_path / "window_fn.m"
        fn.write_text("function s = window_fn()\ns = scidb.Parameter(1);\nend\n")
        assert not is_matlab_entities_script(fn)

        cls = tmp_path / "RawSignal.m"
        cls.write_text("classdef RawSignal < scidb.BaseVariable\nend\n")
        assert not is_matlab_entities_script(cls)

        plain = tmp_path / "plain.m"
        plain.write_text("x = 1;\n")
        assert not is_matlab_entities_script(plain)

    def test_classify_returns_entities_script(self, tmp_path):
        from scistack_gui.matlab_parser import classify_matlab_file

        f = self._write(tmp_path, "window = scidb.Parameter(1);\n")
        kind, _ = classify_matlab_file(f)
        assert kind == "entities_script"

    def test_classify_still_prefers_function_over_entities_script(self, tmp_path):
        """Regression guard: the entities check runs last, so it can never
        steal a file from an existing category. A function that happens to
        construct a Sweep is just a function — the value-getter convention
        no longer exists."""
        from scistack_gui.matlab_parser import classify_matlab_file

        f = tmp_path / "window.m"
        f.write_text("function s = window()\ns = scidb.Parameter(1);\nend\n")
        kind, payload = classify_matlab_file(f)
        assert kind == "function"
        assert payload.name == "window"


class TestLoadEntitiesScript:
    """Entities declared in a script must register into the SAME shared
    registry the getter path uses, so nothing downstream can tell which
    form declared them."""

    def test_registers_path_input_and_sweep(self, tmp_path):
        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text(
            "raw = scidb.PathInput('{s}/a.mat');\nwindow = scidb.Parameter(1, 2);\n"
        )

        matlab_registry.load_entities_script(f)

        pi = registry.get_path_inputs_registry()["raw"]
        assert pi.path_template == "{s}/a.mat"
        sw = registry.get_parameters_registry()["window"]
        assert list(sw.alternatives) == [1, 2]

    def test_registered_sweep_is_a_real_eachof(self, tmp_path):
        from scifor import EachOf

        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text("window = scidb.Parameter(1, 2);\n")
        matlab_registry.load_entities_script(f)

        assert isinstance(registry.get_parameters_registry()["window"], EachOf)

    def test_last_binding_of_a_name_wins(self, tmp_path):
        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text("window = scidb.Parameter(1);\nwindow = scidb.Parameter(9);\n")
        matlab_registry.load_entities_script(f)

        assert list(registry.get_parameters_registry()["window"].alternatives) == [9]

    def test_registers_constant_with_source_declared_identity(self, tmp_path):
        """Before +scidb/Constant.m, a MATLAB constant was an anonymous value
        in a for_each struct with no discoverable name (the old "MATLAB has
        no equivalent" note in code-discovery-categories.md §3)."""
        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text(
            "window = scidb.Parameter(30, description='Analysis window');\n"
        )

        matlab_registry.load_entities_script(f)

        const = registry.get_parameters_registry()["window"]
        assert const.value == 30
        assert const.description == "Analysis window"
        assert "window" in matlab_registry.get_all_parameter_names()

    def test_constant_registers_as_a_real_scidb_constant(self, tmp_path):
        """It must be the same type Python discovery produces, so
        build_parameter_nodes and every other consumer stay language-agnostic."""
        from scidb import Parameter

        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text("window = scidb.Parameter(30);\n")
        matlab_registry.load_entities_script(f)

        assert isinstance(registry.get_parameters_registry()["window"], Parameter)

    def test_constant_description_optional_and_both_syntaxes(self, tmp_path):
        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text(
            "a = scidb.Parameter(1);\n"
            "b = scidb.Parameter(2, description='named');\n"
            "c = scidb.Parameter(3, 'description', 'paired');\n"
        )
        matlab_registry.load_entities_script(f)

        consts = registry.get_parameters_registry()
        assert consts["a"].value == 1 and consts["a"].description == ""
        assert consts["b"].description == "named"
        assert consts["c"].description == "paired"

    def test_constant_string_value(self, tmp_path):
        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text("label = scidb.Parameter('baseline');\n")
        matlab_registry.load_entities_script(f)

        assert registry.get_parameters_registry()["label"].value == "baseline"

    def test_non_literal_constant_stays_unregistered(self, tmp_path):
        from scistack_gui import matlab_registry, registry

        f = tmp_path / "scistack_entities.m"
        f.write_text("window = scidb.Parameter(some_var);\n")
        registry._parameters.pop("window", None)
        matlab_registry.load_entities_script(f)

        assert "window" not in registry.get_parameters_registry()
        assert any(
            "window" in e.get("error", "") for e in matlab_registry.get_load_errors()
        )

    def test_missing_file_is_not_an_error(self, tmp_path):
        from scistack_gui import matlab_registry

        matlab_registry.load_entities_script(tmp_path / "nope.m")
        assert not any(
            "nope.m" in e.get("source", "") for e in matlab_registry.get_load_errors()
        )

    def test_non_literal_declaration_records_a_load_error(self, tmp_path):
        from scistack_gui import matlab_registry

        f = tmp_path / "scistack_entities.m"
        f.write_text("window = scidb.Parameter(some_var);\n")
        matlab_registry.load_entities_script(f)

        assert any(
            "window" in e.get("error", "") for e in matlab_registry.get_load_errors()
        )


class TestClassifyMatlabFile:
    def test_classifies_function(self, tmp_path):
        from scistack_gui.matlab_parser import classify_matlab_file

        f = tmp_path / "foo.m"
        f.write_text("function y = foo(x)\ny=x;\nend\n")

        kind, payload = classify_matlab_file(f)
        assert kind == "function"
        assert payload.name == "foo"

    def test_classifies_variable(self, tmp_path):
        from scistack_gui.matlab_parser import classify_matlab_file

        f = tmp_path / "RawSignal.m"
        f.write_text("classdef RawSignal < scidb.BaseVariable\nend\n")

        kind, payload = classify_matlab_file(f)
        assert kind == "variable"
        assert payload == "RawSignal"

    def test_classdef_with_method_is_variable_not_function(self, tmp_path):
        """A BaseVariable classdef containing a method (which itself has a
        `function` declaration) must be classified as a variable, not a
        function — classdef parsing is tried first specifically for this
        reason."""
        from scistack_gui.matlab_parser import classify_matlab_file

        f = tmp_path / "RawSignal.m"
        f.write_text(
            textwrap.dedent("""\
            classdef RawSignal < scidb.BaseVariable
                methods
                    function obj = RawSignal()
                    end
                end
            end
        """)
        )

        kind, payload = classify_matlab_file(f)
        assert kind == "variable"
        assert payload == "RawSignal"

    def test_neither_returns_none(self, tmp_path):
        from scistack_gui.matlab_parser import classify_matlab_file

        f = tmp_path / "script.m"
        f.write_text("% just a script\nx = 5;\n")

        assert classify_matlab_file(f) is None


class TestMatlabRegistryLoadFromSources:
    def test_mixed_sources_classified_and_registered(self, tmp_path):
        from scistack_gui import matlab_registry

        fn_file = tmp_path / "foo.m"
        fn_file.write_text("function y = foo(x)\ny=x;\nend\n")
        var_file = tmp_path / "RawSignal.m"
        var_file.write_text("classdef RawSignal < scidb.BaseVariable\nend\n")

        matlab_registry._matlab_functions.clear()
        matlab_registry._matlab_variables.clear()
        matlab_registry.load_from_sources([fn_file, var_file])

        assert matlab_registry.is_matlab_function("foo")
        assert "RawSignal" in matlab_registry.get_all_variable_names()

    def test_refresh_deregisters_stale_entry_from_shared_registry(self, tmp_path):
        """Removing a PathInput declaration and refreshing must not leave
        its OLD registered object lingering forever in
        scistack_gui.registry -- matlab_registry.clear() only ever touched
        its own dicts before this fix."""
        from scistack_gui import config, matlab_registry, registry

        entities = tmp_path / "scistack_entities.m"
        entities.write_text("raw_emg = scidb.PathInput('{subject}.mat');\n")
        cfg = config.SciStackConfig(
            project_root=tmp_path, matlab_entities_file=entities
        )

        registry._path_inputs.clear()
        matlab_registry.load_from_config(cfg)
        assert registry.get_path_input("raw_emg") is not None

        # The declaration is deleted from the entities script -- simulate by
        # loading an EMPTY config.
        empty_cfg = config.SciStackConfig(project_root=tmp_path)
        matlab_registry.load_from_config(empty_cfg)

        assert registry.get_path_input("raw_emg") is None

    def test_unclassifiable_file_skipped_without_warning(self, tmp_path, caplog):
        """A folder-scanned .m file that classifies as neither a function
        nor a variable (e.g. an ordinary script) is expected, not a
        misconfiguration — most real MATLAB projects have plenty of these.
        load_from_sources logs it at DEBUG (not WARNING) and does NOT
        record it as a load error; only files explicitly listed in
        matlab.functions/matlab.variables that fail to parse are load
        errors (see load_from_config)."""
        import logging

        from scistack_gui import matlab_registry

        script_file = tmp_path / "script.m"
        script_file.write_text("% just a script\n")

        matlab_registry._matlab_functions.clear()
        matlab_registry._matlab_variables.clear()
        matlab_registry._load_errors.clear()
        with caplog.at_level(logging.DEBUG):
            matlab_registry.load_from_sources([script_file])

        assert matlab_registry.get_all_function_names() == []
        assert "Skipping non-function/non-variable" in caplog.text
        assert "Could not classify" not in caplog.text
        assert matlab_registry.get_load_errors() == []

    def test_same_named_setup_methods_across_test_classes_do_not_shadow(
        self, tmp_path, caplog
    ):
        """Regression test for the 'shadows previous definition' warning
        seen scanning a real MATLAB unittest suite: many test classes each
        define their own local setup method under the same name (e.g.
        resetSchema/addPaths). Folder-scan discovery must not register
        these as standalone functions at all -- so two files reusing the
        same setup-method name must NOT collide in the registry."""
        import logging

        from scistack_gui import matlab_registry

        test_a = tmp_path / "TestA.m"
        test_a.write_text(
            textwrap.dedent("""\
            classdef TestA < matlab.unittest.TestCase
                methods (TestMethodSetup)
                    function resetSchema(~)
                        scifor.set_schema(string.empty(1, 0));
                    end
                end
            end
        """)
        )
        test_b = tmp_path / "TestB.m"
        test_b.write_text(
            textwrap.dedent("""\
            classdef TestB < matlab.unittest.TestCase
                methods (TestMethodSetup)
                    function resetSchema(~)
                        scifor.set_schema(string.empty(1, 0));
                    end
                end
            end
        """)
        )

        matlab_registry._matlab_functions.clear()
        matlab_registry._matlab_variables.clear()
        matlab_registry._load_errors.clear()
        with caplog.at_level(logging.DEBUG):
            matlab_registry.load_from_sources([test_a, test_b])

        assert "resetSchema" not in matlab_registry.get_all_function_names()
        assert "shadows previous definition" not in caplog.text


# ---------------------------------------------------------------------------
# matlab_command tests
# ---------------------------------------------------------------------------


class TestGenerateMatlabCommand:
    def test_template_no_variants(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="bandpass_filter",
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session"],
        )

        assert "bandpass_filter" in cmd
        assert "/data/experiment.duckdb" in cmd
        assert "scihist.configure_database" in cmd
        assert "scidb.for_each" in cmd

    def test_with_variants(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {"signal": "RawSignal"},
                "output_type": "FilteredSignal",
                "constants": {"low_hz": 20},
                "record_count": 4,
            }
        ]

        cmd = generate_matlab_command(
            function_name="bandpass_filter",
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session"],
            variants=variants,
        )

        assert "scidb.register_variable(FilteredSignal())" in cmd
        assert "scidb.register_variable(RawSignal())" in cmd
        assert "@bandpass_filter" in cmd
        assert "RawSignal()" in cmd
        assert "{FilteredSignal()}" in cmd
        assert "20" in cmd

    def test_addpath(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="foo",
            db_path="/data/db.duckdb",
            schema_keys=["subject"],
            addpath_dirs=["/home/user/matlab/lib", "/home/user/shared"],
        )

        assert "addpath('/home/user/matlab/lib')" in cmd
        assert "addpath('/home/user/shared')" in cmd

    def test_schema_filter(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {"x": "X"},
                "output_type": "Y",
                "constants": {},
                "record_count": 2,
            }
        ]

        cmd = generate_matlab_command(
            function_name="process",
            db_path="/data/db.duckdb",
            schema_keys=["subject", "session"],
            variants=variants,
            schema_filter={"subject": [1, 2, 3]},
        )

        assert "'subject'" in cmd
        assert "[1 2 3]" in cmd

    def test_string_schema_filter(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {},
                "output_type": "Y",
                "constants": {},
                "record_count": 1,
            }
        ]

        cmd = generate_matlab_command(
            function_name="process",
            db_path="/db.duckdb",
            schema_keys=["session"],
            variants=variants,
            schema_filter={"session": ["pre", "post"]},
        )

        assert '"pre"' in cmd
        assert '"post"' in cmd

    def test_deduplicates_variants(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        # Same constants, different output types → should deduplicate.
        variants = [
            {
                "input_types": {"x": "X"},
                "output_type": "Y1",
                "constants": {"k": 5},
                "record_count": 1,
            },
            {
                "input_types": {"x": "X"},
                "output_type": "Y2",
                "constants": {"k": 5},
                "record_count": 1,
            },
        ]

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            variants=variants,
        )

        # Should only have one for_each call.
        assert cmd.count("scidb.for_each") == 1

    def test_escape_single_quotes(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/path/with'quote/db.duckdb",
            schema_keys=["s"],
        )

        assert "/path/with''quote/db.duckdb" in cmd

    def test_pyenv_preamble_present(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable="/usr/bin/python3",
        )

        # Stage 1: bind
        assert "pyenv('Version', scistack_pyenv_target__)" in cmd
        assert "scistack_pyenv_target__ = '/usr/bin/python3';" in cmd
        assert 'if scistack_pyenv__.Status == "NotLoaded"' in cmd
        assert "SciStack:PyenvMismatch" in cmd
        # Stage 2: force-load (smoke test)
        assert "py.sys.version" in cmd
        # Stage 3: diagnostic dump on smoke-test failure
        assert "OutOfProcess" in cmd
        # Stage 4: pre-import scidb so py.scidb.* is warm
        assert "py.importlib.import_module('scidb')" in cmd
        # Teardown: clear all temporaries
        assert "clear scistack_pyenv__ scistack_pyenv_target__" in cmd
        # clear functions is NOT emitted — it breaks py.list inside package
        # functions (MATLAB resolves py.X as a module lookup post-cache-clear,
        # which fails for builtins like list).
        assert "clear functions" not in cmd

    def test_pyenv_preamble_omitted_when_none(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable=None,
        )

        assert "pyenv" not in cmd

    def test_pyenv_preamble_escapes_single_quotes(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable="/tmp/O'Neil/python",
        )

        # Single quote in path must be doubled inside the MATLAB literal.
        assert "scistack_pyenv_target__ = '/tmp/O''Neil/python';" in cmd

    def test_pyenv_preamble_windows_path(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable=r"C:\Users\mtillman\venvs\stim-device-comparison\Scripts\python.exe",
        )

        # Backslashes converted to forward slashes for the MATLAB literal.
        assert (
            "scistack_pyenv_target__ = "
            "'C:/Users/mtillman/venvs/stim-device-comparison/Scripts/python.exe';"
        ) in cmd
        assert "\\" not in cmd.split("scistack_pyenv_target__ =")[1].splitlines()[0]

    def test_pyenv_preamble_mismatch_uses_normalized_compare(self):
        """The mismatch check must tolerate backslash/forward-slash differences
        between what MATLAB's pyenv returns and our target literal.
        Regression: previously ``string(Executable) ~= string(target)`` fired
        erroneously when Status=Loaded and the paths differed only in separators.
        """
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable=r"C:\Users\mtillman\venvs\scistack-gui\.venv\Scripts\python.exe",
        )

        # The comparison MUST use a path normalizer (strrep + strcmpi), not a
        # raw string equality.
        assert "scistack_norm_path__" in cmd
        # MATLAB literal: strrep(char(p), '\', '/')  (single backslash in MATLAB).
        assert "strrep(char(p), '\\', '/')" in cmd
        assert "strcmpi(" in cmd
        # And the raw mismatching pattern must NOT be present.
        assert (
            "string(scistack_pyenv__.Executable) ~= string(scistack_pyenv_target__)"
            not in cmd
        )

    def test_pyenv_preamble_ordered_before_addpath(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="f",
            db_path="/db.duckdb",
            schema_keys=["s"],
            addpath_dirs=["/home/user/matlab/lib"],
            python_executable="/usr/bin/python3",
        )

        pyenv_idx = cmd.index("scistack_pyenv_target__")
        addpath_idx = cmd.index("addpath(")
        assert pyenv_idx < addpath_idx


# ---------------------------------------------------------------------------
# _format_path_input tests
# ---------------------------------------------------------------------------


class TestFormatPathInput:
    def test_explicit_root_folder_used_as_is(self):
        from scistack_gui.api.matlab_command import _format_path_input

        pi = {"template": "{subject}/data.mat", "root_folder": "/my/data"}
        result = _format_path_input(pi)
        assert (
            result == 'scifor.PathInput("{subject}/data.mat", root_folder="/my/data")'
        )

    def test_no_root_folder_no_project_root(self):
        from scistack_gui.api.matlab_command import _format_path_input

        pi = {"template": "{subject}/data.mat", "root_folder": None}
        result = _format_path_input(pi)
        assert result == 'scifor.PathInput("{subject}/data.mat")'

    def test_absolute_template_without_root_folder(self):
        from scistack_gui.api.matlab_command import _format_path_input

        pi = {"template": "/absolute/path/{subject}.mat", "root_folder": None}
        result = _format_path_input(pi)
        assert result == 'scifor.PathInput("/absolute/path/{subject}.mat")'

    def test_generate_matlab_command_never_substitutes_project_root(self):
        """A rootless declaration must stay rootless in the generated script.

        Writing the project root into ``root_folder`` changes what
        ``PathInput.to_key()`` records, so the run could no longer be
        content-matched against the declaration that produced it and the canvas
        grew an ``__unresolved__`` ghost node. Resolution is pinned separately
        (see ``test_generate_matlab_command_pins_project_root``).
        """
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="load_file",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            path_inputs={
                "filepath": {"template": "{subject}/data.mat", "root_folder": None}
            },
            project_root="/projects/myexp",
        )
        assert 'scifor.PathInput("{subject}/data.mat")' in cmd
        assert 'root_folder="/projects/myexp"' not in cmd

    def test_generate_matlab_command_keeps_declared_root_folder(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="load_file",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            path_inputs={
                "filepath": {
                    "template": "{subject}/data.mat",
                    "root_folder": "/explicit/root",
                }
            },
            project_root="/projects/myexp",
        )
        assert (
            'scifor.PathInput("{subject}/data.mat", root_folder="/explicit/root")'
            in cmd
        )

    def test_generate_matlab_command_pins_project_root(self):
        """The project root is stated to scifor instead, so a rootless
        PathInput resolves against the project and not MATLAB's cwd."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="load_file",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            path_inputs={
                "filepath": {"template": "{subject}/data.mat", "root_folder": None}
            },
            project_root="/projects/myexp",
        )
        assert (
            "py.scimatlab.bridge.set_pathinput_project_root('/projects/myexp');" in cmd
        )
        # It is a py.* call, so it must land after the pyenv binding and
        # before the first for_each that uses a PathInput.
        assert cmd.index("set_pathinput_project_root") < cmd.index("scifor.PathInput")

    def test_generate_matlab_command_omits_pin_without_project_root(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="load_file",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
        )
        assert "set_pathinput_project_root" not in cmd

    def test_generate_matlab_command_injects_sweep_as_scifor_sweep(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="bandpass_filter",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            sweeps={"low_hz": [10, 20, 30]},
        )
        assert "scidb.Parameter(10, 20, 30)" in cmd

    def test_generate_matlab_command_refuses_a_parameter_with_no_values(self):
        """``scidb.Parameter()`` is perfectly constructible in MATLAB — a
        Parameter with no value yet is a legal object — so emitting it would
        produce a script that runs, expands a zero-length axis and writes
        nothing, failing in the terminal a long way from the click that
        caused it. Refuse at generation time, like the Python run path does
        in execution_service.build_run_inputs."""
        import pytest

        from scistack_gui.api.matlab_command import generate_matlab_command

        with pytest.raises(ValueError, match="has no value yet") as excinfo:
            generate_matlab_command(
                function_name="bandpass_filter",
                db_path="/data/exp.duckdb",
                schema_keys=["subject"],
                sweeps={"low_hz": []},
            )
        assert "'low_hz'" in str(excinfo.value)

    def test_generate_matlab_command_sweep_and_path_input_together(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="bandpass_filter",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            path_inputs={
                "filepath": {"template": "{subject}/data.mat", "root_folder": None}
            },
            sweeps={"low_hz": [10, 20]},
            project_root="/projects/myexp",
        )
        assert "scidb.Parameter(10, 20)" in cmd
        assert "scifor.PathInput" in cmd

    def test_never_run_function_still_iterates_the_schema(self):
        """The no-variants (first run) branch must emit schema kwargs.

        Regression: it emitted ``for_each(@fn, inputs, outputs);`` with no
        iterables at all, so for_each collapsed to one combo and handed the
        function every loaded record at once as a single table. It only looked
        fine for functions whose PathInput template carries a {key}
        placeholder, because discovery then supplied the iterable.
        """
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filter_emg",
            db_path="/data/exp.duckdb",
            schema_keys=["pass", "cycle"],
            variable_inputs={"loaded_data": "RawEMG"},
            output_types=["FilteredEMG"],
        )
        assert "'pass', []" in cmd
        assert "'cycle', []" in cmd

    def test_never_run_function_honors_schema_level(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filter_emg",
            db_path="/data/exp.duckdb",
            schema_keys=["pass", "cycle"],
            schema_level=["pass"],
            variable_inputs={"loaded_data": "RawEMG"},
            output_types=["FilteredEMG"],
        )
        assert "'pass', []" in cmd
        assert "'cycle'" not in cmd

    def test_never_run_function_applies_schema_filter(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filter_emg",
            db_path="/data/exp.duckdb",
            schema_keys=["pass"],
            schema_filter={"pass": [1, 2]},
            variable_inputs={"loaded_data": "RawEMG"},
            output_types=["FilteredEMG"],
        )
        assert "'pass', [1 2]" in cmd

    def test_no_schema_keys_emits_no_trailing_comma(self):
        """A schema-less project must still produce a syntactically valid
        call — the kwargs block is optional, not empty."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filter_emg",
            db_path="/data/exp.duckdb",
            schema_keys=[],
            output_types=["FilteredEMG"],
        )
        assert "{FilteredEMG()});" in cmd
        assert ", ...\n        );" not in cmd


class TestSchemaLevelTriState:
    """``schema_level`` has THREE values and all three must survive generation.

    ``None`` = unspecified (iterate all keys), ``[k...]`` = iterate those,
    ``[]`` = the user deselected every level (iterate nothing — one
    dataset-level call). The generator used to test ``schema_level`` for
    truthiness, so ``[]`` collapsed into ``None`` and a deselect-everything
    node ran the whole schema grid. See
    ``.claude/plan-schema-level-empty-distribute.md``.
    """

    VARIANTS = [
        {
            "input_types": {"sig": "RawEMG"},
            "output_type": "FilteredEMG",
            "constants": {},
        }
    ]

    def _cmd(self, **kwargs):
        from scistack_gui.api.matlab_command import generate_matlab_command

        defaults = dict(
            function_name="filter_emg",
            db_path="/data/exp.duckdb",
            schema_keys=["subject", "session", "cycle"],
            variants=self.VARIANTS,
        )
        defaults.update(kwargs)
        return generate_matlab_command(**defaults)

    def test_none_iterates_every_schema_key(self):
        cmd = self._cmd(schema_level=None)
        assert "'subject', []" in cmd
        assert "'session', []" in cmd
        assert "'cycle', []" in cmd

    def test_subset_iterates_only_those_keys(self):
        cmd = self._cmd(schema_level=["subject"])
        assert "'subject', []" in cmd
        assert "'session'" not in cmd
        assert "'cycle'" not in cmd

    def test_empty_list_iterates_nothing(self):
        """All levels deselected — the run is ONE dataset-level call."""
        cmd = self._cmd(schema_level=[])
        assert "'subject'" not in cmd
        assert "'session'" not in cmd
        assert "'cycle'" not in cmd

    def test_empty_list_with_distribute_still_emits_the_option(self):
        """The reported case: deselect every level + ``distribute=true``.

        Both halves matter. With the schema keys emitted anyway, scifor
        resolves the distribute target from the deepest ITERATED key and lands
        on the level below it ('cycle' here) instead of the top of the schema
        ('subject'), and the run succeeds while saving at the wrong
        granularity.
        """
        cmd = self._cmd(
            schema_level=[],
            run_options={
                "dry_run": False,
                "save": True,
                "distribute": True,
                "as_table": False,
            },
        )
        assert "'distribute', true" in cmd
        assert "'subject'" not in cmd
        assert "'cycle'" not in cmd

    def test_never_run_branch_honors_empty_list(self):
        """The no-variants (first run) branch defaults to all keys for a
        reason (see test_never_run_function_still_iterates_the_schema) — but
        an explicit ``[]`` is a choice, not an absence, and outranks it."""
        cmd = self._cmd(
            variants=[],
            variable_inputs={"sig": "RawEMG"},
            output_types=["FilteredEMG"],
            schema_level=[],
        )
        assert "'subject'" not in cmd
        assert "'cycle'" not in cmd
        assert "scidb.for_each(@filter_emg" in cmd

    def test_pipeline_step_honors_empty_list(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="p1",
            steps=[
                {
                    "function_name": "filter_emg",
                    "variants": self.VARIANTS,
                    "schema_level": [],
                }
            ],
            db_path="/data/exp.duckdb",
            schema_keys=["subject", "session", "cycle"],
        )
        assert "'subject'" not in cmd
        assert "'cycle'" not in cmd


# ---------------------------------------------------------------------------
# _format_sweep tests
# ---------------------------------------------------------------------------


class TestFormatSweep:
    def test_numeric_values(self):
        from scistack_gui.api.matlab_command import _format_sweep

        assert _format_sweep([10, 20, 30]) == "scidb.Parameter(10, 20, 30)"

    def test_string_values(self):
        from scistack_gui.api.matlab_command import _format_sweep

        assert _format_sweep(["low", "high"]) == "scidb.Parameter('low', 'high')"

    def test_single_value(self):
        from scistack_gui.api.matlab_command import _format_sweep

        assert _format_sweep([42]) == "scidb.Parameter(42)"

    def test_dict_value_renders_as_struct_not_a_quoted_repr(self):
        """A dict-valued Parameter (``CONFIG = { a = 1 }`` under
        ``[parameters]``) must reach MATLAB as a struct.

        Regression: everything that was not a scalar or a flat list fell
        through to ``str(val)`` in quotes, so the function received a char
        array holding a Python repr.
        """
        from scistack_gui.api.matlab_command import _format_sweep

        out = _format_sweep([{"order": 4, "cutoff": [10, 400]}])
        assert out == "scidb.Parameter(struct('order', 4, 'cutoff', [10, 400]))"
        assert "'{" not in out


# ---------------------------------------------------------------------------
# _format_matlab_value tests
#
# Every case here is pinned against what +scidb/+internal/from_python.m
# produces for the same value, because the SAME declaration reaches MATLAB
# both ways: inlined into a generated script, and through scidb.entities().
# ---------------------------------------------------------------------------


class TestFormatMatlabValue:
    def test_nested_dict_becomes_nested_struct(self):
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value({"FILTER": {"ORDER": 4}, "VAF": 0.9}) == (
            "struct('FILTER', struct('ORDER', 4), 'VAF', 0.9)"
        )

    def test_empty_dict(self):
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value({}) == "struct()"

    def test_string_list_is_a_string_array_not_concatenated_chars(self):
        """``['HAM', 'RF']`` emitted as ``['HAM', 'RF']`` is the single char
        array ``'HAMRF'`` in MATLAB — square brackets concatenate."""
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value(["HAM", "RF"]) == '["HAM", "RF"]'

    def test_numeric_and_bool_lists(self):
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value([10, 400]) == "[10, 400]"
        assert _format_matlab_value([True, False]) == "[true, false]"

    def test_mixed_list_is_a_cell(self):
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value([1, "a"]) == "{1, 'a'}"

    def test_cell_valued_field_is_double_wrapped(self):
        """``struct('a', {1, 'x'})`` builds a 1x2 STRUCT ARRAY, not a scalar
        struct with a cell field. The extra brace layer is what makes it a
        cell field."""
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value({"a": [1, "x"]}) == "struct('a', {{1, 'x'}})"

    def test_none_is_empty_brackets(self):
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value(None) == "[]"
        assert _format_matlab_value([]) == "[]"

    def test_keys_are_sanitized_like_makeValidName(self):
        """Mirrors pydict_to_struct's matlab.lang.makeValidName, so the two
        routes into MATLAB agree on field names."""
        from scistack_gui.api.matlab_command import _format_matlab_value

        assert _format_matlab_value({"my key": 1}) == "struct('myKey', 1)"
        assert _format_matlab_value({"2bad": 1}) == "struct('x2bad', 1)"
        assert _format_matlab_value({"a-b": 1}) == "struct('a_b', 1)"


# ---------------------------------------------------------------------------
# config MATLAB parsing tests
# ---------------------------------------------------------------------------


class TestConfigMatlabParsing:
    def test_pyproject_with_matlab(self, tmp_path):
        from scistack_gui.config import load_config

        # Create a pyproject.toml with MATLAB section.
        (tmp_path / "pyproject.toml").write_text(
            textwrap.dedent("""\
            [tool.scistack]
            modules = []

            [tool.scistack.matlab]
            functions = ["matlab/bandpass_filter.m"]
            variables = ["matlab/types/*.m"]
            variable_dir = "matlab/types"
        """)
        )

        # Create the referenced files.
        (tmp_path / "matlab").mkdir()
        (tmp_path / "matlab" / "types").mkdir()

        (tmp_path / "matlab" / "bandpass_filter.m").write_text(
            "function y = bandpass_filter(x)\ny = x;\nend\n"
        )
        (tmp_path / "matlab" / "types" / "RawSignal.m").write_text(
            "classdef RawSignal < scidb.BaseVariable\nend\n"
        )

        db_path = tmp_path / "test.duckdb"
        db_path.touch()

        config = load_config(tmp_path, db_path)
        assert len(config.matlab_functions) == 1
        assert config.matlab_functions[0].name == "bandpass_filter.m"
        assert len(config.matlab_variables) == 1
        assert config.matlab_variables[0].name == "RawSignal.m"
        # addpath is auto-derived from parent dirs of functions, variables, and variable_dir
        assert len(config.matlab_addpath) == 2
        # Paths are stored in absolute-but-not-UNC-canonicalized form (see
        # config._normalize); compare against that form, not .resolve().
        addpath_set = set(config.matlab_addpath)
        assert (tmp_path / "matlab") in addpath_set
        assert (tmp_path / "matlab" / "types") in addpath_set
        assert config.matlab_variable_dir == (tmp_path / "matlab" / "types")

    def test_scistack_toml(self, tmp_path):
        from scistack_gui.config import load_config

        # Create a scistack.toml (standalone, no pyproject.toml).
        (tmp_path / "scistack.toml").write_text(
            textwrap.dedent("""\
            modules = []

            [matlab]
            functions = ["process.m"]
        """)
        )

        (tmp_path / "process.m").write_text("function y = process(x)\ny = x;\nend\n")

        db_path = tmp_path / "test.duckdb"
        db_path.touch()

        config = load_config(tmp_path, db_path)
        assert len(config.matlab_functions) == 1

    def test_no_matlab_section(self, tmp_path):
        from scistack_gui.config import load_config

        (tmp_path / "pyproject.toml").write_text(
            textwrap.dedent("""\
            [tool.scistack]
            modules = []
        """)
        )

        db_path = tmp_path / "test.duckdb"
        db_path.touch()

        config = load_config(tmp_path, db_path)
        assert config.matlab_functions == []
        assert config.matlab_variables == []
        assert config.matlab_addpath == []
        assert config.matlab_variable_dir is None
        assert config.matlab_entities_file is None

    def test_explicit_entities_file(self, tmp_path):
        from scistack_gui.config import load_config

        (tmp_path / "pyproject.toml").write_text(
            textwrap.dedent("""\
            [tool.scistack]
            modules = []

            [tool.scistack.matlab]
            functions = ["process.m"]
            entities_file = "scistack_entities.m"
        """)
        )
        (tmp_path / "process.m").write_text("function y = process(x)\ny = x;\nend\n")
        (tmp_path / "scistack_entities.m").write_text(
            "raw_emg = scidb.PathInput('{subject}.mat');\n"
        )

        db_path = tmp_path / "test.duckdb"
        db_path.touch()

        config = load_config(tmp_path, db_path)
        assert len(config.matlab_functions) == 1
        assert config.matlab_entities_file.name == "scistack_entities.m"
        # The entities script's directory joins addpath, so a generated
        # MATLAB command can `run` it.
        assert config.matlab_entities_file.parent in config.matlab_addpath


# ---------------------------------------------------------------------------
# scimatlab MATLAB directory discovery
# ---------------------------------------------------------------------------


class TestGenerateMatlabCommandOutputTypes:
    """Regression: MATLAB function output param names must not leak into the
    generated MATLAB command as BaseVariable class names.

    A function declared as ``function [time, force_left, force_right] = load_csv(f)``
    has output *parameter names* ``time`` / ``force_left`` / ``force_right``.  The
    actual BaseVariable class names are ``Time`` / ``Force_Left`` / ``Force_Right``
    (whatever is wired to the function node's output handles in the GUI).
    The generated MATLAB command must use the class names, not the param names.
    """

    def test_output_types_from_variants_use_class_names(self):
        """When DB variants exist, output_type (class name) must appear in
        the outputs cell array, not the function's output parameter names."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        variants = [
            {
                "input_types": {},
                "output_type": "Time",
                "constants": {},
                "record_count": 1,
            },
            {
                "input_types": {},
                "output_type": "Force_Left",
                "constants": {},
                "record_count": 1,
            },
            {
                "input_types": {},
                "output_type": "Force_Right",
                "constants": {},
                "record_count": 1,
            },
        ]

        cmd = generate_matlab_command(
            function_name="load_csv",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            variants=variants,
        )

        assert "Time()" in cmd
        assert "Force_Left()" in cmd
        assert "Force_Right()" in cmd
        # Lowercase param names must NOT appear as class instantiations
        assert "time()" not in cmd
        assert "force_left()" not in cmd
        assert "force_right()" not in cmd

    def test_output_types_with_no_variants_uses_provided_output_types(self):
        """When no DB variants exist and output_types are provided, the class
        names should appear (not lowercase function param names)."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="load_csv",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            variants=None,
            output_types=["Time", "Force_Left", "Force_Right"],
        )

        assert "Time()" in cmd
        assert "Force_Left()" in cmd
        assert "Force_Right()" in cmd
        assert "time()" not in cmd
        assert "force_left()" not in cmd
        assert "force_right()" not in cmd


class TestDropProjectRootFolder:
    """Old runs recorded with the project root baked into ``root_folder`` must
    not be re-emitted that way, or the divergent key is recorded again and the
    ``__unresolved__`` ghost node never heals."""

    def test_project_root_becomes_none(self):
        from scistack_gui.services.matlab_command_service import (
            _drop_project_root_folder,
        )

        pis = {"filepath": {"template": "data/f.csv", "root_folder": "/proj/exp"}}
        _drop_project_root_folder(pis, "/proj/exp")
        assert pis["filepath"]["root_folder"] is None

    def test_a_real_declared_root_is_kept(self):
        from scistack_gui.services.matlab_command_service import (
            _drop_project_root_folder,
        )

        pis = {"filepath": {"template": "data/f.csv", "root_folder": "/mnt/raw"}}
        _drop_project_root_folder(pis, "/proj/exp")
        assert pis["filepath"]["root_folder"] == "/mnt/raw"

    def test_no_project_root_is_a_no_op(self):
        from scistack_gui.services.matlab_command_service import (
            _drop_project_root_folder,
        )

        pis = {"filepath": {"template": "data/f.csv", "root_folder": "/proj/exp"}}
        _drop_project_root_folder(pis, None)
        assert pis["filepath"]["root_folder"] == "/proj/exp"

    def test_unnormalized_root_still_recognized(self):
        from scistack_gui.services.matlab_command_service import (
            _drop_project_root_folder,
        )

        pis = {"filepath": {"template": "data/f.csv", "root_folder": "/proj/exp/./"}}
        _drop_project_root_folder(pis, "/proj/exp")
        assert pis["filepath"]["root_folder"] is None


class TestSortInferredByParamsOrder:
    def test_reorders_to_match_params(self):
        from scistack_gui.services.matlab_command_service import (
            _sort_inferred_by_params_order,
        )

        inferred = ["Force_Right", "Force_Left", "Time"]
        params = ["time", "force_left", "force_right"]
        result = _sort_inferred_by_params_order(inferred, params)
        assert result == ["Time", "Force_Left", "Force_Right"]

    def test_passthrough_when_already_ordered(self):
        from scistack_gui.services.matlab_command_service import (
            _sort_inferred_by_params_order,
        )

        inferred = ["Time", "Force_Left", "Force_Right"]
        params = ["time", "force_left", "force_right"]
        result = _sort_inferred_by_params_order(inferred, params)
        assert result == ["Time", "Force_Left", "Force_Right"]

    def test_unmatched_appended_at_end(self):
        from scistack_gui.services.matlab_command_service import (
            _sort_inferred_by_params_order,
        )

        inferred = ["Extra", "Time", "Force_Left"]
        params = ["time", "force_left"]
        result = _sort_inferred_by_params_order(inferred, params)
        assert result == ["Time", "Force_Left", "Extra"]

    def test_empty_params_preserves_inferred_order(self):
        from scistack_gui.services.matlab_command_service import (
            _sort_inferred_by_params_order,
        )

        inferred = ["Force_Right", "Time"]
        result = _sort_inferred_by_params_order(inferred, [])
        assert result == ["Force_Right", "Time"]


class TestGroupVariantsNonScalarConstants:
    """A constant is not always a scalar. An inline table under [parameters]
    in scistack_entities.toml *is* the value (docs/claude/entities-toml-format.md
    rule 2), so a project declaring e.g. ``delsys_config = {fs = 2000, ...}``
    hands _group_variants a dict-valued constant.

    Regression: the grouping key was ``tuple(sorted(constants.items()))``,
    hashable only while every value was a scalar. With a dict constant,
    ``grouped.setdefault(key, ...)`` raised ``TypeError: unhashable type:
    'dict'`` and every "Run in MATLAB" for that function failed at
    generate_matlab_command — including the re-run needed to fix a node that
    had been run with the wrong distribute flag.
    """

    def test_dict_constant_groups_without_raising(self):
        from scistack_gui.api.matlab_command import _group_variants

        variants = [
            {
                "input_types": {"raw": "RawEMG"},
                "constants": {"delsys_config": {"fs": 2000, "chans": ["a", "b"]}},
                "output_type": "Filtered",
            }
        ]
        grouped = _group_variants(variants)
        assert len(grouped) == 1
        assert grouped[0]["constants"] == {
            "delsys_config": {"fs": 2000, "chans": ["a", "b"]}
        }
        assert grouped[0]["output_types"] == ["Filtered"]

    def test_multi_output_with_dict_constant_collapses_to_one_call(self):
        """The whole point of the grouping: one for_each call listing every
        output, not one call per output row."""
        from scistack_gui.api.matlab_command import _group_variants

        consts = {"gaitrite_config": {"units": "m"}}
        variants = [
            {"input_types": {"p": "Raw"}, "constants": consts, "output_type": "A"},
            {"input_types": {"p": "Raw"}, "constants": consts, "output_type": "B"},
        ]
        grouped = _group_variants(variants)
        assert len(grouped) == 1
        assert grouped[0]["output_types"] == ["A", "B"]

    def test_differing_dict_constants_stay_separate(self):
        from scistack_gui.api.matlab_command import _group_variants

        variants = [
            {
                "input_types": {"p": "Raw"},
                "constants": {"cfg": {"fs": 2000}},
                "output_type": "A",
            },
            {
                "input_types": {"p": "Raw"},
                "constants": {"cfg": {"fs": 1000}},
                "output_type": "A",
            },
        ]
        assert len(_group_variants(variants)) == 2

    def test_list_constant_groups_without_raising(self):
        from scistack_gui.api.matlab_command import _group_variants

        variants = [
            {
                "input_types": {"p": "Raw"},
                "constants": {"bands": [20, 450]},
                "output_type": "A",
            }
        ]
        grouped = _group_variants(variants)
        assert len(grouped) == 1
        assert grouped[0]["constants"] == {"bands": [20, 450]}


class TestNormalizeInputTypes:
    """derive_target_for_node's never-run fallback (resolve_function_edges)
    returns each input param as a LIST of candidate types — even a single
    candidate is ["RawSignal"], not "RawSignal" — unlike real DB-history
    variants, which are already flat. generate_matlab_pipeline_command
    must flatten this before handing targets to api.matlab_command's
    generator, or a single-candidate list ends up nested inside a dict
    key's tuple and raises `TypeError: unhashable type: 'list'` in
    _group_variants (regression: found via a never-run MATLAB node in
    test_matlab_pipeline_execution.py)."""

    def test_flat_values_pass_through(self):
        from scistack_gui.services.matlab_command_service import (
            _normalize_input_types,
        )

        flat, unresolved = _normalize_input_types({"signal": "RawSignal"})
        assert flat == {"signal": "RawSignal"}
        assert unresolved == []

    def test_single_item_list_collapses_to_scalar(self):
        from scistack_gui.services.matlab_command_service import (
            _normalize_input_types,
        )

        flat, unresolved = _normalize_input_types({"signal": ["RawSignal"]})
        assert flat == {"signal": "RawSignal"}
        assert unresolved == []

    def test_multi_item_list_reported_unresolved(self):
        from scistack_gui.services.matlab_command_service import (
            _normalize_input_types,
        )

        flat, unresolved = _normalize_input_types(
            {"signal": ["RawSignal", "OtherSignal"]}
        )
        assert "signal" not in flat
        assert unresolved == ["signal"]

    def test_mixed_params(self):
        from scistack_gui.services.matlab_command_service import (
            _normalize_input_types,
        )

        flat, unresolved = _normalize_input_types(
            {"a": "Flat", "b": ["OneCandidate"], "c": ["X", "Y"]}
        )
        assert flat == {"a": "Flat", "b": "OneCandidate"}
        assert unresolved == ["c"]


class TestCollectSweepParams:
    """_collect_sweep_params mirrors PathInput's edge collection
    (matlab_command_service's "Source 2"), but a Parameter has no DB-history
    source at all — the registry + edges are the ONLY source.

    Both now route through the shared edge_resolver rather than hand-rolling
    the scan, so the MATLAB generators and the Python execution path agree on
    what a given canvas means."""

    def _edge(self, source, target, target_handle):
        return {"source": source, "target": target, "targetHandle": target_handle}

    def test_wires_parameter_to_the_param_its_handle_names(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_sweep_params,
        )

        edges = [self._edge("param__low_hz", "fn__bandpass_filter", "in__low_hz")]
        result = _collect_sweep_params(
            "bandpass_filter", {"low_hz": [10, 20, 30]}, edges, {}
        )
        assert result == {"low_hz": [10, 20, 30]}

    def test_declared_name_may_differ_from_the_param(self):
        """Keyed by the PARAM the edge names, looked up by the DECLARED name
        — the distinction the old hand-rolled scan collapsed."""
        from scistack_gui.services.matlab_command_service import (
            _collect_sweep_params,
        )

        edges = [self._edge("param__test", "fn__bandpass_filter", "in__low_hz")]
        result = _collect_sweep_params(
            "bandpass_filter", {"test": [10, 20]}, edges, {}
        )
        assert result == {"low_hz": [10, 20]}

    def test_ignores_edges_for_other_functions(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_sweep_params,
        )

        edges = [self._edge("param__low_hz", "fn__other_fn", "in__low_hz")]
        result = _collect_sweep_params(
            "bandpass_filter", {"low_hz": [10, 20, 30]}, edges, {}
        )
        assert result == {}

    def test_ignores_non_parameter_source_edges(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_sweep_params,
        )

        edges = [self._edge("var__low_hz", "fn__bandpass_filter", "in__low_hz")]
        result = _collect_sweep_params(
            "bandpass_filter", {"low_hz": [10, 20, 30]}, edges, {}
        )
        assert result == {}

    def test_parameter_name_not_in_saved_sweeps_is_skipped(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_sweep_params,
        )

        edges = [self._edge("param__unknown", "fn__bandpass_filter", "in__low_hz")]
        result = _collect_sweep_params(
            "bandpass_filter", {"low_hz": [10, 20, 30]}, edges, {}
        )
        assert result == {}

    def test_placement_qualified_fn_endpoint_is_recognised(self):
        """A graduated node's edge carries fn__{name}__{wiring}::{scope};
        the shared _fn_node_ids adopts it, where the old prefix-only scan
        matched on a bare split and could miss it."""
        from scistack_gui.services.matlab_command_service import (
            _collect_sweep_params,
        )

        edges = [
            self._edge(
                "param__low_hz",
                "fn__bandpass_filter__0123456789abcdef::main",
                "in__low_hz",
            )
        ]
        result = _collect_sweep_params(
            "bandpass_filter", {"low_hz": [10]}, edges, {}
        )
        assert result == {"low_hz": [10]}


class TestCollectEdgePathInputs:
    """The PathInput half of the same shared resolution."""

    def _edge(self, source, target, target_handle):
        return {"source": source, "target": target, "targetHandle": target_handle}

    def test_declared_name_may_differ_from_the_param(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_edge_path_inputs,
        )

        edges = [
            self._edge("pathInput__test_pi", "fn__load_raw", "in__filepath")
        ]
        result = _collect_edge_path_inputs(
            "load_raw",
            {"test_pi": {"template": "{subject}.csv", "root_folder": "/data"}},
            edges,
            {},
        )
        assert result == {
            "filepath": {"template": "{subject}.csv", "root_folder": "/data"}
        }

    def test_unknown_declared_name_is_skipped(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_edge_path_inputs,
        )

        edges = [self._edge("pathInput__gone", "fn__load_raw", "in__filepath")]
        result = _collect_edge_path_inputs("load_raw", {}, edges, {})
        assert result == {}

    def test_ignores_edges_for_other_functions(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_edge_path_inputs,
        )

        edges = [self._edge("pathInput__test_pi", "fn__other", "in__filepath")]
        result = _collect_edge_path_inputs(
            "load_raw", {"test_pi": {"template": "x.csv"}}, edges, {}
        )
        assert result == {}


class TestMatlabFnProxyHash:
    """Fix A — the proxy hash must match what MATLAB's scidb.LineageFcn(fn)
    (unpack_output=false default) produces, so scihist.check_node_state does
    not report every combo as "stale: function hash changed"."""

    def test_proxy_uses_unpack_false(self, monkeypatch):
        from hashlib import sha256

        from scistack_gui import matlab_registry as _mr
        from scistack_gui.api.pipeline import _build_matlab_fn_proxy

        class FakeInfo:
            source_hash = "a" * 64
            n_outputs = 3
            params = ("x",)
            output_names = ("a", "b", "c")

        monkeypatch.setattr(_mr, "get_matlab_function", lambda _name: FakeInfo())

        proxy = _build_matlab_fn_proxy("load_csv")
        expected = sha256(f"{FakeInfo.source_hash}-False".encode()).hexdigest()
        assert proxy.hash == expected
        assert proxy.unpack_output is False

    def test_single_output_hash_also_unpack_false(self, monkeypatch):
        from hashlib import sha256

        from scistack_gui import matlab_registry as _mr
        from scistack_gui.api.pipeline import _build_matlab_fn_proxy

        class FakeInfo:
            source_hash = "b" * 64
            n_outputs = 1
            params = ()
            output_names = ("only",)

        monkeypatch.setattr(_mr, "get_matlab_function", lambda _name: FakeInfo())
        proxy = _build_matlab_fn_proxy("fn")
        expected = sha256(f"{FakeInfo.source_hash}-False".encode()).hexdigest()
        assert proxy.hash == expected


class TestMatlabParamToClassFromDb:
    """The DB-derived half of ``matlab_param_to_class``.

    Regression: ``get_aggregated_variants()`` built its variant dicts without
    ``output_num`` even though ``list_pipeline_variants()`` supplied it, so
    this source was empty for EVERY MATLAB fn and the fn->output edge silently
    depended on a hand-drawn manual edge. See
    ``test_variant_queries.py::test_aggregation_preserves_variant_query_output_num``
    for the scidb half of the contract.
    """

    @staticmethod
    def _agg(*variants, fn="loadDelsysEMGOneFile", call_id="5f0b6fe9"):
        return {(fn, call_id): {"variants": list(variants)}}

    def test_output_num_maps_to_declared_output_name(self):
        from scistack_gui.api.pipeline import _matlab_param_to_class_from_db

        result = _matlab_param_to_class_from_db(
            self._agg({"output_type": "RawEMG", "output_num": 0}),
            {"loadDelsysEMGOneFile"},
            {"loadDelsysEMGOneFile": ("loaded_data",)},
        )

        assert result == {"loadDelsysEMGOneFile": {"loaded_data": "RawEMG"}}

    def test_each_slot_maps_to_its_own_output_name(self):
        from scistack_gui.api.pipeline import _matlab_param_to_class_from_db

        result = _matlab_param_to_class_from_db(
            self._agg(
                {"output_type": "RawEMG", "output_num": 0},
                {"output_type": "Cycles", "output_num": 1},
            ),
            {"loadDelsysEMGOneFile"},
            {"loadDelsysEMGOneFile": ("loaded_data", "cycles")},
        )

        assert result == {
            "loadDelsysEMGOneFile": {"loaded_data": "RawEMG", "cycles": "Cycles"}
        }

    def test_missing_output_num_contributes_nothing(self, caplog):
        """The pre-fix behaviour, kept deliberately: fall through to the
        manual-edge source rather than guessing a slot."""
        from scistack_gui.api.pipeline import _matlab_param_to_class_from_db

        with caplog.at_level(logging.INFO):
            result = _matlab_param_to_class_from_db(
                self._agg({"output_type": "RawEMG", "output_num": None}),
                {"loadDelsysEMGOneFile"},
                {"loadDelsysEMGOneFile": ("loaded_data",)},
            )

        assert result == {}
        assert "DB source contributes nothing" in caplog.text

    def test_out_of_range_output_num_is_skipped(self, caplog):
        from scistack_gui.api.pipeline import _matlab_param_to_class_from_db

        with caplog.at_level(logging.INFO):
            result = _matlab_param_to_class_from_db(
                self._agg({"output_type": "RawEMG", "output_num": 7}),
                {"loadDelsysEMGOneFile"},
                {"loadDelsysEMGOneFile": ("loaded_data",)},
            )

        assert result == {}
        assert "out of range" in caplog.text

    def test_non_matlab_functions_are_ignored(self):
        from scistack_gui.api.pipeline import _matlab_param_to_class_from_db

        result = _matlab_param_to_class_from_db(
            self._agg({"output_type": "Filtered", "output_num": 0}, fn="python_fn"),
            {"loadDelsysEMGOneFile"},
            {"loadDelsysEMGOneFile": ("loaded_data",)},
        )

        assert result == {}


class TestGenerateMatlabPipelineCommand:
    """generate_matlab_pipeline_command (whole-pipeline MATLAB execution,
    plan-matlab-pipeline-execution.md Stage 1)."""

    def _two_step_pipeline(self):
        return [
            {
                "function_name": "load_csv",
                "variants": [
                    {
                        "input_types": {},
                        "output_type": "RawSignal",
                        "constants": {},
                    }
                ],
            },
            {
                "function_name": "bandpass_filter",
                "variants": [
                    {
                        "input_types": {"signal": "RawSignal"},
                        "output_type": "FilteredSignal",
                        "constants": {"low_hz": 20},
                    }
                ],
            },
        ]

    def test_wraps_steps_in_scidb_pipeline(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="gait_analysis",
            steps=self._two_step_pipeline(),
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
        )

        assert "pipe = scidb.Pipeline('gait_analysis');" in cmd
        assert cmd.count("scidb.for_each") == 2
        assert "@load_csv" in cmd
        assert "@bandpass_filter" in cmd
        assert "pipe.run_all(" in cmd

    def test_registration_order_independent(self):
        """Pipeline.m's execution_order() topo-sorts server-side — the
        script may register steps in any order."""
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        steps = self._two_step_pipeline()
        cmd_forward = generate_matlab_pipeline_command(
            pipeline_id="p", steps=steps, db_path="/db.duckdb", schema_keys=["s"]
        )
        cmd_reversed = generate_matlab_pipeline_command(
            pipeline_id="p",
            steps=list(reversed(steps)),
            db_path="/db.duckdb",
            schema_keys=["s"],
        )
        assert cmd_forward.count("scidb.for_each") == cmd_reversed.count(
            "scidb.for_each"
        )
        assert "@load_csv" in cmd_reversed
        assert "@bandpass_filter" in cmd_reversed

    def test_mode_until_calls_run_until_with_target(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="p",
            steps=self._two_step_pipeline(),
            db_path="/db.duckdb",
            schema_keys=["s"],
            mode="until",
            target="bandpass_filter",
        )
        assert "pipe.run_until('bandpass_filter'" in cmd
        assert "pipe.run_all(" not in cmd

    def test_mode_until_requires_target(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        with pytest.raises(ValueError):
            generate_matlab_pipeline_command(
                pipeline_id="p",
                steps=self._two_step_pipeline(),
                db_path="/db.duckdb",
                schema_keys=["s"],
                mode="until",
                target="",
            )

    def test_mode_endpoints_calls_run_endpoints(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="p",
            steps=self._two_step_pipeline(),
            db_path="/db.duckdb",
            schema_keys=["s"],
            mode="endpoints",
            finalized=True,
        )
        assert "pipe.run_endpoints(" in cmd
        assert "'include_used', true" in cmd
        assert "'finalized', true" in cmd

    def test_show_mode_rejected(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        with pytest.raises(ValueError):
            generate_matlab_pipeline_command(
                pipeline_id="p",
                steps=self._two_step_pipeline(),
                db_path="/db.duckdb",
                schema_keys=["s"],
                mode="show",
                target="bandpass_filter",
            )

    def test_step_with_no_variants_skipped_with_comment(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        steps = self._two_step_pipeline()
        steps.append({"function_name": "never_run_fn", "variants": []})
        cmd = generate_matlab_pipeline_command(
            pipeline_id="p", steps=steps, db_path="/db.duckdb", schema_keys=["s"]
        )
        assert "SKIPPED: 'never_run_fn'" in cmd
        assert "@never_run_fn" not in cmd
        # The two runnable steps still registered.
        assert cmd.count("scidb.for_each") == 2

    def test_no_runnable_steps_raises(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        with pytest.raises(ValueError):
            generate_matlab_pipeline_command(
                pipeline_id="p",
                steps=[{"function_name": "never_run_fn", "variants": []}],
                db_path="/db.duckdb",
                schema_keys=["s"],
            )

    def test_variable_registration_union_across_steps(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="p",
            steps=self._two_step_pipeline(),
            db_path="/db.duckdb",
            schema_keys=["s"],
        )
        assert cmd.count("scidb.register_variable(RawSignal())") == 1
        assert cmd.count("scidb.register_variable(FilteredSignal())") == 1

    def test_step_sweep_rendered_as_scifor_sweep(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        steps = self._two_step_pipeline()
        # window_seconds is NOT among this step's fixture "constants" —
        # picking a param already in "constants" would collide, since
        # _for_each_call_lines applies constants after sweeps and would
        # silently overwrite the sweep-formatted value for that key.
        steps[1]["sweeps"] = {"window_seconds": [10, 20, 30]}
        cmd = generate_matlab_pipeline_command(
            pipeline_id="p", steps=steps, db_path="/db.duckdb", schema_keys=["s"]
        )
        assert "scidb.Parameter(10, 20, 30)" in cmd

    def test_pyenv_preamble_present(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="p",
            steps=self._two_step_pipeline(),
            db_path="/db.duckdb",
            schema_keys=["s"],
            python_executable="/usr/bin/python3",
        )
        assert "pyenv('Version', scistack_pyenv_target__)" in cmd
        pyenv_idx = cmd.index("scistack_pyenv_target__")
        pipe_idx = cmd.index("pipe = scidb.Pipeline(")
        assert pyenv_idx < pipe_idx

    def test_addpath_and_configure_database_emitted_once(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="p",
            steps=self._two_step_pipeline(),
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            addpath_dirs=["/home/user/matlab/lib"],
        )
        assert cmd.count("addpath('/home/user/matlab/lib')") == 1
        assert cmd.count("scihist.configure_database(") == 1
        assert cmd.count("scidb.close_database(db)") == 2  # success path + catch

    def test_multi_output_function_collapses_to_one_call(self):
        """Same grouping behavior as generate_matlab_command: multiple
        variant rows sharing (input_types, constants) but different
        output_type collapse into one for_each with a multi-item outputs
        cell."""
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        steps = [
            {
                "function_name": "load_csv",
                "variants": [
                    {"input_types": {}, "output_type": "Time", "constants": {}},
                    {"input_types": {}, "output_type": "Force_Left", "constants": {}},
                ],
            }
        ]
        cmd = generate_matlab_pipeline_command(
            pipeline_id="p", steps=steps, db_path="/db.duckdb", schema_keys=["s"]
        )
        assert cmd.count("scidb.for_each") == 1
        assert "{Time(), Force_Left()}" in cmd

    def test_mixed_language_pipeline_only_registers_matlab_steps(self):
        """Regression guard for the mixed-pipeline scope decision: the
        MATLAB script generator only ever sees the steps its caller
        (matlab_command_service.generate_matlab_pipeline_command) already
        filtered to MATLAB functions — passing a step list that mirrors
        'a Python node was excluded upstream' (i.e. simply absent here)
        must still produce a clean script for the remaining MATLAB steps."""
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        # Only the MATLAB step is passed — as the service layer would do
        # after filtering out a co-scoped Python function node.
        steps = [self._two_step_pipeline()[1]]  # bandpass_filter only
        cmd = generate_matlab_pipeline_command(
            pipeline_id="p", steps=steps, db_path="/db.duckdb", schema_keys=["s"]
        )
        assert cmd.count("scidb.for_each") == 1
        assert "@bandpass_filter" in cmd
        assert "@load_csv" not in cmd


class TestPreambleTimingInstrumentation:
    """The generated script must time its own preamble.

    A short MATLAB run measured ~4.4s between the GUI emitting the script and
    MATLAB's first scidb log line, against ~0.43s of logged scidb work — and
    nothing in that window was attributable, because neither side timestamped
    it. These assertions pin the instrumentation that closes the gap; they are
    about observability, not formatting, so they check the pieces a later
    optimization has to be able to measure against.
    """

    def _variant_cmd(self, **kwargs):
        from scistack_gui.api.matlab_command import generate_matlab_command

        defaults = dict(
            function_name="bandpass_filter",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            variants=[
                {
                    "input_types": {"signal": "RawSignal"},
                    "output_type": "FilteredSignal",
                    "constants": {},
                }
            ],
        )
        defaults.update(kwargs)
        return generate_matlab_command(**defaults)

    def test_wall_clock_stamp_is_first_executable_line(self):
        """The stamp pairs with the extension's own sendText timestamp; it
        must precede every section so the dispatch gap is fully covered."""
        cmd = self._variant_cmd(python_executable="/venv/bin/python")

        assert "[SciStack][timing] script_start" in cmd
        assert "datestr(now, 'yyyy-mm-dd HH:MM:SS.FFF')" in cmd
        assert cmd.index("script_start") < cmd.index("scistack_pyenv_target__")

    def test_each_emitted_section_is_timed(self):
        cmd = self._variant_cmd(
            python_executable="/venv/bin/python",
            addpath_dirs=["/matlab/lib"],
            project_root="/proj",
            entities_file="/proj/src/scistack_entities.toml",
        )

        for label in (
            "pyenv_preamble",
            "addpath",
            "project_root",
            "entities",
            "configure_database",
            "register_variables",
        ):
            assert f"[SciStack][timing] {label} %.3fs" in cmd

    def test_absent_sections_emit_no_timing_line(self):
        """A section that wasn't generated must not appear as a 0.000s phase
        line — that reads as "ran and was free" rather than "didn't run"."""
        cmd = self._variant_cmd()

        assert "[SciStack][timing] addpath %.3fs" not in cmd
        assert "[SciStack][timing] pyenv_preamble %.3fs" not in cmd
        assert "[SciStack][timing] configure_database %.3fs" in cmd

    def test_summary_names_only_the_sections_that_ran(self):
        """The summary must not report phases the script never emitted.

        `pyenv=0.000s` for a script with no pyenv preamble reads as "ran and
        was free" rather than "wasn't there" — and it would put the word back
        into a script whose contract (test_pyenv_preamble_omitted_when_none)
        is that no pyenv code is emitted at all. Every variable the summary
        reads is therefore assigned by its own section, upstream of it.
        """
        cmd = self._variant_cmd()  # no python_executable, no addpath

        assert "pyenv" not in cmd
        assert "addpath" not in cmd
        # What did run is still named, and assigned before the summary reads it.
        assert "configure_database=%.3fs" in cmd
        assert cmd.index("scistack_t_configure_db__ = toc(") < cmd.index(
            "[timing] matlab_preamble"
        )

    def test_summary_includes_every_emitted_section(self):
        cmd = self._variant_cmd(
            python_executable="/venv/bin/python",
            addpath_dirs=["/matlab/lib"],
            project_root="/proj",
            entities_file="/proj/src/scistack_entities.toml",
        )

        summary = next(
            line for line in cmd.splitlines() if "[timing] matlab_preamble" in line
        )
        for label in (
            "pyenv_preamble",
            "addpath",
            "project_root",
            "entities",
            "configure_database",
            "register_variables",
        ):
            assert f"{label}=%.3fs" in summary

    def test_consolidated_summary_precedes_the_run(self):
        """Emitted after configure_database (first point where both Python
        and +scidb are reachable) and before for_each, so it lands in
        scidb.log immediately ahead of `for_each_prepare returned in …`."""
        cmd = self._variant_cmd()

        assert "scidb.Log.info('[timing] matlab_preamble: TOTAL=%.3fs" in cmd
        assert cmd.index("matlab_preamble") < cmd.index("scidb.for_each")

    def test_never_emits_the_scihist_for_each_shim(self):
        """The generator must call scidb.for_each, never the scihist shim.

        +scihist/for_each.m is a wrapper, and a wrapper is an extra call frame
        between the user's statement and scidb.for_each. scidb.for_each reads
        `nargout` to decide whether to convert the Python result table back into
        a MATLAB table (~385s of a 641s run on 2026-09-13 when it does), and
        going through a wrapper that assigns the result makes that nargout 1
        even for a bare-statement call. Emitting scidb.for_each directly keeps
        the arity the user actually wrote.
        """
        cmd = self._variant_cmd()

        assert "scihist.for_each" not in cmd
        assert "scidb.for_each" in cmd

    def test_total_is_emitted_after_the_try_block_and_clears_temporaries(self):
        """`run(...)` evaluates in the caller's workspace, so the script's own
        temporaries must not be left next to the user's variables."""
        cmd = self._variant_cmd()

        assert cmd.index("[timing] matlab_script: TOTAL=") > cmd.index(
            "rethrow(scistack_err__);"
        )
        assert "clear scistack_script_t0__ scistack_section_t0__" in cmd
        assert cmd.rstrip().endswith(";")

    def test_template_branch_is_instrumented_too(self):
        """The never-run-function branch returns early — it needs its own
        summary and total, or first runs are the ones left unmeasured."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="brand_new",
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
        )

        assert "[SciStack][timing] script_start" in cmd
        assert "[timing] matlab_preamble" in cmd
        assert "[timing] matlab_script: TOTAL=" in cmd

    def test_pipeline_command_is_instrumented(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="gait",
            steps=[
                {
                    "function_name": "load_csv",
                    "variants": [
                        {
                            "input_types": {},
                            "output_type": "RawSignal",
                            "constants": {},
                        }
                    ],
                }
            ],
            db_path="/data/exp.duckdb",
            schema_keys=["subject"],
            addpath_dirs=["/matlab/lib"],
        )

        assert "[SciStack][timing] script_start" in cmd
        assert "[SciStack][timing] addpath %.3fs" in cmd
        assert cmd.index("[timing] matlab_preamble") < cmd.index("pipe.run_all(")
        assert "[timing] matlab_script: TOTAL=" in cmd


class TestFindSciMatlabMatlabDir:
    def test_finds_matlab_dir(self):
        """scimatlab is installed in this environment; its matlab/ dir must be found."""
        from scistack_gui.server import _find_scimatlab_matlab_dir

        result = _find_scimatlab_matlab_dir()
        assert result is not None, (
            "scimatlab is installed but _find_scimatlab_matlab_dir returned None"
        )
        d = Path(result)
        assert d.is_dir(), f"Expected a directory at {result}"
        # The directory must contain the +scihist, +scidb, +scifor MATLAB packages.
        assert (d / "+scihist").is_dir(), f"+scihist not found under {result}"
        assert (d / "+scidb").is_dir(), f"+scidb not found under {result}"
        assert (d / "+scifor").is_dir(), f"+scifor not found under {result}"

    def test_close_database_helper_present(self):
        """Regression: +scidb/close_database.m must exist so matlab_command.py
        can call scidb.close_database(db) for post-close lock-release logging.
        """
        from scistack_gui.server import _find_scimatlab_matlab_dir

        result = _find_scimatlab_matlab_dir()
        assert result is not None
        close_db = Path(result) / "+scidb" / "close_database.m"
        assert close_db.exists(), f"scidb.close_database not found at {close_db}"
        contents = close_db.read_text()
        # The RELEASED log MUST fire after close returns, not before.
        # Use rfind so docstring mentions of these strings (which appear
        # before the code) don't mask the real code-order check.
        release_idx = contents.rfind("DuckDB lock RELEASED")
        close_idx = contents.rfind("db.close()")
        assert 0 < close_idx < release_idx, (
            "RELEASED log must appear after db.close() in close_database.m"
        )
        # A close error must be logged and rethrown (not silently swallowed).
        assert "db.close FAILED" in contents
        assert "rethrow(close_err__)" in contents

    def test_scihist_configure_database_present(self):
        """Regression: +scihist/configure_database.m must exist so MATLAB can call it."""
        from scistack_gui.server import _find_scimatlab_matlab_dir

        result = _find_scimatlab_matlab_dir()
        assert result is not None
        cfg_db = Path(result) / "+scihist" / "configure_database.m"
        assert cfg_db.exists(), f"scihist.configure_database not found at {cfg_db}"


# ---------------------------------------------------------------------------
# TOML entities -> MATLAB (.claude/plan-entities-toml-26-08-31.md Stage 5)
# ---------------------------------------------------------------------------


class TestMatlabEntitiesBridge:
    """``+scidb/entities.m`` rebuilds MATLAB objects from this payload, so
    its shape is the contract. There is no MATLAB in this environment --
    these cover the Python half; the .m half stays correct-by-inspection
    until someone runs it (same standing caveat as
    docs/claude/entity-editability-model.md)."""

    def _project(self, tmp_path, body):
        (tmp_path / "scistack.toml").write_text(
            'entities_file = "entities.toml"\n', encoding="utf-8"
        )
        (tmp_path / "entities.toml").write_text(body, encoding="utf-8")

    def test_payload_shape(self, tmp_path):
        from scidb import entities
        from scimatlab.bridge import load_entities

        self._project(
            tmp_path,
            'variables = ["StepLength"]\n'
            "\n"
            "[parameters]\n"
            "WINDOW = [10, 20]\n"
            "RATE = 1000\n"
            "\n"
            "[path_inputs]\n"
            'EMG = { template = "{subject}/emg.csv", root_folder = "/data" }\n',
        )
        entities.clear_cache()

        payload = load_entities(str(tmp_path))

        assert payload["variables"] == ["StepLength"]
        assert payload["parameters"] == {"WINDOW": [10, 20], "RATE": [1000]}
        assert payload["path_inputs"] == {
            "EMG": [{"template": "{subject}/emg.csv", "root_folder": "/data"}]
        }
        assert payload["errors"] == []

    def test_missing_root_folder_is_none_not_empty_string(self, tmp_path):
        """MATLAB reads None as [] and branches on isempty; "" would be a
        root folder named the empty string."""
        from scidb import entities
        from scimatlab.bridge import load_entities

        self._project(tmp_path, '[path_inputs]\nEMG = "{subject}/emg.csv"\n')
        entities.clear_cache()

        payload = load_entities(str(tmp_path))

        assert payload["path_inputs"]["EMG"][0]["root_folder"] is None

    def test_alternate_templates_become_multiple_arms(self, tmp_path):
        from scidb import entities
        from scimatlab.bridge import load_entities

        self._project(tmp_path, '[path_inputs]\nEMG = ["a/{s}.csv", "b/{s}.csv"]\n')
        entities.clear_cache()

        payload = load_entities(str(tmp_path))

        assert [arm["template"] for arm in payload["path_inputs"]["EMG"]] == [
            "a/{s}.csv",
            "b/{s}.csv",
        ]

    def test_rejected_entries_are_reported_as_strings(self, tmp_path):
        """Someone running from the MATLAB prompt never sees the GUI's
        load-errors panel, so the errors have to cross the bridge."""
        from scidb import entities
        from scimatlab.bridge import load_entities

        self._project(
            tmp_path,
            '[path_inputs]\nBAD = { template = "x.csv", nonsense = 1 }\n',
        )
        entities.clear_cache()

        payload = load_entities(str(tmp_path))

        assert len(payload["errors"]) == 1
        assert "BAD" in payload["errors"][0]
        assert all(isinstance(e, str) for e in payload["errors"])

    def test_entities_m_exists_and_is_reachable(self):
        from scistack_gui.server import _find_scimatlab_matlab_dir

        result = _find_scimatlab_matlab_dir()
        assert result is not None
        assert (Path(result) / "+scidb" / "entities.m").exists()


class TestMaterializeVariableStubs:
    """A TOML-declared Variable needs a real classdef: MATLAB cannot create
    a class at runtime, and class(obj) is what names the table."""

    def test_creates_a_stub_per_declared_name(self, tmp_path):
        from scistack_gui.matlab_registry import materialize_variable_stubs

        created = materialize_variable_stubs(["StepLength", "Cadence"], tmp_path)

        assert {p.name for p in created} == {"StepLength.m", "Cadence.m"}
        assert "classdef StepLength < scidb.BaseVariable" in (
            (tmp_path / "StepLength.m").read_text()
        )

    def test_existing_classdef_is_never_overwritten(self, tmp_path):
        from scistack_gui.matlab_registry import materialize_variable_stubs

        handwritten = tmp_path / "StepLength.m"
        handwritten.write_text("classdef StepLength < scidb.BaseVariable\n% mine\nend\n")

        created = materialize_variable_stubs(["StepLength"], tmp_path)

        assert created == []
        assert "% mine" in handwritten.read_text()

    def test_is_idempotent(self, tmp_path):
        from scistack_gui.matlab_registry import materialize_variable_stubs

        materialize_variable_stubs(["StepLength"], tmp_path)
        assert materialize_variable_stubs(["StepLength"], tmp_path) == []

    def test_stub_for_a_removed_declaration_is_left_alone(self, tmp_path):
        """Deleting generated-but-referenced files is how a pipeline stops
        running mid-session; the project's ethos is hide, never delete."""
        from scistack_gui.matlab_registry import materialize_variable_stubs

        materialize_variable_stubs(["Gone"], tmp_path)
        materialize_variable_stubs([], tmp_path)

        assert (tmp_path / "Gone.m").exists()

    def test_name_with_a_classdef_elsewhere_is_not_shadowed(self, tmp_path):
        """Two classdefs for one type on the MATLAB path shadow each other,
        and the hand-written one is the declaration."""
        from scistack_gui import matlab_registry
        from scistack_gui.matlab_registry import materialize_variable_stubs

        handwritten = tmp_path / "src" / "StepLength.m"
        handwritten.parent.mkdir()
        handwritten.write_text("classdef StepLength < scidb.BaseVariable\nend\n")
        matlab_registry._matlab_variables["StepLength"] = handwritten

        stub_dir = tmp_path / "stubs"
        created = materialize_variable_stubs(["StepLength"], stub_dir)

        assert created == []
        assert not stub_dir.exists()

    def test_falls_back_to_the_default_dir_when_none_is_configured(self, tmp_path):
        """The failure this fixes: with no [matlab] variable_dir, nothing
        was written at all and the run died with 'Unrecognized function or
        variable' from inside for_each."""
        from scimatlab.stubs import DEFAULT_STUB_DIRNAME
        from scistack_gui.matlab_registry import materialize_variable_stubs

        (tmp_path / "scistack.toml").write_text(
            'entities_file = "src/scistack_entities.toml"\n', encoding="utf-8"
        )
        (tmp_path / "src").mkdir()
        entities = tmp_path / "src" / "scistack_entities.toml"
        entities.write_text('variables = ["RawEMG"]\n', encoding="utf-8")

        created = materialize_variable_stubs(
            ["RawEMG"], None, project_start=entities
        )

        expected = tmp_path / "src" / DEFAULT_STUB_DIRNAME / "RawEMG.m"
        assert created == [expected]
        assert "classdef RawEMG < scidb.BaseVariable" in expected.read_text()


class TestMatlabFunctionPrecedence:
    """From the 2026-09-01 log: after a shared code-libraries folder was added,
    the library's ``plot_EMG_timeseries_SPM`` overwrote the project's own copy
    purely because it was walked second. Editing the project file then did
    nothing, and the only trace was a single WARN among hundreds of lines.
    """

    @staticmethod
    def _info(name, path):
        from scistack_gui.matlab_parser import MatlabFunctionInfo

        return MatlabFunctionInfo(
            name=name, file_path=path, params=[], source_hash="0" * 64
        )

    @staticmethod
    def _configure(monkeypatch, root):
        import types

        from scistack_gui import matlab_registry

        monkeypatch.setattr(
            matlab_registry, "_config", types.SimpleNamespace(project_root=root)
        )

    def test_project_wins_regardless_of_scan_order(self, monkeypatch, tmp_path):
        from scistack_gui import matlab_registry

        root = tmp_path / "proj"
        project = root / "src" / "plot_EMG_timeseries_SPM.m"
        library = tmp_path / "libs" / "table-spm" / "plot_EMG_timeseries_SPM.m"
        self._configure(monkeypatch, root)

        for first, second in (
            (project, library),  # project scanned first — the log's order
            (library, project),  # and the reverse, to pin order-independence
        ):
            matlab_registry._matlab_functions.clear()
            matlab_registry._register_matlab_function(
                self._info("plot_EMG_timeseries_SPM", first)
            )
            matlab_registry._register_matlab_function(
                self._info("plot_EMG_timeseries_SPM", second)
            )

            winner = matlab_registry._matlab_functions["plot_EMG_timeseries_SPM"]
            assert winner.file_path == project, (
                f"library won when scanned as {'second' if second is library else 'first'}"
            )

    def test_two_library_definitions_still_warn(self, monkeypatch, tmp_path, caplog):
        """``energy_tkeo`` in the log: two copies inside the same libraries
        tree. Neither is more specific to the project, so the choice really is
        arbitrary and the warning stays."""
        from scistack_gui import matlab_registry

        root = tmp_path / "proj"
        self._configure(monkeypatch, root)

        with caplog.at_level(logging.WARNING):
            matlab_registry._register_matlab_function(
                self._info("energy_tkeo", tmp_path / "libs" / "a" / "energy_tkeo.m")
            )
            matlab_registry._register_matlab_function(
                self._info("energy_tkeo", tmp_path / "libs" / "b" / "energy_tkeo.m")
            )

        assert "shadows previous definition" in caplog.text
        assert (
            matlab_registry._matlab_functions["energy_tkeo"].file_path
            == tmp_path / "libs" / "b" / "energy_tkeo.m"
        )

    def test_builtin_reference_keeps_last_one_wins(self, monkeypatch, tmp_path):
        """A builtin has no backing file, so there is no tier to compare and
        the old behaviour must be preserved — replay_persisted_builtins relies
        on it."""
        from scistack_gui import matlab_registry

        root = tmp_path / "proj"
        self._configure(monkeypatch, root)

        matlab_registry._register_matlab_function(
            self._info("mean", root / "src" / "mean.m")
        )
        matlab_registry._register_matlab_function(self._info("mean", None))

        assert matlab_registry._matlab_functions["mean"].file_path is None


class TestGeneratedStubAttribution:
    """A generated classdef is output of the TOML declaration, not a rival to it.

    Regression: materialize_variable_stubs wrote the stub and then attributed
    it to its own .m path, so the very same scan warned that the variable was
    "declared in more than one place" and resolved the tie in favour of the
    file it had just generated — with the tie-break being directory scan order.
    It also left registry._variable_sources pointing at the .m file, where
    reload_entities_file's prune of the TOML source could no longer find it.
    """

    @staticmethod
    def _project(tmp_path):
        entities = tmp_path / "scistack_entities.toml"
        entities.write_text('variables = ["RawEMG"]\n', encoding="utf-8")
        return entities, tmp_path / "scistack_matlab_variables"

    def test_generated_stub_is_attributed_to_its_declaring_toml(
        self, tmp_path, caplog
    ):
        from scistack_gui import registry
        from scistack_gui.matlab_registry import materialize_variable_stubs

        entities, stub_dir = self._project(tmp_path)
        # What registry._load_entities_file already did with the TOML by the
        # time the MATLAB half of the scan runs.
        registry._register_variable("RawEMG", source=str(entities))

        with caplog.at_level(logging.DEBUG):
            materialize_variable_stubs(["RawEMG"], stub_dir, project_start=entities)

        assert "declared in more than one place" not in caplog.text
        assert registry._variable_sources["RawEMG"] == str(entities)
        # The warning is gone, but the linkage must still be observable.
        assert "Attributing generated classdef" in caplog.text

    def test_second_scan_over_an_existing_stub_is_still_the_tomls(
        self, tmp_path, caplog
    ):
        """The first scan creates the stub; the second finds it on disk and
        takes the ``skipped`` branch. Both must attribute to the TOML — a stub
        written by a previous session is no more a declaration than a fresh one.
        """
        from scistack_gui import matlab_registry, registry
        from scistack_gui.matlab_registry import materialize_variable_stubs

        entities, stub_dir = self._project(tmp_path)
        registry._register_variable("RawEMG", source=str(entities))
        materialize_variable_stubs(["RawEMG"], stub_dir, project_start=entities)

        # A fresh scan: registries start empty, but the stub file persists.
        matlab_registry._matlab_variables.clear()
        registry._variable_sources.clear()
        registry._register_variable("RawEMG", source=str(entities))

        with caplog.at_level(logging.DEBUG):
            created = materialize_variable_stubs(
                ["RawEMG"], stub_dir, project_start=entities
            )

        assert created == []
        assert "declared in more than one place" not in caplog.text
        assert registry._variable_sources["RawEMG"] == str(entities)

    def test_two_independent_declarations_still_warn(self, tmp_path, caplog):
        """The warning must keep firing for what it was written for: a
        hand-written classdef AND a TOML entry really are two declarations."""
        from scistack_gui import matlab_registry, registry

        entities, _ = self._project(tmp_path)
        handwritten = tmp_path / "src" / "RawEMG.m"
        handwritten.parent.mkdir()
        handwritten.write_text("classdef RawEMG < scidb.BaseVariable\nend\n")

        registry._register_variable("RawEMG", source=str(entities))

        with caplog.at_level(logging.WARNING):
            # What the classdef-file scan does — no declared_by, because that
            # file genuinely is its own declaration.
            matlab_registry._register_matlab_variable("RawEMG", handwritten)

        assert "declared in more than one place" in caplog.text


class TestUnresolvableVarTypePreflight:
    """A generated script that calls ``RawEMG()`` when nothing in the
    project can supply that classdef fails deep inside for_each with
    'Unrecognized function or variable'. Say so at generation time."""

    def test_unknown_output_type_is_flagged_in_the_script(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            "load_emg", "/tmp/x.duckdb", ["subject"], output_types=["RawEMG"]
        )

        assert "% WARNING:" in cmd
        assert "RawEMG" in cmd.split("% WARNING:")[1].splitlines()[0]
        # Still generated: the user may be about to create the variable.
        assert "{RawEMG()}" in cmd

    def test_type_with_a_known_classdef_is_not_flagged(self, tmp_path):
        from scistack_gui import matlab_registry
        from scistack_gui.api.matlab_command import generate_matlab_command

        matlab_registry._matlab_variables["RawEMG"] = tmp_path / "RawEMG.m"

        cmd = generate_matlab_command(
            "load_emg", "/tmp/x.duckdb", ["subject"], output_types=["RawEMG"]
        )

        assert "% WARNING:" not in cmd

    def test_declared_in_the_entities_file_is_not_flagged(self, tmp_path):
        """+scidb/entities.m materializes a classdef for a declared
        variable before the run reaches it, so a declaration is enough."""
        from scistack_gui import matlab_registry
        from scistack_gui.api.matlab_command import generate_matlab_command
        from scistack_gui.config import SciStackConfig

        entities = tmp_path / "scistack_entities.toml"
        entities.write_text('variables = ["RawEMG"]\n', encoding="utf-8")
        matlab_registry._config = SciStackConfig(
            project_root=tmp_path, entities_file=entities
        )

        cmd = generate_matlab_command(
            "load_emg", "/tmp/x.duckdb", ["subject"], output_types=["RawEMG"]
        )

        assert "% WARNING:" not in cmd

    def test_variant_types_are_checked_too(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            "filter_emg",
            "/tmp/x.duckdb",
            ["subject"],
            variants=[
                {
                    "input_types": {"sig": "RawEMG"},
                    "output_type": "FilteredEMG",
                    "constants": {},
                }
            ],
        )

        flagged = cmd.split("% WARNING:")[1].splitlines()[0]
        assert "RawEMG" in flagged and "FilteredEMG" in flagged


class TestSingleNodeMatlabRunRouting:
    """A single-node Run on a MATLAB function must never reach the Python
    registry.

    Routing used to live only in the VS Code extension (`dagPanel.ts`,
    keyed on a `language` field the webview happened to send), so browser
    clients fell through to `_run_in_thread` and failed with "Function '…'
    not found in registry" (todo #5). `route_matlab_single_run` moves the
    decision into the backend, where `matlab_registry` is the authority.
    """

    def _as_matlab(self, monkeypatch, *names):
        from scistack_gui import matlab_registry

        monkeypatch.setattr(
            matlab_registry, "is_matlab_function", lambda n: n in names
        )

    def test_python_function_is_not_routed(self, monkeypatch):
        from scistack_gui.api.run import route_matlab_single_run

        self._as_matlab(monkeypatch)  # nothing is MATLAB
        assert (
            route_matlab_single_run("compute_vo2", {}, "r1", object())
            is None
        )

    def test_host_capable_caller_gets_host_execution_required(self, monkeypatch):
        """The VS Code path: no run is driven here — dagPanel.ts generates
        the script and sends it to the MathWorks terminal, where breakpoints
        work."""
        import threading

        import scistack_gui.api.run as run_mod

        self._as_matlab(monkeypatch, "loadDelsysEMGOneFile")
        started = threading.Event()
        monkeypatch.setattr(
            run_mod,
            "_run_matlab_function_in_thread",
            lambda *a: started.set(),
        )

        result = run_mod.route_matlab_single_run(
            "loadDelsysEMGOneFile",
            {},
            "r2",
            object(),
            host_can_dispatch_matlab=True,
        )

        assert result == {
            "run_id": "r2",
            "host_execution_required": True,
            "language": "matlab",
        }
        assert not started.wait(0.2), "host-dispatched run must not also run here"

    def test_browser_caller_drives_the_sidecar(self, monkeypatch):
        """The standalone path: no privileged host, so the sidecar runs it
        here and the same run_id gets real run_output/run_done."""
        import threading

        import scistack_gui.api.run as run_mod

        self._as_matlab(monkeypatch, "loadDelsysEMGOneFile")
        started = threading.Event()
        seen: list = []

        def _record(run_id, function_name, params, db):
            seen.extend([run_id, function_name, params])
            started.set()

        monkeypatch.setattr(run_mod, "_run_matlab_function_in_thread", _record)

        result = run_mod.route_matlab_single_run(
            "loadDelsysEMGOneFile", {"variants": []}, "r3", object()
        )

        assert result == {"run_id": "r3", "language": "matlab"}
        assert started.wait(2), "sidecar run thread never started"
        assert seen[0] == "r3"
        assert seen[1] == "loadDelsysEMGOneFile"

    def test_start_run_rpc_routes_before_taking_the_db_lock(self, monkeypatch):
        """server.py's handler must decide BEFORE acquire_db_connection:
        holding the DuckDB file lock for a run this process will never
        execute would block the MATLAB session we just dispatched to."""
        from scistack_gui import db as db_mod
        from scistack_gui import server

        self._as_matlab(monkeypatch, "loadDelsysEMGOneFile")
        monkeypatch.setattr(db_mod, "get_db", lambda: object())

        def _must_not_acquire(timeout=5.0):
            raise AssertionError("acquire_db_connection called for a MATLAB run")

        monkeypatch.setattr(db_mod, "acquire_db_connection", _must_not_acquire)

        result = server.METHODS["start_run"](
            {"function_name": "loadDelsysEMGOneFile", "run_id": "r4"}
        )

        assert result["host_execution_required"] is True
        assert result["language"] == "matlab"

    def test_generation_failure_is_reported_on_the_run_not_raised(
        self, monkeypatch
    ):
        """Command generation happens inside the run thread so a failure
        reaches the user as run_output/run_done. Raising it out of the RPC
        instead would leave the Run button stuck on '⏳ Running…'."""
        import scistack_gui.api.run as run_mod

        messages = []
        monkeypatch.setattr(run_mod, "push_message", messages.append)

        class _DB:
            def set_current_db(self):
                pass

        import scistack_gui.services.matlab_command_service as svc

        monkeypatch.setattr(
            svc,
            "generate_matlab_command",
            lambda *a, **k: (_ for _ in ()).throw(ValueError("no such wiring")),
        )

        run_mod._run_matlab_function_in_thread("r5", "someFn", {}, _DB())

        texts = [m.get("text", "") for m in messages if m["type"] == "run_output"]
        assert any("no such wiring" in t for t in texts)
        done = [m for m in messages if m["type"] == "run_done"]
        assert len(done) == 1
        assert done[0]["success"] is False


class TestPathInputNeverRegisteredAsVariable:
    """A PathInput param must never reach ``scidb.register_variable(...)``.

    Regression for the "second run of the same node fails to parse" bug:
    ``input_types`` records a PathInput as its ``PathInput.to_key()`` JSON
    blob, and emitting that as a MATLAB expression produced

        scidb.register_variable({"__type": "PathInput", ...}());

    which MATLAB rejects with "Invalid expression. When calling a function or
    indexing a variable, use parentheses."
    """

    @staticmethod
    def _path_input_variant():
        from scifor import PathInput

        return {
            "input_types": {
                "emgFilePath": PathInput("data/{pass}/emg.mat").to_key(),
                "reference": "RefSignal",
            },
            "output_type": "RawEMG",
            "constants": {},
            "record_count": 3,
        }

    def test_collect_var_types_excludes_path_inputs(self):
        from scistack_gui.api.matlab_command import _collect_var_types

        types = _collect_var_types([self._path_input_variant()])

        assert types == {"RefSignal", "RawEMG"}, (
            "a PathInput's to_key() JSON leaked into the variable-type set"
        )

    def test_generated_command_never_registers_a_path_input(self):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="loadDelsysEMGOneFile",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[self._path_input_variant()],
        )

        registered = [
            ln for ln in cmd.splitlines() if "scidb.register_variable(" in ln
        ]
        assert registered, "expected the variants branch to register its types"
        for line in registered:
            assert "__type" not in line and "{" not in line, (
                f"PathInput JSON emitted as a MATLAB expression: {line}"
            )

    def test_both_first_and_second_run_scripts_are_clean(self):
        """The bug was a branch flip, so neither branch alone catches it.

        Run 1 has no DB variants and takes the template branch; run 1's saves
        then give run 2 the variants that took the register branch. Assert the
        same invariant across both.
        """
        from scistack_gui.api.matlab_command import generate_matlab_command

        first = generate_matlab_command(
            function_name="loadDelsysEMGOneFile",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[],
            path_inputs={"emgFilePath": {"template": "data/{pass}/emg.mat"}},
            output_types=["RawEMG"],
        )
        second = generate_matlab_command(
            function_name="loadDelsysEMGOneFile",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[self._path_input_variant()],
        )

        for label, cmd in (("first run", first), ("second run", second)):
            for line in cmd.splitlines():
                if "scidb.register_variable(" in line:
                    assert "__type" not in line, f"{label}: {line}"


class TestCollectVariableInputs:
    """The third binding kind. ``_collect_edge_path_inputs`` and
    ``_collect_sweep_params`` were the only two collectors this module had, so
    a parameter fed by a VARIABLE node had no source at all until the function
    acquired DB history — see ``_collect_variable_inputs``."""

    def _edge(self, source, target, target_handle):
        return {"source": source, "target": target, "targetHandle": target_handle}

    def test_wires_variable_to_the_param_its_handle_names(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_variable_inputs,
        )

        edges = [self._edge("var__RawEMG", "fn__filterDelsys", "in__loaded_data")]
        assert _collect_variable_inputs("filterDelsys", edges, {}) == {
            "loaded_data": ["RawEMG"]
        }

    def test_class_name_may_differ_from_the_param(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_variable_inputs,
        )

        edges = [self._edge("var__RawEMG", "fn__filterDelsys", "in__signal_in")]
        assert _collect_variable_inputs("filterDelsys", edges, {}) == {
            "signal_in": ["RawEMG"]
        }

    def test_placement_qualified_endpoints_are_recognised(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_variable_inputs,
        )

        edges = [
            self._edge(
                "var__RawEMG::main",
                "fn__filterDelsys__5zhd42::main",
                "in__loaded_data",
            )
        ]
        assert _collect_variable_inputs("filterDelsys", edges, {}) == {
            "loaded_data": ["RawEMG"]
        }

    def test_ignores_edges_for_other_functions(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_variable_inputs,
        )

        edges = [self._edge("var__RawEMG", "fn__other_fn", "in__loaded_data")]
        assert _collect_variable_inputs("filterDelsys", edges, {}) == {}

    def test_excludes_pathinput_and_parameter_sources(self):
        """Those have their own collectors and their own MATLAB expressions;
        emitting them as ``Name()`` would be a parse error."""
        from scistack_gui.services.matlab_command_service import (
            _collect_variable_inputs,
        )

        edges = [
            self._edge("pathInput__emg_file", "fn__filterDelsys", "in__filepath"),
            self._edge("param__delsys_config", "fn__filterDelsys", "in__config"),
        ]
        assert _collect_variable_inputs("filterDelsys", edges, {}) == {}

    def test_two_variables_on_one_handle_is_eachof(self):
        from scistack_gui.services.matlab_command_service import (
            _collect_variable_inputs,
        )

        edges = [
            self._edge("var__RawEMG", "fn__filterDelsys", "in__loaded_data"),
            self._edge("var__RawEMG2", "fn__filterDelsys", "in__loaded_data"),
        ]
        assert _collect_variable_inputs("filterDelsys", edges, {}) == {
            "loaded_data": ["RawEMG", "RawEMG2"]
        }


class TestMatlabInputsBindPositionally:
    """MATLAB has no keyword arguments: ``+scifor/for_each.m`` does
    ``input_names = fieldnames(inputs)`` and then ``fn(call_args{:})``, so the
    emitted struct's FIELD ORDER *is* the argument order.

    Regression for 2026-09-02: ``filterDelsys(loaded_data, config, Fs)`` was
    wired to RawEMG + two Parameters and ran as ``filterDelsys(2000, config)``
    — the variable was never collected (see TestCollectVariableInputs) and the
    two Parameters that were collected went in edge order.
    """

    SIGNATURE = ["loaded_data", "config", "Fs"]

    @pytest.fixture
    def registered_fn(self):
        from scistack_gui import matlab_registry
        from scistack_gui.matlab_parser import MatlabFunctionInfo

        before = dict(matlab_registry._matlab_functions)
        matlab_registry._matlab_functions["filterDelsys"] = MatlabFunctionInfo(
            name="filterDelsys",
            file_path=None,
            params=list(self.SIGNATURE),
            source_hash="0" * 64,
            n_outputs=1,
            output_names=["filtered_data"],
        )
        yield
        matlab_registry._matlab_functions.clear()
        matlab_registry._matlab_functions.update(before)

    @staticmethod
    def _struct_fields(cmd):
        """Field names of the generated ``struct(...)``, in emitted order."""
        import re

        line = next(ln for ln in cmd.splitlines() if "struct(" in ln)
        return re.findall(r"'(\w+)',", line[line.index("struct(") :])

    def test_template_branch_emits_every_binding_in_signature_order(
        self, registered_fn
    ):
        """The exact failing case: no DB history, one variable + two
        Parameters, drawn on the canvas in the wrong order."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filterDelsys",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[],
            output_types=["FilteredEMG"],
            # Collection order deliberately ≠ signature order.
            sweeps={"Fs": [2000], "config": ["cfg"]},
            variable_inputs={"loaded_data": ["RawEMG"]},
        )

        assert self._struct_fields(cmd) == self.SIGNATURE
        assert "RawEMG()" in cmd, "the variable input was dropped entirely"

    def test_variants_branch_orders_by_signature_too(self, registered_fn):
        """The bug was a branch flip once before (see
        TestPathInputNeverRegisteredAsVariable) — assert both branches."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filterDelsys",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[
                {
                    "input_types": {"loaded_data": "RawEMG"},
                    "output_type": "FilteredEMG",
                    "constants": {"Fs": 2000},
                    "record_count": 1,
                }
            ],
            sweeps={"config": ["cfg"]},
        )

        assert self._struct_fields(cmd) == self.SIGNATURE

    def test_unknown_signature_keeps_insertion_order(self):
        """No parsed signature — the function may live only on MATLAB's own
        path, and reordering on a guess is worse than emitting what the
        caller assembled."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="not_in_the_registry",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[],
            output_types=["Out"],
            sweeps={"b": [1], "a": [2]},
        )

        assert self._struct_fields(cmd) == ["b", "a"]

    def test_param_outside_the_signature_is_appended_not_interleaved(
        self, registered_fn
    ):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filterDelsys",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[],
            output_types=["FilteredEMG"],
            sweeps={"mystery": [1], "config": ["cfg"]},
            variable_inputs={"loaded_data": ["RawEMG"]},
        )

        assert self._struct_fields(cmd) == ["loaded_data", "config", "mystery"]

    def test_gap_before_a_bound_param_is_warned_about(self, registered_fn, caplog):
        """The unfixable half: if ``loaded_data`` is simply not wired, no
        ordering can save the call, so it must be loud."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        with caplog.at_level(logging.WARNING, logger="scistack_gui.api.matlab_command"):
            generate_matlab_command(
                function_name="filterDelsys",
                db_path="/db.duckdb",
                schema_keys=["pass"],
                variants=[],
                output_types=["FilteredEMG"],
                sweeps={"Fs": [2000], "config": ["cfg"]},
            )

        messages = [r.getMessage() for r in caplog.records]
        assert any(
            "loaded_data" in m and "WRONG value" in m for m in messages
        ), f"an unwired leading parameter must be warned about; got {messages}"

    def test_multi_type_variable_binding_becomes_eachof(self, registered_fn):
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="filterDelsys",
            db_path="/db.duckdb",
            schema_keys=["pass"],
            variants=[],
            output_types=["FilteredEMG"],
            variable_inputs={"loaded_data": ["RawEMG", "RawEMG2"]},
        )

        assert "scifor.EachOf(RawEMG(), RawEMG2())" in cmd


class TestRunOptionsInGeneratedCommand:
    """Run options must be RENDERED INTO the generated MATLAB script.

    A MATLAB run never receives ``distribute``/``as_table``/``dry_run``/``save``
    as arguments -- it receives text. An option the generator does not emit is
    an option MATLAB never hears about, and the failure is silent: the script
    is still well-formed, the run still succeeds, and it simply does the wrong
    thing. That is precisely what happened on 2026-09-14, when a run requested
    with ``distribute: True`` saved 560 undistributed records.

    See docs/claude/gui-run-options-flow.md.
    """

    VARIANTS = [
        {
            "input_types": {"signal": "RawSignal"},
            "output_type": "FilteredSignal",
            "constants": {"low_hz": 20},
            "record_count": 4,
        }
    ]

    def _cmd(self, run_options, **kwargs):
        from scistack_gui.api.matlab_command import generate_matlab_command

        return generate_matlab_command(
            function_name="bandpass_filter",
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session", "trial"],
            run_options=run_options,
            **kwargs,
        )

    def test_distribute_true_is_emitted(self):
        cmd = self._cmd({"distribute": True}, variants=self.VARIANTS)
        assert "'distribute', true" in cmd

    def test_distribute_false_is_not_emitted(self):
        """MATLAB's default is false, so silence and an explicit false are the
        same run. Omitting keeps generated scripts diffable."""
        cmd = self._cmd({"distribute": False}, variants=self.VARIANTS)
        assert "distribute" not in cmd

    def test_no_run_options_emits_nothing(self):
        cmd = self._cmd(None, variants=self.VARIANTS)
        assert "distribute" not in cmd
        assert "as_table" not in cmd
        assert "dry_run" not in cmd

    def test_distribute_emitted_on_first_run_template_branch(self):
        """The no-variants branch builds its own for_each call. distribute
        shapes the invocation_id, so the FIRST run is exactly when it must be
        right -- that run establishes the identity every later one matches."""
        cmd = self._cmd({"distribute": True}, variants=None)
        assert "scidb.for_each" in cmd
        assert "'distribute', true" in cmd

    def test_dry_run_and_save_use_their_own_defaults(self):
        """save defaults TRUE, so only ``save=False`` is worth emitting;
        dry_run defaults false, so only true is."""
        cmd = self._cmd(
            {"dry_run": True, "save": False}, variants=self.VARIANTS
        )
        assert "'dry_run', true" in cmd
        assert "'save', false" in cmd

        cmd = self._cmd({"dry_run": False, "save": True}, variants=self.VARIANTS)
        assert "dry_run" not in cmd
        assert "'save'" not in cmd

    def test_as_table_bool(self):
        cmd = self._cmd({"as_table": True}, variants=self.VARIANTS)
        assert "'as_table', true" in cmd

    def test_as_table_name_list_becomes_string_array(self):
        """for_each.m:217-223 accepts a logical scalar OR a string array of
        parameter names; both shapes have to round-trip."""
        cmd = self._cmd({"as_table": ["signal", "other"]}, variants=self.VARIANTS)
        assert '\'as_table\', ["signal", "other"]' in cmd

    def test_as_table_empty_list_is_not_emitted(self):
        cmd = self._cmd({"as_table": []}, variants=self.VARIANTS)
        assert "as_table" not in cmd

    def test_options_survive_a_dict_valued_constant(self):
        """Regression pair for the two defects that hit the same run: a dict
        constant used to crash _group_variants outright (unhashable type:
        'dict'), which masked the fact that distribute was being dropped."""
        variants = [
            {
                "input_types": {"gaitRitePath": "PathInput"},
                "output_type": "GAITRiteLoaded",
                "constants": {"config": {"FOLDER": "Gaitrite", "HEADER": 3}},
                "record_count": 1,
            }
        ]
        cmd = self._cmd({"distribute": True}, variants=variants)
        assert "'distribute', true" in cmd

    def test_unknown_option_warns_rather_than_silently_dropping(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._cmd({"parallel": True}, variants=self.VARIANTS)
        assert "parallel" in caplog.text
        assert "will NOT reach MATLAB" in caplog.text

    def test_emitted_options_are_logged(self, caplog):
        with caplog.at_level(logging.INFO):
            self._cmd({"distribute": True}, variants=self.VARIANTS)
        assert "non-default run option" in caplog.text

    def test_pipeline_step_carries_its_own_options(self):
        """A whole-pipeline script registers one for_each per node, and each
        node has its OWN saved options -- they are not a property of the run."""
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="main",
            steps=[
                {
                    "function_name": "step_one",
                    "variants": self.VARIANTS,
                    "run_options": {"distribute": True},
                },
                {
                    "function_name": "step_two",
                    "variants": self.VARIANTS,
                    "run_options": {"distribute": False},
                },
            ],
            db_path="/data/experiment.duckdb",
            schema_keys=["subject", "session", "trial"],
        )
        one = cmd.index("@step_one")
        two = cmd.index("@step_two")
        assert "'distribute', true" in cmd[one:two]
        assert "'distribute', true" not in cmd[two:]


class TestVariableInputColumnSelection:
    """Stage 6 of .claude/plan-column-selection-ui.md.

    MATLAB has no ``ColumnSelection`` wrapper: the column names go to the
    ``BaseVariable`` constructor (``selected_columns``) and ``iterate`` comes
    from ``for_columns`` (``+scidb/BaseVariable.m``). The four spellings below
    are what ``_format_variable_class`` must emit, and what
    ``code_export_service._matlab_literal`` renders too -- two renderers, one
    syntax.
    """

    def _expr(self, ref):
        from scistack_gui.api.matlab_command import _variable_input_items

        return dict(_variable_input_items({"table_in": ref}))["table_in"]

    def test_no_selection_is_the_plain_constructor(self):
        assert self._expr("Trials") == "Trials()"
        assert self._expr({"types": ["Trials"]}) == "Trials()"

    def test_single_column(self):
        assert (
            self._expr({"types": ["Trials"], "columns": ["filename"]})
            == 'Trials("filename")'
        )

    def test_multiple_columns(self):
        assert (
            self._expr({"types": ["Trials"], "columns": ["a", "b"]})
            == 'Trials(["a", "b"])'
        )

    def test_iterate_over_named_columns(self):
        assert (
            self._expr({"types": ["Trials"], "columns": ["a", "b"], "iterate": True})
            == 'Trials().for_columns(["a", "b"])'
        )

    def test_iterate_over_all_columns(self):
        assert (
            self._expr({"types": ["Trials"], "columns": [], "iterate": True})
            == "Trials().for_columns()"
        )

    def test_multi_type_wraps_every_alternative(self):
        assert (
            self._expr({"types": ["A", "B"], "columns": ["c"]})
            == 'scifor.EachOf(A("c"), B("c"))'
        )

    def test_pooled_binding_wraps_in_across_variants(self):
        """A history-derived binding whose run pooled every variant group
        (``_invocation.across_variants``) re-runs pooled from MATLAB too."""
        assert (
            self._expr({"types": ["Trials"], "pool_variants": True})
            == "scidb.AcrossVariants(Trials())"
        )
        assert (
            self._expr({"types": ["Trials"], "columns": ["a"], "pool_variants": True})
            == 'scidb.AcrossVariants(Trials("a"))'
        )
        assert (
            self._expr({"types": ["A", "B"], "pool_variants": True})
            == "scifor.EachOf(scidb.AcrossVariants(A()), scidb.AcrossVariants(B()))"
        )

    def test_type_names_still_reach_the_classdef_preflight(self):
        """``_variable_input_type_names`` feeds the unresolvable-classdef
        warning; the dict shape must not hide the names from it."""
        from scistack_gui.api.matlab_command import _variable_input_type_names

        names = _variable_input_type_names(
            {
                "a": "RawEMG",
                "b": ["RawVO2"],
                "c": {"types": ["Trials"], "columns": ["x"]},
            }
        )
        assert sorted(names) == ["RawEMG", "RawVO2", "Trials"]

    def test_column_names_are_escaped(self):
        assert (
            self._expr({"types": ["Trials"], "columns": ['say "hi"']})
            == 'Trials("say ""hi""")'
        )

    def test_multiple_column_names_are_escaped(self):
        assert (
            self._expr({"types": ["Trials"], "columns": ['a"b', "c'd"]})
            == 'Trials(["a""b", "c\'d"])'
        )


class TestDoubleQuotedStringEscaping:
    """``_format_matlab_string_array`` emits DOUBLE-quoted literals but used
    to escape SINGLE quotes -- the escaping of the other quoting style. An
    embedded ``"`` therefore terminated the literal and an embedded ``'`` was
    corrupted into ``''``.

    Latent while its only callers were identifiers (schema keys, parameter
    names); column selection is what first routes user spreadsheet headers
    through it.
    """

    def test_double_quote_is_doubled(self):
        from scistack_gui.api.matlab_command import _format_matlab_string_array

        assert _format_matlab_string_array(['a"b']) == '["a""b"]'

    def test_single_quote_is_left_alone(self):
        """Inside ``"..."`` a single quote is an ordinary character. Doubling
        it is not a harmless over-escape -- it changes the value."""
        from scistack_gui.api.matlab_command import _format_matlab_string_array

        assert _format_matlab_string_array(["c'd"]) == "[\"c'd\"]"

    def test_single_quoted_helper_still_escapes_single_quotes(self):
        """The other helper is unchanged -- ``'...'`` literals (addpath, db
        path, pipeline id) still need ``''``."""
        from scistack_gui.api.matlab_command import _escape_matlab_string

        assert _escape_matlab_string("c'd") == "c''d"

    def test_agrees_with_the_code_export_renderer(self):
        from scistack_gui.api.matlab_command import _escape_matlab_dq
        from scistack_gui.services.code_export_service import _matlab_str

        for s in ('a"b', "c'd", "plain"):
            assert _matlab_str(s) == f'"{_escape_matlab_dq(s)}"'

    def test_selection_reaches_the_template_branch(self):
        """The first-run branch: no DB variants, wiring is the only source."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="summarise",
            db_path="/db.duckdb",
            schema_keys=["subject"],
            variants=[],
            output_types=["Summary"],
            variable_inputs={
                "table_in": {"types": ["Trials"], "columns": ["filename"]}
            },
        )
        assert 'Trials("filename")' in cmd

    def test_selection_reaches_the_db_variant_branch(self):
        """A function WITH history. The recorded ``input_types`` renders
        ``Trials()``; the overlay must replace it, not sit beside it."""
        from scistack_gui.api.matlab_command import generate_matlab_command

        cmd = generate_matlab_command(
            function_name="summarise",
            db_path="/db.duckdb",
            schema_keys=["subject"],
            variants=[
                {
                    "input_types": {"table_in": "Trials"},
                    "output_type": "Summary",
                    "constants": {},
                }
            ],
            variable_inputs={
                "table_in": {"types": ["Trials"], "columns": ["filename"]}
            },
        )
        assert 'Trials("filename")' in cmd
        assert "'table_in', Trials()" not in cmd

    def test_selection_reaches_a_pipeline_step(self):
        from scistack_gui.api.matlab_command import generate_matlab_pipeline_command

        cmd = generate_matlab_pipeline_command(
            pipeline_id="main",
            steps=[
                {
                    "function_name": "summarise",
                    "variants": [
                        {
                            "input_types": {"table_in": "Trials"},
                            "output_type": "Summary",
                            "constants": {},
                        }
                    ],
                    "variable_inputs": {
                        "table_in": {
                            "types": ["Trials"],
                            "columns": [],
                            "iterate": True,
                        }
                    },
                }
            ],
            db_path="/db.duckdb",
            schema_keys=["subject"],
        )
        assert "Trials().for_columns()" in cmd


class TestVariableInputsView:
    """The MATLAB generator's ``variable_inputs`` map is a RENDERING of the
    resolved bindings the Python run consumes — one derivation, two
    languages. Before 2026-09-19 ``matlab_command_service`` assembled its
    own map from edges plus node config, so a selection recorded in history
    reached a Python re-run and not a MATLAB one.
    """

    def _target(self, bindings):
        return {"constants": {}, "bindings": bindings}

    def test_a_whole_variable_binding_is_the_plain_list(self):
        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import variable_inputs_view

        out = variable_inputs_view([self._target({"table_in": variable_binding(["Trials"])})])
        assert out == {"table_in": ["Trials"]}

    def test_a_selected_binding_is_promoted(self):
        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import variable_inputs_view

        out = variable_inputs_view(
            [self._target({"table_in": variable_binding(["Trials"], ["filename"])})]
        )
        assert out == {
            "table_in": {"types": ["Trials"], "columns": ["filename"], "iterate": False}
        }

    def test_a_history_selection_reaches_matlab_too(self, populated_db, bp_node_id):
        """The regression: a Python-authored `Var["col"]` step re-run from a
        MATLAB command must load that column, with no node config at all."""
        from scistack_gui.services.execution_service import (
            derive_fn_targets,
            variable_inputs_view,
        )

        targets = derive_fn_targets(populated_db, "bandpass_filter")
        out = variable_inputs_view(targets, "bandpass_filter")
        assert "signal" in out  # the recorded binding, whatever its shape

    def test_non_variable_bindings_are_not_variable_inputs(self):
        from scistack_gui.domain.edge_resolver import parameter_binding, pathinput_binding
        from scistack_gui.services.execution_service import variable_inputs_view

        out = variable_inputs_view(
            [self._target({"p": parameter_binding("P"), "f": pathinput_binding("F")})]
        )
        assert out == {}

    def test_two_targets_that_disagree_keep_the_first_and_warn(self, caplog):
        from scistack_gui.domain.edge_resolver import variable_binding
        from scistack_gui.services.execution_service import variable_inputs_view

        with caplog.at_level(logging.WARNING):
            out = variable_inputs_view(
                [
                    self._target({"a": variable_binding(["A"], ["x"])}),
                    self._target({"a": variable_binding(["A"], ["y"])}),
                ],
                "f",
            )
        assert out["a"]["columns"] == ["x"]
        assert "bound differently" in caplog.text


class TestScopeVariantsToNode:
    """One click must run one wiring.

    `matlab_command_service` chose the variants for the generated script by
    function NAME, while the node-scoped derivation it computes a few lines
    later — the same one the Python run path uses — was spent only on variable
    bindings. A function with two wirings on the canvas therefore ran both.

    Observed 2026-09-22 on `grSides`, which gained a second wiring after a run
    through a manual-edge overlay: `_group_variants: 391 variant row(s) -> 2
    for_each call(s)`, the second pass saving `0 new rows`, ~75s per run wasted
    and the DuckDB lock held across both — which made the GUI's own refreshes
    fail with `DB LOCKED` and left stale colours on the canvas. The same log
    line shows the node-scoped derivation getting it right at the time:
    `'side' restricted to "PareticSide" on 1 target(s)`.
    """

    NAME = "grSides"

    def _by_name(self):
        """Two wirings of one function, as `list_pipeline_variants` returns
        them: the old one (history never bound `side`) and the one a run
        through the overlay recorded."""
        old = [
            {
                "function_name": self.NAME,
                "input_types": {"grTableIn": "GAITRiteLoaded"},
                "constants": {},
                "output_type": "GAITRiteLoaded_UA",
            }
        ] * 3
        new = [
            {
                "function_name": self.NAME,
                "input_types": {
                    "grTableIn": "GAITRiteLoaded",
                    "side": "Demographics",
                },
                "constants": {},
                "output_type": "GAITRiteLoaded_UA",
            }
        ]
        return old + new, new

    def test_a_node_scoped_request_runs_only_its_own_wiring(self):
        from scistack_gui.services.matlab_command_service import scope_variants_to_node

        by_name, targets = self._by_name()
        scoped = scope_variants_to_node(
            by_name, targets, "fn__grSides__f99f8a48833ed7ee", self.NAME
        )
        assert scoped == targets

    def test_that_is_one_for_each_call_not_two(self):
        """The consequence, through the function that actually emits them."""
        from scistack_gui.api.matlab_command import _group_variants
        from scistack_gui.services.matlab_command_service import scope_variants_to_node

        by_name, targets = self._by_name()
        assert len(_group_variants(by_name)) == 2, "the bug, pinned"
        scoped = scope_variants_to_node(
            by_name, targets, "fn__grSides__f99f8a48833ed7ee", self.NAME
        )
        assert len(_group_variants(scoped)) == 1

    def test_a_request_with_no_node_keeps_the_name_scoped_list(self):
        """"Run this function", with no canvas behind it — the Python path
        makes the same distinction (`derive_fn_targets` vs
        `derive_target_for_node`)."""
        from scistack_gui.services.matlab_command_service import scope_variants_to_node

        by_name, targets = self._by_name()
        assert scope_variants_to_node(by_name, targets, None, self.NAME) == by_name

    def test_a_node_that_derives_nothing_falls_back_rather_than_refusing(self):
        """Never run, no edges to infer from. History is still the best guess,
        and returning nothing here would turn a working run into a no-op."""
        from scistack_gui.services.matlab_command_service import scope_variants_to_node

        by_name, _ = self._by_name()
        assert (
            scope_variants_to_node(by_name, [], "fn__grSides__abc", self.NAME)
            == by_name
        )

    def test_the_single_wiring_case_is_unchanged(self):
        """The ordinary project, where name and wiring coincide: same rows in,
        same rows out, no log line."""
        from scistack_gui.services.matlab_command_service import scope_variants_to_node

        _, targets = self._by_name()
        assert (
            scope_variants_to_node(targets, targets, "fn__grSides__abc", self.NAME)
            == targets
        )
