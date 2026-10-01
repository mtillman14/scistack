"""Guard: ``+scidb/entities.m``'s cross-language contract with the bridge.

MATLAB resolves ``py.*`` names at call time, so a renamed bridge function
or a renamed key in the dict it returns survives every Python test suite and
fails only when a real MATLAB session reaches that line. The self-healing
classdef materialization added in
``.claude/plan-matlab-variable-classdef-materialization.md`` sits exactly
there: it runs once per generated script, and its whole purpose is to keep a
run from dying with ``Unrecognized function or variable 'RawEMG'``.

Static checks only — no MATLAB required.
"""

import re
from pathlib import Path

_root = Path(__file__).parent.parent.parent
_ENTITIES_M = (
    _root / "scimatlab" / "src" / "scimatlab" / "matlab" / "+scidb" / "entities.m"
)


def _source() -> str:
    return _ENTITIES_M.read_text(encoding="utf-8")


def _code() -> str:
    """Source with whole-line comments dropped — doc comments name the
    bridge function too, and a doc mention must not satisfy these checks."""
    return "\n".join(
        line for line in _source().splitlines() if not line.lstrip().startswith("%")
    )


def test_calls_the_bridge_to_materialize_classdefs():
    from scimatlab import bridge

    assert "py.scimatlab.bridge.ensure_variable_classdefs(" in _code(), (
        "entities.m no longer materializes classdefs for declared variables; a "
        "GUI-created variable would fail at the for_each call with "
        "'Unrecognized function or variable'"
    )
    assert callable(bridge.ensure_variable_classdefs)


def test_passes_the_project_root_through():
    """``scidb.entities(PROJECT_ROOT)`` must reach the bridge — otherwise
    materialization resolves the project from MATLAB's cwd, which is the
    bug that produced 'MATLAB load: 0 variable(s) ... from .'."""
    code = _code()
    assert "ensure_variable_classdefs(" in code
    assert re.search(
        r"ensure_variable_classdefs\(\s*\.\.\.\s*py\.list\(missing\),\s*char\(project_root\)",
        code,
    ), "the PROJECT_ROOT form of the bridge call is missing"


def test_only_asks_for_names_matlab_cannot_resolve():
    """MATLAB's path is the authority. Writing a stub for a name that
    already has a hand-written classdef elsewhere would shadow it."""
    code = _code()
    assert "exist(vname, 'class')" in code
    assert "~= 8" in code


def test_adds_the_stub_directory_to_the_path_and_rehashes():
    code = _code()
    assert "addpath(stub_dir)" in code
    assert re.search(r"^\s*rehash;", code, re.MULTILINE), (
        "without a rehash, a classdef just written into a freshly added "
        "folder is not visible to MATLAB's class resolver"
    )


def test_indexed_payload_keys_match_what_the_bridge_returns(tmp_path):
    """Every ``stub_result{'key'}`` the .m reads must be a key the bridge
    actually returns."""
    from scimatlab.stubs import write_variable_classdefs

    returned = set(write_variable_classdefs(["Probe"], target_dir=tmp_path))
    indexed = set(re.findall(r"stub_result\{'(\w+)'\}", _code()))

    assert indexed, "entities.m no longer reads the bridge result"
    assert indexed <= returned, (
        f"entities.m reads {sorted(indexed - returned)} from the bridge result, "
        f"which returns {sorted(returned)}"
    )


def test_warns_by_name_when_a_declared_variable_still_has_no_class():
    """The diagnostic that was missing: the failure used to surface as
    'Unrecognized function or variable X' from inside for_each."""
    assert "scidb:entities:noClassdef" in _code()


_MATLAB_ROOT = _root / "scimatlab" / "src" / "scimatlab" / "matlab"


def _without_comments_or_continuations(text: str) -> str:
    """Whole-line comments dropped and ``...`` continuations folded, so a
    call written across several lines reads as one."""
    body = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("%")
    )
    return re.sub(r"\.\.\.[^\n]*\n", " ", body)


def _split_top_level(args: str) -> list:
    parts, depth, quoted, current = [], 0, False, ""
    for ch in args:
        if quoted:
            current += ch
            if ch == "'":
                quoted = False
            continue
        if ch == "'":
            quoted = True
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(current.strip())
            current = ""
            continue
        current += ch
    if current.strip():
        parts.append(current.strip())
    return parts


def _pathinput_call_args(text: str) -> list:
    """Argument lists of every ``scidb.PathInput(...)`` /
    ``scifor.PathInput(...)`` construction in ``text``."""
    calls = []
    for match in re.finditer(r"\b(?:scidb|scifor)\.PathInput\(", text):
        # Skip the name when it sits inside a single-quoted char literal: an
        # error message or fprintf format that SHOWS a call (+scifor/PathInput.m
        # since a9357ddd) is not a call. An odd number of quotes before it on
        # its line means it is inside one ('' escapes count as two).
        line_start = text.rfind("\n", 0, match.start()) + 1
        if text.count("'", line_start, match.start()) % 2 == 1:
            continue
        depth, end = 1, match.end()
        while depth and end < len(text):
            if text[end] == "(":
                depth += 1
            elif text[end] == ")":
                depth -= 1
            end += 1
        calls.append(_split_top_level(text[match.end() : end - 1]))
    return calls


def _pathinput_options() -> set:
    """The name-value options declared by ``+scifor/PathInput.m``."""
    src = (_MATLAB_ROOT / "+scifor" / "PathInput.m").read_text(encoding="utf-8")
    block = re.search(r"\n\s*arguments\b(.*?)\n\s*end\b", src, re.S)
    assert block, "+scifor/PathInput.m no longer declares an arguments block"
    return set(re.findall(r"options\.(\w+)", block.group(1)))


def test_pathinput_is_constructed_with_name_value_options():
    """``scifor.PathInput`` takes the template as its only positional; every
    other setting is name-value. ``entities.m`` used to pass root_folder as a
    second positional, which no Python test could see: it failed only in a
    real MATLAB session, with 'Invalid argument at position 2. Function
    requires exactly 1 positional input(s).'

    Checked tree-wide, since any .m file constructing a PathInput can make
    the same mistake. Also asserts the option *names* exist, so renaming one
    in +scifor/PathInput.m cannot silently orphan a call site.
    """
    options = _pathinput_options()
    assert "root_folder" in options

    for path in sorted(_MATLAB_ROOT.rglob("*.m")):
        text = _without_comments_or_continuations(path.read_text(encoding="utf-8"))
        for args in _pathinput_call_args(text):
            if len(args) <= 1 or args[0].startswith("varargin"):
                continue  # bare template, or the scidb.PathInput passthrough
            assert len(args) % 2 == 1, (
                f"{path.name}: PathInput(...) call has an unpaired name-value "
                f"argument: {args}"
            )
            for name in args[1::2]:
                assert name.startswith("'") and name.endswith("'"), (
                    f"{path.name}: PathInput(...) passes {name} positionally; "
                    f"only the template is positional -- use "
                    f"'root_folder', {name} form"
                )
                assert name.strip("'") in options, (
                    f"{path.name}: PathInput(...) passes unknown option "
                    f"{name}; +scifor/PathInput.m declares {sorted(options)}"
                )


def test_entities_m_builds_rooted_path_inputs():
    """The rooted branch must survive: a TOML ``{template, root_folder}``
    entry that loses its root resolves against the wrong base directory."""
    calls = _pathinput_call_args(_without_comments_or_continuations(_source()))
    assert any("'root_folder'" in args for args in calls), (
        "entities.m no longer passes root_folder when the TOML declares one"
    )


def test_a_failed_path_input_names_the_declaration():
    """Same diagnostic contract as the classdef branch: a constructor error
    must say which entity and template it came from, not just surface as a
    bare 'Invalid argument at position 2' from inside scifor.PathInput."""
    assert "scidb:entities:pathInputFailed" in _code()


def test_generated_scripts_pass_the_project_root():
    """The GUI must not emit a bare ``scidb.entities();`` — MATLAB would
    resolve the project from its own cwd."""
    src = (
        _root / "scistack-gui" / "scistack_gui" / "api" / "matlab_command.py"
    ).read_text(encoding="utf-8")

    assert "scidb.entities('{" in src, (
        "generated MATLAB no longer passes the project root to scidb.entities"
    )
