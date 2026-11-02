"""MATLAB frames classify through Python's one frame classifier (runs without MATLAB).

``+scifor/frame_origin.m`` is a one-line delegator to
``scifor.failures.frame_origin(file, (matlabroot,))``. Before P5 stage 1 it
held its own copy of the rule. This guards the three facts the delegation
relies on:

* files in scimatlab's MATLAB directory (``+scifor``, ``+scidb``) are
  ``"framework"`` — Python's framework rule covers them through the
  ``scimatlab`` package directory;
* a file under the ``extra_library_dirs`` root MATLAB passes (``matlabroot``)
  is ``"library"``;
* an empty file name (an anonymous function typed at the prompt) and any
  other path are ``"user"``.
"""

from pathlib import Path

from scifor.failures import frame_origin

_MATLAB = Path(__file__).parent.parent / "src" / "scimatlab" / "matlab"


def test_matlab_framework_files_are_framework():
    for rel in ("+scifor/for_each.m", "+scidb/for_each.m", "+scifor/frame_origin.m"):
        path = _MATLAB / rel
        assert path.is_file(), path
        assert frame_origin(str(path), ()) == "framework", rel


def test_matlabroot_files_are_library(tmp_path):
    root = tmp_path / "MATLAB" / "R2024b"
    toolbox = root / "toolbox" / "matlab" / "elmat" / "zeros.m"
    toolbox.parent.mkdir(parents=True)
    toolbox.write_text("")
    assert frame_origin(str(toolbox), (str(root),)) == "library"
    # Without the extra root the same file is the user's.
    assert frame_origin(str(toolbox), ()) == "user"


def test_empty_and_other_files_are_user(tmp_path):
    root = tmp_path / "matlabroot"
    user_file = tmp_path / "project" / "my_step.m"
    assert frame_origin("", (str(root),)) == "user"
    assert frame_origin(str(user_file), (str(root),)) == "user"


def test_matlab_delegator_calls_python_owner():
    """The .m file holds no rule of its own: it crosses to Python."""
    text = (_MATLAB / "+scifor" / "frame_origin.m").read_text()
    assert "py.scifor.failures.frame_origin" in text
    assert "startsWith" not in text
