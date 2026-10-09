"""code_rewrite.rewrite_imports: the one owner of re-homing Python imports
when code moves between packages (share as library, make my own copy)."""

from __future__ import annotations

from scistack_gui.code_rewrite import rewrite_imports

SRC = '''\
"""Doc mentioning gaitlib.filters (never rewritten)."""
import os, gaitlib
import gaitlib.filters as flt
import gaitlib.filters
from gaitlib.util import scale  # keep this comment
from . import sibling
from os import path


def f():
    from gaitlib import util
    return "gaitlib.util"
'''


def _resolve(name):
    return f"proj.{name}" if name == "gaitlib" or name.startswith("gaitlib.") else None


def test_each_import_form():
    r = rewrite_imports(SRC, _resolve, where="m")
    out = r.text
    assert "import os; from proj import gaitlib" in out
    assert "import proj.gaitlib.filters as flt" in out
    assert "from proj.gaitlib.util import scale  # keep this comment" in out
    assert "    from proj.gaitlib import util" in out
    assert "from . import sibling" in out and "from os import path" in out
    assert '"""Doc mentioning gaitlib.filters (never rewritten)."""' in out
    assert 'return "gaitlib.util"' in out
    assert len(r.changes) == 5


def test_a_bare_dotted_import_is_reported():
    r = rewrite_imports("import gaitlib.filters\n", _resolve, where="m")
    assert r.text == "import proj.gaitlib.filters\n"
    assert r.warnings and "by hand" in r.warnings[0]


def test_nothing_to_rewrite_returns_the_text_unchanged():
    text = "import numpy as np\n"
    assert rewrite_imports(text, _resolve).text is text
