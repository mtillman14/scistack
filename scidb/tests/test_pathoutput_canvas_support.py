"""scidb's half of output paths on the canvas (GitHub issue #19).

* ``normalize_pathoutput_inputs`` is THE owner of the ``pathoutput`` intent
  aspect's shape (``{param: template}``).
* A ``plot_`` endpoint creates the folder its figure goes into: the folder is
  a project setting (``[outputs] folder``) and templates name sub-folders, so
  it routinely does not exist yet.
"""

from __future__ import annotations

from pathlib import Path

from scidb.provenance.intent import (
    ASPECT_PATHOUTPUT,
    ASPECTS,
    PER_KEY_ASPECTS,
    normalize_pathoutput_inputs,
)
from scidb.run.foreach_load import _make_plot_wrapper


class TestNormalize:
    def test_none_is_empty(self):
        assert normalize_pathoutput_inputs(None) == {}

    def test_the_canonical_map_is_kept_sorted(self):
        assert normalize_pathoutput_inputs({"z": "b.png", "filename": "plots/{subject}.png"}) == {
            "filename": "plots/{subject}.png",
            "z": "b.png",
        }

    def test_blank_templates_mean_not_an_output(self):
        assert normalize_pathoutput_inputs({"a": "", "b": "   ", "c": None, "d": "x.png"}) == {"d": "x.png"}

    def test_templates_are_kept_as_written_apart_from_whitespace(self):
        assert normalize_pathoutput_inputs({"f": "  {ColName}_{subject}.pdf "}) == {"f": "{ColName}_{subject}.pdf"}

    def test_a_list_is_not_this_shape(self):
        assert normalize_pathoutput_inputs(["filename"]) == {}

    def test_it_is_a_per_key_aspect(self):
        assert ASPECT_PATHOUTPUT in ASPECTS
        assert ASPECT_PATHOUTPUT in PER_KEY_ASPECTS


class _Figure:
    """Duck-typed matplotlib Figure: the wrapper only calls ``savefig``."""

    def savefig(self, path):
        Path(path).write_bytes(b"png")


def test_the_plot_wrapper_creates_missing_parent_folders(tmp_path):
    target = tmp_path / "outputs" / "plots" / "S1" / "fig.png"

    def plot_thing(df, filename):
        return _Figure()

    wrapped = _make_plot_wrapper(plot_thing, "filename")
    assert wrapped(df=None, filename=str(target)) == str(target)
    assert target.read_bytes() == b"png"
