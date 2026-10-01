"""A Parameter value is recorded as DECLARED, not as the transport spelled it.

Regression for scidb.log 2026-10-01: ``formulaNum = 6`` is declared in the
entities file, and MATLAB passed it as ``6.0`` (MATLAB has no integers).
History recorded ``"6.0"`` and the declaration said ``"6"``, so the canvas drew
two "6" rows. ``constants_identity_key`` is repr-based, so a Python run of the
same declared 6 would also have been a different variant.
The owner is ``scidb.entities.declared_spelling``.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, Parameter, configure_database, entities, for_each
from scidb.entities import declared_spelling
from scifor.each_of import EachOf

SCHEMA = ["subject"]


def _declared(name, *values):
    p = Parameter(*values)
    p.name = name
    return {name: p}


class TestDeclaredSpelling:
    def test_a_double_takes_the_declared_int(self):
        out = declared_spelling({"n": 6.0}, {"n": "formulaNum"}, _declared("formulaNum", 6))
        assert out["n"] == 6 and type(out["n"]) is int

    def test_an_identical_spelling_is_untouched(self):
        inputs = {"n": 6}
        out = declared_spelling(inputs, {"n": "formulaNum"}, _declared("formulaNum", 6))
        assert out["n"] == 6 and type(out["n"]) is int

    def test_a_declared_float_is_kept_as_float(self):
        out = declared_spelling({"n": 6.0}, {"n": "x"}, _declared("x", 6.0))
        assert type(out["n"]) is float

    def test_a_value_not_declared_is_untouched(self):
        out = declared_spelling({"n": 7.0}, {"n": "formulaNum"}, _declared("formulaNum", 6))
        assert out["n"] == 7.0 and type(out["n"]) is float

    def test_an_undeclared_parameter_is_untouched(self):
        out = declared_spelling({"n": 6.0}, {"n": "other"}, _declared("formulaNum", 6))
        assert type(out["n"]) is float

    def test_no_names_is_a_no_op(self):
        inputs = {"n": 6.0}
        assert declared_spelling(inputs, None, _declared("n", 6)) is inputs

    def test_bool_never_matches_a_number(self):
        out = declared_spelling({"n": 1.0}, {"n": "flag"}, _declared("flag", True))
        assert out["n"] == 1.0 and type(out["n"]) is float

    def test_a_list_takes_the_declared_list(self):
        out = declared_spelling(
            {"n": [6.0, 2.0]}, {"n": "pair"}, _declared("pair", [6, 2])
        )
        assert out["n"] == [6, 2] and all(type(v) is int for v in out["n"])

    def test_an_unexpanded_each_of_is_left_for_expansion(self):
        each = EachOf(6.0, 7.0)
        out = declared_spelling({"n": each}, {"n": "formulaNum"}, _declared("formulaNum", 6))
        assert out["n"] is each

    def test_an_array_compared_to_a_list_does_not_raise(self):
        arr = np.array([6.0, 2.0])
        out = declared_spelling({"n": arr}, {"n": "pair"}, _declared("pair", [6, 2]))
        assert out["n"] is arr

    def test_it_is_idempotent(self):
        declared = _declared("formulaNum", 6)
        once = declared_spelling({"n": 6.0}, {"n": "formulaNum"}, declared)
        twice = declared_spelling(once, {"n": "formulaNum"}, declared)
        assert twice == once and type(twice["n"]) is int


# ---------------------------------------------------------------------------
# End to end: the run records the declared spelling
# ---------------------------------------------------------------------------


class SpellRaw(BaseVariable):
    pass


class SpellOut(BaseVariable):
    pass


def scale(signal, n):
    return signal * n


@pytest.fixture
def project_db(tmp_path, monkeypatch):
    (tmp_path / "scistack.toml").write_text(
        'entities_file = "e.toml"\n', encoding="utf-8"
    )
    (tmp_path / "e.toml").write_text(
        "[parameters]\nformulaNum = 6\n", encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    entities.clear_cache()
    _scifor.set_schema([])
    db = configure_database(tmp_path / "spelling.duckdb", SCHEMA)
    SpellRaw.save(np.arange(3.0), db=db, subject=1)
    yield db
    _scifor.set_schema([])
    entities.clear_cache()
    db.close()


def _run(n):
    for_each(
        scale,
        inputs={"signal": SpellRaw, "n": n},
        outputs=[SpellOut],
        subject=[1],
        parameter_names={"n": "formulaNum"},
    )


class TestRunRecordsTheDeclaredValue:
    def test_a_matlab_style_double_is_recorded_as_declared(self, project_db):
        _run(6.0)
        values = project_db.get_aggregated_variants()["constants"]["formulaNum"]["values"]
        assert [v["value"] for v in values] == ["6"]

    def test_matlab_and_python_runs_of_one_value_are_one_variant(self, project_db):
        _run(6.0)
        declared = Parameter(6)
        declared.name = "formulaNum"
        _run(declared)
        agg = project_db.get_aggregated_variants()
        values = agg["constants"]["formulaNum"]["values"]
        assert [v["value"] for v in values] == ["6"]
        scale_sites = [k for k in agg["functions"] if k[0] == "scale"]
        assert len(scale_sites) == 1
