"""Recorded wirings (plan-canvas-as-document Stage 1): the step shape scidb
owns, forward == backward parity for ``wiring_id``, and the non-Cartesian
measure (plan risk 2)."""

import pytest

import scifor as _scifor

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query as pq
from scidb.database import aggregate_pipeline_variants
from scidb.provenance import compute_wiring_id
from scidb.wirings import RecordedWiring, non_cartesian_report, recorded_wirings

SUBJECTS = ["S01", "S02"]


class WiringRaw(BaseVariable):
    pass


class WiringOut(BaseVariable):
    pass


class WiringOutOther(BaseVariable):
    pass


def scale(x, k, m):
    return x * k + m


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "wirings.duckdb", ["subject"])
    for s in SUBJECTS:
        WiringRaw.save(1.0, subject=s)
    yield database
    _scifor.set_schema([])
    database.close()


def _run(k, m, output=WiringOut):
    for_each(scale, {"x": WiringRaw, "k": k, "m": m}, [output], subject=SUBJECTS)


def _wirings(db) -> list[RecordedWiring]:
    return [
        w
        for w in recorded_wirings(aggregate_pipeline_variants(pq.pipeline_variants(db._duck)))
        if w.fn_name == "scale"
    ]


class TestPure:
    def _w(self, rows):
        return RecordedWiring(
            fn_name="f",
            wiring_id="w",
            call_ids=(),
            input_params={},
            outputs=("O",),
            path_inputs={},
            constant_rows=tuple(rows),
        )

    def test_one_axis_is_always_cartesian(self):
        assert self._w([{"k": 1}, {"k": 2}]).missing_combinations() == []

    def test_full_product_is_cartesian(self):
        rows = [{"k": k, "m": m} for k in (1, 2) for m in (3, 4)]
        assert self._w(rows).is_cartesian

    def test_diagonal_names_the_two_that_never_ran(self):
        w = self._w([{"k": 6, "m": 2}, {"k": 10, "m": 4}])
        assert w.missing_combinations() == [{"k": 6, "m": 4}, {"k": 10, "m": 2}]

    def test_values_compare_by_repr_so_dicts_work(self):
        """Constants are not scalars (provenance.constants_identity_key)."""
        a, b = {"x": 1}, {"x": 2}
        w = self._w([{"cfg": a, "m": 1}, {"cfg": b, "m": 1}])
        assert w.is_cartesian
        assert w.constant_axes == {"cfg": [a, b], "m": [1]}

    def test_int_and_float_are_different_values(self):
        """6 and 6.0 hash into different call ids, so they are two values."""
        w = self._w([{"k": 6}, {"k": 6.0}])
        assert len(w.constant_axes["k"]) == 2

    def test_report_lists_only_non_cartesian(self):
        ok = self._w([{"k": 1}])
        bad = self._w([{"k": 6, "m": 2}, {"k": 10, "m": 4}])
        [entry] = non_cartesian_report([ok, bad])
        assert entry["rows"] == 2 and len(entry["missing"]) == 2


class TestFromARealRun:
    def test_forward_equals_backward(self, db):
        """The wiring a step is computed with, forward, is the one history
        reconstructs: the parity tier the architecture review §1 asked for."""
        _run(1.0, 0.0)
        [w] = _wirings(db)
        assert w.wiring_id == compute_wiring_id(
            "scale", {"x": "WiringRaw"}, ["WiringOut"], {}
        )
        assert w.input_params == {"x": "WiringRaw"}
        assert w.outputs == ("WiringOut",)

    def test_constant_variants_are_one_step(self, db):
        _run(1.0, 0.0)
        _run(2.0, 0.0)
        [w] = _wirings(db)
        assert len(w.call_ids) == 2
        assert sorted(r["k"] for r in w.constant_rows) == [1.0, 2.0]
        assert w.is_cartesian

    def test_a_different_output_is_a_different_step(self, db):
        """Different call sites (here: different constants) writing different
        outputs are two steps."""
        _run(1.0, 0.0)
        _run(5.0, 0.0, output=WiringOutOther)
        assert sorted(w.outputs for w in _wirings(db)) == [
            ("WiringOut",),
            ("WiringOutOther",),
        ]

    def test_one_call_site_saving_two_outputs_is_one_step_without_claims(self, db):
        """A call site does not include its outputs, so the same inputs and
        constants saved into two variables are ONE call site. With no GUI
        claims to split it (split_call_site_outputs), it is one wiring over
        the union of its outputs, which is what a script-only history gets."""
        _run(1.0, 0.0)
        _run(1.0, 0.0, output=WiringOutOther)
        [w] = _wirings(db)
        assert w.outputs == ("WiringOut", "WiringOutOther")

    def test_claims_split_one_call_site_into_its_claimed_wirings(self, db):
        _run(1.0, 0.0)
        _run(1.0, 0.0, output=WiringOutOther)
        claims = [
            compute_wiring_id("scale", {"x": "WiringRaw"}, [out], {})
            for out in ("WiringOut", "WiringOutOther")
        ]
        agg = aggregate_pipeline_variants(pq.pipeline_variants(db._duck))
        outputs = sorted(w.outputs for w in recorded_wirings(agg, claims) if w.fn_name == "scale")
        assert outputs == [("WiringOut",), ("WiringOutOther",)]

    def test_diagonal_runs_are_measured_as_non_cartesian(self, db):
        _run(6.0, 2.0)
        _run(10.0, 4.0)
        [w] = _wirings(db)
        missing = sorted(w.missing_combinations(), key=lambda c: c["k"])
        assert missing == [{"k": 6.0, "m": 4.0}, {"k": 10.0, "m": 2.0}]

    def test_full_product_runs_are_cartesian(self, db):
        for k in (6.0, 10.0):
            for m in (2.0, 4.0):
                _run(k, m)
        [w] = _wirings(db)
        assert w.is_cartesian and len(w.constant_rows) == 4
