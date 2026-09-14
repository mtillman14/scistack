"""Pinning an input to a code version — ``Variant(X, code_version=...)``.

Stage 4a of ``.claude/plan-variant-selection.md``: the *selection* half of "code
as an axis of change". A record's variant identity has two dimensions — the
constants upstream of it and the code that produced it — and ``Variant`` now
pins either, through one filter.

Nothing here re-runs old code. That is Stage 4b and needs ``_function_source``
plus an answer to the node-state question in the plan.
"""

import numpy as np
import pytest

import scifor as _scifor
from scidb import BaseVariable, Variant, configure_database, for_each
from scidb.exceptions import AmbiguousParamError
from scidb.variant import CODE_PIN_PREFIX

SCHEMA = ["subject"]
SUBJECTS = ["01", "02"]


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    database = configure_database(tmp_path / "test_code_version_pin.duckdb", SCHEMA)
    yield database
    _scifor.set_schema([])
    database.close()


class Raw(BaseVariable):
    pass


class Scaled(BaseVariable):
    pass


class Consumed(BaseVariable):
    pass


def consume(scaled):
    return float(scaled) + 1.0


@pytest.fixture
def two_code_versions(db):
    """``scale_signal`` runs at two bodies over the same inputs."""
    for subject in SUBJECTS:
        Raw.save(np.array([1.0, 2.0]), subject=subject)

    def scale_v1(raw):
        return float(np.sum(raw))

    def scale_v2(raw):
        return float(np.sum(raw)) + 100.0

    for body in (scale_v1, scale_v2):
        body.__name__ = "scale_signal"
        for_each(body, inputs={"raw": Raw}, outputs=[Scaled], subject=[])
    return db


# --- construction ---------------------------------------------------------


class TestConstruction:
    def test_bare_pin_uses_the_reserved_namespace(self):
        pinned = Variant(Scaled, code_version="v1")

        assert pinned.branch_params == {CODE_PIN_PREFIX: "v1"}

    def test_fn_disambiguates_without_becoming_a_branch_param(self):
        """`fn=` must namespace the code pin as a code pin, NOT produce a
        branch param called `scale_signal.code_version`."""
        pinned = Variant(Scaled, fn="scale_signal", code_version="v2")

        assert pinned.branch_params == {f"{CODE_PIN_PREFIX}.scale_signal": "v2"}

    def test_both_dimensions_coexist(self):
        pinned = Variant(Scaled, fn="scale_signal", low_hz=20, code_version="v1")

        assert pinned.branch_params == {
            "scale_signal.low_hz": 20,
            f"{CODE_PIN_PREFIX}.scale_signal": "v1",
        }

    def test_a_code_only_variant_is_legal(self):
        """The old guard demanded a branch param; a code pin is a pin too."""
        assert Variant(Scaled, code_version="latest").branch_params

    def test_an_empty_variant_is_still_refused(self):
        with pytest.raises(ValueError, match="at least one branch_param"):
            Variant(Scaled)


# --- filtering ------------------------------------------------------------


class TestPinning:
    def test_pinning_a_version_halves_the_fan_out(self, two_code_versions):
        for_each(
            consume,
            inputs={"scaled": Variant(Scaled, code_version="v1")},
            outputs=[Consumed],
            subject=[],
        )

        db = two_code_versions
        scaled = db._duck._fetchone(
            "SELECT COUNT(*) FROM _record WHERE type = 'Scaled'"
        )
        consumed = db._duck._fetchone(
            "SELECT COUNT(*) FROM _record WHERE type = 'Consumed'"
        )
        assert scaled[0] == 2 * len(SUBJECTS)
        assert consumed[0] == len(SUBJECTS), "the pin should have selected one version"

    def test_the_pinned_version_is_the_one_that_ran(self, db, two_code_versions):
        """v1 sums to 3.0, v2 to 103.0 — so the output value names which body
        actually fed the consumer."""
        for_each(
            consume,
            inputs={"scaled": Variant(Scaled, code_version="v1")},
            outputs=[Consumed],
            subject=[],
        )

        values = db.load_all_as_df(Consumed)["data"].tolist()
        assert all(abs(v - 4.0) < 1e-9 for v in values), (
            f"expected v1 (3.0 + 1.0) throughout, got {values}"
        )

    def test_pinning_the_other_version_selects_the_other_body(
        self, db, two_code_versions
    ):
        for_each(
            consume,
            inputs={"scaled": Variant(Scaled, code_version="v2")},
            outputs=[Consumed],
            subject=[],
        )

        values = db.load_all_as_df(Consumed)["data"].tolist()
        assert all(abs(v - 104.0) < 1e-9 for v in values), (
            f"expected v2 (103.0 + 1.0) throughout, got {values}"
        )

    def test_fn_qualified_pin_works(self, db, two_code_versions):
        for_each(
            consume,
            inputs={
                "scaled": Variant(Scaled, fn="scale_signal", code_version="v1")
            },
            outputs=[Consumed],
            subject=[],
        )

        values = db.load_all_as_df(Consumed)["data"].tolist()
        assert len(values) == len(SUBJECTS)
        assert all(abs(v - 4.0) < 1e-9 for v in values)

    def test_latest_keeps_one_record_per_location(self, db, two_code_versions):
        """`latest` is per LOCATION, so every subject still contributes — the
        point of not resolving it as "the highest ordinal"."""
        for_each(
            consume,
            inputs={"scaled": Variant(Scaled, code_version="latest")},
            outputs=[Consumed],
            subject=[],
        )

        values = db.load_all_as_df(Consumed)["data"].tolist()
        assert len(values) == len(SUBJECTS), "no subject may drop out"
        assert all(abs(v - 104.0) < 1e-9 for v in values), "v2 is the newest here"

    def test_a_pin_on_an_unversioned_project_is_a_no_op(self, db):
        """Nothing was ever edited, so the pin must select everything rather
        than nothing — an empty run would be a silent disaster."""
        for subject in SUBJECTS:
            Raw.save(np.array([1.0, 2.0]), subject=subject)

        def scale_signal(raw):
            return float(np.sum(raw))

        for_each(scale_signal, inputs={"raw": Raw}, outputs=[Scaled], subject=[])
        for_each(
            consume,
            inputs={"scaled": Variant(Scaled, code_version="v1")},
            outputs=[Consumed],
            subject=[],
        )

        assert len(db.load_all_as_df(Consumed)) == len(SUBJECTS)


class TestNamedFunctionEdgeCases:
    """The qualified path used to have no guards at all: an `fn=` naming a
    single-version or nonexistent function fell through to matching nothing and
    emptied the run silently."""

    def test_single_version_function_accepts_v1(self, db):
        for subject in SUBJECTS:
            Raw.save(np.array([1.0, 2.0]), subject=subject)

        def scale_signal(raw):
            return float(np.sum(raw))

        for_each(scale_signal, inputs={"raw": Raw}, outputs=[Scaled], subject=[])
        for_each(
            consume,
            inputs={
                "scaled": Variant(Scaled, fn="scale_signal", code_version="v1")
            },
            outputs=[Consumed],
            subject=[],
        )

        assert len(db.load_all_as_df(Consumed)) == len(SUBJECTS), (
            "one recorded version IS v1 — pinning it must not empty the run"
        )

    def test_single_version_function_refuses_v2(self, db):
        for subject in SUBJECTS:
            Raw.save(np.array([1.0, 2.0]), subject=subject)

        def scale_signal(raw):
            return float(np.sum(raw))

        for_each(scale_signal, inputs={"raw": Raw}, outputs=[Scaled], subject=[])

        with pytest.raises(ValueError, match="only one recorded version"):
            for_each(
                consume,
                inputs={
                    "scaled": Variant(Scaled, fn="scale_signal", code_version="v2")
                },
                outputs=[Consumed],
                subject=[],
            )

    def test_an_unknown_function_name_is_an_error(self, two_code_versions):
        with pytest.raises(ValueError, match="not upstream"):
            for_each(
                consume,
                inputs={"scaled": Variant(Scaled, fn="typo", code_version="v1")},
                outputs=[Consumed],
                subject=[],
            )

    def test_an_unknown_version_is_an_error(self, two_code_versions):
        with pytest.raises(ValueError, match="matches nothing"):
            for_each(
                consume,
                inputs={
                    "scaled": Variant(
                        Scaled, fn="scale_signal", code_version="v7"
                    )
                },
                outputs=[Consumed],
                subject=[],
            )


class TestAmbiguity:
    def test_a_bare_pin_refuses_when_two_functions_are_versioned(self, db):
        """Same rule as a bare branch-param name: name it or be told to."""
        for subject in SUBJECTS:
            Raw.save(np.array([1.0, 2.0]), subject=subject)

        def scale_a(raw):
            return float(np.sum(raw))

        def scale_b(raw):
            return float(np.sum(raw)) + 100.0

        for body in (scale_a, scale_b):
            body.__name__ = "scale_signal"
            for_each(body, inputs={"raw": Raw}, outputs=[Scaled], subject=[])

        def consume_v1(scaled):
            return float(scaled) + 1.0

        def consume_v2(scaled):
            return float(scaled) + 2.0

        for body in (consume_v1, consume_v2):
            body.__name__ = "consume"
            for_each(body, inputs={"scaled": Scaled}, outputs=[Consumed], subject=[])

        def summarize(consumed):
            return float(consumed)

        with pytest.raises(AmbiguousParamError, match="ambiguous"):
            for_each(
                summarize,
                inputs={"consumed": Variant(Consumed, code_version="v1")},
                outputs=[Scaled],
                subject=[],
            )


class TestListValuedPin:
    """A list value is membership — the rule branch params always had, and the
    form the Plot Studio location picker sends (``{"__code__.fn": ["v1"]}``).
    The run-options pin hit this first (``run_options="['distribute=true']"
    matches nothing``, 2026-09-14); the code pin shared the same ``str(value)``
    and is closed here pre-emptively through the shared ``_pin_values``."""

    def _load(self, db, pin):
        return db.load_all_as_df(
            Scaled, version_id="all", branch_params_filter={f"{CODE_PIN_PREFIX}.scale_signal": pin}
        )

    def test_single_element_list_selects_that_version(self, two_code_versions):
        assert len(self._load(two_code_versions, ["v1"])) == len(SUBJECTS)
        assert len(self._load(two_code_versions, "v1")) == len(SUBJECTS)

    def test_list_is_membership(self, two_code_versions):
        assert len(self._load(two_code_versions, ["v1", "v2"])) == 2 * len(SUBJECTS)

    def test_a_list_of_only_unknown_versions_still_errors(self, two_code_versions):
        with pytest.raises(ValueError, match="matches nothing"):
            self._load(two_code_versions, ["v9"])
