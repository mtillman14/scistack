"""scidb.schema_level — the ONE owner of "which keys does a run iterate".

Regressions for docs/claude/cleanup-audit.md §4.2: F22 (MATLAB ignored the
default), F24 (the recorded level was function-NAME scoped), F25 (alternate
PathInput templates contributed nothing), F26 (``[]`` meant one call, every
key or unset depending on the route).
"""

import numpy as np
import pytest

import scifor as _scifor
from scifor import EachOf, PathInput

from scidb import BaseVariable, configure_database, for_each
from scidb import provenance_query
from scidb.schema_level import (
    RULE_INPUTS,
    RULE_NOTHING,
    RULE_RECORDED,
    RULE_STATED,
    SchemaLevel,
    input_levels,
    path_input_level,
    resolve_schema_level,
)

KEYS = ["subject", "session", "trial"]


# ---------------------------------------------------------------------------
# The value type
# ---------------------------------------------------------------------------


class TestSchemaLevel:
    def test_the_three_stored_spellings(self):
        assert SchemaLevel.from_stated(None).is_unset
        assert SchemaLevel.from_stated([]).is_one_call
        assert SchemaLevel.from_stated(["subject"]).keys == ("subject",)

    def test_keys_come_out_in_dataset_order_and_unknown_keys_drop(self):
        level = SchemaLevel.from_stated(["trial", "bogus", "subject"], KEYS)
        assert level.keys == ("subject", "trial")

    def test_only_unknown_keys_is_one_call_not_every_key(self):
        assert SchemaLevel.of(["bogus"], KEYS).is_one_call

    def test_for_each_spelling_never_emits_the_empty_list(self):
        """for_each reads schema_keys=[] as EVERY key; one call is None."""
        assert SchemaLevel.one_call().for_each_schema_keys() is None
        assert SchemaLevel.of(["subject"]).for_each_schema_keys() == ["subject"]

    def test_matlab_spelling_is_the_empty_list_for_one_call(self):
        assert SchemaLevel.one_call().iterate_keys() == []

    def test_an_unset_level_cannot_be_run(self):
        with pytest.raises(ValueError, match="resolve_schema_level"):
            SchemaLevel.unset().for_each_schema_keys()
        with pytest.raises(ValueError):
            SchemaLevel.unset().iterate_keys()

    def test_round_trips_to_storage(self):
        for raw in (None, [], ["subject", "session"]):
            assert SchemaLevel.from_stated(raw, KEYS).to_stated() == raw


# ---------------------------------------------------------------------------
# The precedence
# ---------------------------------------------------------------------------


class TestResolve:
    def test_stated_wins_including_the_empty_list(self):
        level, rule = resolve_schema_level(KEYS, [], ["subject"], [["subject"]])
        assert level.is_one_call and rule == RULE_STATED

    def test_recorded_beats_inputs(self):
        level, rule = resolve_schema_level(KEYS, None, ["subject"], [KEYS])
        assert level.keys == ("subject",) and rule == RULE_RECORDED

    def test_a_recorded_one_call_is_an_answer(self):
        level, rule = resolve_schema_level(KEYS, None, [], [KEYS])
        assert level.is_one_call and rule == RULE_RECORDED

    def test_inputs_iterate_their_union(self):
        level, rule = resolve_schema_level(
            KEYS, None, None, [["subject"], ["subject", "trial"]]
        )
        assert level.keys == ("subject", "trial") and rule == RULE_INPUTS

    def test_inputs_with_no_key_mean_one_call(self):
        """loadDemographics: one PathInput, no placeholder — ONE call, not the
        714 the MATLAB route used to make (F22)."""
        level, rule = resolve_schema_level(KEYS, None, None, [[]])
        assert level.is_one_call and rule == RULE_INPUTS

    def test_nothing_to_go_on_is_every_key(self):
        level, rule = resolve_schema_level(KEYS, None, None, [])
        assert level.keys == tuple(KEYS) and rule == RULE_NOTHING


# ---------------------------------------------------------------------------
# Input levels
# ---------------------------------------------------------------------------


class TestPathInputLevel:
    def test_template_placeholders_in_dataset_order(self):
        assert path_input_level(PathInput("{trial}/{subject}.csv", name="{trial}/{subject}.csv"), KEYS) == [
            "subject",
            "trial",
        ]

    def test_no_placeholder_is_an_empty_level(self):
        assert path_input_level(PathInput("demographics.xlsx", name="demographics.xlsx"), KEYS) == []

    def test_alternate_templates_contribute_their_union(self, caplog):
        pi = EachOf(PathInput("{subject}/a.csv", name="{subject}/a.csv"), PathInput("{subject}/{trial}/b.csv", name="{subject}/a.csv"))
        assert path_input_level(pi, KEYS) == ["subject", "trial"]

    def test_anything_else_has_no_level(self):
        assert path_input_level(object(), KEYS) is None
        assert path_input_level(EachOf(1, 2), KEYS) is None


# ---------------------------------------------------------------------------
# DB-backed: input levels and the node-scoped recorded level (F24)
# ---------------------------------------------------------------------------


class Raw(BaseVariable):
    pass


class PerSession(BaseVariable):
    pass


class PerSubject(BaseVariable):
    pass


def scale(x, k):
    # Ignores x: the subject-level call site aggregates sessions into a table,
    # and this test is about which LEVEL each call site ran at, nothing else.
    return float(k)


@pytest.fixture
def db(tmp_path):
    _scifor.set_schema([])
    db = configure_database(tmp_path / "schema_level.duckdb", ["subject", "session"])
    yield db
    _scifor.set_schema([])
    db.close()


def _seed(db):
    for s in (1, 2):
        for sess in ("A", "B"):
            Raw.save(np.arange(3.0), db=db, subject=s, session=sess)


def test_input_levels_read_variable_records(db):
    _seed(db)
    levels = input_levels(
        db._duck, {"Raw"}, [PathInput("{subject}.csv", name="{subject}.csv")], ["subject", "session"]
    )
    assert sorted(map(tuple, levels)) == [("subject",), ("subject", "session")]


def test_recorded_level_is_scoped_to_the_call_sites_asked_about(db):
    """Two call sites of ONE function at two levels. Scoped by call_id each
    answers its own level; the name-scoped form answers whichever ran last."""
    _seed(db)
    for_each(scale, {"x": Raw, "k": 2}, [PerSession], subject=[], session=[])
    for_each(scale, {"x": Raw, "k": 3}, [PerSubject], subject=[])

    by_output = {
        v["output_type"]: v["call_id"]
        for v in provenance_query.pipeline_variants(db._duck)
        if v["function_name"] == "scale"
    }
    keys = ["subject", "session"]
    assert provenance_query.recorded_schema_keys(
        db._duck, "scale", keys, call_ids={by_output["PerSession"]}
    ) == ["subject", "session"]
    assert provenance_query.recorded_schema_keys(
        db._duck, "scale", keys, call_ids={by_output["PerSubject"]}
    ) == ["subject"]


def test_call_sites_with_no_history_have_no_recorded_level(db):
    _seed(db)
    for_each(scale, {"x": Raw, "k": 2}, [PerSession], subject=[], session=[])
    assert (
        provenance_query.recorded_schema_keys(
            db._duck, "scale", ["subject", "session"], call_ids={"0" * 16}
        )
        is None
    )
    assert (
        provenance_query.recorded_schema_keys(
            db._duck, "scale", ["subject", "session"], call_ids=set()
        )
        is None
    )
