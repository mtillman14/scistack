"""scidb.schema_map: importing into another schema (portability Stage 6).

One map, exporter key -> recipient key or None, and one renaming per shape a
key can appear in. Values (levels like "S02") are never translated; a
selection the map touched is flagged.
"""

from __future__ import annotations

import pytest

from scidb.schema_map import KeyMap, MapReport

EXPORTER = ["subject", "session", "trial"]
RECIPIENT = ["participant", "visit", "trial"]


@pytest.fixture
def km():
    return KeyMap.auto(EXPORTER, RECIPIENT, {"subject": "participant", "session": "visit"})


class TestAuto:
    def test_names_in_both_schemas_map_to_themselves(self):
        km = KeyMap.auto(EXPORTER, RECIPIENT)
        assert km.as_dict() == {"subject": None, "session": None, "trial": "trial"}

    def test_the_same_schema_is_the_identity(self):
        assert KeyMap.auto(EXPORTER, EXPORTER).is_identity

    def test_overrides(self, km):
        assert km.as_dict() == {"subject": "participant", "session": "visit", "trial": "trial"}
        assert not km.is_identity

    def test_an_override_must_land_on_the_recipient_schema(self):
        with pytest.raises(ValueError, match="recipient"):
            KeyMap.auto(EXPORTER, RECIPIENT, {"subject": "patient"})

    def test_an_override_must_name_an_exporter_key(self):
        with pytest.raises(ValueError, match="exporter"):
            KeyMap.auto(EXPORTER, RECIPIENT, {"cycle": "trial"})

    def test_two_keys_cannot_share_a_target(self):
        with pytest.raises(ValueError, match="two exporter keys"):
            KeyMap.auto(EXPORTER, RECIPIENT, {"subject": "trial"})


class TestShapes:
    def test_level(self, km):
        r = MapReport()
        assert km.level(["session", "trial"], "n", r) == ["visit", "trial"]
        assert km.level([], "n", r) == []  # one call stays one call
        assert km.level(None, "n", r) is None

    def test_a_dropped_level_key_is_reported_and_all_dropped_means_unset(self):
        km = KeyMap.auto(EXPORTER, ["trial"])
        r = MapReport()
        assert km.level(["subject"], "node f", r) is None
        assert km.level(["subject", "trial"], "node f", r) == ["trial"]
        assert ("node f", "subject") in r.dropped

    def test_locations_rename_drop_and_flag(self):
        km = KeyMap.auto(EXPORTER, ["participant", "trial"], {"subject": "participant"})
        r = MapReport()
        raw = {
            "include": [[["subject", "S01"], ["session", "BL"]], [["trial", "1"]]],
            "exclude_levels": {"subject": ["S02"], "session": ["FU"]},
        }
        out = km.locations(raw, "node f", r)
        assert out["exclude_levels"] == {"participant": ["S02"]}  # value kept
        assert out["include"] == [[["trial", "1"]]]  # the prefix naming session went
        assert ("node f", "session") in r.dropped
        assert any("levels are the exporter's" in why for _, why in r.flagged)

    def test_an_untouched_selection_is_not_flagged(self, km):
        r = MapReport()
        raw = {"exclude_levels": {"trial": ["3"]}}
        assert km.locations(raw, "n", r) == raw
        assert r.flagged == []

    def test_template(self, km):
        r = MapReport()
        assert km.template("{subject}/{session}/t{trial}.csv", "pi", r) == (
            "{participant}/{visit}/t{trial}.csv"
        )
        dropping = KeyMap.auto(EXPORTER, ["trial"])
        assert dropping.template("{subject}.csv", "pi", r) == "{subject}.csv"
        assert any("{subject}" in why for _, why in r.flagged)

    def test_table(self, km):
        r = MapReport()
        out = km.table({"session": ["BL", "FU"], "Demographics.Sex": {"name": "Sex"}}, "t", r)
        assert out == {"visit": ["BL", "FU"], "Demographics.Sex": {"name": "Sex"}}

    def test_exact_strings(self, km):
        r = MapReport()
        spec = {"roles": {"session": "group", "subject": "collapse"}, "groups": ["session"],
                "measures": ["StepLength"], "title": "per session"}
        out = km.exact_strings(spec, "plot", r)
        assert out["roles"] == {"visit": "group", "participant": "collapse"}
        assert out["groups"] == ["visit"]
        assert out["title"] == "per session"  # only EXACT matches
        assert r.flagged

    def test_the_identity_changes_nothing(self):
        km = KeyMap.auto(EXPORTER, EXPORTER)
        r = MapReport()
        raw = {"exclude_levels": {"subject": ["S02"]}}
        assert km.locations(raw, "n", r) is raw
        assert km.template("{subject}.csv", "p", r) == "{subject}.csv"
        assert r.dropped == [] and r.flagged == []
