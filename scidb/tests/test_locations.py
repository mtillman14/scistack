"""Stage 1a of the schema location picker: ``scidb.locations.location_states``.

Companion doc: ``docs/claude/schema-location-status.md``. Plan:
``.claude/plan-schema-location-picker.md``.

The correctness anchor here is :class:`TestAgreesWithCheckComboState` — the new
batch primitive must agree, location for location, with the existing
one-combo-at-a-time :func:`scidb.state.check_combo_state`. If the two ever
disagree on a non-excluded location, one of them is wrong and this test says so
before a colour reaches a user.

Fixture (schema keys subject/trial), for ``LocFilt``::

    (S01, t1)  green   — computed, nothing upstream moved
    (S01, t2)  amber   — LocRaw re-saved after LocFilt was computed
    (S01, t3)  red     — LocRaw exists, bandpass never run there
    (S02, t1)  grey    — deliberately excluded, with a reason
    (S02, t2)  green
"""

import numpy as np
import pytest
from scidb.locations import location_states
from scidb.state import check_combo_state, check_node_state, check_pathinput_node_state

from scidb import BaseVariable, configure_database, for_each, scistack

SCHEMA_KEYS = ["subject", "trial"]


class LocRaw(BaseVariable):
    schema_version = 1


class LocFilt(BaseVariable):
    schema_version = 1


class LocLoaded(BaseVariable):
    schema_version = 1


def loc_bandpass(signal, low_hz):
    return signal * low_hz


@scistack
def loc_import(filepath):
    with open(filepath) as fh:
        return float(fh.read().strip())


def _state_of(tree, **keys):
    """The leaf state at one location, by walking the tree the way a UI would."""
    node = None
    children = tree.roots
    for key in tree.schema_keys:
        if key not in keys:
            break
        node = next(
            (n for n in children if n.key == key and n.value == str(keys[key])), None
        )
        if node is None:
            return None
        children = node.children
    return node.state if node else None


def _node_at(tree, **keys):
    node = None
    children = tree.roots
    for key in tree.schema_keys:
        if key not in keys:
            break
        node = next(
            (n for n in children if n.key == key and n.value == str(keys[key])), None
        )
        if node is None:
            return None
        children = node.children
    return node


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def build_states_db(db_path):
    from scidb.exclusions import exclude_schema

    db = configure_database(db_path, SCHEMA_KEYS)
    for subject, trial in [("S01", "t1"), ("S01", "t2"), ("S02", "t1"), ("S02", "t2")]:
        LocRaw.save(np.array([1.0, 2.0]), subject=subject, trial=trial)

    for_each(
        loc_bandpass,
        {"signal": LocRaw, "low_hz": 20},
        [LocFilt],
        subject=["S01", "S02"],
        trial=["t1", "t2"],
    )

    # amber: an input re-saved after its consumer was computed.
    LocRaw.save(np.array([7.0, 8.0]), subject="S01", trial="t2")
    # red: input data that bandpass has never been run over.
    LocRaw.save(np.array([9.0, 9.0]), subject="S01", trial="t3")
    # grey: a deliberate exclusion, which must leave both counts alone.
    exclude_schema("sensor fell off", subject="S02", trial="t1")
    return db


@pytest.fixture
def states_db(tmp_path):
    db = build_states_db(tmp_path / "loc_states.duckdb")
    yield db
    db.close()


@pytest.fixture
def loader_db(tmp_path):
    """A PathInput-only loader run over only *some* of the files on disk.

    This is the shape ``.claude/plan-pathinput-loader-staleness-gap.md``
    describes: ``check_node_state`` reads green because expected == realized by
    construction, while three files exist and only two were loaded.
    """
    from scifor import PathInput

    root = tmp_path / "data"
    for subject, trial, value in [
        ("S01", "t1", 1.5),
        ("S01", "t2", 2.5),
        ("S02", "t1", 3.5),
    ]:
        d = root / f"sub{subject}"
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{trial}.txt").write_text(str(value))

    db = configure_database(tmp_path / "loc_loader.duckdb", SCHEMA_KEYS)
    path_input = PathInput("sub{subject}/{trial}.txt", root_folder=str(root))
    for_each(
        loc_import,
        {"filepath": path_input},
        [LocLoaded],
        subject=["S01"],  # S02's file is on disk and never loaded
        trial=["t1", "t2"],
    )
    yield db, path_input
    db.close()


@pytest.fixture
def variant_db(tmp_path):
    db = configure_database(tmp_path / "loc_variant.duckdb", SCHEMA_KEYS)
    LocRaw.save(np.array([1.0]), subject="S01", trial="t1")
    LocRaw.save(np.array([2.0]), subject="S02", trial="t1")
    for_each(
        loc_bandpass,
        {"signal": LocRaw, "low_hz": 20},
        [LocFilt],
        subject=["S01", "S02"],
        trial=["t1"],
    )
    # The second variant exists at S01 only — the asymmetry is the point.
    for_each(
        loc_bandpass,
        {"signal": LocRaw, "low_hz": 50},
        [LocFilt],
        subject=["S01"],
        trial=["t1"],
    )
    yield db
    db.close()


# ---------------------------------------------------------------------------
# The four states
# ---------------------------------------------------------------------------


class TestFourStates:
    def test_each_state_lands_where_the_fixture_put_it(self, states_db):
        tree = location_states(LocFilt, db=states_db)
        assert _state_of(tree, subject="S01", trial="t1") == "green"
        assert _state_of(tree, subject="S01", trial="t2") == "amber"
        assert _state_of(tree, subject="S01", trial="t3") == "red"
        assert _state_of(tree, subject="S02", trial="t1") == "grey"
        assert _state_of(tree, subject="S02", trial="t2") == "green"

    def test_header_counts_exclude_grey(self, states_db):
        tree = location_states(LocFilt, db=states_db)
        assert tree.counts == {"green": 2, "amber": 1, "red": 1, "grey": 1}
        assert tree.total == 4  # grey is in neither the numerator nor denominator
        assert tree.green == 2
        assert tree.verdict == "red"

    def test_rollup_takes_the_worst_state_beneath(self, states_db):
        tree = location_states(LocFilt, db=states_db)
        s01 = _node_at(tree, subject="S01")
        assert s01.state == "red"  # worst of green/amber/red
        assert s01.counts == {"green": 1, "amber": 1, "red": 1, "grey": 0}
        s02 = _node_at(tree, subject="S02")
        assert s02.state == "green"  # the grey child does not spoil it
        assert s02.counts == {"green": 1, "amber": 0, "red": 0, "grey": 1}

    def test_top_level_starts_with_one_node_per_first_key(self, states_db):
        tree = location_states(LocFilt, db=states_db)
        assert [(n.key, n.value) for n in tree.roots] == [
            ("subject", "S01"),
            ("subject", "S02"),
        ]
        assert all(n.key == "trial" for n in tree.roots[0].children)

    def test_leaves_carry_the_record_that_would_be_plotted(self, states_db):
        tree = location_states(LocFilt, db=states_db)
        green = _node_at(tree, subject="S01", trial="t1")
        assert green.is_leaf and green.record_id and green.schema_id is not None
        red = _node_at(tree, subject="S01", trial="t3")
        assert red.is_leaf and red.record_id is None

    def test_a_node_with_only_grey_beneath_it_is_grey_not_green(self, states_db):
        from scidb.exclusions import exclude_schema

        exclude_schema("whole subject withdrawn", subject="S02", trial="t2")
        tree = location_states(LocFilt, db=states_db)
        s02 = _node_at(tree, subject="S02")
        assert s02.state == "grey"
        assert s02.counts == {"green": 0, "amber": 0, "red": 0, "grey": 2}

    def test_verdict_is_green_only_when_everything_that_counts_is(self, states_db):
        tree = location_states(LocRaw, db=states_db)
        # LocRaw is raw: no producing function, so nothing can be stale or
        # missing — every location it holds is green.
        assert tree.basis == "present_only"
        assert tree.counts["amber"] == 0 and tree.counts["red"] == 0
        assert tree.verdict == "green"
        assert any("no producing function" in note for note in tree.notes)


class TestAgreesWithCheckComboState:
    """The anchor: the batch primitive and the per-combo API must not diverge."""

    _MAP = {"up_to_date": "green", "stale": "amber", "missing": "red"}

    def test_location_for_location(self, states_db):
        tree = location_states(LocFilt, db=states_db)
        checked = 0
        for subject in ("S01", "S02"):
            for trial in ("t1", "t2", "t3"):
                ours = _state_of(tree, subject=subject, trial=trial)
                if ours is None or ours == "grey":
                    continue  # excluded: check_combo_state has no notion of it
                theirs = check_combo_state(
                    loc_bandpass,
                    [LocFilt],
                    {"subject": subject, "trial": trial},
                    db=states_db,
                )
                assert ours == self._MAP[theirs], (
                    f"{subject}/{trial}: location_states says {ours!r}, "
                    f"check_combo_state says {theirs!r}"
                )
                checked += 1
        assert checked == 4

    def test_excluded_is_the_only_permitted_divergence(self, states_db):
        tree = location_states(LocFilt, db=states_db)
        assert _state_of(tree, subject="S02", trial="t1") == "grey"
        # check_combo_state knows nothing about exclusions and still sees a
        # perfectly good record there. That asymmetry is deliberate.
        assert (
            check_combo_state(
                loc_bandpass,
                [LocFilt],
                {"subject": "S02", "trial": "t1"},
                db=states_db,
            )
            == "up_to_date"
        )


class TestBatchHelpersMatchPerRecord:
    def test_variant_keys_batch_matches_per_record(self, variant_db):
        from scidb import provenance_query as pq

        rids = [
            row[0]
            for row in variant_db._duck._fetchall(
                "SELECT record_id FROM _record WHERE type = ?", ["LocFilt"]
            )
        ]
        assert rids
        batch = pq.variant_keys_batch(variant_db._duck, rids)
        for rid in rids:
            assert batch[rid] == pq._producing_variant_key(variant_db._duck, rid)

    def test_superseded_batch_matches_per_record(self, states_db):
        from scidb import provenance_query as pq
        from scidb.state import _has_superseded_ancestor

        rids = [
            row[0]
            for row in states_db._duck._fetchall(
                "SELECT record_id FROM _record WHERE type = ?", ["LocFilt"]
            )
        ]
        assert rids
        batch = pq.superseded_batch(states_db._duck, rids)
        assert any(batch.values()), "fixture must contain at least one amber"
        for rid in rids:
            assert batch[rid] == _has_superseded_ancestor(states_db, rid, rid)


# ---------------------------------------------------------------------------
# The partial-loader hole (.claude/plan-pathinput-loader-staleness-gap.md)
# ---------------------------------------------------------------------------


class TestPathInputLoaderDenominator:
    def test_unloaded_file_reads_red_here(self, loader_db):
        db, _path_input = loader_db
        tree = location_states(LocLoaded, db=db)
        assert tree.basis == "discovery"
        assert _state_of(tree, subject="S01", trial="t1") == "green"
        assert _state_of(tree, subject="S01", trial="t2") == "green"
        # On disk, never loaded — invisible to every other surface in the system.
        assert _state_of(tree, subject="S02", trial="t1") == "red"
        assert tree.counts == {"green": 2, "amber": 0, "red": 1, "grey": 0}
        assert tree.verdict == "red"

    def test_canvas_node_state_is_deliberately_unchanged(self, loader_db):
        db, _path_input = loader_db
        # The hole itself: expected == realized for an inputless function, so the
        # node badge stays green. Closing that is a separate change with its own
        # cost profile (discovery on every canvas refresh) — this test pins the
        # boundary so the fix here cannot be mistaken for the fix there.
        node = check_node_state(loc_import, [LocLoaded], db=db)
        assert node["state"] == "green"

    def test_matches_the_discovery_check_it_is_built_on(self, loader_db):
        db, path_input = loader_db
        res = check_pathinput_node_state(
            loc_import, [LocLoaded], {"filepath": path_input}, db=db
        )
        assert res["state"] == "red"
        assert res["counts"]["missing"] == 1
        tree = location_states(LocLoaded, db=db)
        assert tree.counts["red"] == res["counts"]["missing"]
        assert tree.counts["green"] == res["counts"]["up_to_date"]


# ---------------------------------------------------------------------------
# Variant scoping
# ---------------------------------------------------------------------------


class TestVariantScoping:
    def test_unscoped_sees_every_variant(self, variant_db):
        tree = location_states(LocFilt, db=variant_db)
        assert _state_of(tree, subject="S01", trial="t1") == "green"
        assert _state_of(tree, subject="S02", trial="t1") == "green"

    def test_a_variant_that_only_ran_somewhere_reds_the_rest(self, variant_db):
        tree = location_states(
            LocFilt, variant={"loc_bandpass.low_hz": 50}, db=variant_db
        )
        assert _state_of(tree, subject="S01", trial="t1") == "green"
        # S02 never ran low_hz=50 — expected there, absent for THIS variant.
        assert _state_of(tree, subject="S02", trial="t1") == "red"
        assert tree.variant == {"loc_bandpass.low_hz": 50}

    def test_the_other_variant_is_symmetric(self, variant_db):
        tree = location_states(
            LocFilt, variant={"loc_bandpass.low_hz": 20}, db=variant_db
        )
        assert _state_of(tree, subject="S01", trial="t1") == "green"
        assert _state_of(tree, subject="S02", trial="t1") == "green"
        assert tree.verdict == "green"

    def test_a_variant_matching_nothing_is_all_red_not_empty(self, variant_db):
        tree = location_states(
            LocFilt, variant={"loc_bandpass.low_hz": 999}, db=variant_db
        )
        assert tree.counts["green"] == 0
        assert tree.counts["red"] == tree.total
        assert tree.total > 0, "the expected set must survive an unmatched variant"


# ---------------------------------------------------------------------------
# Stage 2: the Inspector facade, the renderer, and the CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def states_path(tmp_path):
    """The same fixture, closed — the CLI opens its own connection."""
    path = tmp_path / "loc_cli.duckdb"
    build_states_db(path).close()
    return path


class TestInspectorFacade:
    def test_it_shapes_rather_than_recomputes(self, states_db):
        from scidb.inspect import Inspector

        facade = Inspector(states_db).locations("LocFilt")
        direct = location_states(LocFilt, db=states_db)

        assert facade.to_dict() == direct.to_dict()

    def test_accepts_a_class_as_well_as_a_name(self, states_db):
        from scidb.inspect import Inspector

        insp = Inspector(states_db)

        assert insp.locations(LocFilt).to_dict() == insp.locations("LocFilt").to_dict()

    def test_unknown_type_raises(self, states_db):
        from scidb import NotFoundError
        from scidb.inspect import Inspector

        with pytest.raises(NotFoundError, match="NoSuchVariable"):
            Inspector(states_db).locations("NoSuchVariable")

    def test_problems_only_prunes_but_keeps_the_denominator(self, states_db):
        from scidb.inspect import Inspector

        tree = Inspector(states_db).locations("LocFilt", problems_only=True)

        # S02's only non-green location is excluded, so the whole branch goes.
        assert [n.value for n in tree.roots] == ["S01"]
        # ...and the counts are NOT recomputed: 1 of S01's 3 is still green.
        assert tree.roots[0].counts == {"green": 1, "amber": 1, "red": 1, "grey": 0}
        assert tree.counts == {"green": 2, "amber": 1, "red": 1, "grey": 1}
        assert {n.value for n in tree.roots[0].children} == {"t2", "t3"}

    def test_problems_only_on_a_clean_variable_is_empty(self, states_db):
        from scidb.inspect import Inspector

        tree = Inspector(states_db).locations("LocRaw", problems_only=True)

        assert tree.roots == []
        assert tree.verdict == "green"  # the counts still describe the whole tree

    def test_variant_reaches_through(self, variant_db):
        from scidb.inspect import Inspector

        tree = Inspector(variant_db).locations(
            "LocFilt", variant={"loc_bandpass.low_hz": 50}
        )

        assert tree.variant == {"loc_bandpass.low_hz": 50}
        assert _state_of(tree, subject="S02", trial="t1") == "red"


class TestSerialization:
    def test_to_dict_carries_the_derived_header_numbers(self, states_db):
        """``asdict`` would drop them — they are properties on purpose."""
        import dataclasses

        tree = location_states(LocFilt, db=states_db)
        payload = tree.to_dict()

        assert payload["total"] == 4
        assert payload["green"] == 2
        assert payload["verdict"] == "red"
        assert "total" not in dataclasses.asdict(tree)

    def test_the_whole_tree_is_json_serializable(self, states_db):
        import json

        payload = location_states(LocFilt, db=states_db).to_dict()

        restored = json.loads(json.dumps(payload))
        assert restored["roots"][0]["path"] == [["subject", "S01"]]


class TestRenderer:
    def test_header_and_marks(self, states_db):
        from scidb.inspect import render

        text = render.render_location_tree(location_states(LocFilt, db=states_db))

        assert "LocFilt: 2/4 locations green" in text
        assert "1 amber, 1 red, 1 excluded" in text
        assert "denominator: expected" in text
        assert "subject=S01" in text and "trial=t3" in text

    def test_counts_appear_on_parents_not_leaves(self, states_db):
        from scidb.inspect import render

        lines = render.render_location_tree(
            location_states(LocFilt, db=states_db)
        ).splitlines()
        subject_line = next(line for line in lines if "subject=S01" in line)
        trial_line = next(line for line in lines if "trial=t1" in line)

        assert "1/3" in subject_line  # 1 green of 3 that count
        assert "/" not in trial_line  # a leaf is always 1/1 or 0/1

    def test_excluded_leaves_say_so(self, states_db):
        """Grey has to be legible as a decision, not read as a failure."""
        from scidb.inspect import render

        style = render.ASCII_STYLE
        lines = render.render_location_tree(
            location_states(LocFilt, db=states_db), style=style
        ).splitlines()
        excluded = [line for line in lines if "(excluded)" in line]

        assert len(excluded) == 1
        assert "trial=t1" in excluded[0]
        assert style.loc_mark_grey in excluded[0]

    def test_ascii_style_uses_no_unicode(self, states_db):
        from scidb.inspect import render

        text = render.render_location_tree(
            location_states(LocFilt, db=states_db), style=render.ASCII_STYLE
        )

        assert text.isascii()

    def test_depth_caps_the_output(self, states_db):
        from scidb.inspect import render

        text = render.render_location_tree(
            location_states(LocFilt, db=states_db),
            style=render.ASCII_STYLE,
            depth=1,
        )

        assert "subject=S01" in text
        assert "trial=" not in text  # a trial-level study is thousands of lines
        assert "... +3 deeper" in text  # S01 holds t1, t2, t3
        assert "... +2 deeper" in text  # S02 holds t1, t2

    def test_notes_are_shown_beside_the_count(self, states_db):
        from scidb.inspect import render

        text = render.render_location_tree(location_states(LocRaw, db=states_db))

        assert "! " in text
        assert "no producing function" in text


class TestCli:
    def test_json_payload(self, states_path, capsys):
        payload = run_locations_json(capsys, states_path, ["LocFilt", "--json"])

        assert payload["variable"] == "LocFilt"
        assert (payload["green"], payload["total"]) == (2, 4)
        assert payload["verdict"] == "red"
        assert payload["basis"] == "expected"

    def test_problems_flag(self, states_path, capsys):
        payload = run_locations_json(
            capsys, states_path, ["LocFilt", "--problems", "--json"]
        )

        assert [r["value"] for r in payload["roots"]] == ["S01"]

    def test_variant_flag_literal_evals_values(self, states_path, capsys):
        """``low_hz=20`` must match the stored int, not the string."""
        payload = run_locations_json(
            capsys, states_path, ["LocFilt", "--variant", "loc_bandpass.low_hz=20", "--json"]
        )

        assert payload["variant"] == {"loc_bandpass.low_hz": 20}
        assert payload["green"] == 2  # the same records; there is only one variant

    def test_human_render_smoke(self, states_path, capsys):
        from scidb.inspect.cli import main

        assert main(["--db", str(states_path), "locations", "LocFilt"]) == 0
        assert "locations green" in capsys.readouterr().out

    def test_unknown_type_is_a_clean_failure(self, states_path, capsys):
        from scidb.inspect.cli import main

        assert main(["--db", str(states_path), "locations", "Nope"]) != 0


def run_locations_json(capsys, db_path, argv):
    import json as _json

    from scidb.inspect.cli import main

    rc = main(["--db", str(db_path), "locations", *argv])
    out = capsys.readouterr().out
    assert rc == 0, out
    return _json.loads(out)
