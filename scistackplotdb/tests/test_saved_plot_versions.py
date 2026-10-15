"""
A saved plot remembers which CODE drew it (2026-10-15).

The user's scenario: save a plot of data from v1 of ``load_cbrace.m``, edit the
function, re-run, reopen. The plot must reopen on v1 and report that v2 exists;
"switch to latest" re-pins it to follow latest from then on. Before this, the
plot stored ``variant_sets=[{selection: {}}]`` — no version at all, because
with one version there was no code axis to pin.

Freezing stores function HASHES (ordinals renumber after a real delete) and
works with a single version, which is the usual state at save time.

See ``docs/claude/variant-row-selection.md`` and ``.claude/plan-saved-plot-version-pins.md``.
"""

from __future__ import annotations

import json

import numpy as np
import pytest
from scidb import for_each
from scistackplot import PlotSpec, VariantSet
from scistackplot.variants import apply_variant_sets

from scistackplotdb import (
    ScidbSource,
    follow_latest_saved_plot,
    load_saved_plot,
    save_plot,
)
from scistackplotdb.saved import FROZEN_KEY

from conftest import Scaled, Signal

CODE = "Code:scale_signal"


def _scale_v1(signal):
    return float(np.mean(signal) * 2)


def _scale_v2(signal):
    return float(np.mean(signal) * 2 + 1)


def _run(body):
    body.__name__ = "scale_signal"  # a body EDIT: same name, new hash
    for_each(
        body,
        inputs={"signal": Signal},
        outputs=[Scaled],
        subject=[],
        session=[],
        trial=[],
    )


def _table(db):
    # A fresh source each time: the test changes the data between reads.
    return ScidbSource(db).get_table(["Scaled"])


def _stored_envelope(db, plot_id: str, version: int) -> dict:
    row = db._duck._fetchone(
        "SELECT envelope_json FROM _saved_plot WHERE plot_id = ? AND version = ?",
        [plot_id, version],
    )
    return json.loads(row[0])


@pytest.fixture
def saved_on_v1(seeded):
    """A plot saved while ``scale_signal`` had ONE version."""
    _run(_scale_v1)
    spec = PlotSpec(measures=["Scaled"], variant_sets=[VariantSet()])
    info = save_plot(seeded, "Scaled", "my plot", spec, {}, table=_table(seeded))
    return seeded, info


def test_a_single_version_is_frozen_by_hash(saved_on_v1):
    db, info = saved_on_v1

    frozen = _stored_envelope(db, info.plot_id, info.version)[FROZEN_KEY]

    assert set(frozen) == {"0"}
    assert list(frozen["0"]) == ["scale_signal"]
    assert len(frozen["0"]["scale_signal"]) == 1


def test_reopening_before_any_new_version_changes_nothing(saved_on_v1):
    db, info = saved_on_v1

    saved = load_saved_plot(db, info.plot_id, table=_table(db))

    assert saved.newer == []
    assert saved.spec.variant_sets[0].frozen == {}, "one version: no axis to pin"


def test_after_an_edit_it_reopens_on_v1_and_reports_v2(saved_on_v1):
    db, info = saved_on_v1
    _run(_scale_v2)
    table = _table(db)

    saved = load_saved_plot(db, info.plot_id, table=table)

    assert saved.spec.variant_sets[0].frozen == {CODE: ["v1"]}
    assert [(n["function"], n["frozen"], n["newest"]) for n in saved.newer] == [
        ("scale_signal", ["v1"], "v2")
    ]
    drawn = apply_variant_sets(saved.spec, table).frame
    assert set(drawn[CODE]) == {"v1"}
    assert len(drawn) == 12
    assert saved.to_dict()["newer"] == saved.newer


def test_resaving_a_frozen_plot_keeps_the_same_hash_and_stores_no_ordinal(saved_on_v1):
    db, info = saved_on_v1
    _run(_scale_v2)
    table = _table(db)
    saved = load_saved_plot(db, info.plot_id, table=table)

    again = save_plot(db, "Scaled", "my plot", saved.spec, {}, table=table)

    first = _stored_envelope(db, info.plot_id, info.version)
    second = _stored_envelope(db, again.plot_id, again.version)
    assert second[FROZEN_KEY] == first[FROZEN_KEY]
    assert "frozen" not in second["spec"]["variant_sets"][0], (
        "ordinals are derived on open; storing one could renumber"
    )


def test_switching_to_latest_appends_a_version_that_follows_latest(saved_on_v1):
    db, info = saved_on_v1
    _run(_scale_v2)

    new_info = follow_latest_saved_plot(db, info.plot_id)
    table = _table(db)
    saved = load_saved_plot(db, info.plot_id, table=table)

    assert new_info.version == info.version + 1
    assert saved.info.version == new_info.version
    assert saved.newer == []
    assert saved.spec.variant_sets[0].follow_latest is True
    assert saved.spec.variant_sets[0].frozen == {}
    assert set(apply_variant_sets(saved.spec, table).frame[CODE]) == {"v2"}
    assert _stored_envelope(db, info.plot_id, info.version)[FROZEN_KEY], (
        "the old version keeps its freeze — history is never rewritten"
    )


def test_a_follow_latest_row_is_not_frozen_on_later_saves(saved_on_v1):
    db, info = saved_on_v1
    _run(_scale_v2)
    follow_latest_saved_plot(db, info.plot_id)
    table = _table(db)
    saved = load_saved_plot(db, info.plot_id, table=table)

    again = save_plot(db, "Scaled", "my plot", saved.spec, {}, table=table)

    assert _stored_envelope(db, again.plot_id, again.version)[FROZEN_KEY] == {}


def test_an_explicit_old_version_pin_is_also_dropped_by_switching(saved_on_v1):
    db, info = saved_on_v1
    _run(_scale_v2)
    pinned = PlotSpec(
        measures=["Scaled"], variant_sets=[VariantSet(None, {CODE: "v1"})]
    )
    save_plot(db, "Scaled", "my plot", pinned, {}, table=_table(db))

    follow_latest_saved_plot(db, info.plot_id)
    table = _table(db)
    saved = load_saved_plot(db, info.plot_id, table=table)

    assert saved.spec.variant_sets[0].selection == {}
    assert set(apply_variant_sets(saved.spec, table).frame[CODE]) == {"v2"}


def test_a_plot_saved_without_a_freeze_opens_on_latest_with_no_prompt(seeded):
    """Plots saved before freezing existed: no back-fill, no prompt."""
    _run(_scale_v1)
    spec = PlotSpec(measures=["Scaled"], variant_sets=[VariantSet()])
    info = save_plot(seeded, "Scaled", "old plot", spec, {})  # no table: no freeze
    _run(_scale_v2)
    table = _table(seeded)

    saved = load_saved_plot(seeded, info.plot_id, table=table)

    assert saved.newer == []
    assert saved.spec.variant_sets[0].frozen == {}
    assert set(apply_variant_sets(saved.spec, table).frame[CODE]) == {"v2"}


def test_a_plot_with_no_rows_freezes_into_a_synthesised_row_zero(seeded):
    _run(_scale_v1)
    info = save_plot(
        seeded, "Scaled", "bare", PlotSpec(measures=["Scaled"]), {}, table=_table(seeded)
    )
    _run(_scale_v2)

    saved = load_saved_plot(seeded, info.plot_id, table=_table(seeded))

    assert saved.spec.variant_sets[0].frozen == {CODE: ["v1"]}
    assert saved.newer and saved.newer[0]["newest"] == "v2"
