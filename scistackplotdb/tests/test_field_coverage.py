"""A field that is a number in only a few records says so, and says where.

The 2026-10-15 report: plotting ``CosmedData``'s ``Dist_m`` field "showed one
group/one subject" while ``O2Cost`` and ``EE_min`` followed the grouping. The
log's only clue was ``y limits: 708 of 714 cell(s) ... hold nothing numeric``,
raised long after the load and naming no field. The plot was right — only 6
records held a distance — but nothing said which records, or what the other
708 held. ``load_variable`` now reports per field (``_log_field_coverage``):
INFO for every scalar field that is not a number everywhere, WARN with the
levels the numbers sit at when fewer than half the records hold one.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from scidb import BaseVariable

from scistackplotdb.load import SPARSE_FIELD_SHARE, field_coverage, load_variable


class Cosmed(BaseVariable):
    """One row per (subject, session): a well-filled field and a sparse one."""

    schema_version = 1


def _save(db):
    rows = [
        ("01", "pre", 10.0, 450.0),
        ("01", "post", 11.0, np.nan),
        ("02", "pre", 12.0, np.nan),
        ("02", "post", 13.0, np.nan),
        ("03", "pre", 14.0, np.nan),
    ]
    db.save_batch(
        Cosmed,
        [
            (pd.DataFrame({"O2Cost": [o2], "Dist_m": [dist]}), {"subject": s, "session": t})
            for s, t, o2, dist in rows
        ],
    )


def test_field_coverage_counts_numbers_nulls_and_text():
    frame = pd.DataFrame(
        {
            "full": [1.0, 2.0, 3.0],
            "holes": [1.0, np.nan, np.nan],
            "mixed": pd.Series([4.0, "1,234", None], dtype=object),
            "signal": pd.Series([np.arange(3.0), np.arange(2.0), np.arange(4.0)], dtype=object),
        }
    )
    types = {"full": "DOUBLE", "holes": "DOUBLE", "mixed": "VARCHAR", "signal": "DOUBLE[]"}
    coverage, is_number = field_coverage(frame, types)
    by_name = {c.column: c for c in coverage}
    assert "signal" not in by_name, "a 1-D field is not 'missing a number'"
    assert by_name["full"].missing == 0
    assert (by_name["holes"].numeric, by_name["holes"].null) == (1, 2)
    mixed = by_name["mixed"]
    assert (mixed.numeric, mixed.null, mixed.text) == (1, 1, 1)
    assert mixed.text_examples == ("1,234",)
    assert "1 text (e.g. '1,234')" in mixed.describe()
    assert list(is_number["holes"]) == [True, False, False]


def test_a_sparse_field_is_warned_with_where_its_numbers_are(db, caplog):
    _save(db)
    with caplog.at_level(logging.INFO, logger="scistackplotdb"):
        load_variable(db, "Cosmed")
    messages = [r.getMessage() for r in caplog.records]
    summary = [m for m in messages if m.startswith("field coverage of Cosmed")]
    assert summary, messages
    assert "Dist_m" in summary[0] and "1/5 numeric (4 NULL)" in summary[0]
    assert "O2Cost" not in summary[0], "a full field is not listed"
    warned = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and "Cosmed.Dist_m" in r.getMessage()
    ]
    assert len(warned) == 1, messages
    # The one record with a distance: subject 01, session pre.
    assert "subject ∈ {01}" in warned[0] and "session ∈ {pre}" in warned[0]


def test_a_mostly_filled_field_is_not_warned(db, caplog):
    _save(db)
    with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
        load_variable(db, "Cosmed")
    assert SPARSE_FIELD_SHARE == 0.5
    assert not [r for r in caplog.records if "Cosmed.O2Cost" in r.getMessage()]
