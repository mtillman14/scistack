"""A data column named after a schema key must not break the loader.

The 2026-09-16 failure: ``FunctionalOutcomes`` was first saved (before the
spread/collision rules stripped schema-key columns from returned tables) as one
dataset-level 73x27 record whose columns included ``subject`` and ``session``.
That record put a ``subject`` and a ``session`` DATA column into the variable's
DuckDB table for good — records are excluded, never deleted, and a table's
columns outlive every record. ``load_variable`` selects every data column and
every schema key, so the frame held two ``subject`` columns,
``frame["subject"]`` was a DataFrame, and ``if frame[key].notna().any()`` raised
"The truth value of a Series is ambiguous".

Pinned here:

* ``data_columns_for`` never reports a schema key as a data column, and says
  so at WARN;
* ``load_variable`` returns, with the schema key as a level and one column per
  schema key in the frame;
* the correctly-filed records (the re-save) are what the plot sees.

Since 2026-10-01 ``DatabaseManager._check_column_set`` refuses a save whose
columns differ from the table's, so the re-save can no longer be WRITTEN onto
the legacy table. Databases written before that still hold this mix, and
the loader must still read them; the fixture rebuilds one by switching the
guard off for the setup only — the save path behind it is unchanged.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest
from scidb import BaseVariable

from scistackplotdb.load import data_columns_for, load_variable


class Outcomes(BaseVariable):
    """A wide REDCap-style sheet, one row per (subject, session)."""

    schema_version = 1


def _sheet() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "subject": ["01", "01", "02"],
            "session": ["pre", "post", "pre"],
            "TUG_Time": [8.1, 7.4, 9.0],
            "SixMWT_Distance": [310.0, 340.0, 280.0],
        }
    )


def _save_legacy_and_spread_records(db, monkeypatch):
    # The legacy record: the whole sheet, schema-key columns and all, filed at
    # the dataset level. This is what creates the shadowing table columns.
    # Through `save_batch` (the for_each batch path the 2026-09-15 run took),
    # NOT `Outcomes.save`: `BaseVariable.save` auto-distributes a DataFrame
    # that carries schema-key columns, so it could never write this shape.
    db.save_batch(Outcomes, [(_sheet(), {})])
    # What the spread rule files now: one row per address, keys stripped.
    # Written as a pre-2026-10-01 run did, before the column-set guard.
    with monkeypatch.context() as patch:
        patch.setattr(db, "_check_column_set", lambda *args, **kwargs: None)
        _save_spread_records()


def _save_spread_records():
    for _, row in _sheet().iterrows():
        Outcomes.save(
            pd.DataFrame(
                {
                    "TUG_Time": [row["TUG_Time"]],
                    "SixMWT_Distance": [row["SixMWT_Distance"]],
                }
            ),
            subject=row["subject"],
            session=row["session"],
        )


def test_schema_key_named_columns_are_not_data_columns(db, caplog, monkeypatch):
    _save_legacy_and_spread_records(db, monkeypatch)
    with caplog.at_level(logging.WARNING, logger="scistackplotdb"):
        columns = data_columns_for(db, "Outcomes")
    assert "subject" not in columns and "session" not in columns, columns
    assert set(columns) == {"TUG_Time", "SixMWT_Distance"}, columns
    shadow_lines = [
        r.getMessage() for r in caplog.records if "named after schema key" in r.getMessage()
    ]
    assert shadow_lines, [r.getMessage() for r in caplog.records]
    assert "'subject'" in shadow_lines[0] and "'session'" in shadow_lines[0]


def test_load_variable_survives_shadowing_columns(db, monkeypatch):
    _save_legacy_and_spread_records(db, monkeypatch)
    loaded = load_variable(db, "Outcomes")  # raised before the fix
    frame = loaded.frame
    # Exactly one column per schema key — the address, not the payload copy.
    assert list(frame.columns).count("subject") == 1
    assert list(frame.columns).count("session") == 1
    assert loaded.levels == ["subject", "session"], loaded.levels
    assert set(loaded.data_columns) == {"TUG_Time", "SixMWT_Distance"}
    # The three addressed records plus the legacy dataset-level one.
    addressed = frame[frame["subject"].notna()]
    assert sorted(zip(addressed["subject"], addressed["session"])) == [
        ("01", "post"),
        ("01", "pre"),
        ("02", "pre"),
    ]


def test_the_legacy_mix_cannot_be_written_today(db):
    """Why the fixture switches the guard off: without it, the re-save onto
    the legacy table is refused (and nothing is written)."""
    from scidb import ColumnSetChangedError

    db.save_batch(Outcomes, [(_sheet(), {})])
    with pytest.raises(ColumnSetChangedError, match="Outcomes"):
        _save_spread_records()
