"""A small real scidb database: trial-level step lengths, pre/post sessions."""

from __future__ import annotations

import pytest
from scidb import BaseVariable, configure_database
from scidb.database import _local

SCHEMA = ["subject", "session", "trial"]
SUBJECTS = [f"{n:02d}" for n in range(1, 5)]
SESSIONS = ["pre", "post"]
TRIALS = ["1", "2"]


class StatStepLength(BaseVariable):
    """Scalar, trial level; post is longer than pre by 0.1 + 0.01 * subject."""

    schema_version = 1


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "stats.duckdb", SCHEMA)
    yield database
    database.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture
def seeded(db):
    for subject in SUBJECTS:
        for session in SESSIONS:
            for trial in TRIALS:
                value = 1.0 + 0.05 * int(subject) + 0.01 * int(trial)
                if session == "post":
                    value += 0.1 + 0.01 * int(subject)
                StatStepLength.save(value, subject=subject, session=session, trial=trial)
    return db
