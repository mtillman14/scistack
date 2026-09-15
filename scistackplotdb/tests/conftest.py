"""
A small real scidb database.

Zero-padded subject IDs are deliberate: they are the ordering trap the design
doc calls out, and they only bite against a real database where the keys are
strings by project rule.
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")  # tests must never open a window

import numpy as np
import pytest
from scidb import BaseVariable, configure_database
from scidb.database import _local

SCHEMA = ["subject", "session", "trial"]
SUBJECTS = [f"{n:02d}" for n in range(1, 4)]
SESSIONS = ["pre", "post"]
TRIALS = ["1", "2"]


class StepLength(BaseVariable):
    """Scalar, trial level."""

    schema_version = 1


class Signal(BaseVariable):
    """1-D array, trial level."""

    schema_version = 1


class Mass(BaseVariable):
    """Scalar, subject level — the broadcast-join case."""

    schema_version = 1


class Emg(BaseVariable):
    """Dict-valued: one column per muscle (scidb multi_column mode)."""

    schema_version = 1


class EmgFiltered(BaseVariable):
    """A SECOND dict-valued variable with the same muscles as ``Emg``.

    The archetypal "plot these two together" case: Raw vs Filtered EMG, both
    keyed by muscle, compared muscle by muscle.
    """

    schema_version = 1


class Condition(BaseVariable):
    """Subject-level TEXT — the grouping case (stim vs sham).

    Classifies as CATEGORICAL, so it is rightly refused as a measure; as a
    *factor* it is exactly the grouping a study already records.
    """

    schema_version = 1


class Demographics(BaseVariable):
    """Subject-level WIDE table — the spreadsheet case.

    One column per field (scidb multi_column mode), holding a different KIND of
    value in each: a number, two categoricals, a constant and an identifier. The
    variable as a whole therefore has no value to group by, which is the whole
    reason a grouping has to be able to name a column.
    """

    schema_version = 1


class Scaled(BaseVariable):
    """Produced by a pipeline step, so it can carry branch params."""

    schema_version = 1


class GroupLabel(BaseVariable):
    """Subject-level TEXT **produced by a pipeline step**, so it has variants.

    ``Condition`` is saved directly and therefore has exactly one record per
    subject — which is why it cannot exercise the question "WHICH variant of the
    grouping variable supplies the labels". This one is written by a swept
    ``for_each``, so each subject has two records carrying different labels, and
    a figure grouped by it is wrong in a way that looks like data unless the
    variant is pinned.
    """

    schema_version = 1


class Summarized(BaseVariable):
    """Produced from ``Scaled`` — the SECOND pipeline layer.

    Exists so a test can edit an *upstream* function's body and ask what the
    downstream variable knows about it. One layer is not enough to exercise
    that: ``CodeVersion`` is derived from the immediate producing invocation
    only, so the gap only appears a hop away from the edit.
    See docs/claude/variant-selection.md §2.
    """

    schema_version = 1


class StepLengthFigure(BaseVariable):
    """Endpoint output: the figure's path."""

    schema_version = 1


@pytest.fixture
def db(tmp_path):
    database = configure_database(tmp_path / "plots.duckdb", SCHEMA)
    yield database
    database.close()
    if hasattr(_local, "database"):
        delattr(_local, "database")


@pytest.fixture
def seeded(db):
    """Trial-level scalars and signals, plus subject-level mass."""
    rng = np.random.default_rng(0)
    for subject in SUBJECTS:
        Mass.save(70.0 + int(subject), subject=subject)
        # Subjects 01 and 03 stim, 02 sham — an uneven split, so a test cannot
        # pass by accident on symmetric counts.
        Condition.save("stim" if int(subject) % 2 else "sham", subject=subject)
        for session in SESSIONS:
            for trial in TRIALS:
                StepLength.save(
                    float(rng.normal(1.2, 0.1)),
                    subject=subject,
                    session=session,
                    trial=trial,
                )
                Signal.save(
                    rng.normal(0.0, 1.0, size=8),
                    subject=subject,
                    session=session,
                    trial=trial,
                )
                Emg.save(
                    {
                        "RHAM": rng.normal(0.0, 1.0, size=8),
                        "RTA": rng.normal(0.0, 1.0, size=8),
                        "LMG": rng.normal(0.0, 1.0, size=8),
                    },
                    subject=subject,
                    session=session,
                    trial=trial,
                )
                EmgFiltered.save(
                    {
                        "RHAM": rng.normal(0.0, 0.5, size=8),
                        "RTA": rng.normal(0.0, 0.5, size=8),
                        "LMG": rng.normal(0.0, 0.5, size=8),
                    },
                    subject=subject,
                    session=session,
                    trial=trial,
                )
    return db


@pytest.fixture
def with_demographics(seeded):
    """``seeded`` plus a wide demographics sheet, missing one subject.

    Subject 03 is deliberately absent: a participant who is in the study but not
    in the spreadsheet is the ordinary case, and their trials must still reach
    the figure (as ``MISSING_LEVEL``) rather than disappear from it.

    Kept as its own fixture rather than folded into ``seeded`` so every existing
    test keeps the variable set it was written against.
    """
    rows = {
        "01": {
            "Age": 64.0,
            "Sex": "F",
            "InterventionGroup": "Onward",
            "Site": "Boston",
            "RecordId": "R-0001",
        },
        "02": {
            "Age": 71.0,
            "Sex": "M",
            "InterventionGroup": "Digitimer",
            "Site": "Boston",
            "RecordId": "R-0002",
        },
    }
    for subject, row in rows.items():
        Demographics.save(row, subject=subject)
    return seeded


def _label(mass, scheme):
    """Two labelling schemes that disagree about every subject.

    Module level, not inside the fixture: ``for_each`` hashes the callable and
    records it as the producing function, and a nested def would be a different
    object on every call.
    """
    return f"{scheme}-{'high' if mass > 71.5 else 'low'}"


@pytest.fixture
def two_label_variants(seeded):
    """``GroupLabel`` written twice per subject, under two swept schemes.

    The grouping-variable equivalent of a variable with two pipeline variants,
    and the only fixture here where "which variant supplies the labels" has a
    visible answer: the two schemes label every subject differently, so a figure
    grouped by it is wrong in a way that looks like data unless the variant is
    pinned.

    In conftest rather than beside its first test because the parity suite needs
    the same database — the interactive pin and the exported one have to be
    compared against one set of records, not two that happen to be built the
    same way.
    """
    from scidb import EachOf, for_each

    for_each(
        _label,
        inputs={"mass": Mass, "scheme": EachOf("a", "b")},
        outputs=[GroupLabel],
        subject=[],
    )
    return seeded


def scheme_axis(source) -> str:
    """The branch-param column ``two_label_variants`` produces.

    Read off the frame by SUFFIX rather than hard-coded: the namespacing
    (``<function>.<param>``) is scidb's, and a test is not the place to restate
    it. A second axis appearing later should fail here loudly rather than
    silently pin the wrong one.
    """
    columns = source._variable_frame("GroupLabel").variant_columns
    assert columns, "the fixture should have produced a variant axis"
    matches = [column for column in columns if column.endswith(".scheme")]
    assert len(matches) == 1, f"expected one scheme axis, got {columns}"
    return matches[0]
