"""
The collapse chain: which ``COLLAPSE`` keys average away first, and which one
is the sample.

One owner (``roles.collapse_order``), read by reduce, reducer, ylimits and
codegen. The order is the data's own nesting — deepest first — so "trial
within subject, then subject" is what a methods section means by "mean across
subjects": each subject counts once however many trials it has. The LAST key
is the sample the kind's statistic is computed over.

See docs/claude/grouping-and-collapse.md.
"""

from __future__ import annotations

import pandas as pd

from scistackplot import LongTable, Role
from scistackplot.roles import collapse_order, has_sample, sample_key


def _table(frame, **kwargs) -> LongTable:
    return LongTable.from_frame(frame, name="M", **kwargs)


def _frame(**columns) -> pd.DataFrame:
    return pd.DataFrame(columns)


def test_deepest_key_collapses_first(scalar_table):
    roles = {"subject": Role.COLLAPSE, "session": Role.GROUP, "trial": Role.COLLAPSE}
    assert collapse_order(roles, scalar_table) == ["trial", "subject"]
    assert sample_key(roles, scalar_table) == "subject"


def test_dict_order_does_not_matter(scalar_table):
    """Whichever role the user clicked first, the chain runs by depth."""
    a = {"subject": Role.COLLAPSE, "trial": Role.COLLAPSE}
    b = {"trial": Role.COLLAPSE, "subject": Role.COLLAPSE}
    assert collapse_order(a, scalar_table) == collapse_order(b, scalar_table)


def test_only_collapsed_factors_are_in_the_chain(scalar_table):
    roles = {"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.COLLAPSE}
    assert collapse_order(roles, scalar_table) == ["trial"]
    assert sample_key(roles, scalar_table) == "trial"


def test_nothing_collapsed_means_no_sample(scalar_table):
    roles = {"subject": Role.ITERATE, "session": Role.GROUP, "trial": Role.GROUP}
    assert collapse_order(roles, scalar_table) == []
    assert sample_key(roles, scalar_table) is None
    assert has_sample(roles) is False


def test_has_sample_needs_no_table():
    assert has_sample({"a": Role.GROUP, "b": Role.COLLAPSE}) is True
    assert has_sample({"a": Role.GROUP, "b": Role.ITERATE}) is False


def test_a_field_factor_collapses_innermost(struct_table):
    """ColName sits INSIDE a record: averaging muscles together happens before
    any schema key is touched."""
    roles = {"subject": Role.COLLAPSE, "trial": Role.COLLAPSE, "ColName": Role.COLLAPSE}
    assert collapse_order(roles, struct_table) == ["ColName", "trial", "subject"]


def test_a_joined_factor_variable_collapses_with_its_key():
    """A subject-level ``Group`` column carries depth 1 like ``subject``; it
    goes beside subject, never inside trial."""
    frame = _frame(
        subject=["01", "01", "02", "02"],
        trial=["1", "2", "1", "2"],
        Group=["A", "A", "B", "B"],
        M=[1.0, 2.0, 3.0, 4.0],
    )
    table = _table(
        frame,
        factors=["subject", "trial", "Group"],
        measures=["M"],
        schema_levels=["subject", "trial"],
        factor_depths={"Group": 1},
    )
    roles = {"Group": Role.COLLAPSE, "subject": Role.COLLAPSE, "trial": Role.COLLAPSE}
    order = collapse_order(roles, table)
    assert order[0] == "trial"
    assert set(order[1:]) == {"Group", "subject"}


def test_a_factor_with_no_depth_collapses_last():
    """A derived bucket is not in the hierarchy at all: it cannot be claimed
    to sit inside a subject, so it is the outermost — the sample."""
    frame = _frame(
        subject=["01", "01", "02", "02"],
        bucket=["lo", "hi", "lo", "hi"],
        M=[1.0, 2.0, 3.0, 4.0],
    )
    table = _table(
        frame,
        factors=["subject", "bucket"],
        measures=["M"],
        schema_levels=["subject"],
    )
    roles = {"bucket": Role.COLLAPSE, "subject": Role.COLLAPSE}
    assert collapse_order(roles, table) == ["subject", "bucket"]


def test_a_csv_table_keeps_declaration_order(scalar_frame):
    """No hierarchy at all (a CSV): every factor ranks the same, and the stable
    sort keeps the order the roles were declared in."""
    table = _table(scalar_frame, factors=["subject", "session", "trial"], measures=["StepLength"])
    roles = {"trial": Role.COLLAPSE, "subject": Role.COLLAPSE}
    assert collapse_order(roles, table) == ["trial", "subject"]


def test_factors_the_table_lacks_are_ignored(scalar_table):
    roles = {"trial": Role.COLLAPSE, "ghost": Role.COLLAPSE}
    assert collapse_order(roles, scalar_table) == ["trial"]
