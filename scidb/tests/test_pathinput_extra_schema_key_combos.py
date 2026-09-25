"""A schema key with no possible source must not force the Cartesian product.

PathInput discovery finds the combos that actually exist on disk, and
``PathInput.apply_discovery`` returns them so iteration runs over real files
instead of a Cartesian product that invents combos with no file behind them.
``resolve_pathinput_discovery`` only accepts those disk combos when every
iterated key is a template placeholder.

The regression: a schema key the template cannot supply AND the database cannot
fill (e.g. schema ``[subject, session, cycle]`` against a template with only
``{subject}/{session}``) was still sitting in ``metadata_iterables`` when that
subset test ran, so the test failed and the Cartesian product won -- even
though scidb dropped the key from the iteration moments later. On a real run
that turned 419 files into 952 combos, 533 of which could only fail on a
missing file (2026-09-13, .claude/matlab-run-timing-and-phantom-combos-plan.md).

The fix passes the provably-unsupplyable keys into
``resolve_pathinput_discovery`` so a key on its way out does not vote.
"""

import logging
from pathlib import Path

import pytest
import scifor as _scifor

from scidb import configure_database, for_each


def read_content(filepath):
    return Path(str(filepath)).read_text().strip()


@pytest.fixture
def sparse_tree(tmp_path):
    """A subject x session tree where only SOME combinations exist.

    subjects {s1, s2} x sessions {A, B} would be 4 combos; only 3 files exist.
    """
    root = tmp_path / "data"
    for subject, session in [("s1", "A"), ("s1", "B"), ("s2", "A")]:
        d = root / subject
        d.mkdir(parents=True, exist_ok=True)
        (d / f"{session}.txt").write_text(f"{subject}-{session}")
    return root


@pytest.fixture
def empty_db(tmp_path):
    """Schema carries a third key the template can never supply."""
    db = configure_database(
        tmp_path / "test.duckdb", ["subject", "session", "cycle"]
    )
    yield db
    _scifor.set_schema([])
    db.close()


class TestExtraSchemaKeyDoesNotInventCombos:
    def test_iterates_disk_combos_not_cartesian_product(
        self, empty_db, sparse_tree
    ):
        pi = _scifor.PathInput(str(sparse_tree / "{subject}" / "{session}.txt"), name=str(str(sparse_tree / "{subject}" / "{session}.txt")))

        result = for_each(
            read_content,
            {"filepath": pi},
            [],
            save=False,
            schema_keys=["subject", "session", "cycle"],
        )

        # 3 files on disk, not 2 subjects x 2 sessions == 4.
        assert len(result) == 3
        assert set(result["result"]) == {"s1-A", "s1-B", "s2-A"}

        # The combo that has no file must be absent entirely, not present as a
        # failed/empty row -- that absence is the whole point.
        pairs = set(zip(result["subject"], result["session"]))
        assert ("s2", "B") not in pairs

    def test_unsupplyable_key_still_dropped_from_iteration(
        self, empty_db, sparse_tree
    ):
        """The key with no source is dropped, exactly as before the fix."""
        pi = _scifor.PathInput(str(sparse_tree / "{subject}" / "{session}.txt"), name=str(str(sparse_tree / "{subject}" / "{session}.txt")))

        result = for_each(
            read_content,
            {"filepath": pi},
            [],
            save=False,
            schema_keys=["subject", "session", "cycle"],
        )

        # 'cycle' could not be filled by the DB or the template, so it is not
        # an iteration dimension and contributes no column.
        assert "cycle" not in result.columns

    def test_all_keys_templated_still_uses_disk_combos(
        self, tmp_path, sparse_tree
    ):
        """Control: with no extra schema key the behaviour was already correct.

        Guards against a fix that only works when an unsupplyable key is
        present.
        """
        db = configure_database(tmp_path / "ctl.duckdb", ["subject", "session"])
        try:
            pi = _scifor.PathInput(
                str(sparse_tree / "{subject}" / "{session}.txt"), name=str(str(sparse_tree / "{subject}" / "{session}.txt"))
            )
            result = for_each(
                read_content,
                {"filepath": pi},
                [],
                save=False,
                schema_keys=["subject", "session"],
            )
            assert len(result) == 3
        finally:
            _scifor.set_schema([])
            db.close()

    def test_explicit_user_values_still_win(self, empty_db, sparse_tree, caplog):
        """An explicit value asserts intent: the Cartesian product still drives.

        The fix must not silently start filtering a caller-specified sweep down
        to what happens to exist on disk -- that distinction is what
        user_explicit_keys protects, and it is unchanged here.

        Asserted on the DECISION rather than on the row count, because the two
        paths agree on rows here and disagree on what they attempt: discovery
        never visits the fileless combo, while an explicit sweep visits it and
        fails. ``schema_keys=`` is not usable alongside explicit values --
        ``scifor.expand_schema_keys`` refuses both in one call, being sugar for
        exactly these ``key: []`` entries -- so ``cycle=[]`` is spelled out.
        """
        pi = _scifor.PathInput(str(sparse_tree / "{subject}" / "{session}.txt"), name=str(str(sparse_tree / "{subject}" / "{session}.txt")))

        with caplog.at_level(logging.INFO, logger="scidb"):
            for_each(
                read_content,
                {"filepath": pi},
                [],
                save=False,
                subject=["s1", "s2"],
                session=["A", "B"],
                cycle=[],
            )

        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "PathInput discovery did not drive iteration" in text
        assert "discovery drives iteration directly" not in text

    def test_auto_filled_keys_take_the_disk_combos(self, empty_db, sparse_tree, caplog):
        """The contrast to the test above, on the same observable.

        Nothing explicit -> discovery's own combos drive iteration, so the
        fileless combo is never visited at all.
        """
        pi = _scifor.PathInput(str(sparse_tree / "{subject}" / "{session}.txt"), name=str(str(sparse_tree / "{subject}" / "{session}.txt")))

        with caplog.at_level(logging.INFO, logger="scidb"):
            for_each(
                read_content,
                {"filepath": pi},
                [],
                save=False,
                schema_keys=["subject", "session", "cycle"],
            )

        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "discovery drives iteration directly: 3 combo(s)" in text
        assert "did not drive iteration" not in text
