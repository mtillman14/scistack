"""The discovery walk reads each directory once and says what it read.

``PathInput._list_dir`` has memoized listings, validated by mtime, since the
numeric-fallback work — but ``_walk`` (the discovery path) went around it with a
bare ``os.listdir``, so every ``discover()`` re-read every directory. On a UNC
share that is one network round-trip per directory per call, and on 2026-09-13 a
second discovery of an unchanged share blocked for minutes with nothing logged
(.claude/plot-at-scale-plan.md §9).

Two contracts here: the walk uses the cache, and the walk reports itself.
"""

from __future__ import annotations

import logging
import os

import pytest

from scifor import PathInput

#: Relative on purpose: an absolute template is anchored at the filesystem root
#: and walks the whole path as segments (see `_root_and_segments`), which on
#: macOS crosses the /private/var symlink chain. `root_folder` is how a real
#: project template is written, and it makes the walk root assertable.
TEMPLATE = "{subject}/{session}/data.txt"


@pytest.fixture(autouse=True)
def _clear_listing_cache():
    """The listing cache is process-wide since 2026-09-22; tests must not
    inherit one another's."""
    from scifor import clear_listing_cache

    clear_listing_cache()
    yield
    clear_listing_cache()


@pytest.fixture
def tree(tmp_path):
    for subject in ("s1", "s2"):
        for session in ("A", "B"):
            d = tmp_path / subject / session
            d.mkdir(parents=True)
            (d / "data.txt").write_text("x")
    return tmp_path


def _listdir_calls(monkeypatch) -> list[str]:
    calls: list[str] = []
    original = os.listdir

    def spy(path):
        calls.append(str(path))
        return original(path)

    monkeypatch.setattr(os, "listdir", spy)
    return calls


class TestWalkUsesTheListingCache:
    def test_second_discover_reads_no_directory_twice(self, tree, monkeypatch):
        pi = PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE))
        calls = _listdir_calls(monkeypatch)

        first = pi.discover()
        reads_first = len(calls)
        assert reads_first > 0, "first walk should read the tree"

        second = pi.discover()
        assert second == first
        assert len(calls) == reads_first, (
            f"second discover re-read {len(calls) - reads_first} director(ies) "
            f"that had not changed"
        )

    def test_a_changed_directory_is_re_read(self, tree, monkeypatch):
        """mtime validation still works through the walk: a new entry shows up."""
        pi = PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE))
        assert len(pi.discover()) == 4

        d = tree / "s3" / "A"
        d.mkdir(parents=True)
        (d / "data.txt").write_text("x")
        # Force a visible mtime bump on the root even on coarse filesystems.
        os.utime(tree, None)

        found = {(c["subject"], c["session"]) for c in pi.discover()}
        assert ("s3", "A") in found

    def test_a_fresh_instance_reuses_the_cache(self, tree, monkeypatch):
        """The cache is per PROCESS, not per instance (2026-09-22).

        It used to live on the object, and the canvas rebuilds PathInput
        objects from their stored specs on every refresh — so it was born
        empty every time and never once served a listing: ``0 served from the
        listing cache`` on all 79 walks of one session, ~344 network directory
        reads per refresh, ~3 s of a 5.7-21 s ``get_pipeline``. The reuse is
        between objects, so the cache has to be too.
        """
        first = PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE)).discover()
        calls = _listdir_calls(monkeypatch)

        second = PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE)).discover()

        assert second == first
        assert calls == [], (
            f"a second PathInput over an unchanged tree re-read {len(calls)} "
            f"director(ies)"
        )

    def test_a_changed_directory_is_re_read_across_instances(self, tree):
        """Sharing is only safe because every entry is mtime-validated."""
        assert len(PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE)).discover()) == 4

        d = tree / "s3" / "A"
        d.mkdir(parents=True)
        (d / "data.txt").write_text("x")
        os.utime(tree, None)

        found = {
            (c["subject"], c["session"])
            for c in PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE)).discover()
        }
        assert ("s3", "A") in found, "a shared cache served a stale listing"


class TestDiscoverReportsItself:
    def test_timing_line_names_the_root_and_the_walk(self, tree, caplog):
        """The root named is the WALK root, which is not the template's parent.

        `_root_and_segments` anchors an absolute template at the filesystem
        root ("/" on POSIX, the drive or UNC share elsewhere) and treats the
        whole path as segments — so a template built from tmp_path logs "/",
        not tmp_path. Using `root_folder` makes the walk root the thing we can
        actually assert on, and is the shape a real project template has anyway.
        (Found on macOS, where tmp_path is under /private/var and the mismatch
        was not hidden by coincidence the way it was on Linux.)
        """
        pi = PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE))
        with caplog.at_level(logging.INFO, logger="scifor"):
            pi.discover()
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[timing] pathinput_discover" in text
        assert str(tree) in text
        assert "walk=" in text

    def test_absolute_template_names_the_filesystem_root(self, tree, caplog):
        """The contrast: an absolute template really is anchored at "/"."""
        pi = PathInput(str(tree / "{subject}" / "{session}" / "data.txt"), name=str(str(tree / "{subject}" / "{session}" / "data.txt")))
        with caplog.at_level(logging.INFO, logger="scifor"):
            pi.discover()
        timing = next(
            r.getMessage() for r in caplog.records if "[timing] pathinput_discover" in r.getMessage()
        )
        root, _ = pi._root_and_segments()
        assert str(root) in timing

    def test_read_and_cache_counts_track_the_two_walks(self, tree, caplog):
        """Cold walk: reads > 0, hits == 0. Warm walk: reads == 0, hits > 0.

        This pair is what a slow-share diagnosis reads: a re-open that still
        shows `read from disk` is a walk the cache did not absorb.
        """
        pi = PathInput(TEMPLATE, root_folder=str(tree), name=str(TEMPLATE))

        def counts(text: str) -> tuple[int, int]:
            import re

            m = re.search(
                r"(\d+) director\(ies\) read from disk, (\d+) served from the listing cache",
                text,
            )
            assert m, text
            return int(m.group(1)), int(m.group(2))

        with caplog.at_level(logging.INFO, logger="scifor"):
            pi.discover()
        cold_reads, cold_hits = counts("\n".join(r.getMessage() for r in caplog.records))
        assert cold_reads > 0
        assert cold_hits == 0
        caplog.clear()

        with caplog.at_level(logging.INFO, logger="scifor"):
            pi.discover()
        warm_reads, warm_hits = counts("\n".join(r.getMessage() for r in caplog.records))
        assert warm_reads == 0
        assert warm_hits == cold_reads
