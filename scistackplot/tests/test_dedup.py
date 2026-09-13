"""Concurrent callers for one key get ONE build.

Measured 2026-09-13: two requests both logged `table cache MISS` and both built
the identical 5.2 GB table side by side — 188.8 s and 104.1 s — because the memo
was written after the build returned and the second arrival saw it empty
(.claude/plot-at-scale-plan.md §7.1). These tests pin the primitive that stops
that, and then that both cache layers actually use it.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import pytest

from scistackplot.dedup import SingleFlight
from scistackplot.sources.frame import DataFrameSource

LAYER = "scistackplot"


class TestSingleFlight:
    def test_sequential_calls_each_build(self):
        sf = SingleFlight()
        builds: list[int] = []
        sf.run("k", lambda: builds.append(1) or "a")
        sf.run("k", lambda: builds.append(1) or "b")
        # Not a cache: once a build finishes the key is free again. Caching is
        # the caller's memo; this only collapses OVERLAPPING builds.
        assert builds == [1, 1]

    def test_concurrent_callers_share_one_build(self):
        sf = SingleFlight()
        builds: list[int] = []
        started = threading.Event()
        release = threading.Event()

        def slow_build():
            builds.append(1)
            started.set()
            release.wait(timeout=5)
            return object()

        with ThreadPoolExecutor(max_workers=4) as pool:
            first = pool.submit(sf.run, "k", slow_build)
            assert started.wait(timeout=5)
            # Three more arrive while the first is mid-build.
            others = [pool.submit(sf.run, "k", slow_build) for _ in range(3)]
            # None of them have built anything: they are waiting.
            time.sleep(0.05)
            assert builds == [1]
            release.set()
            results = [first.result(timeout=5)] + [o.result(timeout=5) for o in others]

        assert builds == [1], "a concurrent caller built instead of waiting"
        assert all(r is results[0] for r in results), "waiters got a different object"

    def test_different_keys_build_in_parallel(self):
        sf = SingleFlight()
        both_started = threading.Barrier(2, timeout=5)

        def build():
            # Deadlocks (barrier timeout) if the two keys serialize.
            both_started.wait()
            return True

        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(sf.run, "a", build)
            b = pool.submit(sf.run, "b", build)
            assert a.result(timeout=5) and b.result(timeout=5)

    def test_on_wait_fires_for_waiters_only(self):
        sf = SingleFlight()
        waited: list[int] = []
        started = threading.Event()
        release = threading.Event()

        def build():
            started.set()
            release.wait(timeout=5)
            return 1

        with ThreadPoolExecutor(max_workers=2) as pool:
            owner = pool.submit(sf.run, "k", build, on_wait=lambda: waited.append(1))
            assert started.wait(timeout=5)
            waiter = pool.submit(sf.run, "k", build, on_wait=lambda: waited.append(1))
            time.sleep(0.05)
            release.set()
            owner.result(timeout=5)
            waiter.result(timeout=5)

        assert waited == [1], "on_wait should fire once, for the one waiter"

    def test_failure_reaches_owner_and_waiters(self):
        """A waiter must not get a wrong answer or hang when the build dies."""
        sf = SingleFlight()
        started = threading.Event()
        release = threading.Event()

        def bad_build():
            started.set()
            release.wait(timeout=5)
            raise RuntimeError("boom")

        with ThreadPoolExecutor(max_workers=2) as pool:
            owner = pool.submit(sf.run, "k", bad_build)
            assert started.wait(timeout=5)
            waiter = pool.submit(sf.run, "k", bad_build)
            time.sleep(0.05)
            release.set()
            with pytest.raises(RuntimeError, match="boom"):
                owner.result(timeout=5)
            with pytest.raises(RuntimeError, match="boom"):
                waiter.result(timeout=5)

    def test_key_is_released_after_failure(self):
        sf = SingleFlight()
        with pytest.raises(RuntimeError):
            sf.run("k", lambda: (_ for _ in ()).throw(RuntimeError("x")))
        assert sf.in_flight() == 0
        # A retry gets a clean slot and succeeds.
        assert sf.run("k", lambda: "ok") == "ok"

    def test_key_is_released_after_success(self):
        sf = SingleFlight()
        sf.run("k", lambda: 1)
        assert sf.in_flight() == 0


@pytest.fixture
def slow_source():
    """A source whose build blocks until told to proceed, so a test can
    arrange for a second caller to arrive mid-build deterministically."""
    frame = pd.DataFrame(
        {"subject": ["01", "02"], "Signal": [np.zeros(4), np.zeros(4)]}
    )
    source = DataFrameSource(frame, factors=["subject"], measures=["Signal"])
    started = threading.Event()
    release = threading.Event()
    original = source._build_table
    builds: list[int] = []

    def gated(*args, **kwargs):
        builds.append(1)
        started.set()
        release.wait(timeout=5)
        return original(*args, **kwargs)

    source._build_table = gated  # type: ignore[method-assign]
    return source, started, release, builds


class TestGetTableIsDeduplicated:
    def test_concurrent_get_table_builds_once(self, slow_source, caplog):
        """The actual regression: two `plot_*` requests, one table, one build."""
        source, started, release, builds = slow_source

        with caplog.at_level(logging.INFO, logger=LAYER):
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(source.get_table, ["Signal"])
                assert started.wait(timeout=5)
                second = pool.submit(source.get_table, ["Signal"])
                time.sleep(0.05)
                release.set()
                a = first.result(timeout=5)
                b = second.result(timeout=5)

        assert builds == [1], f"table built {len(builds)} times for one key"
        assert a is b
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert text.count("table cache MISS") == 1
        assert "build already in flight — waiting for it" in text

    def test_caller_after_the_build_hits_the_memo(self, slow_source, caplog):
        """The memo is published before the slot is released, so a caller
        arriving just after completion is a HIT, not a fresh MISS."""
        source, _started, release, builds = slow_source
        release.set()
        source.get_table(["Signal"])
        with caplog.at_level(logging.INFO, logger=LAYER):
            source.get_table(["Signal"])
        assert builds == [1]
        assert "table cache HIT" in "\n".join(r.getMessage() for r in caplog.records)
