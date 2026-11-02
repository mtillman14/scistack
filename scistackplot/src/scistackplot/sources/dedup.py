"""Run a keyed build ONCE even when several threads ask for it at the same time.

Memoizing a build only helps the second caller if the first one has *finished*.
The panel does not work that way: opening it fires ``plot_describe``,
``plot_resolve``, ``plot_capabilities`` and ``plot_location_tree`` together, the
server runs a thread per request, and every one of them asks for the same table.

Measured on 2026-09-13: two requests both logged ``table cache MISS`` and both
built the identical 5.2 GB table, concurrently — 188.8 s and 104.1 s, finishing
within 100 ms of each other (.claude/plot-at-scale-plan.md §7.1). The memo was
written after ``_build_table`` returned, so the second arrival saw an empty cache
and repeated all of it. A classic cache stampede; the 2026-09-11 memoization work
fixed sequential repeat cost and never addressed concurrent arrival.

:class:`SingleFlight` closes that: the first caller for a key builds, everyone
else waits on the same result and pays nothing.
"""

from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import Any, Callable


class SingleFlight:
    """Deduplicate concurrent builds by key.

    One instance per cache. Keys must be hashable and must mean the same thing
    the cache's own key means — a narrower key here would merge builds that are
    not interchangeable.

    Thread-safe. The internal lock is held only while claiming or releasing a
    key, never while building, so two DIFFERENT keys still build in parallel.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inflight: dict[Any, Future] = {}

    def run(
        self,
        key: Any,
        build: Callable[[], Any],
        *,
        on_wait: Callable[[], None] | None = None,
    ) -> Any:
        """``build()``'s result, computing it once across concurrent callers.

        The first caller for ``key`` runs ``build``; any caller arriving while
        that is in flight blocks until it finishes and gets the same object.

        ``on_wait`` is called (once, by each waiter) instead of building, for
        logging — a waiter that silently returns the right answer is
        indistinguishable in a log from a cache hit, and those have very
        different meanings when reading a slow request.

        A failing ``build`` raises in the owner AND in every waiter: the waiters
        asked for a value that could not be produced, and swallowing it here
        would hand them a wrong answer or a hang. The key is released either
        way, so a later attempt is free to retry.
        """
        with self._lock:
            future = self._inflight.get(key)
            owner = future is None
            if owner:
                future = Future()
                self._inflight[key] = future

        if not owner:
            if on_wait is not None:
                on_wait()
            # Raises here if the owner's build failed — by design, see above.
            return future.result()

        try:
            result = build()
        except BaseException as exc:
            # Release BEFORE publishing the failure so a waiter that wakes and
            # immediately retries finds a clear slot rather than this dead one.
            with self._lock:
                self._inflight.pop(key, None)
            future.set_exception(exc)
            raise
        with self._lock:
            self._inflight.pop(key, None)
        future.set_result(result)
        return result

    def in_flight(self) -> int:
        """How many keys are building right now. For tests and diagnostics."""
        with self._lock:
            return len(self._inflight)
