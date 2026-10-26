"""GUI consumer of scifor's failure records (``scifor.failures``).

One owner for the GUI's side of "what failed in this run":

- :func:`on_error_from_config` — the ONE reader of a node's ``on_error`` run
  option (single-node Run, compiled pipeline, code export all call it). It is
  deliberately NOT part of ``scidb.RunOptions``: that value is
  identity-bearing, and stopping early never changes what a run computes.
- :class:`RunFailures` — collects the failed groups of one run (no-data
  groups are dropped HERE, nowhere else), tags each with the canvas node it
  belongs to, and renders the console text. ``to_wire(node_ids)`` is what
  ``run_done.failures`` carries to the extension (badge, failures panel,
  Problems panel, notification).

See docs/claude/error-surfacing.md.
"""

from __future__ import annotations

import logging

from scifor import stopped_run_of, validate_on_error
from scifor.failures import failure_from_exception, format_user_frame

logger = logging.getLogger(__name__)

ON_ERROR_KEY = "on_error"
_CONSOLE_COMBOS_MAX = 3


def on_error_from_config(run_options: "dict | None") -> str:
    """``"continue"`` | ``"stop"`` from a run-options blob; an unknown value
    is logged and read as the default rather than failing the run."""
    value = (run_options or {}).get(ON_ERROR_KEY) or "continue"
    try:
        return validate_on_error(value)
    except ValueError:
        logger.warning(
            "[run_failures] ignoring unknown on_error=%r in run options; using 'continue'",
            value,
        )
        return "continue"


def _console_block(target: str, groups: "list[dict]", completed: "int | None") -> str:
    total = sum(g.get("count", 0) for g in groups)
    done = f" ({completed} completed)" if completed is not None else ""
    lines = [f"⚠ {target}: {total} combo(s) failed{done}"]
    for g in groups:
        lines.append(f"  ✗ {g['reason']} — {g.get('count', 1)} combo(s)")
        lines.append(f"    {format_user_frame_dict(g.get('user_frame'))}")
        combos = g.get("combos") or []
        if combos:
            shown = "; ".join(combos[:_CONSOLE_COMBOS_MAX])
            more = (
                f" (+{g['count'] - len(combos[:_CONSOLE_COMBOS_MAX])} more)"
                if g.get("count", 0) > _CONSOLE_COMBOS_MAX
                else ""
            )
            lines.append(f"    e.g. {shown}{more}")
    return "\n".join(lines) + "\n"


def format_user_frame_dict(frame: "dict | None") -> str:
    """:func:`scifor.failures.format_user_frame` for a wire-shape frame."""
    if not frame:
        return format_user_frame(None)
    return f"at {frame['file']}:{frame['line']} in {frame['function']}"


class RunFailures:
    """The failed groups of one GUI run, each tagged with its node."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        self._groups: list[dict] = []
        self.stopped = False

    def add_summary(
        self,
        groups: "list[dict]",
        *,
        node_id: "str | None",
        fn_name: str,
        target: str,
        completed: "int | None",
    ) -> str:
        """Record a target/step's summary groups; returns console text ("" if
        nothing failed). *completed* is None when the caller cannot know it
        (a MATLAB run, read from its .done marker)."""
        failed = [g for g in groups or [] if not g.get("no_data")]
        for g in failed:
            self._groups.append({**g, "node_id": node_id, "fn_name": fn_name, "target": target})
        if not failed:
            return ""
        logger.info(
            "[run_failures] %s: %d failure group(s) (%d combo(s)) on node %s (run_id=%s): %s",
            target,
            len(failed),
            sum(g.get("count", 0) for g in failed),
            node_id,
            self.run_id,
            "; ".join(
                f"{g['reason'][:120]} {format_user_frame_dict(g.get('user_frame'))}"
                for g in failed
            ),
        )
        return _console_block(target, failed, completed)

    def add_exception(
        self,
        exc: BaseException,
        *,
        node_id: "str | None",
        fn_name: str,
        target: str,
    ) -> str:
        """An exception that escaped ``for_each``. An ``on_error="stop"``
        exception is already in its summary's groups, so it only marks the
        run stopped; anything else (a save failure, a setup error) becomes a
        group of its own with stage ``"run"``. Returns console text."""
        stopped = stopped_run_of(exc)
        if stopped is not None:
            self.stopped = True
            return (
                f"■ Stopped at the first failure (on_error=stop): "
                f"{stopped.failure.reason[:300]}\n"
                f"    {format_user_frame(stopped.failure.user_frame)}\n"
                f"    {stopped.completed} combo(s) completed before it were saved.\n"
            )
        failure = failure_from_exception(exc, fn_name=fn_name, metadata={}, stage="run")
        group = {
            "reason": failure.reason,
            "fn_name": fn_name,
            "exc_type": failure.exc_type,
            "message": failure.message,
            "no_data": False,
            "count": 1,
            "combos": [],
            "user_frame": failure.user_frame.to_dict() if failure.user_frame else None,
            "example": failure.to_dict(),
        }
        self._groups.append({**group, "node_id": node_id, "target": target})
        logger.info(
            "[run_failures] %s: run-level %s on node %s %s (run_id=%s)",
            target,
            failure.exc_type,
            node_id,
            format_user_frame(failure.user_frame),
            self.run_id,
        )
        return f"✗ {target}: {failure.reason[:500]}\n    {format_user_frame(failure.user_frame)}\n"

    @property
    def failed_combos(self) -> int:
        return sum(g.get("count", 0) for g in self._groups)

    def to_wire(self, node_ids: "list[str]") -> dict:
        """``run_done.failures``: ``by_node`` maps EVERY node this run covered
        to its failed groups — an empty list means it ran clean — so a
        consumer just assigns ``state[node] = by_node[node]`` and never
        re-derives which entries a rerun replaces. ``other``: groups with no
        node (a run started without one)."""
        by_node: dict[str, list[dict]] = {nid: [] for nid in node_ids}
        other: list[dict] = []
        for g in self._groups:
            nid = g.get("node_id")
            if nid:
                by_node.setdefault(nid, []).append(g)
            else:
                other.append(g)
        return {"by_node": by_node, "other": other}
