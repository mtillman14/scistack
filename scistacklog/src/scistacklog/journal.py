"""The run journal: ``scistack.log``, written for the scientist.

The debug log (``scistack-debug.log``) records everything the framework does.
The journal records only what happened to the analysis: one block per run
(what ran, how many combinations completed / failed / had no data, how long
it took), each failure grouped by reason and labelled with WHO can fix it,
and the occasional note about the data. Every block names a ``run=<id>``; the
same token stamps every debug-log line written during that run, so a bug
report is "search the debug log for run=<id>".

This module only RENDERS events (plain dicts); it imports nothing from the
rest of the stack. Who-can-fix-it (``blame``) is decided once, by
``scifor.failures`` (Python) and ``+scifor/failure_from_mexception.m``
(MATLAB), and arrives here as a field of each failure dict.

Event shapes (all keys optional except ``kind``)::

    {"kind": "run", "name", "run_id", "started" (epoch s), "elapsed" (s),
     "completed", "failed", "no_data", "total", "up_to_date", "saved",
     "cancelled", "stopped", "failures": [FailureGroup.to_dict()...],
     "error": ComboFailure.to_dict() | None, "language": "python"|"matlab"}
    {"kind": "action_failed", "action", "run_id", "started",
     "failure": ComboFailure.to_dict()}
    {"kind": "note", "text", "started"}

See docs/claude/logging-architecture.md.
"""

from __future__ import annotations

import json
import time

__all__ = ["render", "blame_headline", "BLAME_HEADLINES"]

RULE_WIDTH = 72
COMBOS_SHOWN = 5
MESSAGE_LINES_SHOWN = 12
_INDENT = "   "

#: The one wording of each blame category. ``{fn}`` is the function's name.
BLAME_HEADLINES = {
    "user_code": "Error in your function {fn}",
    "user_value": "Fixable in your function {fn}",
    "user_value_probable": "Probably fixable in your function {fn}",
    "setup": "Problem with your setup or data",
    "internal": "SciStack internal error, not caused by your code",
    "no_data": "No data",
    # An exception no layer classified (it escaped before one could).
    "unknown": "The run failed",
}


def render(event: dict) -> str:
    """The journal text for one event. Never raises: an event this cannot
    render comes back as its JSON, so the journal never loses an entry."""
    try:
        kind = event.get("kind")
        if kind == "run":
            return _render_run(event)
        if kind == "action_failed":
            return _render_action(event)
        if kind == "note":
            return _render_note(event)
        raise ValueError(f"unknown journal event kind {kind!r}")
    except Exception as e:  # noqa: BLE001 — a journal entry must not be lost
        try:
            body = json.dumps(event, default=str)
        except Exception:  # noqa: BLE001
            body = repr(event)
        return f"(journal entry could not be formatted: {type(e).__name__}: {e})\n{body}"


# ---------------------------------------------------------------------------
# Pieces
# ---------------------------------------------------------------------------


def _clock(started) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(started)))
    except (TypeError, ValueError):
        return time.strftime("%Y-%m-%d %H:%M:%S")


def _duration(seconds) -> str:
    try:
        s = float(seconds)
    except (TypeError, ValueError):
        return ""
    if s < 60:
        return f"{s:.1f}s"
    m, s = divmod(int(round(s)), 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m"


def _rule(title: str) -> str:
    head = f"── {title} "
    return head + "─" * max(3, RULE_WIDTH - len(head))


def _int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def blame_headline(blame: "str | None", probable: bool, fn_name: str) -> str:
    """The one wording of who can fix a failure. The journal uses it, and
    ``scifor.failures.FailureGroup.to_dict`` sends it to the GUI as
    ``blame_label``, so the GUI never keeps a copy of these strings."""
    blame = blame or "unknown"
    key = "user_value_probable" if blame == "user_value" and probable else blame
    template = BLAME_HEADLINES.get(key, BLAME_HEADLINES["unknown"])
    return template.format(fn=fn_name or "(unknown)")


def _headline(failure: dict, fn_name: str) -> str:
    return blame_headline(failure.get("blame"), bool(failure.get("probable")), fn_name)


def _message_lines(failure: dict) -> list[str]:
    exc_type = failure.get("exc_type") or "Error"
    message = str(failure.get("message") or "")
    lines = message.splitlines() or [""]
    first = f"{exc_type}: {lines[0]}" if lines[0] else exc_type
    rest = lines[1:]
    if len(rest) > MESSAGE_LINES_SHOWN:
        hidden = len(rest) - MESSAGE_LINES_SHOWN
        rest = rest[:MESSAGE_LINES_SHOWN] + [f"(… {hidden} more line(s) in scistack-debug.log)"]
    return [first] + rest


def _frame_lines(frame) -> list[str]:
    if not isinstance(frame, dict) or not frame.get("file"):
        return []
    out = [f"at {frame.get('file')}:{_int(frame.get('line'))} in {frame.get('function') or '?'}"]
    code = str(frame.get("code") or "").strip()
    if code:
        out.append(f"    {code}")
    return out


def _combo_line(combos, count: int) -> list[str]:
    if isinstance(combos, str):
        combos = [combos]
    combos = [str(c) for c in (combos or []) if str(c)]
    if not combos:
        return []
    shown = combos[:COMBOS_SHOWN]
    more = count - len(shown)
    tail = f" (+{more} more)" if more > 0 else ""
    return [f"combos: {'; '.join(shown)}{tail}"]


def _failure_block(failure: dict, fn_name: str, *, count: int | None, combos) -> list[str]:
    """The lines for one failure (a group's example, or a run-level error)."""
    head = _headline(failure, fn_name)
    if count is not None:
        head = f"{head} — {count} combo{'s' if count != 1 else ''}"
    body = _message_lines(failure)
    # A user-code failure points at the user's line; the others point at
    # nothing the scientist can edit, so the frame would only mislead.
    if (failure.get("blame") or "unknown") == "user_code":
        body += _frame_lines(failure.get("user_frame"))
    body += _combo_line(combos, count or 0)
    return [f"✗ {head}"] + [f"  {line}" for line in body]


def _advice(failures: list[dict], run_id: str) -> list[str]:
    """One line per kind of follow-up the failures call for, in a fixed order."""
    blames = {f.get("blame") for f in failures}
    probable = any(f.get("blame") == "user_value" and f.get("probable") for f in failures)
    out = []
    if "internal" in blames:
        out.append(
            f'Please report this: send scistack-debug.log (search "run={run_id}").'
        )
    if probable:
        out.append(
            "If the function's outputs look consistent, this may be a SciStack "
            f'bug: report it with scistack-debug.log (search "run={run_id}").'
        )
    return out


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


def _render_run(ev: dict) -> str:
    name = str(ev.get("name") or "for_each")
    run_id = str(ev.get("run_id") or "-")
    error = ev.get("error") if isinstance(ev.get("error"), dict) else None
    raw = ev.get("failures") or []
    if isinstance(raw, dict):  # MATLAB jsonencode: a 1-element array is an object
        raw = [raw]
    groups = [g for g in raw if isinstance(g, dict)]
    real = [g for g in groups if not g.get("no_data")]
    no_data = [g for g in groups if g.get("no_data")]

    completed = _int(ev.get("completed"))
    failed = _int(ev.get("failed"))
    n_no_data = _int(ev.get("no_data"))
    up_to_date = _int(ev.get("up_to_date"))

    if error is not None:
        status = "did not finish"
    elif ev.get("cancelled"):
        status = "cancelled"
    elif ev.get("stopped"):
        status = "stopped at the first error"
    elif failed:
        status = f"finished with {failed} failure{'s' if failed != 1 else ''}"
    else:
        status = "finished"
    lines = [_rule(f"{_clock(ev.get('started'))}  {name}  (run={run_id})")]

    counts = [f"✓ {completed} completed", f"✗ {failed} failed", f"– {n_no_data} no data"]
    if up_to_date:
        counts.append(f"· {up_to_date} already up to date")
    duration = _duration(ev.get("elapsed"))
    tail = f"   {duration}" if duration else ""
    lines.append(f"{_INDENT}{status}: " + "   ".join(counts) + tail)
    if ev.get("saved") is not None:
        lines.append(f"{_INDENT}saved {_int(ev.get('saved'))} record(s)")

    for g in real:
        example = g.get("example") if isinstance(g.get("example"), dict) else {}
        failure = {**example, **{k: g[k] for k in ("blame", "probable") if k in g}}
        lines.append("")
        lines += [
            _INDENT + line
            for line in _failure_block(
                failure,
                str(g.get("fn_name") or example.get("fn_name") or name),
                count=_int(g.get("count")) or None,
                combos=g.get("combos"),
            )
        ]
    if error is not None:
        lines.append("")
        lines += [
            _INDENT + line
            for line in _failure_block(
                error, str(error.get("fn_name") or name), count=None, combos=None
            )
        ]
        lines.append(f"{_INDENT}  The run stopped here; results computed before it were kept.")
    for g in no_data:
        combos = _combo_line(g.get("combos"), _int(g.get("count")))
        where = f" — {combos[0][len('combos: '):]}" if combos else ""
        lines.append(f"{_INDENT}– no data for {_int(g.get('count'))} combo(s){where}")

    advice = _advice(real + ([error] if error else []), run_id)
    if real and not error and not ev.get("stopped"):
        advice.insert(0, "Failed combos saved nothing; completed combos were kept.")
    if advice:
        lines.append("")
        lines += [_INDENT + a for a in advice]
    return "\n".join(lines)


def _render_action(ev: dict) -> str:
    action = str(ev.get("action") or "action")
    run_id = str(ev.get("run_id") or "-")
    failure = ev.get("failure") if isinstance(ev.get("failure"), dict) else {}
    lines = [_rule(f"{_clock(ev.get('started'))}  {action} failed  (run={run_id})")]
    blame = failure.get("blame") or "internal"
    if blame == "internal":
        # A GUI action's error is often a deliberate refusal whose message
        # says what to change; the frames cannot tell the two apart, so the
        # wording does not claim more than is known.
        lines.append(f"{_INDENT}✗ Couldn't complete {action}")
        lines += [f"{_INDENT}  {line}" for line in _message_lines(failure)]
        lines.append(
            f"{_INDENT}If the message doesn't say what to change, this is likely a "
            f'SciStack bug: report it with scistack-debug.log (search "run={run_id}").'
        )
    else:
        lines += [
            _INDENT + line
            for line in _failure_block(
                failure, str(failure.get("fn_name") or action), count=None, combos=None
            )
        ]
    return "\n".join(lines)


def _render_note(ev: dict) -> str:
    return f"· {_clock(ev.get('started'))}  note: {ev.get('text') or ''}"
