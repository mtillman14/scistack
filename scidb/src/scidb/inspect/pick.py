"""Interactive record selection: pure drill-down logic over PickCandidates.

Scope guard (plan decision / Phase 6): the picker *selects* a record, it
never displays data — that is the GUI's job. The flow narrows candidates
variable → schema keys → variant, asking only when a level actually
disambiguates (single-valued levels are skipped).

The chooser is injected (``choose(title, labels) -> index``), so this module
has no I/O and is unit-testable with a scripted chooser; the CLI supplies a
stderr-menu/stdin implementation. A future full TUI (textual/prompt_toolkit
behind a ``scidb[tui]`` extra) would replace only the chooser, not this flow.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence

from scidb.inspect.api import PickCandidate

Chooser = Callable[[str, Sequence[str]], int]
"""choose(title, labels) -> selected index. Raises PickAborted to cancel."""


class PickAborted(Exception):
    """The user cancelled interactive selection."""


def variant_label(candidate: PickCandidate) -> str:
    """How one candidate is presented on the variant-level menu."""
    if candidate.function_name is None:
        head = "(raw save)"
    else:
        head = candidate.function_name
    params = " ".join(f"{k}={v}" for k, v in sorted(candidate.branch_params.items()))
    parts = [head]
    if params:
        parts.append(params)
    if candidate.saved:
        parts.append(f"saved {candidate.saved}")
    return "  ".join(parts)


def drill_down(
    candidates: list[PickCandidate],
    schema_keys: list[str],
    choose: Chooser,
) -> PickCandidate:
    """Narrow ``candidates`` to exactly one via the injected chooser.

    Levels: each schema key in hierarchy order (only when the remaining
    candidates disagree on it), then the variant level (branch params /
    producing function). Raises ValueError on an empty candidate list.
    """
    if not candidates:
        raise ValueError("No candidates to pick from")
    remaining = list(candidates)

    for key in schema_keys:
        if len(remaining) == 1:
            break
        values = sorted({c.schema.get(key, "") for c in remaining})
        if len(values) <= 1:
            continue  # this level does not disambiguate — don't ask
        labels = []
        for v in values:
            n = sum(1 for c in remaining if c.schema.get(key, "") == v)
            shown = v if v != "" else "(unset)"
            labels.append(f"{key}={shown}   ({n} candidate{'s' if n != 1 else ''})")
        idx = choose(f"Select {key}:", labels)
        chosen = values[idx]
        remaining = [c for c in remaining if c.schema.get(key, "") == chosen]

    if len(remaining) > 1:
        # Deterministic menu order: candidates arrive newest-save-first from
        # _find_record, which varies run to run. Sort by label so the same
        # variants always appear in the same order (k=1 before k=2, …).
        remaining.sort(key=variant_label)
        idx = choose("Select variant:", [variant_label(c) for c in remaining])
        remaining = [remaining[idx]]

    return remaining[0]
