"""The order the sidebar lists names in.

One owner for "alphabetical" in every sidebar list (functions, variables,
Parameters, PathInputs): case-insensitive, so ``EmgActivation`` sorts
between ``compute_vif`` and ``load_xsens`` rather than ahead of every
lowercase name the way Python's plain ``sorted`` puts it. Python and MATLAB
functions are ordered together in ONE list -- the sidebar used to show the
Python list followed by the MATLAB list, which reads as a broken sort.

The frontend never re-sorts what it is sent; it renders this order as is.
"""

from __future__ import annotations

from typing import Callable, Iterable, TypeVar

T = TypeVar("T")


def display_key(name: str) -> tuple[str, str]:
    """Case-insensitive, with the exact spelling as the tiebreak so two
    names differing only in case still land in a stable order."""
    return (name.casefold(), name)


def display_sorted(
    items: Iterable[T], name: Callable[[T], str] = str
) -> list[T]:
    """*items* in sidebar order, keyed by *name* (the item itself for a
    list of strings)."""
    return sorted(items, key=lambda item: display_key(name(item)))
