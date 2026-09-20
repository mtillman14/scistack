"""What a ``for_each`` input spec is wrapped in, and what it is wrapped AROUND.

An input arrives as a bare variable class or as a stack of wrappers:
``AcrossVariants(Variant(Fixed(MyVar["a"], subject="01"), low_hz=20))``. Each
wrapper answers a different question — which records (``Variant``, ``Fixed``),
which columns (``ColumnSelection``), how the call is made
(``AcrossVariants``) — and the order is deliberately free, because they
compose orthogonally (``scidb/variant.py``).

Almost every identity path needs the same thing out of that stack: **the
variable type underneath**. The call-site view of an input is its type
(`foreach_config.CallSite`), the expected-invocation predictor enumerates
that type's records, the binding classifier names it, the loader needs the
class to call ``.load()`` on.

That unwrap used to be written six times — in ``foreach``,
``foreach_config`` (twice), ``provenance_query`` and ``provenance_save`` —
each handling a DIFFERENT subset of the wrappers, which is not a tidiness
problem: the copy in ``config_from_inputs`` did not know about ``Variant``,
so a ``Variant``-pinned input vanished from the predicted config entirely
(``scidb/tests/test_variant_pin_node_state.py``). One owner now.

Deliberately a near-leaf: it imports the wrapper types and nothing else of
scidb, so any module may import it at the top.
"""

from __future__ import annotations

from typing import Any

from .across_variants import AcrossVariants
from .variant import Variant

#: How to reach the inner spec of each wrapper, in one table. A wrapper added
#: without an entry here is simply not unwrapped — and every identity path
#: sees the wrapper instead of the type, which is exactly the failure this
#: module exists to make impossible to have in only some of them.
_INNER_ATTR: dict = {}


def _wrapper_table() -> dict:
    """``{wrapper type: attribute holding the inner spec}``.

    Built lazily and cached: ``scifor`` is a hard dependency but importing it
    at module import time would make this module non-leaf for ``scidb``'s own
    import order.
    """
    if not _INNER_ATTR:
        from scifor import ColumnSelection, Fixed

        _INNER_ATTR.update(
            {
                AcrossVariants: "var_type",
                Variant: "var_type",
                Fixed: "data",
                ColumnSelection: "data",
            }
        )
    return _INNER_ATTR


def peel(spec: Any) -> Any:
    """*spec* with every wrapper removed — the innermost thing, whatever it
    is (a variable class, a ``Merge``, a ``PathInput``, a DataFrame, a
    constant). Order-agnostic and depth-agnostic: it peels until nothing
    peels."""
    table = _wrapper_table()
    seen = 0
    while seen < 8:  # a stack deeper than this is a bug, not a use case
        for wrapper, attr in table.items():
            if isinstance(spec, wrapper):
                spec = getattr(spec, attr, spec)
                break
        else:
            return spec
        seen += 1
    return spec


def wrappers_of(spec: Any) -> list[type]:
    """The wrapper types around *spec*, outermost first — for messages and
    for a caller that needs to know a pin is present without re-checking
    ``isinstance`` in its own order."""
    table = _wrapper_table()
    out: list[type] = []
    seen = 0
    while seen < 8:
        for wrapper, attr in table.items():
            if isinstance(spec, wrapper):
                out.append(wrapper)
                spec = getattr(spec, attr, spec)
                break
        else:
            break
        seen += 1
    return out


def find_wrapper(spec: Any, wrapper: type) -> Any:
    """The *wrapper* instance anywhere in *spec*'s stack, or ``None``.

    Order-agnostic, like the wrappers themselves: ``Fixed(Var["a"], ...)``
    and ``Variant(Var["a"], ...)`` both carry a column selection, and a
    caller asking "is there a selection on this input?" must not have to
    enumerate the stackings. Enumerating two of them is how a selection
    under a ``Variant`` reached the graph as "no selection".
    """
    table = _wrapper_table()
    seen = 0
    while seen < 8:
        if isinstance(spec, wrapper):
            return spec
        for wrapped, attr in table.items():
            if isinstance(spec, wrapped):
                spec = getattr(spec, attr, spec)
                break
        else:
            return None
        seen += 1
    return None


def variable_type(spec: Any) -> Any:
    """The loadable variable type *spec* binds, or ``None``.

    "Loadable" is a class or anything exposing ``.load()``, with ``PathInput``
    excluded BEFORE that fallback — it has a real ``.load()`` (for
    standalone/scifor use) but under scidb its per-combo resolution belongs
    to scifor's for_each loop, not the variable loader, and it is identified
    by its TEMPLATE (``to_key()``), not by a type name. The same exclusion,
    for the same reason, as ``foreach._is_loadable``.

    Returns ``None`` for a constant, a ``PathInput``, a ``Merge``, a marker
    or a bare DataFrame, so a caller can use it as the "does this input bind
    records of ONE variable type?" question too.
    """
    from scifor.pathinput import PathInput

    inner = peel(spec)
    if isinstance(inner, PathInput):
        return None
    if isinstance(inner, type) or hasattr(inner, "load"):
        return inner
    return None


def type_name(spec: Any) -> str | None:
    """The NAME of the variable type *spec* binds, or ``None`` — the
    call-site view of an input (``foreach_config.CallSite.inputs``)."""
    inner = variable_type(spec)
    name = getattr(inner, "__name__", None)
    return name if isinstance(name, str) else None
