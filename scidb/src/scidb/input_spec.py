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

Deliberately a LEAF: it imports nothing of scidb at module level (the
wrapper types are imported inside the functions, like ``scifor`` is), so
every module — the wrapper modules themselves included, for their display
names — may import it at the top.
"""

from __future__ import annotations

from typing import Any

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

        from .across_variants import AcrossVariants
        from .variant import Variant

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
    for the same reason, as ``is_loadable`` below.

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


def is_loadable(spec: Any) -> bool:
    """Is *spec* something ``for_each`` LOADS — a variable type, a wrapper
    around one, a ``Merge``, a bare DataFrame, or anything with ``.load()``?

    ``PathInput`` is deliberately excluded before the ``hasattr(..., "load")``
    fallback: it has a real ``.load()`` method (for standalone/scifor use),
    but under scidb its per-combo resolution is owned by scifor's for_each
    loop, not scidb's variable-loading machinery. Treating it as loadable
    here would (re)route it through ``PerComboLoader`` and would also flip
    ``ForEachConfig``'s classification of it for version-key hashing.

    Lived in ``foreach`` as ``_is_loadable`` until 2026-09-20; five modules
    imported it inside a function to dodge the cycle.
    """
    from scifor import ColumnSelection, Fixed, Merge
    from scifor.pathinput import PathInput

    from .across_variants import AcrossVariants
    from .variant import Variant

    if isinstance(spec, PathInput):
        return False
    try:
        import pandas as pd

        if isinstance(spec, pd.DataFrame):
            return True
    except ImportError:
        pass
    return isinstance(
        spec, (type, Fixed, Variant, AcrossVariants, ColumnSelection, Merge)
    ) or hasattr(spec, "load")


def spec_name(spec: Any) -> str:
    """A human-readable spelling of *spec* for logs and error messages —
    ``Fixed(Wide, subject=01)``, ``Variant(Wide, bandpass.low_hz=20)``,
    ``AcrossVariants(Wide)``, ``ColumnSelection(Wide, ['a'])``.

    Display only. The IDENTITY spelling of a spec is its ``to_key()`` and
    ``CallSite``; this one is free to be readable.
    """
    from scifor import ColumnSelection, Fixed, Merge

    from .across_variants import AcrossVariants
    from .variant import Variant

    if isinstance(spec, Merge):
        return spec.__name__
    if isinstance(spec, Fixed):
        fixed_str = ", ".join(f"{k}={v}" for k, v in spec.fixed_metadata.items())
        return f"Fixed({spec_name(spec.data)}, {fixed_str})"
    if isinstance(spec, Variant):
        bp_str = ", ".join(f"{k}={v}" for k, v in sorted(spec.branch_params.items()))
        return f"Variant({spec_name(spec.var_type)}, {bp_str})"
    if isinstance(spec, AcrossVariants):
        return f"AcrossVariants({spec_name(spec.var_type)})"
    if isinstance(spec, ColumnSelection):
        return f"ColumnSelection({spec_name(spec.data)}, {spec.columns})"
    if isinstance(spec, type):
        return spec.__name__
    if hasattr(spec, "__name__"):
        return spec.__name__
    return type(spec).__name__


def find_pathinput(inputs: dict):
    """The first ``PathInput`` among *inputs*, unwrapping ``Fixed``, or
    ``None``."""
    from scifor import Fixed
    from scifor.pathinput import PathInput

    for v in inputs.values():
        if isinstance(v, PathInput):
            return v
        if isinstance(v, Fixed) and isinstance(v.data, PathInput):
            return v.data
    return None
