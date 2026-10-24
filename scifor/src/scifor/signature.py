"""
THE reader of a function's arguments.

Every layer that asks "which arguments does this function take, and which
have a default?" asks here: scifor's keyword injection
(``foreach._accepted_param_names``), scidb's step values
(``pipeline._default_input_names``), and the GUI's input ports and tunables
(``scistack_gui.registry.function_arguments``). Before 2026-10-24 each called
``inspect.signature`` itself, with its own fallback and its own idea of which
names count, so "has a default" could disagree between the tunables panel and
the canvas.

The callable is inspected exactly as given (``inspect.signature`` follows
``__wrapped__``). A callable with no introspectable signature falls back to
``__scidb_params__`` (set by scidb / scimatlab wrappers): those arguments are
reported with unknown defaults, as required.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any

from scistacklog import Log

#: Sentinel for "no default" (``inspect.Parameter.empty``).
EMPTY = inspect.Parameter.empty

#: Kinds that can be passed by name: the only ones an input can feed.
NAMED_KINDS = (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)


@dataclass(frozen=True)
class Argument:
    """One argument of a function's signature."""

    name: str
    kind: Any  # inspect.Parameter kind
    default: Any = EMPTY

    @property
    def private(self) -> bool:
        """`_`-prefixed: never a port or a tunable."""
        return self.name.startswith("_")

    @property
    def has_default(self) -> bool:
        return self.default is not EMPTY

    @property
    def named(self) -> bool:
        """Can be passed by name (not ``*args`` / ``**kwargs`` / positional-only)."""
        return self.kind in NAMED_KINDS

    @property
    def variadic(self) -> bool:
        return self.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)

    @property
    def optional(self) -> bool:
        """Leaving it unfed is fine: it has a default, or it's ``*args`` /
        ``**kwargs``."""
        return self.has_default or self.variadic

    def default_repr(self, limit: int = 40) -> str:
        """A short display of the default (``''`` when there is none)."""
        if self.kind == inspect.Parameter.VAR_POSITIONAL:
            return "*args"
        if self.kind == inspect.Parameter.VAR_KEYWORD:
            return "**kwargs"
        if not self.has_default:
            return ""
        try:
            text = repr(self.default)
        except Exception:  # noqa: BLE001 - a default whose repr raises
            text = f"<{type(self.default).__name__}>"
        return text if len(text) <= limit else text[: limit - 1] + "…"


def function_arguments(fn) -> "list[Argument] | None":
    """The arguments of *fn* in signature order, private ``_`` names
    included (see :attr:`Argument.private`). ``None`` when nothing is
    known: no signature and no ``__scidb_params__``."""
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        params = getattr(fn, "__scidb_params__", None)
        if params is None:
            Log.debug(f"[signature] {_name(fn)}: no signature, no __scidb_params__", layer="scifor")
            return None
        args = [Argument(str(p), inspect.Parameter.POSITIONAL_OR_KEYWORD) for p in params]
        Log.debug(f"[signature] {_name(fn)}: {len(args)} arg(s) from __scidb_params__", layer="scifor")
        return args
    args = [Argument(name, p.kind, p.default) for name, p in sig.parameters.items()]
    Log.debug(
        f"[signature] {_name(fn)}: {len(args)} arg(s), "
        f"{sum(a.has_default for a in args)} with a default",
        layer="scifor",
    )
    return args


def _name(fn) -> str:
    return getattr(fn, "__qualname__", None) or getattr(fn, "__name__", None) or repr(fn)
