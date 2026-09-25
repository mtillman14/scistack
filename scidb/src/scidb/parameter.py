"""
Parameter — a named pipeline configuration value, with one or more values.

Replaces the former ``scidb.constant()`` (one value) and ``scidb.Sweep()``
(many). They were two constructs for one idea, which forced an entity to
change *kind* the moment a second value was added — see
docs/claude/entity-editability-model.md (D6). One class, one node type in
the GUI, one thing to write::

    from scidb import Parameter

    SAMPLING_RATE_HZ = Parameter(1000, description="Recording rate")
    WINDOW_SECONDS   = Parameter(10, 20, 30)

    # Values can be built programmatically -- it is plain varargs:
    THRESHOLDS = Parameter(*range(10, 60, 10))
    SCALES     = Parameter(*[2**k for k in range(5)])

``Parameter`` IS an :class:`~scifor.each_of.EachOf`, so ``for_each`` fans it
out with no special handling: each value becomes an independent call with
that concrete value. **A one-value Parameter is not a special case** --
``EachOf`` expansion has no branch for it, so ``Parameter(30)`` produces
byte-identical ``version_keys``/``call_id`` to passing a bare ``30``. That
property is what lets adding a second value be *only* adding an argument,
with no change of form, id, node or history.

**A Parameter may hold no values at all**::

    WINDOW_SECONDS = Parameter()   # declared, not yet valued

That is the state the GUI's "New parameter" form produces -- it collects a
name and nothing else -- and it used to be papered over by scaffolding a
placeholder ``0`` into source, which is indistinguishable from a real value
once written and gets stamped into records by any run started before the
user notices. Declared-but-unvalued is a legitimate state and is now
representable as one. It is legal at rest -- in source, in the entities
file, in the registry, on the canvas -- and an error at execution:
``scifor.require_alternatives`` refuses an empty axis at ``for_each``
expansion, because a zero-length axis would otherwise iterate zero times and
write nothing while appearing to succeed.

Single-valued Parameters keep the transparent-proxy behaviour the old
``Constant`` had, so they read naturally at a call site::

    duration = 5 * SAMPLING_RATE_HZ     # 5000

Multi-valued ones deliberately do not: arithmetic on "10, 20 or 30" has no
meaning, so it raises rather than guessing. Use :attr:`values` to get the
list.
"""

from __future__ import annotations

import inspect
from typing import Any

from scifor.each_of import EachOf


class Parameter(EachOf):
    """A named configuration value with one or more alternatives.

    Prefer letting the discovery scanner find these as top-level bindings
    (``WINDOW = Parameter(10, 20)``) rather than constructing one inline at
    a call site -- an unnamed one has no identity for the GUI to show, the
    same rule that applied to ``Sweep`` before it.

    Zero values is a legal, nameable state -- see the module docstring.
    """

    def __init__(self, *values: Any, description: str = "") -> None:
        super().__init__(*values)
        self.description = description
        # The DECLARED name -- the binding in the entities file or module that
        # made this a Parameter. Set by whoever declares it (the entities
        # loader, the discovery scanner), never by the caller of for_each:
        # a Parameter's identity is its declaration, and the function
        # argument it fills can be named differently. for_each records it on
        # the constant's provenance edge so history can name the Parameter
        # the canvas shows -- see declared_input_names below. Not part of
        # any hash: identity is still the value alone.
        self.name: "str | None" = None
        # Where it was declared, for the GUI sidebar. Best-effort: a caller
        # constructed from C or exec'd code has no frame to inspect.
        frame = inspect.currentframe()
        caller = frame.f_back if frame is not None else None
        self.source_file = caller.f_code.co_filename if caller is not None else ""
        self.source_line = caller.f_lineno if caller is not None else 0

    # ------------------------------------------------------------------
    # Values
    # ------------------------------------------------------------------
    @property
    def values(self) -> list:
        """Every alternative, in declaration order."""
        return list(self.alternatives)

    @property
    def value(self) -> Any:
        """The single wrapped value.

        Raises for a multi-valued Parameter rather than silently returning
        the first -- picking one arbitrarily is how a fan-out quietly
        becomes a single run.
        """
        return self._single("value")

    def _single(self, op: str) -> Any:
        if not self.alternatives:
            # Distinct from the "too many" case below: nothing was declared
            # yet, so pointing at .values would be pointing at an empty list.
            raise TypeError(
                f"{op} needs a value, and this Parameter has none yet -- give "
                f"it at least one value."
            )
        if len(self.alternatives) != 1:
            raise TypeError(
                f"{op} is only defined for a single-valued Parameter; this one "
                f"has {len(self.alternatives)} values {self.alternatives!r}. "
                f"Use .values for the full list."
            )
        return self.alternatives[0]

    def __repr__(self) -> str:
        # Built as an argument list, not an f-string with a fixed comma: a
        # value-less Parameter would otherwise repr as "Parameter(,
        # description='')", which is not even valid syntax to paste back.
        args = [repr(a) for a in self.alternatives]
        args.append(f"description={self.description!r}")
        return f"Parameter({', '.join(args)})"

    # ------------------------------------------------------------------
    # Transparent proxy — single-valued only (see module docstring)
    # ------------------------------------------------------------------
    def __getattr__(self, name: str) -> Any:
        # Only reached when normal lookup fails, so real attributes
        # (alternatives, description, source_file, ...) take precedence.
        # Guard against recursion during __init__ before alternatives exists.
        if name in ("alternatives", "description", "source_file", "source_line", "name"):
            raise AttributeError(name)
        # MUST raise AttributeError, never TypeError, for a multi-valued
        # Parameter: hasattr() only swallows AttributeError, so anything
        # else escapes as a crash. scidb probes exactly this way --
        # input_spec.is_loadable ends in hasattr(var_spec, "load") -- so a
        # TypeError here takes down every for_each carrying a multi-valued
        # Parameter.
        if not self.alternatives:
            raise AttributeError(
                f"{name!r} is not available on a Parameter with no value yet"
            )
        if len(self.alternatives) != 1:
            raise AttributeError(
                f"{name!r} is not available on a {len(self.alternatives)}-value "
                f"Parameter; use .values"
            )
        return getattr(self.alternatives[0], name)

    def __bool__(self) -> bool:
        return bool(self._single("bool()"))

    def __int__(self) -> int:
        return int(self._single("int()"))

    def __float__(self) -> float:
        return float(self._single("float()"))

    def __complex__(self) -> complex:
        return complex(self._single("complex()"))

    def __str__(self) -> str:
        if len(self.alternatives) == 1:
            return str(self.alternatives[0])
        return repr(self)

    def __format__(self, spec: str) -> str:
        return format(self._single("format()"), spec)

    def __bytes__(self) -> bytes:
        return bytes(self._single("bytes()"))

    def __index__(self) -> int:
        return self._single("index()").__index__()

    def __hash__(self) -> int:
        # A single-valued Parameter hashes AS its value, so it can be used
        # interchangeably with the raw value as a dict key or set member --
        # the same transparency __eq__ provides. Hashing the 1-tuple instead
        # would make ``{Parameter(42): x}[42]`` a KeyError while
        # ``Parameter(42) == 42`` stayed True: equal objects with unequal
        # hashes, which silently breaks every hash-based lookup.
        if len(self.alternatives) == 1:
            return hash(self.alternatives[0])
        return hash(tuple(self.alternatives))

    # --- Comparison -----------------------------------------------------
    @staticmethod
    def _unwrap(other: Any) -> Any:
        if isinstance(other, Parameter) and len(other.alternatives) == 1:
            return other.alternatives[0]
        return other

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Parameter):
            return self.alternatives == other.alternatives
        if len(self.alternatives) == 1:
            return self.alternatives[0] == other
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        result = self.__eq__(other)
        return result if result is NotImplemented else not result

    def __lt__(self, other: Any) -> bool:
        return self._single("<") < self._unwrap(other)

    def __le__(self, other: Any) -> bool:
        return self._single("<=") <= self._unwrap(other)

    def __gt__(self, other: Any) -> bool:
        return self._single(">") > self._unwrap(other)

    def __ge__(self, other: Any) -> bool:
        return self._single(">=") >= self._unwrap(other)

    # --- Arithmetic -----------------------------------------------------
    def __add__(self, other: Any) -> Any:
        return self._single("+") + self._unwrap(other)

    def __sub__(self, other: Any) -> Any:
        return self._single("-") - self._unwrap(other)

    def __mul__(self, other: Any) -> Any:
        return self._single("*") * self._unwrap(other)

    def __truediv__(self, other: Any) -> Any:
        return self._single("/") / self._unwrap(other)

    def __floordiv__(self, other: Any) -> Any:
        return self._single("//") // self._unwrap(other)

    def __mod__(self, other: Any) -> Any:
        return self._single("%") % self._unwrap(other)

    def __pow__(self, other: Any) -> Any:
        return self._single("**") ** self._unwrap(other)

    def __matmul__(self, other: Any) -> Any:
        return self._single("@") @ self._unwrap(other)

    def __lshift__(self, other: Any) -> Any:
        return self._single("<<") << self._unwrap(other)

    def __rshift__(self, other: Any) -> Any:
        return self._single(">>") >> self._unwrap(other)

    def __and__(self, other: Any) -> Any:
        return self._single("&") & self._unwrap(other)

    def __xor__(self, other: Any) -> Any:
        return self._single("^") ^ self._unwrap(other)

    def __or__(self, other: Any) -> Any:
        return self._single("|") | self._unwrap(other)

    # --- Reflected arithmetic -------------------------------------------
    def __radd__(self, other: Any) -> Any:
        return self._unwrap(other) + self._single("+")

    def __rsub__(self, other: Any) -> Any:
        return self._unwrap(other) - self._single("-")

    def __rmul__(self, other: Any) -> Any:
        return self._unwrap(other) * self._single("*")

    def __rtruediv__(self, other: Any) -> Any:
        return self._unwrap(other) / self._single("/")

    def __rfloordiv__(self, other: Any) -> Any:
        return self._unwrap(other) // self._single("//")

    def __rmod__(self, other: Any) -> Any:
        return self._unwrap(other) % self._single("%")

    def __rpow__(self, other: Any) -> Any:
        return self._unwrap(other) ** self._single("**")

    def __rmatmul__(self, other: Any) -> Any:
        return self._unwrap(other) @ self._single("@")

    def __rlshift__(self, other: Any) -> Any:
        return self._unwrap(other) << self._single("<<")

    def __rrshift__(self, other: Any) -> Any:
        return self._unwrap(other) >> self._single(">>")

    def __rand__(self, other: Any) -> Any:
        return self._unwrap(other) & self._single("&")

    def __rxor__(self, other: Any) -> Any:
        return self._unwrap(other) ^ self._single("^")

    def __ror__(self, other: Any) -> Any:
        return self._unwrap(other) | self._single("|")

    # --- Unary ----------------------------------------------------------
    def __neg__(self) -> Any:
        return -self._single("unary -")

    def __pos__(self) -> Any:
        return +self._single("unary +")

    def __abs__(self) -> Any:
        return abs(self._single("abs()"))

    def __invert__(self) -> Any:
        return ~self._single("~")

    def __round__(self, ndigits: int | None = None) -> Any:
        single = self._single("round()")
        return round(single) if ndigits is None else round(single, ndigits)

    # --- Container ------------------------------------------------------
    #
    # These proxy to the single wrapped value (a Parameter wrapping a tuple
    # or string is indexable) rather than to the ALTERNATIVES list -- see
    # .values for that. Iterating a multi-valued Parameter is deliberately
    # an error: it would otherwise silently read as "iterate the values",
    # which is what .values is for, and the two meanings differ for a
    # single-valued Parameter wrapping a sequence.
    def __len__(self) -> int:
        return len(self._single("len()"))

    def __iter__(self):
        return iter(self._single("iteration"))

    def __contains__(self, item: Any) -> bool:
        return item in self._single("'in'")

    def __getitem__(self, key: Any) -> Any:
        return self._single("indexing")[key]

    def __reversed__(self):
        return reversed(self._single("reversed()"))


# ---------------------------------------------------------------------------
# Which declared entity (Parameter or PathInput) fed which argument
# ---------------------------------------------------------------------------
def path_input_declaration(obj: Any, binding: str) -> "tuple[str | None, str | None]":
    """``(name, problem)`` for a PathInput declaration bound as *binding*.

    *obj* is a ``PathInput`` or an ``EachOf`` of them (alternate templates).
    Its name is the ``name=`` every arm carries (required since 2026-09-25 —
    the name IS the identity). The rules, owned here for both scanners (scidb
    discovery and the GUI registry):

    * every arm of an ``EachOf`` must carry the SAME name — the alternatives
      are one PathInput in several places (``problem`` otherwise);
    * a binding whose name differs from ``name=`` is a re-export (``B = A``)
      or a mismatch; either way it is not a declaration of *binding*:
      ``name`` is returned and the caller registers it only if *name* is also
      bound under itself (``problem`` is ``None``; the caller decides).
    """
    from scifor.pathinput import PathInput

    arms = obj.alternatives if isinstance(obj, EachOf) else [obj]
    names = {a.name for a in arms if isinstance(a, PathInput)}
    if len(names) != 1:
        return None, (
            f"{binding}: the alternatives of one PathInput must share one name=, "
            f"got {sorted(names)}"
        )
    return names.pop(), None


def check_path_input_names(inputs: "dict[str, Any]") -> None:
    """Refuse one ``for_each`` call in which two arguments carry the SAME
    PathInput name but point at different files.

    A name is a PathInput's identity (``PathInput.to_key``), so two such
    arguments would be recorded as one input and their runs could not be
    told apart. The same PathInput fed to two arguments is fine, and so are
    the alternatives of one ``EachOf`` (one PathInput in several places, by
    design). Raises ``ValueError`` naming both arguments.
    """
    from scifor.pathinput import PathInput

    seen: dict[str, tuple[str, frozenset]] = {}
    for arg, value in inputs.items():
        arms = value.alternatives if isinstance(value, EachOf) else [value]
        pis = [a for a in arms if isinstance(a, PathInput)]
        if not pis:
            continue
        for name in {p.name for p in pis}:
            specs = frozenset(p.to_spec() for p in pis if p.name == name)
            prior = seen.get(name)
            if prior is not None and prior[1] != specs:
                raise ValueError(
                    f"PathInput name {name!r} is used by arguments {prior[0]!r} and "
                    f"{arg!r} with different templates/root folders "
                    f"({sorted(prior[1])} vs {sorted(specs)}). The name is the "
                    f"PathInput's identity — give each a distinct name=."
                )
            seen.setdefault(name, (arg, specs))


def path_input_specs_of(inputs: "dict[str, Any]") -> dict[str, str]:
    """``{argument: PathInput.to_spec()}`` for the bare PathInputs of one
    (already EachOf-expanded) call — what ``record_run`` stores on each
    PathInput record. Descriptive only; identity is ``to_key()``."""
    from scifor.pathinput import PathInput

    return {
        arg: value.to_spec() for arg, value in inputs.items() if isinstance(value, PathInput)
    }


def _declared_name_of(value: Any) -> "str | None":
    """The declared name a for_each input carries, if any: a named Parameter,
    a named PathInput, or an EachOf whose alternatives are one named
    PathInput declaration (all arms carry the same name)."""
    from scifor.pathinput import PathInput

    if isinstance(value, Parameter):
        return value.name
    if isinstance(value, PathInput):
        return getattr(value, "name", None)
    if isinstance(value, EachOf) and value.alternatives and all(
        isinstance(a, PathInput) for a in value.alternatives
    ):
        names = {getattr(a, "name", None) for a in value.alternatives}
        return names.pop() if len(names) == 1 else None
    return None


def declared_input_names(
    inputs: "dict[str, Any]", explicit: "dict[str, str] | None" = None
) -> dict[str, str]:
    """``{argument: declared entity name}`` for one ``for_each`` call.

    THE owner of this mapping, for both kinds of named input:

    * a **Parameter** — history records a constant under the function
      ARGUMENT it filled, but the canvas shows the Parameter under the name it
      was DECLARED with, and the two differ whenever a Parameter declared
      ``gaitrite_config`` feeds an argument ``gaitRiteConfig``. With no
      record of the declared name, the history reader invented a second
      Parameter node named after the argument on the first run
      (docs/claude/cleanup-audit.md B1);
    * a **PathInput** — the canvas groups PathInput-fed steps by WHICH
      declared PathInput feeds them. Unrecorded, only the GUI (from its
      registry) could say, so ``scidb graph`` grouped steps differently from
      the canvas (cleanup-audit F38).

    The name is stamped on the argument's provenance edge (the constant edge
    or the PathInput edge) as ``_invocation_input.declared_name``.

    Two sources, merged here and nowhere else:

    * *explicit* — the caller states it (the GUI from its wiring, a generated
      MATLAB command via ``parameter_names=``). Wins, because a value already
      recorded in history reaches ``for_each`` as a bare scalar, which
      carries no name;
    * a named input in *inputs* (``.name``, set by the entities loader and
      the discovery scanner — for a PathInput it is its required ``name=``)
      — how a plain script run gets it.

    An argument with neither is absent; readers fall back to the argument
    name (Parameters) or the template (PathInputs).
    """
    names: dict[str, str] = {}
    for arg, value in inputs.items():
        declared = _declared_name_of(value)
        if declared:
            names[arg] = str(declared)
    for arg, declared in (explicit or {}).items():
        if declared:
            names[str(arg)] = str(declared)
    return names


def parameter_node_name(argument: str, recorded: "dict[str, str] | None") -> str:
    """The Parameter a recorded constant belongs to: the declared name its run
    recorded (``_invocation_input.declared_name``), else the argument it
    filled. Readers of history ask this, never spell the fallback themselves.
    """
    return (recorded or {}).get(argument) or argument
