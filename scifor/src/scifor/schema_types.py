"""Schema-key types: the ONE conversion rule, and how a loop applies it.

A schema key may be declared ``"numeric"`` or ``"string"``
(``docs/claude/schema-key-types.md``):

* ``numeric`` — every spelling of one number is one value: ``"001"``, ``1`` and
  ``1.0`` are all ``1``; zero-padded filenames still resolve.
* ``string`` — spelling IS identity: ``"1"`` and ``"001"`` are different, and a
  PathInput matches the key exactly (no zero-padding bridge).
* undeclared — the caller's layer decides. Standalone scifor converts
  digit-only DISCOVERED values (``condense_numeric``); scidb keeps them as
  spelled.

**One owner of the rule.** :func:`canonical_numeric_value` lived in scidb
while scifor's standalone ``condense_numeric`` used a bare ``int(value)``, so
``"1.0"`` or ``"1.50"`` read from disk got a different value depending on which
layer read it (2026-10-21). scidb imports the rule from here now.

**Who declares.** Without a database there is nowhere to store a declaration
and nothing stored to protect, so in standalone scifor the
``for_each(schema_key_types=...)`` call owns it. With scidb the database owns
it (``scidb.schema_types``) and scidb applies it through the helpers below,
before it hands scifor a pre-built combo list.
"""

from __future__ import annotations

from typing import Any, Callable

from scistacklog import Log

#: The declarable schema-key types.
VALID_SCHEMA_KEY_TYPES = ("numeric", "string")


class SchemaKeyTypeError(ValueError):
    """A schema key's value violates its declared type, or its spelling is
    ambiguous. ``scidb.SchemaKeyTypeError`` subclasses this one."""

    # scifor.for_each aborts the whole run on this error instead of recording
    # a per-combo skip: it is a configuration problem that would repeat
    # identically for every combo.
    scifor_fatal = True


def validate_key_types(key_types: "dict | None") -> dict[str, str]:
    """Check every value is ``"numeric"`` or ``"string"``; return a copy.

    Keys are not checked here — standalone scifor has no fixed schema. scidb
    additionally requires every key to be a schema key.
    """
    key_types = dict(key_types or {})
    bad = {k: t for k, t in key_types.items() if t not in VALID_SCHEMA_KEY_TYPES}
    if bad:
        raise ValueError(
            f"schema_key_types values must be one of {VALID_SCHEMA_KEY_TYPES}, got: {bad}"
        )
    return key_types


def canonical_numeric_value(key: str, value: Any) -> Any:
    """Canonical value for a schema key declared ``"numeric"``.

    Collapses every spelling of the same number to one identity: ints stay
    ints, integral floats become ints (MATLAB doubles arrive as ``1.0``),
    digit strings lose leading zeros (``"001"`` → 1), and float-like strings
    normalize through ``float`` (``"1.50"`` → 1.5).  Values that cannot be
    read as a number violate the declaration and raise SchemaKeyTypeError —
    declared types are enforced, never guessed around.
    """
    # numpy scalars (np.int64 from a DataFrame column) behave as numbers.
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, bool):
        raise SchemaKeyTypeError(
            f"Schema key '{key}' is declared numeric but got a bool: {value!r}"
        )
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return int(s)
        try:
            f = float(s)
            return int(f) if f.is_integer() else f
        except ValueError:
            pass
    raise SchemaKeyTypeError(
        f"Schema key '{key}' is declared numeric but got a non-numeric value: {value!r}"
    )


def canonicalize_values(key: str, values: list, key_type: "str | None") -> list:
    """An iterable's values under *key_type*: converted and de-duplicated
    (first spelling's position kept) when ``numeric``, unchanged otherwise."""
    if key_type != "numeric":
        return list(values)
    return list(dict.fromkeys(canonical_numeric_value(key, v) for v in values))


def canonicalize_combos(
    combos: list[dict],
    key_types: "dict[str, str] | None",
    *,
    condense_undeclared: bool = False,
    log: "Callable[[str], None] | None" = None,
) -> list[dict]:
    """Apply declared types to discovered (or pre-built) combos.

    * ``numeric`` keys go through :func:`canonical_numeric_value`;
    * ``string`` keys keep their spelling;
    * undeclared keys are converted only when *condense_undeclared* and the
      value is all digits (standalone scifor's automatic ``condense_numeric``).

    Combos that become identical (``6MWT-1.mat`` and ``6MWT-001.mat`` both on
    disk) are de-duplicated, keeping the first. Returns new dicts; the input
    is not mutated.
    """
    key_types = key_types or {}
    out: list[dict] = []
    seen: set = set()
    n_converted = 0
    for combo in combos:
        new = dict(combo)
        for key, value in combo.items():
            key_type = key_types.get(key)
            if key_type == "string":
                continue
            if key_type == "numeric" or (
                condense_undeclared and isinstance(value, str) and value.isdigit()
            ):
                converted = canonical_numeric_value(key, value)
                if converted != value or type(converted) is not type(value):
                    n_converted += 1
                    if log is not None and str(converted) != str(value):
                        log(f"condensed discovered value: {key}={value!r} -> {converted!r}")
                new[key] = converted
        signature = tuple(sorted((k, repr(v)) for k, v in new.items()))
        if signature in seen:
            continue
        seen.add(signature)
        out.append(new)
    if len(out) != len(combos):
        Log.info(
            "schema key types: %d of %d combo(s) collapsed into another under %s",
            len(combos) - len(out),
            len(combos),
            describe(key_types) if key_types else "condense_numeric",
            layer="scifor",
        )
    if n_converted:
        Log.debug(
            "schema key types: %d value(s) converted in %d combo(s)",
            n_converted,
            len(combos),
            layer="scifor",
        )
    return out


def numeric_match_keys(keys, key_types: "dict[str, str] | None") -> "set | None":
    """Keys a PathInput may match through zero-padding (``1`` ↔ ``"001"``):
    everything except ``string``-declared keys. ``None`` (= every key) when
    nothing is declared ``string``."""
    strings = {k for k, t in (key_types or {}).items() if t == "string"}
    if not strings:
        return None
    return {k for k in keys if k not in strings}


def describe(key_types: "dict | None") -> str:
    """``trial=numeric, subject=string`` or ``none declared`` — for log lines."""
    if not key_types:
        return "none declared"
    return ", ".join(f"{k}={t}" for k, t in sorted(key_types.items()))
