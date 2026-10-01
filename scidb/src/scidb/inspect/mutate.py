"""Mutator — the write-side facade of scidb.inspect.

**The bright line** (plan decision 5): tools built on this facade may flip
*declarative flags that the pipeline/analysis layer consults* — schema
exclusions today; future capabilities like record/variable annotations,
documented hypotheses & findings, canonical-variant marks. They must never
mutate records, invocations, or lineage, and never delete anything. Reads
stay on ``Inspector`` (strictly read-only); the two facades are separate so
the read-only guarantee is structural, not conventional.

**The one exception** (user decision 2026-09-30): ``delete_variant`` really
deletes an erroneous variant and everything computed from it, through
``scidb.variant_delete``. It is a dry-run-first, all-or-nothing operation with
a required reason and a tombstone. Do not add a second one without asking.

**How to add a new write capability** (keep this checklist honest):

1. The primitive lives in its owning scidb module (``exclusions.py`` today;
   e.g. a future ``annotations.py``) — the Mutator never writes SQL itself.
2. Add a thin Mutator method that calls the primitive and returns a
   ``MutationResult``. Decorate it ``@_mutation`` — that gives every write
   the same INFO-level audit logging and timing (NOTE 2), for free.
3. Require a human explanation (``reason``/``note``) whenever the primitive
   supports one — the audit trail is the product, not an afterthought.
4. CLI: register the command with ``_write_handler`` (not ``_handler``) in
   ``cli.py`` — dispatch then routes it through ``Mutator.open`` (write
   session with lock-error mapping) instead of the read-only Inspector.
5. Tests: mutation round-trip observed through the Inspector, invalid
   transitions, and (if new plumbing) the lock-contention path.

Write sessions are per-invocation: ``Mutator.open`` opens read-write, the
command runs, ``close()`` releases the file. Lock contention with a running
GUI/MATLAB session surfaces as ``DatabaseLockedError`` with a one-line
explanation, never a stack trace.
"""

from __future__ import annotations

import contextlib
import functools
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from ..exceptions import DatabaseLockedError
from ..log import Log

if TYPE_CHECKING:
    from ..database import DatabaseManager


@dataclass
class MutationResult:
    """Uniform result shape for every write operation (current and future),
    so the CLI/JSON rendering never needs per-operation code."""

    operation: str  # e.g. "exclude_schema"
    target: dict  # what it applied to (schema keys, record_id, …)
    reason: str
    detail: str = ""  # optional human-readable outcome note
    extra: dict = field(default_factory=dict)  # operation-specific payload


@contextlib.contextmanager
def lock_errors_mapped(db_path):
    """Convert DuckDB file-lock failures into DatabaseLockedError."""
    try:
        yield
    except DatabaseLockedError:
        raise
    except Exception as e:
        if "lock" in str(e).lower():
            raise DatabaseLockedError(
                f"{db_path} is locked by another session (a running GUI or "
                f"MATLAB session, or another process). Close it and retry."
            ) from e
        raise


def _mutation(method):
    """Audit every write: operation, target, reason, timing (NOTE 2)."""

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        t0 = time.perf_counter()
        result: MutationResult = method(self, *args, **kwargs)
        ms = (time.perf_counter() - t0) * 1000.0
        Log.info(
            f"mutate.{result.operation}: target={result.target} "
            f"reason={result.reason!r} ({ms:.1f} ms)"
        )
        return result

    return wrapper


class Mutator:
    """Write-side facade over a DatabaseManager (see module docstring)."""

    def __init__(self, db: DatabaseManager, _owns_db: bool = False):
        self._db = db
        self._owns_db = _owns_db

    @classmethod
    def open(cls, db_path: str | Path) -> Mutator:
        """Open an existing database read-write for one mutation session.

        Discovers the schema keys from the database itself (like
        Inspector.open) and maps lock contention to DatabaseLockedError.
        """
        from sciduckdb import schema_keys_from_db

        from ..database import DatabaseManager

        with lock_errors_mapped(db_path):
            keys = schema_keys_from_db(db_path)
            db = DatabaseManager(db_path, keys)
        Log.info(f"Mutator.open: {db_path} (read-write, schema_keys={keys})")
        return cls(db, _owns_db=True)

    def close(self) -> None:
        if self._owns_db:
            self._db._duck.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- write operations (each wraps an owning-layer primitive) ------------

    @_mutation
    def exclude_schema(self, reason: str, **schema_keys) -> MutationResult:
        """Mark a schema-key combination excluded from every analysis.

        Values are passed verbatim (schema values are strings; zero-padded
        values like "01" survive). Omitted keys act as wildcards.
        """
        from ..exclusions import exclude_schema

        exclude_schema(reason, db=self._db, **schema_keys)
        return MutationResult(
            operation="exclude_schema",
            target=dict(schema_keys),
            reason=reason,
            detail="excluded from all analyses (for_each skips it; history preserved)",
        )

    @_mutation
    def pin_variant(self, variable, selection: dict, reason: str) -> MutationResult:
        """Make one variant the DEFAULT of ``variable`` (``scidb.variant_pins``).

        A declarative flag the loaders consult. It hides nothing: a load that
        names a variant still reaches every other one.
        """
        from ..variant_pins import pin_variant

        pin = pin_variant(self._db, variable, selection, reason)
        return MutationResult(
            operation="pin_variant",
            target={"variable": pin.variable, "selection": pin.selection},
            reason=reason,
            detail=(
                "now the default for unnamed loads of this variable and of "
                "everything downstream; other variants stay loadable by name"
            ),
            extra={"pin_id": pin.pin_id},
        )

    @_mutation
    def release_pin(self, variable, reason: str) -> MutationResult:
        """Release ``variable``'s pin; unnamed loads go back to every current
        variant. The pin row is kept, with ``released_at`` set."""
        from ..variant_pins import release_pin

        name = getattr(variable, "__name__", variable)
        pin = release_pin(self._db, name, reason)
        return MutationResult(
            operation="release_pin",
            target={"variable": name},
            reason=reason,
            detail=(
                f"released pin {pin.pin_id}" if pin else "there was no active pin"
            ),
            extra={"pin_id": pin.pin_id if pin else None},
        )

    @_mutation
    def delete_variant(
        self, targets: list[dict], reason: str, expect_fingerprint: str | None = None
    ) -> MutationResult:
        """REALLY delete variant(s) and everything computed from them
        (``scidb.variant_delete``).

        This is the one write that crosses the facade's bright line ("never
        delete"), by explicit user decision, for erroneous runs. It is all or
        nothing, it requires a reason, and it leaves a tombstone. Pass the
        ``fingerprint`` of the plan the user confirmed, so a database that
        changed in between is refused rather than deleted from.
        """
        from ..variant_delete import delete_variant

        result = delete_variant(
            self._db, targets, reason, expect_fingerprint=expect_fingerprint
        )
        return MutationResult(
            operation="delete_variant",
            target={"targets": targets},
            reason=reason,
            detail=(
                f"deleted {result.plan.total_records} record(s) "
                f"{result.plan.by_variable}; tombstone {result.tombstone.tombstone_id}"
                + (f"; released pins {result.released_pins}" if result.released_pins else "")
            ),
            extra={
                "tombstone_id": result.tombstone.tombstone_id,
                "deleted_rows": result.deleted_rows,
                "released_pins": result.released_pins,
                "by_variable": result.plan.by_variable,
            },
        )

    @_mutation
    def include_schema(self, reason: str, **schema_keys) -> MutationResult:
        """Re-include a previously excluded schema-key combination."""
        from ..exclusions import include_schema

        include_schema(reason, db=self._db, **schema_keys)
        return MutationResult(
            operation="include_schema",
            target=dict(schema_keys),
            reason=reason,
            detail="re-included (the exclusion history rows are preserved)",
        )
