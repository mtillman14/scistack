"""Custom exceptions for scidb."""

from scifor.schema_types import SchemaKeyTypeError as _SciforSchemaKeyTypeError


class SciStackError(Exception):
    """Base exception for all scidb errors."""

    pass


class NotRegisteredError(SciStackError):
    """Raised when trying to save/load an unregistered variable type."""

    pass


class NotFoundError(SciStackError):
    """Raised when no matching data is found for the given metadata."""

    pass


class DatabaseNotConfiguredError(SciStackError):
    """Raised when trying to use implicit database before configuration."""

    pass


class ReservedMetadataKeyError(SciStackError):
    """Raised when user tries to use a reserved metadata key."""

    pass


class AmbiguousVersionError(SciStackError):
    """Raised when load() matches multiple variants and no branch filter narrows to one."""

    pass


class AmbiguousParamError(SciStackError):
    """Raised when a bare param name matches multiple namespaced keys in branch_params."""

    pass


class PipelineCycleError(SciStackError):
    """Raised when a Pipeline's variable-type dependency graph has a cycle.

    The one-step case is a function registered as consuming and producing
    the same variable type.
    """

    pass


class SchemaKeyTypeError(SciStackError, _SciforSchemaKeyTypeError):
    """Raised when a schema key's spelling is ambiguous or violates its type.

    Two situations:
    - A PathInput numeric fallback had to bridge spellings (e.g. trial=1
      matched "001" on disk) for a schema key with no declared type — the
      dataset has proven the spelling ambiguous, so the user must declare
      the key numeric or string (the GUI's Numeric checkbox,
      db.set_schema_key_type, or configure_database(schema_key_types=...)).
    - A key declared "numeric" received a value that is not numeric.
    """

    # scifor.for_each aborts the whole run on this error instead of
    # recording a per-combo skip: the failure is a configuration problem
    # that would repeat identically for every combo.
    scifor_fatal = True


class SchemaKeyTypeConflictError(SciStackError, ValueError):
    """``configure_database(schema_key_types=...)`` disagrees with the type the
    database already stores for a key (``scidb.schema_types`` owns it)."""

    pass


class SchemaKeyTypeChangeError(SciStackError, ValueError):
    """A schema-key type change would re-spell or merge stored values and was
    not confirmed. ``.impact`` is ``scidb.schema_types.impact``'s dict."""

    def __init__(self, message: str, impact: dict):
        super().__init__(message)
        self.impact = impact


class DatabaseLockedError(SciStackError):
    """Raised when the database file is locked by another session.

    DuckDB allows one read-write connection (or multiple read-only ones);
    opening while a GUI/MATLAB session or another process holds the file
    raises this instead of a raw DuckDB IO error.
    """

    pass


class OutputSaveError(SciStackError):
    """Raised by for_each when an output could not be saved.

    The function ran, but its results did not reach the database. The save
    used to log ERROR and return normally, so a MATLAB run that computed 420
    results and saved none was reported "done" (scistack.log 2026-10-01,
    GaitRiteSymmetry). Raised after every output has been attempted and
    provenance has been written for what did save, so one failing output
    neither hides nor blocks the others.

    ``failures`` holds one line per failed output: its name, how many records
    were lost, and the cause.
    """

    def __init__(self, fn_name: str, failures: "list[str]", saved: int) -> None:
        self.fn_name = fn_name
        self.failures = list(failures)
        self.saved = saved
        super().__init__(
            f"{fn_name}: {len(self.failures)} output(s) could not be saved "
            f"({saved} record(s) saved): " + "; ".join(self.failures)
        )


class ColumnSetChangedError(SciStackError):
    """Raised by save_batch when a save's data columns differ from the ones
    the variable already stores.

    A variable's table takes its columns from its first save. A save with
    other columns used to fail as an opaque DuckDB ``Binder Error`` (a new
    column), or, worse, succeed and overwrite the variable's column list so
    the OLD records loaded without their columns (a missing column). This
    names the problem and the two ways out. Nothing is written for the batch.
    """

    def __init__(
        self,
        variable: str,
        added: "list[str]",
        removed: "list[str]",
        n_existing: int,
    ) -> None:
        self.variable = variable
        self.added = list(added)
        self.removed = list(removed)
        self.n_existing = n_existing
        parts = []
        if self.added:
            parts.append(f"{len(self.added)} new ({_preview(self.added)})")
        if self.removed:
            parts.append(f"{len(self.removed)} missing ({_preview(self.removed)})")
        super().__init__(
            f"{variable} already stores {n_existing} record(s) with a different "
            f"set of data columns than these results: " + " and ".join(parts) + ". "
            f"A variable keeps the columns of its first save, so nothing was "
            f"saved. To fix it, either save this output into a NEW variable "
            f"(for example {variable}_2), or delete {variable}'s existing records "
            f"(in the GUI: Variants popup -> Delete, which also deletes what was "
            f"computed from them) and run again; an emptied variable takes the "
            f"new columns."
        )


class ColumnTypeChangedError(SciStackError):
    """Raised by a save whose values contradict the type of a data column
    the variable already stores values in (user decision 2026-10-22: refuse,
    never silently re-type stored data).

    A column's type is decided by the first save that brings it non-empty
    values; a column that is still empty takes any type, an integer column
    widens to DOUBLE, and a text column takes numbers as text. What is left —
    text into a numeric column, a scalar where an array is stored, and so
    on — lands here. Nothing is written for the save.
    """

    def __init__(
        self,
        variable: str,
        conflicts: "list[tuple[str, str, str, list]]",
        n_existing: int,
    ) -> None:
        # conflicts: (column, stored type, incoming type, sample values)
        self.variable = variable
        self.conflicts = list(conflicts)
        self.n_existing = n_existing
        lines = [
            f"  {col}: stored as {stored} in the existing records, but this "
            f"save has {incoming} values (e.g. {', '.join(repr(v) for v in sample)})"
            for col, stored, incoming, sample in self.conflicts
        ]
        super().__init__(
            f"{variable} already stores {n_existing} record(s), and this save's "
            f"values do not fit {len(self.conflicts)} of its column type(s), "
            f"so nothing was saved:\n" + "\n".join(lines) + "\n"
            f"A column's type is set by the first save that has values in it. "
            f"To fix it, make the function return values of the stored type "
            f"(for pandas.read_csv: dtype={{column: str}} keeps a column as "
            f"text), save into a NEW variable (for example {variable}_2), or "
            f"delete {variable}'s existing records (in the GUI: Variants popup "
            f"-> Delete) and run again."
        )


def _preview(names: "list[str]", limit: int = 4) -> str:
    shown = ", ".join(names[:limit])
    return shown + (f", ... +{len(names) - limit} more" if len(names) > limit else "")
