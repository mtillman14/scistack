"""Custom exceptions for scidb."""


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


class SchemaKeyTypeError(SciStackError):
    """Raised when a schema key's spelling is ambiguous or violates its type.

    Two situations:
    - A PathInput numeric fallback had to bridge spellings (e.g. trial=1
      matched "001" on disk) for a schema key with no declared type — the
      dataset has proven the spelling ambiguous, so the user must declare
      the key numeric or string via configure_database(schema_key_types=...).
    - A key declared "numeric" received a value that is not numeric.
    """

    # scifor.for_each aborts the whole run on this error instead of
    # recording a per-combo skip: the failure is a configuration problem
    # that would repeat identically for every combo.
    scifor_fatal = True


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
    results and saved none was reported "done" (scidb.log 2026-10-01,
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


def _preview(names: "list[str]", limit: int = 4) -> str:
    shown = ", ".join(names[:limit])
    return shown + (f", ... +{len(names) - limit} more" if len(names) > limit else "")
