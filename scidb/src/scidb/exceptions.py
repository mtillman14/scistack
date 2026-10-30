"""Custom exceptions for scidb."""

from scifor.failures import ActionableError
from scifor.schema_types import SchemaKeyTypeError as _SciforSchemaKeyTypeError


class SciStackError(ActionableError):
    """Base exception for all scidb errors.

    Every one is raised on purpose, with a message that says what to change,
    so it is an ``ActionableError``: the journal files it under "your setup
    or data", never under "SciStack internal error"
    (``scifor.failures.classify_blame``)."""

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


class ForColumnsNoColumnsError(SciStackError, ValueError):
    """``MyVar.for_columns()`` (every column) found no data column to iterate:
    the variable has no saved records yet, or every column it has is a schema
    key or internal. Before 2026-10-28 this was a plain ``ValueError`` and the
    run journal filed it as "SciStack internal error"."""

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
    were lost, and the cause. ``causes`` holds the exceptions themselves, each
    already passed through the save seam (``scifor.failures.as_user_value_error``),
    so each is an ``ActionableError``; who can fix this error is read from them.
    """

    def __init__(
        self,
        fn_name: str,
        failures: "list[str]",
        saved: int,
        causes: "list[BaseException] | None" = None,
    ) -> None:
        self.fn_name = fn_name
        self.failures = list(failures)
        self.saved = saved
        self.causes = list(causes or [])
        super().__init__(
            f"{fn_name}: {len(self.failures)} output(s) could not be saved "
            f"({saved} record(s) saved): " + "; ".join(self.failures)
        )

    @property
    def blame(self) -> str:  # type: ignore[override]
        """``user_value`` if any output failed on its values, else ``setup``."""
        blames = {getattr(c, "blame", "setup") for c in self.causes}
        return "user_value" if "user_value" in blames else "setup"

    @property
    def probable(self) -> bool:  # type: ignore[override]
        """True when every value-caused failure is a best guess (no check
        named its cause)."""
        valued = [c for c in self.causes if getattr(c, "blame", None) == "user_value"]
        return bool(valued) and all(getattr(c, "probable", False) for c in valued)

    @property
    def remedy(self) -> "dict | None":  # type: ignore[override]
        """The first cause's remedy (e.g. ``DuplicateLocationError``), so the
        GUI popup for a failed output save needs no unwrapping of its own."""
        from scifor.failures import remedy_of

        for cause in self.causes:
            remedy = remedy_of(cause)
            if remedy:
                return remedy
        return None


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


class DuplicateLocationError(SciStackError):
    """Raised by save_batch when one save would put two DIFFERENT records of
    the same variant at one schema location (docs/claude/duplicate-location-vs-stale.md).

    The usual cause: a table loaded once and saved per row, with two rows for
    one subject (scistack.log 2026-10-27, subject 2455369). Saved together,
    both records would stay "latest" and readers would see two values where
    one is expected. Which row is right, or whether the rows need a finer
    schema key, is the user's call, so nothing is written for the save.

    ``collisions``: one dict per location, ``{location: {key: value}, count,
    differing_columns}``. ``remedy`` is the popup the GUI shows; this class is
    the only owner of its wording.
    """

    #: Locations shown in the message; the remedy lists up to REMEDY_ITEMS_MAX.
    MESSAGE_ITEMS_MAX = 5
    REMEDY_ITEMS_MAX = 20

    def __init__(
        self,
        variable: str,
        collisions: "list[dict]",
        schema_keys: "list[str]",
    ) -> None:
        self.variable = variable
        self.collisions = list(collisions)
        self.schema_keys = list(schema_keys)
        level = self._level()
        shown = [
            f"  {_location_text(c['location'])} x{c['count']}"
            + (
                f" (differ in {_preview(c['differing_columns'])})"
                if c.get("differing_columns")
                else ""
            )
            for c in self.collisions[: self.MESSAGE_ITEMS_MAX]
        ]
        more = len(self.collisions) - len(shown)
        if more > 0:
            shown.append(f"  ... +{more} more location(s)")
        super().__init__(
            f"{variable}: {len(self.collisions)} {level} location(s) would get "
            f"more than one different record from this save, so nothing was "
            f"saved:\n" + "\n".join(shown) + "\n"
            f"The schema keys ({', '.join(self.schema_keys)}) give these rows "
            f"the same location, so SciStack can't tell which one is the value. "
            f"To fix it, change the dataset schema (a schema key, such as a visit "
            f"or session column, that tells the rows apart; schema keys are set "
            f"when a database is created, so this means a new project database) "
            f"or change the data (remove the extra row in the "
            f"source file, or combine the rows in your function), or save with "
            f"on_duplicate=\"combine\" to make each location one record whose "
            f"differing values are lists."
        )

    def _level(self) -> str:
        """The deepest schema key the colliding locations name."""
        named = {k for c in self.collisions for k in c["location"]}
        deepest = [k for k in self.schema_keys if k in named]
        return deepest[-1] if deepest else "schema"

    @property
    def remedy(self) -> dict:  # type: ignore[override]
        level = self._level()
        items = [
            {
                "location": _location_text(c["location"]),
                "count": int(c["count"]),
                "differing_columns": list(c.get("differing_columns") or []),
            }
            for c in self.collisions[: self.REMEDY_ITEMS_MAX]
        ]
        return {
            "kind": "duplicate_location",
            "title": f"More than one row for the same {level} in {self.variable}",
            "explanation": (
                f"This save has more than one different row at the same "
                f"location. The dataset's schema keys "
                f"({', '.join(self.schema_keys)}) are what tell records apart, "
                f"and these rows have the same values in all of them, so "
                f"SciStack can't tell which row is the value. Nothing was saved "
                f"for {self.variable}."
            ),
            "items": items,
            "n_more": max(0, len(self.collisions) - len(items)),
            "steps": [
                {
                    "title": "Change the dataset schema",
                    "text": (
                        f"If the rows are separate observations (for example "
                        f"two visits or sessions), the dataset needs a schema "
                        f"key that tells them apart, and the data needs that "
                        f"column. A database's schema keys "
                        f"({', '.join(self.schema_keys)}) are set when it is "
                        f"created, so this means creating a new project "
                        f"database with the extra key and running again."
                    ),
                    "action": None,
                },
                {
                    "title": "Change the data",
                    "text": (
                        "If only one row is right, remove the other one in the "
                        "source file. If the rows belong together, combine them "
                        "in your function before returning."
                    ),
                    "action": "reveal_function",
                },
                {
                    "title": "Combine the rows",
                    "text": (
                        "If the rows belong to one location (for example two "
                        "hospital stays of one subject), save them as one "
                        "record: a column whose rows agree keeps that one "
                        "value, and a column whose rows differ holds the list "
                        "of every row's value, in row order. This turns on "
                        "on_duplicate=\"combine\" for this function and runs "
                        "it again."
                    ),
                    "action": "combine_rows",
                },
            ],
        }


def _location_text(location: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in location.items()) or "(no schema keys)"


def _preview(names: "list[str]", limit: int = 4) -> str:
    shown = ", ".join(names[:limit])
    return shown + (f", ... +{len(names) - limit} more" if len(names) > limit else "")
