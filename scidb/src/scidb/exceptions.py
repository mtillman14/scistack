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


class UnsavedIntermediateError(SciStackError):
    """Raised when strict lineage mode detects an unsaved intermediate variable.

    In strict mode, all upstream BaseVariables must be saved before saving
    downstream results. This ensures complete data provenance and enables
    cache hits at every step.
    """

    pass
