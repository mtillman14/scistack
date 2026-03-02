"""Path template input for for_each."""

from pathlib import Path
from typing import Any


class PathInput:
    """
    Resolve a path template using iteration metadata.

    Works as an input to for_each: on each iteration, .load() substitutes
    the current metadata values into the template and returns the resolved
    file path.  The user's function receives the path and handles file
    reading itself.

    Args:
        path_template: A format string with {key} placeholders, e.g.
                      "{subject}/trial_{trial}.mat"
        root_folder: Optional root directory.  If provided, paths are
                    resolved relative to it.  If None, resolved relative
                    to the current working directory.

    Example:
        for_each(
            process_file,
            inputs={
                "filepath": PathInput("{subject}/trial_{trial}.mat",
                                      root_folder="/data"),
            },
            outputs=[ProcessedSignal],
            subject=[1, 2, 3],
            trial=[0, 1, 2],
        )
    """

    def __init__(self, path_template: str, root_folder: str | Path | None = None):
        self.path_template = path_template
        self.root_folder = Path(root_folder) if root_folder is not None else None
        self.__name__ = f"PathInput({path_template!r})"

    def load(self, db=None, **metadata: Any) -> Path:
        """Resolve the template with the given metadata and return the path.

        Args:
            db: Accepted for compatibility with for_each's uniform db= passthrough.
                Ignored since PathInput resolves file paths, not database records.
            **metadata: Template substitution values.
        """
        relative_path = Path(self.path_template.format(**metadata))
        if self.root_folder is not None:
            return (self.root_folder / relative_path).resolve()
        return relative_path.resolve()

    def __repr__(self) -> str:
        return (
            f"PathInput({self.path_template!r}, "
            f"root_folder={self.root_folder!r})"
        )
