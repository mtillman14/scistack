"""File I/O classes for scifor: MatFile and CsvFile.

These can be used as either inputs or outputs to for_each.
They implement the .load(**metadata) / .save(data, **metadata) protocol.
"""

import os
from pathlib import Path
from typing import Any


class MatFile:
    """Load and save MATLAB .mat files using scipy.io.

    Args:
        path_template: A format string with {key} placeholders.
                       e.g. "data/{subject}/{session}.mat"

    Example:
        for_each(
            process,
            inputs={"signal": MatFile("data/{subject}/{session}.mat")},
            outputs=[MatFile("results/{subject}/{session}.mat")],
            subject=[1, 2],
            session=["pre", "post"],
        )
    """

    def __init__(self, path_template: str):
        self.path_template = path_template
        self.__name__ = f"MatFile({path_template!r})"

    def _resolve_path(self, metadata: dict) -> Path:
        return Path(self.path_template.format(**metadata))

    def load(self, **metadata: Any) -> dict:
        """Load a .mat file and return the contents as a dict.

        Args:
            **metadata: Template substitution values. Any extra kwargs
                        (e.g. db=) are ignored.
        """
        import scipy.io
        # Strip non-template kwargs silently
        template_keys = _template_keys(self.path_template)
        template_meta = {k: v for k, v in metadata.items() if k in template_keys}
        path = self._resolve_path(template_meta)
        return scipy.io.loadmat(str(path))

    def save(self, data: Any, **metadata: Any) -> None:
        """Save data to a .mat file.

        Args:
            data: Data to save. If a dict, saved as-is. Otherwise
                  wrapped under the key ``"data"``.
            **metadata: Template substitution values. Any extra kwargs
                        (e.g. db=, __fn=) are ignored.
        """
        import scipy.io
        template_keys = _template_keys(self.path_template)
        template_meta = {k: v for k, v in metadata.items() if k in template_keys}
        path = self._resolve_path(template_meta)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not isinstance(data, dict):
            data = {"data": data}
        scipy.io.savemat(str(path), data)

    def __repr__(self) -> str:
        return f"MatFile({self.path_template!r})"


class CsvFile:
    """Load and save CSV files using pandas.

    Args:
        path_template: A format string with {key} placeholders.
                       e.g. "data/{subject}/{session}.csv"

    Example:
        for_each(
            process,
            inputs={"table": CsvFile("data/{subject}/{session}.csv")},
            outputs=[CsvFile("results/{subject}/{session}.csv")],
            subject=[1, 2],
            session=["pre", "post"],
        )
    """

    def __init__(self, path_template: str):
        self.path_template = path_template
        self.__name__ = f"CsvFile({path_template!r})"

    def _resolve_path(self, metadata: dict) -> Path:
        return Path(self.path_template.format(**metadata))

    def load(self, **metadata: Any) -> "pd.DataFrame":
        """Load a CSV file and return a pandas DataFrame.

        Args:
            **metadata: Template substitution values. Extra kwargs (e.g. db=)
                        are ignored.
        """
        import pandas as pd
        template_keys = _template_keys(self.path_template)
        template_meta = {k: v for k, v in metadata.items() if k in template_keys}
        path = self._resolve_path(template_meta)
        return pd.read_csv(str(path))

    def save(self, data: Any, **metadata: Any) -> None:
        """Save data to a CSV file.

        Args:
            data: pandas DataFrame or any object convertible to one.
            **metadata: Template substitution values. Extra kwargs (e.g. db=)
                        are ignored.
        """
        import pandas as pd
        template_keys = _template_keys(self.path_template)
        template_meta = {k: v for k, v in metadata.items() if k in template_keys}
        path = self._resolve_path(template_meta)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)
        data.to_csv(str(path), index=False)

    def __repr__(self) -> str:
        return f"CsvFile({self.path_template!r})"


def _template_keys(template: str) -> set[str]:
    """Extract {key} placeholder names from a format string."""
    import string
    formatter = string.Formatter()
    return {field_name for _, field_name, _, _ in formatter.parse(template)
            if field_name is not None}
