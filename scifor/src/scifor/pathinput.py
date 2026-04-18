"""Path template input for for_each."""

import json
import os
import re
import string as _string
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

    def to_key(self) -> str:
        """Return a structured JSON string for version_keys serialization."""
        return json.dumps({
            "__type": "PathInput",
            "template": self.path_template,
            "root_folder": str(self.root_folder) if self.root_folder is not None else None,
        })

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

    def placeholder_keys(self) -> list[str]:
        """Return the list of unique placeholder keys in the template."""
        seen: set[str] = set()
        keys: list[str] = []
        for _, field_name, _, _ in _string.Formatter().parse(self.path_template):
            if field_name is not None and field_name not in seen:
                seen.add(field_name)
                keys.append(field_name)
        return keys

    def discover(self) -> list[dict[str, str]]:
        """Walk the filesystem and return all metadata combos matching the template.

        Splits the path template into segments and recursively matches each
        segment against actual directory entries.  Literal segments must match
        exactly, segments with ``{key}`` placeholders are converted to regexes
        with named capture groups.

        Returns a list of dicts (one per valid complete path), where each dict
        maps placeholder keys to their string values.
        """
        root = self.root_folder if self.root_folder is not None else Path.cwd()
        root = Path(root)

        # Split template into path segments
        # Normalise separators to '/'
        normalised = self.path_template.replace("\\", "/")
        segments = [s for s in normalised.split("/") if s]

        if not segments:
            return []

        results: list[dict[str, str]] = []
        self._walk(root, segments, 0, {}, results)
        return results

    # ------------------------------------------------------------------
    # Internal recursive walker
    # ------------------------------------------------------------------

    def _walk(
        self,
        current_dir: Path,
        segments: list[str],
        seg_idx: int,
        bindings: dict[str, str],
        results: list[dict[str, str]],
    ) -> None:
        """Recursively descend through *segments*, matching filesystem entries."""
        if seg_idx >= len(segments):
            return

        segment = segments[seg_idx]
        is_last = seg_idx == len(segments) - 1

        # Check if segment contains any placeholders
        has_placeholder = "{" in segment and "}" in segment

        if not has_placeholder:
            # Literal segment — must match exactly
            candidate = current_dir / segment
            if is_last:
                if candidate.exists():
                    results.append(dict(bindings))
            else:
                if candidate.is_dir():
                    self._walk(candidate, segments, seg_idx + 1, bindings, results)
            return

        # Segment has placeholder(s) — build a regex
        pattern = self._segment_to_regex(segment)

        try:
            entries = os.listdir(current_dir)
        except OSError:
            return

        for entry in sorted(entries):
            m = re.fullmatch(pattern, entry)
            if m is None:
                continue

            # Validate captured values against existing bindings
            captured = m.groupdict()
            # Strip numbered suffixes we added for duplicate keys
            clean_captured: dict[str, str] = {}
            for raw_key, val in captured.items():
                # Keys are like "key" or "key_2", "key_3" etc.
                key = re.sub(r"_\d+$", "", raw_key)
                clean_captured[key] = val

            consistent = True
            for key, val in clean_captured.items():
                if key in bindings and bindings[key] != val:
                    consistent = False
                    break
            if not consistent:
                continue

            new_bindings = {**bindings, **clean_captured}
            entry_path = current_dir / entry

            if is_last:
                if entry_path.exists():
                    results.append(dict(new_bindings))
            else:
                if entry_path.is_dir():
                    self._walk(entry_path, segments, seg_idx + 1, new_bindings, results)

    @staticmethod
    def _segment_to_regex(segment: str) -> str:
        """Convert a template segment like ``{subject}_XSENS_{session}`` to a regex."""
        parts = _string.Formatter().parse(segment)
        regex = ""
        key_counts: dict[str, int] = {}
        for literal, field_name, _, _ in parts:
            if literal:
                regex += re.escape(literal)
            if field_name is not None:
                # Handle duplicate keys in the same segment by numbering
                key_counts[field_name] = key_counts.get(field_name, 0) + 1
                count = key_counts[field_name]
                group_name = field_name if count == 1 else f"{field_name}_{count}"
                regex += f"(?P<{group_name}>[^/\\\\]+)"
        return regex

    def __repr__(self) -> str:
        return (
            f"PathInput({self.path_template!r}, "
            f"root_folder={self.root_folder!r})"
        )
