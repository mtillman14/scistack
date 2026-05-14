"""Path template input for for_each."""

import json
import os
import re
import string as _string
from pathlib import Path
from typing import Any


def _find_project_root(start: Path | None = None) -> Path:
    """Walk up from *start* (or cwd) to find the nearest project root.

    The root is the first ancestor directory that contains ``pyproject.toml``
    or ``scistack.toml``.  Falls back to *start* (or cwd) when neither file
    is found anywhere in the hierarchy.
    """
    current = (start or Path.cwd()).resolve()
    for directory in [current, *current.parents]:
        if (directory / "pyproject.toml").exists() or (directory / "scistack.toml").exists():
            return directory
    return current


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
                    resolved relative to it.  If None and the template is
                    a relative path, the nearest ancestor directory
                    containing ``pyproject.toml`` or ``scistack.toml`` is
                    used; falls back to the current working directory when
                    neither file is found.

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

    def __init__(
        self,
        path_template: str,
        root_folder: str | Path | None = None,
        regex: bool = False,
    ):
        self.path_template = path_template
        self.root_folder = Path(root_folder) if root_folder is not None else None
        self.regex = bool(regex)
        self.__name__ = f"PathInput({path_template!r})"

    def to_key(self) -> str:
        """Return a structured JSON string for version_keys serialization.

        ``regex`` is only included when ``True`` so existing non-regex
        version keys remain byte-identical to records saved before this
        flag existed.
        """
        payload: dict = {
            "__type": "PathInput",
            "template": self.path_template,
            "root_folder": str(self.root_folder) if self.root_folder is not None else None,
        }
        if self.regex:
            payload["regex"] = True
        return json.dumps(payload)

    def load(self, db=None, **metadata: Any) -> Path:
        """Resolve the template with the given metadata and return the path.

        Args:
            db: Accepted for compatibility with for_each's uniform db= passthrough.
                Ignored since PathInput resolves file paths, not database records.
            **metadata: Template substitution values.

        Substitution is literal — only ``{key}`` patterns where ``key`` is
        one of the metadata names get replaced.  Anything else (e.g. a
        regex quantifier like ``{0,2}``) passes through untouched, which
        keeps the regex= path safe and matches MATLAB's ``strrep``
        semantics so the two layers stay in sync.

        When ``regex=True`` was passed at construction, the final path
        segment is then treated as a regular expression rather than a
        literal filename.  The last segment is matched against
        ``^pattern$`` over the files (not directories) in the parent
        directory.  Exactly one file must match — zero matches raise
        ``FileNotFoundError`` and multiple matches raise ``RuntimeError``.
        """
        resolved_str = self.path_template
        for key, value in metadata.items():
            resolved_str = resolved_str.replace("{" + key + "}", str(value))
        resolved_path = Path(resolved_str)

        if not self.regex:
            if self.root_folder is not None:
                return (self.root_folder / resolved_path).resolve()
            if not resolved_path.is_absolute():
                return (_find_project_root() / resolved_path).resolve()
            return resolved_path.resolve()

        # regex=True: treat the last segment as a regex.  Split on '/'
        # only — backslashes belong to the regex pattern (e.g. ``\d``,
        # ``\.``) and must not be confused with Windows path separators.
        # Templates that need Windows-style directories should use '/'.
        if "/" in resolved_str:
            dir_part, pattern = resolved_str.rsplit("/", 1)
        else:
            dir_part, pattern = "", resolved_str

        if Path(dir_part).is_absolute():
            dir_path = Path(dir_part)
        elif self.root_folder is not None:
            dir_path = self.root_folder / dir_part if dir_part else self.root_folder
        else:
            dir_path = _find_project_root() / dir_part if dir_part else _find_project_root()
        dir_path = dir_path.resolve()

        try:
            entries = [e for e in dir_path.iterdir() if e.is_file()]
        except OSError:
            entries = []

        matches = [e for e in entries if re.fullmatch(pattern, e.name)]

        if not matches:
            raise FileNotFoundError(
                f"PathInput regex pattern {pattern!r} matched no files in {dir_path}"
            )
        if len(matches) > 1:
            names = ", ".join(sorted(m.name for m in matches))
            raise RuntimeError(
                f"PathInput regex pattern {pattern!r} matched {len(matches)} files "
                f"in {dir_path}: {names}"
            )
        return matches[0].resolve()

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
        root = Path(self.root_folder) if self.root_folder is not None else _find_project_root()

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
