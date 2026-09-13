"""Path template input for for_each."""

import json
import os
import re
import string as _string
from pathlib import Path
from typing import Any

from scistacklog import Log


def _expects_file(segment: str) -> bool:
    """Heuristic: does this LAST path segment's template text look like a
    file (has a literal ``.ext``-style suffix) rather than a directory?

    Operates on the raw segment text, placeholders and all —
    ``Path(...).suffix`` only cares about the trailing ``.something``, so
    a placeholder immediately before the dot doesn't matter:
    ``{subject}.csv`` and ``report_{year}.csv`` both read as file-like via
    their literal ``.csv``; a bare ``{subject}`` (no literal suffix at
    all) reads as directory-like. This is what lets the LAST segment's
    filesystem match be restricted to files-only or directories-only —
    every other segment is already directory-only by construction (an
    intermediate path component the walker descends into).
    """
    return bool(Path(segment).suffix)


def _matches_kind(path: Path, expects_file: bool) -> bool:
    """True if *path* is the filesystem kind (file/directory) implied by
    ``_expects_file``."""
    return path.is_file() if expects_file else path.is_dir()


def _numeric_like(value: Any) -> bool:
    """True when *value* denotes an integer regardless of representation.

    Covers Python ints, integral floats (MATLAB doubles cross the bridge as
    ``1.0``), and digit strings (the MATLAB wrapper's ``num2str`` marshaling
    produces ``"1"``).  Bools are excluded — ``True`` is not the number 1 in
    a filename.
    """
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value.is_integer()
    if isinstance(value, str):
        return value.isdigit()
    return False


_project_root_override: Path | None = None


def set_project_root(root: "str | Path | None") -> "Path | None":
    """Pin the directory a *rootless* PathInput resolves against.

    A ``PathInput`` with no ``root_folder`` resolves relative paths against
    ``_find_project_root()``, which walks up from the **cwd**.  That is right
    for a script run from inside the project and wrong for every embedded
    interpreter: MATLAB's cwd is wherever the user's MATLAB happens to be
    sitting (for a generated command, a temp script directory), so the walk
    finds the wrong project or none at all.

    Callers that already know which project they are running — ``scidb.entities``
    over the MATLAB bridge, the GUI's generated command preamble — set it here
    once, and every rootless PathInput in the process resolves against it.

    This deliberately changes *resolution only*.  ``root_folder`` stays ``None``
    and ``to_key()`` is untouched, so the recorded identity of a PathInput is
    the same no matter where it ran.  Writing the project root into
    ``root_folder`` instead is what produced ``__unresolved__`` ghost nodes in
    the GUI (a run recorded under a key its own declaration does not have) and
    would bake a machine-specific absolute path into portable history.

    Passing ``None`` clears the override.  Returns the resolved override.
    """
    global _project_root_override
    _project_root_override = Path(root).resolve() if root is not None else None
    Log.info("[pathinput] project root override set to %s", _project_root_override)
    return _project_root_override


def get_project_root() -> "Path | None":
    """The pinned project root, or ``None`` when resolution falls back to
    walking up from the cwd.  See :func:`set_project_root`."""
    return _project_root_override


def clear_project_root() -> None:
    """Drop the pinned project root (tests, and any caller switching projects
    inside one process)."""
    set_project_root(None)


def _find_project_root(start: Path | None = None) -> Path:
    """Walk up from *start* (or cwd) to find the nearest project root.

    The root is the first ancestor directory that contains ``pyproject.toml``
    or ``scistack.toml``.  Falls back to *start* (or cwd) when neither file
    is found anywhere in the hierarchy.

    With no explicit *start*, a project root pinned by
    :func:`set_project_root` wins outright — the caller that pinned it knows
    which project is running, and the cwd does not.
    """
    if start is None and _project_root_override is not None:
        return _project_root_override
    current = (start or Path.cwd()).resolve()
    for directory in [current, *current.parents]:
        if (directory / "pyproject.toml").exists() or (
            directory / "scistack.toml"
        ).exists():
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
        aliases: Optional ``{key: {canonical: [spelling, ...]}}`` map so a
                schema key can have multiple on-disk spellings that all mean
                one canonical value, e.g.
                ``{"session": {"BL": ["Baseline", "1. Baseline"]}}`` — a
                ``{session}`` folder spelled "Baseline" resolves as "BL" and
                vice versa. Match-only: never affects how a path is written,
                only how it's found (``load()``) or reported (``discover()``).
        key_regex: Optional ``{key: pattern}`` map overriding the default
                greedy ``[^/\\]+`` capture used for ``{key}`` when building
                ``discover()``'s matching regex. Needed when two placeholders
                are adjacent with no delimiter between them (e.g.
                ``"{speed}{trial}"``) — without a literal to anchor the
                split, greedy backtracking hands everything but the last
                character to the first placeholder. Declaring
                ``key_regex={"speed": r"[A-Za-z]+", "trial": r"\d+"}``
                resolves the ambiguity explicitly. *pattern* is a raw regex
                fragment (no capturing group) substituted into the named
                group scifor builds internally; unrelated keys and
                delimiter-separated segments are unaffected.

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
        aliases: "dict[str, dict[str, list[str]]] | None" = None,
        key_regex: "dict[str, str] | None" = None,
    ):
        self.path_template = path_template
        self.root_folder = Path(root_folder) if root_folder is not None else None
        self.regex = bool(regex)
        self.__name__ = f"PathInput({path_template!r})"
        # Numeric-fallback caches (see load()): learned zero-pad width per
        # placeholder key, and per-directory listings validated by mtime.
        self._pad_width: dict[str, int] = {}
        self._dir_cache: dict[str, tuple[int, list[str]]] = {}
        # Listing-cache counters, reported by discover(): how many directories
        # a walk actually read from disk vs. served from a still-valid listing.
        self._dir_cache_reads = 0
        self._dir_cache_hits = 0
        self.aliases = aliases or {}
        self._alias_reverse = self._build_alias_reverse(self.aliases)
        self.key_regex = self._validate_key_regex(key_regex or {})

    def _build_alias_reverse(
        self, aliases: "dict[str, dict[str, list[str]]]"
    ) -> "dict[str, dict[str, str]]":
        """Validate and flatten ``aliases`` into ``key -> {spelling: canonical}``.

        The canonical value is always an implicit valid spelling of itself
        (no need to list it in its own alias list). Raises ``ValueError`` for
        an alias key that isn't a template placeholder, or a spelling
        (including a canonical acting as its own spelling) claimed by two
        different canonicals for the same key.
        """
        placeholder_set = set(self.placeholder_keys())
        reverse: dict[str, dict[str, str]] = {}
        for key, canon_map in aliases.items():
            if key not in placeholder_set:
                raise ValueError(
                    f"PathInput aliases key {key!r} is not a placeholder in "
                    f"template {self.path_template!r}"
                )
            key_reverse: dict[str, str] = {}
            for canonical, spellings in canon_map.items():
                for spelling in (canonical, *spellings):
                    existing = key_reverse.get(spelling)
                    if existing is not None and existing != canonical:
                        raise ValueError(
                            f"PathInput aliases for key {key!r}: spelling "
                            f"{spelling!r} is ambiguous between canonical "
                            f"values {existing!r} and {canonical!r}"
                        )
                    key_reverse[spelling] = canonical
            reverse[key] = key_reverse
        return reverse

    def _validate_key_regex(self, key_regex: "dict[str, str]") -> "dict[str, str]":
        """Validate ``key_regex`` keys are actual template placeholders."""
        placeholder_set = set(self.placeholder_keys())
        for key in key_regex:
            if key not in placeholder_set:
                raise ValueError(
                    f"PathInput key_regex key {key!r} is not a placeholder in "
                    f"template {self.path_template!r}"
                )
        return dict(key_regex)

    def to_key(self) -> str:
        """Return a structured JSON string for version_keys serialization.

        ``regex`` and ``aliases`` are only included when non-default so
        existing version keys remain byte-identical to records saved before
        those fields existed.
        """
        payload: dict = {
            "__type": "PathInput",
            "template": self.path_template,
            "root_folder": str(self.root_folder)
            if self.root_folder is not None
            else None,
        }
        if self.regex:
            payload["regex"] = True
        if self.aliases:
            payload["aliases"] = self.aliases
        if self.key_regex:
            payload["key_regex"] = self.key_regex
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

        Zero-padded numeric filenames are handled natively in non-regex
        mode: when the literally-resolved path does not exist and at least
        one metadata value is numeric-like (int, integral float, or digit
        string), the template is re-matched against the filesystem with
        each numeric placeholder matching any digit run of equal integer
        value — so ``trial=1`` finds ``6MWT-001.mat``.  Exactly one such
        match is returned; multiple numerically-equal matches raise
        ``RuntimeError``; zero matches fall back to returning the literal
        path unchanged (preserving the historical non-checking behavior).
        """
        return self.load_with_captures(metadata)[0]

    def load_with_captures(
        self,
        metadata: dict,
        db=None,
        numeric_match: "set | None" = None,
    ) -> "tuple[Path, dict[str, str]]":
        """Resolve like ``load(**metadata)`` and report spelling resolutions.

        Returns ``(path, resolutions)`` where *resolutions* maps each
        metadata key whose on-disk spelling differs from ``str(value)`` —
        i.e. the numeric or alias fallback bridged spellings, e.g.
        ``trial=1`` matching ``6MWT-001.mat`` yields ``{"trial": "001"}``,
        and ``session="BL"`` matching a ``Baseline`` folder (declared via
        ``aliases={"session": {"BL": ["Baseline"]}}``) yields
        ``{"session": "Baseline"}``.  Empty when the literal path resolved,
        no match was found, or in regex mode.

        Args:
            metadata: Template substitution values (dict form of load()'s
                kwargs).
            db: Accepted and ignored, mirroring load().
            numeric_match: Keys eligible for the numeric-equivalence
                fallback.  ``None`` (default) means every numeric-like key,
                matching load()'s behavior.  Callers that consider a key's
                spelling semantically significant (e.g. scidb's
                string-declared schema keys) exclude it here so it only ever
                matches literally.
        """
        resolved_str = self.path_template
        for key, value in metadata.items():
            resolved_str = resolved_str.replace("{" + key + "}", str(value))
        resolved_path = Path(resolved_str)

        if not self.regex:
            literal = self._absolutize(resolved_path)
            if literal.exists():
                return literal, {}

            numeric_keys = {
                k: int(v)
                for k, v in metadata.items()
                if _numeric_like(v) and (numeric_match is None or k in numeric_match)
            }
            alias_keys: "dict[str, tuple[dict[str, str], str]]" = {}
            for k, v in metadata.items():
                key_reverse = self._alias_reverse.get(k)
                if key_reverse is None:
                    continue
                canonical = key_reverse.get(str(v))
                if canonical is not None:
                    alias_keys[k] = (key_reverse, canonical)
            if not numeric_keys and not alias_keys:
                return literal, {}

            # Shortcut: re-render with previously learned pad widths and
            # try a single stat before scanning any directory.  Aliases have
            # no width cache, so this only ever short-circuits the numeric
            # side.
            padded, spellings = self._padded_literal(metadata, numeric_keys)
            if padded is not None and padded != literal and padded.exists():
                resolutions = {
                    k: sp for k, sp in spellings.items() if sp != str(metadata[k])
                }
                Log.debug(
                    "pathinput_numeric_fallback: pad-width cache hit: %s "
                    "(resolved spellings: %s)",
                    padded,
                    resolutions,
                    layer="scifor",
                )
                return padded, resolutions

            Log.debug(
                "pathinput_fallback: literal path missing, scanning for a "
                "numeric- or alias-equivalent match: %s",
                literal,
                layer="scifor",
            )
            matches = self._fallback_scan(metadata, numeric_keys, alias_keys)
            if len(matches) == 1:
                path, bindings = matches[0]
                for key, captured in bindings.items():
                    if key in numeric_keys:
                        self._pad_width[key] = len(captured)
                resolutions = {
                    k: cap for k, cap in bindings.items() if cap != str(metadata[k])
                }
                Log.debug(
                    "pathinput_fallback: matched %s (captures: %s)",
                    path,
                    bindings,
                    layer="scifor",
                )
                return path.resolve(), resolutions
            if len(matches) > 1:
                names = ", ".join(sorted(str(p) for p, _ in matches))
                raise RuntimeError(
                    f"PathInput fallback matched {len(matches)} files "
                    f"for template {self.path_template!r} with metadata "
                    f"{metadata!r}: {names}"
                )
            Log.debug(
                "pathinput_fallback: no numeric- or alias-equivalent match, "
                "returning literal path %s",
                literal,
                layer="scifor",
            )
            return literal, {}

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
            dir_path = (
                _find_project_root() / dir_part if dir_part else _find_project_root()
            )
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
        return matches[0].resolve(), {}

    def _absolutize(self, resolved_path: Path) -> Path:
        """Anchor a substituted template path exactly like the historical
        non-regex resolution: root_folder, else project root, else as-is."""
        if self.root_folder is not None:
            return (self.root_folder / resolved_path).resolve()
        if not resolved_path.is_absolute():
            return (_find_project_root() / resolved_path).resolve()
        return resolved_path.resolve()

    def _padded_literal(
        self, metadata: dict, numeric_keys: dict
    ) -> "tuple[Path | None, dict[str, str]]":
        """Render the template with cached zero-pad widths applied to numeric
        keys.  Returns ``(path, spellings)`` where *spellings* maps each
        numeric key to the text substituted for it; ``(None, {})`` when no
        width has been learned yet."""
        if not any(k in self._pad_width for k in numeric_keys):
            return None, {}
        rendered = self.path_template
        spellings: dict[str, str] = {}
        for key, value in metadata.items():
            if key in numeric_keys:
                text = str(numeric_keys[key]).zfill(self._pad_width.get(key, 0))
                spellings[key] = text
            else:
                text = str(value)
            rendered = rendered.replace("{" + key + "}", text)
        return self._absolutize(Path(rendered)), spellings

    def _root_and_segments(self) -> "tuple[Path, list[str]]":
        """Split the template into a walk root and relative segments.

        Shared by ``discover()`` and the fallback scan, mirroring
        the literal resolution's anchoring: an absolute template wins
        (POSIX ``/``, Windows drive ``Y:``, or UNC ``\\\\server\\share``),
        then ``root_folder``, then the project root.
        """
        normalised = self.path_template.replace("\\", "/")
        segments = [s for s in normalised.split("/") if s]
        if normalised.startswith("//") and len(segments) >= 2:
            # UNC path: the share (\\server\share) is the walk root — its
            # components are not listable directories on their own.
            return Path(f"//{segments[0]}/{segments[1]}"), segments[2:]
        if normalised.startswith("/"):
            return Path("/"), segments
        if segments and re.fullmatch(r"[A-Za-z]:", segments[0]):
            return Path(segments[0] + "/"), segments[1:]
        if self.root_folder is not None:
            return self.root_folder, segments
        return _find_project_root(), segments

    def _fallback_segment_regex(
        self,
        segment: str,
        metadata: dict,
        numeric_keys: dict,
        alias_keys: "dict[str, tuple[dict[str, str], str]]",
    ) -> "tuple[str, list, bool] | None":
        """Compile one template segment for the numeric/alias fallback scan.

        Returns ``(regex, checks, has_fallback)`` where *checks* is a list of
        ``(group_name, key, kind, param)`` constraints — ``kind="numeric"``
        with ``param`` the required int value, or ``kind="alias"`` with
        ``param`` a ``(reverse_map, canonical)`` pair the captured text must
        resolve to — or ``None`` when the segment cannot be Formatter-parsed
        (caller substitutes it literally, matching load()'s tolerance of
        regex-ish braces).
        """
        try:
            parts = list(_string.Formatter().parse(segment))
        except ValueError:
            return None
        regex = ""
        checks: list = []
        has_fallback = False
        key_counts: dict[str, int] = {}
        for literal, field_name, _, _ in parts:
            if literal:
                regex += re.escape(literal)
            if field_name is None:
                continue
            if field_name in numeric_keys:
                key_counts[field_name] = key_counts.get(field_name, 0) + 1
                count = key_counts[field_name]
                group = field_name if count == 1 else f"{field_name}_{count}"
                regex += f"(?P<{group}>\\d+)"
                checks.append((group, field_name, "numeric", numeric_keys[field_name]))
                has_fallback = True
            elif field_name in alias_keys:
                key_counts[field_name] = key_counts.get(field_name, 0) + 1
                count = key_counts[field_name]
                group = field_name if count == 1 else f"{field_name}_{count}"
                regex += f"(?P<{group}>[^/\\\\]+)"
                checks.append((group, field_name, "alias", alias_keys[field_name]))
                has_fallback = True
            elif field_name in metadata:
                regex += re.escape(str(metadata[field_name]))
            else:
                # Unknown key: literal-resolution leaves "{key}" untouched.
                regex += re.escape("{" + field_name + "}")
        return regex, checks, has_fallback

    def _list_dir(self, directory: Path) -> list[str]:
        """Directory listing memoized per instance, invalidated by mtime.

        Note the mtime check is itself one ``stat`` round-trip per directory,
        so on a network share a warm cache is cheaper than a cold one but not
        free — a re-walk of an unchanged share still touches every directory
        once. Skipping that entirely needs a memo ABOVE the walk (of the
        discovered combos), which is the caller's business; this cache's job is
        to never re-read a listing whose directory has not changed.
        """
        key = str(directory)
        try:
            mtime = directory.stat().st_mtime_ns
        except OSError:
            return []
        cached = self._dir_cache.get(key)
        if cached is not None and cached[0] == mtime:
            self._dir_cache_hits += 1
            return cached[1]
        try:
            entries = sorted(os.listdir(directory))
        except OSError:
            return []
        self._dir_cache[key] = (mtime, entries)
        self._dir_cache_reads += 1
        return entries

    def _fallback_scan(
        self,
        metadata: dict,
        numeric_keys: dict,
        alias_keys: "dict[str, tuple[dict[str, str], str]]",
    ) -> "list[tuple[Path, dict[str, str]]]":
        """Walk the template segments matching numeric placeholders by
        integer-equivalence and alias placeholders by declared spelling.
        Returns complete matches as ``(path, captures)`` where *captures*
        maps each fallback key to the raw text found on disk (numeric
        captures are used to learn pad widths; alias captures are the
        resolved on-disk spelling).

        Same file-vs-directory discipline as ``discover()``'s ``_walk``
        (see its docstring): the LAST segment is restricted to files or
        directories per ``_expects_file``, with an unfiltered second pass
        if that finds nothing at all.
        """
        root, segments = self._root_and_segments()
        if not segments:
            return []

        def _substitute_literal(segment: str) -> str:
            for key, value in metadata.items():
                segment = segment.replace("{" + key + "}", str(value))
            return segment

        def _walk(
            current_dir: Path, seg_idx: int, captures: dict, results: list, enforce_kind: bool
        ) -> None:
            segment = segments[seg_idx]
            is_last = seg_idx == len(segments) - 1
            compiled = self._fallback_segment_regex(
                segment, metadata, numeric_keys, alias_keys
            )

            if compiled is None or not compiled[2]:
                # No fallback placeholder in this segment: descend literally.
                candidate = current_dir / _substitute_literal(segment)
                if is_last:
                    if candidate.exists() and (
                        not enforce_kind
                        or _matches_kind(candidate, _expects_file(segment))
                    ):
                        results.append((candidate, dict(captures)))
                elif candidate.is_dir():
                    _walk(candidate, seg_idx + 1, captures, results, enforce_kind)
                return

            regex, checks, _ = compiled
            for entry in self._list_dir(current_dir):
                m = re.fullmatch(regex, entry)
                if m is None:
                    continue
                ok = True
                new_captures = dict(captures)
                for group, key, kind, param in checks:
                    text = m.group(group)
                    if kind == "numeric":
                        if int(text) != param:
                            ok = False
                            break
                    else:  # "alias"
                        reverse_map, canonical = param
                        if reverse_map.get(text) != canonical:
                            ok = False
                            break
                    new_captures[key] = text
                if not ok:
                    continue
                entry_path = current_dir / entry
                if is_last:
                    if not enforce_kind or _matches_kind(
                        entry_path, _expects_file(segment)
                    ):
                        results.append((entry_path, new_captures))
                elif entry_path.is_dir():
                    _walk(entry_path, seg_idx + 1, new_captures, results, enforce_kind)

        results: list = []
        _walk(root, 0, {}, results, enforce_kind=True)
        if not results:
            _walk(root, 0, {}, results, enforce_kind=False)
        return results

    def placeholder_keys(self) -> list[str]:
        """Return the list of unique placeholder keys in the template."""
        seen: set[str] = set()
        keys: list[str] = []
        for _, field_name, _, _ in _string.Formatter().parse(self.path_template):
            if field_name is not None and field_name not in seen:
                seen.add(field_name)
                keys.append(field_name)
        return keys

    def apply_discovery(
        self,
        metadata_iterables: dict,
        user_explicit_keys: "set | None" = None,
        log=None,
        condense_numeric: bool = False,
    ) -> "tuple[dict, list[dict] | None]":
        """Fill empty metadata iterables from filesystem discovery.

        Canonical PathInput discovery-orchestration shared by the scidb and
        scifor layers (see scidb.foreach Step 3 and scifor.foreach standalone
        resolution).  Runs ``discover()`` and decides how the discovered combos
        relate to the caller-supplied ``metadata_iterables``:

        * **Empty discovery** — nothing on disk matched; returns the iterables
          unchanged with ``discovered_combos=None``.
        * **Case A — no metadata iterables at all**: adopt every discovered key
          with all of its discovered values and return the discovered combos so
          they drive iteration directly.
        * **Case B — keys provided (some may be ``[]``)**: fill each empty
          template key from disk.  If the caller passed an *explicit*
          (non-empty) value for any template key (``user_explicit_keys``), that
          asserts intent — the Cartesian product of the iterables drives
          iteration, so ``discovered_combos=None``.  Otherwise every template
          key was auto-filled from disk, so the discovered combos are returned
          directly to avoid inventing non-existent Cartesian combos (e.g.
          ``{sub1,sub2} x {sessA,sessB}`` producing ``{sub2,sessB}`` when only
          three of the four files exist).

        Args:
            metadata_iterables: Mutable mapping of key -> list of values.
                Mutated in place (empty lists filled) and also returned.
            user_explicit_keys: Keys the caller passed with explicit non-empty
                values (i.e. NOT delegated to DB/filesystem resolution).  These
                suppress the discovered-combos shortcut in Case B.
            log: Optional ``log(msg)`` callback for parity with the layer-level
                logging around this decision.
            condense_numeric: When True, a discovered value that is purely
                digits (e.g. a zero-padded filename token ``"001"``) is
                collapsed to ``int`` (``1``) before it enters
                ``metadata_iterables``/the returned combos. Off by default —
                callers with stored identity to protect (scidb) must opt in
                explicitly via ``schema_key_types`` instead; this flag is for
                policy-free standalone use only. See
                ``docs/claude/schema-key-types.md``.

        Returns:
            ``(metadata_iterables, discovered_combos | None)``.
        """
        if user_explicit_keys is None:
            user_explicit_keys = set()

        def _log(msg: str) -> None:
            if log is not None:
                log(msg)

        combos = self.discover()
        _log(
            f"PathInput discovery: template={self.path_template!r}, "
            f"root_folder={self.root_folder!r}, matching_files={len(combos)}"
        )
        if condense_numeric and combos:
            condensed_combos = []
            for combo in combos:
                new_combo = dict(combo)
                for key, value in combo.items():
                    if isinstance(value, str) and value.isdigit():
                        condensed = int(value)
                        if str(condensed) != value:
                            _log(
                                f"condensed discovered value: {key}={value!r} "
                                f"-> {condensed!r}"
                            )
                        new_combo[key] = condensed
                condensed_combos.append(new_combo)
            combos = condensed_combos
        if not combos:
            return metadata_iterables, None

        combo_keys = list(combos[0].keys())

        # Case A: no metadata keys passed at all -> adopt every discovered key.
        if not metadata_iterables:
            for key in combo_keys:
                metadata_iterables[key] = list(dict.fromkeys(c[key] for c in combos))
                _log(
                    f"discovered {key} -> {len(metadata_iterables[key])} values from filesystem"
                )
            return metadata_iterables, combos

        # Case B: keys provided (some may be []).  Fill empty template keys
        # from disk; explicit user-provided values are left alone.
        user_filter_seen = False
        for key in combo_keys:
            if key not in metadata_iterables:
                continue
            user_vals = metadata_iterables[key]
            if not user_vals:
                metadata_iterables[key] = list(dict.fromkeys(c[key] for c in combos))
                _log(
                    f"discovered {key} -> {len(metadata_iterables[key])} values from filesystem"
                )
            elif key in user_explicit_keys:
                user_filter_seen = True

        if user_filter_seen:
            # User asserted intent for at least one template key; let the
            # Cartesian product of the iterables drive iteration.  Combos with
            # missing files surface at runtime rather than being dropped here.
            _log(
                "explicit user values for template keys; skipping discovery "
                "filter — Cartesian product of iterables will drive combos"
            )
            return metadata_iterables, None

        # All template keys filled from disk -> use discovered combos directly.
        _log(
            f"no user-explicit template keys; using {len(combos)} disk combos directly"
        )
        return metadata_iterables, combos

    def discover(self) -> list[dict[str, str]]:
        """Walk the filesystem and return all metadata combos matching the template.

        Splits the path template into segments and recursively matches each
        segment against actual directory entries.  Literal segments must match
        exactly, segments with ``{key}`` placeholders are converted to regexes
        with named capture groups.

        Returns a list of dicts (one per valid complete path), where each dict
        maps placeholder keys to their string values. Keys declared in
        ``aliases`` are canonicalized (e.g. an on-disk ``Baseline`` folder
        comes back as ``"BL"``); an unrecognized on-disk spelling under an
        aliased key passes through unchanged (fail-open, logged at debug).

        Absolute templates (POSIX ``/``, Windows drive ``Y:``, UNC
        ``\\\\server\\share``) anchor the walk at their own root, matching
        load()'s resolution semantics; ``root_folder`` and the project root
        only apply to relative templates.

        The LAST segment's matches are restricted to files or directories
        per ``_expects_file`` (e.g. a bare ``{subject}`` only matches
        subject DIRECTORIES, never a same-level file that happens to share
        a name) — found by hand: a flat file sitting next to real subject
        folders was silently discovered as a fake subject and crashed
        downstream. If that strict pass finds nothing at all (the
        heuristic guessed wrong — e.g. a directory genuinely named
        ``results.final``), a second, unfiltered pass runs as a fallback
        so a real match is never lost to the heuristic.
        """
        root, segments = self._root_and_segments()

        if not segments:
            return []

        # Timed, and the root named, because this is a filesystem walk with no
        # timeout in it: on a network share a cold `listdir` can block for
        # minutes and raise nothing. Before this line existed that showed up in
        # the log as a request that simply stopped between two database
        # queries — indistinguishable from a DuckDB hang, and looked for in the
        # wrong layer for an hour (2026-09-13,
        # .claude/plot-at-scale-plan.md §9). The directory count says how many
        # round-trips the walk cost; `cached` says how many the listing cache
        # absorbed, which is the number that should climb on a re-open.
        reads_before, hits_before = self._dir_cache_reads, self._dir_cache_hits
        with Log.timer(
            "pathinput_discover", layer="scifor", extra=str(root)
        ) as timer:
            results: list[dict[str, str]] = []
            with timer.phase("walk"):
                self._walk(root, segments, 0, {}, results, enforce_kind=True)
            if not results:
                with timer.phase("walk_unfiltered"):
                    self._walk(root, segments, 0, {}, results, enforce_kind=False)
        Log.info(
            "pathinput_discover: %d combo(s); %d director(ies) read from disk, "
            "%d served from the listing cache",
            len(results),
            self._dir_cache_reads - reads_before,
            self._dir_cache_hits - hits_before,
            layer="scifor",
        )
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
        enforce_kind: bool = True,
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
                if candidate.exists() and (
                    not enforce_kind or _matches_kind(candidate, _expects_file(segment))
                ):
                    results.append(dict(bindings))
            else:
                if candidate.is_dir():
                    self._walk(
                        candidate, segments, seg_idx + 1, bindings, results, enforce_kind
                    )
            return

        # Segment has placeholder(s) — build a regex
        pattern = self._segment_to_regex(segment)

        # Through the mtime-validated listing cache rather than a bare
        # os.listdir. This walk runs on every discovery, and every discovery on
        # a UNC share pays one network round-trip per directory; on 2026-09-13 a
        # second Schema Key Locations open re-walked the same unchanged share
        # and blocked for minutes with nothing logged (the listing had gone cold
        # in the SMB client). `_list_dir` already existed for the numeric
        # fallback path; this was the one caller still going around it.
        entries = self._list_dir(current_dir)
        if not entries:
            return

        for entry in entries:
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

            # Canonicalize aliased keys (e.g. "Baseline" -> "BL") before the
            # consistency check, so two segments binding the same key via
            # different on-disk spellings of one canonical don't spuriously
            # conflict.  Match-only: an on-disk value that isn't recognized
            # under a declared alias key is passed through unchanged rather
            # than rejected — surfaced via debug log so a typo'd folder name
            # is observable instead of silently producing a stray value.
            for key, raw_val in list(clean_captured.items()):
                key_reverse = self._alias_reverse.get(key)
                if key_reverse is None:
                    continue
                canonical = key_reverse.get(raw_val)
                if canonical is not None:
                    clean_captured[key] = canonical
                else:
                    Log.debug(
                        "pathinput_alias_unresolved: key=%r on-disk value=%r "
                        "has no matching alias/canonical entry; passing "
                        "through unchanged",
                        key,
                        raw_val,
                        layer="scifor",
                    )

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
                if not enforce_kind or _matches_kind(entry_path, _expects_file(segment)):
                    results.append(dict(new_bindings))
            else:
                if entry_path.is_dir():
                    self._walk(
                        entry_path, segments, seg_idx + 1, new_bindings, results, enforce_kind
                    )

    def _segment_to_regex(self, segment: str) -> str:
        """Convert a template segment like ``{subject}_XSENS_{session}`` to a regex.

        A placeholder declared in ``key_regex`` uses its custom pattern
        instead of the default greedy ``[^/\\]+`` — needed to disambiguate
        placeholders that are adjacent with no delimiter between them (see
        the ``key_regex`` docstring on ``__init__``).
        """
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
                pattern = self.key_regex.get(field_name, r"[^/\\]+")
                regex += f"(?P<{group_name}>{pattern})"
        return regex

    def __repr__(self) -> str:
        return f"PathInput({self.path_template!r}, root_folder={self.root_folder!r})"
