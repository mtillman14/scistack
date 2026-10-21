"""Path template input for for_each."""

import json
import os
import re
import string as _string
from pathlib import Path
from typing import Any

from scistacklog import Log

#: ``{directory: (mtime_ns, sorted entries)}`` — directory listings shared by
#: every PathInput in the process.
#:
#: MODULE-LEVEL on purpose (2026-09-22). It lived on the instance, and the
#: canvas rebuilds PathInput objects from their stored specs on every refresh,
#: so the cache was born empty every single time: ``0 served from the listing
#: cache`` on all 79 walks of one session, ~344 network directory reads per
#: refresh and ~3 s of a 5.7–21 s ``get_pipeline``. The instance docstring
#: already said the memo belonged above the walk; this is above the *object*,
#: which is where the reuse actually is.
#:
#: Safe to share because every entry is validated against the directory's
#: mtime on read — a stale listing cannot be served, only re-read. The cost of
#: sharing is one ``stat`` per directory per walk, which is what the instance
#: cache paid too.
#:
#: Unbounded by design: it holds one small list per directory a template has
#: ever walked, and a process that walks enough directories for that to matter
#: has a much larger problem. ``clear_listing_cache()`` exists for tests and
#: for the "I just changed the filesystem" case.
_DIR_CACHE: dict[str, tuple[int, list[str]]] = {}


def validate_path_input_name(name: Any) -> str:
    """*name* as a PathInput name, or ``ValueError``.

    A non-empty string with no surrounding whitespace. The one rule, used by
    the constructor and by anything that rebuilds a PathInput from stored
    data.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError(
            "PathInput requires a non-empty name=, e.g. "
            "PathInput('{subject}/data.csv', name='RawData'). The name is the "
            f"PathInput's identity; the template is only where to look (got {name!r})."
        )
    if name != name.strip():
        raise ValueError(f"PathInput name {name!r} has leading/trailing whitespace")
    return name


def path_input_key(name: str) -> str:
    """The identity key of the PathInput named *name* — what
    :meth:`PathInput.to_key` returns. THE one spelling: scidb derives the key
    of a stored spec through this, so forward and backward agree by
    construction."""
    return json.dumps({"__type": "PathInput", "name": name})


# Every field PathInput.to_spec() can write; PathInput.from_spec() reads
# exactly these and refuses any other.
_SPEC_FIELDS = (
    "__type", "name", "template", "root_folder", "regex", "aliases", "key_regex",
)


def key_regex_pattern_error(pattern: Any) -> "str | None":
    """Why *pattern* cannot be a ``key_regex`` fragment, or ``None``.

    THE one check, used by the constructor, the entities-file reader and the
    GUI's live validation. The fragment is spliced into a named group inside
    a ``fullmatch``, so it must be a string that compiles and has no
    capturing group of its own (one would shift every group after it; use
    ``(?:...)``). Anchors are unnecessary — the whole entry name must match.
    """
    if not isinstance(pattern, str) or not pattern:
        return f"pattern must be a non-empty string, got {pattern!r}"
    try:
        compiled = re.compile(pattern)
    except re.error as exc:
        return f"pattern {pattern!r} does not compile: {exc}"
    if compiled.groups:
        return (
            f"pattern {pattern!r} has a capturing group; write (?:...) instead"
        )
    return None


def clear_listing_cache() -> None:
    """Forget every cached directory listing."""
    _DIR_CACHE.clear()


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
    :func:`project_root`, which is the **cwd** unless pinned.  That is right
    for a script run from its project folder and wrong for every embedded
    interpreter: MATLAB's cwd is wherever the user's MATLAB happens to be
    sitting (for a generated command, a temp script directory), so the cwd
    names the wrong folder.

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
    """The pinned project root, or ``None`` when :func:`project_root` falls
    back to the cwd.  See :func:`set_project_root`."""
    return _project_root_override


def enter_project_root() -> Path:
    """Make the pinned project root the process's working directory.

    Every execution starts from the project root, so a user function that
    opens a relative path finds it in the same place no matter who launched
    the run.  Called at the start of each run rather than once at startup:
    a user function is free to ``os.chdir`` and the next run must not
    inherit that.  The process stays there afterwards (there is no
    "previous" directory worth restoring in a server process), matching the
    generated MATLAB script, whose ``cd`` also sticks.

    With no pin, :func:`project_root` *is* the cwd, so nothing moves.  A
    pinned root that is not a directory is logged and left alone rather
    than failing the run -- resolution against it would fail loudly anyway.
    Returns the working directory the run starts in.
    """
    root = _project_root_override
    try:
        before = Path.cwd().resolve()
    except FileNotFoundError:
        # The old cwd was deleted out from under the process.
        before = None
    if root is None:
        Log.info("[pathinput] working directory %s (no project root pinned)", before)
        return before if before is not None else Path.cwd()
    if not root.is_dir():
        Log.warn(
            "[pathinput] project root %s is not a directory -- run starts in %s",
            root,
            before,
        )
        return before if before is not None else Path.cwd()
    if before != root:
        os.chdir(root)
        Log.info("[pathinput] working directory %s -> %s (project root)", before, root)
    else:
        Log.info("[pathinput] working directory %s (project root)", root)
    return root


def clear_project_root() -> None:
    """Drop the pinned project root (tests, and any caller switching projects
    inside one process)."""
    set_project_root(None)


def project_root() -> Path:
    """**The** project root: the pinned root, else the working directory.

    The one owner of "which directory is this project?" for every layer --
    PathInput resolution here, config lookup in ``scidb.entities`` and
    ``scidb.schema_order``, MATLAB stub placement. The GUI decides its root
    (``scistack_gui.config.resolve_project_root``) and pins it here, so a GUI
    run resolves exactly where the canvas matched.

    It deliberately does NOT walk up looking for ``pyproject.toml`` or
    ``scistack.toml``. That walk made a stray config in a parent folder, or
    an unrelated ``pyproject.toml`` beneath the project, silently decide
    where every rootless PathInput resolved -- and each layer walked by a
    different rule, so they could disagree.
    """
    if _project_root_override is not None:
        return _project_root_override
    return Path.cwd().resolve()


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
                    a relative path, :func:`project_root` is used (the
                    pinned root, else the current working directory).
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
                delimiter-separated segments are unaffected. On an aliased
                key, the declared spellings also match (then canonicalize).

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
        *,
        name: str,
    ):
        self.path_template = path_template
        self.root_folder = Path(root_folder) if root_folder is not None else None
        self.regex = bool(regex)
        # The NAME is the PathInput's identity (2026-09-25): `to_key()` is the
        # name alone, so the template and root_folder — where the files are on
        # THIS machine — never reach a call id, an invocation id or a record
        # id. Moving the data, or running on another computer, changes nothing;
        # two differently named PathInputs never share an invocation. Required,
        # in scripts as in declarations, so there is no fallback to a path.
        self.name: str = validate_path_input_name(name)
        self.__name__ = f"PathInput({self.name!r})"
        # Numeric-fallback caches (see load()): learned zero-pad width per
        # placeholder key, and per-directory listings validated by mtime.
        self._pad_width: dict[str, int] = {}
        # Listing-cache counters, reported by discover(): how many directories
        # a walk actually read from disk vs. served from a still-valid listing.
        # The cache ITSELF is module-level (`_DIR_CACHE`) — see there.
        self._dir_cache_reads = 0
        self._dir_cache_hits = 0
        # Where the last discover() walk stopped; read by _log_zero_result.
        self._deepest_miss: "dict | None" = None
        self.aliases = aliases or {}
        self._alias_reverse = self._build_alias_reverse(self.aliases)
        self.key_regex = self._validate_key_regex(key_regex or {})
        for key in sorted(set(self.key_regex) & set(self._alias_reverse)):
            Log.info(
                "[pathinput] %r: {%s} matches pattern %r or the alias "
                "spelling(s) %s, then canonicalizes through the aliases",
                self.name,
                key,
                self.key_regex[key],
                sorted(self._alias_reverse[key]),
                layer="scifor",
            )

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
        """Validate ``key_regex``: every key a template placeholder, every
        pattern accepted by :func:`key_regex_pattern_error`."""
        placeholder_set = set(self.placeholder_keys())
        for key, pattern in key_regex.items():
            if key not in placeholder_set:
                raise ValueError(
                    f"PathInput key_regex key {key!r} is not a placeholder in "
                    f"template {self.path_template!r}"
                )
            error = key_regex_pattern_error(pattern)
            if error is not None:
                raise ValueError(f"PathInput key_regex for {key!r}: {error}")
        return dict(key_regex)

    def _key_pattern(self, key: str) -> str:
        """The regex fragment that captures an UNBOUND ``{key}``.

        ``key_regex[key]`` when declared, else the default ``[^/\\]+``. An
        aliased key with a declared pattern also accepts every declared
        spelling literally (2026-10-21, user decision): aliases ARE on-disk
        spellings, so a ``session = '\\d{2}'`` pattern must not drop a
        ``Baseline`` folder declared as an alias of ``BL``. The match is then
        canonicalized by the alias map, as before — regex first, alias second.
        """
        pattern = self.key_regex.get(key)
        if pattern is None:
            return r"[^/\\]+"
        spellings = sorted(self._alias_reverse.get(key, {}), key=lambda s: (-len(s), s))
        if not spellings:
            return pattern
        return "(?:" + "|".join([pattern, *(re.escape(s) for s in spellings)]) + ")"

    def to_key(self) -> str:
        """The IDENTITY of this PathInput: its name, as a JSON string.

        What version keys, call ids, invocation ids and the skip gate all
        hash. Deliberately path-free — see ``__init__``. The full spec is
        :meth:`to_spec`.
        """
        return path_input_key(self.name)

    def to_spec(self) -> str:
        """The full DESCRIPTION of this PathInput as a JSON string — name,
        template, root_folder and the matching options.

        Stored on the PathInput's provenance record so history can show and
        re-discover it, but never hashed: see :meth:`to_key`. ``regex``,
        ``aliases`` and ``key_regex`` are only included when non-default.
        """
        payload: dict = {
            "__type": "PathInput",
            "name": self.name,
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

    @classmethod
    def from_spec(cls, spec: "str | dict") -> "PathInput":
        """Rebuild a PathInput from :meth:`to_spec` — its exact inverse.

        The ONE reader of the spec. Every hand-off that crosses a boundary
        (the MATLAB bridge, both ways) ships ``to_spec()`` and rebuilds here,
        so a matching option cannot be dropped in transit. Before 2026-10-21
        the bridge copied name/template/root_folder/regex field by field and
        lost ``aliases`` and ``key_regex``: a ``Baseline`` folder holding
        ``*_BL_*`` files discovered 0 combos on a MATLAB run while a Python
        run of the same declaration found them all.

        An unknown field raises rather than being ignored, so a field added
        to :meth:`to_spec` without a reader here fails loudly.
        """
        payload = json.loads(spec) if isinstance(spec, str) else dict(spec)
        if not isinstance(payload, dict) or payload.get("__type") != "PathInput":
            raise ValueError(f"not a PathInput spec: {spec!r}")
        unknown = set(payload) - set(_SPEC_FIELDS)
        if unknown:
            raise ValueError(
                f"PathInput spec has unknown field(s) {sorted(unknown)}; "
                f"from_spec reads {sorted(_SPEC_FIELDS)}"
            )
        return cls(
            payload["template"],
            root_folder=payload.get("root_folder"),
            regex=bool(payload.get("regex", False)),
            aliases=payload.get("aliases"),
            key_regex=payload.get("key_regex"),
            name=payload["name"],
        )

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
        report: str = "all",
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
            report: ``"all"`` (default) reports every bridged spelling;
                ``"numeric"`` reports only numeric-equivalence bridges
                (``1`` -> ``"001"``), leaving out alias bridges. This module
                owns which kind a bridge was (numeric takes precedence over
                an alias for the same key, see ``_fallback_segment_regex``),
                so scidb's zero-padding policy asks for ``"numeric"`` rather
                than guessing: an alias spelling is a DECLARED equivalence,
                never an ambiguous one.
        """
        if report not in ("all", "numeric"):
            raise ValueError(f"report must be 'all' or 'numeric', got {report!r}")
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
                    k: cap
                    for k, cap in bindings.items()
                    if cap != str(metadata[k])
                    and (report == "all" or k in numeric_keys)
                }
                Log.debug(
                    "pathinput_fallback: matched %s (captures: %s, numeric "
                    "keys: %s, alias keys: %s, report=%s)",
                    path,
                    bindings,
                    sorted(numeric_keys),
                    sorted(k for k in alias_keys if k not in numeric_keys),
                    report,
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
                project_root() / dir_part if dir_part else project_root()
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
            return (project_root() / resolved_path).resolve()
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
        return project_root(), segments

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
        cached = _DIR_CACHE.get(key)
        if cached is not None and cached[0] == mtime:
            self._dir_cache_hits += 1
            return cached[1]
        try:
            entries = sorted(os.listdir(directory))
        except OSError:
            return []
        _DIR_CACHE[key] = (mtime, entries)
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
        self._deepest_miss = None
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
        if not results:
            self._log_zero_result(root, segments)
        return results

    def _log_zero_result(self, root: Path, segments: list[str]) -> None:
        """WARN where a zero-combo walk stopped and what it saw there.

        A bare ``0 combo(s)`` cannot tell a wrong root from a typo'd folder
        from a filename the pattern does not fit (2026-10-21: an XSENS run
        found nothing and the log could not say why).
        """
        miss = self._deepest_miss
        if miss is None:
            Log.warn(
                "pathinput_discover found nothing for %r under %s, and no "
                "directory was listed — does the root exist?",
                self.name,
                root,
                layer="scifor",
            )
            return
        Log.warn(
            "pathinput_discover found nothing for %r: the walk got as far as "
            "segment %d of %d (%r) in %d director(ies), e.g. %s; pattern %s "
            "matched none of its %d entr(ies), e.g. %s",
            self.name,
            miss["seg_idx"] + 1,
            len(segments),
            segments[miss["seg_idx"]],
            miss["dirs"],
            miss["directory"],
            miss["pattern"],
            miss["n_rejected"],
            miss["rejected"] or "(directory empty or missing)",
            layer="scifor",
        )

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
                    self._note_miss(seg_idx, current_dir, segment, [segment])
            else:
                if candidate.is_dir():
                    self._walk(
                        candidate, segments, seg_idx + 1, bindings, results, enforce_kind
                    )
                else:
                    self._note_miss(seg_idx, current_dir, segment, [segment])
            return

        # Segment has placeholder(s) — build a regex. Keys an earlier segment
        # already bound are matched literally (see _segment_to_regex).
        pattern = self._segment_to_regex(segment, bindings)

        # Through the mtime-validated listing cache rather than a bare
        # os.listdir. This walk runs on every discovery, and every discovery on
        # a UNC share pays one network round-trip per directory; on 2026-09-13 a
        # second Schema Key Locations open re-walked the same unchanged share
        # and blocked for minutes with nothing logged (the listing had gone cold
        # in the SMB client). `_list_dir` already existed for the numeric
        # fallback path; this was the one caller still going around it.
        entries = self._list_dir(current_dir)
        if not entries:
            self._note_miss(seg_idx, current_dir, pattern, [])
            return

        # Entries that did not survive this segment, for the zero-result
        # report in discover() — the only way to see WHY a walk found nothing.
        rejected: list[str] = []
        for entry in entries:
            m = re.fullmatch(pattern, entry)
            if m is None:
                rejected.append(entry)
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
                rejected.append(f"{entry} (conflicts with {bindings})")
                continue

            new_bindings = {**bindings, **clean_captured}
            entry_path = current_dir / entry

            if is_last:
                if not enforce_kind or _matches_kind(entry_path, _expects_file(segment)):
                    results.append(dict(new_bindings))
                else:
                    rejected.append(f"{entry} (wrong kind: file vs directory)")
            else:
                if entry_path.is_dir():
                    self._walk(
                        entry_path, segments, seg_idx + 1, new_bindings, results, enforce_kind
                    )
                else:
                    rejected.append(f"{entry} (not a directory)")
        if len(rejected) == len(entries):
            self._note_miss(seg_idx, current_dir, pattern, rejected)

    def _note_miss(
        self, seg_idx: int, directory: Path, pattern: str, rejected: list[str]
    ) -> None:
        """Remember where a walk stopped, keeping the DEEPEST segment reached
        — that is the one whose pattern disagrees with the files on disk."""
        miss = self._deepest_miss
        if miss is None or seg_idx > miss["seg_idx"]:
            self._deepest_miss = {
                "seg_idx": seg_idx,
                "directory": str(directory),
                "pattern": pattern,
                "dirs": 1,
                "rejected": rejected[:8],
                "n_rejected": len(rejected),
            }
        elif seg_idx == miss["seg_idx"]:
            miss["dirs"] += 1

    def _segment_to_regex(
        self, segment: str, bound: "dict[str, str] | None" = None
    ) -> str:
        """Convert a template segment like ``{subject}_XSENS_{session}`` to a regex.

        A placeholder declared in ``key_regex`` uses its custom pattern
        instead of the default greedy ``[^/\\]+`` — needed to disambiguate
        placeholders that are adjacent with no delimiter between them (see
        the ``key_regex`` docstring on ``__init__``).

        A placeholder already in *bound* (captured by an earlier segment) is
        matched as that value's literal spelling(s), not re-captured. Before
        2026-10-21 it was re-captured and compared afterwards, and a greedy
        capture loses that comparison whenever a value holds the delimiter:
        folder ``SS01``/``Pre_Op`` with file ``SS01_Pre_Op_x.xlsx`` captured
        subject ``SS01_Pre``, conflicted with the folder, and the file was
        silently dropped. An aliased key accepts every spelling of its
        canonical, since the folder and the file may spell it differently.
        """
        bound = bound or {}
        parts = _string.Formatter().parse(segment)
        regex = ""
        key_counts: dict[str, int] = {}
        for literal, field_name, _, _ in parts:
            if literal:
                regex += re.escape(literal)
            if field_name is not None and field_name in bound:
                canonical = bound[field_name]
                spellings = [canonical] + [
                    s
                    for s, c in self._alias_reverse.get(field_name, {}).items()
                    if c == canonical and s != canonical
                ]
                regex += "(?:" + "|".join(re.escape(s) for s in spellings) + ")"
                continue
            if field_name is not None:
                # Handle duplicate keys in the same segment by numbering
                key_counts[field_name] = key_counts.get(field_name, 0) + 1
                count = key_counts[field_name]
                group_name = field_name if count == 1 else f"{field_name}_{count}"
                regex += f"(?P<{group_name}>{self._key_pattern(field_name)})"
        return regex

    def __repr__(self) -> str:
        return (
            f"PathInput({self.path_template!r}, root_folder={self.root_folder!r}, "
            f"name={self.name!r})"
        )
