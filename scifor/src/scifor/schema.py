"""Module-level schema key registry for scifor, and the schema key TREE.

Two things live here, and they have different jobs
(docs/claude/branched-schema-keys.md, plan-branched-schema-keys.md):

* the flat ordered key list (``get_schema``) owns key ORDER: column order,
  sort order, default nesting of a plot axis;
* :class:`SchemaTree` owns PARENTS: ancestry, coherence, the default level,
  and which level checkboxes to offer.

The one idea: **the tree is a default, the key set is the truth.** A record's
location is the set of keys it names, and any set is legal. The tree only picks
sensible defaults and what to offer first; it never refuses anything. A plain
list of keys is a tree with one branch (each key's parent is the previous
key), which is exactly the behaviour before trees existed.

Everything in :class:`SchemaTree` is pure: no I/O, no globals.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from scistacklog import Log

#: Reasons are data, so the GUI, the docs and the tests all say the same words.
REASON_OTHER_BRANCH = "{key} is on a different branch from {ticked}"


@dataclass(frozen=True)
class Offered:
    """What a level picker shows for a set of bound inputs.

    ``keys``      the keys to offer a checkbox for, in dataset order.
    ``disabled``  offered keys whose tick would make the current selection
                  incoherent, mapped to the reason (a tooltip).
    """

    keys: tuple[str, ...]
    disabled: dict[str, str]


@dataclass(frozen=True)
class SchemaTree:
    """The dataset's keys and each key's parent.

    ``keys`` is the flat key order (first appearance across the declared
    branches, so a parent always precedes its children). ``parents[i]`` is the
    parent of ``keys[i]``, or None for a root. Build one with
    :meth:`from_paths` or :meth:`linear`.
    """

    keys: tuple[str, ...]
    parents: tuple[str | None, ...]

    # --- construction ------------------------------------------------------

    @classmethod
    def from_paths(cls, paths: Sequence) -> SchemaTree:
        """Build from one path per branch, or from a plain list of keys.

        ``["a", "b"]`` is one branch. ``[["a", "b"], ["a", "c"]]`` is two.
        Refused, naming the key: an empty path, a key repeated inside one
        path, and a key with two different parents across paths.
        """
        if not paths:
            return cls((), ())
        if all(isinstance(p, str) for p in paths):
            paths = [list(paths)]
        elif any(isinstance(p, str) for p in paths):
            raise ValueError(
                "Schema keys must be a list of key names (one branch) or a list "
                f"of lists (one per branch), not a mix: {list(paths)!r}"
            )
        order: list[str] = []
        parent_of: dict[str, str | None] = {}
        for path in paths:
            path = list(path)
            if not path:
                raise ValueError("A schema key branch cannot be empty.")
            seen: set[str] = set()
            previous: str | None = None
            for key in path:
                if not isinstance(key, str) or not key:
                    raise ValueError(f"Schema key names must be non-empty strings, got {key!r}.")
                if key in seen:
                    raise ValueError(f"Schema key {key!r} is repeated within the branch {path}.")
                seen.add(key)
                if key in parent_of and parent_of[key] != previous:
                    raise ValueError(
                        f"Schema key {key!r} has two different parents: "
                        f"{parent_of[key]!r} and {previous!r}. A key sits under one "
                        "parent; a record that names keys from two branches covers "
                        "the cross-branch case."
                    )
                if key not in parent_of:
                    parent_of[key] = previous
                    order.append(key)
                previous = key
        return cls(tuple(order), tuple(parent_of[k] for k in order))

    @classmethod
    def from_parents(cls, keys: Sequence[str], parent_of: dict) -> SchemaTree:
        """Build from the flat key order and each key's parent (None = root).

        The inverse of (``keys``, ``parent_of``), used where the order is
        already fixed (a stored database, a bundle manifest). Refuses a parent
        that is not a key or that comes AFTER its child.
        """
        keys = list(keys)
        if len(set(keys)) != len(keys):
            raise ValueError(f"Schema keys must be unique: {keys}")
        extra = set(parent_of) - set(keys)
        if extra:
            raise ValueError(f"Parents given for unknown schema key(s): {sorted(extra)}")
        parents: list[str | None] = []
        for i, key in enumerate(keys):
            parent = parent_of.get(key)
            if parent is not None and parent not in keys[:i]:
                raise ValueError(
                    f"Schema key {key!r} has parent {parent!r}, which is not an "
                    f"earlier schema key (parents must come before children): {keys}"
                )
            parents.append(parent)
        return cls(tuple(keys), tuple(parents))

    @classmethod
    def linear(cls, keys: Sequence[str]) -> SchemaTree:
        """One branch: each key's parent is the previous key."""
        return cls.from_paths(list(keys))

    # --- reading the tree --------------------------------------------------

    @property
    def parent_of(self) -> dict[str, str | None]:
        return dict(zip(self.keys, self.parents, strict=True))

    @property
    def is_linear(self) -> bool:
        return all(len(self.children(k)) <= 1 for k in self.keys) and (
            sum(p is None for p in self.parents) <= 1
        )

    def paths(self) -> list[list[str]]:
        """One root-to-leaf path per leaf: the declaration form, round-trippable."""
        return [self.chain(k) for k in self.keys if not self.children(k)]

    def ancestors(self, key: str) -> list[str]:
        """The keys above *key*, root first."""
        self._require(key)
        parent_of = self.parent_of
        above: list[str] = []
        current = parent_of[key]
        while current is not None:
            above.append(current)
            current = parent_of[current]
        return above[::-1]

    def chain(self, key: str) -> list[str]:
        """*key* and its ancestors, root first."""
        return [*self.ancestors(key), key]

    def children(self, key: str) -> list[str]:
        self._require(key)
        return [k for k, p in zip(self.keys, self.parents, strict=True) if p == key]

    def descendants(self, key: str) -> list[str]:
        """Every key below *key*, in dataset order."""
        self._require(key)
        below: set[str] = set()
        for k in self.keys:
            if key in self.ancestors(k):
                below.add(k)
        return [k for k in self.keys if k in below]

    # --- key sets ----------------------------------------------------------

    def leaves(self, key_set: Iterable[str]) -> list[str]:
        """The members of *key_set* with no descendant in it, in dataset order."""
        members = self._members(key_set)
        below = {a for k in members for a in self.ancestors(k)}
        return [k for k in self.keys if k in members and k not in below]

    def is_chain(self, key_set: Iterable[str]) -> bool:
        """True iff the set lies on one root-to-leaf chain of the tree."""
        members = self._members(key_set)
        leaves = self.leaves(members)
        if not leaves:
            return True
        return len(leaves) == 1 and members <= set(self.chain(leaves[0]))

    def coherent(self, key_set: Iterable[str], input_sets: Iterable[Iterable[str]] = ()) -> bool:
        """On one chain, OR carried whole by some bound input (the data shows
        the combination exists)."""
        members = self._members(key_set)
        if self.is_chain(members):
            return True
        return any(members <= self._members(s) for s in input_sets)

    def default_level(self, input_sets: Iterable[Iterable[str]]) -> tuple[str, ...]:
        """The keys a run over these inputs iterates by default.

        The union of the inputs' key sets when that union is coherent;
        otherwise the keys EVERY input carries. Dataset order.
        """
        # An input that names no key (dataset-level) broadcasts anywhere: it
        # adds nothing to the union and must not narrow the intersection.
        sets = [s for s in (self._members(s) for s in input_sets) if s]
        if not sets:
            return ()
        union = set().union(*sets)
        if self.coherent(union, sets):
            chosen = union
        else:
            chosen = set.intersection(*sets)
        return tuple(k for k in self.keys if k in chosen)

    def offered(
        self, input_sets: Iterable[Iterable[str]], ticked: Iterable[str] = ()
    ) -> Offered:
        """Which level checkboxes to show, and which of those to disable.

        A key is offered iff adding it to each input's key set stays
        coherent. An offered, unticked key is disabled when ticking it would
        make the current selection incoherent.
        """
        sets = [self._members(s) for s in input_sets]
        selection = self._members(ticked)
        offered = tuple(
            k for k in self.keys if all(self.coherent(s | {k}, sets) for s in sets)
        )
        disabled: dict[str, str] = {}
        for k in offered:
            if k in selection or self.coherent(selection | {k}, sets):
                continue
            clash = [t for t in self.keys if t in selection and not self.coherent({t, k}, sets)]
            disabled[k] = REASON_OTHER_BRANCH.format(key=k, ticked=", ".join(clash or sorted(selection)))
        return Offered(offered, disabled)

    # --- internals ---------------------------------------------------------

    def _require(self, key: str) -> None:
        if key not in self.keys:
            raise ValueError(f"{key!r} is not a schema key (schema keys: {list(self.keys)}).")

    def _members(self, key_set: Iterable[str]) -> set[str]:
        members = set(key_set)
        for k in members:
            self._require(k)
        return members

    def describe(self) -> str:
        parents = ", ".join(f"{k}<-{p}" for k, p in zip(self.keys, self.parents, strict=True) if p)
        shape = "linear" if self.is_linear else "branched"
        return f"keys={list(self.keys)}, parents=[{parents}], {shape}"



def as_tree(declaration) -> SchemaTree:
    """A SchemaTree from any declaration: a tree (returned as is), a plain
    list of keys (one branch) or one list per branch."""
    if isinstance(declaration, SchemaTree):
        return declaration
    return SchemaTree.from_paths(declaration)

_schema_keys: list[str] = []
_schema_tree: SchemaTree = SchemaTree((), ())


def set_schema(keys_or_paths: Sequence) -> None:
    """Set the global schema keys.

    Args:
        keys_or_paths: Either an ordered list of key names (one branch, e.g.
            ``["subject", "session"]``) or one list per branch
            (``[["brand", "category"], ["brand", "location"]]``).
            A ready-made :class:`SchemaTree` is also accepted.

    Called automatically by ``scidb.configure_database()`` as a side effect.
    Standalone users call this once before using ``for_each``.
    """
    global _schema_keys, _schema_tree
    tree = as_tree(keys_or_paths)
    changed = tree != _schema_tree
    _schema_tree = tree
    _schema_keys = list(tree.keys)
    # for_each re-sets the schema on every call; say so only when it changed.
    if changed and tree.keys:
        Log.info(f"schema tree set: {tree.describe()}", layer="scifor")


def get_schema() -> list[str]:
    """Return a copy of the flat, ordered schema key list."""
    return list(_schema_keys)


def get_schema_tree() -> SchemaTree:
    """The schema tree (immutable, so no copy)."""
    return _schema_tree


def tree_for(keys: Sequence[str]) -> SchemaTree:
    """The tree for a key list a caller holds.

    The registered tree when *keys* is the registered schema; otherwise one
    branch (a list with no known parents behaves as it always did: each key
    under the one before). The single place that choice is made.
    """
    keys = list(keys)
    if list(_schema_tree.keys) == keys:
        return _schema_tree
    return SchemaTree.linear(keys)


def expand_schema_keys(schema_keys: list[str], metadata_iterables: dict) -> dict:
    """Seed ``metadata_iterables`` with an empty list for each requested
    schema key, structural sugar for "iterate over these keys" without
    spelling out ``key=[]`` by hand.

    This is pure bookkeeping — it does no I/O. Actual value resolution for
    the ``[]`` placeholders happens downstream, wherever the caller's own
    empty-list resolver lives (scifor's own DataFrame scan for standalone
    use; scidb's database query when DB-backed). Both scifor.for_each() and
    scidb.for_each() call this same function so the "which keys to iterate"
    bookkeeping is written once and shared.

    Mutually exclusive with an already-populated ``metadata_iterables``:
    callers pass either schema_keys= or explicit **metadata_iterables, not
    both, in the same call.

    Args:
        schema_keys: Schema key names to iterate.
        metadata_iterables: The caller's existing metadata_iterables dict.
                    Must be empty when schema_keys is used.

    Returns:
        A new dict with one ``key: []`` entry per schema key.
    """
    if metadata_iterables:
        raise ValueError(
            "Cannot use both schema_keys and **metadata_iterables. "
            "Use schema_keys for automatic iteration, or **metadata_iterables "
            "for manual control."
        )
    return {key: [] for key in schema_keys}


# --- the one-text-box declaration ------------------------------------------
#
# Places that only have one text box (the VS Code "Create database" prompt,
# the GUI bootstrap wizard, ``--schema-keys``) write a branched schema as
# commas within a branch and semicolons between branches:
#
#     brand, category, item; brand, location, person
#
# No semicolon is one branch, so the text people type today is unchanged. This
# is only the SPLIT; every rule (two parents, repeated key, empty branch) stays
# in ``SchemaTree.from_paths``, which the caller runs on the result.


def parse_schema_text(text: str) -> list:
    """``"a, b; a, c"`` -> ``[["a", "b"], ["a", "c"]]``; ``"a, b"`` -> ``["a", "b"]``.

    Blank keys are dropped, so a trailing comma is harmless. A branch that is
    empty after that (``"a; ;b"``) is kept as ``[]`` so ``from_paths`` can refuse
    it by name rather than it vanishing silently. An empty text is ``[]``.
    """
    if not text or not text.strip():
        return []
    branches = [[k.strip() for k in part.split(",") if k.strip()] for part in text.split(";")]
    if len(branches) == 1:
        return branches[0]
    return branches


def format_schema_text(declaration) -> str:
    """The text form of any declaration (the inverse of :func:`parse_schema_text`)."""
    tree = as_tree(declaration)
    paths = tree.paths()
    return "; ".join(", ".join(path) for path in paths)
