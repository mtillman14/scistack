"""Which canvas node a recorded wiring belongs to (D-2026-09-22-2).

The rule is ``docs/claude/node-identity.md`` §7; ``scistack_gui/node_wiring.py``
is the storage; this module is the decision, kept pure so it can be tested
without a database.

---

## The question

A graph build reads history and finds a set of wirings — ``(fn_name,
wiring_id)`` pairs, each a distinct shape that function has run in. It has to
turn each one into a **canvas node id**.

Until 2026-09-22 that was a string format: ``fn__{fn}__{wiring_id}``. The id
was therefore a function of the *recorded* bindings, so recording one more
binding renamed the node — and a run through an edge the user drew does
exactly that. Same picture, two ids, and every statement keyed by the old one
stranded.

## The rule, in one pass

For each wiring, in this order:

1. **Already associated** — ``_node_wiring`` says a node has run as it. Use
   that node. This covers the normal case completely, because a GUI-started
   run records the association AT DISPATCH: the GUI already holds the node id
   and there is nothing to infer (``node-identity.md`` §7a).
2. **Stated by exactly one existing node** — some node already on the canvas,
   with its drawn edges folded into its current wiring, *states* this wiring.
   That node ran as it; attribute. This is what stops the duplicate forming,
   and it does so from the STATED wiring — the thing the user controls —
   rather than by a repair applied afterwards. It is also what covers a script
   or terminal run, which carries no node id.
3. **Stated by several** — §7b. Prefer a node that has already run as it, else
   the oldest, and **tell the user**: two nodes stating identical wiring
   compute identical things, so the state is almost certainly unintended, and
   it is recoverable (rewire one). Never auto-merged — silently collapsing two
   nodes a person created is a worse failure than a message they can act on.
4. **Nobody** — allocate. ``node_wiring.mint_node_id``.

**One pass, no iteration, no ordering.** A node can only state a wiring if it
already exists, and a node exists exactly when ``_node_wiring`` knows it — so
rule 2's candidates are read from the table, not from the assignment being
built. Nothing this pass mints can claim anything in the same pass, because a
node minted for an unclaimed wiring has, by construction, no edges drawn onto
an id that did not exist a moment ago.

That is why there is no "mint the oldest first" step and no chronology here.
An earlier draft minted one wiring at a time so that a freshly minted node
could absorb a wiring in the next round — which was only ever needed to fold
in a duplicate that had formed *before* this change. Folding those in is a
migration, the project takes clean breaks instead (D-2026-09-22-4), and
removing it removed the need for ordering, for iteration, and for the
``first_saved`` field it had been reading out of scidb.

## What a node IS, versus what it HAS RUN AS

Attribution gives a node a growing list of wirings. That list is **history**.
The node's shape on the canvas is its *current* wiring — the latest one it ran
as, plus any edge drawn since. ``node_wiring.current_wiring`` is that; the list
is ``node_wiring.wirings_for_node``. Handles, edges and the Run button follow
the first; attribution and the Variants view follow the second.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from scistack_gui.ids import ROOT_SCOPE, BareNodeId

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Ambiguity:
    """Two or more nodes in one scope state the same wiring.

    Carried out of the resolution rather than logged and forgotten: it is a
    **popup**, not a log line (user decision 2026-09-22). A warning buried in
    ``scidb.log`` is a warning nobody reads, and this one is both surprising
    (two nodes computing identical things) and recoverable (rewire one).
    """

    function_name: str
    wiring_id: str
    chosen: str
    others: tuple[str, ...]
    scope: str = ROOT_SCOPE

    @property
    def signature(self) -> str:
        """A stable key for "this same ambiguity", so it can be reported once
        rather than on every build."""
        return f"{self.function_name}:{self.wiring_id}:" + ",".join(
            sorted((self.chosen, *self.others))
        )

    def message(self) -> str:
        names = ", ".join(sorted((self.chosen, *self.others)))
        return (
            f"Two or more '{self.function_name}' nodes are wired identically "
            f"({names}). They compute the same thing, so runs will be "
            f"attributed to {self.chosen} until they differ. Rewire one of "
            f"them — or hide it — to resolve this. Nothing has been merged."
        )


@dataclass
class IdentityPlan:
    """The answer, plus everything the caller has to act on."""

    #: ``{(fn_name, wiring_id): node_id}`` — every wiring in history.
    node_by_wiring: dict[tuple[str, str], BareNodeId] = field(default_factory=dict)
    #: New ``_node_wiring`` rows the caller must persist, as
    #: ``(node_id, wiring_id, scope)``. Written by the caller rather than
    #: here so this module stays pure and testable without a database.
    to_record: list[tuple[str, str, str]] = field(default_factory=list)
    #: Nodes minted in this pass — the ones that had no claimant at all.
    minted: list[tuple[str, str]] = field(default_factory=list)
    ambiguities: list[Ambiguity] = field(default_factory=list)
    #: ``{(fn_name, node token): wiring}`` — the shape each node currently HAS,
    #: as against every shape it has run as. The canvas draws this one.
    current_by_token: dict[tuple[str, str], str] = field(default_factory=dict)

    def token(self, fn_name: str, wiring: str) -> str:
        """The node-id SUFFIX for a wiring — what the grouped graph is keyed
        by and what a node id's trailing segment means after this change.

        A wiring no node has claimed is its own token. Under allocated ids
        that can never collide with a real node id, so it reads as "no node",
        which is what every caller already does with it.
        """
        node_id = self.node_by_wiring.get((fn_name, wiring))
        if node_id is None:
            return wiring
        return _suffix_of(node_id, fn_name)

    def is_current(self, fn_name: str, wiring: str) -> bool:
        """Whether *wiring* is the shape its node currently has, rather than
        one it merely used to run as.

        The graph build takes a node's handles, edges and constants from its
        CURRENT wiring only. Taking the union of everything it has ever run as
        would draw handles for a shape the user rewired away from, which is a
        different way of showing them a node that is not the node they see.
        """
        token = self.token(fn_name, wiring)
        return self.current_by_token.get((fn_name, token), wiring) == wiring

    def wirings_of(self, fn_name: str, token: str) -> set[str]:
        """Every wiring the node with this suffix stands for — its history."""
        return {
            wiring
            for (fn, wiring), node_id in self.node_by_wiring.items()
            if fn == fn_name and _suffix_of(node_id, fn_name) == token
        } or {token}


def _suffix_of(node_id: str, fn_name: str) -> str:
    """``fn__bandpass__abc`` → ``abc``, for the node id of *fn_name*."""
    prefix = f"fn__{fn_name}__"
    return node_id[len(prefix) :] if node_id.startswith(prefix) else node_id


def identity_token(fn_name: str, wiring: str) -> str:
    """The no-database default: the node token IS the wiring.

    Every ``token_for=`` parameter in ``graph_builder`` defaults to this, so a
    caller (or a test) that knows nothing about node allocation behaves
    exactly as the module did before allocation existed.
    """
    return wiring


def resolve_identities(
    history_wirings,
    *,
    associations,
    stated_by=None,
    current_by_node=None,
    scope_of=None,
    mint=None,
) -> IdentityPlan:
    """Assign a node id to every wiring in history. ONE pass.

    Args:
        history_wirings: iterable of ``(fn_name, wiring_id)`` — every shape
            the database has recorded.
        associations: ``[{"node_id", "wiring_id", "first_seen", ...}]`` from
            ``node_wiring.associations``, oldest first. ``first_seen`` doubles
            as node age for the §7b tie-break.
        stated_by: ``{(fn_name, stated_wiring): [node_id, ...]}`` — which
            EXISTING nodes state which wiring, once their drawn edges are
            folded in. Computed by the caller from the association table and
            the manual edges; nothing minted in this pass can appear in it,
            because a node that did not exist a moment ago has no edges drawn
            onto its id.
        current_by_node: ``{node_id: its current wiring}`` from
            ``node_wiring.current_wiring_by_node``. A wiring attributed in this
            pass supersedes it — that IS the node's newest shape.
        scope_of: ``node_id -> scope``, for the ambiguity report. Defaults to
            the root scope, which is where DB-derived nodes live until a
            position says otherwise.
        mint: ``(fn_name, taken) -> node_id``. Defaults to
            ``node_wiring.mint_node_id``.

    Returns an :class:`IdentityPlan`. **Nothing is written** — the caller
    persists ``to_record`` — so this function is a pure decision and can be
    exercised against hand-built inputs.
    """
    from scistack_gui.node_wiring import mint_node_id

    mint = mint or mint_node_id
    stated_by = stated_by or {}
    current_by_node = current_by_node or {}
    scope_of = scope_of or (lambda node_id: ROOT_SCOPE)

    plan = IdentityPlan()
    taken: set[str] = set()

    by_wiring: dict[str, list[str]] = {}
    #: ``node_id -> (first_seen, node_id)`` — how old each node is, for the
    #: §7b tie-break. ``associations`` arrives oldest first.
    age: dict[str, tuple] = {}
    for row in associations:
        by_wiring.setdefault(row["wiring_id"], []).append(row["node_id"])
        taken.add(row["node_id"])
        key = (str(row.get("first_seen") or "") or "9999", row["node_id"])
        if row["node_id"] not in age or key < age[row["node_id"]]:
            age[row["node_id"]] = key

    for key in sorted(history_wirings):
        fn_name, wiring = key
        # A node id names its function, so an association recorded under
        # another function's node — a wiring reused after a rename — must not
        # capture this one. No `or claimants` fallback: if the filter empties
        # the list, there genuinely is no claimant for THIS function and the
        # next rule should get its turn.
        claimants = [
            n for n in (by_wiring.get(wiring) or []) if n.startswith(f"fn__{fn_name}__")
        ]
        source = "recorded"
        if not claimants:
            claimants = [n for n in dict.fromkeys(stated_by.get(key) or ()) if n]
            source = "stated"
        if not claimants:
            node_id = BareNodeId(mint(fn_name, taken))
            taken.add(str(node_id))
            plan.node_by_wiring[key] = node_id
            plan.to_record.append((str(node_id), wiring, ROOT_SCOPE))
            plan.minted.append((str(node_id), wiring))
            continue

        chosen = _prefer(claimants, by_wiring.get(wiring) or [], age)
        plan.node_by_wiring[key] = BareNodeId(chosen)
        if source == "stated":
            plan.to_record.append((chosen, wiring, scope_of(chosen)))
            logger.info(
                "[node_identity] wiring %s of '%s' attributed to %s — that node "
                "STATES it (%d candidate(s))",
                wiring,
                fn_name,
                chosen,
                len(claimants),
            )
        if len(claimants) > 1:
            plan.ambiguities.append(
                Ambiguity(
                    function_name=fn_name,
                    wiring_id=wiring,
                    chosen=chosen,
                    others=tuple(n for n in claimants if n != chosen),
                    scope=scope_of(chosen),
                )
            )

    # --- which shape each node currently HAS --------------------------------
    # The table's answer, except where this pass has just attributed a newer
    # one: a wiring attributed or minted in this pass is the shape the node has
    # only now been seen running as, and it is the shape the canvas must draw.
    fn_of_node = {
        str(node_id): fn_name for (fn_name, _w), node_id in plan.node_by_wiring.items()
    }
    current: dict[str, str] = {
        node_id: wiring
        for node_id, wiring in current_by_node.items()
        if node_id in fn_of_node
    }
    for node_id, wiring, _scope in plan.to_record:
        current[node_id] = wiring
    for node_id, wiring in current.items():
        fn_name = fn_of_node[node_id]
        plan.current_by_token[(fn_name, _suffix_of(node_id, fn_name))] = wiring

    if plan.minted:
        logger.info(
            "[node_identity] allocated %d node id(s): %s",
            len(plan.minted),
            plan.minted,
        )
    return plan


def _prefer(candidates: list[str], already_ran: list[str], age: dict[str, tuple]) -> str:
    """§7b, in order: a node that has already run as this wiring, else the
    **oldest**.

    Age is the node's earliest ``first_seen`` in ``_node_wiring`` — when it
    first ran as anything. The node id breaks a tie, so attribution is
    deterministic and does not move between rebuilds, which is what keeps a
    duplicated-wiring canvas stable rather than flickering between two nodes.
    """
    for node_id in already_ran:
        if node_id in candidates:
            return node_id
    return min(candidates, key=lambda n: age.get(n, ("9999", n)))
