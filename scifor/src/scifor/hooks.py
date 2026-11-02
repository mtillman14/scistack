"""``LoopHooks``: how a layer above ``scifor.for_each`` drives its loop.

``scifor.for_each`` is a pure loop over plain tables. A caller that knows
more than the tables say (scidb knows which records each combination reads,
how a variable is stored, and which combinations already exist) passes that
knowledge in through ONE object instead of eight private keyword arguments.
Users of standalone scifor never need it.

Every field defaults to "no opinion", so ``LoopHooks()`` behaves exactly like
passing no hooks at all.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from .pathinput import PathInput


@dataclass(frozen=True)
class LoopHooks:
    """Caller-supplied knowledge for one ``scifor.for_each`` run.

    Attributes:
        all_combos: The combinations to run, as ``{key: value}`` dicts,
            already decided by the caller. Replaces the Cartesian product of
            the metadata iterables (and the empty-list resolution and
            PathInput discovery that feed it). scidb passes the combinations
            that exist in the database, after applying the database's
            schema-key types itself; ``locations=`` still narrows them.
        path_input_resolver: ``(pathinput, metadata) -> path`` override for
            resolving a ``PathInput`` per combination. scidb uses it to
            enforce the database's schema-key types (a string key matches
            exactly; an undeclared key that needs zero-padding raises).
            Default: ``pathinput.load(**metadata)``, or the exact-match
            resolver when ``schema_key_types=`` declares a string key.
        mapping_inputs: ``{input_name: [data column names]}`` naming inputs
            whose rows are records of a *mapping-valued* variable (each row
            one dict, spread one column per key) rather than rows of a
            table. A single row of such an input reaches the function as
            that dict (a struct in MATLAB), not as a one-row frame. scifor
            cannot infer this: "one row, N data columns" is also exactly
            what a table-valued variable looks like, so the layer that knows
            the storage layout says so (scidb's ``_resolve_mapping_inputs``).
            Ignored for an input passed through ``as_table`` or with a column
            selection.
        select_rows: ``(input_name, frame, metadata) -> frame``, called on
            every per-combination DataFrame input AFTER scifor's own
            schema-key filter and BEFORE extraction / column selection. A
            caller that knows which rows a combination reads more precisely
            than the schema keys say narrows the frame here, and drops any
            bookkeeping column of its own before the function sees it
            (scidb: its record-id column, matched against the combination's
            ``Selection``). A frame the caller has nothing to say about must
            be returned unchanged.
        keep_no_output: Leave ``NoOutput`` slots (an output with nothing to
            save at a combination whose other outputs were kept) in the
            returned table, for a caller that saves per output and skips
            them (scidb). Default: each becomes ``None``, a missing cell.
        progress_fn: Receives one dict per loop event: ``combo_start``,
            ``combo_done``, ``combo_skip`` (with a small ``failure``),
            ``cancelled``, and a final ``summary`` carrying the counts and
            the failure groups. The GUI's progress bar and scidb's run
            summary read these.
        cancel_check: Called before each combination (and between EachOf
            alternatives); returning True stops the run cooperatively. The
            combinations already finished are still returned.
    """

    all_combos: "list[dict] | None" = None
    path_input_resolver: "Callable[[PathInput, dict], Any] | None" = None
    mapping_inputs: "dict[str, list[str]] | None" = None
    select_rows: "Callable[[str, pd.DataFrame, dict], pd.DataFrame] | None" = None
    keep_no_output: bool = False
    progress_fn: "Callable[[dict], None] | None" = None
    cancel_check: "Callable[[], bool] | None" = None

    def cancelled(self) -> bool:
        """Has the caller asked the run to stop?"""
        return self.cancel_check is not None and bool(self.cancel_check())

    def emit(self, event: dict) -> None:
        """Send one loop event to ``progress_fn`` (no-op without one)."""
        if self.progress_fn is not None:
            self.progress_fn(event)
