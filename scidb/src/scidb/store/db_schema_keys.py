"""DatabaseManager: the dataset's schema keys — their types, order, aliases and colours, metadata canonicalisation, and the distinct values stored.

Moved verbatim out of database.py (grug follow-up): DatabaseManager inherits
this mixin, so every method is still `db.<name>`. Each method's `self` is
annotated as DatabaseManager, so the IDE knows its attributes.
"""

from typing import TYPE_CHECKING

import pandas as pd
import scifor
from importlib import metadata
from scicanonicalhash import datetime_key_text
from scidb.model.log import Log
from scidb.store import schema_types
from scidb.store.db_helpers import _declared_rank_column
from scidb.store.schema_values import canonical_numeric_value
from scidb.store.schema_values import from_datetime_key_text
from scidb.store.schema_values import from_schema_str

if TYPE_CHECKING:
    from scidb.store.database import DatabaseManager


# Hoisted from function bodies (grug follow-up): no cycle, and already
# loaded by `import scidb`, so this changes neither import order risk nor cost.
from scifor.project import aliases as _aliases
from scifor.project import colors as _colors
from scifor.project import schema_order as _schema_order
from scifor.project.schema_order import order_levels
class SchemaKeysMixin:
    """DatabaseManager: the dataset's schema keys — their types, order, aliases and colours, metadata canonicalisation, and the distinct values stored."""

    @property
    def dataset_schema_key_types(self: "DatabaseManager") -> dict[str, str]:
        """Declared schema-key types, as stored in THIS database.

        Read-only view of ``scidb.store.schema_types``, the one owner. Read from the
        database at open and on :meth:`reopen` (another process — the GUI, a
        MATLAB run — may have changed it in between), and replaced by every
        write. Cached because ``canonicalize_metadata`` asks once per saved
        item. Change it with :meth:`set_schema_key_type`.
        """
        return self._schema_key_types

    def _refresh_schema_key_types(self: "DatabaseManager") -> None:
        """Re-read the declaration from the database (``schema_types``)."""
        previous = getattr(self, "_schema_key_types", {})
        current = schema_types.read_types(self._duck)
        self._schema_key_types = current
        if current != previous:
            self._forget_schema_key_kinds()

    def set_schema_key_type(
        self: "DatabaseManager",
        key: str,
        key_type: "str | None",
        *,
        confirm_respelling: bool = False,
        source: str = "api",
    ) -> dict:
        """Declare schema key *key* ``"numeric"`` or ``"string"``, or clear it
        (``None``). See :func:`scidb.store.schema_types.set_type`; returns the impact."""
        return schema_types.set_type(
            self, key, key_type, confirm_respelling=confirm_respelling, source=source
        )

    def declare_schema_key_types(
        self: "DatabaseManager", key_types: dict, *, dry_run: bool = False, source: str = "for_each"
    ) -> None:
        """Record ``schema_key_types=`` from a call (``for_each``, the MATLAB
        bridge) the way ``configure_database`` does: store what is missing,
        ignore what matches, raise ``SchemaKeyTypeConflictError`` on a
        difference. *dry_run* checks without writing."""
        schema_types.declare(
            self,
            schema_types.validate(key_types, self.dataset_schema_keys),
            source=source,
            dry_run=dry_run,
        )

    def schema_key_type_impact(self: "DatabaseManager", key: str, key_type: "str | None") -> dict:
        """What :meth:`set_schema_key_type` would do to the stored values,
        without writing (:func:`scidb.store.schema_types.impact`)."""
        return schema_types.impact(self, key, key_type)

    def canonicalize_metadata(self: "DatabaseManager", metadata: dict) -> dict:
        """Apply declared schema-key types to addressing metadata.

        Keys declared ``"numeric"`` collapse every spelling of the same
        number to one canonical identity (``"001"`` → 1 → stored ``"1"``);
        list values (load()'s OR semantics) canonicalize element-wise.
        ``"string"`` and undeclared keys pass through verbatim.  Returns a
        new dict; the input is not mutated.
        """
        if not self.dataset_schema_key_types:
            return metadata
        out = dict(metadata)
        for key, key_type in self.dataset_schema_key_types.items():
            if key_type != "numeric" or key not in out or out[key] is None:
                continue
            value = out[key]
            if isinstance(value, (list, tuple)):
                out[key] = [canonical_numeric_value(key, v) for v in value]
            else:
                out[key] = canonical_numeric_value(key, value)
        return out

    def _split_metadata(self: "DatabaseManager", flat_metadata: dict) -> dict:
        """
        Split flat metadata into nested schema/version structure.

        Keys in schema_keys go to "schema", all other keys go to "version".
        """
        schema = {}
        version = {}
        for key, value in flat_metadata.items():
            if key in self.dataset_schema_keys:
                # A date or datetime enters the record id as its one spelling,
                # so a value and its loaded copy are one record.
                text = datetime_key_text(value)
                schema[key] = value if text is None else text
            else:
                version[key] = value
        return {"schema": schema, "version": version}

    def _infer_schema_level(self: "DatabaseManager", schema_keys: dict) -> str | None:
        """
        Infer the schema level from provided keys.

        Walks dataset_schema_keys top-down. Returns the deepest provided key.
        Keys need not be contiguous — any subset of schema keys is valid.

        Returns None if no schema keys are provided.
        """
        if not schema_keys:
            return None

        level = None
        for key in self.dataset_schema_keys:
            if key in schema_keys:
                level = key
        return level

    @property
    def dataset_schema_key_order(self: "DatabaseManager") -> dict[str, list[str]]:
        """``{schema key: [levels]}`` from the project's ``[schema_keys]``, LIVE.

        Read through :func:`scifor.project.schema_order.project_level_order` on every
        access rather than once at open: this used to be a snapshot, so an
        edit to scistack.toml changed no table and no figure until the GUI (or
        the session) was restarted. The reader caches the config's location
        for a couple of seconds and its content on the file's mtime, so an
        access is a ``stat``.

        Each time the declaration CHANGES it is validated against this
        dataset's keys, so a typo is reported when it is written, not just
        when the database opens.
        """

        order = _schema_order.project_level_order()
        if order is not self._validated_key_order:
            self._validated_key_order = order
            if order:
                _schema_order.validate(order, self.dataset_schema_keys)
        return order

    @property
    def dataset_aliases(self: "DatabaseManager") -> "dict[str, dict]":
        """``{thing: {"name": …, "levels": {…}}}`` from the project's
        ``[aliases]``, LIVE — the display names every plot starts from.

        Same discipline as :attr:`dataset_schema_key_order`: read through
        :func:`scifor.project.aliases.project_aliases` on every access (a ``stat``, the
        parse is cached on mtime), and validated against this dataset's schema
        keys and variables each time the content CHANGES, so a typo is
        reported when it is written. Display only: nothing in scidb reads it.
        """

        table = _aliases.project_aliases()
        if table is not getattr(self, "_validated_aliases", None):
            if not table:
                self._validated_aliases = table
            else:
                variables = self._variable_names_for_validation("aliases")
                # Unavailable (the GUI released the connection before Plot
                # Studio read this): leave it unvalidated so the next access
                # validates it, instead of marking it done with no variables.
                if variables is not None:
                    self._validated_aliases = table
                    _aliases.validate(
                        table, schema_keys=self.dataset_schema_keys, variables=variables
                    )
        return table

    @property
    def dataset_colors(self: "DatabaseManager") -> "dict[str, object]":
        """``{"default": "#…", thing: {level: "#…"}}`` from the project's
        ``[colors]``, LIVE — the mark colours every plot starts from.

        The discipline of :attr:`dataset_aliases`: read through
        :func:`scifor.project.colors.project_colors` on every access, validated when
        the content CHANGES. Display only: nothing in scidb reads it.
        """

        table = _colors.project_colors()
        if table is not getattr(self, "_validated_colors", None):
            if not _colors.things_of(table):
                self._validated_colors = table
            else:
                variables = self._variable_names_for_validation("colors")
                if variables is None:
                    return table  # validate on a later access (see dataset_aliases)
                self._validated_colors = table
                _colors.validate(
                    table, schema_keys=self.dataset_schema_keys, variables=variables
                )
        return table

    def _sort_by_schema_keys(self: "DatabaseManager", df: pd.DataFrame) -> pd.DataFrame:
        """Sort DataFrame by schema keys with numeric sorting for numeric-only columns.

        For each schema key:
        - If all values are purely numeric (convertible to float), sort numerically
        - Otherwise, sort alphabetically (lexicographically)

        This ensures "1", "2", "10" sorts as 1, 2, 10 (not "1", "10", "2").
        But "S01", "S02", "S10" or mixed "1", "S01", "10" sorts alphabetically.
        """
        if len(df) == 0:
            return df

        # Create temporary sort columns for each schema key
        sort_cols = []
        sort_ascending = []
        temp_col_names = []
        # Asked once per sort: the property re-reads the project config.
        key_order = self.dataset_schema_key_order or {}

        for key in self.dataset_schema_keys:
            if key not in df.columns:
                continue

            temp_col_name = f"__sort_{key}"
            temp_col_names.append(temp_col_name)
            col = df[key]

            # A DECLARED order wins outright: `[schema_keys] session = ["BL",
            # "POST", "FU"]` is the user saying these levels are chronological,
            # not alphabetical, and no amount of value inspection can work that
            # out. Undeclared levels rank after every declared one and then
            # sort among themselves by the rules below — which is also what
            # every other consumer of the declaration does (scifor.project.schema_order).
            declared = key_order.get(key)
            if declared:
                df[temp_col_name] = _declared_rank_column(col, declared)
                sort_cols.append(temp_col_name)
                sort_ascending.append(True)
                continue

            # Get non-null values to check if column is numeric-only
            non_null_mask = col.notna()
            non_null_values = col[non_null_mask]

            # Check if all non-null values are numeric (can be converted to float)
            is_numeric = True
            if len(non_null_values) > 0:
                for val in non_null_values:
                    try:
                        float(str(val))
                    except (ValueError, TypeError):
                        is_numeric = False
                        break

            if is_numeric and len(non_null_values) > 0:
                # Numeric-only: convert to float for numeric sorting
                df[temp_col_name] = pd.to_numeric(col, errors="coerce")
            else:
                # Contains non-numeric: use string sorting
                df[temp_col_name] = col.astype(str)

            sort_cols.append(temp_col_name)
            sort_ascending.append(True)

        # Add timestamp as final tiebreaker (descending)
        if "timestamp" in df.columns:
            sort_cols.append("timestamp")
            sort_ascending.append(False)

        # Sort by all columns
        if sort_cols:
            df = df.sort_values(sort_cols, ascending=sort_ascending)

        # Drop temporary sort columns
        df = df.drop(columns=temp_col_names, errors="ignore")

        return df

    def schema_key_is_numeric(self: "DatabaseManager", key: str) -> bool:
        """Whether loaded values of ``key`` are restored to numbers.

        The rule, in order:

        1. a declared type (``schema_key_types``) wins — ``"numeric"`` or
           ``"string"``;
        2. otherwise the key is numeric only if EVERY value stored for it
           round-trips through :func:`from_schema_str` unchanged in spelling.
           One zero-padded ``"01"`` among the values makes the whole key a
           string key, so ``"10"`` beside it stays ``"10"`` rather than
           becoming 10 and giving the column two types.

        Cached per key; the cache is dropped whenever records are saved, since
        a new value can change the answer (a first ``"01"`` after ``1, 2``).
        """
        cache = self.__dict__.setdefault("_schema_key_numeric_cache", {})
        if key in cache:
            return cache[key]
        declared = (self.dataset_schema_key_types or {}).get(key)
        if declared in ("numeric", "string"):
            numeric = declared == "numeric"
            reason = f"declared {declared}"
        else:
            values = [v for v in self._duck.distinct_schema_values(key) if v is not None]
            kept = [v for v in values if isinstance(from_schema_str(v), str)]
            numeric = bool(values) and not kept
            reason = (
                "no values stored yet"
                if not values
                else "every stored value is a plain number"
                if numeric
                else f"{len(kept)} of {len(values)} stored value(s) keep their spelling "
                f"(e.g. {kept[0]!r})"
            )
        Log.debug(
            "schema key %r loads as %s: %s",
            key,
            "numbers" if numeric else "strings",
            reason,
        )
        cache[key] = numeric
        return numeric

    def schema_key_is_datetime(self: "DatabaseManager", key: str) -> bool:
        """Whether loaded values of ``key`` are restored to dates and datetimes:
        no declared type, and EVERY stored value is the exact spelling of one
        (``scicanonicalhash.datetime_key_text``). Days and timestamps may mix
        in one key. Cached like :meth:`schema_key_is_numeric`."""
        cache = self.__dict__.setdefault("_schema_key_datetime_cache", {})
        if key in cache:
            return cache[key]
        values = [v for v in self._duck.distinct_schema_values(key) if v is not None]
        answer = (
            (self.dataset_schema_key_types or {}).get(key) not in ("numeric", "string")
            and bool(values)
            and all(not isinstance(from_datetime_key_text(v), str) for v in values)
        )
        if answer:
            Log.debug(f"schema key {key!r} loads as dates: every stored value is one")
        cache[key] = answer
        return answer

    def schema_key_loads_as(self: "DatabaseManager", key: str) -> str:
        """``"numbers"``, ``"dates"`` or ``"strings"``: what a loaded value of
        ``key`` is."""
        if self.schema_key_is_numeric(key):
            return "numbers"
        return "dates" if self.schema_key_is_datetime(key) else "strings"

    def restore_schema_value(self: "DatabaseManager", key: str, value):
        """A loaded schema value with the type its KEY has — see
        :meth:`schema_key_loads_as`. The one restore every load path uses."""
        if value is None or not isinstance(value, str):
            return value
        if self.schema_key_is_numeric(key):
            return from_schema_str(value)
        return from_datetime_key_text(value) if self.schema_key_is_datetime(key) else value

    def _forget_schema_key_kinds(self: "DatabaseManager") -> None:
        """Drop the per-key type caches — called after any save."""
        self.__dict__.pop("_schema_key_numeric_cache", None)
        self.__dict__.pop("_schema_key_datetime_cache", None)

    def distinct_schema_values(self: "DatabaseManager", key: str) -> list:
        """Return all distinct values stored for a schema key.

        Args:
            key: A schema key name (e.g. "subject", "session")

        Returns:
            Distinct non-null values for that key: the levels declared in the
            project's ``[schema_keys]`` first, in that order, then the rest in
            the database's own (sorted) order.

        The declared order applies here, and not only to loaded frames,
        because this is where ``for_each(subject=[], session=[])`` gets its
        levels — so it decides the iteration order, and therefore the row
        order of every table ``for_each`` hands back, and the GUI's level lists.
        """

        return order_levels(
            key,
            self._duck.distinct_schema_values(key),
            declared=self.dataset_schema_key_order or {},
            fallback=lambda values: values,
        )

    def distinct_schema_combinations(self: "DatabaseManager", keys: list[str]) -> list[tuple]:
        """Return all distinct combinations for multiple schema keys.

        Args:
            keys: List of schema key names (e.g. ["subject", "session"])

        Returns:
            List of tuples of distinct non-null value combinations (strings)
        """
        return self._duck.distinct_schema_combinations(keys)
