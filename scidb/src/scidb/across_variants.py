"""AcrossVariants pooling wrapper: opt IN to cross-variant aggregation."""

from typing import Any

from .input_spec import spec_name


class AcrossVariants:
    """
    Wrapper to deliberately pool ALL branch_param variants of an input into one
    aggregated table, with each row's upstream branch_params attached as columns.

    Aggregation-mode ``for_each`` (iterating a strict subset of the schema keys)
    **auto-splits** by upstream branch_param signature by default: one call per
    variant group, as if the user had written
    ``EachOf(Variant(X, ...), Variant(X, ...))``. That default is right for
    ordinary aggregation and for endpoint (``plot_``/``stat_``) functions — a
    pooled t-test double-counts, and a figure mixing two filter settings is
    wrong.

    ``AcrossVariants`` is the explicit opt-out for **multiverse /
    specification-curve analysis**: the one legitimate cross-variant use case,
    where the analysis deliberately spans pipeline decisions to quantify
    robustness. The pooled rows keep their variant identity — each namespaced
    branch_param key (e.g. ``bandpass.low_hz``) becomes an ordinary DataFrame
    column so the function can group by specification::

        def robustness(df):
            return df.groupby("bandpass.low_hz")["value"].mean()

        for_each(robustness, {"df": AcrossVariants(Filtered)}, [Spec],
                 subject=["S01", "S02"])

    Composition:

    - May wrap a variable type, a ``Fixed``, or a ``Variant``
      (``AcrossVariants(Variant(X, low_hz=20))`` pins some params and pools
      whatever variants remain).
    - ``AcrossVariants(ColumnSelection)`` is an ERROR: the column selection
      would drop the attached branch_param columns, silently defeating the
      pooling contract.
    - ``AcrossVariants(Merge(...))`` is an ERROR (mirrors ``Variant`` /
      ``Fixed``): pool per constituent instead.
    - ``AcrossVariants(EachOf(...))`` is an ERROR: ``EachOf`` must stay the
      outermost wrapper.

    In full-iteration mode (all schema keys iterated) each combo already sees
    exactly one variant, so pooling is meaningless: the input behaves as if
    unwrapped and a warning is logged.
    """

    def __init__(self, var_type: Any):
        from scifor import ColumnSelection, EachOf, Merge

        if isinstance(var_type, Merge):
            raise TypeError(
                "AcrossVariants cannot wrap a Merge. branch_params are namespaced "
                "per producing function, so pooling must happen per constituent: "
                "Merge(AcrossVariants(A), B)."
            )
        if isinstance(var_type, EachOf):
            raise TypeError(
                "AcrossVariants cannot wrap an EachOf. EachOf must stay the "
                "outermost wrapper."
            )
        if isinstance(var_type, ColumnSelection):
            raise TypeError(
                "AcrossVariants cannot wrap a ColumnSelection: the column "
                "selection would drop the attached branch_param columns. "
                "Select columns inside your function instead."
            )
        if isinstance(var_type, AcrossVariants):
            var_type = var_type.var_type  # idempotent

        self.var_type = var_type

    def to_key(self) -> str:
        """Return a canonical string for use as a version key.

        A pooled run and an auto-split run of the same function must not
        collide: this key lands in the ``__inputs`` config key (same mechanism
        as ``Variant.to_key()``).
        """
        if hasattr(self.var_type, "to_key"):
            inner_key = self.var_type.to_key()
        elif isinstance(self.var_type, type):
            inner_key = self.var_type.__name__
        else:
            inner_key = repr(self.var_type)
        return f"AcrossVariants({inner_key})"

    @property
    def __name__(self) -> str:
        """Display name for format_inputs and error messages."""
        return f"AcrossVariants({spec_name(self.var_type)})"
