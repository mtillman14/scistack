"""Output-path template input for for_each.

Unlike :class:`~scifor.pathinput.PathInput` — which *locates existing input
files* by substituting metadata into a template and walking the filesystem
(discovery, regex matching, per-combo loading) — ``PathOutput`` is a pure
*output* path template. It substitutes the current combo's metadata and the
current ``for_columns`` column into a template and hands the resolved path to
the function as a plain argument. There is no discovery, no regex, no file
reading, and no ``.load`` — the function decides what to write where.
"""

from pathlib import Path
from typing import Any

# Token replaced with the current for_columns column name. Resolution is a
# literal str.replace (not str.format) so any *other* braces in the path
# (e.g. regex quantifiers a downstream consumer might use) pass through.
COLUMN_TOKEN = "{ColName}"


class PathOutput:
    """
    Resolve an output-path template from iteration metadata and the current column.

    Used as a constant input to ``for_each``: on each call the template is
    resolved and the function receives the finished path. Two substitution
    sources, both literal ``str.replace``:

    - **Combo metadata** — every ``{key}`` matching a metadata name for the
      current combo is replaced (e.g. ``{subject}``, ``{session}``). Keys not
      present in the combo are left untouched.
    - **Current column** — the token ``{ColName}`` is replaced with the name of
      the column currently being processed inside a ``for_columns`` iteration.
      Using ``{ColName}`` requires at least one iterate input; for_each raises if
      there is none.

    Because substitution covers *every* combo-metadata key, higher layers can
    supply additional placeholder values by adding keys to the combo: scidb
    uses this to resolve branch_param placeholders (``{low_hz}``,
    ``{bandpass.low_hz}``, ``{variant}``) so each pipeline-variant group gets
    its own artifact path. scifor itself needs no knowledge of them.

    The result keeps the template's type: a ``Path`` in yields a ``Path`` out, a
    ``str`` yields a ``str``.

    Example:
        for_each(
            anova2way,
            inputs={
                "data": ColumnSelection(means_df, columns=[], iterate=True),
                "data_column": ColName,
                "filename": PathOutput(root / "{subject}_{ColName}_anova2way.pdf"),
            },
            subject=[1, 2],
        )
    """

    def __init__(self, template: "str | Path"):
        """
        Args:
            template: The output path template, a ``str`` or ``pathlib.Path``
                containing ``{metadata_key}`` and/or ``{ColName}`` placeholders.
        """
        if not isinstance(template, (str, Path)):
            raise TypeError(
                f"PathOutput template must be a str or pathlib.Path, "
                f"got {type(template).__name__}"
            )
        self.template = template
        self.__name__ = f"PathOutput({template!r})"

    @property
    def has_column_token(self) -> bool:
        """True if the template references the current ``for_columns`` column."""
        return COLUMN_TOKEN in str(self.template)

    def resolve(
        self, metadata: "dict[str, Any] | None" = None, column: "str | None" = None
    ) -> "Path | str":
        """Resolve the template for the current combo (and column, if iterating).

        Args:
            metadata: The current combo's metadata; each ``{key}`` is replaced
                with ``str(value)``. Missing keys are left untouched.
            column: The current ``for_columns`` column name. When given, the
                ``{ColName}`` token is replaced with it.

        Returns:
            The resolved path — a ``Path`` if the template was a ``Path``, else
            a ``str``.
        """
        resolved = str(self.template)
        if metadata:
            for key, value in metadata.items():
                resolved = resolved.replace("{" + str(key) + "}", str(value))
        if column is not None:
            resolved = resolved.replace(COLUMN_TOKEN, column)
        # A relative result goes under the project's [outputs] folder when one
        # is stated (scifor.project.outputs — the one owner). Unset: unchanged.
        from scifor.project.outputs import under_output_folder

        return under_output_folder(
            Path(resolved) if isinstance(self.template, Path) else resolved
        )

    def __repr__(self) -> str:
        return f"PathOutput({self.template!r})"
