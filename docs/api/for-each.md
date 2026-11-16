# Batch Processing API — `for_each`

`for_each` runs a function over every combination of conditions. For concepts and
examples see the [Batch Processing guide](../guide/for_each.md).

The full signatures, every keyword argument and the syntax of every input
wrapper, in Python and MATLAB, are on the two API pages:

- [`scifor.for_each`](scifor.md#for_each) and [scifor wrappers](scifor.md#input-wrappers):
  plain DataFrames, nothing saved.
- [`scidb.for_each`](scidb.md#batch-processing) and [scidb wrappers](scidb.md#input-wrappers):
  inputs loaded from the database, outputs saved with their history.

## Column selection { #column-selection }

Moved: see [column selection in scidb](scidb.md#column-selection) and
[`ColumnSelection` in scifor](scifor.md#columnselection).

## Return value

Both return a pandas DataFrame (one row per combination, metadata columns
then outputs) — or `None` when there is nothing to return.
`scidb.for_each` also saves outputs unless `save=False`, and returns a `Step` when it registers into a pipeline.

**See also:** [Variables](variables.md) · [Filters](filters.md) ·
[Database](database.md) · [Guide: Batch Processing](../guide/for_each.md)
