# Schema keys: order, contiguity and gaps

*Rewritten 2026-11-15. This described `_schema_key_columns`, which no longer
exists. For branches see `branched-schema-keys.md`.*

## What a schema is

`dataset_schema_keys` is an ordered list of key names, e.g.
`["subject", "intervention", "timepoint", "speed", "trial", "cycle"]`. The
`_schema` table has one VARCHAR column per key (in that order, after `schema_id`
and `schema_level`). The list owns key ORDER; parents (which key sits under
which) are `branched-schema-keys.md`'s `SchemaTree`, and a plain list is the
one-branch tree where each key sits under the previous one.

## A record names any SET of keys

A save names whichever keys it has, as keyword arguments or as columns of a
returned table. There is no contiguity requirement:

```python
db.save("cohens_d", 0.85, subject="S01", intervention="RMT30", speed="SSV")
```

- `schema_level` is the deepest named key in the list,
- the `_schema` row has `timepoint`, `trial` and `cycle` NULL,
- a NULL means "this record does not name this key", not "unknown".

That is what lets results collapse across intermediate dimensions (Cohen's d
across timepoints for each speed), and what lets a branched schema hold a
record at `brand/location/person` with `category` and `item` NULL.

## Where key resolution happens

- `SciDuck.get_or_create_schema_id` (`scidb/store/sciduck.py`) derives the key
  columns from the keys the caller gave and requires NULL in every other schema
  column, so different key sets are different `_schema` rows even at the same
  `schema_level`.
- `batch_get_or_create_schema_ids` groups by `(schema_level, frozenset(keys))`
  for the same reason.
- `load` selects every schema column and filters on whichever keys were given.
- Variable views select every schema column.

## What a variable's "level" is

A variable's level is the set of keys its records populate
(`scidb.model.filters.schema_id_key_closure`): the populated keys plus their
ancestors. On a linear schema that is the prefix of the list up to the deepest
populated key, which is why this used to be called "the level index". A
variable saved with a gap (subject and trial, no session) is still at the
prefix through trial.

## Changing keys

See `changing-schema-keys.md` (rebuild while empty) and
`branched-schema-keys.md` (add keys, and change unused ones, while data exists).
