# What's a Schema?

One of the first steps in creating any new projects is deciding on the dataset's schema. But what's a schema?

All datasets are organized a certain way - that organization is called a "schema". In SciStack, schema is specified as a list of all lowercase strings, by convention. The schema order, moving from left to right, is one-to-many. Think carefully about your dataset's schema, as it cannot be changed once a database is created.

For example, for a dataset containing baseball player performance statistics broken down by every game, such as number of homeruns or number of hits (batting), average pitch speed or number of allowed runs (in the field), etc. the schema might be `["player", "year", "team", "game_number"]`. If the dataset instead contained other statistics that span an entire season, like on-base percentage, slugging percentage, etc., the schema might be `["player", "year", "team"]`. `"game_number"` is not relevant to this dataset, so it is excluded from the schema.

/// danger | Don't include Variables
Variables are **never** part of the schema. Schema describes the set of possible rows in a tabular dataset - Variables are the columns.
///
