"""scidb's user-facing model: variables, parameters, variants, filters, input
wrappers, exceptions and the logging facade.

The lowest of scidb's layers (model -> store -> provenance -> run -> inspect;
``scidb/tests/import_layers.toml``). Nothing is re-exported here: import the
module (``scidb.model.variable``) or the facade (``scidb``).
"""
