"""scidb.project: the model-bound half of project runtime.

Everything about a project that needs scidb's model (Variables,
Parameters, the database): materializing declared entities, discovering a
project's code, creating a project (``init``), libraries, bundles
(export/import), environments, verification, table copies, schema-key maps,
and the ``scistack export|import|library`` commands (``cli``). The
model-free half (root, config, discovery walk, display grammars, the
entities-file grammar) is ``scifor.project``.
"""
