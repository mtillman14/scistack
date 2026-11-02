"""scifor.project: the model-free half of project runtime.

Project root, machine roots, code discovery, ``scistack.toml`` and the
display grammars that live in it. Nothing here imports the scifor engine
(``for_each``, ``PathInput``, locations); ``tests/test_import_layers.py``
enforces that. The model-bound half (Variables, Parameters, the database)
is ``scidb.project``.
"""
