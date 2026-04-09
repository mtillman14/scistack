import ast
for f in [
    'scistack-gui/scistack_gui/api/run.py',
    'scistack-gui/scistack_gui/api/layout.py',
    'scistack-gui/scistack_gui/pipeline_store.py',
    'scistack-gui/scistack_gui/server.py',
    'scistack-gui/scistack_gui/api/pipeline.py',
]:
    with open(f) as fh:
        ast.parse(fh.read())
    print(f'{f}: OK')