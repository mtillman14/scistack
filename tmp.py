from scidb import configure_database
import json

# Look at what get_pipeline_structure returns
import inspect
import scidb
db = configure_database('test_gui.duckdb', ['subject', 'session'])
print(type(db))
print([m for m in dir(db) if not m.startswith('_')])