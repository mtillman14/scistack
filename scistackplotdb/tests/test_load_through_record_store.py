"""``scistackplotdb.load`` reads a variable's records through scidb's
``RecordStore``; it writes no storage SQL of its own.

Before P4 stage 5 (design review 06 PD1) the bulk load of a variable existed
twice: ``DatabaseManager.load_all_as_df`` and this module's own SELECTs
against ``_registered_types``, ``_variables``, ``information_schema`` and the
data tables. The plot-at-scale behaviour (metadata-only mode selects no data
column; payload only through ``fetchdf``) is pinned by
``test_load_fetch.py`` and ``test_variant_table.py``; this file pins where
the SQL lives, and that the load still goes through the store's long frame.
"""

import re
from pathlib import Path

from scistackplotdb import load as load_mod

#: A query string: a string literal (f-string or plain) that opens with a
#: statement keyword.
_QUERY = re.compile(r"""f?["'](SELECT|INSERT|UPDATE|DELETE|WITH)\b""")


def test_load_module_has_no_storage_sql():
    code = Path(load_mod.__file__).read_text(encoding="utf-8")
    assert _QUERY.findall(code) == [], "storage SQL belongs in scidb.store.record_store"
    assert "_duck.fetch" not in code and "_duck.execute" not in code


def test_load_variable_reads_the_store_long_frame(seeded, monkeypatch):
    calls = []
    store = seeded.record_store
    real = store.long_frame

    def spy(type_name, columns=()):
        calls.append((type_name, list(columns)))
        return real(type_name, columns)

    monkeypatch.setattr(store, "long_frame", spy)
    frame = load_mod.load_variable(seeded, "Emg", include_data=False)
    assert calls == [("Emg", [])]
    assert frame.data_columns == []
