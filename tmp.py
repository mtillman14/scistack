import datetime, numpy as np, pandas as pd
from scidb import configure_database, BaseVariable

db = configure_database("/tmp/nat_check.duckdb", ["subject"])

class When(BaseVariable):
    schema_version = 1

cases = {
    "S01": pd.Timestamp("2026-01-01 10:00"),
    "S02": pd.NaT,
    "S03": datetime.datetime(2026, 1, 3, 12, 0),
}
for subj, v in cases.items():
    When.save(v, subject=subj)

for subj in cases:
    rec = When.load(subject=subj)
    print(subj, repr(cases[subj]), "->", repr(rec.data), type(rec.data).__name__, "isna:", pd.isna(rec.data))

class When64(BaseVariable):
    schema_version = 1

for subj, v in {"S01": np.datetime64("2026-01-01T10:00"), "S02": np.datetime64("NaT")}.items():
    try:
        When64.save(v, subject=subj)
        rec = When64.load(subject=subj)
        print("np", subj, repr(v), "->", repr(rec.data))
    except Exception as e:
        print("np", subj, "FAILED:", type(e).__name__, e)