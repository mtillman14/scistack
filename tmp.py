import sys, numpy as np
from pathlib import Path
sys.path.insert(0, "tests")
import scifor as _scifor
from scidb import BaseVariable, configure_database, for_each
from scilineage.hashing import compute_function_hash

class VpRaw(BaseVariable): pass
class VpScaled(BaseVariable): pass

_scifor.set_schema([])
db = configure_database(Path("/tmp/vp_diag.duckdb").resolve(), ["subject", "trial"])
for subject in ["01", "02"]:
    VpRaw.save(np.array([1.0, 2.0]), subject=subject, trial=1)

def same_a(raw):
    return float(np.sum(raw))

def same_b(raw):
    total = float(np.sum(raw))
    return total

for body in (same_a, same_b):
    body.__name__ = "vp_same"
    print("hash", body.__qualname__, compute_function_hash(body, truncate=16))
    for_each(body, inputs={"raw": VpRaw}, outputs=[VpScaled], subject=[], trial=[])

q = lambda sql: db._duck._fetchall(sql)
print("_invocation      :", q("SELECT invocation_id, function_name, function_hash FROM _invocation"))
print("_invocation_output:", q("SELECT * FROM _invocation_output"))
print("_run              :", q("SELECT run_id, function_name FROM _run"))
print("_run_invocation   :", q("SELECT * FROM _run_invocation"))
print("VpScaled records  :", q("SELECT record_id FROM _record WHERE type='VpScaled'"))
print("_record_save      :", q("SELECT record_id, timestamp FROM _record_save ORDER BY timestamp"))
db.close()