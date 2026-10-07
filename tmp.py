from scidb.database import DatabaseManager

db = DatabaseManager(
    "/Users/mitchelltillman/Documents/Work/Stroke-R01-Aim-2/data/Stroke-R01-Aim2.duckdb",
    ["subject", "session", "speed", "trial", "cycle"],
    read_only=True,
)
agg = db.get_aggregated_variants()
for row in agg["constants"].get("formulaNum", {}).get("values", []):
    print("history value:", repr(row["value"]), "records:", row["record_count"])
from scistack_gui import registry
p = registry.get_parameters_registry().get("formulaNum")
print("declared values:", [repr(v) for v in p.values] if p else None)
db.close()