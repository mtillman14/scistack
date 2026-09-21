import sys
sys.path.insert(0, "examples/aim2/src/cycles")
import scidb
import pipeline

scidb.configure_database("examples/aim2/aim2.duckdb",
                        ["subject", "session", "speed", "trial", "cycle"])

sym = pipeline.CycleSymmetry.load(as_df=True)
print("CycleSymmetry dtypes:\n", sym.dtypes)
print("cycle types:", sorted({type(v).__name__ for v in sym["cycle"]}))
print("cycle values:", sorted({repr(v) for v in sym["cycle"]}))
print("trial values:", sorted({repr(v) for v in sym["trial"]}))

dev = pipeline.CycleDeviation.load(as_df=True)
print("\nCycleDeviation rows:", len(dev))
print("cycle types:", sorted({type(v).__name__ for v in dev["cycle"]}))
print("cycle values:", sorted({repr(v) for v in dev["cycle"]}))
print("per trial:", dev.groupby(["subject", "session", "speed", "trial"]).size().describe().to_dict())
