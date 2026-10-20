# Does for_each (-> save_batch) call a custom to_db? Run from /workspace.
import tempfile
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scidb import BaseVariable, configure_database, for_each


@dataclass
class Trajectory:
    t: np.ndarray
    label: str


class RawT(BaseVariable):
    pass


class MarkerTrajectory(BaseVariable):
    def to_db(self) -> pd.DataFrame:
        print("[repro] to_db called")
        return pd.DataFrame({"t": self.data.t, "label": self.data.label})

    @classmethod
    def from_db(cls, df: pd.DataFrame) -> Trajectory:
        print("[repro] from_db called")
        return Trajectory(t=df["t"].to_numpy(), label=str(df["label"].iloc[0]))


def make_traj(raw):
    return Trajectory(t=np.asarray(raw, dtype=float), label="heel")


db = configure_database(Path(tempfile.mkdtemp()) / "repro.duckdb", ["subject"])
RawT.save(np.array([0.0, 0.1, 0.2]), subject="S01")

print("[repro] direct save:")
MarkerTrajectory.save(Trajectory(t=np.array([1.0, 2.0]), label="direct"), subject="S02")
v = MarkerTrajectory.load(subject="S02")
print("[repro]   loaded type:", type(v.data).__name__)

print("[repro] for_each save:")
try:
    for_each(make_traj, {"raw": RawT}, [MarkerTrajectory], subject=["S01"])
    v = MarkerTrajectory.load(subject="S01")
    print("[repro]   loaded type:", type(v.data).__name__)
except Exception as e:
    print("[repro]   FAILED:", type(e).__name__, e)
db.close()
