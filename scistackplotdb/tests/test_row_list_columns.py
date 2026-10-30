"""Plot Studio reads a row-list column (rows combined by
``on_duplicate="combine"``) as a 1-D measure: every cell a float array, one
element per source row (``[7]``, ``[5, 12]``). Decision recorded in
``.claude/plan-combine-duplicate-locations.md`` §6.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scidb import BaseVariable

from scistackplotdb.load import load_variable


class StayDays(BaseVariable):
    pass


def test_row_list_column_cells_are_1d_arrays(db):
    df = pd.DataFrame(
        {"subject": ["01", "01", "02"], "days": [5.0, 12.0, 7.0]}
    )
    StayDays.save(df, db=db, on_duplicate="combine")
    frame = load_variable(db, "StayDays", with_variants=False).frame
    value_cols = [c for c in frame.columns if c not in ("record_id", "subject", "session", "trial")]
    cells = {s: frame.loc[frame["subject"] == s, value_cols[0]].iloc[0] for s in ("01", "02")}
    assert all(isinstance(c, np.ndarray) for c in cells.values())
    assert list(cells["01"]) == [5.0, 12.0]
    assert list(cells["02"]) == [7.0]
