import numpy as np, pandas as pd
from scistackplot import HistogramOptions, HistStat, LongTable, PlotKind, PlotSpec, Role, generate_plot_function
VALUES = [0.0, 0.0, 0.0, 0.0, np.nan, np.nan, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
rows = [{"group": g, "trial": str(i), "Speed": v} for g in ("A", "B") for i, v in enumerate(VALUES)]
table = LongTable.from_frame(pd.DataFrame(rows), factors=["group", "trial"], measures=["Speed"], schema_levels=["group", "trial"])
spec = PlotSpec(measures=["Speed"], roles={"group": Role.GROUP, "trial": Role.COLLAPSE}, groups=["group"],
                kind=PlotKind.HISTOGRAM, histogram=HistogramOptions(exact_values=(0.0,), stat=HistStat.PERCENT))
print(generate_plot_function(spec, table, function_name="plot_h"))