# scistackplotdb

## Plot what's in the database

`scistackplotdb` loads SciDB variables into the long format
[`scistackplot`](../scistackplot/README.md) consumes, and generates pipeline
endpoints from a finished plot spec.

```bash
pip install scistackplotdb
```

```python
from scidb import configure_database
from scistackplot import PlotSpec, Role, PlotKind, render
from scistackplotdb import ScidbSource

db = configure_database("experiment.duckdb", ["subject", "session", "trial"])
source = ScidbSource(db)

table = source.get_table(["StepLength"])
spec = PlotSpec(
    measures=["StepLength"],
    roles={"session": Role.X, "subject": Role.FREE, "trial": Role.FREE},
    kind=PlotKind.BOX,
)
figure = render(table, spec)
```

## What this layer actually solves

The long format is nearly free — schema keys are already columns once a
variable is joined to `_schema`, the same shape `stat_` functions receive. The
real work is the four things a flat CSV never had.

**Shape classification.** Scalar, 1-D, or 2-D, decided from observed values
rather than declared SQL type names, and cached. It determines which plot kinds
are offered at all.

**Joins across schema depth.** Plotting trial-level `Speed` against
subject-level `Mass` broadcasts the shallower variable down the hierarchy:

```python
source.joinable_with("StepLength")     # -> ["Mass"]  (Signal is 1-D: no x axis)
table = source.get_table(["StepLength", "Mass"])   # one Mass value per trial row
```

Because the dataset schema is an ordered, contiguous hierarchy, one variable's
levels are always a prefix of the other's or the two cannot be joined — and
`join_frames` refuses the latter with a message saying why.

**Variants are factors — this one is a correctness trap.** A variable produced
at two filter cutoffs has *two records per schema combination*. Treating those
branch params as ordinary columns silently plots two pipelines' results as if
they were replicates of one:

```python
spec = PlotSpec(measures=["Scaled"], roles={"session": Role.X})
validate(spec, table)
# RoleError: Variant factor(s) ['scale.factor'] would be pooled: their levels
# are different pipeline variants, not replicates... Assign them
# 'color'/'facet'/'iterate', select the variants you want with
# PlotSpec.variant_sets, or — to pool them deliberately — set them to
# 'aggregate' or 'free' yourself.
```

**A transport budget.** 1-D data across hundreds of trials is megabytes.
`resolve(..., max_points=N)` downsamples for the interactive panel; export
never does.

## From spec to pipeline endpoint

```python
from scistackplotdb import generate_endpoint

code = generate_endpoint(spec, table, input_variable="StepLength")
print(code.source)
```

```python
def plot_steplength(df, filename):
    ...
    return g.figure

for_each(
    plot_steplength,
    inputs={
        "df": StepLength,
        "filename": PathOutput("plots/steplength_{subject}.png"),
    },
    outputs=[StepLengthFigure],
    as_table=['df'],
    finalized=True,
    subject=[],
)
```

The one translation that has to be exactly right is `Role.ITERATE` → a
`for_each` iteration keyword. Interactively, ITERATE fans out through a pandas
`groupby`; in the pipeline it fans out through `for_each` + `PathOutput`. If
those disagree, the exported pipeline is not what you previewed —
`tests/test_fanout_parity.py` runs both paths against the same database and
compares the figure sets.

Everything about *recording* the figure — `finalized`, artifact stamping,
`skip_computed`, `scidb report` — is SciDB's existing endpoint machinery and is
untouched.

## Ordering

Factor levels are ordered by SciDB's declared `schema_key_types`, not by
pandas' default: a key declared `numeric` sorts numerically, and everything
else goes through a natural sort so zero-padded IDs land as
`01, 02, … 10` instead of `01, 10, 02`.

See [`docs/claude/plotting-library-design.md`](../docs/claude/plotting-library-design.md).
