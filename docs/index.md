# SciStack

SciStack is a software package made for scientists by scientists to reduce the friction between your programming language (Python, MATLAB, etc.) and your science.

Spend more time building on previous work to advance science, and less time debugging.

- Bindings for Python and MATLAB

- Command Line Interface (CLI)

- VS Code Extension Graphical User Interface (GUI)

**SciStack itself is not equipped to do any analysis. It is an interface to help scientists more effectively utilize analytical code written by yourself and others.**

## The Current State of Scientific Computing

The current state of scientific computing is a fractured one. Different disciplines use tools that are unbeknownst to one another; labs within the same field waste time rediscovering the metaphorical wheel, all because journal articles' data and code [are not often published](https://pmc.ncbi.nlm.nih.gov/articles/PMC10334349/).

Thanks to recent [US federal funding mandates](https://grants.nih.gov/policy-and-compliance/policy-topics/sharing-policies/dms/policy-overview#introduction-to-the-2023-nih-data-management-&-sharing-policy), since 2023 [data sharing rates have increased to about 14.6%](https://pmc.ncbi.nlm.nih.gov/articles/PMC11135734/)! While this is progress, it is still very poor performance. Worse still, no such mandates have been given for code sharing, another crucial part of reproducible science.

## Better Research Tools, Better Research Outcomes

<!-- Ground truth (source/tests win over prose). Verified against this session's reconciled
     pages and: scidb/__init__ (BaseVariable, configure_database, get_database, for_each);
     scihist/__init__ (for_each, save, configure_database, AND re-exports lineage_fcn,
     LineageFcn, set_schema, Fixed, ... from the layers below); scimatlab configure_database
     (2 args); all data + lineage in one DuckDB file. NOTE: three USER-FACING layers
     (scifor/scidb/scihist); lineage is a scihist feature (lineage_fcn is re-exported by
     scihist, so import it from scihist — the scilineage package is internal); decorator is
     @lineage_fcn (not @thunk); configure_database takes 2 args (no SQLite "pipeline.db");
     persist lineage results via scihist.save, not VarClass.save. -->

To advance science by leveraging existing research, better tools are sorely needed. While there are many pieces of brilliant domain-specific analytical software written by and for researchers, a lack of interoperability between tools greatly hampers their efficacy to advance science.

The problem, as I see it, is that domain-specific software sits - unsupported - too high up on the [software stack](https://en.wikipedia.org/wiki/Solution_stack), missing an abstraction that should sit between the software's programming language (Python, MATLAB, etc.) and the domain-specific software. This is the niche that SciStack fills.

<!-- **A layered framework for reproducible scientific data processing.**

SciStack turns ordinary analysis functions into versioned, provenance-tracked,
cache-aware pipelines. Every result remembers how it was produced; nothing is
recomputed unless an input or the code actually changed; and all data plus lineage
lives in a single, inspectable DuckDB file. It works from both **Python** and
**MATLAB**.

## Enter at any level

SciStack is a *stack* of small packages — use only the layer you need, and adopt
the ones above it when you want more. (See
[Choosing Your Layer](getting-started/choosing-a-layer.md).)

| Layer | Package | What it adds |
|---|---|---|
| Batch iteration | `scifor` | Run a function over every condition combination on plain tables — no database |
| Storage | `scidb` | Typed, versioned variables in a database; DB-backed `for_each` |
| Full pipeline | `scihist` | `for_each` with automatic lineage + "recompute only what's stale" |

Each layer re-exports the one below it, so the top layer (`scihist`) gives you the
whole API from a single import.

## Quick example

```python
import numpy as np
from scidb import BaseVariable
from scihist import for_each, configure_database, lineage_fcn

# One DuckDB file holds data and lineage; declare the experiment's condition keys
configure_database("experiment.duckdb", ["subject", "session"])

class RawSignal(BaseVariable): schema_version = 1
class SignalPower(BaseVariable): schema_version = 1

# A tracked function: each call records its inputs + a hash of the function
@lineage_fcn
def compute_power(signal):
    return float(np.mean(signal ** 2))

# Process every subject/session, loading inputs and saving outputs with lineage
for_each(compute_power, inputs={"signal": RawSignal}, outputs=[SignalPower],
         subject=[1, 2, 3], session=["A", "B"])

# Re-run: nothing recomputes — every result is already current
for_each(compute_power, inputs={"signal": RawSignal}, outputs=[SignalPower],
         subject=[1, 2, 3], session=["A", "B"])
```

## Why SciStack

| Question | How SciStack answers it |
|---|---|
| "Which version of this data did I use?" | Content-addressed `record_id`s and automatic versioning |
| "What produced this result?" | Lineage captured automatically via `@lineage_fcn` |
| "I already computed this — why recompute?" | Cache hits and `skip_computed` reuse current results |
| "How do I organize experimental data?" | Address records by flexible metadata (subject/session/…) |
| "Can I share or inspect the database?" | One DuckDB file, queryable in DBeaver |

## Get started

- [Installation](getting-started/installation.md) — install the layers you need
- [Quickstart](quickstart.md) — a working pipeline in a few minutes
- [Choosing Your Layer](getting-started/choosing-a-layer.md) — pick your entry point
- [Concepts](concepts/index.md) — variables, lineage, caching, hashing
- [User Guide](guide/index.md) — task-oriented how-tos
- [API Reference](api/index.md) — the full surface
- [MATLAB Setup](matlab-setup.md) — the same workflow from MATLAB -->
