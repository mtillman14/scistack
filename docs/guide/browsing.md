# Browsing and Exporting Data

SciStack stores data in a DuckDB database file using native DuckDB types (arrays, JSON, etc.). This means you can inspect data directly using DBeaver or any DuckDB-compatible viewer.

This guide covers how to browse and export your data.

## Viewing Data in DuckDB Browsers

Open your `.duckdb` file in DBeaver or any DuckDB viewer. Each variable type has its own table with these columns:

| Column            | Description                                  |
|-------------------|----------------------------------------------|
| `value`           | The stored data (DuckDB native type)         |
| `_dtype_meta`     | Type metadata for round-trip restoration     |
| `_record_id`      | Unique content hash                          |
| `_content_hash`   | Hash of data content                         |
| `_lineage_hash`   | Hash of computation lineage (if applicable)  |
| `_schema_version` | Schema version number                        |
| `_metadata`       | JSON with addressing keys                    |
| `_user_id`        | User ID (if set via SCIDB_USER_ID env var)   |
| `_created_at`     | Timestamp                                    |
| Schema columns    | One column per dataset schema key            |

For variables with custom serialization (`to_db()`/`from_db()`), the data columns match the DataFrame structure returned by `to_db()`.

## Exporting to CSV

### Single Variable

```python
var = TimeSeries.load(subject=1, trial=1)
var.to_csv("subject1_trial1.csv")
```

### Multiple Variables

```python
# Export all matching records to a single CSV
count = db.export_to_csv(
    TimeSeries,
    "all_experiment_data.csv",
    experiment="exp1"
)
print(f"Exported {count} records")
```

The exported CSV includes:
- All columns from `to_db()` output
- `_record_id` column for traceability
- `_meta_*` columns for each metadata key

### Example CSV Output

```csv
index,value,_record_id,_meta_subject,_meta_trial
0,0.123,abc123...,1,1
1,0.456,abc123...,1,1
2,0.789,abc123...,1,1
0,0.234,def456...,1,2
1,0.567,def456...,1,2
```

## Workflow Recommendations

1. **Quick inspection**: Use DBeaver or a DuckDB viewer to browse tables directly
2. **Detailed analysis**: Use `var.to_csv()` or `db.export_to_csv()` to export, then open in Excel/pandas
3. **Programmatic access**: Use `load()` and work with data directly in Python

## Example: Full Workflow

```python
from scidb import configure_database, BaseVariable
import numpy as np

class Measurement(BaseVariable):
    pass

# Setup
db = configure_database(
    "experiment.duckdb",
    ["subject", "experiment"],
    "pipeline.db",
)

# Save some data
for subject in [1, 2, 3]:
    data = np.random.randn(100)
    Measurement.save(data, subject=subject, experiment="demo")

# Export for external analysis
db.export_to_csv(Measurement, "demo_data.csv", experiment="demo")

# Or view in DuckDB browser - open experiment.duckdb and browse
# the 'Measurement' table directly
```
