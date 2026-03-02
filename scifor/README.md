# scifor

Standalone `for_each` batch execution utilities for data pipelines.

Works with plain DataFrames, file I/O, or any custom `.load()`/`.save()` implementation.
No database required.

## Usage

```python
import pandas as pd
from scifor import set_schema, for_each, Col

set_schema(["subject", "session"])

raw_df = pd.DataFrame({
    "subject": [1, 1, 2, 2],
    "session": ["pre", "post", "pre", "post"],
    "emg": [...],
})

result = for_each(
    my_fn,
    inputs={"signal": raw_df},
    outputs=[],
    subject=[1, 2],
    session=["pre", "post"],
)
```
