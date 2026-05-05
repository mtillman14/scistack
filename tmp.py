import sys
sys.path.insert(0, "scidb/src")
sys.path.insert(0, "scifor/src")

import numpy as np
import pandas as pd
from scidb import BaseVariable, configure_database
import tempfile
import os

# We need to patch the for_each function to add debug output to the wrapper
# This is tricky because the wrapper is defined inside for_each
# Let me create a modified version

SCHEMA = ["subject", "session"]

class RawSignal(BaseVariable):
    pass

class Aggregated(BaseVariable):
    pass

def aggregate_sum(signal):
    print(f"\n>>> Function received: type={type(signal)}")
    return signal

# Add this attribute to check
print(f"aggregate_sum.__lineage_wrapper__ = {getattr(aggregate_sum, '__lineage_wrapper__', 'NOT SET')}")

with tempfile.TemporaryDirectory() as tmp:
    db = configure_database(os.path.join(tmp, "test.duckdb"), SCHEMA)

    RawSignal.save(1.0, subject="S01", session="1")

    # Import after setup so we can check the function
    from scidb.foreach import for_each

    # Check the wrapper conditions
    print(f"\nBefore calling for_each:")
    print(f"  Function: {aggregate_sum.__name__}")
    print(f"  Has __lineage_wrapper__: {hasattr(aggregate_sum, '__lineage_wrapper__')}")

    result = for_each(aggregate_sum, {"signal": RawSignal}, [Aggregated],
                    subject=["S01"], session=["1"],
                    save=False,
                    _inject_combo_metadata=False)  # Explicitly disable