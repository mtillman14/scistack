# \_record_metadata table

1. What is the purpose of having version_keys and branch_params separate? version_keys are for the current computation, and branch_params are the version_keys of the upstream computations?

# Step 1: EachOf expansion (lines 131–173)

1. scifor would benefit from supporting EachOf expansion in the future. Obviously the inputs would be MATLAB tables or pd.DataFrame rather than BaseVariable classes

# Step 9:

1. While the current implementation is functional, it leads to unnecessary [skip] lines, which just serves to confuse a user. [skip] lines should be minimal.

# Step 10:

1. "wrap in a PerComboLoader sentinel (loaded individually per-combo during iteration)" What's the purpose/benefit of the PerComboLoader sentinel? I'm not familiar with this concept.
2. The additional cleanup during assembly section says "Version-key columns starting with ** are stripped (they are internal metadata, not data)
   Constants that were stored in version keys (recorded in **constants) are also stripped — if low_hz=20 was saved as a version key, it would appear as a column named low_hz in the loaded DataFrame, which would confuse scifor's data-column detection (scifor would see low_hz as a data column instead of recognizing it as metadata)". These concepts could use some more elaboration
