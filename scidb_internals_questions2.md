# \_record_metadata table

OK 1. What is the purpose of having version_keys and branch_params separate? version_keys are for the current computation, and branch_params are the version_keys of the upstream computations?

# Step 1: EachOf expansion (lines 131–173)

1. scifor would benefit from supporting EachOf expansion in the future, that would make it more flexible. Obviously the inputs would be MATLAB tables or pd.DataFrame rather than BaseVariable classes

# Step 9:

1. While the current implementation is functional, it leads to unnecessary [skip] lines, which just serves to confuse a user. [skip] lines should be minimal.

# Step 10:

1. "wrap in a PerComboLoader sentinel (loaded individually per-combo during iteration)" What's the purpose/benefit of the PerComboLoader sentinel? I'm not familiar with this concept.
2. The additional cleanup during assembly section says "Version-key columns starting with \_\_ are stripped (they are internal metadata, not data)
   Constants that were stored in version keys (recorded in \_\_constants) are also stripped — if low_hz=20 was saved as a version key, it would appear as a column named low_hz in the loaded DataFrame, which would confuse scifor's data-column detection (scifor would see low_hz as a data column instead of recognizing it as metadata)". These concepts could use some more elaboration in the docs
3. `load_all(version_id="latest")` what are the other options for version_id?
4. "\_\_branch_params: A JSON string from \_record_metadata.branch_params. This is the accumulated set of upstream pipeline choices that led to this record. For example, '{"bandpass.low_hz": 20}' tells you this record was produced (directly or indirectly) by a bandpass function call with low_hz=20. The format is a flat dict where keys are "function_name.param_name" and values are the parameter values." What if the same function is reused in multiple places? e.g. one use of the bandpass filter function is on walking data with low_hz=20, and there's a variant where low_hz=10. But then the same bandpass filter function is reused on maximal contraction data with low_hz=10. What would happen in that case?

# Step 12:

1. "**But first: aggregation mode detection (lines 368–389):**

There is one important exception. If you are intentionally _not_ iterating over all schema keys — for example, iterating over `subject` but not `session` when the schema is `[subject, session]` — you are performing an aggregation. You _want_ the function to receive multiple rows (all sessions for a subject) as a single multi-row DataFrame.

In this case, separating records by variant would defeat the purpose. If you are computing "mean across all sessions for subject 1", you want all rows for subject 1, regardless of which upstream variant they came from."

I think this is a false statement. If I'm understanding correctly, this would combine variants when aggregating data, which I don't think we want. For example, let's say we bandpass filter some data with low_hz=20, and have another variant with low_hz=30. We do this at the trial level in a [subject, trial] database schema. Then, a downstream function wants to iterate over subjects only, i.e. aggregate over all trials within each subject. I don't want the low_hz=20 and low_hz=30 data mixed when aggregating. Just like when fully iterating, these two separate variants should be fed into the computation separately. The only difference is in the case of aggregation, the data in each variant is aggregated appropriately first.

2. "When all schema keys are being iterated, scidb performs variant expansion:" why shouldn't this also happen in aggregation mode?

# Step 13:

OK 1. I don't think that this mechanism for enumerating the expected combos is sufficiently flexible. What happens when the same function is used in two different places? Reusability is a critical goal of this software, and if deleting all records of that function across all of the places where it's reused, would that mess up tracking in the other places where the same function is reused?

2. "For database-backed inputs, this can be inferred from the existing data. But for PathInput-only functions (where inputs come from the filesystem, not the database), there are no database records to infer from. So scidb explicitly persists the expected set." Why can't scidb just use the output from the PathInput.discover() method to get the authoritative list of expected combinations from walking the file system?

# Step 16:

1. "generates_file functions (used via scihist)" Shouldn't `generates_file` live in the scidb layer? Why is the scihist layer the better choice?

# Step 19:

OK 1. `branch_params` relies only on function name and constant name, with the format `function_name.constant_name`. Whereas `version_keys` record much more data, including the function name, hash, inputs, constants, where, distribute, and as_table. Wouldn't it be more comprehensive to include all of that in the `branch_params`? And then the `version_keys` and `branch_params` concepts could be unified and the whole codebase simplified? Is that what Step 19d is attempting to do?
