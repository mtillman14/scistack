Here is Claude's plan:  
╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌
Plan: Sync MATLAB scidb.for_each with Python Implementation

Context

The user reported a bug: when a second for_each loads data saved by a first for_each, the loaded table contains internal metadata columns (x**fn, x**inputs, configNamesStruct) instead of just the actual data.
This happens because MATLAB's thunk_outputs_to_table() includes ALL metadata fields, while Python's equivalent \_stringify_meta() strips internal keys and constants.

A comprehensive audit revealed multiple gaps between Python's scidb.for_each and MATLAB's. This plan addresses them in priority order.

---

Tier 1: Fix metadata stripping on load (fixes the user's bug)

File: /workspace/sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m

Change 1a: Add strip_internal_meta() helper function

Add a new local function (after is_metadata_compatible around line 1216) that mirrors Python's \_stringify_meta() at scidb/src/scidb/foreach.py:617-633. It should:

1.  Drop all fields starting with x** (MATLAB's jsondecode sanitizes **fn → x\_\_fn)
2.  Drop fields that appear in the x\_\_constants JSON (so constant inputs like configNamesStruct don't pollute the input table)
3.  Return a cleaned struct with only schema keys and user metadata

Change 1b: Call strip_internal_meta() in thunk_outputs_to_table()

At line 545 (after n = numel(results), before the all_tables check), strip metadata from all results:

for i = 1:n
results(i).metadata = strip_internal_meta(results(i).metadata);
end

This covers both the "table data" branch (lines 556-600) and the "non-table data" branch (lines 601-625).

Python reference: \_stringify_meta() at foreach.py:617-633

---

Tier 2: Add \_\_constants to config version keys

File: /workspace/sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m

Change 2: Update build_config_nv() (lines 1375-1407)

Add \_\_constants JSON encoding of non-loadable, metadata-compatible inputs. This mirrors Python's ForEachConfig.\_get_direct_constants() at foreach_config.py:56-59 and to_version_keys() lines 42-44.

Add after the \_\_inputs block (~line 1387):

% Serialize metadata-compatible constants as **constants JSON
const_struct = struct();
has_const = false;
for p = 1:numel(input_names)
if ~loadable_idx(p)
val = inputs.(input_names{p});
if is_metadata_compatible(val)
const_struct.(input_names{p}) = val;
has_const = true;
end
end
end
if has_const
nv{end+1} = '**constants';
nv{end+1} = jsonencode(const_struct);
end

Why this matters: Without **constants, the Tier 1 strip_internal_meta() function cannot identify which metadata keys came from constants (like configNamesStruct). With **constants stored, the stripping logic can
parse it and remove those keys.

Note: For records already saved WITHOUT \_\_constants, the constant keys will still appear in loaded data. This is acceptable — new saves will have proper stripping.

---

Tier 3: Variant tracking infrastructure (deferred — design discussion needed)

Python's for_each (lines 208-322, 823-895) has full variant tracking:

- **record_id / **rid\_{param} columns on loaded DataFrames
- rid_to_bp mapping for branch_params propagation
- Schema extension for \__rid_\* keys
- **branch_params merging and **upstream tracking on save

Status: ~150+ lines of MATLAB code needed. This is a separate effort requiring its own design and testing. It only matters when multiple variants exist at the same schema location.

User note: "scidb should not be interacting with lineage" — variant tracking is about version disambiguation (scidb's responsibility), not lineage (scilineage's). But the implementation is complex enough to
warrant a separate PR.

---

Tier 4: scihist integration hooks (deferred)

- \_pre_combo_hook — for scihist's skip_computed (Python lines 84, 306-309)
- \_inject_combo_metadata — for scihist's generates_file (Python lines 84, 346-349)

Status: Only needed when scihist MATLAB integration is built.

---

Tier 5: Minor API gaps (deferred)

- BaseVariable.save() missing index parameter
- BaseVariable.load() missing loc/iloc parameters
- BaseVariable.load_all() missing include_record_id parameter
- Per-combo loaders for classes without load_all (beyond PathInput)

Status: Low priority. Add as needed.

---

Files to modify
┌────────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────┬───────┐
│ File │ Changes │ Tier │
├────────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────┼───────┤
│ sci-matlab/src/sci_matlab/matlab/+scidb/for_each.m │ Add strip_internal_meta(), call it in thunk_outputs_to_table(), add \_\_constants to build_config_nv() │ 1 & 2 │
└────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────┴───────┘
Verification

Since we cannot run MATLAB directly, we will:

1.  Provide the modified for_each.m code for copy-paste
2.  Include a diagnostic snippet the user can add to verify metadata fields before/after stripping
3.  The user tests by re-running their two for_each commands and confirming x**fn, x**inputs, and configNamesStruct no longer appear in the loaded data
