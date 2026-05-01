# scifor

1. Why no "EachOf" expansion?
2. Where is the description of PathInput filesystem discovery?

# scidb

## Step 1

1. Should "EachOf" expansion be handled here, or in scifor?

## Step 2 (also tied in with Step 9)

1. If I'm understanding correctly, this results in the cartesian product of all the values found at each schema level? The list of metadata combinations should be filtered to only those that exist, e.g. if subject 1 has trial 1 but subject 2 does not have trial 1, remove (subject 2, trial 1) from the list.
2. The final step of metadata iterables should filter for the metadata that actually exist for the full set of input variables. If input1 has a value for subject=1 but input2 does not, then skip subject=1.

## Step 3

1. The two cases of PathInput filesystem discovery need better explanations, I don't fully understand how this works still.

## Step 6

1. Please explain how view_name() works and where it lives.

## Step 8:

1. Please elaborate on what is meant by "computation fingerprinting system".
2. "The function hash is computed by \_compute_fn_hash() (lines 9–21 of foreach_config.py): it takes inspect.getsource(fn), hashes it with SHA-256, and truncates to 16 hex characters" Is truncating a SHA-256 hash to 16 hex characters enough to guarantee uniqueness? This feels like this might lead to future problems. How does Git handle hash generation? Their hashes are pretty short.

## Step 10:

1. You mention iterating over the inputs dict, but the explanation is not super clear on where the inputs dict is created or what its exact format is.
2. \_\_record_id and \_\_branch_params are mentioned here for the first time. Again, it's not clear what these are, what their purpose is, where they come from, and what the format is.
3. Need details of what the database tables formats are, what each column is, and how the data in each column is formatted/generated/loaded/saved.

## Step 11:

1. As the docs currently say, this step is the heart of variant disambiguation. It's also fairly technical and dense. This section needs more explanation to clarify in more plain language exactly how these mechanisms operate, and what the purpose of each field/mechanism is.

## Step 12:

1. The docs say: "If not all schema keys are being iterated over — for example, iterating over subject but not session when the schema is [subject, session] — then lower-level records should be aggregated into multi-row DataFrames rather than separated. In this case, rid expansion is skipped entirely, \__rid_\* columns are stripped from all DataFrames, and base_combos is used directly." It's unclear why this logic is needed, please explain the rationale.
2. Please explain the whole section in more lay terms. Don't sacrifice any accuracy, but the motivation behind the architectural decisions needs to be clearer, to convince skeptical readers that this system is doing what it claims to be doing.

## Steps 13-16:

1. Similar improvement needed as in Step 12. More justification for why things are being done and how they work in lay language to explain the software to a new, skeptical reader. That reader should be able to follow these explanations step by step to understand the implementation, and also help troubleshoot issues.

## Step 18:

1. After Steps 13-16 are expanded on, it should similarly be made clear here why this schema reversion is necessary.

## Later sections:

1. "The variant tracking system (branch_params)" This is starting to provide the sort of helpful information that the above sections were missing. But for example in the "How it flows:" section, the way in which record ID's and disambiguation and variant tracking works are only alluded to, not fully explained.
2. The concepts of \_\_record_id and \_\_branch_params, and \_rid_to_bp, etc. are the most confusing and poorly explained topics, even with the more concrete explanations in the later sections.
