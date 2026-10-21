# Constants identity in the GUI layer + crashed-run reporting (2026-10-21)

Trigger: node run died with `TypeError: unhashable type: 'dict'` in
`execution_service._apply_manual_output_types`; the GUI spun until cancel.

1. Route every GUI grouping key over constants through
   `scidb.provenance.constants_identity_key` (execution_service x2,
   code_export_service, matlab_command_service, variant_resolver x4,
   scidb/inspect/graph.py).
2. AST guard `test_no_hand_built_constants_key` over all shipped source.
3. Dict-constant regression tests for `_apply_manual_output_types`,
   `deduplicate_variants`, `merge_pending_constants`.
4. `api/run._report_crashed_run`: anything escaping `_run_in_thread` is
   logged with traceback and, if the run is still registered, ends with a
   failed `run_done`.
5. Docs: constants-are-not-scalars.md §2a; gui-manual-testing-todo §0zzzzzz.
