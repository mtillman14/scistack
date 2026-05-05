pytest scihist-lib/tests/test_unified_variant_tracking.py::TestVersionKeysCompleteness -v

  # Branch params accumulation
  pytest scihist-lib/tests/test_unified_variant_tracking.py::TestBranchParamsAccumulation -v

  # Fixed input tracking
  pytest scihist-lib/tests/test_unified_variant_tracking.py::TestFixedInputTracking -v

  # Graceful degradation
  pytest scidb/tests/test_optional_lineage_dependency.py::TestScidbWithoutLineage -v