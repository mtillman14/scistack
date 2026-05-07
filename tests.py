1. Run all skip_computed tests to ensure we didn't break anything else:                                                                                        
  python -m pytest scihist-lib/tests/test_skip_computed.py -xvs                                                                                                  
  2. Run variant/branch_params tests to ensure legitimate variants still work:                                                                                   
  python -m pytest scidb/tests/test_branch_params.py -xvs                                                                                                        
  3. Run integration tests:                                                                                                                                      
  python -m pytest scidb/tests/test_integration.py -xvs