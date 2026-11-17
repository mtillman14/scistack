function cfg = glue_test_read_factor(path)
%GLUE_TEST_READ_FACTOR  Test glue: parse a JSON config file, counting calls.
%   A PathInput-fed glue (issue #21) receives the RESOLVED path and returns
%   what the consuming function gets. Increments GLUE_TEST_CALLS so a test
%   can assert it ran once per distinct file, not once per combo.
    global GLUE_TEST_CALLS %#ok<GVMIS>
    if isempty(GLUE_TEST_CALLS)
        GLUE_TEST_CALLS = 0;
    end
    GLUE_TEST_CALLS = GLUE_TEST_CALLS + 1;
    cfg = jsondecode(fileread(char(path)));
end
