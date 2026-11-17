function y = glue_test_double_value(x)
%GLUE_TEST_DOUBLE_VALUE  Test glue for a CONSTANT input: doubles it.
%   In a MATLAB run, MATLAB glue on a constant runs before Python's prepare
%   (+scidb/for_each.m glue_constants_before_prepare).
    y = x * 2;
end
