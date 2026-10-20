function y = fail_on_subject_two(x)
%FAIL_ON_SUBJECT_TWO  Test helper: doubles x, but raises for the input [4 5 6]
%   (subject 2 in TestIterationTally) so one iteration fails.
    if isequal(x(:)', [4 5 6])
        error('test:failOnSubjectTwo', 'subject two has no data here');
    end
    y = x * 2;
end
