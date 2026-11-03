classdef TestSciforEmptyTableReturns < matlab.unittest.TestCase
%TESTSCIFOREMPTYTABLERETURNS  A function returning an EMPTY table (table(),
%   or 0 rows with variables) has nothing to save at that combination.
%
%   The rule is scifor.outputs.is_empty_table (Python), applied by the
%   MATLAB loop as height(t) == 0 (note_empty_returns in +scifor/for_each.m):
%   every output empty -> one no-data combination; some empty -> those
%   outputs are left out of their table; the summary WARNs per output.
%   Before 2026-11-03 one empty table among 1-row tables made the flatten
%   vertcat fail (the 0-row part had only the metadata columns), so the
%   whole run errored.

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.set_schema(string.empty(1, 0));
        end
    end

    methods (Test)

        function test_empty_table_skips_that_combination(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1; 2; 3], [10; 20; 30], 'VariableNames', {'subject', 'value'});
            box = containers.Map();
            result = scifor.for_each(@empty_at_20, struct('x', tbl), ...
                'subject', [1 2 3], '_summary_fn', @(s) store(box, s));

            tc.verifyTrue(istable(result));
            tc.verifyEqual(height(result), 2);
            tc.verifyFalse(any(result.subject == 2));
            tc.verifyEqual(result.v, [100; 300]);
            s = box('summary');
            tc.verifyEqual(double(s.completed), 2);
            tc.verifyEqual(double(s.no_data), 1);
            tc.verifyEqual(double(s.failed), 0);
        end

        function test_zero_row_table_with_variables_is_empty(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1; 2; 3], [10; 20; 30], 'VariableNames', {'subject', 'value'});
            result = scifor.for_each(@zero_rows_at_20, struct('x', tbl), subject=[1 2 3]);

            tc.verifyEqual(height(result), 2);
            tc.verifyFalse(any(result.subject == 2));
        end

        function test_every_combination_empty(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1; 2], [10; 20], 'VariableNames', {'subject', 'value'});
            box = containers.Map();
            result = scifor.for_each(@(x) table(), struct('x', tbl), ...
                'subject', [1 2], '_summary_fn', @(s) store(box, s));

            tc.verifyTrue(isempty(result) || height(result) == 0);
            s = box('summary');
            tc.verifyEqual(double(s.completed), 0);
            tc.verifyEqual(double(s.no_data), 2);
        end

        function test_one_empty_output_keeps_the_other(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1; 2; 3], [10; 20; 30], 'VariableNames', {'subject', 'value'});
            box = containers.Map();
            [a, b] = scifor.for_each(@a_empty_at_20, struct('x', tbl), ...
                'outputs', {'A', 'B'}, 'subject', [1 2 3], '_summary_fn', @(s) store(box, s));

            tc.verifyEqual(height(a), 2, 'A has no row where it was empty');
            tc.verifyFalse(any(a.subject == 2));
            tc.verifyEqual(height(b), 3, 'B keeps every combination');
            s = box('summary');
            tc.verifyEqual(double(s.completed), 3);
        end
    end
end


function out = empty_at_20(x)
    if x == 20
        out = table();
    else
        out = table(x * 10, 'VariableNames', {'v'});
    end
end


function out = zero_rows_at_20(x)
    if x == 20
        out = table('Size', [0 1], 'VariableTypes', {'double'}, 'VariableNames', {'v'});
    else
        out = table(x * 10, 'VariableNames', {'v'});
    end
end


function [a, b] = a_empty_at_20(x)
    if x == 20
        a = table();
    else
        a = table(x, 'VariableNames', {'a'});
    end
    b = table(x * 10, 'VariableNames', {'b'});
end


function store(box, s)
    box('summary') = s;
end
