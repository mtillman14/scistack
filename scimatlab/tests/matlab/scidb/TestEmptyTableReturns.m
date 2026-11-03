classdef TestEmptyTableReturns < matlab.unittest.TestCase
%TESTEMPTYTABLERETURNS  scidb.for_each: a function returning an EMPTY table
%   saves nothing at that combination; the other combinations save.
%
%   MATLAB's loop leaves the empty combination out (note_empty_returns in
%   +scifor/for_each.m, the rule scifor.outputs.is_empty_table owns). Before
%   2026-11-03 the empty table crossed the bridge as an empty DataFrame and
%   reached save_batch as a record value: with multi-row tables and the empty
%   one FIRST, the batch's reference schema had zero columns and the save
%   failed; otherwise it was a phantom record with no data rows.
%   Python twin: scidb/tests/test_empty_table_returns.py.

    properties
        test_dir
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function setupDatabase(testCase)
            testCase.test_dir = tempname;
            mkdir(testCase.test_dir);
            scidb.configure_database( ...
                fullfile(testCase.test_dir, 'test.duckdb'), ["subject"]);
            for s = 1:3
                RawSignal().save([s, s + 1, s + 2], 'subject', s);
            end
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            try
                scidb.get_database().close();
            catch
            end
            if isfolder(testCase.test_dir)
                rmdir(testCase.test_dir, 's');
            end
        end
    end

    methods (Test)
        function test_empty_first_multi_row_saves_the_rest(testCase)
            scidb.for_each(@multi_row_empty_at_1, struct('x', RawSignal()), ...
                {ProcessedSignal()}, 'subject', [1 2 3]);

            testCase.verifyError(@() ProcessedSignal().load('subject', 1), ...
                'scidb:NotFoundError');
            for s = 2:3
                r = ProcessedSignal().load('subject', s);
                testCase.verifyEqual(height(r.data), 3);
                testCase.verifyEqual(double(r.data.v(:))', [s, s + 1, s + 2], 'AbsTol', 1e-10);
            end
        end

        function test_empty_in_spread_mode_saves_nothing_there(testCase)
            scidb.for_each(@one_row_empty_at_2, struct('x', RawSignal()), ...
                {ProcessedSignal()}, 'subject', [1 2 3]);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2), ...
                'scidb:NotFoundError');
            r = ProcessedSignal().load('subject', 3);
            testCase.verifyEqual(double(r.data.v), 4.0, 'AbsTol', 1e-10);
        end

        function test_every_combination_empty_saves_nothing(testCase)
            scidb.for_each(@(x) table(), struct('x', RawSignal()), ...
                {ProcessedSignal()}, 'subject', [1 2 3]);

            testCase.verifyError(@() ProcessedSignal().load('subject', 1), ...
                'scidb:NotFoundError');
        end
    end
end


function out = multi_row_empty_at_1(x)
    if x(1) == 1
        out = table();
    else
        out = table(x(:), 'VariableNames', {'v'});
    end
end


function out = one_row_empty_at_2(x)
    if x(1) == 2
        out = table();
    else
        out = table(mean(x), 'VariableNames', {'v'});
    end
end
