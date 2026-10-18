classdef TestForEachParity < matlab.unittest.TestCase
%TESTFOREACHPARITY  scidb.for_each accepts a scifor.for_each-shaped call.
%
%   - outputs={...} as a name-value pair (scifor style) as well as positional
%   - a class's column_name Constant property renames its result column but
%     the output still saves and loads as its own type
%   - categorical=true (a scifor option) converts the RETURNED table's
%     metadata columns, after saving

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
                fullfile(testCase.test_dir, 'test.duckdb'), ...
                ["subject", "session"]);
            for s = [1 2]
                RawSignal().save(3, 'subject', s, 'session', 'A');
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

        function test_outputs_as_name_value(testCase)
            result = scidb.for_each(@double_values, struct('x', RawSignal()), ...
                outputs={ProcessedSignal()}, subject=[1 2], session="A");
            testCase.verifyTrue(ismember('ProcessedSignal', result.Properties.VariableNames));
            loaded = ProcessedSignal().load(subject=1, session="A");
            testCase.verifyEqual(loaded.data, 6);
        end

        function test_column_name_renames_result_column(testCase)
            result = scidb.for_each(@double_values, struct('x', RawSignal()), ...
                {RenamedScalarVar()}, subject=[1 2], session="A");
            testCase.verifyTrue(ismember('renamed_val', result.Properties.VariableNames));
            testCase.verifyFalse(ismember('RenamedScalarVar', result.Properties.VariableNames));
            loaded = RenamedScalarVar().load(subject=2, session="A");
            testCase.verifyEqual(loaded.data, 6);
        end

        function test_categorical_applies_to_returned_table(testCase)
            result = scidb.for_each(@double_values, struct('x', RawSignal()), ...
                {ProcessedSignal()}, subject=[1 2], session="A", categorical=true);
            testCase.verifyTrue(iscategorical(result.subject));
            testCase.verifyTrue(iscategorical(result.session));
            % Saving was unaffected by the conversion.
            loaded = ProcessedSignal().load(subject=2, session="A");
            testCase.verifyEqual(loaded.data, 6);
        end

        function test_same_call_runs_on_scifor(testCase)
            % The drop-in direction: a scidb call shape (positional outputs,
            % save=, db=) on scifor with a table input.
            scifor.set_schema(["subject", "session"]);
            tbl = table([1;2], ["A";"A"], [3;3], ...
                'VariableNames', {'subject', 'session', 'value'});
            result = scifor.for_each(@double_values, struct('x', tbl), ...
                {RenamedScalarVar()}, save=true, db=[], subject=[1 2], session="A");
            testCase.verifyEqual(result.renamed_val, [6; 6]);
        end

    end

end
