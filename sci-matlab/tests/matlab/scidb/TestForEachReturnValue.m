classdef TestForEachReturnValue < matlab.unittest.TestCase
%TESTFOREACHRETURNVALUE  Tests for the table returned by scidb.for_each().

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

        function test_returns_table(testCase)
            % for_each should return a MATLAB table
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            testCase.verifyTrue(istable(result));
        end

        function test_nested_mode_has_metadata_and_output_columns(testCase)
            % Returned table has metadata columns and one output column
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            testCase.verifyTrue(ismember('subject', result.Properties.VariableNames));
            testCase.verifyTrue(ismember('session', result.Properties.VariableNames));
            testCase.verifyTrue(ismember('ProcessedSignal', result.Properties.VariableNames));
            testCase.verifyEqual(height(result), 1);
            testCase.verifyEqual(result.subject(1), 1);
            testCase.verifyEqual(string(result.session(1)), "A");
        end

        function test_nested_mode_output_value_correct(testCase)
            % Output column contains the computed data
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            computed = result.ProcessedSignal{1};
            testCase.verifyEqual(computed, [20 40 60]', 'AbsTol', 1e-10);
        end

        function test_multiple_combinations_multiple_rows(testCase)
            % Each metadata combination produces one row
            for s = [1 2 3]
                RawSignal().save(s * [1 2 3], 'subject', s, 'session', 'A');
            end

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A");

            testCase.verifyEqual(height(result), 3);
            testCase.verifyTrue(ismember(1, result.subject));
            testCase.verifyTrue(ismember(2, result.subject));
            testCase.verifyTrue(ismember(3, result.subject));
        end

        function test_multiple_metadata_keys(testCase)
            % All metadata keys appear as columns
            for s = [1 2]
                for sess = ["A", "B"]
                    RawSignal().save(s * [1 2], 'subject', s, 'session', sess);
                end
            end

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A", "B"]);

            testCase.verifyEqual(height(result), 4);
            testCase.verifyTrue(ismember('subject', result.Properties.VariableNames));
            testCase.verifyTrue(ismember('session', result.Properties.VariableNames));
        end

        function test_multiple_output_types(testCase)
            % Each output type becomes a separate column
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');

            result = scidb.for_each(@split_data, ...
                struct('x', RawSignal()), ...
                {SplitFirst(), SplitSecond()}, ...
                'subject', [1 2], ...
                'session', "A");

            testCase.verifyEqual(height(result), 2);
            testCase.verifyTrue(ismember('SplitFirst', result.Properties.VariableNames));
            testCase.verifyTrue(ismember('SplitSecond', result.Properties.VariableNames));
        end

        function test_skipped_iterations_excluded(testCase)
            % Failed iterations should not appear in the result
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            % subject=2 has no data — will be skipped
            RawSignal().save([7 8 9], 'subject', 3, 'session', 'A');

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A");

            testCase.verifyEqual(height(result), 2);
            subjects = result.subject;
            testCase.verifyTrue(ismember(1, subjects));
            testCase.verifyTrue(ismember(3, subjects));
            testCase.verifyFalse(ismember(2, subjects));
        end

        function test_all_skipped_returns_empty_table(testCase)
            % When all iterations fail, result is an empty table
            % Save nothing — all loads will fail

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A");

            testCase.verifyTrue(istable(result));
            testCase.verifyEqual(height(result), 0);
        end

        function test_dry_run_returns_empty(testCase)
            % dry_run=true should return [] not a table
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'dry_run', true, ...
                'subject', 1, ...
                'session', "A");

            testCase.verifyEmpty(result);
        end

        function test_save_false_still_returns_data(testCase)
            % save=false should still return the computed data
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');
            RawSignal().save([40 50 60], 'subject', 2, 'session', 'A');

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'save', false, ...
                'subject', [1 2], ...
                'session', "A");

            testCase.verifyTrue(istable(result));
            testCase.verifyEqual(height(result), 2);

            % Nothing should have been saved to the database
            all_saved = ProcessedSignal().load_all();
            testCase.verifyEmpty(all_saved);
        end

        function test_flatten_mode_table_outputs(testCase)
            % When fn returns a table, metadata is replicated per row
            % and data columns are expanded inline
            tbl_data = table([1.0; 2.0; 3.0], [10.0; 20.0; 30.0], ...
                'VariableNames', {'A', 'B'});
            TableVar().save(tbl_data, 'subject', 1, 'session', 'A');

            tbl_data2 = table([4.0; 5.0], [40.0; 50.0], ...
                'VariableNames', {'A', 'B'});
            TableVar().save(tbl_data2, 'subject', 2, 'session', 'A');

            result = scidb.for_each(@double_table_values, ...
                struct('x', TableVar()), ...
                {TableVar()}, ...
                'subject', [1 2], ...
                'session', "A");

            % subject=1 has 3 rows, subject=2 has 2 rows → 5 total
            testCase.verifyTrue(istable(result));
            testCase.verifyEqual(height(result), 5);
            testCase.verifyTrue(ismember('subject', result.Properties.VariableNames));
            testCase.verifyTrue(ismember('A', result.Properties.VariableNames));
            testCase.verifyTrue(ismember('B', result.Properties.VariableNames));

            % Verify subject=1 rows have doubled values
            s1_rows = result(result.subject == 1, :);
            testCase.verifyEqual(height(s1_rows), 3);
            testCase.verifyEqual(s1_rows.A, [2.0; 4.0; 6.0], 'AbsTol', 1e-10);
        end

        function test_return_does_not_affect_saves(testCase)
            % Returning data should not change what gets saved to the database
            for s = [1 2 3]
                RawSignal().save(s * [1 2 3], 'subject', s, 'session', 'A');
            end

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A");

            testCase.verifyEqual(height(result), 3);

            % All 3 outputs should be in the database
            all_saved = ProcessedSignal().load_all();
            testCase.verifyEqual(numel(all_saved), 3);
        end

        function test_return_and_save_consistent(testCase)
            % Values in the returned table should match what was saved
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            computed = result.ProcessedSignal{1};
            saved = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(computed, saved, 'AbsTol', 1e-10);
        end

    end
end
