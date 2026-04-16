classdef TestColumnSelection < matlab.unittest.TestCase
%TESTCOLUMNSELECTION  Edge-case tests for column selection in for_each().
%
%   Covers cases beyond the basics in TestForEach:
%   - Different column types (string, numeric, mixed)
%   - Scalar vs. vector results (single-row tables)
%   - Selecting one, two, three, or all columns
%   - Column order in multi-column selection
%   - One nonexistent column in a multi-column request
%   - Column selection combined with Fixed inputs
%   - The preload=false per-iteration load path
%   - Column selection in parallel mode
%   - Multi-column selection across multiple iterations

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

        % -----------------------------------------------------------------
        % Column types
        % -----------------------------------------------------------------

        function test_single_string_column(testCase)
            % A string column should be extracted and passed to the function
            % as an array of strings (or char cells).  Verified indirectly:
            % count_elements() on a 3-row string column returns 3.
            input_tbl = table(["apple"; "banana"; "cherry"], [1.0; 2.0; 3.0], ...
                'VariableNames', {'label', 'value'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@count_elements, ...
                struct('x', RawSignal("label")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data, 3.0, 'AbsTol', 1e-10);
        end

        function test_single_numeric_column_returns_array_not_table(testCase)
            % Selecting a single numeric column must return a plain array,
            % not a table — even when the stored data was a table.
            input_tbl = table([7.0; 8.0; 9.0], [0.1; 0.2; 0.3], ...
                'VariableNames', {'signal', 'noise'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal("signal")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyFalse(istable(result.data));
            testCase.verifyEqual(result.data, [7.0; 8.0; 9.0], 'AbsTol', 1e-10);
        end

        function test_mixed_type_columns_subtable_has_correct_width(testCase)
            % Selecting a numeric column and a string column together should
            % produce a 2-column subtable.  Verified via table_width_fn().
            input_tbl = table([1.0; 2.0], ["alpha"; "beta"], ...
                'VariableNames', {'score', 'label'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@table_width_fn, ...
                struct('x', RawSignal(["score", "label"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data, 2.0, 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % Scalar vs. vector (single-row tables)
        % -----------------------------------------------------------------

        function test_single_row_single_column_gives_scalar(testCase)
            % A single-row table with single-column selection should deliver
            % a scalar to the function, not a 1-element array wrapped in a table.
            input_tbl = table(42.0, 99.0, 'VariableNames', {'col_a', 'col_b'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal("col_a")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyFalse(istable(result.data));
            testCase.verifyEqual(numel(result.data), 1);
            testCase.verifyEqual(double(result.data), 42.0, 'AbsTol', 1e-10);
        end

        function test_single_row_multiple_columns_gives_one_row_table(testCase)
            % A single-row table with multi-column selection should produce
            % a 1-row subtable (not a scalar or a vector).
            input_tbl = table(3.14, 2.72, 1.41, ...
                'VariableNames', {'pi_col', 'e_col', 'rt2_col'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["pi_col", "e_col"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(height(result.data), 1);
            testCase.verifyEqual(width(result.data), 2);
            testCase.verifyEqual(result.data.Properties.VariableNames, {'pi_col', 'e_col'});
            testCase.verifyEqual(result.data.pi_col, 3.14, 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % Selecting different column counts
        % -----------------------------------------------------------------

        function test_three_of_four_columns(testCase)
            % Selecting 3 of 4 columns produces a 3-column subtable;
            % the unselected column must not appear in the result.
            input_tbl = table([1.0; 2.0], [3.0; 4.0], [5.0; 6.0], [7.0; 8.0], ...
                'VariableNames', {'a', 'b', 'c', 'd'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["a", "b", "c"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(width(result.data), 3);
            testCase.verifyEqual(result.data.Properties.VariableNames, {'a', 'b', 'c'});
            testCase.verifyFalse(ismember('d', result.data.Properties.VariableNames));
        end

        function test_select_all_columns_by_name(testCase)
            % Explicitly naming every column is allowed and produces a
            % subtable equal in width to the original table.
            input_tbl = table([10.0; 20.0], [30.0; 40.0], ...
                'VariableNames', {'x_col', 'y_col'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["x_col", "y_col"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(width(result.data), 2);
            testCase.verifyTrue(ismember('x_col', result.data.Properties.VariableNames));
            testCase.verifyTrue(ismember('y_col', result.data.Properties.VariableNames));
            % Values must be correct too
            testCase.verifyEqual(result.data.x_col, [10.0; 20.0], 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % Column order
        % -----------------------------------------------------------------

        function test_column_order_preserved_in_multi_selection(testCase)
            % The output subtable must use the requested column order,
            % not the order in the stored table.
            input_tbl = table([1.0; 2.0], [10.0; 20.0], [100.0; 200.0], ...
                'VariableNames', {'a', 'b', 'c'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            % Request columns in reverse order: c, a (skipping b entirely)
            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["c", "a"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            names = result.data.Properties.VariableNames;
            testCase.verifyEqual(names{1}, 'c');  % c must come first
            testCase.verifyEqual(names{2}, 'a');  % a must come second
            testCase.verifyFalse(ismember('b', names));
        end

        % -----------------------------------------------------------------
        % Error / skip behaviour
        % -----------------------------------------------------------------

        function test_one_of_multiple_selected_columns_nonexistent_skips(testCase)
            % If any column in a multi-column selection does not exist in
            % the loaded data, the entire iteration must be skipped.
            input_tbl = table([1.0; 2.0], [3.0; 4.0], ...
                'VariableNames', {'col_a', 'col_b'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["col_a", "does_not_exist"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            results = ProcessedSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEmpty(results);
        end

        function test_only_requested_column_reaches_function(testCase)
            % The function must NOT receive the full table when a single
            % column is specified.  sum_array([1;2;3]) succeeds and returns 6;
            % sum_array(full_table) would error.  A successful save proves
            % the column was actually extracted before calling the function.
            input_tbl = table([1.0; 2.0; 3.0], ["x"; "y"; "z"], ...
                'VariableNames', {'num_col', 'str_col'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            % sum_array works on numeric arrays only — would fail on a table
            scidb.for_each(@sum_array, ...
                struct('x', RawSignal("num_col")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            % sum([1;2;3]) = 6
            testCase.verifyEqual(result.data, 6.0, 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % Fixed inputs + column selection
        % -----------------------------------------------------------------

        function test_column_selection_with_fixed_input(testCase)
            % Fixed and column selection can be combined: always load from
            % session "BL" and extract the "signal" column.
            input_tbl = table([10.0; 20.0; 30.0], [0.1; 0.2; 0.3], ...
                'VariableNames', {'signal', 'noise'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'BL');

            scidb.for_each(@sum_array, ...
                struct('x', scidb.Fixed(RawSignal("signal"), 'session', 'BL')), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            % sum([10; 20; 30]) = 60
            testCase.verifyEqual(result.data, 60.0, 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % preload=false (per-iteration load path)
        % -----------------------------------------------------------------

        function test_column_selection_preload_false(testCase)
            % Column selection must also work through the per-iteration load
            % path (preload=false), not only through the bulk-preload path.
            input_tbl = table([5.0; 6.0; 7.0], [100.0; 200.0; 300.0], ...
                'VariableNames', {'col_a', 'col_b'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@sum_array, ...
                struct('x', RawSignal("col_a")), ...
                {ProcessedSignal()}, ...
                'preload', false, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            % sum([5; 6; 7]) = 18
            testCase.verifyEqual(result.data, 18.0, 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % parallel=true
        % -----------------------------------------------------------------

        function test_column_selection_parallel_mode(testCase)
            % Column selection must work in parallel mode (Phase A resolves
            % all inputs including column selection before the parfor).
            tbl1 = table([1.0; 2.0], [10.0; 20.0], 'VariableNames', {'col_a', 'col_b'});
            tbl2 = table([3.0; 4.0], [30.0; 40.0], 'VariableNames', {'col_a', 'col_b'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 2, 'session', 'A');

            scidb.for_each(@sum_array, ...
                struct('x', RawSignal("col_a")), ...
                {ProcessedSignal()}, ...
                'parallel', true, ...
                'subject', [1 2], ...
                'session', "A");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            r2 = ProcessedSignal().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(r1.data, 3.0, 'AbsTol', 1e-10);   % sum([1;2])
            testCase.verifyEqual(r2.data, 7.0, 'AbsTol', 1e-10);   % sum([3;4])
        end

        % -----------------------------------------------------------------
        % Multi-column selection across multiple iterations
        % -----------------------------------------------------------------

        function test_multi_column_selection_across_iterations(testCase)
            % Multi-column selection must produce the correct subtable at
            % every iteration, with the correct column names and values.
            tbl1 = table([1.0; 2.0], [10.0; 20.0], [100.0; 200.0], ...
                'VariableNames', {'a', 'b', 'c'});
            tbl2 = table([3.0; 4.0], [30.0; 40.0], [300.0; 400.0], ...
                'VariableNames', {'a', 'b', 'c'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 2, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["a", "b"])), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            r2 = ProcessedSignal().load('subject', 2, 'session', 'A');

            % Subject 1: columns a and b, no column c
            testCase.verifyTrue(istable(r1.data));
            testCase.verifyEqual(width(r1.data), 2);
            testCase.verifyFalse(ismember('c', r1.data.Properties.VariableNames));
            testCase.verifyEqual(r1.data.a, [1.0; 2.0], 'AbsTol', 1e-10);
            testCase.verifyEqual(r1.data.b, [10.0; 20.0], 'AbsTol', 1e-10);

            % Subject 2: same columns, different values
            testCase.verifyTrue(istable(r2.data));
            testCase.verifyEqual(r2.data.a, [3.0; 4.0], 'AbsTol', 1e-10);
            testCase.verifyEqual(r2.data.b, [30.0; 40.0], 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % Correct values are preserved end-to-end
        % -----------------------------------------------------------------

        function test_selected_column_values_are_correct(testCase)
            % Verify that the function receives the exact values from the
            % selected column (not zeros, not values from another column).
            input_tbl = table([100.0; 200.0; 300.0], [0.1; 0.2; 0.3], ...
                'VariableNames', {'signal', 'noise'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            % sum([100;200;300]) = 600; sum([0.1;0.2;0.3]) ≈ 0.6
            scidb.for_each(@sum_array, ...
                struct('x', RawSignal("signal")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data, 600.0, 'AbsTol', 1e-10);
        end

        function test_multi_column_values_are_correct(testCase)
            % Verify that the correct column values appear in the subtable
            % after multi-column selection.
            input_tbl = table([5.0; 6.0], [50.0; 60.0], [500.0; 600.0], ...
                'VariableNames', {'a', 'b', 'c'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["a", "c"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyFalse(ismember('b', result.data.Properties.VariableNames));
            testCase.verifyEqual(result.data.a, [5.0; 6.0], 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.c, [500.0; 600.0], 'AbsTol', 1e-10);
        end

    end
end
