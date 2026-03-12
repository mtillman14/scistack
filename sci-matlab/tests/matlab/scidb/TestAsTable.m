classdef TestAsTable < matlab.unittest.TestCase
%TESTASTABLE  Tests for as_table parameter in load() and for_each().

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

        function test_load_default_returns_table(testCase)
            %% load() with multiple matches returns a table by default
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');

            tbl = RawSignal().load('subject', 1);
            testCase.verifyTrue(istable(tbl));
            testCase.verifyEqual(height(tbl), 2);
            testCase.verifyTrue(ismember('subject', tbl.Properties.VariableNames));
            testCase.verifyTrue(ismember('session', tbl.Properties.VariableNames));
            testCase.verifyTrue(ismember('RawSignal', tbl.Properties.VariableNames));
        end

        function test_load_as_table_multi_result(testCase)
            %% load(as_table=true) with multiple matches returns a table
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');

            tbl = RawSignal().load('as_table', true, 'subject', 1);
            testCase.verifyTrue(istable(tbl));
            testCase.verifyEqual(height(tbl), 2);
            testCase.verifyTrue(ismember('subject', tbl.Properties.VariableNames));
            testCase.verifyTrue(ismember('session', tbl.Properties.VariableNames));
            testCase.verifyTrue(ismember('RawSignal', tbl.Properties.VariableNames));
        end

        function test_load_single_result_returns_raw_data(testCase)
            %% load() with single match returns raw data
            ScalarVar().save(42, 'subject', 1, 'session', 'A');
            result = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, 42);
        end

        function test_load_as_table_false_returns_raw_data(testCase)
            %% load(as_table=false) with multiple matches returns raw data
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');

            result = ScalarVar().load('as_table', false, 'subject', 1);
            testCase.verifyTrue(isnumeric(result));
            testCase.verifyEqual(numel(result), 2);
            testCase.verifyEqual(sort(result), [10; 20]);
        end

        function test_load_as_table_false_array_data_returns_cell(testCase)
            %% load(as_table=false) with non-scalar data returns cell array
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');

            result = RawSignal().load('as_table', false, 'subject', 1);
            testCase.verifyTrue(iscell(result));
            testCase.verifyEqual(numel(result), 2);
        end

        function test_loadall_as_table_true_multiple_versions(testCase)
            %% load_all(as_table=true) with multiple saves returns all records
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'A');

            result = RawSignal().load_all('as_table', true, 'subject', 1, 'session', 'A');
            testCase.verifyTrue(isa(result, 'table'));
            testCase.verifyEqual(height(result), 2);
        end

        function test_loadall_as_table_false_multiple_versions(testCase)
            %% load_all(as_table=false) with multiple saves returns ThunkOutput array
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'A');

            result = RawSignal().load_all('as_table', false, 'subject', 1, 'session', 'A');
            testCase.verifyTrue(isa(result, 'scidb.ThunkOutput'));
            testCase.verifyEqual(numel(result), 2);

            % test default is 'false'
            result = RawSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyTrue(isa(result, 'scidb.ThunkOutput'));
            testCase.verifyEqual(numel(result), 2);
        end

        function test_for_each_as_table(testCase)
            %% for_each with as_table converts multi-result to table
            %  Save per-session data, iterate per subject
            RawSignal().save([1 2], 'subject', 1, 'session', 'A');
            RawSignal().save([3 4], 'subject', 1, 'session', 'B');

            received_table = [];

            function result = table_consumer(values)
                received_table = values;
                result = sum(cellfun(@(x) sum(x), values.RawSignal));
            end

            scidb.for_each(@table_consumer, ...
                struct('values', RawSignal()), ...
                {ScalarVar()}, ...
                'as_table', ["values"], ...
                'subject', 1);

            testCase.verifyTrue(istable(received_table));
            testCase.verifyEqual(height(received_table), 2);
        end

        % -----------------------------------------------------------------
        % as_table + column selection interaction
        % -----------------------------------------------------------------

        function test_as_table_with_single_column_selection_returns_table(testCase)
            %% as_table=true + single column selection should return a table
            %  with metadata columns AND the selected data column — not a
            %  raw vector.
            tbl1 = table([10.0; 20.0], [0.1; 0.2], ...
                'VariableNames', {'signal', 'noise'});
            tbl2 = table([30.0; 40.0], [0.3; 0.4], ...
                'VariableNames', {'signal', 'noise'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 1, 'session', 'B');

            received = [];

            function result = capture_input(values)
                received = values;
                result = 0;
            end

            scidb.for_each(@capture_input, ...
                struct('values', RawSignal("signal")), ...
                {ScalarVar()}, ...
                'as_table', true, ...
                'subject', 1);

            % Must be a table (as_table controls this)
            testCase.verifyTrue(istable(received), ...
                'as_table=true should produce a table even with column selection');
            % Must have metadata columns
            testCase.verifyTrue(ismember('subject', received.Properties.VariableNames), ...
                'Table should contain subject metadata column');
            testCase.verifyTrue(ismember('session', received.Properties.VariableNames), ...
                'Table should contain session metadata column');
            % Must have the selected data column
            testCase.verifyTrue(ismember('signal', received.Properties.VariableNames), ...
                'Table should contain the selected data column');
            % Must NOT have the unselected data column
            testCase.verifyFalse(ismember('noise', received.Properties.VariableNames), ...
                'Table should NOT contain unselected data columns');
            % 2 sessions x 2 rows each = 4 rows total (flattened)
            testCase.verifyEqual(height(received), 4);
            % Verify data values are correct (sort since row order is not guaranteed)
            testCase.verifyEqual(sort(received.signal), [10.0; 20.0; 30.0; 40.0], 'AbsTol', 1e-10);
        end

        function test_as_table_with_multi_column_selection_returns_table(testCase)
            %% as_table=true + multi-column selection should return a table
            %  with metadata columns AND only the selected data columns.
            tbl1 = table([1.0; 2.0], [10.0; 20.0], [100.0; 200.0], ...
                'VariableNames', {'a', 'b', 'c'});
            tbl2 = table([3.0; 4.0], [30.0; 40.0], [300.0; 400.0], ...
                'VariableNames', {'a', 'b', 'c'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 1, 'session', 'B');

            received = [];

            function result = capture_input(values)
                received = values;
                result = 0;
            end

            scidb.for_each(@capture_input, ...
                struct('values', RawSignal(["a", "b"])), ...
                {ScalarVar()}, ...
                'as_table', true, ...
                'subject', 1);

            % Must be a table
            testCase.verifyTrue(istable(received));
            % Must have metadata columns
            testCase.verifyTrue(ismember('subject', received.Properties.VariableNames));
            testCase.verifyTrue(ismember('session', received.Properties.VariableNames));
            % Must have selected data columns
            testCase.verifyTrue(ismember('a', received.Properties.VariableNames));
            testCase.verifyTrue(ismember('b', received.Properties.VariableNames));
            % Must NOT have unselected data column
            testCase.verifyFalse(ismember('c', received.Properties.VariableNames));
            % 2 sessions x 2 rows = 4 rows total
            testCase.verifyEqual(height(received), 4);
            % Verify values
            testCase.verifyEqual(sort(received.a), [1.0; 2.0; 3.0; 4.0], 'AbsTol', 1e-10);
            testCase.verifyEqual(sort(received.b), [10.0; 20.0; 30.0; 40.0], 'AbsTol', 1e-10);
        end

        function test_as_table_column_types(testCase)
            %% as_table=true should return string columns for string metadata,
            %  numeric columns for numeric metadata, and numeric data columns
            %  for scalar numeric data — not cell arrays.
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 2, 'session', 'A');
            ScalarVar().save(30, 'subject', 1, 'session', 'B');

            % --- Test via load(as_table=true) ---
            tbl = ScalarVar().load('as_table', true);
            testCase.verifyTrue(isnumeric(tbl.subject), ...
                'Numeric metadata column should be numeric, not cell');
            testCase.verifyTrue(isstring(tbl.session), ...
                'String metadata column should be string, not cell');
            testCase.verifyTrue(isnumeric(tbl.ScalarVar), ...
                'Scalar numeric data column should be numeric, not cell');
            testCase.verifyEqual(sort(tbl.ScalarVar), [10; 20; 30]);

            % --- Test via for_each(as_table=true) with scalar data ---
            received = [];

            function result = capture_table(values)
                received = values;
                result = 0;
            end

            scidb.for_each(@capture_table, ...
                struct('values', ScalarVar()), ...
                {RawSignal()}, ...
                'as_table', true, ...
                'subject', 1);

            testCase.verifyTrue(istable(received));
            testCase.verifyTrue(isnumeric(received.subject), ...
                'for_each as_table: numeric metadata column should be numeric, not cell');
            testCase.verifyTrue(isstring(received.session), ...
                'for_each as_table: string metadata column should be string, not cell');
            testCase.verifyTrue(isnumeric(received.ScalarVar), ...
                'for_each as_table: scalar numeric data column should be numeric, not cell');
        end

        function test_as_table_array_data_stays_cell(testCase)
            %% as_table=true with non-scalar data should keep data as cell column
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');

            tbl = RawSignal().load('as_table', true, 'subject', 1);
            testCase.verifyTrue(iscell(tbl.RawSignal), ...
                'Non-scalar array data should remain as cell column');
        end

        function test_for_each_as_table_flatten_column_types(testCase)
            %% as_table=true with table data (flatten mode) should also
            %  return proper typed columns for metadata.
            tbl1 = table([10; 20], 'VariableNames', {'value'});
            tbl2 = table([30; 40], 'VariableNames', {'value'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 1, 'session', 'B');

            received = [];

            function result = capture_flat(values)
                received = values;
                result = 0;
            end

            scidb.for_each(@capture_flat, ...
                struct('values', RawSignal()), ...
                {ScalarVar()}, ...
                'as_table', true, ...
                'subject', 1);

            testCase.verifyTrue(istable(received));
            % Flatten mode: table data gets merged with metadata columns
            testCase.verifyTrue(isnumeric(received.subject), ...
                'Flatten mode: numeric metadata should be numeric, not cell');
            testCase.verifyTrue(isstring(received.session), ...
                'Flatten mode: string metadata should be string, not cell');
            testCase.verifyTrue(isnumeric(received.value), ...
                'Flatten mode: numeric data column should be numeric');
        end

        function test_load_as_table_no_metadata_filters(testCase)
            %% load(as_table=true) with no metadata filters should return
            %  all records as a table — as_table must not leak into the
            %  metadata query.
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 2, 'session', 'A');
            ScalarVar().save(30, 'subject', 1, 'session', 'B');

            tbl = ScalarVar().load('as_table', true);
            testCase.verifyTrue(istable(tbl), ...
                'load(as_table=true) with no metadata should return a table');
            testCase.verifyEqual(height(tbl), 3, ...
                'Should return all 3 records when no metadata filter is given');
        end

        function test_as_table_false_with_column_selection_returns_vector(testCase)
            %% as_table=false + single column selection should return a
            %  plain vector (no metadata columns) — existing behavior.
            input_tbl = table([7.0; 8.0; 9.0], [0.1; 0.2; 0.3], ...
                'VariableNames', {'signal', 'noise'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal("signal")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyFalse(istable(result), ...
                'as_table=false + column selection should return a vector, not a table');
            testCase.verifyEqual(result, [7.0; 8.0; 9.0], 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % categorical parameter
        % -----------------------------------------------------------------

        function test_load_categorical_returns_table(testCase)
            %% load(categorical=true) should return a table (not error)
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');
            ScalarVar().save(30, 'subject', 2, 'session', 'A');

            tbl = ScalarVar().load('categorical', true);
            testCase.verifyTrue(istable(tbl));
            testCase.verifyEqual(height(tbl), 3);
        end

        function test_load_categorical_metadata_columns_are_categorical(testCase)
            %% categorical=true should convert metadata columns to categorical type
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');
            ScalarVar().save(30, 'subject', 2, 'session', 'A');

            tbl = ScalarVar().load('categorical', true);
            testCase.verifyTrue(iscategorical(tbl.subject), ...
                'subject column should be categorical');
            testCase.verifyTrue(iscategorical(tbl.session), ...
                'session column should be categorical');
        end

        function test_load_categorical_data_column_not_categorical(testCase)
            %% categorical=true should NOT convert the data column
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');

            tbl = ScalarVar().load('categorical', true, 'subject', 1);
            testCase.verifyTrue(isnumeric(tbl.ScalarVar), ...
                'Data column should remain numeric, not categorical');
        end

        function test_load_categorical_false_not_categorical(testCase)
            %% categorical=false (default) should NOT convert metadata columns
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');

            tbl = ScalarVar().load('subject', 1);
            testCase.verifyFalse(iscategorical(tbl.session), ...
                'Default load should not produce categorical columns');
        end

        function test_load_categorical_with_metadata_filter(testCase)
            %% categorical=true should work alongside metadata filters
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');
            ScalarVar().save(30, 'subject', 2, 'session', 'A');

            tbl = ScalarVar().load('categorical', true, 'subject', 1);
            testCase.verifyTrue(istable(tbl));
            testCase.verifyEqual(height(tbl), 2);
            testCase.verifyTrue(iscategorical(tbl.session));
        end

        function test_load_categorical_preserves_unique_values(testCase)
            %% categorical columns should have correct category values
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');
            ScalarVar().save(30, 'subject', 1, 'session', 'C');

            tbl = ScalarVar().load('categorical', true, 'subject', 1);
            cats = categories(tbl.session);
            testCase.verifyEqual(numel(cats), 3);
        end

    end
end
