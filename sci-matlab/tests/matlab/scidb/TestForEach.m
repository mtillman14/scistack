classdef TestForEach < matlab.unittest.TestCase
%TESTFOREACH  Integration tests for scidb.for_each batch processing.

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
        % --- Basic iteration ---

        function test_single_key_iteration(testCase)
            % Save input data for 3 subjects
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            RawSignal().save([7 8 9], 'subject', 3, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A");

            % Verify all 3 outputs were saved
            for s = [1 2 3]
                result = ProcessedSignal().load('subject', s, 'session', 'A');
                raw = RawSignal().load('subject', s, 'session', 'A');
                testCase.verifyEqual(result, raw * 2, 'AbsTol', 1e-10);
            end
        end

        function test_cartesian_product(testCase)
            % Save input data for all combinations
            for s = [1 2]
                for sess = ["A", "B"]
                    RawSignal().save(s * [1 2 3], ...
                        'subject', s, 'session', sess);
                end
            end

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A", "B"]);

            % Should produce 2 * 2 = 4 outputs
            all_results = ProcessedSignal().load_all();
            testCase.verifyEqual(numel(all_results), 4);
        end

        function test_output_data_correct(testCase)
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, [20 40 60]', 'AbsTol', 1e-10);
        end

        % --- Constants ---

        function test_constant_input_passed_to_function(testCase)
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 5), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, [15 25 35]', 'AbsTol', 1e-10);
        end

        function test_constant_included_in_save_metadata(testCase)
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 5), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            % The constant 'offset' should appear in saved metadata
            versions = ProcessedSignal().list_versions( ...
                'subject', 1, 'session', 'A');
            testCase.verifyGreaterThanOrEqual(numel(versions), 1);
        end

        % --- Two loadable inputs ---

        function test_two_variable_inputs(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            ProcessedSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            scidb.for_each(@sum_inputs, ...
                struct('a', RawSignal(), 'b', ProcessedSignal()), ...
                {FilteredSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = FilteredSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, [11 22 33]', 'AbsTol', 1e-10);
        end

        % --- Fixed inputs ---

        function test_fixed_input_overrides_metadata(testCase)
            % Save baseline data with session="BL"
            BaselineSignal().save([100 200 300], ...
                'subject', 1, 'session', 'BL');
            BaselineSignal().save([100 200 300], ...
                'subject', 2, 'session', 'BL');

            % Save current data with session="A" and "B"
            RawSignal().save([110 210 310], 'subject', 1, 'session', 'A');
            RawSignal().save([120 220 320], 'subject', 2, 'session', 'A');

            % Use Fixed to always load baseline from session="BL"
            scidb.for_each(@subtract_baseline, ...
                struct('current', RawSignal(), ...
                       'baseline', scidb.Fixed(BaselineSignal(), ...
                           'session', 'BL')), ...
                {DeltaSignal()}, ...
                'subject', [1 2], ...
                'session', "A");

            % Subject 1: [110-100, 210-200, 310-300] = [10, 10, 10]
            d1 = DeltaSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(d1, [10 10 10]', 'AbsTol', 1e-10);

            % Subject 2: [120-100, 220-200, 320-300] = [20, 20, 20]
            d2 = DeltaSignal().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(d2, [20 20 20]', 'AbsTol', 1e-10);
        end

        % --- dry_run ---

        function test_dry_run_does_not_save(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'dry_run', true, ...
                'subject', 1, ...
                'session', "A");

            % Nothing should be saved
            results = ProcessedSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEmpty(results);
        end

        % --- save=false ---

        function test_save_false_executes_but_does_not_save(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'save', false, ...
                'subject', 1, ...
                'session', "A");

            results = ProcessedSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEmpty(results);
        end

        % --- Multiple outputs ---

        function test_multiple_outputs_with_plain_function(testCase)
            RawSignal().save([1 2 3 4], 'subject', 1, 'session', 'A');

            scidb.for_each(@split_data, ...
                struct('x', RawSignal()), ...
                {SplitFirst(), SplitSecond()}, ...
                'subject', 1, ...
                'session', "A");

            r1 = SplitFirst().load('subject', 1, 'session', 'A');
            r2 = SplitSecond().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1, [1 2]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2, [3 4]', 'AbsTol', 1e-10);
        end

        % --- PathInput ---

        function test_path_input_resolves_template(testCase)
            % PathInput resolves template to file path; function receives path string
            scidb.for_each(@path_length, ...
                struct('filepath', scifor.PathInput("{subject}/data.mat", ...
                    'root_folder', '/data')), ...
                {ScalarVar()}, ...
                'subject', 1, ...
                'session', "A");

            % The function should have received a resolved path
            result = ScalarVar().load('subject', 1, 'session', 'A');
            expected_path = fullfile('/data', '1', 'data.mat');
            testCase.verifyEqual(result, double(strlength(expected_path)), ...
                'AbsTol', 1e-10);
        end

        % --- Skipped iterations ---

        function test_missing_input_skips_iteration(testCase)
            % Only save data for subject 1, not subject 2
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A");

            % Subject 1 should be saved
            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1, [2 4 6]', 'AbsTol', 1e-10);

            % Subject 2 should be skipped (no input data)
            results = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results);
        end

        % --- Parallel mode ---

        function test_parallel_basic(testCase)
            % Same as test_single_key_iteration but with parallel=true
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            RawSignal().save([7 8 9], 'subject', 3, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'parallel', true, ...
                'subject', [1 2 3], ...
                'session', "A");

            for s = [1 2 3]
                result = ProcessedSignal().load('subject', s, 'session', 'A');
                raw = RawSignal().load('subject', s, 'session', 'A');
                testCase.verifyEqual(result, raw * 2, 'AbsTol', 1e-10);
            end
        end

        function test_parallel_cartesian(testCase)
            % Same as test_cartesian_product but with parallel=true
            for s = [1 2]
                for sess = ["A", "B"]
                    RawSignal().save(s * [1 2 3], ...
                        'subject', s, 'session', sess);
                end
            end

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'parallel', true, ...
                'subject', [1 2], ...
                'session', ["A", "B"]);

            all_results = ProcessedSignal().load_all();
            testCase.verifyEqual(numel(all_results), 4);
        end

        function test_parallel_with_constant(testCase)
            % Same as test_constant_input_passed_to_function with parallel=true
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 5), ...
                {ProcessedSignal()}, ...
                'parallel', true, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, [15 25 35]', 'AbsTol', 1e-10);
        end

        function test_parallel_skips_missing(testCase)
            % Same as test_missing_input_skips_iteration with parallel=true
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'parallel', true, ...
                'subject', [1 2], ...
                'session', "A");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1, [2 4 6]', 'AbsTol', 1e-10);

            results = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results);
        end

        function test_parallel_false_is_default(testCase)
            % parallel=false should behave identically to the default
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'parallel', false, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, [20 40 60]', 'AbsTol', 1e-10);
        end

        function test_parallel_multiple_outputs(testCase)
            % Parallel mode with multiple outputs
            RawSignal().save([1 2 3 4], 'subject', 1, 'session', 'A');

            scidb.for_each(@split_data, ...
                struct('x', RawSignal()), ...
                {SplitFirst(), SplitSecond()}, ...
                'parallel', true, ...
                'subject', 1, ...
                'session', "A");

            r1 = SplitFirst().load('subject', 1, 'session', 'A');
            r2 = SplitSecond().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1, [1 2]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2, [3 4]', 'AbsTol', 1e-10);
        end

        % --- Column selection ---

        function test_column_selection_single_column(testCase)
            % Single column selection should pass the column's values as an array.
            % Verified indirectly: sum([1;2;3]) = 6, sum([10;20;30]) = 60.
            % If col_a is selected, result = 6; if full table passed, error.
            input_tbl = table([1.0; 2.0; 3.0], [10.0; 20.0; 30.0], ...
                'VariableNames', {'col_a', 'col_b'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@sum_array, ...
                struct('x', RawSignal("col_a")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            % sum([1;2;3]) = 6
            testCase.verifyEqual(result, 6.0, 'AbsTol', 1e-10);
        end

        function test_column_selection_multiple_columns(testCase)
            % Multiple column selection should pass a subtable (not the full table).
            % noop_func returns its input unchanged; we check the saved data is a
            % 2-column subtable.
            input_tbl = table([1.0; 2.0], [10.0; 20.0], [100.0; 200.0], ...
                'VariableNames', {'col_a', 'col_b', 'col_c'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["col_a", "col_b"])), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(result));
            testCase.verifyEqual(result.Properties.VariableNames, {'col_a', 'col_b'});
            testCase.verifyEqual(height(result), 2);
        end

        function test_column_selection_multiple_iterations(testCase)
            % Column selection should work correctly across multiple iterations.
            tbl1 = table([1.0; 2.0], [10.0; 20.0], 'VariableNames', {'col_a', 'col_b'});
            tbl2 = table([3.0; 4.0], [30.0; 40.0], 'VariableNames', {'col_a', 'col_b'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 2, 'session', 'A');

            scidb.for_each(@sum_array, ...
                struct('x', RawSignal("col_a")), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            r2 = ProcessedSignal().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(r1, 3.0, 'AbsTol', 1e-10);  % sum([1;2])
            testCase.verifyEqual(r2, 7.0, 'AbsTol', 1e-10);  % sum([3;4])
        end

        function test_column_selection_invalid_column_skips_iteration(testCase)
            % Invalid column name should cause a skip (no output saved).
            input_tbl = table([1.0; 2.0], 'VariableNames', {'col_a'});
            RawSignal().save(input_tbl, 'subject', 1, 'session', 'A');

            % Should complete without crashing; iteration is skipped
            scidb.for_each(@noop_func, ...
                struct('x', RawSignal("nonexistent")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            results = ProcessedSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEmpty(results);
        end

        function test_column_selection_non_table_skips_iteration(testCase)
            % Column selection on non-table data should cause a skip.
            RawSignal().save([1.0 2.0 3.0], 'subject', 1, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal("col_a")), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A");

            results = ProcessedSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEmpty(results);
        end

        % --- Multiple subjects and sessions ---

        function test_full_pipeline(testCase)
            % Populate raw data for a 2x2 grid
            subjects = [1 2];
            sessions = ["A", "B"];
            for s = subjects
                for sess = sessions
                    RawSignal().save(s * [1 2 3], ...
                        'subject', s, 'session', sess);
                end
            end

            % Process all combinations
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', subjects, ...
                'session', sessions);

            % Verify each output
            for s = subjects
                for sess = sessions
                    result = ProcessedSignal().load('subject', s, 'session', sess);
                    testCase.verifyEqual(result, s * [2 4 6]', 'AbsTol', 1e-10);
                end
            end
        end

        % --- Distribute a table to multiple schema id's ---

        function test_distribute_true_with_lowest_key_throws_error(testCase)
            tbl = table;
            tbl.A = [1, 2, 3];
            tbl.B = [4, 5, 6];
            tbl.subject = ["1", "1", "1"];
            tbl.session = [1, 2, 3];

            subjects = 1;
            session=[1, 2, 3];
    
            testCase.verifyError( ...
                @() scidb.for_each( ...
                    @double_values, ...
                    struct('x', RawSignal()), ...
                    {ProcessedSignal()}, ...
                    'subject', subjects, ...
                    'session', session, ...
                    distribute=true ...
                ), ...
                "scidb:for_each" ...
            );
        end

        function test_distribute_true_saves_to_multiple_rows(testCase)
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];
            tbl.subject = ["1"; "1"; "1"];

            subjects = "1";
            sessions = [1, 2, 3];

            scidb.for_each(@double_table_values, ...
                struct('x', tbl), ...
                {ProcessedSignal()}, ...
                'subject', subjects, ...
                distribute=true ...
            );

            % Verify each output: distribute saves one row per session,
            % so session=k should hold row k of the processed table.
            for s = subjects
                for sess = sessions
                    result = ProcessedSignal().load('subject', s, 'session', sess);
                    testCase.verifyTrue(istable(result));
                    testCase.verifyEqual(result.A, tbl.A(sess) * 2, 'AbsTol', 1e-10);
                    testCase.verifyEqual(result.B, tbl.B(sess) * 2, 'AbsTol', 1e-10);
                    testCase.verifyEqual(result.subject, tbl.subject(sess));
                    testCase.verifyTrue(all(ismember({'A', 'B', 'subject'}, result.Properties.VariableNames)));
                end
            end
        end

        function test_distribute_multiple_subjects(testCase)
            % Two subjects, same constant table input. Each subject's output
            % table is independently distributed to sessions 1-3.
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];

            subjects = ["1", "2"];
            sessions = [1, 2, 3];

            scidb.for_each(@double_table_values, ...
                struct('x', tbl), ...
                {ProcessedSignal()}, ...
                'subject', subjects, ...
                distribute=true ...
            );

            % Both subjects should have identical doubled values at each session
            for s = subjects
                for sess = sessions
                    result = ProcessedSignal().load('subject', s, 'session', sess);
                    testCase.verifyTrue(istable(result));
                    testCase.verifyEqual(result.A, tbl.A(sess) * 2, 'AbsTol', 1e-10);
                    testCase.verifyEqual(result.B, tbl.B(sess) * 2, 'AbsTol', 1e-10);
                end
            end
        end

        function test_distribute_from_loaded_variable(testCase)
            % Input is a BaseVariable loaded from the database (RawSignal saved
            % at the subject level with no session). for_each loads it, the
            % function returns a numeric vector, and distribute= splits the
            % vector into individual per-session scalar records.
            RawSignal().save([1; 2; 3], 'subject', 1);

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                distribute=true ...
            );

            % Row k of [2; 4; 6] goes to session k
            expected = [2; 4; 6];
            for sess = 1:3
                result = ProcessedSignal().load('subject', 1, 'session', sess);
                testCase.verifyEqual(result, expected(sess), 'AbsTol', 1e-10);
            end
        end

        function test_distribute_session_column_in_output(testCase)
            % When the output table contains a column named after the
            % distribute_key ("session"), distribute= uses those column values
            % to determine which session each row is stored at (instead of
            % 1-based row indices). The session column must NOT appear in the
            % saved data.
            %
            % table_with_session_col doubles A/B and adds session=[3;1;2],
            % so: row 1 (A=2) → session=3, row 2 (A=4) → session=1,
            %     row 3 (A=6) → session=2.
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];

            sessions = [1, 2, 3];
            % for sess = sessions
            %     ProcessedSignal().save(tbl(sess,:), 'subject', '1', 'session', sess);
            % end

            scidb.for_each(@table_with_session_col, ...
                struct('x', tbl), ...
                {ProcessedSignal()}, ...
                'subject', "1", ...
                distribute=true ...
            );

            % session=1 ← row 2 of output (A=4), session=2 ← row 3 (A=6),
            % session=3 ← row 1 (A=2)
            expected_A = containers.Map({1, 2, 3}, {4.0, 6.0, 2.0});
            expected_B = containers.Map({1, 2, 3}, {10.0, 12.0, 8.0});
            for sess = sessions
                result = ProcessedSignal().load('subject', '1', 'session', sess);
                testCase.verifyTrue(istable(result));
                testCase.verifyEqual(result.A, expected_A(sess), 'AbsTol', 1e-10);
                testCase.verifyEqual(result.B, expected_B(sess), 'AbsTol', 1e-10);
                % The session column must have been stripped from the saved data
                testCase.verifyFalse(ismember('session', result.Properties.VariableNames));
            end
        end

        function test_distribute_idempotent(testCase)
            % Running for_each with distribute=true twice with the same inputs
            % must not create duplicate records. The second pass should be a
            % no-op (same content hash → same record_id → idempotency check).
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];

            for run = 1:2
                scidb.for_each(@double_table_values, ...
                    struct('x', tbl), ...
                    {ProcessedSignal()}, ...
                    'subject', "1", ...
                    distribute=true ...
                );
            end

            % Exactly 3 session records — not 6 — should exist
            all_results = ProcessedSignal().load_all();
            testCase.verifyEqual(numel(all_results), 3);

            % Data must be correct (second run didn't corrupt anything)
            for sess = 1:3
                result = ProcessedSignal().load('subject', '1', 'session', sess);
                testCase.verifyEqual(result.A, tbl.A(sess) * 2, 'AbsTol', 1e-10);
                testCase.verifyEqual(result.B, tbl.B(sess) * 2, 'AbsTol', 1e-10);
            end
        end

        function test_distribute_dry_run_no_saves(testCase)
            % dry_run=true must suppress all saves even in distribute mode.
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];

            scidb.for_each(@double_table_values, ...
                struct('x', tbl), ...
                {ProcessedSignal()}, ...
                'subject', "1", ...
                distribute=true, ...
                dry_run=true ...
            );

            results = ProcessedSignal().load_all();
            testCase.verifyEmpty(results);
        end
    end
end