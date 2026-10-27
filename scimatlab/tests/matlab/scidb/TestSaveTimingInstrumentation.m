classdef TestSaveTimingInstrumentation < matlab.unittest.TestCase
%TESTSAVETIMINGINSTRUMENTATION  Exercises bulk-save strategies and timing
%   logs added to save_from_table.
%
%   Three bulk-save strategies are exercised in save_from_table:
%     Strategy A (vertcat): data_col is a cell of 1-row MATLAB tables.
%       MATLAB vertcats all records into one N-row table, crosses the bridge
%       once, and Python splits back into N 1-row DataFrames.
%     Strategy B (flatten): data_col is a cell of variable-length numeric
%       vectors.  try_flatten_cell_column produces a flat array + lengths
%       vector (2 bridge crossings) and Python uses split_flat_to_lists.
%     Strategy C (per-row): data that fails both vertcat and flatten.
%       Falls back to N individual to_python calls; identical to the old
%       behavior.  This path is also covered by existing TestSaveLoad tests.
%
%   After a run, grep the scidb log for `[timing]` lines:
%
%       [timing] save_from_table(DummyMixed): data_convert=...s, mode=vertcat, n=700
%       [timing] save_batch_bridge(DummyMixed): n=700, mode=vertcat_df, ...
%       [timing] save_batch(DummyMixed): 700 items ...

    properties
        test_dir
        log_archive_dir
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
                ["subject", "session", "speed", "trial"]);

            % The file sink defaults to INFO; raise it so the archived log
            % keeps the DEBUG per-phase timing tables this test documents.
            scidb.Log.set_level('DEBUG', 'file');

            this_dir = fileparts(mfilename('fullpath'));
            testCase.log_archive_dir = fullfile(this_dir, '..', '..', '..', '..', 'timing-logs');
            if ~isfolder(testCase.log_archive_dir)
                mkdir(testCase.log_archive_dir);
            end
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            try
                live_log = fullfile(testCase.test_dir, 'scistack.log');
                if isfile(live_log) && ~isempty(testCase.log_archive_dir)
                    ts = char(datetime('now', 'Format', 'yyyyMMdd-HHmmss'));
                    archived = fullfile(testCase.log_archive_dir, ...
                        sprintf('scidb-save-%s.log', ts));
                    copyfile(live_log, archived);
                    fprintf('\n[timing-test] scistack.log archived to: %s\n', archived);
                end
            catch err
                fprintf('\n[timing-test] WARNING: could not archive scistack.log: %s\n', err.message);
            end

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

        function test_strategy_a_table_data(testCase)
            % Strategy A: same 7,000-record schema as TestForEachTimingInstrumentation.
            %   Schema: [subject, session, speed, trial] = 20x5x10x7 combos
            %   Data: 1-row MATLAB table per record, 54 columns
            %     27 scalar double (DOUBLE), 25 cell-of-vector double (DOUBLE[]),
            %     2 struct (JSON) — forces dataframe-mode storage.
            % Expected: mode=vertcat in timing log; save time well under 40s.
            subjects = 1:20;
            sessions = 1:5;
            speeds   = 1:10;
            trials   = 1:7;

            [S1, S2, S3, S4] = ndgrid(subjects, sessions, speeds, trials);
            subject_col = S1(:);
            session_col = S2(:);
            speed_col   = S3(:);
            trial_col   = S4(:);
            n_records   = numel(subject_col);
            testCase.assertEqual(n_records, 7000);

            n_dbl  = 27;
            n_arr  = 25;
            n_json = 2;
            dbl_names  = arrayfun(@(i) sprintf('dbl_%02d', i),  1:n_dbl,  'UniformOutput', false);
            arr_names  = arrayfun(@(i) sprintf('arr_%02d', i),  1:n_arr,  'UniformOutput', false);
            json_names = arrayfun(@(i) sprintf('json_%02d', i), 1:n_json, 'UniformOutput', false);

            data_col = cell(n_records, 1);
            rng(0);
            arr_len = 10;
            for i = 1:n_records
                t = table();
                for j = 1:n_dbl
                    t.(dbl_names{j}) = randn();
                end
                for j = 1:n_arr
                    t.(arr_names{j}) = {randn(arr_len, 1)};
                end
                for j = 1:n_json
                    t.(json_names{j}) = {struct('mean', randn(), 'idx', i)};
                end
                data_col{i} = t;
            end

            tbl = table( ...
                subject_col, session_col, speed_col, trial_col, data_col, ...
                'VariableNames', {'subject','session','speed','trial','data'});

            fprintf('\n[timing-test] Strategy A: saving %d table-mode records...\n', n_records);
            save_t0 = tic;
            DummyMixed().save_from_table( ...
                tbl, "data", ["subject","session","speed","trial"]);
            elapsed = toc(save_t0);
            fprintf('[timing-test] Strategy A done in %.2fs\n', elapsed);

            % Round-trip: shape check
            sample = DummyMixed().load( ...
                'subject', 1, 'session', 1, 'speed', 1, 'trial', 1);
            testCase.assertTrue(istable(sample.data), ...
                'expected dataframe-mode storage (data should be a MATLAB table)');
            testCase.verifyEqual(height(sample.data), 1);
            testCase.verifyEqual(width(sample.data), n_dbl + n_arr + n_json);

            % Round-trip: scalar double column value
            original = data_col{1};
            testCase.verifyEqual(sample.data.(dbl_names{1}), original.(dbl_names{1}), ...
                'AbsTol', 1e-12);

            % Round-trip: variable-length array column value
            testCase.verifyEqual(sample.data.(arr_names{1}){1}, original.(arr_names{1}){1}, ...
                'AbsTol', 1e-12);
        end


        function test_strategy_b_variable_length_arrays(testCase)
            % Strategy B: each record is a variable-length double vector.
            % save_from_table should use try_flatten_cell_column (flatten mode).
            % Expected: mode=flatten in timing log.
            subjects = 1:20;
            sessions = 1:5;
            speeds   = 1:10;
            trials   = 1:7;

            [S1, S2, S3, S4] = ndgrid(subjects, sessions, speeds, trials);
            subject_col = S1(:);
            session_col = S2(:);
            speed_col   = S3(:);
            trial_col   = S4(:);
            n_records   = numel(subject_col);
            testCase.assertEqual(n_records, 7000);

            % Each record: a variable-length row vector (lengths cycle 1..10).
            rng(2);
            data_col = cell(n_records, 1);
            expected_lengths = zeros(n_records, 1);
            for i = 1:n_records
                len = mod(i, 10) + 1;
                data_col{i} = randn(1, len);
                expected_lengths(i) = len;
            end

            tbl = table(subject_col, session_col, speed_col, trial_col, data_col, ...
                'VariableNames', {'subject','session','speed','trial','data'});

            fprintf('\n[timing-test] Strategy B: saving %d variable-length-array records...\n', n_records);
            save_t0 = tic;
            ScalarVar().save_from_table( ...
                tbl, "data", ["subject","session","speed","trial"]);
            elapsed = toc(save_t0);
            fprintf('[timing-test] Strategy B done in %.2fs\n', elapsed);

            % Round-trip: check a few records by length and values.
            for check_idx = [1, 1000, 7000]
                s  = subject_col(check_idx);
                se = session_col(check_idx);
                sp = speed_col(check_idx);
                tr = trial_col(check_idx);
                result = ScalarVar().load( ...
                    'subject', s, 'session', se, 'speed', sp, 'trial', tr);
                testCase.verifyEqual(numel(result.data), expected_lengths(check_idx), ...
                    sprintf('record %d: wrong length', check_idx));
                testCase.verifyEqual(result.data(:)', data_col{check_idx}(:)', ...
                    'AbsTol', 1e-12, sprintf('record %d: data mismatch', check_idx));
            end
        end


        function test_strategy_a_multirow_table_records(testCase)
            % Verify that row_heights > 1 works: each record is a 3-row table.
            % Python must split the bulk DataFrame using row_heights = [3,3,...].
            n_records = 20;
            subject_col = (1:n_records)';
            session_col = ones(n_records, 1);
            speed_col   = ones(n_records, 1);
            trial_col   = ones(n_records, 1);

            rng(3);
            data_col = cell(n_records, 1);
            for i = 1:n_records
                data_col{i} = table(randn(3,1), randn(3,1), ...
                    'VariableNames', {'x','y'});
            end

            tbl = table(subject_col, session_col, speed_col, trial_col, data_col, ...
                'VariableNames', {'subject','session','speed','trial','data'});

            TableVar().save_from_table( ...
                tbl, "data", ["subject","session","speed","trial"]);

            % Each loaded record should be a 3-row table.
            sample = TableVar().load( ...
                'subject', 1, 'session', 1, 'speed', 1, 'trial', 1);
            testCase.assertTrue(istable(sample.data));
            testCase.verifyEqual(height(sample.data), 3);
            testCase.verifyEqual(width(sample.data),  2);

            % Verify values match original.
            testCase.verifyEqual(sample.data.x, data_col{1}.x, 'AbsTol', 1e-12);
            testCase.verifyEqual(sample.data.y, data_col{1}.y, 'AbsTol', 1e-12);
        end

    end
end
