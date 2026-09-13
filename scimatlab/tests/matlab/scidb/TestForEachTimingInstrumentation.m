classdef TestForEachTimingInstrumentation < matlab.unittest.TestCase
%TESTFOREACHTIMINGINSTRUMENTATION  Exercises [timing] log lines in
%   scidb/_find_record, db.load_all, and scidb/foreach._load_var_type_all.
%
%   Reproduces the 7,000-record DummyMixed dataset that prompted the
%   instrumentation work:
%     * Schema: [subject, session, speed, trial]
%     * 20 x 5 x 10 x 7 = 7,000 unique combos (one record per combo)
%     * Each record's data: 1-row MATLAB table, 54 columns
%         - 27 DOUBLE       (scalar double cells)
%         - 25 DOUBLE[]     (cell-of-vector double cells, length 10)
%         -  2 JSON         (struct cells)
%       This shape forces dataframe-mode storage (each DataFrame cell
%       becomes one DuckDB cell with its own column type).
%     * Calls scidb.for_each(subject=[], session=[], speed=[], trial=[])
%       with a no-op fn so all 7,000 combos drive a single bulk load_all
%       call and exercise the timing path end-to-end.
%
%   This is a long-running test (setup alone saves 7,000 records).  Run
%   it directly when you want to validate the [timing] log output, e.g.:
%
%       runtests('TestForEachTimingInstrumentation')
%
%   After the run, grep the scidb log for `[timing]` lines.  The expected
%   per-call breakdown looks like:
%
%       [timing] _find_record(DummyMixed, version_id='latest'): total=...
%       [timing] load_all(DummyMixed): pre-yield setup: find=..., chunks_total=...
%       [timing] load_all(DummyMixed): TOTAL=...
%       [timing] _load_var_type_all(DummyMixed): load_all+materialize = ...
%       [timing] _load_var_type_all(DummyMixed): assembly=..., branch=dataframe

    properties
        test_dir
        log_archive_dir  % stable location where scidb.log is copied before teardown
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

            % Stable archive dir for the scidb.log copy.  Located at the
            % workspace root so multiple runs accumulate side-by-side and
            % survive the test_dir teardown that wipes the temp folder.
            this_dir = fileparts(mfilename('fullpath'));
            testCase.log_archive_dir = fullfile(this_dir, '..', '..', '..', '..', 'timing-logs');
            if ~isfolder(testCase.log_archive_dir)
                mkdir(testCase.log_archive_dir);
            end
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            % Copy scidb.log out of the doomed temp dir before teardown
            % nukes it.  Uses a timestamp + test method name so successive
            % runs don't clobber.
            try
                live_log = fullfile(testCase.test_dir, 'scidb.log');
                if isfile(live_log) && ~isempty(testCase.log_archive_dir)
                    ts = char(datetime('now', 'Format', 'yyyyMMdd-HHmmss'));
                    archived = fullfile(testCase.log_archive_dir, ...
                        sprintf('scidb-%s.log', ts));
                    copyfile(live_log, archived);
                    fprintf('\n[timing-test] scidb.log archived to: %s\n', archived);
                else
                    fprintf('\n[timing-test] no scidb.log found at %s\n', live_log);
                end
            catch err
                fprintf('\n[timing-test] WARNING: could not archive scidb.log: %s\n', err.message);
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
        function test_dataframe_mode_7k_records(testCase)
            % ---- Phase 1: build the dummy data ------------------------
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

            % 27 DOUBLE + 25 DOUBLE[] + 2 JSON = 54 data columns.
            n_dbl  = 27;
            n_arr  = 25;
            n_json = 2;

            dbl_names = arrayfun(@(i) sprintf('dbl_%02d', i),  1:n_dbl,  'UniformOutput', false);
            arr_names = arrayfun(@(i) sprintf('arr_%02d', i),  1:n_arr,  'UniformOutput', false);
            json_names = arrayfun(@(i) sprintf('json_%02d', i), 1:n_json, 'UniformOutput', false);

            % Pre-allocate the per-record cell column.  Each cell holds a
            % 1-row MATLAB table that becomes one record's data.  scidb's
            % dataframe-mode storage stamps a column type per DataFrame
            % column on the first save: scalar double -> DOUBLE,
            % cell-of-vector -> DOUBLE[], struct -> JSON.
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

            % ---- Phase 2: save to the temporary database --------------
            fprintf('\n[timing-test] saving %d records via save_from_table...\n', n_records);
            save_t0 = tic;
            DummyMixed().save_from_table( ...
                tbl, "data", ["subject","session","speed","trial"]);
            fprintf('[timing-test] save complete in %.2fs\n', toc(save_t0));

            % Sanity check: a single .load() should return a 1-row 54-col table.
            sample = DummyMixed().load( ...
                'subject', 1, 'session', 1, 'speed', 1, 'trial', 1);
            testCase.assertTrue(istable(sample.data), ...
                'expected dataframe-mode storage (data should be a MATLAB table)');
            testCase.verifyEqual(height(sample.data), 1);
            testCase.verifyEqual(width(sample.data), n_dbl + n_arr + n_json);

            % ---- Phase 3: run for_each to exercise the timing path ----
            % Empty [] for every schema key forces DB resolution in
            % _for_each_prepare Step 2, then the bulk load_all path on
            % DummyMixed.  Watch the scidb log for [timing] lines.
            fprintf('\n[timing-test] running scidb.for_each (watch the log for [timing] lines)...\n');
            run_t0 = tic;
            scidb.for_each(@dummy_return_one, ...
                struct('x', DummyMixed()), ...
                {DummyOut()}, ...
                'as_table', true, ...
                'subject', [], 'session', [], 'speed', [], 'trial', []);
            fprintf('[timing-test] scidb.for_each complete in %.2fs\n', toc(run_t0));

            % Verify outputs landed (one DummyOut per combo).
            results = DummyOut().load('version', 'all');
            testCase.verifyEqual(numel(results), n_records, ...
                sprintf('expected %d DummyOut records, got %d', ...
                        n_records, numel(results)));
        end

        function test_postsave_timing_line_is_emitted(testCase)
            %TEST_POSTSAVE_TIMING_LINE_IS_EMITTED
            %   The window between 'for_each_save returned' and 'for_each done'
            %   used to emit nothing at all. On 2026-09-13 it was ~385s of a
            %   641s run -- 60% of the runtime, invisible. This pins the named
            %   breakdown that closed that gap.
            DummyMixed().save(table(1, 'VariableNames', {'v'}), ...
                'subject', 1, 'session', 1, 'speed', 1, 'trial', 1);

            % Assigned call so the post-save conversion actually runs.
            result = scidb.for_each(@dummy_return_one, ...
                struct('x', DummyMixed()), ...
                {DummyOut()}, ...
                'as_table', true, ...
                'subject', 1, 'session', 1, 'speed', 1, 'trial', 1);
            testCase.verifyEqual(height(result), 1);

            lines = strsplit(fileread(fullfile(testCase.test_dir, 'scidb.log')), newline);
            hits = lines(contains(lines, '[timing] for_each_postsave:'));
            testCase.verifyNotEmpty(hits, ...
                'no [timing] for_each_postsave line found in scidb.log');

            summary = hits{end};
            for label = {'from_python', 'flatten', 'type_restore', ...
                         'introspect_parse', 'bytes'}
                testCase.verifyTrue(contains(summary, [label{1} '=']), ...
                    sprintf('postsave summary is missing %s=', label{1}));
            end

            % TOTAL must cover the named parts -- a part timed outside the
            % TOTAL window would make the breakdown lie about where time went.
            total = str2double(regexp(summary, '(?<=TOTAL=)[\d.]+', 'match', 'once'));
            parts = str2double(regexp(summary, ...
                '(?<==)[\d.]+(?=s[,)])', 'match'));
            testCase.verifyGreaterThanOrEqual(total, sum(parts) - 1e-6, ...
                'postsave TOTAL is less than the sum of its named phases');
        end

        function test_postsave_conversion_skipped_when_result_discarded(testCase)
            %TEST_POSTSAVE_CONVERSION_SKIPPED_WHEN_RESULT_DISCARDED
            %   A statement call must report from_python=0 -- that zero IS the
            %   optimization. Records still save (covered in
            %   TestForEachReturnValue); this pins the instrumentation proving
            %   the conversion was skipped rather than merely fast.
            DummyMixed().save(table(1, 'VariableNames', {'v'}), ...
                'subject', 1, 'session', 1, 'speed', 1, 'trial', 1);

            scidb.for_each(@dummy_return_one, ...
                struct('x', DummyMixed()), ...
                {DummyOut()}, ...
                'as_table', true, ...
                'subject', 1, 'session', 1, 'speed', 1, 'trial', 1);

            lines = strsplit(fileread(fullfile(testCase.test_dir, 'scidb.log')), newline);
            hits = lines(contains(lines, '[timing] for_each_postsave:'));
            testCase.verifyNotEmpty(hits, ...
                'no [timing] for_each_postsave line found in scidb.log');
            testCase.verifyTrue(contains(hits{end}, 'from_python=0.000s'), ...
                'statement call still spent time in from_python — gate not firing');

            skipped = lines(contains(lines, 'caller requested no output (nargout=0)'));
            testCase.verifyNotEmpty(skipped, ...
                'expected the nargout=0 skip to be logged');
        end
    end
end
