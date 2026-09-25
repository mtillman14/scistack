classdef TestForEachSchemaFiltering < matlab.unittest.TestCase
%TESTFOREACHSCHEMAFILTERING  Tests for filtering cartesian product to
%   existing schema combinations when [] is used in for_each.

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

        function test_filtering_removes_nonexistent_combos(testCase)
            % Save data only for (1,A) and (2,B) — not (1,B) or (2,A)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'B');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [], ...
                'session', []);

            % Only 2 of 4 possible combos should have been processed
            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 2);
        end

        function test_no_filtering_when_all_explicit(testCase)
            % Save data only for (1,A) — but provide explicit lists
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A", "B"]);

            % Explicit values => no filtering; 3 of 4 combos are skipped
            % (only (1,A) has data), but all 4 are attempted
            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 1);
        end

        function test_no_filtering_with_pathinput(testCase)
            % With PathInput, filtering should be bypassed even with []
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            % PathInput resolves templates — no DB filtering should happen
            scidb.for_each(@path_length, ...
                struct('filepath', scifor.PathInput("{subject}/data.mat", ...
                    'root_folder', '/data', 'name', "{subject}/data.mat")), ...
                {ScalarVar()}, ...
                'subject', [], ...
                'session', []);

            % Subject 1, session A exist in DB. With PathInput present,
            % filtering is skipped so all combos are attempted.
            % (subject=1, session=A) succeeds; no others exist but all
            % are tried because PathInput bypasses filtering.
            all_results = ScalarVar().load();
            testCase.verifyGreaterThanOrEqual(numel(all_results), 1);
        end

        function test_no_filtering_with_fixed_pathinput(testCase)
            % Fixed(PathInput) should also bypass filtering
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@path_length, ...
                struct('filepath', scidb.Fixed( ...
                    scifor.PathInput("{subject}/data.mat", ...
                        'root_folder', '/data', 'name', "{subject}/data.mat"), ...
                    'session', 'A')), ...
                {ScalarVar()}, ...
                'subject', [], ...
                'session', []);

            all_results = ScalarVar().load();
            testCase.verifyGreaterThanOrEqual(numel(all_results), 1);
        end

        function test_mixed_resolved_and_explicit(testCase)
            % One key resolved via [], one explicit — filtering applies
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            RawSignal().save([7 8 9], 'subject', 3, 'session', 'B');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [], ...
                'session', ["A", "B"]);

            % Existing combos: (1,A), (2,A), (3,B). Full product would be
            % 3 subjects * 2 sessions = 6. After filtering: 3.
            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 3);
        end

        function test_info_message_logged(testCase)
            % The combo-filtering notice is an INFO record in scidb.log
            % (console output moved to stderr in the logging redesign, which
            % evalc cannot capture — assert on the log file instead).
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'B');

            scidb.for_each(@double_values, struct('x', RawSignal()), ...
                {ProcessedSignal()}, 'subject', [], 'session', []);
            log_text = fileread(char(scidb.Log.get_path()));
            testCase.verifySubstring(log_text, 'filtered 2 non-existent schema combinations');
            testCase.verifySubstring(log_text, 'from 4 to 2');
        end

        function test_no_info_message_when_nothing_filtered(testCase)
            % All combos exist — no filtering notice in scidb.log
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            RawSignal().save([7 8 9], 'subject', 2, 'session', 'A');
            RawSignal().save([10 11 12], 'subject', 2, 'session', 'B');

            scidb.for_each(@double_values, struct('x', RawSignal()), ...
                {ProcessedSignal()}, 'subject', [], 'session', []);
            log_text = fileread(char(scidb.Log.get_path()));
            testCase.verifyTrue(~contains(log_text, 'non-existent schema combinations'));
            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 4);
        end

        function test_integer_to_string_coercion(testCase)
            % Integer subject values should match string DB values
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'B');

            % subject=[] resolves to numeric [1, 2] from DB, but
            % _schema stores VARCHAR "1", "2". Coercion must handle this.
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [], ...
                'session', []);

            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 2);
        end

        function test_dry_run_reflects_filtered_count(testCase)
            % dry_run should show the filtered iteration count
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'B');

            output = evalc( ...
                'scidb.for_each(@double_values, struct(''x'', RawSignal()), {ProcessedSignal()}, ''dry_run'', true, ''subject'', [], ''session'', [])');
            % Should say "2 iterations" not "4 iterations"
            testCase.verifySubstring(output, '2 iterations');
        end

        function test_string_schema_values_survive_combo_filtering(testCase)
            % Regression: schema_str must handle string values returned by
            % distinct_schema_values.  In some MATLAB versions, Python
            % proxy objects (py.str) can satisfy isnumeric(), causing
            % floor() to fail.  This test verifies the combo filtering path
            % works when session values are strings resolved via [].
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'X');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'Y');
            RawSignal().save([7 8 9], 'subject', 1, 'session', 'Y');

            % Both keys use [] → DB resolution → combo filtering.
            % Session values "X", "Y" must be stringified correctly by
            % schema_str; subject values 1, 2 must be handled as numerics.
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [], ...
                'session', []);

            % 3 of 4 possible combos exist: (1,X), (2,Y), (1,Y)
            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 3);
        end

        function test_pathinput_rerun_does_not_invent_combos(testCase)
            % Regression: on a re-run with a PathInput input, the filesystem
            % must stay the source of truth for which combos exist.  Prior
            % behavior re-ran the full Cartesian product of DB-resolved
            % values, inventing combos for files that never existed.
            %
            % Scenario: 3 subjects x 2 sessions = 6 possible combos, but
            % only 4 files on disk.  A prior run populated the DB so
            % distinct_schema_values now returns all 3 subjects and both
            % sessions — we must still iterate 4 combos, not 6.
            disc_dir = tempname;
            mkdir(disc_dir);
            cleanupDir = onCleanup(@() rmdir(disc_dir, 's'));

            % Files on disk: (1,A), (2,A), (1,B), (3,B).  Missing: (2,B), (3,A).
            on_disk = {{'1','A'}, {'2','A'}, {'1','B'}, {'3','B'}};
            for k = 1:numel(on_disk)
                subj = on_disk{k}{1};
                sess = on_disk{k}{2};
                d = fullfile(disc_dir, ['sub' subj]);
                if ~isfolder(d)
                    mkdir(d);
                end
                fclose(fopen(fullfile(d, ['sess' sess '.csv']), 'w'));
            end

            % Prime the DB as if a prior run produced rows for every
            % (subject, session) pair — including the two with no file.
            % distinct_schema_values will now return all 3 subjects and
            % both sessions.
            for subj = [1 2 3]
                for sess = ["A", "B"]
                    RawSignal().save([1 2 3], 'subject', subj, 'session', sess);
                end
            end

            pi = scifor.PathInput("sub{subject}/sess{session}.csv", ...
                'root_folder', disc_dir, 'name', "sub{subject}/sess{session}.csv");
            scidb.for_each(@path_length, ...
                struct('filepath', pi), ...
                {ScalarVar()}, ...
                'subject', [], ...
                'session', []);

            % Must match the 4 files on disk — not the 6-combo product.
            all_results = ScalarVar().load();
            testCase.verifyEqual(numel(all_results), 4, ...
                'PathInput re-run invented combos that have no file on disk.');
        end

        function test_pathinput_filters_by_user_provided_values(testCase)
            % When the user passes an explicit list for a template key,
            % the discovered combos are intersected with that list.
            disc_dir = tempname;
            mkdir(disc_dir);
            cleanupDir = onCleanup(@() rmdir(disc_dir, 's'));
            for subj = ["1", "2", "3"]
                d = fullfile(disc_dir, char("sub" + subj));
                mkdir(d);
                fclose(fopen(fullfile(d, 'data.csv'), 'w'));
            end

            pi = scifor.PathInput("sub{subject}/data.csv", 'root_folder', disc_dir, 'name', "sub{subject}/data.csv");
            scidb.for_each(@path_length, ...
                struct('filepath', pi), ...
                {ScalarVar()}, ...
                'subject', ["1", "2"], ...
                'session', "A");

            % Only subjects 1 and 2 should run even though subject 3 is on disk.
            all_results = ScalarVar().load();
            testCase.verifyEqual(numel(all_results), 2);
        end

    end
end
