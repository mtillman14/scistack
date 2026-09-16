classdef TestSpreadParity < matlab.unittest.TestCase
%TESTSPREADPARITY  The spread rule reaches the MATLAB path.
%
%   A returned table whose rows carry a schema key the run did NOT pin is
%   filed one record per row, at that row's own address, without
%   distribute=. scifor (Python) owns the rule (_spread_decision) and the
%   bridge applies it in for_each_save; before 2026-09-15 the MATLAB path
%   saved such a table as ONE record (scidb.log, loadFunctionalOutcomes:
%   `result_tbl shape=(1, 1)`, `DataFrame 73x27`).

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
        function test_labelled_table_spreads_with_nothing_iterated(testCase)
            % The loadFunctionalOutcomes shape: every schema level
            % deselected (no schema kwargs at all), distribute off, and the
            % function returns rows labelled by subject AND session.
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];

            scidb.for_each(@table_with_subject_session_cols, ...
                struct('x', tbl), ...
                {ProcessedSignal()});

            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 3, ...
                'one record per (subject, session) row, not one blob');

            % Row 1 (A=2) -> (A,1); row 2 (A=4) -> (A,2); row 3 (A=6) -> (B,1)
            expected = {"A", 1, 2.0; "A", 2, 4.0; "B", 1, 6.0};
            for i = 1:size(expected, 1)
                result = ProcessedSignal().load( ...
                    'subject', expected{i, 1}, 'session', expected{i, 2});
                testCase.verifyTrue(istable(result.data));
                testCase.verifyEqual(height(result.data), 1);
                testCase.verifyEqual(result.data.A, expected{i, 3}, 'AbsTol', 1e-10);
            end
        end

        function test_labelled_table_spreads_below_iterated_key(testCase)
            % Iterating subject only; the returned rows name their session
            % (non-sequential [3;1;2], so values — not row positions —
            % must decide the address). Same expectations as the
            % distribute= variant of this test in TestForEach, with
            % distribute OFF.
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];

            scidb.for_each(@table_with_session_col, ...
                struct('x', tbl), ...
                {ProcessedSignal()}, ...
                'subject', "1");

            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 3);

            expected_A = containers.Map({1, 2, 3}, {4.0, 6.0, 2.0});
            for sess = [1, 2, 3]
                result = ProcessedSignal().load('subject', '1', 'session', sess);
                testCase.verifyTrue(istable(result.data));
                testCase.verifyEqual(result.data.A, expected_A(sess), 'AbsTol', 1e-10);
            end
        end

        function test_unlabelled_table_stays_one_record(testCase)
            % No schema-key column in the returned table: nothing addresses
            % its rows, so the whole table is ONE record at the combo.
            tbl = table;
            tbl.A = [1; 2; 3];
            tbl.B = [4; 5; 6];

            scidb.for_each(@double_table_values, ...
                struct('x', tbl), ...
                {ProcessedSignal()}, ...
                'subject', "1");

            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 1);
            result = ProcessedSignal().load('subject', '1');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(height(result.data), 3);
        end
    end
end
