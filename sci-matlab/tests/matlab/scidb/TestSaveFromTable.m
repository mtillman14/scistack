classdef TestSaveFromTable < matlab.unittest.TestCase
%TESTSAVEFROMTABLE  Integration tests for BaseVariable save_from_table.

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

        % --- Basic functionality ---

        function test_returns_record_ids(testCase)
            tbl = table([1;2;3], [10;20;30], ["A";"B";"C"], ...
                'VariableNames', {'subject','MyVar','session'});
            ids = ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);
            testCase.verifyEqual(numel(ids), 3);
            for i = 1:3
                testCase.verifyEqual(strlength(ids(i)), 16);
            end
        end

        function test_data_loads_back_correctly(testCase)
            tbl = table([1;1;2], ["A";"B";"A"], [0.5;0.6;0.7], ...
                'VariableNames', {'subject','session','MyVar'});
            ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);

            v1 = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(v1.data, 0.5, 'AbsTol', 1e-10);

            v2 = ScalarVar().load('subject', 1, 'session', 'B');
            testCase.verifyEqual(v2.data, 0.6, 'AbsTol', 1e-10);

            v3 = ScalarVar().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(v3.data, 0.7, 'AbsTol', 1e-10);
        end

        function test_record_ids_are_unique(testCase)
            tbl = table([1;2;3], [10;20;30], ["A";"B";"C"], ...
                'VariableNames', {'subject','MyVar','session'});
            ids = ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);
            testCase.verifyEqual(numel(unique(ids)), 3);
        end

        % --- Common metadata ---

        function test_common_metadata_applied_to_all(testCase)
            tbl = table([1;2], [0.5;0.6], ...
                'VariableNames', {'subject','MyVar'});
            ScalarVar().save_from_table(tbl, "MyVar", ["subject"], ...
                'session', 'X');

            v1 = ScalarVar().load('subject', 1, 'session', 'X');
            testCase.verifyEqual(v1.data, 0.5, 'AbsTol', 1e-10);

            v2 = ScalarVar().load('subject', 2, 'session', 'X');
            testCase.verifyEqual(v2.data, 0.6, 'AbsTol', 1e-10);
        end

        % --- Idempotency ---

        function test_idempotent_no_duplicates(testCase)
            tbl = table([1;2], ["A";"B"], [0.5;0.6], ...
                'VariableNames', {'subject','session','MyVar'});

            ids1 = ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);
            ids2 = ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);

            testCase.verifyEqual(ids1, ids2);

            % load_all should return exactly 2 records, not 4
            results = ScalarVar().load_all('subject', [1, 2]);
            testCase.verifyEqual(numel(results), 2);
        end

        % --- Integer data ---

        function test_integer_data(testCase)
            tbl = table([1;2], ["A";"B"], int64([100;200]), ...
                'VariableNames', {'subject','session','MyVar'});
            ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);

            v1 = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(v1.data, 100);
        end

        % --- Many rows ---

        function test_many_rows(testCase)
            n = 200;
            subjects = repmat((1:10)', 20, 1);
            sessions = repelem(string((1:20)'), 10);
            values = (1:n)' * 0.1;
            tbl = table(subjects, sessions, values, ...
                'VariableNames', {'subject','session','MyVar'});

            ids = ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);
            testCase.verifyEqual(numel(ids), n);

            % Spot-check a few values
            v = ScalarVar().load('subject', 1, 'session', '1');
            testCase.verifyEqual(v.data, 0.1, 'AbsTol', 1e-10);

            v = ScalarVar().load('subject', 10, 'session', '20');
            testCase.verifyEqual(v.data, n * 0.1, 'AbsTol', 1e-10);
        end

        % --- Numeric metadata ---

        function test_numeric_metadata_columns(testCase)
            tbl = table([1;1;2;2], [1;2;1;2], [0.1;0.2;0.3;0.4], ...
                'VariableNames', {'subject','session','MyVar'});
            ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);

            v = ScalarVar().load('subject', 2, 'session', 2);
            testCase.verifyEqual(v.data, 0.4, 'AbsTol', 1e-10);
        end

        % --- Empty table ---

        function test_empty_table_returns_empty(testCase)
            tbl = table(double.empty(0,1), string.empty(0,1), double.empty(0,1), ...
                'VariableNames', {'subject','session','MyVar'});
            ids = ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);
            testCase.verifyEmpty(ids);
        end

        % --- String data ---

        function test_string_data_column(testCase)
            tbl = table([1;2], ["A";"B"], ["hello";"world"], ...
                'VariableNames', {'subject','session','MyVar'});
            ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);

            v = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(string(v.data), "hello");
        end

    end
end
