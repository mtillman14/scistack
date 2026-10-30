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

        function test_one_row_table(testCase)
            % A 1x1 numeric column crosses the bridge as a scalar, which
            % py.numpy.array turns into a 0-d array (tolist() -> a number).
            tbl = table(1, "A", 0.5, ...
                'VariableNames', {'subject','session','MyVar'});
            ids = ScalarVar().save_from_table(tbl, "MyVar", ["subject","session"]);
            testCase.verifyEqual(numel(ids), 1);
            r = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r.data, 0.5, 'AbsTol', 1e-10);
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

            % load should return exactly 2 records, not 4
            results = ScalarVar().load('subject', [1, 2], 'version', 'all');
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

        % --- Auto-distribute via save() ---

        function test_categorical_data_column(testCase)
            % Categorical data column must be converted to string, not fall
            % through to the per-row path which confuses save_batch into
            % dataframe mode (breaks the ON CONFLICT PRIMARY KEY clause).
            tbl = table([1;2;3], categorical(["SHAM";"STIM";"ONWARD"]), ...
                'VariableNames', {'subject','intervention'});
            ids = ScalarVar().save_from_table(tbl, "intervention", ["subject"]);
            testCase.verifyEqual(numel(ids), 3);

            v = ScalarVar().load('subject', 1);
            testCase.verifyEqual(string(v.data), "SHAM");
            v = ScalarVar().load('subject', 3);
            testCase.verifyEqual(string(v.data), "ONWARD");
        end

        function test_auto_distribute_single_data_col(testCase)
            % save() with a table whose column names include a schema key
            % should auto-distribute: one record per row, schema key as metadata.
            tbl = table([1;2;3], [10.0;20.0;30.0], ...
                'VariableNames', {'subject','MyVar'});
            ids = ScalarVar().save(tbl);
            testCase.verifyEqual(numel(ids), 3);

            v1 = ScalarVar().load('subject', 1);
            testCase.verifyEqual(v1.data, 10.0, 'AbsTol', 1e-10);
            v3 = ScalarVar().load('subject', 3);
            testCase.verifyEqual(v3.data, 30.0, 'AbsTol', 1e-10);
        end

        function test_auto_distribute_multiple_data_cols(testCase)
            % When the table has multiple non-schema-key columns, each row is
            % saved as a 1-row sub-table. Schema keys are [subject, session],
            % so MyVar and MyVar2 are the two non-schema data columns.
            tbl = table([1;2], ["A";"B"], [0.5;0.6], [1.0;2.0], ...
                'VariableNames', {'subject','session','MyVar','MyVar2'});
            ids = ScalarVar().save(tbl);
            testCase.verifyEqual(numel(ids), 2);

            v = ScalarVar().load('subject', 1, 'session', 'A');
            % data should be the sub-table of non-schema columns for that row
            testCase.verifyTrue(istable(v.data));
            testCase.verifyEqual(height(v.data), 1);
            testCase.verifyEqual(width(v.data), 2);
        end

        function test_auto_distribute_no_schema_cols_falls_through(testCase)
            % A table with no schema-key columns should save as one record.
            tbl = table([1;2;3], [10.0;20.0;30.0], ...
                'VariableNames', {'a','b'});
            id = ScalarVar().save(tbl);
            testCase.verifyEqual(numel(id), 1);  % single record_id string
        end

        function test_auto_distribute_with_common_metadata(testCase)
            % Extra name-value args become common metadata for all rows.
            scidb.configure_database( ...
                fullfile(testCase.test_dir, 'test.duckdb'), ...
                ["subject", "session"]);
            tbl = table([1;2], [7.0;8.0], ...
                'VariableNames', {'subject','MyVar'});
            ids = ScalarVar().save(tbl, 'session', 'X');
            testCase.verifyEqual(numel(ids), 2);

            v = ScalarVar().load('subject', 1, 'session', 'X');
            testCase.verifyEqual(v.data, 7.0, 'AbsTol', 1e-10);
        end

        % --- on_duplicate="combine" (plan-combine-duplicate-locations) ---

        function test_duplicate_rows_refused_by_default(testCase)
            tbl = table([1;1], [5.0;12.0], 'VariableNames', {'subject','MyVar'});
            testCase.verifyError( ...
                @() ScalarVar().save_from_table(tbl, "MyVar", "subject"), ...
                ?MException);
        end

        function test_on_duplicate_combine_lists_values(testCase)
            tbl = table([1;1;2], [5.0;12.0;7.0], ...
                'VariableNames', {'subject','MyVar'});
            ids = ScalarVar().save_from_table(tbl, "MyVar", "subject", ...
                'on_duplicate', 'combine');
            % one id per input row; combined rows share theirs
            testCase.verifyEqual(numel(ids), 3);
            testCase.verifyEqual(ids(1), ids(2));
            testCase.verifyNotEqual(ids(1), ids(3));
            v1 = ScalarVar().load('subject', 1);
            testCase.verifyEqual(double(v1.data(:))', [5 12], 'AbsTol', 1e-10);
            v2 = ScalarVar().load('subject', 2);
            testCase.verifyEqual(v2.data, 7.0, 'AbsTol', 1e-10);
        end

        function test_on_duplicate_combine_through_save(testCase)
            % save() auto-distributes a table to save_from_table; the
            % option rides along with the other name-value pairs.
            tbl = table([1;1], [5.0;12.0], 'VariableNames', {'subject','MyVar'});
            ids = ScalarVar().save(tbl, 'on_duplicate', 'combine');
            testCase.verifyEqual(numel(ids), 2);
            v = ScalarVar().load('subject', 1);
            testCase.verifyEqual(double(v.data(:))', [5 12], 'AbsTol', 1e-10);
        end

        function test_on_duplicate_d1_sequence(testCase)
            % 7 alone (DOUBLE), then 7 and 8 combined (column converted in
            % place), then 7 alone again (loads as a plain 7).
            ScalarVar().save_from_table( ...
                table(1, 7.0, 'VariableNames', {'subject','MyVar'}), "MyVar", "subject");
            ScalarVar().save_from_table( ...
                table([1;1], [7.0;8.0], 'VariableNames', {'subject','MyVar'}), ...
                "MyVar", "subject", 'on_duplicate', 'combine');
            v = ScalarVar().load('subject', 1);
            testCase.verifyEqual(double(v.data(:))', [7 8], 'AbsTol', 1e-10);
            ScalarVar().save_from_table( ...
                table(1, 7.0, 'VariableNames', {'subject','MyVar'}), "MyVar", "subject");
            v = ScalarVar().load('subject', 1);
            testCase.verifyEqual(v.data, 7.0, 'AbsTol', 1e-10);
        end

        function test_on_duplicate_invalid_value(testCase)
            tbl = table(1, 5.0, 'VariableNames', {'subject','MyVar'});
            testCase.verifyError( ...
                @() ScalarVar().save_from_table(tbl, "MyVar", "subject", ...
                    'on_duplicate', 'merge'), ?MException);
        end

    end
end
