classdef TestToCsv < matlab.unittest.TestCase
%TESTTOCSV  Tests for BaseVariable.to_csv() flat-table export.
%
%   to_csv() loads a variable across all matching schema_ids and writes one
%   row per record: one column per schema key plus a single value column
%   named after the variable type. Scalar-only — vectors/tables error.

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

        function test_scalar_export_writes_table(testCase)
            %% Scalar variable -> one row per subject/session, both schema cols
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');
            ScalarVar().save(30, 'subject', 2, 'session', 'A');

            out = fullfile(testCase.test_dir, 'scalars.csv');
            ScalarVar().to_csv(out);

            testCase.verifyTrue(isfile(out));
            tbl = readtable(out);
            cols = string(tbl.Properties.VariableNames);
            testCase.verifyTrue(ismember("subject", cols));
            testCase.verifyTrue(ismember("session", cols));
            testCase.verifyTrue(ismember("ScalarVar", cols));
            testCase.verifyEqual(height(tbl), 3);
            testCase.verifyEqual(sort(tbl.ScalarVar), [10; 20; 30]);
        end

        function test_metadata_filter_restricts_rows(testCase)
            %% Metadata kwargs are forwarded to load()
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');
            ScalarVar().save(30, 'subject', 2, 'session', 'A');

            out = fullfile(testCase.test_dir, 'subj1.csv');
            ScalarVar().to_csv(out, 'subject', 1);

            tbl = readtable(out);
            testCase.verifyEqual(height(tbl), 2);
            testCase.verifyTrue(all(tbl.subject == 1));
        end

        function test_where_filter_restricts_rows(testCase)
            %% where= (scidb.Filter) is forwarded to load()
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            ScalarVar().save(20, 'subject', 1, 'session', 'B');
            Side().save('L', 'subject', 1, 'session', 'A');
            Side().save('R', 'subject', 1, 'session', 'B');

            out = fullfile(testCase.test_dir, 'left.csv');
            ScalarVar().to_csv(out, 'where', Side() == "L");

            tbl = readtable(out);
            testCase.verifyEqual(height(tbl), 1);
            testCase.verifyEqual(tbl.ScalarVar, 10);
        end

        function test_filename_must_end_with_csv(testCase)
            %% Non-.csv filename raises an error
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            out = fullfile(testCase.test_dir, 'scalars.txt');
            testCase.verifyError(@() ScalarVar().to_csv(out), ...
                'scidb:ToCsvError');
        end

        function test_vector_variable_raises(testCase)
            %% A vector (DOUBLE[]) variable cannot be exported to flat CSV
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            out = fullfile(testCase.test_dir, 'vec.csv');
            % Python raises ValueError -> surfaces as a MATLAB:Python error
            testCase.verifyError(@() RawSignal().to_csv(out), ...
                ?MException);
        end

        function test_no_match_raises(testCase)
            %% No matching records raises (NotFoundError from load())
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            out = fullfile(testCase.test_dir, 'none.csv');
            testCase.verifyError(@() ScalarVar().to_csv(out, 'subject', 999), ...
                ?MException);
        end

        % -----------------------------------------------------------------
        % Multi-column (single-row table) + ColumnSelection
        % -----------------------------------------------------------------

        function test_single_row_table_exports_columns(testCase)
            %% A single-row table exports one column per table column
            TableVar().save(table(1.2, 110, 'VariableNames', {'speed', 'cadence'}), ...
                'subject', 1, 'session', 'A');
            TableVar().save(table(1.5, 120, 'VariableNames', {'speed', 'cadence'}), ...
                'subject', 1, 'session', 'B');

            out = fullfile(testCase.test_dir, 'table.csv');
            TableVar().to_csv(out);

            tbl = readtable(out);
            cols = string(tbl.Properties.VariableNames);
            testCase.verifyTrue(all(ismember(["subject", "session", "speed", "cadence"], cols)));
            testCase.verifyEqual(height(tbl), 2);
        end

        function test_column_selection_exports_selected_column(testCase)
            %% TableVar("speed").to_csv() exports only the selected column
            TableVar().save(table(1.2, 110, 'VariableNames', {'speed', 'cadence'}), ...
                'subject', 1, 'session', 'A');

            out = fullfile(testCase.test_dir, 'speed.csv');
            TableVar("speed").to_csv(out);

            tbl = readtable(out);
            cols = string(tbl.Properties.VariableNames);
            testCase.verifyTrue(ismember("speed", cols));
            testCase.verifyFalse(ismember("cadence", cols));
            testCase.verifyEqual(tbl.speed, 1.2, 'AbsTol', 1e-10);
        end

        function test_multirow_table_raises(testCase)
            %% A multi-row table per schema_id cannot fit one CSV row
            TableVar().save(table([1.0; 2.0; 3.0], 'VariableNames', {'x'}), ...
                'subject', 1, 'session', 'A');
            out = fullfile(testCase.test_dir, 'multirow.csv');
            testCase.verifyError(@() TableVar().to_csv(out), ?MException);
        end

        % -----------------------------------------------------------------
        % Merge
        % -----------------------------------------------------------------

        function test_extra_variable_arg_points_to_merge(testCase)
            %% Passing another variable as an arg errors (use Merge instead)
            ScalarVar().save(10, 'subject', 1, 'session', 'A');
            out = fullfile(testCase.test_dir, 'x.csv');
            testCase.verifyError(@() ScalarVar().to_csv(out, Side()), ...
                'scidb:ToCsvError');
        end

        function test_merge_of_scalars_writes_wide_table(testCase)
            %% Merge joins scalar variables column-wise on shared schema keys
            ScalarVar().save(0.65, 'subject', 1, 'session', 'A');
            ScalarVar().save(0.72, 'subject', 1, 'session', 'B');
            Side().save('L', 'subject', 1, 'session', 'A');
            Side().save('R', 'subject', 1, 'session', 'B');

            out = fullfile(testCase.test_dir, 'merged.csv');
            scidb.Merge(ScalarVar(), Side(), name="merged").to_csv(out);

            tbl = readtable(out, 'TextType', 'string');
            cols = string(tbl.Properties.VariableNames);
            testCase.verifyTrue(all(ismember(["subject", "session", "ScalarVar", "Side"], cols)));
            testCase.verifyEqual(height(tbl), 2);
        end

    end
end
