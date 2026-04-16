classdef TestDuckDBTypes < matlab.unittest.TestCase
%TESTDUCKDBTYPES  Verify DuckDB storage types for structs and tables.
%
%   Tests that MATLAB structs (→ Python dicts) and tables (→ DataFrames)
%   produce the correct DuckDB column types and round-trip correctly.

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
        % =================================================================
        % Struct (dict) tests — DuckDB type verification
        % =================================================================

        function test_struct_scalar_doubles(testCase)
            %% Struct with scalar double fields → DOUBLE columns
            s = struct('ratio', 3.14, 'offset', -1.5);
            StructVar().save(s, 'subject', 1, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('ratio'), "DOUBLE");
            testCase.verifyEqual(types('offset'), "DOUBLE");

            result = StructVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data.ratio, 3.14, 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.offset, -1.5, 'AbsTol', 1e-10);
        end

        function test_struct_scalar_int(testCase)
            %% Struct with int64 scalar → BIGINT column
            s = struct('count', int64(42));
            StructVar().save(s, 'subject', 2, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('count'), "BIGINT");

            result = StructVar().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(result.data.count, 42, 'AbsTol', 1e-10);
        end

        function test_struct_scalar_string(testCase)
            %% Struct with string field → VARCHAR column
            s = struct('name', "hello");
            StructVar().save(s, 'subject', 3, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('name'), "VARCHAR");

            result = StructVar().load('subject', 3, 'session', 'A');
            testCase.verifyEqual(result.data.name, "hello");
        end

        function test_struct_scalar_bool(testCase)
            %% Struct with logical field → BOOLEAN column
            s = struct('flag', true, 'enabled', false);
            StructVar().save(s, 'subject', 4, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('flag'), "BOOLEAN");
            testCase.verifyEqual(types('enabled'), "BOOLEAN");

            result = StructVar().load('subject', 4, 'session', 'A');
            testCase.verifyTrue(result.data.flag);
            testCase.verifyFalse(result.data.enabled);
        end

        function test_struct_float_vectors(testCase)
            %% Struct with double vector fields → DOUBLE[] columns
            s = struct( ...
                'force', [1.0, 2.0, 3.0, 4.0, 5.0], ...
                'velocity', [10.0, 20.0, 30.0, 40.0, 50.0]);
            StructVar().save(s, 'subject', 5, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('force'), "DOUBLE[]");
            testCase.verifyEqual(types('velocity'), "DOUBLE[]");

            result = StructVar().load('subject', 5, 'session', 'A');
            testCase.verifyEqual(result.data.force, [1;2;3;4;5], 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.velocity, [10;20;30;40;50], 'AbsTol', 1e-10);
        end

        function test_struct_int_vectors(testCase)
            %% Struct with int64 vector fields → BIGINT[] columns
            s = struct( ...
                'ids', int64([10, 20, 30]), ...
                'counts', int64([1, 2, 3]));
            StructVar().save(s, 'subject', 6, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('ids'), "BIGINT[]");
            testCase.verifyEqual(types('counts'), "BIGINT[]");

            result = StructVar().load('subject', 6, 'session', 'A');
            testCase.verifyEqual(result.data.ids, [10;20;30], 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.counts, [1;2;3], 'AbsTol', 1e-10);
        end

        function test_struct_string_array(testCase)
            %% Struct with string array field → VARCHAR[] column
            s = struct('tags', ["alpha", "beta", "gamma"]);
            StructVar().save(s, 'subject', 7, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('tags'), "VARCHAR[]");

            result = StructVar().load('subject', 7, 'session', 'A');
            testCase.verifyEqual(result.data.tags, ["alpha", "beta", "gamma"]);
        end

        function test_struct_mixed_scalar_and_vector(testCase)
            %% Struct mixing scalars and vectors → correct per-field types
            s = struct( ...
                'name', "experiment_1", ...
                'weight', 0.75, ...
                'scores', [90.0, 85.5, 92.3]);
            StructVar().save(s, 'subject', 8, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('name'), "VARCHAR");
            testCase.verifyEqual(types('weight'), "DOUBLE");
            testCase.verifyEqual(types('scores'), "DOUBLE[]");

            result = StructVar().load('subject', 8, 'session', 'A');
            testCase.verifyEqual(result.data.name, "experiment_1");
            testCase.verifyEqual(result.data.weight, 0.75, 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.scores, [90.0;85.5;92.3], 'AbsTol', 1e-10);
        end

        function test_struct_equal_length_vectors_one_row(testCase)
            %% Struct with equal-length arrays must be ONE record, not N rows
            s = struct( ...
                'force', (0:9), ...
                'velocity', (10:19));
            StructVar().save(s, 'subject', 9, 'session', 'A');

            % Must be array columns, not scalar
            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('force'), "DOUBLE[]");
            testCase.verifyEqual(types('velocity'), "DOUBLE[]");

            % Must be exactly one row
            db = scidb.get_database();
            duck = py.getattr(db, '_duck');
            count = duck.fetchall( ...
                'SELECT COUNT(*) FROM "StructVar_data"');
            count = double(count{1}{1});
            testCase.verifyEqual(count, 1);

            % Must round-trip as full vectors
            result = StructVar().load('subject', 9, 'session', 'A');
            testCase.verifyEqual(numel(result.data.force), 10);
            testCase.verifyEqual(numel(result.data.velocity), 10);
            testCase.verifyEqual(result.data.force, (0:9)', 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.velocity, (10:19)', 'AbsTol', 1e-10);
        end

        function test_struct_column_vectors(testCase)
            %% Struct with Nx1 column vectors store as DOUBLE[] and load as columns
            s = struct( ...
                'force', (0:9)', ...
                'velocity', (10:19)');
            StructVar().save(s, 'subject', 10, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('force'), "DOUBLE[]");
            testCase.verifyEqual(types('velocity'), "DOUBLE[]");

            result = StructVar().load('subject', 10, 'session', 'A');
            testCase.verifyEqual(result.data.force, (0:9)', 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.velocity, (10:19)', 'AbsTol', 1e-10);
        end

        function test_struct_2d_matrix(testCase)
            %% Struct with 2D matrix field → DOUBLE[][] column
            s = struct('matrix', [1 2 3; 4 5 6]);
            StructVar().save(s, 'subject', 11, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('matrix'), "DOUBLE[][]");

            result = StructVar().load('subject', 11, 'session', 'A');
            testCase.verifyEqual(result.data.matrix, [1 2 3; 4 5 6], 'AbsTol', 1e-10);
        end

        function test_struct_vector_orientation(testCase)
            %% Both row and column vectors store as DOUBLE[] and load as Nx1 columns.
            %  Orientation is not preserved on save, but all vectors load as columns.
            s = struct('row_vec', [1.0, 2.0, 3.0], ...   % 1x3
                       'col_vec', [4.0; 5.0; 6.0]);      % 3x1
            StructVar().save(s, 'subject', 12, 'session', 'A');

            types = testCase.getColumnTypes('StructVar_data');
            testCase.verifyEqual(types('row_vec'), "DOUBLE[]");
            testCase.verifyEqual(types('col_vec'), "DOUBLE[]");

            result = StructVar().load('subject', 12, 'session', 'A');
            % Both load as column vectors regardless of original orientation
            testCase.verifyEqual(result.data.row_vec, [1;2;3], 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.col_vec, [4;5;6], 'AbsTol', 1e-10);
        end

        % =================================================================
        % Table (DataFrame) tests — DuckDB type verification
        % =================================================================

        function test_table_float_columns(testCase)
            %% Table with double columns → DOUBLE (one DuckDB row per table row)
            t = table([1.0; 2.0; 3.0], [4.0; 5.0; 6.0], ...
                'VariableNames', {'force', 'velocity'});
            TableVar().save(t, 'subject', 1, 'session', 'A');

            types = testCase.getColumnTypes('TableVar_data');
            testCase.verifyEqual(types('force'), "DOUBLE");
            testCase.verifyEqual(types('velocity'), "DOUBLE");

            result = TableVar().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(result.data.force, [1; 2; 3], 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.velocity, [4; 5; 6], 'AbsTol', 1e-10);
        end

        function test_table_int_columns(testCase)
            %% Table with int64 columns → BIGINT (one DuckDB row per table row)
            t = table(int64([10; 20; 30]), int64([1; 2; 3]), ...
                'VariableNames', {'ids', 'counts'});
            TableVar().save(t, 'subject', 2, 'session', 'A');

            types = testCase.getColumnTypes('TableVar_data');
            testCase.verifyEqual(types('ids'), "BIGINT");
            testCase.verifyEqual(types('counts'), "BIGINT");

            result = TableVar().load('subject', 2, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(result.data.ids, [10; 20; 30], 'AbsTol', 1e-10);
            testCase.verifyEqual(result.data.counts, [1; 2; 3], 'AbsTol', 1e-10);
        end

        function test_table_string_columns(testCase)
            %% Table with string columns → VARCHAR (one DuckDB row per table row)
            t = table(["alice"; "bob"; "carol"], ["A"; "B"; "A"], ...
                'VariableNames', {'name', 'grp'});
            TableVar().save(t, 'subject', 3, 'session', 'A');

            types = testCase.getColumnTypes('TableVar_data');
            testCase.verifyEqual(types('name'), "VARCHAR");
            testCase.verifyEqual(types('grp'), "VARCHAR");

            result = TableVar().load('subject', 3, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(result.data.name, ["alice"; "bob"; "carol"]);
            testCase.verifyEqual(result.data.grp, ["A"; "B"; "A"]);
        end

        function test_table_mixed_columns(testCase)
            %% Table with float, int, and string columns → correct types
            t = table( ...
                [95.5; 87.3; 91.0], ...
                int64([1; 2; 3]), ...
                ["alice"; "bob"; "carol"], ...
                'VariableNames', {'score', 'rank', 'name'});
            TableVar().save(t, 'subject', 4, 'session', 'A');

            types = testCase.getColumnTypes('TableVar_data');
            testCase.verifyEqual(types('score'), "DOUBLE");
            testCase.verifyEqual(types('rank'), "BIGINT");
            testCase.verifyEqual(types('name'), "VARCHAR");

            result = TableVar().load('subject', 4, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(result.data.Properties.VariableNames, ...
                {'score', 'rank', 'name'});
        end

        function test_table_single_row(testCase)
            %% A 1-row table should round-trip as a table
            t = table(42.0, "only", 'VariableNames', {'x', 'label'});
            TableVar().save(t, 'subject', 5, 'session', 'A');

            types = testCase.getColumnTypes('TableVar_data');
            testCase.verifyEqual(types('x'), "DOUBLE");
            testCase.verifyEqual(types('label'), "VARCHAR");

            result = TableVar().load('subject', 5, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(height(result.data), 1);
        end

        % function test_table_multiple_rows(testCase)
        %     %% A multiple-row table should round-trip as a multiple-row table
        %     t = table([1; 2], [3; 4], 'VariableNames', {'a','b'});
        %     TableVar().save(t, 'subject', [1, 1], 'session', {'A', 'B'});
        % 
        %     types = testCase.getColumnTypes('TableVar_data');
        %     testCase.verifyEqual(types('a'), "DOUBLE[]");
        %     testCase.verifyEqual(types('b'), "DOUBLE[]");
        % end


        function test_table_column_order_preserved(testCase)
            %% Column order of the original table should be preserved on load
            t = table([1; 2], [3; 4], [5; 6], ...
                'VariableNames', {'z_col', 'a_col', 'm_col'});
            TableVar().save(t, 'subject', 6, 'session', 'A');

            result = TableVar().load('subject', 6, 'session', 'A');
            testCase.verifyTrue(istable(result.data));
            testCase.verifyEqual(result.data.Properties.VariableNames, ...
                {'z_col', 'a_col', 'm_col'});
        end
    end

    methods (Access = private)
        function types = getColumnTypes(testCase, table_name)
            %% Query DuckDB information_schema for column types
            db = scidb.get_database();
            duck = py.getattr(db, '_duck');
            rows = duck.fetchall( ...
                "SELECT column_name, data_type FROM information_schema.columns " + ...
                "WHERE table_name = ? ORDER BY ordinal_position", ...
                py.list({table_name}));

            types = containers.Map();
            rows_cell = cell(rows);
            for i = 1:numel(rows_cell)
                row = cell(rows_cell{i});
                col_name = string(row{1});
                col_type = string(row{2});
                if col_name ~= "record_id"
                    types(col_name) = col_type;
                end
            end
        end
    end
end
