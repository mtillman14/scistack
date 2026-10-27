classdef TestMetadataNumbers < matlab.unittest.TestCase
%TESTMETADATANUMBERS  One rule for MATLAB metadata numbers on every route.
%
%   MATLAB numbers are doubles; metadata is hashed into record ids as given
%   (canonical_hash: int 1 -> "1", float 1.0 -> "1.0"). The owner is
%   scidb.internal.metadata_number (whole -> int64); number_to_python
%   (save, load filters, save_from_table, include/exclude_schema) and
%   whole_metadata_columns (for_each result tables) consume it. Before
%   2026-10-18 save_from_table and for_each sent subject=1.0 while save()
%   sent 1, and the returned for_each table must still hold doubles.

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
        % --- The rule (owner) ---

        function test_whole_numbers_become_int64(testCase)
            [v, whole] = scidb.internal.metadata_number(1);
            testCase.verifyClass(v, 'int64');
            testCase.verifyTrue(whole);
            [v, whole] = scidb.internal.metadata_number([1 2 3]);
            testCase.verifyClass(v, 'int64');
            testCase.verifyTrue(whole);
        end

        function test_non_whole_numbers_stay_double(testCase)
            for val = {1.5, [1 2.5], NaN, Inf, [1 NaN]}
                [v, whole] = scidb.internal.metadata_number(val{1});
                testCase.verifyClass(v, 'double');
                testCase.verifyFalse(whole);
            end
        end

        % --- Consumers ---

        function test_number_to_python(testCase)
            testCase.verifyClass(scidb.internal.number_to_python(1), 'py.int');
            arr = scidb.internal.number_to_python([1 2]);
            testCase.verifyEqual(char(arr.dtype.name), 'int64');
            arr = scidb.internal.number_to_python([1 2.5]);
            testCase.verifyEqual(char(arr.dtype.name), 'float64');
        end

        function test_whole_metadata_columns_skips_data_and_bookkeeping(testCase)
            tbl = table([1; 2], ["A"; "B"], [1.5; 2], [3; 4], [1; 2], ...
                'VariableNames', {'subject', 'session', 'speed', 'MyOut', 'x__combo'});
            [out, cast_cols] = scidb.internal.whole_metadata_columns(tbl, {'MyOut'});
            testCase.verifyClass(out.subject, 'int64');
            testCase.verifyClass(out.speed, 'double');     % not whole
            testCase.verifyClass(out.MyOut, 'double');     % output data
            testCase.verifyClass(out.x__combo, 'double');  % bookkeeping
            testCase.verifyEqual(cast_cols, "subject");
        end

        % --- End to end ---

        function test_for_each_saves_integer_metadata(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            scidb.for_each(@double_values, struct('x', RawSignal()), ...
                {ProcessedSignal()}, 'subject', 1, 'session', "A");
            log_path = fullfile(testCase.test_dir, 'scistack.log');
            testCase.assumeTrue(isfile(log_path), 'scistack.log not written next to the database');
            log_text = fileread(log_path);
            testCase.verifySubstring(log_text, '[save] subject=1, session=A: ProcessedSignal');
            testCase.verifyFalse(contains(log_text, '[save] subject=1.0'));
        end

        function test_for_each_returned_table_keeps_double(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            result = scidb.for_each(@double_values, struct('x', RawSignal()), ...
                {ProcessedSignal()}, 'subject', [1 2], 'session', "A");
            testCase.verifyClass(result.subject, 'double');
            testCase.verifyEqual(sort(result.subject(:))', [1 2]);
        end

        function test_save_and_save_from_table_same_record_id(testCase)
            id_direct = ScalarVar().save(0.5, 'subject', 3, 'session', 'A');
            tbl = table([3; 4], ["A"; "A"], [0.5; 0.6], ...
                'VariableNames', {'subject', 'session', 'MyVar'});
            ids = ScalarVar().save_from_table(tbl, "MyVar", ["subject", "session"]);
            testCase.verifyEqual(ids(1), id_direct);
        end
    end
end
