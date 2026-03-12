classdef TestDataRoundTrip < matlab.unittest.TestCase
%TESTDATAROUNDTRIP  Verify data type preservation through save/load cycle.
%
%   Tests that data saved from MATLAB, stored in DuckDB via Python, and
%   loaded back to MATLAB matches the original in type, shape, and values.

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
                ["subject"]);
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
        function test_double_row_vector(testCase)
            data = [1.5, 2.7, 3.9, 4.1, 5.0];
            RawSignal().save(data, 'subject', 1);
            result = RawSignal().load('subject', 1);
            testCase.verifyEqual(result, data', 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result), size(data'));
        end

        function test_double_column_vector(testCase)
            data = [1.5; 2.7; 3.9; 4.1; 5.0];
            RawSignal().save(data, 'subject', 2);
            result = RawSignal().load('subject', 2);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result), size(data));
        end

        function test_double_matrix_values(testCase)
            data = [1 2 3 4; 5 6 7 8; 9 10 11 12];
            RawSignal().save(data, 'subject', 3);
            result = RawSignal().load('subject', 3);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
        end

        function test_double_matrix_shape(testCase)
            data = randn(3, 7);
            RawSignal().save(data, 'subject', 4);
            result = RawSignal().load('subject', 4);
            testCase.verifyEqual(size(result), [3, 7]);
        end

        function test_double_square_matrix(testCase)
            data = magic(5);
            RawSignal().save(data, 'subject', 5);
            result = RawSignal().load('subject', 5);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
        end

        function test_scalar_double(testCase)
            data = 3.14159;
            ScalarVar().save(data, 'subject', 1);
            result = ScalarVar().load('subject', 1);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
        end

        function test_scalar_integer(testCase)
            data = 42;
            ScalarVar().save(data, 'subject', 2);
            result = ScalarVar().load('subject', 2);
            testCase.verifyEqual(result, 42, 'AbsTol', 1e-10);
        end

        function test_negative_values(testCase)
            data = [-1.5, -2.7, 0, 3.9, -4.1];
            RawSignal().save(data, 'subject', 6);
            result = RawSignal().load('subject', 6);
            testCase.verifyEqual(result, data', 'AbsTol', 1e-10);
        end

        function test_zeros_array(testCase)
            data = zeros(5, 3);
            RawSignal().save(data, 'subject', 7);
            result = RawSignal().load('subject', 7);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result), [5, 3]);
        end

        function test_ones_array(testCase)
            data = ones(4, 6);
            RawSignal().save(data, 'subject', 8);
            result = RawSignal().load('subject', 8);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
        end

        function test_large_matrix(testCase)
            rng(42);
            data = randn(100, 50);
            RawSignal().save(data, 'subject', 9);
            result = RawSignal().load('subject', 9);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result), [100, 50]);
        end

        function test_single_element_array(testCase)
            data = [7.7];
            RawSignal().save(data, 'subject', 10);
            result = RawSignal().load('subject', 10);
            testCase.verifyEqual(result, 7.7, 'AbsTol', 1e-10);
        end

        function test_single_precision(testCase)
            data = single([1.0, 2.0, 3.0, 4.0]);
            RawSignal().save(data, 'subject', 11);
            result = RawSignal().load('subject', 11);
            % from_python converts to double, so compare with double tolerance
            testCase.verifyEqual(result, double(data'), 'AbsTol', 1e-6);
        end

        function test_int32_array(testCase)
            data = int32([1, 2, 3, 4, 5]);
            RawSignal().save(data, 'subject', 12);
            result = RawSignal().load('subject', 12);
            testCase.verifyEqual(result, double(data'), 'AbsTol', 1e-10);
        end

        function test_matrix_element_order(testCase)
            % Verify that element ordering is preserved through
            % the MATLAB→Python→MATLAB transpose pipeline
            data = [1 2 3; 4 5 6];  % 2x3 matrix
            RawSignal().save(data, 'subject', 13);
            result = RawSignal().load('subject', 13);
            testCase.verifyEqual(result(1,1), 1, 'AbsTol', 1e-10);
            testCase.verifyEqual(result(1,2), 2, 'AbsTol', 1e-10);
            testCase.verifyEqual(result(1,3), 3, 'AbsTol', 1e-10);
            testCase.verifyEqual(result(2,1), 4, 'AbsTol', 1e-10);
            testCase.verifyEqual(result(2,2), 5, 'AbsTol', 1e-10);
            testCase.verifyEqual(result(2,3), 6, 'AbsTol', 1e-10);
        end

        function test_wide_matrix_shape(testCase)
            data = randn(2, 20);
            RawSignal().save(data, 'subject', 14);
            result = RawSignal().load('subject', 14);
            testCase.verifyEqual(size(result), [2, 20]);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
        end

        function test_tall_matrix_shape(testCase)
            data = randn(20, 2);
            RawSignal().save(data, 'subject', 15);
            result = RawSignal().load('subject', 15);
            testCase.verifyEqual(size(result), [20, 2]);
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
        end

        function test_content_hash_deterministic(testCase)
            % Same data should produce the same content hash
            data = [1 2 3 4 5];
            RawSignal().save(data, 'subject', 16);
            RawSignal().save(data, 'subject', 17);
            r1 = RawSignal().load_all('subject', 16);
            r2 = RawSignal().load_all('subject', 17);
            testCase.verifyEqual(r1(1).content_hash, r2(1).content_hash);
        end

        function test_content_hash_changes_with_data(testCase)
            RawSignal().save([1 2 3], 'subject', 18);
            RawSignal().save([1 2 4], 'subject', 19);
            r1 = RawSignal().load_all('subject', 18);
            r2 = RawSignal().load_all('subject', 19);
            testCase.verifyNotEqual(r1(1).content_hash, r2(1).content_hash);
        end
    end
end
