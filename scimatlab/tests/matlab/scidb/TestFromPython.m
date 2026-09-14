classdef TestFromPython < matlab.unittest.TestCase
%TESTFROMPYTHON  Unit tests for scidb.internal.from_python.
%
%   Verifies that Python proxy objects (py.int, py.str, py.float, py.list)
%   are properly converted to native MATLAB types, and that native MATLAB
%   types that were auto-converted by the Python bridge are returned
%   directly without hitting the expensive fallback path.

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (Test)

        % --- Python proxy objects must be converted, not returned as-is ---

        function test_py_int_converts_to_double(testCase)
            result = scidb.internal.from_python(py.int(42));
            testCase.verifyClass(result, 'double');
            testCase.verifyEqual(result, 42);
        end

        function test_py_float_converts_to_double(testCase)
            result = scidb.internal.from_python(py.float(3.14));
            testCase.verifyClass(result, 'double');
            testCase.verifyEqual(result, 3.14, 'AbsTol', 1e-12);
        end

        function test_py_str_converts_to_string(testCase)
            result = scidb.internal.from_python(py.str('hello'));
            testCase.verifyClass(result, 'string');
            testCase.verifyEqual(result, "hello");
        end

        function test_py_bool_true_converts_to_logical(testCase)
            result = scidb.internal.from_python(py.bool(true));
            testCase.verifyClass(result, 'logical');
            testCase.verifyTrue(result);
        end

        function test_py_bool_false_converts_to_logical(testCase)
            result = scidb.internal.from_python(py.bool(false));
            testCase.verifyClass(result, 'logical');
            testCase.verifyFalse(result);
        end

        function test_py_none_converts_to_empty(testCase)
            result = scidb.internal.from_python(py.None);
            testCase.verifyEmpty(result);
        end

        % --- Native MATLAB types (auto-converted by bridge) pass through ---

        function test_native_double_passthrough(testCase)
            result = scidb.internal.from_python(42.5);
            testCase.verifyClass(result, 'double');
            testCase.verifyEqual(result, 42.5);
        end

        function test_native_logical_passthrough(testCase)
            result = scidb.internal.from_python(true);
            testCase.verifyClass(result, 'logical');
            testCase.verifyTrue(result);
        end

        function test_native_string_passthrough(testCase)
            result = scidb.internal.from_python("hello");
            testCase.verifyClass(result, 'string');
            testCase.verifyEqual(result, "hello");
        end

        % --- Lists of numeric values ---

        function test_py_list_of_floats(testCase)
            py_list = py.list({1.0, 2.0, 3.0});
            result = scidb.internal.from_python(py_list);
            testCase.verifyClass(result, 'double');
            testCase.verifyEqual(result(:)', [1 2 3], 'AbsTol', 1e-12);
        end

        function test_py_list_of_ints(testCase)
            py_list = py.list({py.int(10), py.int(20), py.int(30)});
            result = scidb.internal.from_python(py_list);
            testCase.verifyTrue(isnumeric(result));
            testCase.verifyEqual(double(result(:)'), [10 20 30]);
        end

        function test_py_list_of_strings(testCase)
            py_list = py.list({"alpha", "beta", "gamma"});
            result = scidb.internal.from_python(py_list);
            testCase.verifyTrue(isstring(result));
            testCase.verifyEqual(result, ["alpha", "beta", "gamma"]);
        end

        % --- Numpy arrays ---

        function test_numpy_1d_float(testCase)
            py_arr = py.numpy.array({1.0, 2.0, 3.0});
            result = scidb.internal.from_python(py_arr);
            testCase.verifyClass(result, 'double');
            testCase.verifyEqual(result, [1; 2; 3]);
        end

        function test_numpy_bool(testCase)
            py_arr = py.numpy.array({true, false, true});
            result = scidb.internal.from_python(py_arr);
            testCase.verifyClass(result, 'logical');
            testCase.verifyEqual(result, [true; false; true]);
        end

        % --- Dict ---

        function test_py_dict_converts_to_struct(testCase)
            py_dict = py.dict(pyargs('a', py.int(1), 'b', 'hello'));
            result = scidb.internal.from_python(py_dict);
            testCase.verifyClass(result, 'struct');
            testCase.verifyEqual(result.a, 1);
            testCase.verifyEqual(result.b, "hello");
        end

        % --- DataFrame: all-None object column preserves row count ---
        % Regression: scidb.for_each in aggregation mode (iterating fewer
        % schema keys than the full schema) loads inputs whose un-iterated
        % schema columns are entirely NULL.  In pandas these come back as
        % an object-dtype column of Nones.  Before the fix, the MATLAB
        % conversion collapsed N empties into a single [] via vertcat,
        % producing a table assignment of width 1 to a height-N table and
        % raising "number of rows must match the height of the table".
        function test_dataframe_all_none_object_column_keeps_row_count(testCase)
            n_rows = 5;
            py_df = py.pandas.DataFrame(py.dict(pyargs( ...
                'subject', py.list({'s1','s1','s1','s1','s1'}), ...
                'cycle',   py.list({py.None, py.None, py.None, py.None, py.None}) ...
            )));
            result = scidb.internal.from_python(py_df);
            testCase.verifyTrue(istable(result), ...
                'expected a MATLAB table back from a pandas DataFrame');
            testCase.verifyEqual(height(result), n_rows, ...
                'all-None column must not collapse the table height');
            testCase.verifyTrue(ismember('cycle', string(result.Properties.VariableNames)), ...
                'cycle column should be present in the converted table');
            cycle_col = result.cycle;
            testCase.verifyEqual(size(cycle_col, 1), n_rows, ...
                'cycle column must carry one entry per row');
        end

        % --- Bulk buffer path (ndarray_via_buffer) ---
        % These arrays all cross the bridge as one byte buffer instead of
        % one crossing per element. The values, shapes and CLASSES must be
        % indistinguishable from the element-by-element path they replaced —
        % the transfer got faster, the contract did not move.

        function test_numpy_large_1d_roundtrip(testCase)
            % Big enough that the element-by-element path would be visibly
            % slow; small enough to stay a unit test.
            n = 100000;
            py_arr = py.numpy.arange(n, pyargs('dtype', 'float64'));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyClass(result, 'double');
            testCase.verifyEqual(size(result), [n 1], ...
                '1-D arrays must come back as column vectors');
            testCase.verifyEqual(result(1), 0);
            testCase.verifyEqual(result(end), n - 1);
            testCase.verifyEqual(sum(result), n * (n - 1) / 2, 'AbsTol', 1e-3);
        end

        function test_numpy_2d_orientation_is_not_transposed(testCase)
            % The buffer is written Fortran-order precisely so MATLAB can
            % reshape without a permute. A C/Fortran mix-up here would
            % silently transpose every matrix-valued record, so pin the
            % exact element positions rather than just the size.
            py_arr = py.numpy.reshape( ...
                py.numpy.arange(6, pyargs('dtype', 'float64')), ...
                py.tuple({int32(2), int32(3)}));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyEqual(size(result), [2 3]);
            testCase.verifyEqual(result, [0 1 2; 3 4 5], 'AbsTol', 1e-12);
        end

        function test_numpy_int_array_still_returns_double(testCase)
            % The element-by-element path returned double for every numeric
            % dtype; TestDataRoundTrip.test_int32_array depends on it.
            py_arr = py.numpy.array(py.list({py.int(1), py.int(2), py.int(3)}));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyClass(result, 'double', ...
                'integer numpy arrays must still arrive as double');
            testCase.verifyEqual(result, [1; 2; 3], 'AbsTol', 1e-12);
        end

        function test_numpy_single_array_still_returns_double(testCase)
            py_arr = py.numpy.array(py.list({1.5, 2.5}), pyargs('dtype', 'float32'));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyClass(result, 'double', ...
                'single-precision numpy arrays must still arrive as double');
            testCase.verifyEqual(result, [1.5; 2.5], 'AbsTol', 1e-6);
        end

        function test_numpy_empty_array_returns_empty(testCase)
            py_arr = py.numpy.array(py.list({}), pyargs('dtype', 'float64'));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyEmpty(result);
        end

        function test_numpy_nan_and_inf_survive_the_buffer(testCase)
            py_np = py.importlib.import_module('numpy');
            py_arr = py_np.array(py.list({py_np.nan, py_np.inf, -1.5}));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyTrue(isnan(result(1)));
            testCase.verifyEqual(result(2), Inf);
            testCase.verifyEqual(result(3), -1.5, 'AbsTol', 1e-12);
        end

        function test_numpy_object_array_falls_back(testCase)
            % Object dtype has no raw buffer: the buffer path must decline
            % and the element-by-element path must still produce a cell.
            py_arr = py.numpy.array(py.list({'alpha', 'beta'}), ...
                pyargs('dtype', 'object'));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyEqual(numel(result), 2, ...
                'object arrays must still convert element-by-element');
        end

        function test_numpy_bool_2d_roundtrip(testCase)
            % bool crosses as one byte per element and comes back logical.
            py_arr = py.numpy.reshape( ...
                py.numpy.array(py.list({true, false, true, true})), ...
                py.tuple({int32(2), int32(2)}));
            result = scidb.internal.from_python(py_arr);
            testCase.verifyClass(result, 'logical');
            testCase.verifyEqual(result, [true false; true true]);
        end

    end
end
