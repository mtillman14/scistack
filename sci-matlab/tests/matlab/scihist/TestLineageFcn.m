classdef TestLineageFcn < matlab.unittest.TestCase
%TESTLINEAGEFCN  Integration tests for scidb.LineageFcn execution and caching.

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
            scihist.configure_database( ...
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
        % --- Basic LineageFcn creation ---

        function test_create_lineage_fcn(testCase)
            lfcn = scidb.LineageFcn(@double_values);
            testCase.verifyClass(lfcn, 'scidb.LineageFcn');
        end

        function test_create_lineage_fcn_with_unpack_output(testCase)
            lfcn = scidb.LineageFcn(@split_data, 'unpack_output', true);
            testCase.verifyClass(lfcn, 'scidb.LineageFcn');
        end

        function test_anonymous_function_errors(testCase)
            testCase.verifyError( ...
                @() scidb.LineageFcn(@(x) x + 1), ...
                'scidb:AnonymousFunction');
        end

        % --- LineageFcn execution ---

        function test_lineage_fcn_returns_result(testCase)
            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn([1 2 3]);
            testCase.verifyClass(result, 'scidb.LineageFcnResult');
        end

        function test_lineage_fcn_computes_correct_result(testCase)
            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn([1 2 3]);
            testCase.verifyEqual(result.data, [2 4 6], 'AbsTol', 1e-10);
        end

        function test_lineage_fcn_with_constant_argument(testCase)
            lfcn = scidb.LineageFcn(@add_offset);
            result = lfcn([10 20 30], 5);
            testCase.verifyEqual(result.data, [15 25 35], 'AbsTol', 1e-10);
        end

        function test_lineage_fcn_with_matrix_input(testCase)
            lfcn = scidb.LineageFcn(@double_values);
            data = [1 2; 3 4; 5 6];
            result = lfcn(data);
            testCase.verifyEqual(result.data, data * 2, 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result.data), [3, 2]);
        end

        function test_lineage_fcn_with_scalar_input(testCase)
            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn(21);
            testCase.verifyEqual(result.data, 42, 'AbsTol', 1e-10);
        end

        function test_lineage_fcn_result_has_py_obj(testCase)
            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn([1 2 3]);
            testCase.verifyNotEmpty(result.py_obj);
        end

        % --- LineageFcn with loaded inputs ---

        function test_lineage_fcn_with_loaded_variable(testCase)
            % Save raw data, load it, pass to lineage function
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');
            raw = RawSignal().load('subject', 1, 'session', 'A');

            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn(raw);

            testCase.verifyEqual(result.data, [20 40 60]', 'AbsTol', 1e-10);
        end

        function test_lineage_fcn_with_two_loaded_variables(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            ProcessedSignal().save([10 20 30]', 'subject', 1, 'session', 'A');

            raw = RawSignal().load('subject', 1, 'session', 'A');
            proc = ProcessedSignal().load('subject', 1, 'session', 'A');

            lfcn = scidb.LineageFcn(@sum_inputs);
            result = lfcn(raw, proc);

            testCase.verifyEqual(result.data, [11 22 33]', 'AbsTol', 1e-10);
        end

        function test_lineage_fcn_with_mixed_inputs(testCase)
            % One loaded variable + one constant
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');
            raw = RawSignal().load('subject', 1, 'session', 'A');

            lfcn = scidb.LineageFcn(@add_offset);
            result = lfcn(raw, 5);

            testCase.verifyEqual(result.data, [15 25 35]', 'AbsTol', 1e-10);
        end

        % --- Chained lineage functions ---

        function test_chained_lineage_fcns(testCase)
            lfcn1 = scidb.LineageFcn(@double_values);
            lfcn2 = scidb.LineageFcn(@triple_values);

            result1 = lfcn1([1 2 3]);
            result2 = lfcn2(result1);

            testCase.verifyEqual(result1.data, [2 4 6], 'AbsTol', 1e-10);
            testCase.verifyEqual(result2.data, [6 12 18], 'AbsTol', 1e-10);
        end

        function test_chained_lineage_fcns_preserve_lineage(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            raw = RawSignal().load('subject', 1, 'session', 'A');

            lfcn1 = scidb.LineageFcn(@double_values);
            lfcn2 = scidb.LineageFcn(@triple_values);

            step1 = lfcn1(raw);
            step2 = lfcn2(step1);

            % Save final result and verify lineage is tracked
            FilteredSignal().save(step2, 'subject', 1, 'session', 'A');
            loaded = FilteredSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(strlength(loaded.lineage_hash) > 0);

            % Provenance should reference the last function in the chain
            prov = FilteredSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(prov.function_name), 'triple_values');
        end

        % --- Multi-output via nargout (natural MATLAB pattern) ---

        function test_multi_nargout_returns_separate_results(testCase)
            % When a function naturally returns multiple outputs, LineageFcn
            % should return one LineageFcnResult per requested output.
            lfcn = scidb.LineageFcn(@multi_output_fn);
            [r1, r2, r3] = lfcn([1 2 3]);
            testCase.verifyClass(r1, 'scidb.LineageFcnResult');
            testCase.verifyClass(r2, 'scidb.LineageFcnResult');
            testCase.verifyClass(r3, 'scidb.LineageFcnResult');
            testCase.verifyEqual(r1.data, [2 4 6], 'AbsTol', 1e-10);
            testCase.verifyEqual(r2.data, [3 6 9], 'AbsTol', 1e-10);
            testCase.verifyEqual(r3.data, [4 8 12], 'AbsTol', 1e-10);
        end

        function test_multi_nargout_single_output_still_works(testCase)
            % Requesting 1 output from a multi-output function should still work.
            lfcn = scidb.LineageFcn(@multi_output_fn);
            r1 = lfcn([1 2 3]);
            testCase.verifyClass(r1, 'scidb.LineageFcnResult');
            testCase.verifyEqual(r1.data, [2 4 6], 'AbsTol', 1e-10);
        end

        function test_multi_nargout_different_lineage_hashes(testCase)
            lfcn = scidb.LineageFcn(@multi_output_fn);
            [r1, r2, r3] = lfcn([1 2 3]);
            SplitFirst().save(r1, 'subject', 1, 'session', 'A');
            SplitSecond().save(r2, 'subject', 1, 'session', 'A');
            FilteredSignal().save(r3, 'subject', 1, 'session', 'A');
            loaded1 = SplitFirst().load('subject', 1, 'session', 'A');
            loaded2 = SplitSecond().load('subject', 1, 'session', 'A');
            loaded3 = FilteredSignal().load('subject', 1, 'session', 'A');
            testCase.verifyNotEqual(loaded1.lineage_hash, loaded2.lineage_hash);
            testCase.verifyNotEqual(loaded2.lineage_hash, loaded3.lineage_hash);
        end

        % --- Unpack output (multi-output) ---

        function test_unpack_output_returns_multiple(testCase)
            lfcn = scidb.LineageFcn(@split_data, 'unpack_output', true);
            [first, second] = lfcn([1 2 3 4 5 6]);
            testCase.verifyClass(first, 'scidb.LineageFcnResult');
            testCase.verifyClass(second, 'scidb.LineageFcnResult');
            testCase.verifyEqual(first.data, [1 2 3], 'AbsTol', 1e-10);
            testCase.verifyEqual(second.data, [4 5 6], 'AbsTol', 1e-10);
        end

        function test_unpack_output_save_separately(testCase)
            lfcn = scidb.LineageFcn(@split_data, 'unpack_output', true);
            [first, second] = lfcn([10 20 30 40]);

            SplitFirst().save(first, 'subject', 1, 'session', 'A');
            SplitSecond().save(second, 'subject', 1, 'session', 'A');

            r1 = SplitFirst().load('subject', 1, 'session', 'A');
            r2 = SplitSecond().load('subject', 1, 'session', 'A');

            testCase.verifyEqual(r1.data, [10 20]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2.data, [30 40]', 'AbsTol', 1e-10);
        end

        function test_unpack_output_different_lineage_hashes(testCase)
            lfcn = scidb.LineageFcn(@split_data, 'unpack_output', true);
            [first, second] = lfcn([1 2 3 4]');

            SplitFirst().save(first, 'subject', 1, 'session', 'A');
            SplitSecond().save(second, 'subject', 1, 'session', 'A');

            r1 = SplitFirst().load('subject', 1, 'session', 'A');
            r2 = SplitSecond().load('subject', 1, 'session', 'A');

            % Different output_nums should produce different lineage hashes
            testCase.verifyNotEqual(r1.lineage_hash, r2.lineage_hash);
        end

        % --- Caching ---

        function test_cache_hit_returns_same_data(testCase)
            lfcn = scidb.LineageFcn(@double_values);

            % First call: cache miss, function executes
            result1 = lfcn([1 2 3]);
            ProcessedSignal().save(result1, 'subject', 1, 'session', 'A');

            % Second call with same args: should hit cache
            result2 = lfcn([1 2 3]);
            testCase.verifyEqual(result2.data, [2 4 6]', 'AbsTol', 1e-10);
        end

        function test_cache_miss_with_different_inputs(testCase)
            lfcn = scidb.LineageFcn(@double_values);

            result1 = lfcn([1 2 3]);
            ProcessedSignal().save(result1, 'subject', 1, 'session', 'A');

            % Different input should not hit cache
            result2 = lfcn([10 20 30]);
            testCase.verifyEqual(result2.data, [20 40 60], 'AbsTol', 1e-10);
        end

        function test_cache_miss_with_different_function(testCase)
            lfcn1 = scidb.LineageFcn(@double_values);
            lfcn2 = scidb.LineageFcn(@triple_values);

            result1 = lfcn1([1 2 3]);
            ProcessedSignal().save(result1, 'subject', 1, 'session', 'A');

            % Different function with same input should not hit cache
            result2 = lfcn2([1 2 3]);
            testCase.verifyEqual(result2.data, [3 6 9], 'AbsTol', 1e-10);
        end

        % --- Save lineage function result and verify lineage ---

        function test_save_lineage_result_stores_lineage_hash(testCase)
            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn([1 2 3]);
            ProcessedSignal().save(result, 'subject', 1, 'session', 'A');

            loaded = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(strlength(loaded.lineage_hash) > 0);
        end

        function test_lineage_result_as_input_tracks_lineage(testCase)
            % Save raw, load, lineage fcn, save processed
            RawSignal().save([5 10 15], 'subject', 1, 'session', 'A');
            raw = RawSignal().load('subject', 1, 'session', 'A');

            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn(raw);
            ProcessedSignal().save(result, 'subject', 1, 'session', 'A');

            % Verify provenance references the input
            prov = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyFalse(isempty(prov));
            testCase.verifyEqual(char(prov.function_name), 'double_values');
            testCase.verifyTrue(numel(prov.inputs) >= 1);
        end
    end
end
