classdef TestHistProvenance < matlab.unittest.TestCase
%TESTPROVENANCE  Integration tests for provenance tracking via the
%   bipartite provenance graph.
%
%   Post lineage-simplification migration: there is no per-call lineage
%   wrapper. Provenance is recorded automatically by scidb.for_each and
%   read back via Type().provenance(...), which surfaces the graph's
%   producing-invocation function name/hash, variable inputs, and
%   constants.

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
        function test_raw_data_no_provenance(testCase)
            % A raw direct save has no producing invocation -> [].
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            prov = RawSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEmpty(prov);
        end

        function test_for_each_output_has_provenance(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");

            prov = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyFalse(isempty(prov));
            testCase.verifyTrue(isstruct(prov));
        end

        function test_provenance_function_name(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");

            prov = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(prov.function_name), 'double_values');
        end

        function test_provenance_function_hash(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");

            prov = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            % MATLAB function source hash is a 64-char SHA-256 hex digest.
            testCase.verifyTrue(strlength(prov.function_hash) == 64);
        end

        function test_provenance_has_required_fields(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");

            prov = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyTrue(isfield(prov, 'function_name'));
            testCase.verifyTrue(isfield(prov, 'function_hash'));
            testCase.verifyTrue(isfield(prov, 'inputs'));
            testCase.verifyTrue(isfield(prov, 'constants'));
        end

        function test_provenance_inputs_and_constants(testCase)
            % add_offset(x, offset): x is a loaded variable input, offset is
            % a constant.
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 5), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");

            prov = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(numel(prov.inputs), 1);
            % constants is a struct {param_name: value}.
            testCase.verifyEqual(numel(fieldnames(prov.constants)), 1);
            testCase.verifyTrue(isfield(prov.constants, 'offset'));
            testCase.verifyEqual(double(prov.constants.offset), 5);
        end

        function test_provenance_chained_for_each(testCase)
            % Chain: raw -> double_values -> triple_values
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");

            scidb.for_each(@triple_values, ...
                struct('x', ProcessedSignal()), {FilteredSignal()}, ...
                'subject', 1, 'session', "A");

            % Provenance of the final result references triple_values.
            prov = FilteredSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(prov.function_name), 'triple_values');
            testCase.verifyEqual(numel(prov.inputs), 1);

            % The single input references the upstream ProcessedSignal record.
            input_info = prov.inputs{1};
            testCase.verifyTrue(isstruct(input_info));
            testCase.verifyEqual(char(input_info.variable_type), 'ProcessedSignal');
        end

        function test_provenance_two_variable_inputs(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            ProcessedSignal().save([10 20 30], 'subject', 1, 'session', 'A');

            scidb.for_each(@sum_inputs, ...
                struct('a', RawSignal(), 'b', ProcessedSignal()), ...
                {FilteredSignal()}, ...
                'subject', 1, 'session', "A");

            prov = FilteredSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(prov.function_name), 'sum_inputs');
            testCase.verifyEqual(numel(prov.inputs), 2);
            % No constants -> struct with zero fields.
            testCase.verifyEqual(numel(fieldnames(prov.constants)), 0);
        end

        function test_different_functions_different_hashes(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");
            scidb.for_each(@triple_values, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'subject', 1, 'session', "A");

            prov1 = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            prov2 = FilteredSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyNotEqual(prov1.function_hash, prov2.function_hash);
        end

        function test_idempotent_rerun_same_record_id(testCase)
            % Content-addressing: re-running an identical pipeline reproduces
            % the same output record_id (replaces the old lineage_hash
            % determinism check).
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");
            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");
            r2 = ProcessedSignal().load('subject', 1, 'session', 'A');

            testCase.verifyEqual(r1.record_id, r2.record_id);
        end

        function test_provenance_inputs_have_record_id(testCase)
            % Variable inputs carry the source record_id, allowing exact
            % tracing back to the specific saved variable consumed.
            record_id = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', 1, 'session', "A");

            prov = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(numel(prov.inputs), 1);

            input_info = prov.inputs{1};
            testCase.verifyTrue(isstruct(input_info));
            testCase.verifyTrue(isfield(input_info, 'record_id'));
            testCase.verifyEqual(string(input_info.record_id), string(record_id));
        end
    end
end
