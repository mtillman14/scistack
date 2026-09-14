classdef TestForEachSchemaKeys < matlab.unittest.TestCase
%TESTFOREACHSCHEMAKEYS  Integration tests for scidb.for_each's schema_keys=/
%   schema_filter= parameters (DB-backed, via the Python bridge).
%
%   Tests that:
%     - schema_keys=[...] auto-resolves each key's values from the database,
%       same as bare key=[] kwargs
%     - schema_keys naming fewer than all schema keys aggregates over the
%       rest (one call per iterated-key value, not per record)
%     - schema_filter on a key NOT in schema_keys constrains which records
%       load (regression test for a latent bug where this was silently
%       ignored — see docs/claude/scidb-for-each-internals.md)
%     - schema_keys conflicts with explicit metadata name-value pairs

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
                ["subject", "session", "trial"]);
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

        function test_schema_keys_resolves_all_values(testCase)
        %   schema_keys=[...] auto-resolves each key's DB values.
            for subj = [1 2]
                for trial = [1 2]
                    RawSignal().save(1.0, 'subject', subj, 'session', 'A', 'trial', trial);
                end
            end

            result = scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                schema_keys=["subject", "session", "trial"]);

            testCase.verifyEqual(height(result), 4);
        end

        function test_schema_keys_subset_is_aggregation(testCase)
        %   schema_keys naming fewer than all schema keys aggregates over
        %   the rest.
            for subj = [1 2]
                for sess = ["A" "B"]
                    for trial = [1 2]
                        RawSignal().save(1.0, 'subject', subj, 'session', sess, 'trial', trial);
                    end
                end
            end

            result = scidb.for_each(@count_table_rows, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                as_table=true, schema_keys=["subject"]);

            result = sortrows(result, 'subject');
            % Each subject aggregates 2 sessions x 2 trials = 4 rows.
            testCase.verifyEqual(result.ProcessedSignal, [4; 4]);
        end

        function test_schema_filter_on_non_iterated_key_constrains_loaded_data(testCase)
        %   Regression test: schema_filter on a key NOT in schema_keys used
        %   to be silently ignored. Now it's ANDed into where= via
        %   SchemaKeyInFilter, so it actually restricts which records load.
            for subj = [1 2]
                for sess = ["A" "B"]
                    for trial = [1 2]
                        RawSignal().save(1.0, 'subject', subj, 'session', sess, 'trial', trial);
                    end
                end
            end

            result = scidb.for_each(@count_table_rows, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                as_table=true, schema_keys=["subject"], ...
                schema_filter=struct('session', "A"));

            result = sortrows(result, 'subject');
            % Constrained to session="A": 1 session x 2 trials = 2 rows.
            testCase.verifyEqual(result.ProcessedSignal, [2; 2]);
        end

        function test_schema_keys_conflicts_with_metadata(testCase)
        %   Error when combining schema_keys with explicit metadata
        %   name-value pairs.
            RawSignal().save(1.0, 'subject', 1, 'session', 'A', 'trial', 1);

            testCase.verifyError(@() scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                schema_keys=["subject"], subject=[1]), ...
                ?MException);
        end

    end

end
