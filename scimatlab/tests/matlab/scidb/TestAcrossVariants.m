classdef TestAcrossVariants < matlab.unittest.TestCase
%TESTACROSSVARIANTS  scidb.AcrossVariants pooling opt-in through the bridge.
%
%   Aggregation auto-splits per branch_param group by default (D1);
%   AcrossVariants pools all groups into ONE call with each namespaced
%   branch_param attached as a table column (multiverse analysis). All
%   pooling logic lives in Python prepare; MATLAB is only the builder.

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

    methods (Access = private)
        function makeTwoVariants(~)
            RawSignal().save(5, 'subject', 1, 'session', "A");
            for offset = [10 20]
                scidb.for_each(@add_offset, ...
                    struct('x', RawSignal(), 'offset', offset), ...
                    {ProcessedSignal()}, ...
                    'subject', 1, 'session', "A");
            end
        end
    end

    methods (Test)
        function testConstructorRejectsMerge(testCase)
            testCase.verifyError( ...
                @() scidb.AcrossVariants(scidb.Merge(RawSignal(), ProcessedSignal(), name="merged")), ...
                'scidb:AcrossVariants');
        end

        function testIdempotentNesting(testCase)
            av = scidb.AcrossVariants(scidb.AcrossVariants(RawSignal()));
            testCase.verifyClass(av.var_type, 'RawSignal');
        end

        function testDefaultAggregationSplitsPerGroup(testCase)
            % Baseline (D1): unpinned aggregation runs once per variant group.
            testCase.makeTwoVariants();
            result = scidb.for_each(@sum_all, ...
                struct('vals', ProcessedSignal()), ...
                {DummyOut()}, ...
                'subject', 1);   % session not iterated -> aggregation
            testCase.verifyEqual(height(result), 2, ...
                'one call per branch_param group');
        end

        function testAcrossVariantsPoolsWithBpColumns(testCase)
            testCase.makeTwoVariants();
            result = scidb.for_each(@count_specs, ...
                struct('df', scidb.AcrossVariants(ProcessedSignal())), ...
                {DummyOut()}, ...
                'subject', 1);
            testCase.verifyEqual(height(result), 1, ...
                'AcrossVariants pools into a single call');
            % count_specs returns the number of DISTINCT add_offset.offset
            % values it saw in the attached branch_param column.
            testCase.verifyEqual(result.DummyOut(1), 2);
        end
    end
end
