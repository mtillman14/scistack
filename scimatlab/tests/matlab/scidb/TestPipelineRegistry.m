classdef TestPipelineRegistry < matlab.unittest.TestCase
%TESTPIPELINEREGISTRY  Pipeline registry MATLAB parity (stage 4).
%
%   scidb.Pipeline(name) activates a Python-side pipeline; scidb.for_each
%   calls then REGISTER as deferred steps (fn handles + raw args stay
%   MATLAB-side, keyed by the Python step index). run_all/run_until ask
%   Python for the topo order and replay each MATLAB step through the
%   normal two-pass scidb.for_each; plan() is the non-executing dry run.
%   Bindings adapt used pipelines without touching their source.

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
            scidb.internal.pipeline_registry('clear');
            global PIPELINE_TEST_CALLS_DOUBLE PIPELINE_TEST_CALLS_MEAN ...
                   PIPELINE_TEST_FACTORS %#ok<GVMIS>
            PIPELINE_TEST_CALLS_DOUBLE = 0;
            PIPELINE_TEST_CALLS_MEAN = 0;
            PIPELINE_TEST_FACTORS = [];
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            scidb.internal.pipeline_registry('clear');
            try
                scidb.get_database().close();
            catch
            end
            close all force;
            if isfolder(testCase.test_dir)
                rmdir(testCase.test_dir, 's');
            end
        end
    end

    methods (Access = private)
        function seed(~)
            RawSignal().save([1 2 3], 'subject', "S01", 'session', "1");
            RawSignal().save([4 5 6], 'subject', "S02", 'session', "1");
        end
    end

    methods (Test)
        function testRegistrationDefersExecution(testCase)
            testCase.seed();
            global PIPELINE_TEST_CALLS_DOUBLE %#ok<GVMIS>

            pipe = scidb.Pipeline("gait");
            out = scidb.for_each(@pipeline_counting_double, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");

            testCase.verifyTrue(isstruct(out) && out.deferred, ...
                'deferred registration must return the step struct');
            testCase.verifyEqual(out.fn_name, 'pipeline_counting_double');
            testCase.verifyEqual(PIPELINE_TEST_CALLS_DOUBLE, 0, ...
                'nothing may execute at registration');
            pipe.deactivate();
        end

        function testPipelineNoneForcesEager(testCase)
            testCase.seed();
            global PIPELINE_TEST_CALLS_DOUBLE %#ok<GVMIS>

            pipe = scidb.Pipeline("gait");
            scidb.for_each(@pipeline_counting_double, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'pipeline', 'none', ...
                'subject', ["S01" "S02"], 'session', "1");

            testCase.verifyEqual(PIPELINE_TEST_CALLS_DOUBLE, 2, ...
                'pipeline=none must run eagerly despite the active pipeline');
            pipe.deactivate();
        end

        function testRunUntilExecutesAncestryInOrder(testCase)
            testCase.seed();
            global PIPELINE_TEST_CALLS_DOUBLE PIPELINE_TEST_CALLS_MEAN %#ok<GVMIS>

            pipe = scidb.Pipeline("gait");
            % Consumer registered BEFORE its producer: topo must reorder.
            scidb.for_each(@pipeline_counting_mean, ...
                struct('x', FilteredSignal()), {ProcessedSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");
            scidb.for_each(@pipeline_counting_double, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");

            pipe.run_until("pipeline_counting_mean");

            testCase.verifyEqual(PIPELINE_TEST_CALLS_DOUBLE, 2);
            testCase.verifyEqual(PIPELINE_TEST_CALLS_MEAN, 2);
            rec = ProcessedSignal().load('subject', "S01", 'session', "1");
            testCase.verifyEqual(rec.data, 4, 'AbsTol', 1e-9); % mean([2 4 6])
        end

        function testSecondRunSkipsCurrentSteps(testCase)
            testCase.seed();
            global PIPELINE_TEST_CALLS_DOUBLE %#ok<GVMIS>

            pipe = scidb.Pipeline("gait");
            scidb.for_each(@pipeline_counting_double, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");
            pipe.run_all();
            testCase.verifyEqual(PIPELINE_TEST_CALLS_DOUBLE, 2);

            % Running deactivates but does NOT consume the graph: a second
            % run replays the same steps, all current -> skipped.
            pipe.run_all();

            testCase.verifyEqual(PIPELINE_TEST_CALLS_DOUBLE, 2, ...
                'second run must skip already-computed combos');
        end

        function testPlanReportsWithoutExecuting(testCase)
            testCase.seed();
            global PIPELINE_TEST_CALLS_DOUBLE %#ok<GVMIS>

            pipe = scidb.Pipeline("gait");
            scidb.for_each(@pipeline_counting_double, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");

            entries = pipe.plan();

            testCase.verifyEqual(numel(entries), 1);
            testCase.verifyEqual(char(string(entries(1).step)), ...
                'pipeline_counting_double');
            testCase.verifyFalse(logical(entries(1).endpoint));
            testCase.verifyEqual(PIPELINE_TEST_CALLS_DOUBLE, 0, ...
                'plan must not execute anything');
        end

        function testCompositionResolvesAcrossPipelines(testCase)
            testCase.seed();
            global PIPELINE_TEST_CALLS_DOUBLE PIPELINE_TEST_CALLS_MEAN %#ok<GVMIS>

            loading = scidb.Pipeline("loading");
            scidb.for_each(@pipeline_counting_double, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");
            loading.deactivate();

            analysis = scidb.Pipeline("analysis", 'uses', {loading});
            scidb.for_each(@pipeline_counting_mean, ...
                struct('x', FilteredSignal()), {ProcessedSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");

            analysis.run_until("pipeline_counting_mean");

            testCase.verifyEqual(PIPELINE_TEST_CALLS_DOUBLE, 2, ...
                'producer inside the used pipeline must run first');
            testCase.verifyEqual(PIPELINE_TEST_CALLS_MEAN, 2);
        end

        function testBindingParamsOverrideConstants(testCase)
            testCase.seed();
            global PIPELINE_TEST_FACTORS %#ok<GVMIS>

            scaling = scidb.Pipeline("scaling");
            scidb.for_each(@pipeline_scale_by, ...
                struct('x', RawSignal(), 'factor', 2), {FilteredSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");
            scaling.deactivate();

            binding = scaling.bind('params', struct('factor', 3));
            analysis = scidb.Pipeline("analysis", 'uses', {binding});
            analysis.run_until("pipeline_scale_by");

            testCase.verifyEqual(PIPELINE_TEST_FACTORS, [3 3], ...
                'the bound factor (3) must reach the function, not the source''s 2');
        end

        function testEndpointPlotThroughPipeline(testCase)
            testCase.seed();
            plots_dir = fullfile(testCase.test_dir, 'plots');
            mkdir(plots_dir);
            template = fullfile(plots_dir, '{subject}_{session}.png');

            pipe = scidb.Pipeline("report");
            scidb.for_each(@pipeline_counting_double, ...
                struct('x', RawSignal()), {FilteredSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");
            scidb.for_each(@plot_signal_line, ...
                struct('signal', FilteredSignal(), ...
                       'filename', scifor.PathOutput(template)), ...
                {ProcessedSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");

            eps = pipe.endpoints();
            testCase.verifyEqual(numel(eps), 1);
            testCase.verifyEqual(char(string(eps(1).step)), 'plot_signal_line');

            pipe.run_endpoints('finalized', true);

            testCase.verifyTrue(isfile(fullfile(plots_dir, 'S01_1.png')));
            rec = ProcessedSignal().load('subject', "S01", 'session', "1");
            testCase.verifyTrue(contains(char(string(rec.data)), '.png'), ...
                'finalized endpoint must store the figure path record');
        end
    end
end
