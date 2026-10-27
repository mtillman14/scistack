classdef TestIterationTally < matlab.unittest.TestCase
%TESTITERATIONTALLY  scidb.for_each reports failed iterations to the run
%   tally (+scidb/+internal/iteration_tally.m), which the GUI's generated
%   script reads to fail a run whose iterations failed (scistack.log 2026-10-20:
%   completed=0, failed=1 was reported verdict=done).

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
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            scidb.internal.iteration_tally('reset');
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
        function test_a_failed_iteration_is_counted(testCase)
            scidb.for_each(@fail_on_subject_two, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', [1 2], 'session', "A");
            t = scidb.internal.iteration_tally('get');
            testCase.verifyEqual(t.completed, 1);
            testCase.verifyEqual(t.failed, 1);
            testCase.verifyTrue(contains(t.message, 'fail_on_subject_two: 1 of 2'));
            testCase.verifyTrue(contains(t.message, 'subject two has no data here'));
        end

        function test_a_clean_run_counts_no_failures(testCase)
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', [1 2], 'session', "A");
            t = scidb.internal.iteration_tally('get');
            testCase.verifyEqual(t.failed, 0);
            testCase.verifyEqual(t.completed, 2);
            testCase.verifyEmpty(t.message);
        end

        function test_reset_clears_the_tally(testCase)
            scidb.for_each(@fail_on_subject_two, ...
                struct('x', RawSignal()), {ProcessedSignal()}, ...
                'subject', [1 2], 'session', "A");
            scidb.internal.iteration_tally('reset');
            t = scidb.internal.iteration_tally('get');
            testCase.verifyEqual(t.failed, 0);
        end
    end
end
