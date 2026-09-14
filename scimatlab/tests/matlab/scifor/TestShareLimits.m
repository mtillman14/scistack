classdef TestShareLimits < matlab.unittest.TestCase
%TESTSHARELIMITS  MATLAB port of scifor's share_limits prepass.
%
%   share_limits = struct('signal', ["subject"]) makes every combo within a
%   subject receive the SAME [min max] over that subject's data across the
%   other iterated keys, appended as a trailing positional argument. The fn
%   declares the trailing `<input>_limits` parameter (or varargin) to
%   receive it; a fn without capacity is called unchanged.

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function setSchema(~)
            scifor.set_schema(["subject", "trial"]);
        end
    end

    methods (TestMethodTeardown)
        function clearSchema(~)
            scifor.set_schema(string.empty);
        end
    end

    methods (Access = private)
        function t = seedTable(~)
            % subject 1: small amplitudes; subject 2: large.
            t = table( ...
                ["1"; "1"; "1"; "2"; "2"; "2"], ...
                ["1"; "2"; "3"; "1"; "2"; "3"], ...
                {[0 1 2]; [1 2 3]; [0.5 1.5 2.5]; ...
                 [0 50 100]; [10 60 110]; [5 55 105]}, ...
                'VariableNames', {'subject', 'trial', 'signal'});
        end
    end

    methods (Test)
        function testPerGroupLimitsIdenticalWithinSubject(testCase)
            t = testCase.seedTable();

            result = scifor.for_each(@grab_limits, ...
                struct('signal', t), ...
                'share_limits', struct('signal', "subject"), ...
                'output_names', {'limits'}, ...
                'subject', ["1" "2"], 'trial', ["1" "2" "3"]);

            testCase.verifyEqual(height(result), 6);
            % All trials within a subject share one limits pair; subjects differ.
            captured = containers.Map('KeyType', 'char', 'ValueType', 'any');
            for subj = ["1" "2"]
                rows = result(result.subject == subj, :);
                lims = rows.limits;
                testCase.verifyTrue(iscell(lims) || isnumeric(lims));
                if iscell(lims)
                    first = lims{1};
                    for r = 2:numel(lims)
                        testCase.verifyEqual(lims{r}, first, ...
                            'limits must be identical within a subject');
                    end
                else
                    first = lims(1, :);
                    for r = 2:size(lims, 1)
                        testCase.verifyEqual(lims(r, :), first, ...
                            'limits must be identical within a subject');
                    end
                end
                captured(char(subj)) = first;
            end
            testCase.verifyNotEqual(captured('1'), captured('2'));
            testCase.verifyEqual(captured('1'), [0 3]);
            testCase.verifyEqual(captured('2'), [0 110]);
        end

        function testFnWithoutCapacityRunsUnchanged(testCase)
            t = testCase.seedTable();
            result = scifor.for_each(@count_elems, ...
                struct('signal', t), ...
                'share_limits', struct('signal', "subject"), ...
                'output_names', {'n'}, ...
                'subject', ["1" "2"], 'trial', ["1" "2" "3"]);
            testCase.verifyEqual(height(result), 6, ...
                'fn without limit-arg capacity must run unchanged');
        end
    end
end


function out = grab_limits(signal, signal_limits) %#ok<INUSD>
%GRAB_LIMITS  Return the injected shared limits (declares trailing capacity).
    out = signal_limits;
end


function out = count_elems(signal)
%COUNT_ELEMS  Declares exactly one arg: limits must NOT be appended.
    out = numel(signal);
end
