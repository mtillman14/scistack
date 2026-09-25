classdef TestEachOf < matlab.unittest.TestCase
%TESTEACHOF  Integration tests for scifor.EachOf bridged through
%   scidb.for_each — added alongside the standalone scifor.for_each
%   coverage in scimatlab/tests/matlab/scifor/TestSciforEachOf.m (previously
%   EachOf was Python-only; see docs/claude/each-of-variant-expansion.md).
%   The motivating use case: a scifor.PathInput input spanning two on-disk
%   locations (assessment-day vs. training-day GAITRite folders) while the
%   shared pipeline function and its DB-backed for_each call stay
%   unchanged.

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
        function root = makeLocationTree(~, base, name, subject_session_content)
            % subject_session_content: Nx3 cell {subject, session, content}
            root = fullfile(base, name);
            for i = 1:size(subject_session_content, 1)
                subj = subject_session_content{i, 1};
                sess = subject_session_content{i, 2};
                content = subject_session_content{i, 3};
                d = fullfile(root, subj, sess);
                mkdir(d);
                fid = fopen(fullfile(d, 'data.txt'), 'w');
                fprintf(fid, '%s', content);
                fclose(fid);
            end
        end
    end

    methods (Test)
        function test_two_pathinputs_save_distinct_records_per_location(testCase)
            % Two locations, matching {subject}/{session} placeholder
            % names, disjoint combos -- mirrors assessment-day vs.
            % training-day GAITRite folders sharing one pipeline function.
            assessment_root = testCase.makeLocationTree(testCase.test_dir, 'assessment', ...
                {'1', 'A', 'assess-1A'; '2', 'A', 'assess-2A'});
            training_root = testCase.makeLocationTree(testCase.test_dir, 'training', ...
                {'1', 'T1', 'train-1T1'; '2', 'T1', 'train-2T1'});

            template = "{subject}/{session}/data.txt";
            pi_assessment = scifor.PathInput(template, 'root_folder', assessment_root, 'name', char(template));
            pi_training = scifor.PathInput(template, 'root_folder', training_root, 'name', char(template));

            fn = @(filepath) string(fileread(filepath));
            result = scidb.for_each(fn, ...
                struct('filepath', scifor.EachOf(pi_assessment, pi_training)), ...
                {ProcessedSignal()}, ...
                'subject', [], 'session', []);

            testCase.verifyEqual(height(result), 4);

            % Query by type-agnostic content rather than by subject/session
            % value directly -- discovery yields char values, and whether
            % they round-trip as char or get coerced to numeric depends on
            % what class() the caller's [] iterable had, which is
            % orthogonal to what this test is checking (that both
            % locations' combos were saved as distinct records, neither
            % overwriting the other).
            all_results = ProcessedSignal().load();
            testCase.verifyEqual(numel(all_results), 4);
            contents = sort(arrayfun(@(r) string(r.data), all_results));
            contents = contents(:);   % load() shape varies; compare as a column
            testCase.verifyEqual(contents, ...
                sort(["assess-1A"; "assess-2A"; "train-1T1"; "train-2T1"]));
        end
    end
end
