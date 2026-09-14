classdef TestSciforEachOf < matlab.unittest.TestCase
%TESTSCIFOREACHOF  Integration tests for scifor.EachOf's standalone expansion in
%   scifor.for_each — added when scifor.EachOf/scidb.for_each's EachOf
%   recursion were bridged to MATLAB for the first time (previously
%   Python-only; see docs/claude/each-of-variant-expansion.md). The
%   motivating use case is a scifor.PathInput input that needs to span two
%   on-disk locations (e.g. assessment-day vs. training-day GAITRite
%   folders) without changing the shared pipeline function.

    properties
        tmp_dir  string
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function createTempDir(testCase)
            % scifor's schema is global process state, and these tests depend
            % on it being EMPTY: with no declared schema, PathInput discovery
            % adopts the keys it finds ({subject}/{session}) and drives
            % iteration directly. Pin it rather than inherit whatever the
            % previously-run class left behind — several scifor test classes
            % set a schema and do not reset it, so an ambient schema makes
            % these tests depend on alphabetical class order.
            scifor.set_schema(string.empty(1, 0));
            testCase.tmp_dir = string(tempname);
            mkdir(testCase.tmp_dir);
        end
    end

    methods (TestMethodTeardown)
        function removeTempDir(testCase)
            scifor.set_schema(string.empty(1, 0));
            if isfolder(testCase.tmp_dir)
                rmdir(testCase.tmp_dir, 's');
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
        function test_two_pathinputs_concatenate_both_locations(testCase)
            % Two locations, matching {subject}/{session} placeholder
            % names, disjoint combos (mirrors assessment-day vs.
            % training-day GAITRite folders). Both must show up in the
            % concatenated result.
            assessment_root = testCase.makeLocationTree(testCase.tmp_dir, 'assessment', ...
                {'1', 'A', 'assess-1A'; '2', 'A', 'assess-2A'});
            training_root = testCase.makeLocationTree(testCase.tmp_dir, 'training', ...
                {'1', 'T1', 'train-1T1'; '2', 'T1', 'train-2T1'});

            template = "{subject}/{session}/data.txt";
            pi_assessment = scifor.PathInput(template, 'root_folder', assessment_root);
            pi_training = scifor.PathInput(template, 'root_folder', training_root);

            fn = @(filepath) string(fileread(filepath));
            result = scifor.for_each(fn, ...
                struct('filepath', scifor.EachOf(pi_assessment, pi_training)));

            testCase.verifyEqual(height(result), 4);
            contents = sort(result.output);
            testCase.verifyEqual(contents, sort(["assess-1A"; "assess-2A"; "train-1T1"; "train-2T1"]));
            testCase.verifyTrue(all(ismember(["subject" "session"], ...
                string(result.Properties.VariableNames))));
        end

        function test_single_alternative_matches_direct_pathinput(testCase)
            % EachOf with one alternative behaves like passing it directly.
            root = testCase.makeLocationTree(testCase.tmp_dir, 'solo', ...
                {'1', 'A', 'solo-1A'});
            pi = scifor.PathInput("{subject}/{session}/data.txt", 'root_folder', root);

            fn = @(filepath) string(fileread(filepath));
            direct = scifor.for_each(fn, struct('filepath', pi));
            wrapped = scifor.for_each(fn, struct('filepath', scifor.EachOf(pi)));

            testCase.verifyEqual(height(direct), height(wrapped));
            testCase.verifyEqual(sort(direct.output), sort(wrapped.output));
        end

        function test_mismatched_placeholder_keys_errors_clearly(testCase)
            % Two PathInput alternatives whose templates discover different
            % placeholder key sets (session vs. day) produce result tables
            % with different columns. MATLAB's table vertcat has no
            % pandas-style NaN-union leniency, so this must be a clear,
            % named error rather than a raw vertcat failure.
            root_a = testCase.makeLocationTree(testCase.tmp_dir, 'a', ...
                {'1', 'A', 'a-1A'});
            root_b_base = fullfile(testCase.tmp_dir, 'b');
            mkdir(fullfile(root_b_base, '1', 'D1'));
            fid = fopen(fullfile(root_b_base, '1', 'D1', 'other.txt'), 'w');
            fprintf(fid, 'b-1D1');
            fclose(fid);

            pi_a = scifor.PathInput("{subject}/{session}/data.txt", 'root_folder', root_a);
            pi_b = scifor.PathInput("{subject}/{day}/other.txt", 'root_folder', root_b_base);

            fn = @(filepath) string(fileread(filepath));
            testCase.verifyError(@() scifor.for_each(fn, ...
                struct('filepath', scifor.EachOf(pi_a, pi_b))), ...
                'scifor:for_each:EachOfColumnMismatch');
        end

    end
end
