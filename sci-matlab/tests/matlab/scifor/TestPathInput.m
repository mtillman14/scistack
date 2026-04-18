classdef TestPathInput < matlab.unittest.TestCase
%TESTPATHINPUT  Integration tests for scifor.PathInput.

    properties
        tmp_dir  string  % Temp directory for regex tests
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function createTempDir(testCase)
            testCase.tmp_dir = string(tempname);
            mkdir(testCase.tmp_dir);
            % Create subdirectory with test files for regex tests
            sub_dir = fullfile(testCase.tmp_dir, '1');
            mkdir(sub_dir);
            % Zero-padded files
            fclose(fopen(fullfile(sub_dir, '6mwt-001.xlsx'), 'w'));
            fclose(fopen(fullfile(sub_dir, '6mwt-010.xlsx'), 'w'));
            fclose(fopen(fullfile(sub_dir, '6mwt-100.xlsx'), 'w'));
            % Duplicate-match files
            dup_dir = fullfile(testCase.tmp_dir, 'dup');
            mkdir(dup_dir);
            fclose(fopen(fullfile(dup_dir, 'data_v1.csv'), 'w'));
            fclose(fopen(fullfile(dup_dir, 'data_v2.csv'), 'w'));
            % Single exact file
            exact_dir = fullfile(testCase.tmp_dir, 'exact');
            mkdir(exact_dir);
            fclose(fopen(fullfile(exact_dir, 'report.txt'), 'w'));
        end
    end

    methods (TestMethodTeardown)
        function removeTempDir(testCase)
            if isfolder(testCase.tmp_dir)
                rmdir(testCase.tmp_dir, 's');
            end
        end
    end

    methods (Test)
        function test_basic_resolution(testCase)
            pi = scifor.PathInput("{subject}/data.mat", ...
                'root_folder', '/data');
            path = pi.load('subject', 1);
            expected = string(fullfile('/data', '1', 'data.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_multiple_placeholders(testCase)
            pi = scifor.PathInput("{subject}/session_{session}/trial.mat", ...
                'root_folder', '/experiment');
            path = pi.load('subject', 1, 'session', 'A');
            expected = string(fullfile('/experiment', '1', 'session_A', 'trial.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_numeric_value_in_template(testCase)
            pi = scifor.PathInput("sub{subject}_trial{trial}.mat", ...
                'root_folder', '/data');
            path = pi.load('subject', 3, 'trial', 7);
            testCase.verifyTrue(contains(path, "sub3"));
            testCase.verifyTrue(contains(path, "trial7"));
        end

        function test_string_value_in_template(testCase)
            pi = scifor.PathInput("{group}/results.csv", ...
                'root_folder', '/output');
            path = pi.load('group', 'control');
            expected = string(fullfile('/output', 'control', 'results.csv'));
            testCase.verifyEqual(path, expected);
        end

        function test_no_root_folder_uses_pwd(testCase)
            pi = scifor.PathInput("{x}/data.mat");
            path = pi.load('x', 1);
            expected = string(fullfile(pwd, '1', 'data.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_returns_string(testCase)
            pi = scifor.PathInput("{x}.mat", 'root_folder', '/data');
            path = pi.load('x', 1);
            testCase.verifyClass(path, 'string');
        end

        function test_unused_metadata_ignored(testCase)
            % Extra metadata keys not in template should not cause errors
            pi = scifor.PathInput("{subject}/data.mat", ...
                'root_folder', '/data');
            path = pi.load('subject', 1, 'session', 'A');
            expected = string(fullfile('/data', '1', 'data.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_absolute_path_in_template(testCase)
            pi = scifor.PathInput("{subject}/data.mat", ...
                'root_folder', '/absolute/root');
            path = pi.load('subject', 5);
            % Verify the path contains the root folder and resolved template
            testCase.verifyTrue(contains(path, "absolute"));
            testCase.verifyTrue(contains(path, "root"));
            testCase.verifyTrue(contains(path, "5"));
            testCase.verifyTrue(contains(path, "data.mat"));
        end

        %% Absolute path template tests (no root_folder needed)

        function test_absolute_template_no_root_folder(testCase)
            pi = scifor.PathInput("/data/{subject}/trial_{trial}.mat");
            path = pi.load('subject', 1, 'trial', 2);
            testCase.verifyEqual(path, "/data/1/trial_2.mat");
        end

        function test_absolute_template_ignores_root_folder(testCase)
            % When template resolves to absolute path, root_folder is ignored
            pi = scifor.PathInput("/data/{subject}/file.mat", ...
                'root_folder', '/other/root');
            path = pi.load('subject', 5);
            testCase.verifyEqual(path, "/data/5/file.mat");
        end

        function test_absolute_template_string_placeholder(testCase)
            pi = scifor.PathInput("/mnt/share/{group}/{subject}.csv");
            path = pi.load('group', 'control', 'subject', 'p01');
            testCase.verifyEqual(path, "/mnt/share/control/p01.csv");
        end

        function test_absolute_template_no_placeholders(testCase)
            pi = scifor.PathInput("/fixed/path/data.mat");
            path = pi.load();
            testCase.verifyEqual(path, "/fixed/path/data.mat");
        end

        function test_absolute_template_returns_string(testCase)
            pi = scifor.PathInput("/data/{x}.mat");
            path = pi.load('x', 1);
            testCase.verifyClass(path, 'string');
        end

        function test_absolute_template_regex(testCase)
            % Create temp files under an absolute path and use regex
            sub_dir = fullfile(testCase.tmp_dir, 'abs_regex');
            mkdir(sub_dir);
            fclose(fopen(fullfile(sub_dir, 'result_final.csv'), 'w'));

            template = testCase.tmp_dir + "/abs_regex/result_final\.csv";
            pi = scifor.PathInput(template, 'regex', true);
            path = pi.load();
            expected = string(fullfile(sub_dir, 'result_final.csv'));
            testCase.verifyEqual(path, expected);
        end

        %% Regex tests

        function test_regex_basic(testCase)
            % An exact filename used as the regex pattern should match
            pi = scifor.PathInput("exact/report\.txt", ...
                'root_folder', testCase.tmp_dir, 'regex', true);
            path = pi.load();
            expected = string(fullfile(testCase.tmp_dir, 'exact', 'report.txt'));
            testCase.verifyEqual(path, expected);
        end

        function test_regex_zero_padding(testCase)
            % Pattern with regex quantifier matches zero-padded filename
            pi = scifor.PathInput("{subject}/6mwt-0{0,2}1\.xlsx", ...
                'root_folder', testCase.tmp_dir, 'regex', true);
            path = pi.load('subject', 1);
            expected = string(fullfile(testCase.tmp_dir, '1', '6mwt-001.xlsx'));
            testCase.verifyEqual(path, expected);
        end

        function test_regex_no_match_errors(testCase)
            % Pattern that matches nothing should error
            pi = scifor.PathInput("{subject}/nonexistent.*\.xyz", ...
                'root_folder', testCase.tmp_dir, 'regex', true);
            testCase.verifyError(@() pi.load('subject', 1), ...
                'scifor:PathInput:NoMatch');
        end

        function test_regex_multiple_match_errors(testCase)
            % Pattern that matches multiple files should error
            pi = scifor.PathInput("dup/data_v\d\.csv", ...
                'root_folder', testCase.tmp_dir, 'regex', true);
            testCase.verifyError(@() pi.load(), ...
                'scifor:PathInput:MultipleMatches');
        end

        %% placeholder_keys tests

        function test_placeholder_keys_simple(testCase)
            pi = scifor.PathInput("{subject}/data.mat");
            keys = pi.placeholder_keys();
            testCase.verifyEqual(keys, {'subject'});
        end

        function test_placeholder_keys_multiple(testCase)
            pi = scifor.PathInput("{subject}/{session}/data.mat");
            keys = pi.placeholder_keys();
            testCase.verifyEqual(keys, {'subject', 'session'});
        end

        function test_placeholder_keys_mixed_segment(testCase)
            pi = scifor.PathInput("{subject}_XSENS_{session}_{speed}-001.xlsx");
            keys = pi.placeholder_keys();
            testCase.verifyEqual(keys, {'subject', 'session', 'speed'});
        end

        function test_placeholder_keys_none(testCase)
            pi = scifor.PathInput("data/raw/file.mat");
            keys = pi.placeholder_keys();
            testCase.verifyEmpty(keys);
        end

        function test_placeholder_keys_duplicates(testCase)
            pi = scifor.PathInput("{subject}/{subject}_data.mat");
            keys = pi.placeholder_keys();
            testCase.verifyEqual(keys, {'subject'});
        end

        %% discover tests

        function test_discover_basic(testCase)
            % Create a multi-level directory tree
            disc_dir = fullfile(testCase.tmp_dir, 'disc');
            for subj = ["A", "B"]
                for sess = ["s1", "s2"]
                    d = fullfile(disc_dir, subj, sess);
                    mkdir(d);
                    fclose(fopen(fullfile(d, char(subj + "_" + sess + ".csv")), 'w'));
                end
            end

            pi = scifor.PathInput("{subject}/{session}/{subject}_{session}.csv", ...
                'root_folder', disc_dir);
            combos = pi.discover();
            testCase.verifyLength(combos, 4);

            % Verify specific combos exist
            found_A_s1 = false;
            found_B_s2 = false;
            for c = 1:numel(combos)
                if strcmp(combos{c}.subject, 'A') && strcmp(combos{c}.session, 's1')
                    found_A_s1 = true;
                end
                if strcmp(combos{c}.subject, 'B') && strcmp(combos{c}.session, 's2')
                    found_B_s2 = true;
                end
            end
            testCase.verifyTrue(found_A_s1);
            testCase.verifyTrue(found_B_s2);
        end

        function test_discover_empty_filesystem(testCase)
            empty_dir = fullfile(testCase.tmp_dir, 'empty_disc');
            mkdir(empty_dir);
            pi = scifor.PathInput("{x}/data/{file}.csv", ...
                'root_folder', empty_dir);
            combos = pi.discover();
            testCase.verifyEmpty(combos);
        end

        function test_discover_literal_segment(testCase)
            % Literal segment filters out non-matching dirs
            disc_dir = fullfile(testCase.tmp_dir, 'lit');
            mkdir(fullfile(disc_dir, 'XSENS'));
            fclose(fopen(fullfile(disc_dir, 'XSENS', 'data.csv'), 'w'));
            mkdir(fullfile(disc_dir, 'OTHER'));
            fclose(fopen(fullfile(disc_dir, 'OTHER', 'data.csv'), 'w'));

            pi = scifor.PathInput("XSENS/{file}.csv", ...
                'root_folder', disc_dir);
            combos = pi.discover();
            testCase.verifyLength(combos, 1);
            testCase.verifyEqual(combos{1}.file, 'data');
        end

        function test_discover_consistency_check(testCase)
            % {x} in dir must match {x} in filename
            disc_dir = fullfile(testCase.tmp_dir, 'consist');
            mkdir(fullfile(disc_dir, 'A'));
            fclose(fopen(fullfile(disc_dir, 'A', 'A_data.csv'), 'w'));
            fclose(fopen(fullfile(disc_dir, 'A', 'B_data.csv'), 'w')); % inconsistent

            pi = scifor.PathInput("{x}/{x}_data.csv", ...
                'root_folder', disc_dir);
            combos = pi.discover();
            testCase.verifyLength(combos, 1);
            testCase.verifyEqual(combos{1}.x, 'A');
        end

        function test_discover_no_placeholders(testCase)
            % No placeholders — returns one combo if file exists
            disc_dir = fullfile(testCase.tmp_dir, 'noplace');
            mkdir(disc_dir);
            fclose(fopen(fullfile(disc_dir, 'data.mat'), 'w'));

            pi = scifor.PathInput("data.mat", 'root_folder', disc_dir);
            combos = pi.discover();
            testCase.verifyLength(combos, 1);
            testCase.verifyEmpty(fieldnames(combos{1}));
        end

        function test_discover_mixed_filename(testCase)
            % Template with literal+placeholder in filename
            disc_dir = fullfile(testCase.tmp_dir, 'mixed');
            mkdir(disc_dir);
            fclose(fopen(fullfile(disc_dir, 'report_2024_final.csv'), 'w'));
            fclose(fopen(fullfile(disc_dir, 'report_2023_draft.csv'), 'w'));
            fclose(fopen(fullfile(disc_dir, 'other.csv'), 'w'));

            pi = scifor.PathInput("report_{year}_{status}.csv", ...
                'root_folder', disc_dir);
            combos = pi.discover();
            testCase.verifyLength(combos, 2);
        end

        function test_discover_values_are_strings(testCase)
            disc_dir = fullfile(testCase.tmp_dir, 'strtypes');
            mkdir(fullfile(disc_dir, '1'));
            fclose(fopen(fullfile(disc_dir, '1', 'data.csv'), 'w'));

            pi = scifor.PathInput("{num}/data.csv", ...
                'root_folder', disc_dir);
            combos = pi.discover();
            testCase.verifyLength(combos, 1);
            testCase.verifyClass(combos{1}.num, 'char');
        end
    end
end
