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
            % Alias-spelling folders for the folder-name alias tests
            baseline_dir = fullfile(testCase.tmp_dir, 'alias', 'sub1', 'Baseline');
            mkdir(baseline_dir);
            fclose(fopen(fullfile(baseline_dir, 'data.mat'), 'w'));
            bl_dir = fullfile(testCase.tmp_dir, 'alias', 'sub2', 'BL');
            mkdir(bl_dir);
            fclose(fopen(fullfile(bl_dir, 'data.mat'), 'w'));
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
        function test_name_is_required(testCase)
            % The name is the PathInput's identity (2026-09-25).
            testCase.verifyError(@() scifor.PathInput("{subject}/data.mat"), ...
                'scifor:PathInput:NameRequired');
            testCase.verifyError(@() scifor.PathInput("{subject}/data.mat", 'name', "  "), ...
                'scifor:PathInput:NameRequired');
        end

        function test_name_reaches_python(testCase)
            pi = scifor.PathInput("{subject}/data.mat", 'name', "RawData");
            testCase.verifyEqual(string(pi.name), "RawData");
            testCase.verifyEqual(string(pi.py_obj.name), "RawData");
        end

        function test_basic_resolution(testCase)
            pi = scifor.PathInput("{subject}/data.mat", ...
                'root_folder', '/data', 'name', "{subject}/data.mat");
            path = pi.load('subject', 1);
            expected = string(fullfile('/data', '1', 'data.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_multiple_placeholders(testCase)
            pi = scifor.PathInput("{subject}/session_{session}/trial.mat", ...
                'root_folder', '/experiment', 'name', "{subject}/session_{session}/trial.mat");
            path = pi.load('subject', 1, 'session', 'A');
            expected = string(fullfile('/experiment', '1', 'session_A', 'trial.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_numeric_value_in_template(testCase)
            pi = scifor.PathInput("sub{subject}_trial{trial}.mat", ...
                'root_folder', '/data', 'name', "sub{subject}_trial{trial}.mat");
            path = pi.load('subject', 3, 'trial', 7);
            testCase.verifyTrue(contains(path, "sub3"));
            testCase.verifyTrue(contains(path, "trial7"));
        end

        function test_string_value_in_template(testCase)
            pi = scifor.PathInput("{group}/results.csv", ...
                'root_folder', '/output', 'name', "{group}/results.csv");
            path = pi.load('group', 'control');
            expected = string(fullfile('/output', 'control', 'results.csv'));
            testCase.verifyEqual(path, expected);
        end

        function test_no_root_folder_uses_project_root(testCase)
            % When no root_folder is supplied, PathInput.load() resolves
            % the template relative to scifor's project-root finder
            % (nearest pyproject.toml or scistack.toml ancestor; falls
            % back to cwd when neither is found). Compute the expected
            % path through the bridge so the test is robust to
            % OS-specific symlink resolution (e.g. macOS /var/folders
            % vs /private/var/folders) and to where the test is launched
            % from.
            root_str = char(py.scimatlab.bridge.pathinput_project_root());
            expected_py = py.pathlib.Path(root_str).joinpath('1', 'data.mat').resolve();
            expected = string(char(py.str(expected_py)));

            pi = scifor.PathInput("{x}/data.mat", 'name', "{x}/data.mat");
            path = pi.load('x', 1);
            testCase.verifyEqual(path, expected);
        end

        function test_returns_string(testCase)
            pi = scifor.PathInput("{x}.mat", 'root_folder', '/data', 'name', "{x}.mat");
            path = pi.load('x', 1);
            testCase.verifyClass(path, 'string');
        end

        function test_unused_metadata_ignored(testCase)
            % Extra metadata keys not in template should not cause errors
            pi = scifor.PathInput("{subject}/data.mat", ...
                'root_folder', '/data', 'name', "{subject}/data.mat");
            path = pi.load('subject', 1, 'session', 'A');
            expected = string(fullfile('/data', '1', 'data.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_absolute_path_in_template(testCase)
            pi = scifor.PathInput("{subject}/data.mat", ...
                'root_folder', '/absolute/root', 'name', "{subject}/data.mat");
            path = pi.load('subject', 5);
            % Verify the path contains the root folder and resolved template
            testCase.verifyTrue(contains(path, "absolute"));
            testCase.verifyTrue(contains(path, "root"));
            testCase.verifyTrue(contains(path, "5"));
            testCase.verifyTrue(contains(path, "data.mat"));
        end

        %% Absolute path template tests (no root_folder needed)

        function test_absolute_template_no_root_folder(testCase)
            pi = scifor.PathInput("/data/{subject}/trial_{trial}.mat", 'name', "/data/{subject}/trial_{trial}.mat");
            path = pi.load('subject', 1, 'trial', 2);
            testCase.verifyEqual(path, "/data/1/trial_2.mat");
        end

        function test_absolute_template_ignores_root_folder(testCase)
            % When template resolves to absolute path, root_folder is ignored
            pi = scifor.PathInput("/data/{subject}/file.mat", ...
                'root_folder', '/other/root', 'name', "/data/{subject}/file.mat");
            path = pi.load('subject', 5);
            testCase.verifyEqual(path, "/data/5/file.mat");
        end

        function test_absolute_template_string_placeholder(testCase)
            pi = scifor.PathInput("/mnt/share/{group}/{subject}.csv", 'name', "/mnt/share/{group}/{subject}.csv");
            path = pi.load('group', 'control', 'subject', 'p01');
            testCase.verifyEqual(path, "/mnt/share/control/p01.csv");
        end

        function test_absolute_template_no_placeholders(testCase)
            pi = scifor.PathInput("/fixed/path/data.mat", 'name', "/fixed/path/data.mat");
            path = pi.load();
            testCase.verifyEqual(path, "/fixed/path/data.mat");
        end

        function test_absolute_template_returns_string(testCase)
            pi = scifor.PathInput("/data/{x}.mat", 'name', "/data/{x}.mat");
            path = pi.load('x', 1);
            testCase.verifyClass(path, 'string');
        end

        function test_absolute_template_regex(testCase)
            % Create temp files under an absolute path and use regex
            sub_dir = fullfile(testCase.tmp_dir, 'abs_regex');
            mkdir(sub_dir);
            fclose(fopen(fullfile(sub_dir, 'result_final.csv'), 'w'));

            template = testCase.tmp_dir + "/abs_regex/result_final\.csv";
            pi = scifor.PathInput(template, 'regex', true, 'name', char(template));
            path = pi.load();
            expected = string(fullfile(sub_dir, 'result_final.csv'));
            testCase.verifyEqual(path, expected);
        end

        %% Regex tests

        function test_regex_basic(testCase)
            % An exact filename used as the regex pattern should match
            pi = scifor.PathInput("exact/report\.txt", ...
                'root_folder', testCase.tmp_dir, 'regex', true, 'name', "exact/report\.txt");
            path = pi.load();
            expected = string(fullfile(testCase.tmp_dir, 'exact', 'report.txt'));
            testCase.verifyEqual(path, expected);
        end

        function test_regex_zero_padding(testCase)
            % Pattern with regex quantifier matches zero-padded filename
            pi = scifor.PathInput("{subject}/6mwt-0{0,2}1\.xlsx", ...
                'root_folder', testCase.tmp_dir, 'regex', true, 'name', "{subject}/6mwt-0{0,2}1\.xlsx");
            path = pi.load('subject', 1);
            expected = string(fullfile(testCase.tmp_dir, '1', '6mwt-001.xlsx'));
            testCase.verifyEqual(path, expected);
        end

        function test_regex_no_match_errors(testCase)
            % Pattern that matches nothing should error
            pi = scifor.PathInput("{subject}/nonexistent.*\.xyz", ...
                'root_folder', testCase.tmp_dir, 'regex', true, 'name', "{subject}/nonexistent.*\.xyz");
            testCase.verifyError(@() pi.load('subject', 1), ...
                'scifor:PathInput:NoMatch');
        end

        function test_regex_multiple_match_errors(testCase)
            % Pattern that matches multiple files should error
            pi = scifor.PathInput("dup/data_v\d\.csv", ...
                'root_folder', testCase.tmp_dir, 'regex', true, 'name', "dup/data_v\d\.csv");
            testCase.verifyError(@() pi.load(), ...
                'scifor:PathInput:MultipleMatches');
        end

        %% Zero-padded numeric fallback (no regex syntax needed)

        function test_padded_fallback_numeric_double(testCase)
            % A plain MATLAB double finds the zero-padded file natively.
            pi = scifor.PathInput("{subject}/6mwt-{trial}.xlsx", ...
                'root_folder', testCase.tmp_dir, 'name', "{subject}/6mwt-{trial}.xlsx");
            path = pi.load('subject', 1, 'trial', 1);
            expected = string(fullfile(testCase.tmp_dir, '1', '6mwt-001.xlsx'));
            testCase.verifyEqual(path, expected);
        end

        function test_padded_fallback_multi_digit(testCase)
            pi = scifor.PathInput("{subject}/6mwt-{trial}.xlsx", ...
                'root_folder', testCase.tmp_dir, 'name', "{subject}/6mwt-{trial}.xlsx");
            path = pi.load('subject', 1, 'trial', 10);
            expected = string(fullfile(testCase.tmp_dir, '1', '6mwt-010.xlsx'));
            testCase.verifyEqual(path, expected);
        end

        function test_padded_fallback_ambiguous_errors(testCase)
            % Both 6mwt-1.xlsx and 6mwt-001.xlsx numerically equal 1.  Pass
            % "01" so the literally-resolved 6mwt-01.xlsx is missing and the
            % numeric fallback sees the two-way tie.
            amb_dir = fullfile(testCase.tmp_dir, '1');
            fclose(fopen(fullfile(amb_dir, '6mwt-1.xlsx'), 'w'));
            pi = scifor.PathInput("{subject}/6mwt-{trial}.xlsx", ...
                'root_folder', testCase.tmp_dir, 'name', "{subject}/6mwt-{trial}.xlsx");
            testCase.verifyError(@() pi.load('subject', 1, 'trial', "01"), ...
                'scifor:PathInput:MultipleMatches');
        end

        function test_padded_fallback_no_match_returns_literal(testCase)
            % Historical behavior preserved: missing files still return the
            % literally-resolved path (the caller's function surfaces it).
            pi = scifor.PathInput("{subject}/6mwt-{trial}.xlsx", ...
                'root_folder', testCase.tmp_dir, 'name', "{subject}/6mwt-{trial}.xlsx");
            path = pi.load('subject', 1, 'trial', 99);
            expected = string(fullfile(testCase.tmp_dir, '1', '6mwt-99.xlsx'));
            testCase.verifyEqual(path, expected);
        end

        %% Folder-name aliases (match-only)

        function test_alias_resolves_on_disk_spelling(testCase)
            % session="BL" finds the on-disk "Baseline" folder.
            pi = scifor.PathInput("alias/sub1/{session}/data.mat", ...
                'root_folder', testCase.tmp_dir, ...
                'aliases', struct('session', struct('BL', ["Baseline", "1. Baseline"])), 'name', "alias/sub1/{session}/data.mat");
            path = pi.load('session', 'BL');
            expected = string(fullfile(testCase.tmp_dir, 'alias', 'sub1', 'Baseline', 'data.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_alias_canonical_folder_is_literal_hit(testCase)
            % session="BL" finds a folder literally named "BL" too.
            pi = scifor.PathInput("alias/sub2/{session}/data.mat", ...
                'root_folder', testCase.tmp_dir, ...
                'aliases', struct('session', struct('BL', "Baseline")), 'name', "alias/sub2/{session}/data.mat");
            path = pi.load('session', 'BL');
            expected = string(fullfile(testCase.tmp_dir, 'alias', 'sub2', 'BL', 'data.mat'));
            testCase.verifyEqual(path, expected);
        end

        function test_alias_ambiguous_errors(testCase)
            % Both "Baseline" and "1. Baseline" present under the same
            % canonical -> the fallback scan finds two matches.
            alt_dir = fullfile(testCase.tmp_dir, 'alias', 'sub1', '1. Baseline');
            mkdir(alt_dir);
            fclose(fopen(fullfile(alt_dir, 'data.mat'), 'w'));
            pi = scifor.PathInput("alias/sub1/{session}/data.mat", ...
                'root_folder', testCase.tmp_dir, ...
                'aliases', struct('session', struct('BL', ["Baseline", "1. Baseline"])), 'name', "alias/sub1/{session}/data.mat");
            testCase.verifyError(@() pi.load('session', 'BL'), ...
                'scifor:PathInput:MultipleMatches');
        end

        function test_discover_canonicalizes_alias_spelling(testCase)
            pi = scifor.PathInput("alias/{subject}/{session}/data.mat", ...
                'root_folder', testCase.tmp_dir, ...
                'aliases', struct('session', struct('BL', "Baseline")), 'name', "alias/{subject}/{session}/data.mat");
            combos = pi.discover();
            sessions = containers.Map();
            for i = 1:numel(combos)
                sessions(combos{i}.subject) = combos{i}.session;
            end
            testCase.verifyEqual(sessions('sub1'), 'BL');  % on disk: "Baseline"
            testCase.verifyEqual(sessions('sub2'), 'BL');  % on disk: "BL" itself
        end

        %% key_regex: adjacent-placeholder disambiguation

        function test_key_regex_unknown_key_errors(testCase)
            % The Python-side ValueError (key_regex key that isn't an
            % actual template placeholder) crosses the MATLAB/Python
            % bridge as some MException; the specific identifier is an
            % implementation detail of that bridge, so match generically.
            testCase.verifyError(@() scifor.PathInput("{subject}/data.mat", ...
                'key_regex', struct('session', '\d+'), 'name', "{subject}/data.mat"), ...
                ?MException);
        end

        function test_key_regex_default_greedy_split_is_wrong(testCase)
            % Baseline: without key_regex, adjacent placeholders with no
            % delimiter split at "everything but the last character."
            emg_dir = fullfile(testCase.tmp_dir, 'emg');
            mkdir(emg_dir);
            fclose(fopen(fullfile(emg_dir, 'SS01_EMG_SSV10.mat'), 'w'));

            pi = scifor.PathInput("{subject}_EMG_{speed}{trial}.mat", ...
                'root_folder', emg_dir, 'name', "{subject}_EMG_{speed}{trial}.mat");
            combos = pi.discover();
            testCase.verifyLength(combos, 1);
            testCase.verifyEqual(combos{1}.speed, 'SSV1');
            testCase.verifyEqual(combos{1}.trial, '0');
        end

        function test_key_regex_letters_digits_split_resolves_ambiguity(testCase)
            emg_dir = fullfile(testCase.tmp_dir, 'emg');
            mkdir(emg_dir);
            fclose(fopen(fullfile(emg_dir, 'SS01_EMG_SSV1.mat'), 'w'));
            fclose(fopen(fullfile(emg_dir, 'SS01_EMG_SSV10.mat'), 'w'));
            fclose(fopen(fullfile(emg_dir, 'SS02_EMG_FV12.mat'), 'w'));

            pi = scifor.PathInput("{subject}_EMG_{speed}{trial}.mat", ...
                'root_folder', emg_dir, ...
                'key_regex', struct('speed', '[A-Za-z]+', 'trial', '\d+'), 'name', "{subject}_EMG_{speed}{trial}.mat");
            combos = pi.discover();
            testCase.verifyLength(combos, 3);

            found = containers.Map('KeyType', 'char', 'ValueType', 'char');
            for i = 1:numel(combos)
                found(combos{i}.subject) = sprintf('%s|%s', combos{i}.speed, combos{i}.trial);
            end
            testCase.verifyEqual(found('SS02'), 'FV|12');
        end

        function test_key_regex_no_match_when_value_violates_pattern(testCase)
            emg_dir = fullfile(testCase.tmp_dir, 'emg_bad');
            mkdir(emg_dir);
            fclose(fopen(fullfile(emg_dir, 'EMG_SSV1a.mat'), 'w'));

            pi = scifor.PathInput("EMG_{speed}{trial}.mat", ...
                'root_folder', emg_dir, ...
                'key_regex', struct('speed', '[A-Za-z]+', 'trial', '\d+'), 'name', "EMG_{speed}{trial}.mat");
            combos = pi.discover();
            testCase.verifyEmpty(combos);
        end

        %% placeholder_keys tests

        function test_placeholder_keys_simple(testCase)
            pi = scifor.PathInput("{subject}/data.mat", 'name', "{subject}/data.mat");
            keys = pi.placeholder_keys();
            testCase.verifyEqual(keys, {'subject'});
        end

        function test_placeholder_keys_multiple(testCase)
            pi = scifor.PathInput("{subject}/{session}/data.mat", 'name', "{subject}/{session}/data.mat");
            keys = pi.placeholder_keys();
            testCase.verifyEqual(keys, {'subject', 'session'});
        end

        function test_placeholder_keys_mixed_segment(testCase)
            pi = scifor.PathInput("{subject}_XSENS_{session}_{speed}-001.xlsx", 'name', "{subject}_XSENS_{session}_{speed}-001.xlsx");
            keys = pi.placeholder_keys();
            testCase.verifyEqual(keys, {'subject', 'session', 'speed'});
        end

        function test_placeholder_keys_none(testCase)
            pi = scifor.PathInput("data/raw/file.mat", 'name', "data/raw/file.mat");
            keys = pi.placeholder_keys();
            testCase.verifyEmpty(keys);
        end

        function test_placeholder_keys_duplicates(testCase)
            pi = scifor.PathInput("{subject}/{subject}_data.mat", 'name', "{subject}/{subject}_data.mat");
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
                'root_folder', disc_dir, 'name', "{subject}/{session}/{subject}_{session}.csv");
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
                'root_folder', empty_dir, 'name', "{x}/data/{file}.csv");
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
                'root_folder', disc_dir, 'name', "XSENS/{file}.csv");
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
                'root_folder', disc_dir, 'name', "{x}/{x}_data.csv");
            combos = pi.discover();
            testCase.verifyLength(combos, 1);
            testCase.verifyEqual(combos{1}.x, 'A');
        end

        function test_discover_no_placeholders(testCase)
            % No placeholders — returns one combo if file exists
            disc_dir = fullfile(testCase.tmp_dir, 'noplace');
            mkdir(disc_dir);
            fclose(fopen(fullfile(disc_dir, 'data.mat'), 'w'));

            pi = scifor.PathInput("data.mat", 'root_folder', disc_dir, 'name', "data.mat");
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
                'root_folder', disc_dir, 'name', "report_{year}_{status}.csv");
            combos = pi.discover();
            testCase.verifyLength(combos, 2);
        end

        function test_discover_values_are_strings(testCase)
            disc_dir = fullfile(testCase.tmp_dir, 'strtypes');
            mkdir(fullfile(disc_dir, '1'));
            fclose(fopen(fullfile(disc_dir, '1', 'data.csv'), 'w'));

            pi = scifor.PathInput("{num}/data.csv", ...
                'root_folder', disc_dir, 'name', "{num}/data.csv");
            combos = pi.discover();
            testCase.verifyLength(combos, 1);
            testCase.verifyClass(combos{1}.num, 'char');
        end
    end

    % ------------------------------------------------------------------
    % apply_discovery + standalone scifor.for_each discovery integration
    % ------------------------------------------------------------------
    methods (Access = private)
        function root = makeGaitTree(~, base)
            % subjects 1,2; subject 1 has sessions A,B (A: fast,slow; B: fast),
            % subject 2 has session A (fast). 4 real files of 8 Cartesian combos.
            root = fullfile(base, 'gait');
            specs = {{'1','A','fast'}, {'1','A','slow'}, {'1','B','fast'}, {'2','A','fast'}};
            for i = 1:numel(specs)
                s = specs{i}{1}; ses = specs{i}{2}; sp = specs{i}{3};
                d = fullfile(root, s, 'XSENS', ses);
                if ~isfolder(d); mkdir(d); end
                fname = sprintf('%s_XSENS_%s_%s-001.xlsx', s, ses, sp);
                fclose(fopen(fullfile(d, fname), 'w'));
            end
        end

        function root = makePaddedNumericTree(~, base)
            % root/1/6MWT-001.mat (content "1.5"), root/2/6MWT-002.mat ("2.5")
            root = fullfile(base, 'padded_numeric');
            specs = {{'1','001','1.5'}, {'2','002','2.5'}};
            for i = 1:numel(specs)
                s = specs{i}{1}; trial = specs{i}{2}; content = specs{i}{3};
                d = fullfile(root, s);
                if ~isfolder(d); mkdir(d); end
                fid = fopen(fullfile(d, sprintf('6MWT-%s.mat', trial)), 'w');
                fprintf(fid, '%s', content);
                fclose(fid);
            end
        end
    end

    methods (Test)
        function test_apply_discovery_all_empty_uses_disk_combos(testCase)
            root = testCase.makeGaitTree(testCase.tmp_dir);
            pi = scifor.PathInput( ...
                "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx", ...
                'root_folder', root, 'name', "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx");
            iter = struct('subject', {{}}, 'session', {{}}, 'speed', {{}});
            [filled, combos] = pi.apply_discovery(iter, string.empty);
            testCase.verifyEqual(sort(string(filled.subject)), ["1" "2"]);
            testCase.verifyEqual(sort(string(filled.session)), ["A" "B"]);
            % Discovered combos drive iteration directly: 4 real files, not 8.
            testCase.verifyNotEmpty(combos);
            testCase.verifyLength(combos, 4);
        end

        function test_apply_discovery_explicit_key_falls_back(testCase)
            root = testCase.makeGaitTree(testCase.tmp_dir);
            pi = scifor.PathInput( ...
                "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx", ...
                'root_folder', root, 'name', "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx");
            iter = struct('subject', {{'1'}}, 'session', {{}}, 'speed', {{}});
            [filled, combos] = pi.apply_discovery(iter, "subject");
            % Empty keys still filled from disk...
            testCase.verifyEqual(sort(string(filled.session)), ["A" "B"]);
            % ...but combos do not drive iteration (Cartesian does).
            testCase.verifyEmpty(combos);
        end

        function test_foreach_standalone_discovers_pathinput_values(testCase)
            % Regression: scifor.for_each called directly with a PathInput and
            % all-empty [] schema keys must discover values from the filesystem
            % rather than raising "no input DataFrame has that column".
            root = testCase.makeGaitTree(testCase.tmp_dir);
            scifor.set_schema(["subject", "session", "speed"]);
            cleanup = onCleanup(@() scifor.set_schema(string.empty(1,0))); %#ok<NASGU>
            pathTemplate = "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx";
            gaitPath = scifor.PathInput(pathTemplate, 'root_folder', root, 'name', char(pathTemplate));

            % fn receives the resolved path; return its basename to verify.
            fn = @(p) string(p);
            result = scifor.for_each(fn, ...
                struct('xlsx_file_path', gaitPath), ...
                subject=[], session=[], speed=[]);

            % One row per real file (4), not the 8-combo Cartesian product.
            testCase.verifyEqual(height(result), 4);
            testCase.verifyTrue(all(ismember(["subject" "session" "speed"], ...
                string(result.Properties.VariableNames))));
        end

        function test_foreach_standalone_case_a_adopts_template_keys(testCase)
            % Case A: NO schema declared and NO key=[] kwargs. Discovery must
            % still run and adopt the template's placeholders as the iteration.
            %
            % This is the case the gate at +scifor/for_each.m:242 used to miss:
            % with no keys at all, `any(cellfun(@isempty, meta_values))` is
            % `any([])` = false, so discovery never ran and for_each returned an
            % empty table (0 rows, no columns) without logging anything. The
            % EachOf tests in TestSciforEachOf.m exercise the same path, but
            % only through EachOf -- this pins it directly so a regression is
            % attributed to discovery rather than to EachOf expansion.
            root = testCase.makeGaitTree(testCase.tmp_dir);
            scifor.set_schema(string.empty(1, 0));
            cleanup = onCleanup(@() scifor.set_schema(string.empty(1,0))); %#ok<NASGU>
            pathTemplate = "{subject}/XSENS/{session}/{subject}_XSENS_{session}_{speed}-001.xlsx";
            gaitPath = scifor.PathInput(pathTemplate, 'root_folder', root, 'name', char(pathTemplate));

            fn = @(p) string(p);
            result = scifor.for_each(fn, struct('xlsx_file_path', gaitPath));

            % One row per real file (4), not the 8-combo Cartesian product --
            % Case A returns the disk combos to drive iteration directly.
            testCase.verifyEqual(height(result), 4);

            % Adopted in TEMPLATE PLACEHOLDER order, matching Python's
            % combos[0].keys(). Repeated placeholders ({subject} and {session}
            % each appear twice) are adopted once, at first appearance.
            cols = string(result.Properties.VariableNames);
            key_cols = cols(ismember(cols, ["subject" "session" "speed"]));
            testCase.verifyEqual(key_cols, ["subject" "session" "speed"], ...
                'Case A must adopt keys in template-placeholder order');
        end

        % --------------------------------------------------------------
        % condense_numeric: MATLAB parity with Python's standalone-only
        % zero-padded discovery condensation (docs/claude/schema-key-types.md)
        % --------------------------------------------------------------

        function test_apply_discovery_condense_numeric_collapses_zero_padding(testCase)
            root = testCase.makePaddedNumericTree(testCase.tmp_dir);
            pi = scifor.PathInput('{subject}/6MWT-{trial}.mat', 'root_folder', root, 'name', '{subject}/6MWT-{trial}.mat');
            iter = struct('subject', {{}}, 'trial', {{}});
            [filled, combos] = pi.apply_discovery(iter, string.empty, true);

            testCase.verifyClass(filled.trial{1}, 'double');
            testCase.verifyEqual(sort([filled.trial{:}]), [1 2]);
            testCase.verifyClass(filled.subject{1}, 'double');
            testCase.verifyEqual(sort([filled.subject{:}]), [1 2]);

            trial_vals = cellfun(@(s) s.trial, combos);
            testCase.verifyEqual(sort(trial_vals), [1 2]);
        end

        function test_apply_discovery_default_condense_numeric_false(testCase)
            % Default (3-arg call, matching every pre-existing call site)
            % stays verbatim char -- no behavior change without opt-in.
            root = testCase.makePaddedNumericTree(testCase.tmp_dir);
            pi = scifor.PathInput('{subject}/6MWT-{trial}.mat', 'root_folder', root, 'name', '{subject}/6MWT-{trial}.mat');
            iter = struct('subject', {{}}, 'trial', {{}});
            [filled, ~] = pi.apply_discovery(iter, string.empty);

            testCase.verifyClass(filled.trial{1}, 'char');
            testCase.verifyEqual(sort(string(filled.trial)), ["001" "002"]);
        end

        function test_apply_discovery_condense_numeric_explicit_value_untouched(testCase)
            % An explicit (non-empty) value is user intent -- condensation
            % only ever touches values PathInput itself discovered from disk.
            root = testCase.makePaddedNumericTree(testCase.tmp_dir);
            pi = scifor.PathInput('{subject}/6MWT-{trial}.mat', 'root_folder', root, 'name', '{subject}/6MWT-{trial}.mat');
            iter = struct('subject', {{'001'}}, 'trial', {{}});
            [filled, ~] = pi.apply_discovery(iter, "subject", true);

            testCase.verifyEqual(filled.subject, {'001'});
        end

        function test_foreach_standalone_condenses_zero_padded_schema_column(testCase)
            % End-to-end: scifor.for_each's own standalone discovery call
            % site always opts in to condense_numeric, so a zero-padded
            % 'trial' discovered from disk comes back as a numeric column.
            root = testCase.makePaddedNumericTree(testCase.tmp_dir);
            scifor.set_schema(["subject", "trial"]);
            cleanup = onCleanup(@() scifor.set_schema(string.empty(1,0))); %#ok<NASGU>
            pi = scifor.PathInput('{subject}/6MWT-{trial}.mat', 'root_folder', root, 'name', '{subject}/6MWT-{trial}.mat');

            fn = @(filepath) str2double(fileread(filepath));
            result = scifor.for_each(fn, struct('filepath', pi), subject=[], trial=[]);

            testCase.verifyEqual(height(result), 2);
            testCase.verifyTrue(isnumeric(result.trial));
            testCase.verifyEqual(sort(result.trial)', [1 2]);
            testCase.verifyTrue(isnumeric(result.subject));
            testCase.verifyEqual(sort(result.subject)', [1 2]);
        end
    end
end
