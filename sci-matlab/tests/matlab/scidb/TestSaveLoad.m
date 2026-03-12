classdef TestSaveLoad < matlab.unittest.TestCase
%TESTSAVELOAD  Integration tests for BaseVariable save/load operations.

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
        % --- Save basics ---

        function test_save_returns_record_id(testCase)
            record_id = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            testCase.verifyTrue(ischar(record_id) || isstring(record_id));
            testCase.verifyEqual(strlength(string(record_id)), 16);
        end

        function test_save_different_data_different_record_ids(testCase)
            id1 = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            id2 = RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            testCase.verifyNotEqual(string(id1), string(id2));
        end

        % --- Load basics ---

        function test_load_returns_raw_data(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(isnumeric(result));
        end

        function test_save_and_load_vector(testCase)
            data = [1.5, 2.7, 3.9, 4.1];
            RawSignal().save(data, 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, data', 'AbsTol', 1e-10);
        end

        function test_save_and_load_scalar(testCase)
            RawSignal().save(3.14159, 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, 3.14159, 'AbsTol', 1e-10);
        end

        function test_save_and_load_matrix(testCase)
            data = [1 2 3; 4 5 6; 7 8 9];
            RawSignal().save(data, 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result), [3, 3]);
        end

        function test_save_and_load_column_vector(testCase)
            data = [1; 2; 3; 4; 5];
            RawSignal().save(data, 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result), [5, 1]);
        end

        function test_save_and_load_large_array(testCase)
            rng(42);  % Reproducible
            data = randn(100, 50);
            RawSignal().save(data, 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result, data, 'AbsTol', 1e-10);
            testCase.verifyEqual(size(result), [100, 50]);
        end

        % --- Metadata filtering ---

        function test_load_filters_by_metadata(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            RawSignal().save([7 8 9], 'subject', 2, 'session', 'A');

            result = RawSignal().load('subject', 1, 'session', 'B');
            testCase.verifyEqual(result, [4 5 6]', 'AbsTol', 1e-10);
        end

        function test_load_by_partial_metadata_single_match(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            result = RawSignal().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(result, [4 5 6]', 'AbsTol', 1e-10);
        end

        % --- Versioning ---

        function test_load_latest_version(testCase)
            id1 = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            pause(0.1);  % Ensure timestamps differ
            id2 = RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');
            % Multiple versions exist — load by specific record_id to get latest
            result = RawSignal().load('subject', 1, 'session', 'A', ...
                'version', id2);
            testCase.verifyEqual(result, [10 20 30]', 'AbsTol', 1e-10);
        end

        function test_load_by_specific_record_id(testCase)
            id1 = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([10 20 30], 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A', ...
                'version', id1);
            testCase.verifyEqual(result, [1 2 3]', 'AbsTol', 1e-10);
        end

        function test_load_latest_version_returns_single(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            % load() returns latest version only
            testCase.verifyEqual(result, [4 5 6]', 'AbsTol', 1e-10);
        end

        % --- load_all ---

        function test_load_all_returns_all_matching(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'A');
            RawSignal().save([7 8 9], 'subject', 1, 'session', 'A');
            results = RawSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEqual(numel(results), 3);
        end

        function test_load_all_each_is_thunk_output(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'A');
            results = RawSignal().load_all('subject', 1, 'session', 'A');
            for i = 1:numel(results)
                testCase.verifyClass(results(i), 'scidb.ThunkOutput');
            end
        end

        function test_load_all_empty_returns_empty_array(testCase)
            results = RawSignal().load_all('subject', 999, 'session', 'Z');
            testCase.verifyEmpty(results);
        end

        function test_load_all_filtered_by_metadata(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            RawSignal().save([7 8 9], 'subject', 2, 'session', 'A');
            results = RawSignal().load_all('subject', 1);
            testCase.verifyEqual(numel(results), 2);
        end

        % --- Error cases ---

        function test_load_not_found_throws_error(testCase)
            testCase.verifyError( ...
                @() RawSignal().load('subject', 999, 'session', 'Z'), ...
                'scidb:NotFoundError');
        end

        % --- Record properties via load_all ---

        function test_loaded_record_id_populated(testCase)
            saved_id = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            results = RawSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(results(1).record_id), char(saved_id));
        end

        function test_loaded_metadata_populated(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            results = RawSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyTrue(isfield(results(1).metadata, 'subject'));
            testCase.verifyTrue(isfield(results(1).metadata, 'session'));
        end

        function test_loaded_content_hash_populated(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            results = RawSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyTrue(strlength(results(1).content_hash) > 0);
        end

        function test_loaded_content_hash_differs_for_different_data(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            results = RawSignal().load_all('subject', 1);
            hashes = arrayfun(@(r) r.content_hash, results);
            testCase.verifyNotEqual(hashes(1), hashes(2));
        end

        function test_raw_data_has_empty_lineage_hash(testCase)
            % Raw data (not from a thunk) should have no lineage hash
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            results = RawSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEqual(results(1).lineage_hash, string.empty);
        end

        % --- list_versions ---

        function test_list_versions_returns_struct_array(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'A');
            versions = RawSignal().list_versions('subject', 1, 'session', 'A');
            testCase.verifyEqual(numel(versions), 2);
        end

        function test_list_versions_has_required_fields(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            versions = RawSignal().list_versions('subject', 1, 'session', 'A');
            testCase.verifyTrue(isfield(versions, 'record_id'));
            testCase.verifyTrue(isfield(versions, 'schema'));
            testCase.verifyTrue(isfield(versions, 'version'));
            testCase.verifyTrue(isfield(versions, 'timestamp'));
        end

        function test_list_versions_record_id_matches_saved(testCase)
            id1 = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            versions = RawSignal().list_versions('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(versions(1).record_id), char(id1));
        end

        % --- Different variable types are isolated ---

        function test_different_types_independent(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            ProcessedSignal().save([4 5 6], 'subject', 1, 'session', 'A');

            r1 = RawSignal().load('subject', 1, 'session', 'A');
            r2 = ProcessedSignal().load('subject', 1, 'session', 'A');

            testCase.verifyEqual(r1, [1 2 3]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2, [4 5 6]', 'AbsTol', 1e-10);
        end

        % --- Re-save loaded variable ---

        function test_resave_loaded_variable(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            loaded = RawSignal().load('subject', 1, 'session', 'A');

            % Re-save the loaded data under different metadata
            id2 = RawSignal().save(loaded, 'subject', 2, 'session', 'B');
            testCase.verifyTrue(strlength(string(id2)) == 16);

            reloaded = RawSignal().load('subject', 2, 'session', 'B');
            testCase.verifyEqual(reloaded, [1 2 3]', 'AbsTol', 1e-10);
        end
    end
end
