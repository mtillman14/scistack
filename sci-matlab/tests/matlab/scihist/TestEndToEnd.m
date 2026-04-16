classdef TestEndToEnd < matlab.unittest.TestCase
%TESTENDTOEND  End-to-end integration tests exercising full workflows.
%
%   These tests verify complete realistic scenarios involving multiple
%   components working together: configure -> save -> lineage fcn -> save ->
%   load -> provenance -> for_each.

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
            scihist.configure_database( ...
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
        function test_full_pipeline_save_process_load(testCase)
            %% Step 1: Save raw data for multiple subjects
            for s = [1 2 3]
                data = s * [10 20 30 40 50];
                RawSignal().save(data, 'subject', s, 'session', 'A');
            end

            %% Step 2: Process with a lineage function
            lfcn = scidb.LineageFcn(@double_values);
            for s = [1 2 3]
                raw = RawSignal().load('subject', s, 'session', 'A');
                result = lfcn(raw);
                ProcessedSignal().save(result, 'subject', s, 'session', 'A');
            end

            %% Step 3: Verify processed data
            for s = [1 2 3]
                proc = ProcessedSignal().load('subject', s, 'session', 'A');
                expected = s * [20 40 60 80 100];
                testCase.verifyEqual(proc.data, expected', 'AbsTol', 1e-10);
            end

            %% Step 4: Verify lineage
            for s = [1 2 3]
                prov = ProcessedSignal().provenance('subject', s, 'session', 'A');
                testCase.verifyEqual(char(prov.function_name), 'double_values');
                testCase.verifyEqual(numel(prov.inputs), 1);
            end

            %% Step 5: Verify list_versions
            versions = ProcessedSignal().list_versions('subject', 1, 'session', 'A');
            testCase.verifyEqual(numel(versions), 1);
            testCase.verifyTrue(isfield(versions, 'record_id'));
        end

        function test_for_each_pipeline(testCase)
            %% Step 1: Save raw data
            for s = [1 2]
                for sess = ["A", "B"]
                    data = s * 10 + double(char(sess)) * [1 2 3];
                    RawSignal().save(data, 'subject', s, 'session', sess);
                end
            end

            %% Step 2: Process with for_each
            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A", "B"]);

            %% Step 3: Verify all outputs exist and are correct
            for s = [1 2]
                for sess = ["A", "B"]
                    raw = RawSignal().load('subject', s, 'session', sess);
                    proc = ProcessedSignal().load('subject', s, 'session', sess);
                    testCase.verifyEqual(proc.data, raw.data * 2, 'AbsTol', 1e-10);
                end
            end
        end

        function test_chained_processing_pipeline(testCase)
            %% Save raw data
            RawSignal().save([1 2 3 4 5 6], 'subject', 1, 'session', 'A');

            %% Chain 1: double
            raw = RawSignal().load('subject', 1, 'session', 'A');
            lfcn1 = scidb.LineageFcn(@double_values);
            step1 = lfcn1(raw);
            ProcessedSignal().save(step1, 'subject', 1, 'session', 'A');

            %% Chain 2: add offset
            proc = ProcessedSignal().load('subject', 1, 'session', 'A');
            lfcn2 = scidb.LineageFcn(@add_offset);
            step2 = lfcn2(proc, 100);
            FilteredSignal().save(step2, 'subject', 1, 'session', 'A');

            %% Verify final data: (x * 2) + 100
            final = FilteredSignal().load('subject', 1, 'session', 'A');
            expected = [1 2 3 4 5 6] * 2 + 100;
            testCase.verifyEqual(final.data, expected', 'AbsTol', 1e-10);

            %% Verify lineage chain
            prov = FilteredSignal().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(prov.function_name), 'add_offset');
            % Input should reference the processed signal (lineage result)
            testCase.verifyEqual(numel(prov.inputs), 1);
            testCase.verifyEqual(numel(prov.constants), 1);
        end

        function test_baseline_subtraction_workflow(testCase)
            %% Save baseline data
            BaselineSignal().save([100 200 300], 'subject', 1, 'session', 'BL');
            BaselineSignal().save([150 250 350], 'subject', 2, 'session', 'BL');

            %% Save current session data
            RawSignal().save([110 210 310], 'subject', 1, 'session', 'A');
            RawSignal().save([105 205 305], 'subject', 1, 'session', 'B');
            RawSignal().save([170 270 370], 'subject', 2, 'session', 'A');
            RawSignal().save([160 260 360], 'subject', 2, 'session', 'B');

            %% Process with for_each using Fixed baseline
            scidb.for_each(@subtract_baseline, ...
                struct('current', RawSignal(), ...
                       'baseline', scidb.Fixed(BaselineSignal(), ...
                           'session', 'BL')), ...
                {DeltaSignal()}, ...
                'subject', [1 2], ...
                'session', ["A", "B"]);

            %% Verify results
            % Subject 1, Session A: [110-100, 210-200, 310-300]
            d = DeltaSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(d.data, [10 10 10]', 'AbsTol', 1e-10);

            % Subject 1, Session B: [105-100, 205-200, 305-300]
            d = DeltaSignal().load('subject', 1, 'session', 'B');
            testCase.verifyEqual(d.data, [5 5 5]', 'AbsTol', 1e-10);

            % Subject 2, Session A: [170-150, 270-250, 370-350]
            d = DeltaSignal().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(d.data, [20 20 20]', 'AbsTol', 1e-10);

            % Subject 2, Session B: [160-150, 260-250, 360-350]
            d = DeltaSignal().load('subject', 2, 'session', 'B');
            testCase.verifyEqual(d.data, [10 10 10]', 'AbsTol', 1e-10);
        end

        function test_cache_hit_in_for_each(testCase)
            %% Save raw data
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            %% Process manually (creates cache entry)
            raw = RawSignal().load('subject', 1, 'session', 'A');
            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn(raw);
            ProcessedSignal().save(result, 'subject', 1, 'session', 'A');

            %% Process again with for_each using same lineage fcn (should hit cache)
            scidb.for_each(lfcn, ...
                struct('x', RawSignal()), ...
                {FilteredSignal()}, ...
                'subject', 1, ...
                'session', "A");

            %% Both outputs should have the same data
            proc = ProcessedSignal().load('subject', 1, 'session', 'A');
            filt = FilteredSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(proc.data, filt.data, 'AbsTol', 1e-10);
        end

        function test_resave_preserves_data(testCase)
            %% Save original
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            %% Load and re-save under different metadata
            loaded = RawSignal().load('subject', 1, 'session', 'A');
            RawSignal().save(loaded.data, 'subject', 2, 'session', 'B');

            %% Verify both copies exist with correct data
            r1 = RawSignal().load('subject', 1, 'session', 'A');
            r2 = RawSignal().load('subject', 2, 'session', 'B');
            testCase.verifyEqual(r1.data, [1 2 3]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2.data, [1 2 3]', 'AbsTol', 1e-10);
        end

        function test_split_and_save_pipeline(testCase)
            %% Save input data
            RawSignal().save([10 20 30 40], 'subject', 1, 'session', 'A');

            %% Split using lineage function with unpack_output
            raw = RawSignal().load('subject', 1, 'session', 'A');
            lfcn = scidb.LineageFcn(@split_data, 'unpack_output', true);
            [first, second] = lfcn(raw);

            SplitFirst().save(first, 'subject', 1, 'session', 'A');
            SplitSecond().save(second, 'subject', 1, 'session', 'A');

            %% Load and verify
            r1 = SplitFirst().load('subject', 1, 'session', 'A');
            r2 = SplitSecond().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1.data, [10 20]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2.data, [30 40]', 'AbsTol', 1e-10);

            %% Verify provenance for both outputs
            p1 = SplitFirst().provenance('subject', 1, 'session', 'A');
            p2 = SplitSecond().provenance('subject', 1, 'session', 'A');
            testCase.verifyEqual(char(p1.function_name), 'split_data');
            testCase.verifyEqual(char(p2.function_name), 'split_data');

            %% Different outputs should have different lineage hashes
            testCase.verifyNotEqual(r1.lineage_hash, r2.lineage_hash);
        end

        function test_version_history(testCase)
            %% Save multiple versions
            id1 = RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            pause(0.1);
            id2 = RawSignal().save([4 5 6], 'subject', 1, 'session', 'A');
            pause(0.1);
            id3 = RawSignal().save([7 8 9], 'subject', 1, 'session', 'A');

            %% list_versions should show all 3
            versions = RawSignal().list_versions('subject', 1, 'session', 'A');
            testCase.verifyEqual(numel(versions), 3);

            %% load() returns latest
            latest = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(latest.data, [7 8 9]', 'AbsTol', 1e-10);

            %% load by version returns specific record
            specific = RawSignal().load('subject', 1, 'session', 'A', ...
                'version', id1);
            testCase.verifyEqual(specific.data, [1 2 3]', 'AbsTol', 1e-10);

            %% load_all returns all 3
            all_results = RawSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEqual(numel(all_results), 3);
        end

        function test_lineage_result_used_as_input_to_for_each(testCase)
            %% Save raw data
            RawSignal().save([5 10 15], 'subject', 1, 'session', 'A');
            RawSignal().save([6 12 18], 'subject', 2, 'session', 'A');

            %% Use a lineage function within for_each
            lfcn = scidb.LineageFcn(@double_values);
            scidb.for_each(lfcn, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A");

            %% Verify outputs have lineage
            p1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            p2 = ProcessedSignal().load('subject', 2, 'session', 'A');

            testCase.verifyEqual(p1.data, [10 20 30]', 'AbsTol', 1e-10);
            testCase.verifyEqual(p2.data, [12 24 36]', 'AbsTol', 1e-10);
            testCase.verifyTrue(strlength(p1.lineage_hash) > 0);
            testCase.verifyTrue(strlength(p2.lineage_hash) > 0);

            %% Verify provenance
            prov1 = ProcessedSignal().provenance('subject', 1, 'session', 'A');
            prov2 = ProcessedSignal().provenance('subject', 2, 'session', 'A');
            testCase.verifyEqual(char(prov1.function_name), 'double_values');
            testCase.verifyEqual(char(prov2.function_name), 'double_values');
        end

        function test_matrix_through_lineage_pipeline(testCase)
            %% Verify matrix shapes survive the full pipeline
            data = [1 2 3; 4 5 6; 7 8 9; 10 11 12];  % 4x3
            RawSignal().save(data, 'subject', 1, 'session', 'A');

            raw = RawSignal().load('subject', 1, 'session', 'A');
            lfcn = scidb.LineageFcn(@double_values);
            result = lfcn(raw);
            ProcessedSignal().save(result, 'subject', 1, 'session', 'A');

            proc = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(size(proc.data), [4, 3]);
            testCase.verifyEqual(proc.data, data * 2, 'AbsTol', 1e-10);
        end
    end
end
