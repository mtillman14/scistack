classdef TestForEachPlotEndpoint < matlab.unittest.TestCase
%TESTFOREACHPLOTENDPOINT  plot_ endpoint leaves on the MATLAB path (D7).
%
%   A NAMED plot_* function returns a graphics handle; the framework exports
%   it to the combo's resolved scifor.PathOutput path, closes it, stores the
%   path as a record with lineage and embeds a provenance stamp in the file.
%   With save=false it is a DRAFT: files render, nothing is recorded.

    properties
        test_dir
        plots_dir
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
            testCase.plots_dir = fullfile(testCase.test_dir, 'plots');
            mkdir(testCase.plots_dir);
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
        function testRendersAndRecords(testCase)
            testCase.seed();
            template = fullfile(testCase.plots_dir, '{subject}_{session}.png');
            result = scidb.for_each(@plot_signal_line, ...
                struct('signal', RawSignal(), ...
                       'filename', scifor.PathOutput(template)), ...
                {ProcessedSignal()}, ...
                'subject', ["S01" "S02"], 'session', "1");

            testCase.verifyEqual(height(result), 2);
            f1 = fullfile(testCase.plots_dir, 'S01_1.png');
            testCase.verifyTrue(isfile(f1), 'figure file must exist');

            % The stored record holds the path string.
            rec = ProcessedSignal().load('subject', "S01", 'session', "1");
            testCase.verifyTrue(endsWith(string(rec.data), "S01_1.png"));

            % D4 stamp: read back through Python; record_id matches.
            blob = py.scidb.read_artifact_stamp(f1);
            testCase.verifyFalse(isa(blob, 'py.NoneType'), ...
                'recorded figure must carry an embedded provenance stamp');
            testCase.verifyEqual(char(blob{'record_id'}), char(rec.record_id));
        end

        function testDraftRendersWithoutRecording(testCase)
            testCase.seed();
            template = fullfile(testCase.plots_dir, '{subject}_{session}.png');
            scidb.for_each(@plot_signal_line, ...
                struct('signal', RawSignal(), ...
                       'filename', scifor.PathOutput(template)), ...
                {ProcessedSignal()}, ...
                'save', false, ...
                'subject', ["S01" "S02"], 'session', "1");

            testCase.verifyTrue(isfile(fullfile(testCase.plots_dir, 'S02_1.png')));
            % Nothing recorded.
            versions = ProcessedSignal().list_versions();
            testCase.verifyEmpty(versions);
            % Draft stamp: full blob, draft flag, no record_id.
            blob = py.scidb.read_artifact_stamp( ...
                fullfile(testCase.plots_dir, 'S01_1.png'));
            testCase.verifyFalse(isa(blob, 'py.NoneType'));
            testCase.verifyTrue(logical(blob{'draft'}));
        end

        function testCharReturnPassesThrough(testCase)
            testCase.seed();
            template = fullfile(testCase.plots_dir, 'self_{subject}.png');
            result = scidb.for_each(@plot_self_saving, ...
                struct('signal', RawSignal(), ...
                       'filename', scifor.PathOutput(template)), ...
                {ProcessedSignal()}, ...
                'subject', "S01", 'session', "1");
            testCase.verifyEqual(height(result), 1);
            testCase.verifyTrue(isfile(fullfile(testCase.plots_dir, 'self_S01.png')));
        end

        function testVariantPlaceholderFilePerGroup(testCase)
            % Two upstream variants -> {offset} placeholder -> one file each.
            RawSignal().save(5, 'subject', "S01", 'session', "1");
            for offset = [10 20]
                scidb.for_each(@add_offset, ...
                    struct('x', RawSignal(), 'offset', offset), ...
                    {ProcessedSignal()}, ...
                    'subject', "S01", 'session', "1");
            end
            template = fullfile(testCase.plots_dir, 'v_{offset}.png');
            scidb.for_each(@plot_signal_line, ...
                struct('signal', ProcessedSignal(), ...
                       'filename', scifor.PathOutput(template)), ...
                {DummyOut()}, ...
                'subject', "S01", 'session', "1");
            testCase.verifyTrue(isfile(fullfile(testCase.plots_dir, 'v_10.png')));
            testCase.verifyTrue(isfile(fullfile(testCase.plots_dir, 'v_20.png')));
        end
    end
end
