classdef TestForEachStatEndpoint < matlab.unittest.TestCase
%TESTFOREACHSTATENDPOINT  stat_ endpoint leaves on the MATLAB path (D7).
%
%   A NAMED stat_* function returns a result struct; the bridge canonicalizes
%   it (normalize_stat_result) so MATLAB- and Python-run stats store
%   byte-identical JSON payloads. Draft (save=false): the resolved PathOutput arg
%   arrives as [], the result is printed, nothing is recorded. Record (the
%   default): canonical JSON stored with lineage; report_path embedded; the
%   report PDF stamped.

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
            close all force;
            if isfolder(testCase.test_dir)
                rmdir(testCase.test_dir, 's');
            end
        end
    end

    methods (Access = private)
        function seed(~)
            RawSignal().save(1.0, 'subject', "S01", 'session', "1");
            RawSignal().save(2.0, 'subject', "S01", 'session', "2");
        end
    end

    methods (Test)
        function testStoresCanonicalJson(testCase)
            testCase.seed();
            report = fullfile(testCase.test_dir, 'report.pdf');
            result = scidb.for_each(@stat_row_count, ...
                struct('df', RawSignal(), ...
                       'filename', scifor.PathOutput(report)), ...
                {DummyOut()}, ...
                'subject', "S01");   % session not iterated: aggregation

            testCase.verifyEqual(height(result), 1);
            rec = DummyOut().load('subject', "S01");
            parsed = jsondecode(char(string(rec.data)));
            % stat fn received the long-format table (as_table defaulted on).
            testCase.verifyEqual(parsed.n, 2);
            testCase.verifyFalse(isfield(parsed, 'date'), ...
                'wall-clock date must be stripped for reproducibility');
            testCase.verifyTrue(endsWith(string(parsed.report_path), "report.pdf"));
            testCase.verifyTrue(isfile(report), 'report PDF written in record mode');

            % D4: the report artifact carries the record''s stamp.
            blob = py.scidb.read_artifact_stamp(report);
            testCase.verifyFalse(isa(blob, 'py.NoneType'));
            testCase.verifyEqual(char(blob{'record_id'}), char(rec.record_id));
        end

        function testDraftPrintsAndRecordsNothing(testCase)
            testCase.seed();
            report = fullfile(testCase.test_dir, 'draft_report.pdf');
            result = scidb.for_each(@stat_row_count, ...
                struct('df', RawSignal(), ...
                       'filename', scifor.PathOutput(report)), ...
                {DummyOut()}, ...
                'save', false, ...
                'subject', "S01");

            testCase.verifyEqual(height(result), 1);
            testCase.verifyFalse(isfile(report), ...
                'filename=[] must disable the report artifact in draft');
            versions = DummyOut().list_versions();
            testCase.verifyEmpty(versions);
        end

        function testSkipComputedSecondRunSkips(testCase)
            % Cross-run identity: the canonical payload + graph identity must
            % make an identical recorded re-run skip (the byte-identity
            % canary for Python-side normalization).
            testCase.seed();
            report = fullfile(testCase.test_dir, 'skip_report.pdf');
            args = { ...
                struct('df', RawSignal(), ...
                       'filename', scifor.PathOutput(report)), ...
                {DummyOut()}, ...
                'skip_computed', true, ...
                'subject', "S01"};

            r1 = scidb.for_each(@stat_row_count, args{:});
            testCase.verifyEqual(height(r1), 1);
            r2 = scidb.for_each(@stat_row_count, args{:});
            testCase.verifyEqual(height(r2), 0, ...
                'identical recorded re-run must skip every combo');
        end
    end
end
