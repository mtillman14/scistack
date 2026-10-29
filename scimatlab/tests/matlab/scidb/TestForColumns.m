classdef TestForColumns < matlab.unittest.TestCase
%TESTFORCOLUMNS  for_columns — column-wise iteration + reassembly in for_each().
%
%   TypeClass().for_columns([...]) (or .for_columns() for all columns) runs
%   the function once per column of a wide-table variable and reassembles the
%   per-column results into a single output variable whose data, per schema
%   combo, is a one-row table with the same column names as the source.
%
%   This mirrors scidb/tests/test_for_columns.py for MATLAB parity.
%
%   Covers:
%   - all-columns resolution and explicit-subset selection
%   - output reassembled into one variable (1 x N, same column names)
%   - two for_columns inputs zipped by name (baseline Fixed + value)
%   - mismatched column sets raise
%   - column drift (a requested column absent) is a hard error
%   - caching: identical re-run is a hit; changing the function is not

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
        function seedWide(~, session)
            if nargin < 2
                session = "A";
            end
            GaitData().save( ...
                table([1.0; 2.0; 3.0], [10.0; 20.0; 30.0], ...
                      'VariableNames', {'StepLength', 'Cadence'}), ...
                'subject', "1", 'session', session);
            GaitData().save( ...
                table([4.0; 5.0; 6.0], [40.0; 50.0; 60.0], ...
                      'VariableNames', {'StepLength', 'Cadence'}), ...
                'subject', "2", 'session', session);
        end
    end

    methods (Test)

        % -----------------------------------------------------------------
        % Reassembly into a single output variable
        % -----------------------------------------------------------------

        function test_all_columns_reassembled(testCase)
            testCase.seedWide();

            scidb.for_each(@col_mean, ...
                struct('value', GaitData().for_columns()), ...
                {DeltaGait()}, ...
                'subject', [], 'session', []);

            d1 = DeltaGait().load('subject', "1", 'session', "A");
            testCase.verifyTrue(istable(d1.data));
            testCase.verifyTrue(ismember('StepLength', d1.data.Properties.VariableNames));
            testCase.verifyTrue(ismember('Cadence', d1.data.Properties.VariableNames));
            testCase.verifyEqual(d1.data.StepLength(1), 2.0, 'AbsTol', 1e-10);
            testCase.verifyEqual(d1.data.Cadence(1), 20.0, 'AbsTol', 1e-10);

            d2 = DeltaGait().load('subject', "2", 'session', "A");
            testCase.verifyEqual(d2.data.StepLength(1), 5.0, 'AbsTol', 1e-10);
            testCase.verifyEqual(d2.data.Cadence(1), 50.0, 'AbsTol', 1e-10);
        end

        function test_subset_columns(testCase)
            testCase.seedWide();

            scidb.for_each(@col_mean, ...
                struct('value', GaitData().for_columns("StepLength")), ...
                {DeltaGait()}, ...
                'subject', [], 'session', []);

            d1 = DeltaGait().load('subject', "1", 'session', "A");
            testCase.verifyTrue(ismember('StepLength', d1.data.Properties.VariableNames));
            testCase.verifyFalse(ismember('Cadence', d1.data.Properties.VariableNames));
            testCase.verifyEqual(d1.data.StepLength(1), 2.0, 'AbsTol', 1e-10);
        end

        % -----------------------------------------------------------------
        % Deferred ColName() — resolves to the current for_columns column
        % -----------------------------------------------------------------

        function test_deferred_colname_resolves_per_column(testCase)
            testCase.seedWide();

            scidb.for_each(@col_name_len, ...
                struct('value', GaitData().for_columns(), ...
                       'col_name', scidb.ColName()), ...
                {DeltaGait()}, ...
                'subject', [], 'session', []);

            % Per-column struct return -> "<col>__name_len"; the value is the
            % length of that source column's own name (StepLength=10, Cadence=7).
            d1 = DeltaGait().load('subject', "1", 'session', "A");
            testCase.verifyEqual(d1.data.StepLength__name_len(1), 10);
            testCase.verifyEqual(d1.data.Cadence__name_len(1), 7);
        end

        function test_deferred_colname_without_iterate_errors(testCase)
            testCase.seedWide();

            % A plain (non-iterate) variable input -> no for_columns axis, so
            % the deferred ColName() has nothing to resolve against and errors.
            testCase.verifyError(@() scidb.for_each(@col_name_len, ...
                struct('value', GaitData(), ...
                       'col_name', scidb.ColName()), ...
                {DeltaGait()}, ...
                'subject', [], 'session', []), ...
                ?MException);
        end

        % -----------------------------------------------------------------
        % Two for_columns inputs zipped by name (baseline + value)
        % -----------------------------------------------------------------

        function test_baseline_and_value(testCase)
            GaitData().save( ...
                table([1.0; 1.0], [10.0; 10.0], ...
                      'VariableNames', {'StepLength', 'Cadence'}), ...
                'subject', "1", 'session', "BL");
            GaitData().save( ...
                table([3.0; 3.0], [40.0; 40.0], ...
                      'VariableNames', {'StepLength', 'Cadence'}), ...
                'subject', "1", 'session', "FV");

            scidb.for_each(@mean_change, ...
                struct( ...
                    'baseline', scidb.Fixed(GaitData().for_columns(), 'session', "BL"), ...
                    'value', GaitData().for_columns()), ...
                {DeltaGait()}, ...
                'subject', [], 'session', "FV");

            d = DeltaGait().load('subject', "1", 'session', "FV");
            testCase.verifyEqual(d.data.StepLength(1), 2.0, 'AbsTol', 1e-10);  % 3 - 1
            testCase.verifyEqual(d.data.Cadence(1), 30.0, 'AbsTol', 1e-10);    % 40 - 10
        end

        function test_mismatched_column_sets_raise(testCase)
            testCase.seedWide();

            testCase.verifyError(@() scidb.for_each(@mean_change, ...
                struct( ...
                    'baseline', GaitData().for_columns("StepLength"), ...
                    'value', GaitData().for_columns("Cadence")), ...
                {DeltaGait()}, ...
                'subject', [], 'session', []), ...
                ?MException);
        end

        % -----------------------------------------------------------------
        % Column drift is a hard error
        % -----------------------------------------------------------------

        function test_missing_column_raises(testCase)
            testCase.seedWide();

            testCase.verifyError(@() scidb.for_each(@col_mean, ...
                struct('value', GaitData().for_columns(["StepLength", "DoesNotExist"])), ...
                {DeltaGait()}, ...
                'subject', [], 'session', []), ...
                ?MException);
        end

        % -----------------------------------------------------------------
        % Caching
        % -----------------------------------------------------------------

        function test_identical_rerun_is_cached(testCase)
            testCase.seedWide();

            scidb.for_each(@col_mean, ...
                struct('value', GaitData().for_columns()), ...
                {DeltaGait()}, 'subject', [], 'session', []);
            scidb.for_each(@col_mean, ...
                struct('value', GaitData().for_columns()), ...
                {DeltaGait()}, 'subject', [], 'session', []);

            versions = DeltaGait().list_versions('subject', "1", 'session', "A");
            testCase.verifyEqual(numel(versions), 1);
        end

        function test_changing_function_creates_new_record(testCase)
            % A different function over the same columns -> distinct version
            % key -> a new record coexists (same physical output columns).
            testCase.seedWide();

            scidb.for_each(@col_mean, ...
                struct('value', GaitData().for_columns("StepLength")), ...
                {DeltaGait()}, 'subject', [], 'session', []);
            scidb.for_each(@col_max, ...
                struct('value', GaitData().for_columns("StepLength")), ...
                {DeltaGait()}, 'subject', [], 'session', []);

            versions = DeltaGait().list_versions('subject', "1", 'session', "A");
            testCase.verifyEqual(numel(versions), 2);
        end


        % -----------------------------------------------------------------
        % Column groups (.claude/plan-for-columns-groups.md)
        % -----------------------------------------------------------------

        function test_groups_split_saves_each_group_sparsely(testCase)
            testCase.seedWalks();
            scidb.for_each(@latest_score, ...
                struct('g', Walks().for_columns( ...
                    {["Six", "Six_Date"], ["Ten", "Ten_Date"]})), ...
                {SixMWT(), TenMWT()}, 'subject', [], 'session', []);
            s1 = SixMWT().load('subject', "1", 'session', "A");
            testCase.verifyEqual(double(s1.data), 12, 'AbsTol', 1e-10);
            t2 = TenMWT().load('subject', "2", 'session', "A");
            testCase.verifyEqual(double(t2.data), 8, 'AbsTol', 1e-10);
            % Sparse: no SixMWT record for subject 2, no TenMWT for subject 3.
            testCase.verifyError(@() SixMWT().load('subject', "2", 'session', "A"), ?MException);
            testCase.verifyError(@() TenMWT().load('subject', "3", 'session', "A"), ?MException);
        end

        function test_groups_combine_saves_a_column_per_group(testCase)
            testCase.seedWalks();
            scidb.for_each(@latest_score, ...
                struct('g', Walks().for_columns(dictionary(["Six", "Ten"], ...
                    {["Six", "Six_Date"], ["Ten", "Ten_Date"]}))), ...
                {WalkSummary()}, 'subject', [], 'session', []);
            w2 = WalkSummary().load('subject', "2", 'session', "A");
            testCase.verifyEqual(string(w2.data.Properties.VariableNames), ["Six", "Ten"]);
            testCase.verifyTrue(isnan(w2.data.Six(1)));
            testCase.verifyEqual(w2.data.Ten(1), 8, 'AbsTol', 1e-10);
        end

        function test_groups_every_column_cannot_split(testCase)
            testCase.seedWalks();
            testCase.verifyError(@() scidb.for_each(@latest_score, ...
                struct('g', Walks().for_columns()), ...
                {SixMWT(), TenMWT()}, 'subject', [], 'session', []), ?MException);
        end

    end

    methods (Access = private)
        function seedWalks(~)
        %SEEDWALKS  The whiteboard example per subject: subject 2 blank for
        %   Six, subject 3 for Ten.
            six = {[10; 12], [NaN; NaN], [8; 9]};
            six_d = {["a"; "b"], [""; "  "], ["c"; "d"]};
            ten = {[5; 6], [7; 8], [NaN; NaN]};
            ten_d = {["e"; "f"], ["g"; "h"], [""; ""]};
            for s = 1:3
                Walks().save(table(six{s}, six_d{s}, ten{s}, ten_d{s}, ...
                    'VariableNames', {'Six', 'Six_Date', 'Ten', 'Ten_Date'}), ...
                    'subject', string(s), 'session', "A");
            end
        end
    end
end
