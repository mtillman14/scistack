classdef TestMerge < matlab.unittest.TestCase
%TESTMERGE  Integration tests for scidb.Merge in for_each.

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
        % --- Merge class construction ---

        function test_merge_requires_two_inputs(testCase)
            testCase.verifyError(@() scidb.Merge(RawSignal()), ...
                'scidb:Merge');
        end

        function test_merge_accepts_two_inputs(testCase)
            m = scidb.Merge(RawSignal(), ProcessedSignal());
            testCase.verifyEqual(numel(m.var_specs), 2);
        end

        function test_merge_rejects_nested_merge(testCase)
            testCase.verifyError( ...
                @() scidb.Merge(RawSignal(), scidb.Merge(ProcessedSignal(), BaselineSignal())), ...
                'scidb:Merge');
        end

        % --- Basic merge in for_each ---

        function test_merge_two_tables(testCase)
            % Save table data for GaitData
            tbl1 = table;
            tbl1.side = ["L"; "R"];
            tbl1.force = [10; 20];
            GaitData().save(tbl1, 'subject', 1, 'session', 'A');

            % Save array data for PareticSide
            PareticSide().save(["paretic"; "nonparetic"], ...
                'subject', 1, 'session', 'A');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");

            result = MergedResult().load('subject', 1, 'session', 'A');
            % Schema keys (subject, session) + GaitData (side, force) + PareticSide = 5 cols
            testCase.verifyEqual(result.data, 5);
        end

        function test_merge_two_arrays(testCase)
            % Save numeric arrays
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            ProcessedSignal().save([4 5 6], 'subject', 1, 'session', 'A');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(RawSignal(), ProcessedSignal())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");

            result = MergedResult().load('subject', 1, 'session', 'A');
            % Schema keys (subject, session) + two arrays → 4 columns
            testCase.verifyEqual(result.data, 4);
        end

        function test_merge_table_and_scalar(testCase)
            % Table with 3 rows
            tbl = table;
            tbl.val = [10; 20; 30];
            GaitData().save(tbl, 'subject', 1, 'session', 'A');

            % Scalar
            PareticSide().save("left", 'subject', 1, 'session', 'A');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");

            result = MergedResult().load('subject', 1, 'session', 'A');
            % Schema keys (subject, session) + 1 table col + 1 scalar col = 4
            testCase.verifyEqual(result.data, 4);
        end

        function test_merge_multiple_iterations(testCase)
            for s = [1 2]
                RawSignal().save(s * [1 2 3], ...
                    'subject', s, 'session', 'A');
                ProcessedSignal().save(s * [4 5 6], ...
                    'subject', s, 'session', 'A');
            end

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(RawSignal(), ProcessedSignal())), ...
                {MergedResult()}, ...
                'subject', [1 2], 'session', "A");

            % Both iterations should produce results
            r1 = MergedResult().load('subject', 1, 'session', 'A');
            r2 = MergedResult().load('subject', 2, 'session', 'A');
            % Schema keys (subject, session) + 2 data cols = 4
            testCase.verifyEqual(r1.data, 4);
            testCase.verifyEqual(r2.data, 4);
        end

        % --- Merge with Fixed ---

        function test_merge_with_fixed(testCase)
            % GaitData at session A and B
            tbl_a = table;
            tbl_a.val = [1; 2; 3];
            GaitData().save(tbl_a, 'subject', 1, 'session', 'A');

            tbl_b = table;
            tbl_b.val = [10; 20; 30];
            GaitData().save(tbl_b, 'subject', 1, 'session', 'B');

            % PareticSide at session A only (used as fixed baseline)
            PareticSide().save(["p"; "np"; "p"], ...
                'subject', 1, 'session', 'A');

            % Merge GaitData (iterates) with Fixed PareticSide (always session A)
            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge( ...
                    GaitData(), ...
                    scidb.Fixed(PareticSide(), 'session', 'A'))), ...
                {MergedResult()}, ...
                'subject', 1, 'session', ["A", "B"]);

            % Both iterations should succeed (PareticSide always loads from A)
            r_a = MergedResult().load('subject', 1, 'session', 'A');
            r_b = MergedResult().load('subject', 1, 'session', 'B');
            % Schema keys (subject, session) + 2 data cols = 4
            testCase.verifyEqual(r_a.data, 4);
            testCase.verifyEqual(r_b.data, 4);
        end

        % --- Merge with column selection ---

        function test_merge_with_column_selection(testCase)
            tbl = table;
            tbl.side = ["L"; "R"];
            tbl.force = [10; 20];
            tbl.angle = [30; 40];
            GaitData().save(tbl, 'subject', 1, 'session', 'A');

            PareticSide().save(["p"; "np"], 'subject', 1, 'session', 'A');

            % Select only 'force' from GaitData
            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge( ...
                    GaitData("force"), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");

            result = MergedResult().load('subject', 1, 'session', 'A');
            % Schema keys (subject, session) + 1 selected col + 1 PareticSide col = 4
            testCase.verifyEqual(result.data, 4);
        end

        % --- Dry run ---

        function test_merge_dry_run(testCase)
            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A", ...
                'dry_run', true);

            % Just verify it doesn't error — dry run should print merge info
            % and not attempt any loads
            testCase.verifyError(@() MergedResult().load(), 'scidb:NotFoundError');
        end

        % --- Multi-record merge (join by schema keys) ---

        function test_merge_multi_record_join(testCase)
            % Save data at two sessions for both constituents
            RawSignal().save(10, 'subject', 1, 'session', 'A');
            RawSignal().save(20, 'subject', 1, 'session', 'B');
            ProcessedSignal().save(100, 'subject', 1, 'session', 'A');
            ProcessedSignal().save(200, 'subject', 1, 'session', 'B');

            % Iterate over subject only — each constituent returns 2 records
            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge(RawSignal(), ProcessedSignal())), ...
                {MergedResult()}, ...
                'subject', 1);

            result = MergedResult().load('subject', 1);
            % Inner join on [subject, session] → 2 rows
            % Columns: subject, session, RawSignal, ProcessedSignal → 4 cols
            testCase.verifyEqual(result.data, [2, 4]');
        end

        function test_merge_multi_record_unmatched_is_refused(testCase)
            % One Merge rule (P6, docs/claude/merge-cases.json "unmatched"): a
            % Merge that would DROP rows is an error, not a quiet inner join.
            % RawSignal has 3 sessions, ProcessedSignal only 2: session C has
            % no partner, so the combination fails and nothing is saved.
            RawSignal().save(10, 'subject', 1, 'session', 'A');
            RawSignal().save(20, 'subject', 1, 'session', 'B');
            RawSignal().save(30, 'subject', 1, 'session', 'C');
            ProcessedSignal().save(100, 'subject', 1, 'session', 'A');
            ProcessedSignal().save(200, 'subject', 1, 'session', 'B');

            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge(RawSignal(), ProcessedSignal())), ...
                {MergedResult()}, ...
                'subject', 1);

            testCase.verifyError(@() MergedResult().load('subject', 1), ...
                'scidb:NotFoundError');
        end

        function test_merge_mixed_single_and_multi_record(testCase)
            % RawSignal at 2 sessions (multi-record when queried by subject)
            RawSignal().save(10, 'subject', 1, 'session', 'A');
            RawSignal().save(20, 'subject', 1, 'session', 'B');

            % ProcessedSignal only at session A
            ProcessedSignal().save(100, 'subject', 1, 'session', 'A');

            % Fixed forces ProcessedSignal to always load session A;
            % session is excluded from join keys so it broadcasts to all
            % RawSignal records
            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge( ...
                    RawSignal(), ...
                    scidb.Fixed(ProcessedSignal(), 'session', 'A'))), ...
                {MergedResult()}, ...
                'subject', 1);

            result = MergedResult().load('subject', 1);
            % Fixed broadcasts: 2 rows (sessions A, B), 4 cols
            testCase.verifyEqual(result.data, [2, 4]');
        end

        % --- Cross-level merge (coarse variable + fine variable) ---

        function test_merge_subject_level_with_session_level(testCase)
            % Regression: merging a subject-level variable (no session metadata)
            % with a session-level variable crashed with "Conversion from cell failed"
            % because the spread layout fills unused schema key columns with NaN,
            % which arrived in MATLAB as {0x0 double} cells that string() cannot convert.
            %
            % Expected: SubjectGrouping broadcasts to all sessions for each subject.

            % PareticSide saved at subject-level only (no session)
            PareticSide().save("left",  'subject', 1);
            PareticSide().save("right", 'subject', 2);

            % RawSignal saved at session-level
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            RawSignal().save([7 8 9], 'subject', 2, 'session', 'A');

            % for_each over session — Merge must broadcast PareticSide (no session)
            % to all sessions without crashing.
            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(PareticSide(), RawSignal())), ...
                {MergedResult()}, ...
                'subject', [1 2], 'session', ["A", "B"], ...
                as_table=true);

            % subject=1/session=A and subject=1/session=B should both succeed
            r1a = MergedResult().load('subject', 1, 'session', 'A');
            r1b = MergedResult().load('subject', 1, 'session', 'B');
            r2a = MergedResult().load('subject', 2, 'session', 'A');
            % schema keys (subject, session) + PareticSide + RawSignal = 4
            testCase.verifyEqual(r1a.data, 4);
            testCase.verifyEqual(r1b.data, 4);
            testCase.verifyEqual(r2a.data, 4);
        end

        % --- Error cases ---

        function test_merge_skip_on_missing_data(testCase)
            % Only save GaitData, not PareticSide
            tbl = table;
            tbl.val = [1; 2];
            GaitData().save(tbl, 'subject', 1, 'session', 'A');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A");

            % Should skip — no output saved
            testCase.verifyError(@() MergedResult().load(), 'scidb:NotFoundError');
        end

        % --- where= selects one variant by provenance (multi-variant merge) ---

        function test_merge_selects_variant_by_branch_param(testCase)
        %TEST_MERGE_SELECTS_VARIANT_BY_BRANCH_PARAM  When a variable has been
        %   computed by for_each in MULTIPLE constant (branch_param) variants that
        %   share the same schema keys, loading it through scidb.Merge with the
        %   constituent pinned via scidb.Variant must select the single matching
        %   variant — exactly like a direct branch_param-filtered load. Otherwise
        %   every variant leaks through (N rows per schema-key combo instead of 1).
        %
        %   Under the where= redesign, variant identity is semantic — by consumed
        %   inputs — so two all-pass where= filters over the same inputs are the
        %   SAME variant. A constant applied via branch_param/Variant is what
        %   distinguishes same-input variants (parity with scidb Python
        %   test_variable_filter_merge.py::TestMergeSelectsVariantByBranchParam).
        %
        %   Two add_offset variants over the same combos, distinguishable by value:
        %       offset=100 : ProcessedSignal = RawSignal + 100
        %       offset=200 : ProcessedSignal = RawSignal + 200
            RawSignal().save(10, 'subject', 1, 'session', 'A');
            RawSignal().save(20, 'subject', 1, 'session', 'B');
            PareticSide().save("p",  'subject', 1, 'session', 'A');
            PareticSide().save("np", 'subject', 1, 'session', 'B');

            % Variant 1: offset=100, ProcessedSignal = RawSignal + 100
            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 100), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', ["A" "B"]);
            % Variant 2: offset=200, ProcessedSignal = RawSignal + 200
            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 200), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', ["A" "B"]);

            % Merge pinning offset=100 must keep only variant 1:
            %   2 rows (sessions A,B), ProcessedSignal = [110,120], sum = 230.
            % Without the pin both variants leak → 4 rows, sum = 660.
            scidb.for_each(@merge_processed_summary, ...
                struct('data', scidb.Merge(PareticSide(), ...
                    scidb.Variant(ProcessedSignal(), offset=100))), ...
                {MergedResult()}, ...
                'subject', 1);

            r = MergedResult().load('subject', 1);
            testCase.verifyEqual(r.data, [2; 230], 'AbsTol', 1e-10);
        end

        function test_merge_selects_other_variant_by_branch_param(testCase)
        %TEST_MERGE_SELECTS_OTHER_VARIANT_BY_BRANCH_PARAM  Same setup, pinned to
        %   the OTHER variant (offset=200) selects variant 2 — proving the constant
        %   branch_param, not the shared schema keys, drives selection.
            RawSignal().save(10, 'subject', 1, 'session', 'A');
            RawSignal().save(20, 'subject', 1, 'session', 'B');
            PareticSide().save("p",  'subject', 1, 'session', 'A');
            PareticSide().save("np", 'subject', 1, 'session', 'B');

            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 100), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', ["A" "B"]);
            scidb.for_each(@add_offset, ...
                struct('x', RawSignal(), 'offset', 200), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', ["A" "B"]);

            % Merge pinning offset=200 must keep only variant 2:
            %   2 rows, ProcessedSignal = [210,220], sum = 430.
            scidb.for_each(@merge_processed_summary, ...
                struct('data', scidb.Merge(PareticSide(), ...
                    scidb.Variant(ProcessedSignal(), offset=200))), ...
                {MergedResult()}, ...
                'subject', 1);

            r = MergedResult().load('subject', 1);
            testCase.verifyEqual(r.data, [2; 430], 'AbsTol', 1e-10);
        end
    end
end
