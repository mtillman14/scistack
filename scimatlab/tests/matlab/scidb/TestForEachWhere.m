classdef TestForEachWhere < matlab.unittest.TestCase
%TESTFOREACHWHRE  Integration tests for scidb.for_each with where= parameter.
%
%   Tests that:
%     - where= skips iterations where no input data matches the filter
%     - where= works with negation, compound AND/OR, and raw_sql filters
%     - where= is applied in the preload path (default) and the
%       per-iteration path (preload=false)
%     - where= works in parallel=true mode
%     - where= is applied to Fixed inputs via their pinned metadata
%     - where= is applied when inputs use SelectedColumn syntax
%     - All filter types (VariableFilter, SchemaKey, Raw) ARE applied to
%       scidb.Merge inputs (filter propagated to constituents)
%     - A SchemaKey filter still restricts rows when the Merge constituents
%       were themselves for_each-computed (carry a stored __where, so the
%       loader matches by provenance via Strategy 1)
%     - Coverage is validated against the Merge result (not per-constituent),
%       so a filter gap at a row eliminated by the Merge inner join does NOT
%       raise an error, but a gap at a row that survives DOES raise one
%     - dry_run=true with where= shows the filter note but makes no saves

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

        % ================================================================
        % Basic equality filter
        % ================================================================

        function test_where_eq_skips_non_matching(testCase)
        %TEST_WHERE_EQ_SKIPS_NON_MATCHING  Side()=="L" skips subjects with Side="R".
        %   Subjects 1 and 3 have Side="L" and are processed.
        %   Subject 2 has Side="R" and is skipped.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');
            Side().save("L", 'subject', 3, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            RawSignal().save([7 8 9], 'subject', 3, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A", ...
                where=Side() == "L");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            r3 = ProcessedSignal().load('subject', 3, 'session', 'A');
            testCase.verifyEqual(r1.data, [2 4 6]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r3.data, [14 16 18]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % Negation filter
        % ================================================================

        function test_where_not_filter(testCase)
        %TEST_WHERE_NOT_FILTER  ~(Side()=="L") keeps only Side="R" subjects.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A", ...
                where=~(Side() == "L"));

            r2 = ProcessedSignal().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(r2.data, [8 10 12]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 1, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % Compound AND filter
        % ================================================================

        function test_where_compound_and_filter(testCase)
        %TEST_WHERE_COMPOUND_AND_FILTER  Both Side=="L" AND StepLength>0.6 required.
        %   Subject 1: Side=L, StepLength=0.70 → both conditions met → processed.
        %   Subject 2: Side=L, StepLength=0.50 → StepLength fails → skipped.
        %   Subject 3: Side=R, StepLength=0.80 → Side fails → skipped.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("L", 'subject', 2, 'session', 'A');
            Side().save("R", 'subject', 3, 'session', 'A');
            StepLength().save(0.70, 'subject', 1, 'session', 'A');
            StepLength().save(0.50, 'subject', 2, 'session', 'A');
            StepLength().save(0.80, 'subject', 3, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            RawSignal().save([7 8 9], 'subject', 3, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A", ...
                where=(Side() == "L") & (StepLength() > 0.6));

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1.data, [2 4 6]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');

            testCase.verifyError(@() ProcessedSignal().load('subject', 3, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % Compound OR filter
        % ================================================================

        function test_where_compound_or_filter(testCase)
        %TEST_WHERE_COMPOUND_OR_FILTER  Side=="L" OR Side=="R" passes all subjects.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A", ...
                where=(Side() == "L") | (Side() == "R"));

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            r2 = ProcessedSignal().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(r1.data, [2 4 6]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2.data, [8 10 12]', 'AbsTol', 1e-10);
        end

        % ================================================================
        % raw_sql filter on the input variable's own value
        % ================================================================

        function test_where_raw_sql_filter(testCase)
        %TEST_WHERE_RAW_SQL_FILTER  raw_sql filter on the loaded variable's value.
        %   StepLength is the input variable; raw_sql filters by its own
        %   "value" column so only subjects where StepLength > 0.60 are processed.
        %   Subject 1: 0.70 > 0.60 → processed (output = 1.40).
        %   Subject 2: 0.45 not > 0.60 → skipped.
        %   Subject 3: 0.60 not strictly > 0.60 → skipped.
            StepLength().save(0.70, 'subject', 1, 'session', 'A');
            StepLength().save(0.45, 'subject', 2, 'session', 'A');
            StepLength().save(0.60, 'subject', 3, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', StepLength()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A", ...
                where=scidb.raw_sql('"value" > 0.60'));

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1.data, 1.40, 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');

            testCase.verifyError(@() ProcessedSignal().load('subject', 3, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % Multiple schema-key iteration levels
        % ================================================================

        function test_where_multiple_schema_keys(testCase)
        %TEST_WHERE_MULTIPLE_SCHEMA_KEYS  Iterating [subject x session]; filter
        %   applies per-combo.
        %   Subject 1 session A: Side=L → processed.
        %   Subject 1 session B: Side=R → skipped.
        %   Subject 2 session A: Side=R → skipped.
        %   Subject 2 session B: Side=L → processed.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');
            Side().save("R", 'subject', 2, 'session', 'A');
            Side().save("L", 'subject', 2, 'session', 'B');

            for s = [1 2]
                for sess = ["A" "B"]
                    RawSignal().save(s * [1 2 3], ...
                        'subject', s, 'session', sess);
                end
            end

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A" "B"], ...
                where=Side() == "L");

            r1a = ProcessedSignal().load('subject', 1, 'session', 'A');
            r2b = ProcessedSignal().load('subject', 2, 'session', 'B');
            testCase.verifyEqual(r1a.data, [2 4 6]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2b.data, [4 8 12]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 1, 'session', 'B'), 'scidb:NotFoundError');
            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % where= with preload=false (per-iteration load path)
        % ================================================================

        function test_where_preload_false(testCase)
        %TEST_WHERE_PRELOAD_FALSE  where= applies via per-iteration load when
        %   preload=false. Same behavior as with preload=true.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A", ...
                where=Side() == "L", ...
                preload=false);

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1.data, [2 4 6]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % where= with parallel=true
        % ================================================================

        function test_where_parallel(testCase)
        %TEST_WHERE_PARALLEL  where= is respected in parallel mode.
        %   The preload phase applies the filter; only matching combos
        %   are resolved and executed.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');
            Side().save("L", 'subject', 3, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');
            RawSignal().save([7 8 9], 'subject', 3, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2 3], ...
                'session', "A", ...
                parallel=true, ...
                where=Side() == "L");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            r3 = ProcessedSignal().load('subject', 3, 'session', 'A');
            testCase.verifyEqual(r1.data, [2 4 6]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r3.data, [14 16 18]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % where= with scidb.Fixed inputs
        % ================================================================

        function test_where_with_fixed_input(testCase)
        %TEST_WHERE_WITH_FIXED_INPUT  where= filters the non-fixed input.
        %   BaselineSignal is pinned to session="BL" via Fixed.
        %   Side is saved at both session A and BL so the filter is
        %   resolvable for both input preload queries.
        %   Subject 1: Side=L at session A → RawSignal passes filter → processed.
        %   Subject 2: Side=R at session A → RawSignal filtered out → skipped.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("L", 'subject', 1, 'session', 'BL');
            Side().save("R", 'subject', 2, 'session', 'A');
            Side().save("L", 'subject', 2, 'session', 'BL');

            BaselineSignal().save([100 200 300], 'subject', 1, 'session', 'BL');
            BaselineSignal().save([100 200 300], 'subject', 2, 'session', 'BL');
            RawSignal().save([110 210 310], 'subject', 1, 'session', 'A');
            RawSignal().save([120 220 320], 'subject', 2, 'session', 'A');

            scidb.for_each(@subtract_baseline, ...
                struct('current', RawSignal(), ...
                       'baseline', scidb.Fixed(BaselineSignal(), 'session', 'BL')), ...
                {DeltaSignal()}, ...
                'subject', [1 2], ...
                'session', "A", ...
                where=Side() == "L");

            d1 = DeltaSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(d1.data, [10 10 10]', 'AbsTol', 1e-10);

            testCase.verifyError(@() DeltaSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % where= with SelectedColumn inputs (single column)
        % ================================================================

        function test_where_with_selected_column_single(testCase)
        %TEST_WHERE_WITH_SELECTED_COLUMN_SINGLE  where= + single-column selection.
        %   Subject 1 (Side=L): col_a=[1;2;3] selected, sum=6 saved.
        %   Subject 2 (Side=R): skipped by where= filter.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');

            tbl1 = table([1.0; 2.0; 3.0], [10.0; 20.0; 30.0], ...
                'VariableNames', {'col_a', 'col_b'});
            tbl2 = table([4.0; 5.0; 6.0], [40.0; 50.0; 60.0], ...
                'VariableNames', {'col_a', 'col_b'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 2, 'session', 'A');

            scidb.for_each(@sum_array, ...
                struct('x', RawSignal("col_a")), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A", ...
                where=Side() == "L");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r1.data, 6.0, 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % where= with SelectedColumn inputs (multiple columns)
        % ================================================================

        function test_where_with_selected_column_multi(testCase)
        %TEST_WHERE_WITH_SELECTED_COLUMN_MULTI  where= + multi-column selection.
        %   Subject 1 (Side=L): 2-column subtable {col_a, col_b} saved.
        %   Subject 2 (Side=R): skipped.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');

            tbl1 = table([1.0; 2.0], [10.0; 20.0], [100.0; 200.0], ...
                'VariableNames', {'col_a', 'col_b', 'col_c'});
            tbl2 = table([3.0; 4.0], [30.0; 40.0], [300.0; 400.0], ...
                'VariableNames', {'col_a', 'col_b', 'col_c'});
            RawSignal().save(tbl1, 'subject', 1, 'session', 'A');
            RawSignal().save(tbl2, 'subject', 2, 'session', 'A');

            scidb.for_each(@noop_func, ...
                struct('x', RawSignal(["col_a", "col_b"])), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A", ...
                where=Side() == "L");

            r1 = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyTrue(istable(r1.data));
            testCase.verifyEqual(r1.data.Properties.VariableNames, {'col_a', 'col_b'});
            testCase.verifyEqual(height(r1.data), 2);

            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % VariableFilter where= with scidb.Merge (bypasses coverage check)
        % ================================================================

        function test_where_merge_only_not_filtered(testCase)
        %TEST_WHERE_MERGE_ONLY_NOT_FILTERED  VariableFilter (Side()=="L") IS now
        %   applied to Merge constituents. Coverage is validated once against the
        %   Merge inner-join result (not per-constituent), so no false-positive.
        %   Subject 1, session A: Side="R" → filter Side()=="L" excludes this
        %   schema location → iteration is skipped, no output saved.
            Side().save("R", 'subject', 1, 'session', 'A');

            tbl = table;
            tbl.val = [10; 20];
            GaitData().save(tbl, 'subject', 1, 'session', 'A');
            PareticSide().save(["p"; "np"], 'subject', 1, 'session', 'A');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', "A", ...
                where=Side() == "L");

            % Iteration skipped — filter applied, Side=R excluded
            testCase.verifyError( ...
                @() MergedResult().load('subject', 1, 'session', 'A'), ...
                'scidb:NotFoundError');
        end

        function test_where_merge_with_fixed_constituent(testCase)
        %TEST_WHERE_MERGE_WITH_FIXED_CONSTITUENT  where= IS applied to Merge
        %   with a Fixed constituent. Coverage validated against Merge result.
        %   The Merge effective ids are {sub=1,sess=A} (intersection: GaitData
        %   has A+B, Fixed(PareticSide,session=A) has only A). Coverage check:
        %   Side covers {sub=1,sess=A} → OK. Filter Side=="L": Side="R" at
        %   sess=A → no matching rows → both iterations skipped.
            tbl_a = table;
            tbl_a.val = [1; 2; 3];
            GaitData().save(tbl_a, 'subject', 1, 'session', 'A');

            tbl_b = table;
            tbl_b.val = [10; 20; 30];
            GaitData().save(tbl_b, 'subject', 1, 'session', 'B');

            PareticSide().save(["p"; "np"; "p"], 'subject', 1, 'session', 'A');

            % Side="R" for both sessions → filter Side()=="L" excludes them
            Side().save("R", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge( ...
                    GaitData(), ...
                    scidb.Fixed(PareticSide(), 'session', 'A'))), ...
                {MergedResult()}, ...
                'subject', 1, 'session', ["A" "B"], ...
                where=Side() == "L");

            % Both iterations skipped — filter applied, Side=R excluded
            testCase.verifyError( ...
                @() MergedResult().load('subject', 1, 'session', 'A'), ...
                'scidb:NotFoundError');
            testCase.verifyError( ...
                @() MergedResult().load('subject', 1, 'session', 'B'), ...
                'scidb:NotFoundError');
        end

        function test_where_merge_multi_record_join_with_filter(testCase)
        %TEST_WHERE_MERGE_MULTI_RECORD_JOIN_WITH_FILTER  Merge with multi-record
        %   join (iterating at subject level, both constituents return multiple
        %   sessions). where= IS applied: coverage validated against Merge result
        %   ({sub=1,sess=A; sub=1,sess=B}). Side saved for both sessions (both
        %   "R") satisfies coverage. Filter Side=="L" → empty → iteration skipped.
            RawSignal().save(10, 'subject', 1, 'session', 'A');
            RawSignal().save(20, 'subject', 1, 'session', 'B');
            ProcessedSignal().save(100, 'subject', 1, 'session', 'A');
            ProcessedSignal().save(200, 'subject', 1, 'session', 'B');

            % Side="R" for BOTH sessions (covers all Merge-result schema_ids)
            Side().save("R", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');

            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge(RawSignal(), ProcessedSignal())), ...
                {MergedResult()}, ...
                'subject', 1, ...
                where=Side() == "L");

            % Iteration skipped — filter applied, Side=R excluded
            testCase.verifyError( ...
                @() MergedResult().load('subject', 1), ...
                'scidb:NotFoundError');
        end

        % ================================================================
        % VariableFilter where= with scidb.Merge (now IS applied)
        % ================================================================

        function test_variable_filter_filters_merge_rows(testCase)
        %TEST_VARIABLE_FILTER_FILTERS_MERGE_ROWS  VariableFilter (Side()=="L")
        %   IS propagated to Merge constituents. Coverage validated once against
        %   Merge result. Sessions where Side=R are excluded; Side=L are kept.
        %   sess=A: Side=L → processed. sess=B: Side=R → skipped.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');

            tbl = table;
            tbl.val = [10; 20];
            GaitData().save(tbl, 'subject', 1, 'session', 'A');
            GaitData().save(tbl, 'subject', 1, 'session', 'B');
            PareticSide().save(["p"; "np"], 'subject', 1, 'session', 'A');
            PareticSide().save(["p"; "np"], 'subject', 1, 'session', 'B');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, 'session', ["A" "B"], ...
                where=Side() == "L");

            % sess=A processed (Side=L)
            r_a = MergedResult().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(r_a.data, 4);  % subject, session, val, PareticSide

            % sess=B skipped (Side=R)
            testCase.verifyError( ...
                @() MergedResult().load('subject', 1, 'session', 'B'), ...
                'scidb:NotFoundError');
        end

        function test_variable_filter_merge_coverage_error(testCase)
        %TEST_VARIABLE_FILTER_MERGE_COVERAGE_ERROR  Side missing for sess=B which
        %   IS in the Merge result → coverage error raised.
        %   Merge effective ids = {sub=1,sess=A; sub=1,sess=B} (both constituents
        %   have both sessions). Side only covers sess=A → missing sess=B → error.
            Side().save("L", 'subject', 1, 'session', 'A');
            % Side NOT saved for sess=B intentionally

            tbl = table;
            tbl.val = [10; 20];
            GaitData().save(tbl, 'subject', 1, 'session', 'A');
            GaitData().save(tbl, 'subject', 1, 'session', 'B');
            PareticSide().save(["p"; "np"], 'subject', 1, 'session', 'A');
            PareticSide().save(["p"; "np"], 'subject', 1, 'session', 'B');

            % Should raise because Side is missing for sess=B which IS in Merge result
            try
                scidb.for_each(@table_col_count, ...
                    struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                    {MergedResult()}, ...
                    'subject', 1, 'session', ["A" "B"], ...
                    where=Side() == "L");
                testCase.verifyTrue(false, 'Expected coverage error but none was raised');
            catch ME
                testCase.verifyTrue(contains(ME.message, 'missing data'), ...
                    ['Expected "missing data" in error, got: ', ME.message]);
            end
        end

        % ================================================================
        % SchemaKey where= with scidb.Merge (IS applied — no coverage check)
        % ================================================================

        function test_schema_key_isin_filters_merge_as_table(testCase)
        %TEST_SCHEMA_KEY_ISIN_FILTERS_MERGE_AS_TABLE  ismember(scidb.SchemaKey(key),
        %   values) IS propagated to Merge constituent loading.
        %   Regression: before the fix, the session filter was silently dropped
        %   for Merge inputs, so the function received rows for ALL sessions.
        %
        %   Setup: GaitData+PareticSide saved at sessions BL, POST, FOL.
        %   Filter: SchemaKey("session") IN ["BL","POST"].
        %   Expected: function receives table with 4 rows (2 sessions × 2 rows),
        %   NOT 6 rows (which would indicate FOL was included).
            GaitData().save(table([1;2], 'VariableNames', {'val'}), ...
                'subject', 1, 'session', 'BL');
            GaitData().save(table([3;4], 'VariableNames', {'val'}), ...
                'subject', 1, 'session', 'POST');
            GaitData().save(table([5;6], 'VariableNames', {'val'}), ...
                'subject', 1, 'session', 'FOL');

            PareticSide().save(["p";"np"], 'subject', 1, 'session', 'BL');
            PareticSide().save(["p";"np"], 'subject', 1, 'session', 'POST');
            PareticSide().save(["p";"np"], 'subject', 1, 'session', 'FOL');

            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'subject', 1, ...
                as_table=true, ...
                where=ismember(scidb.SchemaKey("session"), ["BL", "POST"]));

            result = MergedResult().load('subject', 1);
            % 2 sessions × 2 rows each = 4 rows; FOL excluded
            testCase.verifyEqual(result.data(1), 4, ...
                'Expected 4 rows (BL+POST only); FOL should have been filtered out');
        end

        function test_schema_key_compare_filters_merge_as_table(testCase)
        %TEST_SCHEMA_KEY_COMPARE_FILTERS_MERGE_AS_TABLE  scidb.SchemaKey(key)==val
        %   IS propagated to Merge constituent loading (numeric equality).
        %   Setup: GaitData+PareticSide saved for subjects 1 and 2.
        %   Filter: SchemaKey("subject") == 1.
        %   Expected: function receives table with only subject=1 rows.
            for subj = [1 2]
                GaitData().save(table([subj*10; subj*20], 'VariableNames', {'val'}), ...
                    'subject', subj, 'session', 'BL');
                PareticSide().save(["p";"np"], 'subject', subj, 'session', 'BL');
            end

            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge(GaitData(), PareticSide())), ...
                {MergedResult()}, ...
                'session', "BL", ...
                as_table=true, ...
                where=(scidb.SchemaKey("subject") == 1));

            result = MergedResult().load('session', 'BL');
            % subject=1 only: 1 row per session in GaitData/PareticSide
            % but each was saved with 2-row table, so height=2
            testCase.verifyEqual(result.data(1), 2, ...
                'Expected 2 rows (subject=1 only); subject=2 should have been filtered out');
        end

        % ================================================================
        % SchemaKey where= with scidb.Merge of for_each-COMPUTED constituents
        % (the Strategy 1 path: constituents carry a stored __where)
        % ================================================================

        function test_schema_key_isin_filters_computed_merge_constituents(testCase)
        %TEST_SCHEMA_KEY_ISIN_FILTERS_COMPUTED_MERGE_CONSTITUENTS  SchemaKey
        %   combined with a variable-level filter must still restrict rows when
        %   the Merge constituents were themselves computed by for_each, and so
        %   carry a stored __where version key.
        %
        %   This is the exact condition the raw-saved Merge tests above miss: a
        %   stored __where makes _load_with_where Strategy 1 fire (match by
        %   provenance), and before the fix Strategy 1 returned every schema_id
        %   sharing that variant — silently dropping the SchemaKey restriction so
        %   ALL sessions leaked through, just as the user observed.
        %
        %   Setup: RawSignal + Side="L" at sessions BL, POST, FOL.  Two outputs
        %   (ProcessedSignal, DeltaSignal) are computed via for_each with
        %   where=Side()=="L", so BOTH carry __where = "Side == 'L'".  Using two
        %   computed constituents means the Merge inner join cannot mask a leak
        %   in one of them (a raw-saved constituent would have restricted it).
        %
        %   Filter: SchemaKey("session") IN ["BL","POST"] AND Side()=="L".
        %   Expected: merged table has 2 rows (BL, POST); FOL excluded.
        %   Before the fix it had 3 rows (FOL leaked).
            for sess = ["BL" "POST" "FOL"]
                Side().save("L", 'subject', 1, 'session', sess);
                RawSignal().save([1 2 3], 'subject', 1, 'session', sess);
            end

            % Two for_each-computed constituents, both storing __where="Side == 'L'".
            scidb.for_each(@sum_array, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, 'session', ["BL" "POST" "FOL"], ...
                where=Side() == "L");
            scidb.for_each(@sum_array, ...
                struct('x', RawSignal()), ...
                {DeltaSignal()}, ...
                'subject', 1, 'session', ["BL" "POST" "FOL"], ...
                where=Side() == "L");

            % Merge the two computed outputs; filter by SchemaKey AND the same
            % variable-level filter that produced them (matches their __where).
            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge(ProcessedSignal(), DeltaSignal())), ...
                {MergedResult()}, ...
                'subject', 1, ...
                as_table=true, ...
                where=ismember(scidb.SchemaKey("session"), ["BL", "POST"]) & (Side() == "L"));

            result = MergedResult().load('subject', 1);
            % table_dims returns [height, width]; height = number of sessions.
            testCase.verifyEqual(result.data(1), 2, ...
                ['Expected 2 rows (BL+POST); a value of 3 means FOL leaked ' ...
                 'because the SchemaKey filter was dropped on computed Merge constituents']);
        end

        % ================================================================
        % where= with dry_run=true
        % ================================================================

        function test_where_dry_run(testCase)
        %TEST_WHERE_DRY_RUN  dry_run=true with where= prints the filter note
        %   and does not execute or save anything.
            Side().save("L", 'subject', 1, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', 1, ...
                'session', "A", ...
                dry_run=true, ...
                where=Side() == "L");

            testCase.verifyError(@() ProcessedSignal().load(), 'scidb:NotFoundError');
        end

        % ================================================================
        % All iterations skipped (filter matches nothing)
        % ================================================================

        function test_where_all_skipped_when_no_data_matches(testCase)
        %TEST_WHERE_ALL_SKIPPED_WHEN_NO_DATA_MATCHES  If no subject passes the
        %   filter, all iterations are skipped and no output is saved.
            Side().save("R", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'A');
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 2, 'session', 'A');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', "A", ...
                where=Side() == "L");

            testCase.verifyError(@() ProcessedSignal().load(), 'scidb:NotFoundError');
        end

        % ================================================================
        % where= with multiple schema key levels (3-key schema via alternate db)
        % ================================================================

        function test_where_three_schema_key_iteration(testCase)
        %TEST_WHERE_THREE_SCHEMA_KEY_ITERATION  where= works when all subject-
        %   session combos are iterated and the filter is session-specific.
        %   2 subjects × 2 sessions = 4 combos; filter keeps only 2.
            subjects = [1 2];
            sessions = ["A" "B"];

            % Subject 1, session A: Side=L → processed
            % Subject 1, session B: Side=R → skipped
            % Subject 2, session A: Side=R → skipped
            % Subject 2, session B: Side=L → processed
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');
            Side().save("R", 'subject', 2, 'session', 'A');
            Side().save("L", 'subject', 2, 'session', 'B');

            StepLength().save(0.60, 'subject', 1, 'session', 'A');
            StepLength().save(0.65, 'subject', 1, 'session', 'B');
            StepLength().save(0.70, 'subject', 2, 'session', 'A');
            StepLength().save(0.55, 'subject', 2, 'session', 'B');

            for s = subjects
                for sess = sessions
                    RawSignal().save(s * [1 2 3], ...
                        'subject', s, 'session', sess);
                end
            end

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', subjects, ...
                'session', sessions, ...
                where=Side() == "L");

            % Only (1,A) and (2,B) pass
            r1a = ProcessedSignal().load('subject', 1, 'session', 'A');
            r2b = ProcessedSignal().load('subject', 2, 'session', 'B');
            testCase.verifyEqual(r1a.data, [2 4 6]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2b.data, [4 8 12]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 1, 'session', 'B'), 'scidb:NotFoundError');
            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        function test_where_three_schema_key_compound_filter(testCase)
        %TEST_WHERE_THREE_SCHEMA_KEY_COMPOUND_FILTER  Compound AND filter across
        %   two schema keys: both Side=="L" and StepLength>0.58 must hold.
            % Subject 1, session A: Side=L, SL=0.60 → both pass → processed
            % Subject 1, session B: Side=R, SL=0.65 → Side fails → skipped
            % Subject 2, session A: Side=L, SL=0.50 → SL fails → skipped
            % Subject 2, session B: Side=L, SL=0.70 → both pass → processed
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');
            Side().save("L", 'subject', 2, 'session', 'A');
            Side().save("L", 'subject', 2, 'session', 'B');

            StepLength().save(0.60, 'subject', 1, 'session', 'A');
            StepLength().save(0.65, 'subject', 1, 'session', 'B');
            StepLength().save(0.50, 'subject', 2, 'session', 'A');
            StepLength().save(0.70, 'subject', 2, 'session', 'B');

            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            RawSignal().save([7 8 9], 'subject', 2, 'session', 'A');
            RawSignal().save([10 11 12], 'subject', 2, 'session', 'B');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A" "B"], ...
                where=(Side() == "L") & (StepLength() > 0.58));

            r1a = ProcessedSignal().load('subject', 1, 'session', 'A');
            r2b = ProcessedSignal().load('subject', 2, 'session', 'B');
            testCase.verifyEqual(r1a.data, [2 4 6]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2b.data, [20 22 24]', 'AbsTol', 1e-10);

            testCase.verifyError(@() ProcessedSignal().load('subject', 1, 'session', 'B'), 'scidb:NotFoundError');
            testCase.verifyError(@() ProcessedSignal().load('subject', 2, 'session', 'A'), 'scidb:NotFoundError');
        end

        % ================================================================
        % where= saved to database
        % ================================================================

        function test_compound_where_saved_to_db(testCase)
        % TEST_COMPOUND_WHERE_SAVED_TO_DB Test that the where= config is saved.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');
            Side().save("L", 'subject', 2, 'session', 'A');
            Side().save("L", 'subject', 2, 'session', 'B');

            StepLength().save(0.60, 'subject', 1, 'session', 'A');
            StepLength().save(0.65, 'subject', 1, 'session', 'B');
            StepLength().save(0.50, 'subject', 2, 'session', 'A');
            StepLength().save(0.70, 'subject', 2, 'session', 'B');

            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            RawSignal().save([7 8 9], 'subject', 2, 'session', 'A');
            RawSignal().save([10 11 12], 'subject', 2, 'session', 'B');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A" "B"], ...
                where=(Side() == "L") & (StepLength() > 0.58));

            % where= is no longer part of record identity. The issued filter
            % is recorded (display-only) on the producing run's
            % _run.where_clause. Verify it captured the filter string.
            db = scidb.get_database();
            rows = py.getattr(db, "_duck").fetchall( ...
                "SELECT where_clause FROM _run WHERE function_name = 'double_values'", ...
                py.list());
            testCase.verifyGreaterThan(int64(py.len(rows)), 0, ...
                'Expected at least one _run row for double_values');
            first_wc = char(string(rows{1}{1}));
            testCase.verifySubstring(first_wc, 'Side');
            testCase.verifySubstring(first_wc, 'StepLength');
        end

        function test_loading_with_different_where_same_level(testCase)
        % TEST_LOADING_WITH_DIFFERENT_WHERE_SAME_LEVEL Test that I can use
        % where= as an execution filter to reduce which schema ids are
        % iterated over.
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');

            StepLength().save(0.60, 'subject', 1, 'session', 'A');
            StepLength().save(0.65, 'subject', 1, 'session', 'B');

            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A" "B"], ...
                where=Side() == "L");

            scidb.for_each(@double_values, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                'session', ["A" "B"], ...
                where=Side() == "R");

            % The two issued filters are recorded (display-only) on the two
            % producing runs' _run.where_clause. Verify both are present.
            db = scidb.get_database();
            rows = py.getattr(db, "_duck").fetchall( ...
                "SELECT where_clause FROM _run WHERE function_name = 'double_values'", ...
                py.list());
            testCase.verifyGreaterThan(int64(py.len(rows)), 0, ...
                'Expected _run rows for double_values');
            wc_strings = cell(1, int64(py.len(rows)));
            for i = 1:numel(wc_strings)
                wc_strings{i} = char(string(rows{i}{1}));
            end
            has_L = any(cellfun(@(s) contains(s, "Side == 'L'"), wc_strings));
            has_R = any(cellfun(@(s) contains(s, "Side == 'R'"), wc_strings));
            testCase.verifyTrue(has_L, 'Expected a run with where_clause Side L');
            testCase.verifyTrue(has_R, 'Expected a run with where_clause Side R');

            % Check that loading with matching where= returns correct data
            result = ProcessedSignal().load(subject=1, session="A", where=Side()=="L");
            testCase.verifyEqual(result.data, [2; 4; 6]);

            % Loading with non-matching where= should throw an error:
            % there's no subject=1 session="A" ProcessedSignal saved with where=Side()=="R"
            testCase.verifyError(...
                @() ProcessedSignal().load(subject=1, session="A", where=Side()=="R"), ...
                'scidb:NotFoundError');

            % Check the R version at its correct location
            result_R = ProcessedSignal().load(subject=1, session="B", where=Side()=="R");
            testCase.verifyEqual(result_R.data, [8; 10; 12]);
        end

        function test_loading_with_different_where_cross_level(testCase)
            % TEST_LOADING_WITH_DIFFERENT_WHERE_CROSS_LEVEL Test that
            % where= successfully loads different versions of a variable
            % saved to a higher schema level than the where= filter
            % variable (e.g. Side is session level, ProcessedSignal is
            % subject level).
            Side().save("L", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');
            Side().save("L", 'subject', 2, 'session', 'A');
            Side().save("R", 'subject', 2, 'session', 'B');

            StepLength().save(0.60, 'subject', 1, 'session', 'A');
            StepLength().save(0.65, 'subject', 1, 'session', 'B');
            StepLength().save(0.50, 'subject', 2, 'session', 'A');
            StepLength().save(0.70, 'subject', 2, 'session', 'B');

            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5 6], 'subject', 1, 'session', 'B');
            RawSignal().save([7 8 9], 'subject', 2, 'session', 'A');
            RawSignal().save([10 11 12], 'subject', 2, 'session', 'B');

            scidb.for_each(@sum_array, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                where=Side() == "L");

            scidb.for_each(@sum_array, ...
                struct('x', RawSignal()), ...
                {ProcessedSignal()}, ...
                'subject', [1 2], ...
                where=Side() == "R");

            % The two issued filters are recorded (display-only) on the two
            % producing runs' _run.where_clause. Verify both are present.
            db = scidb.get_database();
            rows = py.getattr(db, "_duck").fetchall( ...
                "SELECT where_clause FROM _run WHERE function_name = 'sum_array'", ...
                py.list());
            testCase.verifyGreaterThan(int64(py.len(rows)), 0, ...
                'Expected _run rows for sum_array');
            wc_strings = cell(1, int64(py.len(rows)));
            for i = 1:numel(wc_strings)
                wc_strings{i} = char(string(rows{i}{1}));
            end
            has_L = any(cellfun(@(s) contains(s, "Side == 'L'"), wc_strings));
            has_R = any(cellfun(@(s) contains(s, "Side == 'R'"), wc_strings));
            testCase.verifyTrue(has_L, 'Expected a run with where_clause Side L');
            testCase.verifyTrue(has_R, 'Expected a run with where_clause Side R');

            % Check that I can use where= to filter for different versions of
            % ProcessedSignal.
            %
            % Each load MUST resolve to exactly one record, and that is checked
            % FIRST. Both variants live at the same output schema location
            % (subject=N, session unset) and differ only by which input location
            % the producing run consumed, so a where= that fails to narrow
            % returns both. Asserting the value straight off a multi-record
            % result does not report that: `result.data` on a struct array
            % expands to a comma-separated list, so `verifyEqual(result.data, 6)`
            % becomes verifyEqual(d1, d2, 6), 6 lands in the DIAGNOSTIC slot,
            % and the framework throws MATLAB:invalidType — burying "where=
            % matched 2 records" under an argument-validation error.
            expected = struct( ...
                'subject', {1, 1, 2, 2}, ...
                'side',    {"L", "R", "L", "R"}, ...
                'value',   {6, 15, 24, 33});
            for e = 1:numel(expected)
                exp = expected(e);
                result = ProcessedSignal().load( ...
                    subject=exp.subject, where=Side() == exp.side);
                testCase.verifyEqual(numel(result), 1, sprintf( ...
                    ['load(subject=%d, where=Side=="%s") must resolve to one ' ...
                     'record; %d matched, so where= did not select a variant'], ...
                    exp.subject, exp.side, numel(result)));
                testCase.verifyEqual(result(1).data, exp.value, sprintf( ...
                    'subject=%d where=%s', exp.subject, exp.side));
            end
        end

    end
end
