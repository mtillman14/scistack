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
%     - where= is accepted but NOT applied to scidb.Merge inputs
%       (Merge constituents bypass the filter path by design)
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

            results2 = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);
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

            results1 = ProcessedSignal().load_all('subject', 1, 'session', 'A');
            testCase.verifyEmpty(results1);
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

            results2 = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);

            results3 = ProcessedSignal().load_all('subject', 3, 'session', 'A');
            testCase.verifyEmpty(results3);
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

            results2 = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);

            results3 = ProcessedSignal().load_all('subject', 3, 'session', 'A');
            testCase.verifyEmpty(results3);
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

            results1b = ProcessedSignal().load_all('subject', 1, 'session', 'B');
            results2a = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results1b);
            testCase.verifyEmpty(results2a);
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

            results2 = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);
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

            results2 = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);
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

            results2 = DeltaSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);
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

            results2 = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);
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

            results2 = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(results2);
        end

        % ================================================================
        % where= with scidb.Merge (Merge bypasses the filter path)
        % ================================================================

        function test_where_merge_only_not_filtered(testCase)
        %TEST_WHERE_MERGE_ONLY_NOT_FILTERED  When the only input is a Merge,
        %   where= has no effect: Merge constituents are always loaded without
        %   the filter, so the iteration runs even if the filter would have
        %   excluded it.
        %   Subject 1: Side="R" (would be excluded by where=Side()=="L"),
        %   but since the only input is a Merge, the iteration still runs.
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

            % The iteration runs despite Side="R" — Merge bypasses where=
            result = MergedResult().load('subject', 1, 'session', 'A');
            % Columns: subject, session, val, PareticSide → 4
            testCase.verifyEqual(result.data, 4);
        end

        function test_where_merge_with_fixed_constituent(testCase)
        %TEST_WHERE_MERGE_WITH_FIXED_CONSTITUENT  where= is accepted alongside
        %   a Merge with a Fixed constituent. The Merge still joins correctly
        %   using its own schema-key logic; where= does not block it.
            tbl_a = table;
            tbl_a.val = [1; 2; 3];
            GaitData().save(tbl_a, 'subject', 1, 'session', 'A');

            tbl_b = table;
            tbl_b.val = [10; 20; 30];
            GaitData().save(tbl_b, 'subject', 1, 'session', 'B');

            PareticSide().save(["p"; "np"; "p"], 'subject', 1, 'session', 'A');

            % Side="R" would block the iteration, but all inputs are Merge
            Side().save("R", 'subject', 1, 'session', 'A');
            Side().save("R", 'subject', 1, 'session', 'B');

            scidb.for_each(@table_col_count, ...
                struct('data', scidb.Merge( ...
                    GaitData(), ...
                    scidb.Fixed(PareticSide(), 'session', 'A'))), ...
                {MergedResult()}, ...
                'subject', 1, 'session', ["A" "B"], ...
                where=Side() == "L");

            % Both iterations run despite Side="R" — Merge bypasses where=
            r_a = MergedResult().load('subject', 1, 'session', 'A');
            r_b = MergedResult().load('subject', 1, 'session', 'B');
            % Columns: subject, session, val, PareticSide → 4
            testCase.verifyEqual(r_a.data, 4);
            testCase.verifyEqual(r_b.data, 4);
        end

        function test_where_merge_multi_record_join_with_filter(testCase)
        %TEST_WHERE_MERGE_MULTI_RECORD_JOIN_WITH_FILTER  Merge with multi-record
        %   join (iterating at subject level, both constituents return multiple
        %   sessions) is unaffected by where=. The inner join by schema keys
        %   works as normal.
            RawSignal().save(10, 'subject', 1, 'session', 'A');
            RawSignal().save(20, 'subject', 1, 'session', 'B');
            ProcessedSignal().save(100, 'subject', 1, 'session', 'A');
            ProcessedSignal().save(200, 'subject', 1, 'session', 'B');

            % Side="R" for subject 1 — would block if filter were applied
            Side().save("R", 'subject', 1, 'session', 'A');

            scidb.for_each(@table_dims, ...
                struct('data', scidb.Merge(RawSignal(), ProcessedSignal())), ...
                {MergedResult()}, ...
                'subject', 1, ...
                where=Side() == "L");

            % Iteration runs: inner join yields 2 rows, 4 cols
            result = MergedResult().load('subject', 1);
            testCase.verifyEqual(result.data, [2, 4]');
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

            results = ProcessedSignal().load_all();
            testCase.verifyEmpty(results);
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

            results = ProcessedSignal().load_all();
            testCase.verifyEmpty(results);
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

            res1b = ProcessedSignal().load_all('subject', 1, 'session', 'B');
            res2a = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(res1b);
            testCase.verifyEmpty(res2a);
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

            res1b = ProcessedSignal().load_all('subject', 1, 'session', 'B');
            res2a = ProcessedSignal().load_all('subject', 2, 'session', 'A');
            testCase.verifyEmpty(res1b);
            testCase.verifyEmpty(res2a);
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

            % Check the duckdb database to ensure that the where= has
            % been stored in the database as a version key.
            db = scidb.get_database();
            rows = py.getattr(db, "_duck").fetchall( ...
                "SELECT version_keys FROM _record_metadata WHERE variable_name = 'ProcessedSignal'", ...
                py.list());
            testCase.verifyGreaterThan(int64(py.len(rows)), 0, ...
                'Expected at least one ProcessedSignal record in _record_metadata');
            % Check that __where is present in the version_keys JSON
            first_vk = char(string(rows{1}{1}));
            testCase.verifySubstring(first_vk, '__where');
            testCase.verifySubstring(first_vk, 'Side');
            testCase.verifySubstring(first_vk, 'StepLength');
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

            % Check the duckdb database to ensure that the where= has
            % been stored in the database as a version key.
            db = scidb.get_database();
            rows = py.getattr(db, "_duck").fetchall( ...
                "SELECT version_keys FROM _record_metadata WHERE variable_name = 'ProcessedSignal'", ...
                py.list());
            testCase.verifyGreaterThan(int64(py.len(rows)), 0, ...
                'Expected ProcessedSignal records in _record_metadata');
            % Verify both __where variants are stored
            vk_strings = cell(1, int64(py.len(rows)));
            for i = 1:numel(vk_strings)
                vk_strings{i} = char(string(rows{i}{1}));
            end
            has_L = any(cellfun(@(s) contains(s, "Side == 'L'"), vk_strings));
            has_R = any(cellfun(@(s) contains(s, "Side == 'R'"), vk_strings));
            testCase.verifyTrue(has_L, 'Expected __where with Side L in version_keys');
            testCase.verifyTrue(has_R, 'Expected __where with Side R in version_keys');

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

            % Check the duckdb database to ensure that the where= has
            % been stored in the database as a version key.
            db = scidb.get_database();
            rows = py.getattr(db, "_duck").fetchall( ...
                "SELECT version_keys FROM _record_metadata WHERE variable_name = 'ProcessedSignal'", ...
                py.list());
            testCase.verifyGreaterThan(int64(py.len(rows)), 0, ...
                'Expected ProcessedSignal records in _record_metadata');
            vk_strings = cell(1, int64(py.len(rows)));
            for i = 1:numel(vk_strings)
                vk_strings{i} = char(string(rows{i}{1}));
            end
            has_L = any(cellfun(@(s) contains(s, "Side == 'L'"), vk_strings));
            has_R = any(cellfun(@(s) contains(s, "Side == 'R'"), vk_strings));
            testCase.verifyTrue(has_L, 'Expected __where with Side L in version_keys');
            testCase.verifyTrue(has_R, 'Expected __where with Side R in version_keys');

            % Check that I can use where= to filter for different versions of ProcessedSignal
            % subject=1 where=L: loads RawSignal(1,A)=[1,2,3] (Side(1,A)=L), sum=6
            result = ProcessedSignal().load(subject=1, where=Side() == "L");
            testCase.verifyEqual(result.data, 6);
            % subject=1 where=R: loads RawSignal(1,B)=[4,5,6] (Side(1,B)=R), sum=15
            result = ProcessedSignal().load(subject=1, where=Side() == "R");
            testCase.verifyEqual(result.data, 15);
            % subject=2 where=L: loads RawSignal(2,A)=[7,8,9] (Side(2,A)=L), sum=24
            result = ProcessedSignal().load(subject=2, where=Side() == "L");
            testCase.verifyEqual(result.data, 24);
            % subject=2 where=R: loads RawSignal(2,B)=[10,11,12] (Side(2,B)=R), sum=33
            result = ProcessedSignal().load(subject=2, where=Side() == "R");
            testCase.verifyEqual(result.data, 33);
        end

    end
end
