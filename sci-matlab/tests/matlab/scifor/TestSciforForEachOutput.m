classdef TestSciforForEachOutput < matlab.unittest.TestCase
%TESTSCIFORFOREACHOUTPUT  Comprehensive tests for scifor.for_each output behavior.
%
%   Tests the output table structure for:
%   - Non-table outputs (scalar, vector, string, matrix, struct, logical)
%   - Table outputs (preserved columns, metadata prepended, conflict detection)
%   - Multiple outputs via varargout
%   - output_names interaction
%   - Edge cases (empty, dry_run, skipped)
%   - Feature interactions (distribute, where, Fixed, Merge)

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.schema_store_(string.empty(1, 0));
        end
    end

    % =====================================================================
    % A. Non-table outputs
    % =====================================================================

    methods (Test)

        function test_scalar_output_has_output_column(tc)
        %   Scalar output → table with metadata + 'output' column.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x * 2, ...
                struct('x', tbl), subject=[1 2]);

            tc.verifyTrue(istable(result));
            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [20.0; 40.0]);
            tc.verifyEqual(result.subject, [1; 2]);
        end

        function test_vector_output_in_cell_column(tc)
        %   Vector output → stored in 'output' cell column.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) [x; x*2; x*3], ...
                struct('x', tbl), subject=[1 2]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(height(result), 2);
            % Vector outputs → cell column
            tc.verifyTrue(iscell(result.output));
            tc.verifyEqual(result.output{1}, [10; 20; 30]);
            tc.verifyEqual(result.output{2}, [20; 40; 60]);
        end

        function test_string_output(tc)
        %   String scalar output → string column.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], ["hello";"hello"], ...
                'VariableNames', {'subject','value'});
            result = scifor.for_each(@(val) val, ...
                struct('val', tbl), subject=[1 2]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, ["hello"; "hello"]);
        end

        function test_matrix_output_in_cell_column(tc)
        %   Matrix output → stored in 'output' cell column.
            scifor.set_schema(["subject"]);

            result = scifor.for_each(@() eye(3), struct(), subject=[1 2]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyTrue(iscell(result.output));
            tc.verifyEqual(result.output{1}, eye(3));
        end

        function test_struct_output_as_struct_array(tc)
        %   Scalar struct output → stored in 'output' as struct array.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() struct('a', 1, 'b', 2), struct(), subject=[1]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyTrue(isstruct(result.output));
            tc.verifyFalse(iscell(result.output));
            tc.verifyEqual(result.output(1).a, 1);
            tc.verifyEqual(result.output(1).b, 2);
        end

        function test_logical_scalar_output(tc)
        %   Logical scalar output → logical column.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x > 15, ...
                struct('x', tbl), subject=[1 2]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [false; true]);
        end

    end

    % =====================================================================
    % B. Table outputs
    % =====================================================================

    methods (Test)

        function test_table_output_preserves_columns(tc)
        %   Table output → metadata prepended, original columns preserved.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([1.0; 2.0], [10.0; 20.0], 'VariableNames', {'A','B'}), ...
                struct(), subject=[1 2]);

            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
            tc.verifyTrue(ismember('A', result.Properties.VariableNames));
            tc.verifyTrue(ismember('B', result.Properties.VariableNames));
            % 2 subjects * 2 rows = 4
            tc.verifyEqual(height(result), 4);
        end

        function test_table_output_metadata_replicated(tc)
        %   Metadata is replicated for each row of table output.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([10; 20; 30], 'VariableNames', {'val'}), ...
                struct(), subject=[1 2]);

            tc.verifyEqual(height(result), 6);
            % Subject=1 should have 3 rows, subject=2 should have 3 rows
            tc.verifyEqual(sum(result.subject == 1), 3);
            tc.verifyEqual(sum(result.subject == 2), 3);
        end

        function test_table_output_with_mixed_column_types(tc)
        %   Table output with numeric and string columns.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([1.0; 2.0], ["a"; "b"], 'VariableNames', {'num','str'}), ...
                struct(), subject=[1]);

            tc.verifyTrue(ismember('num', result.Properties.VariableNames));
            tc.verifyTrue(ismember('str', result.Properties.VariableNames));
            tc.verifyEqual(result.num, [1.0; 2.0]);
            tc.verifyEqual(result.str, ["a"; "b"]);
        end

        function test_table_output_varying_row_counts(tc)
        %   Different combos can return tables with different row counts.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [3;2], 'VariableNames', {'subject','n'});
            result = scifor.for_each( ...
                @(n) table((1:n)', 'VariableNames', {'idx'}), ...
                struct('n', tbl), subject=[1 2]);

            % subject=1 returns 3 rows, subject=2 returns 2 rows → 5 total
            tc.verifyEqual(height(result), 5);
            tc.verifyEqual(sum(result.subject == 1), 3);
            tc.verifyEqual(sum(result.subject == 2), 2);
        end

        function test_table_output_column_conflict_with_metadata_errors(tc)
        %   Table output with column matching metadata key → error.
            scifor.set_schema(["subject"]);

            % Function returns table with 'subject' column — conflicts with metadata
            tc.verifyError(@() scifor.for_each( ...
                @() table([1; 2], 'VariableNames', {'subject'}), ...
                struct(), subject=[1]), ...
                'scifor:for_each');
        end

        function test_table_output_with_two_metadata_keys(tc)
        %   Table output with multiple metadata keys.
            scifor.set_schema(["subject", "session"]);

            result = scifor.for_each( ...
                @() table([10.0], 'VariableNames', {'val'}), ...
                struct(), subject=[1 2], session=["A" "B"]);

            % 2 subjects * 2 sessions * 1 row each = 4
            tc.verifyEqual(height(result), 4);
            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
            tc.verifyTrue(ismember('session', result.Properties.VariableNames));
            tc.verifyTrue(ismember('val', result.Properties.VariableNames));
        end

    end

    % =====================================================================
    % C. Multiple outputs via varargout
    % =====================================================================

    methods (Test)

        function test_two_scalar_outputs(tc)
        %   Two scalar outputs → two separate tables.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            [r1, r2] = scifor.for_each(@(x) deal(x*2, x+1), ...
                struct('x', tbl), subject=[1 2]);

            % r1: doubled values
            tc.verifyTrue(istable(r1));
            tc.verifyEqual(r1.output, [20.0; 40.0]);
            tc.verifyEqual(r1.subject, [1; 2]);

            % r2: incremented values
            tc.verifyTrue(istable(r2));
            tc.verifyEqual(r2.output, [11.0; 21.0]);
            tc.verifyEqual(r2.subject, [1; 2]);
        end

        function test_two_table_outputs(tc)
        %   Two table outputs → two separate flatten-mode tables.
            scifor.set_schema(["subject"]);

            [r1, r2] = scifor.for_each( ...
                @() deal( ...
                    table([1;2], 'VariableNames', {'A'}), ...
                    table([10;20;30], 'VariableNames', {'B'})), ...
                struct(), subject=[1]);

            % r1: 2-row table
            tc.verifyEqual(height(r1), 2);
            tc.verifyTrue(ismember('A', r1.Properties.VariableNames));
            tc.verifyTrue(ismember('subject', r1.Properties.VariableNames));

            % r2: 3-row table
            tc.verifyEqual(height(r2), 3);
            tc.verifyTrue(ismember('B', r2.Properties.VariableNames));
            tc.verifyTrue(ismember('subject', r2.Properties.VariableNames));
        end

        function test_mixed_table_and_scalar_outputs(tc)
        %   Mixed: first output table, second scalar → different structures.
            scifor.set_schema(["subject"]);

            [r1, r2] = scifor.for_each( ...
                @() deal( ...
                    table([1.0; 2.0; 3.0], 'VariableNames', {'val'}), ...
                    42.0), ...
                struct(), subject=[1 2]);

            % r1: table output → flatten mode, 2*3=6 rows
            tc.verifyEqual(height(r1), 6);
            tc.verifyTrue(ismember('val', r1.Properties.VariableNames));
            tc.verifyFalse(ismember('output', r1.Properties.VariableNames));

            % r2: scalar output → nested mode, 2 rows
            tc.verifyEqual(height(r2), 2);
            tc.verifyTrue(ismember('output', r2.Properties.VariableNames));
            tc.verifyEqual(r2.output, [42.0; 42.0]);
        end

        function test_three_outputs(tc)
        %   Three outputs → three separate tables.
            scifor.set_schema(["subject"]);

            [r1, r2, r3] = scifor.for_each( ...
                @() deal(1, "hello", true), struct(), subject=[1 2]);

            tc.verifyEqual(r1.output, [1; 1]);
            tc.verifyEqual(r2.output, ["hello"; "hello"]);
            tc.verifyEqual(r3.output, [true; true]);
        end

        function test_capture_fewer_outputs_than_function_returns(tc)
        %   Capturing only first output when function returns two.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) deal(x*2, x+1), ...
                struct('x', tbl), ...
                output_names={"doubled", "incremented"}, ...
                subject=[1 2]);

            % Only first output captured
            tc.verifyTrue(istable(result));
            tc.verifyTrue(ismember('doubled', result.Properties.VariableNames));
            tc.verifyEqual(result.doubled, [20.0; 40.0]);
        end

        function test_multiple_outputs_each_has_metadata(tc)
        %   Each output table has its own metadata columns.
            scifor.set_schema(["subject", "session"]);

            [r1, r2] = scifor.for_each( ...
                @() deal(1.0, 2.0), struct(), ...
                subject=[1 2], session=["A"]);

            for r = {r1, r2}
                t = r{1};
                tc.verifyTrue(ismember('subject', t.Properties.VariableNames));
                tc.verifyTrue(ismember('session', t.Properties.VariableNames));
            end
        end

    end

    % =====================================================================
    % D. output_names interaction
    % =====================================================================

    methods (Test)

        function test_output_names_overrides_default(tc)
        %   output_names={"my_result"} → column named 'my_result'.
            scifor.set_schema(["subject"]);

            result = scifor.for_each(@() 42, struct(), ...
                output_names={"my_result"}, subject=[1]);

            tc.verifyTrue(ismember('my_result', result.Properties.VariableNames));
            tc.verifyFalse(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.my_result, 42);
        end

        function test_output_names_with_multiple_outputs(tc)
        %   output_names names each output table's data column.
            scifor.set_schema(["subject"]);

            [r1, r2] = scifor.for_each(@() deal(10, 20), struct(), ...
                output_names={"speed","force"}, subject=[1]);

            tc.verifyTrue(ismember('speed', r1.Properties.VariableNames));
            tc.verifyEqual(r1.speed, 10);
            tc.verifyTrue(ismember('force', r2.Properties.VariableNames));
            tc.verifyEqual(r2.force, 20);
        end

        function test_output_names_integer_creates_N_tables(tc)
        %   output_names=2 → 2 separate tables with 'output' columns.
            scifor.set_schema(["subject"]);

            [r1, r2] = scifor.for_each(@() deal(1, 2), struct(), ...
                output_names=2, subject=[1]);

            tc.verifyTrue(ismember('output', r1.Properties.VariableNames));
            tc.verifyEqual(r1.output, 1);
            tc.verifyTrue(ismember('output', r2.Properties.VariableNames));
            tc.verifyEqual(r2.output, 2);
        end

        function test_output_names_does_not_apply_to_table_output(tc)
        %   For table outputs, columns come from the table, not output_names.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([1;2], 'VariableNames', {'my_col'}), ...
                struct(), output_names={"ignored"}, subject=[1]);

            tc.verifyTrue(ismember('my_col', result.Properties.VariableNames));
            % The output_name 'ignored' is not used since the output is a table
            tc.verifyFalse(ismember('ignored', result.Properties.VariableNames));
        end

    end

    % =====================================================================
    % E. Edge cases
    % =====================================================================

    methods (Test)

        function test_all_skipped_returns_empty_tables(tc)
        %   When all iterations fail, each output is an empty table.
            scifor.set_schema(["subject"]);

            [r1, r2] = scifor.for_each( ...
                @(x) deal(error('fail'), error('fail')), ...
                struct('x', table([1;2], [1;2], 'VariableNames', {'subject','v'})), ...
                output_names=2, subject=[1 2]);

            tc.verifyTrue(istable(r1));
            tc.verifyEqual(height(r1), 0);
            tc.verifyTrue(istable(r2));
            tc.verifyEqual(height(r2), 0);
        end

        function test_dry_run_returns_empty_for_all_outputs(tc)
        %   dry_run=true → [] for each output.
            scifor.set_schema(["subject"]);

            [r1, r2] = scifor.for_each(@() deal(1, 2), struct(), ...
                output_names=2, dry_run=true, subject=[1]);

            tc.verifyTrue(isempty(r1));
            tc.verifyTrue(isempty(r2));
        end

        function test_single_combo_scalar_output(tc)
        %   Single metadata combination, scalar result.
            scifor.set_schema(["subject"]);

            result = scifor.for_each(@() 99, struct(), subject=[1]);

            tc.verifyEqual(height(result), 1);
            tc.verifyEqual(result.output, 99);
            tc.verifyEqual(result.subject, 1);
        end

        function test_no_metadata_keys(tc)
        %   No metadata → single iteration, result has output column only.
            scifor.set_schema(["subject"]);

            result = scifor.for_each(@() 42, struct());

            tc.verifyEqual(height(result), 1);
            tc.verifyEqual(result.output, 42);
        end

    end

    % =====================================================================
    % F. Feature interactions
    % =====================================================================

    methods (Test)

        function test_distribute_non_table_output(tc)
        %   distribute + non-table output: vector split into rows.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() [10.0; 20.0; 30.0], struct(), ...
                distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 3);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [10.0; 20.0; 30.0]);
            tc.verifyEqual(result.trial, [1; 2; 3]);
        end

        function test_distribute_table_output(tc)
        %   distribute + table output: table split by row.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() table([10;20], [100;200], 'VariableNames', {'A','B'}), ...
                struct(), distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 2);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
            tc.verifyTrue(ismember('A', result.Properties.VariableNames));
            tc.verifyTrue(ismember('B', result.Properties.VariableNames));
        end

        function test_distribute_multiple_outputs(tc)
        %   distribute + two outputs: each distributed separately.
            scifor.set_schema(["subject", "trial"]);

            [r1, r2] = scifor.for_each( ...
                @() deal([10; 20], [100; 200; 300]), ...
                struct(), output_names=2, ...
                distribute=true, subject=[1]);

            % First output: 2 pieces
            tc.verifyEqual(height(r1), 2);
            tc.verifyEqual(r1.output, [10; 20]);

            % Second output: 3 pieces
            tc.verifyEqual(height(r2), 3);
            tc.verifyEqual(r2.output, [100; 200; 300]);
        end

        function test_distribute_expands_single_row_table_cells(tc)
        %   distribute + height-1 table with cell-array columns: cells
        %   are expanded into rows, scalar columns are replicated.
            scifor.set_schema(["subject", "cycle"]);

            fn = @() table({[10;20;30]}, {[100;200;300]}, 42, ...
                'VariableNames', {'A', 'B', 'scalar_col'});

            result = scifor.for_each(fn, struct(), ...
                distribute=true, subject=[1]);

            % 3 rows from the 3-element vectors
            tc.verifyEqual(height(result), 3);
            tc.verifyEqual(result.cycle, [1; 2; 3]);
            % Expanded columns present
            tc.verifyTrue(ismember('A', result.Properties.VariableNames));
            tc.verifyTrue(ismember('B', result.Properties.VariableNames));
            tc.verifyEqual(result.A, [10; 20; 30]);
            tc.verifyEqual(result.B, [100; 200; 300]);
            % Scalar column replicated
            tc.verifyTrue(ismember('scalar_col', result.Properties.VariableNames));
            tc.verifyEqual(result.scalar_col, [42; 42; 42]);
        end

        function test_distribute_single_row_with_logical_cells(tc)
        %   distribute + height-1 table with logical cell columns.
            scifor.set_schema(["subject", "cycle"]);

            fn = @() table({[true;false;true]}, {[1.1;2.2;3.3]}, ...
                'VariableNames', {'flags', 'vals'});

            result = scifor.for_each(fn, struct(), ...
                distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 3);
            tc.verifyEqual(result.flags, [true; false; true]);
            tc.verifyEqual(result.vals, [1.1; 2.2; 3.3]);
        end

        function test_distribute_single_row_replicates_mismatched_lengths(tc)
        %   distribute + cell columns of different lengths: matching-length
        %   columns are expanded, mismatched columns are replicated as cells.
            scifor.set_schema(["subject", "cycle"]);

            fn = @() table({[10;20;30]}, {[100;200;300]}, {[1;2]}, ...
                'VariableNames', {'A', 'B', 'short'});

            result = scifor.for_each(fn, struct(), ...
                distribute=true, subject=[1]);

            % A and B have length 3 (most common) → expanded
            tc.verifyEqual(height(result), 3);
            tc.verifyEqual(result.A, [10; 20; 30]);
            tc.verifyEqual(result.B, [100; 200; 300]);
            % short has length 2 → replicated as cell
            tc.verifyTrue(ismember('short', result.Properties.VariableNames));
            tc.verifyTrue(iscell(result.short));
            tc.verifyEqual(result.short{1}, [1;2]);
        end

        function test_distribute_single_row_no_expandable_keeps_original(tc)
        %   distribute + height-1 table with only scalar cells: no expansion,
        %   produces 1 distributed row as before.
            scifor.set_schema(["subject", "cycle"]);

            fn = @() table(42, "hello", 'VariableNames', {'num', 'str'});

            result = scifor.for_each(fn, struct(), ...
                distribute=true, subject=[1]);

            % No cell-array vectors → table stays height-1 → 1 cycle
            tc.verifyEqual(height(result), 1);
            tc.verifyEqual(result.cycle, 1);
        end

        function test_distribute_single_row_consistent_across_passes(tc)
        %   distribute + multiple passes with different cell lengths:
        %   all passes produce the same column set (no vertcat error).
            scifor.set_schema(["subject", "cycle"]);

            tbl = table([1;2], {[10;20;30];[100;200]}, {[1;2;3];[4;5]}, ...
                'VariableNames', {'subject', 'A', 'B'});

            result = scifor.for_each(@(data) data, ...
                struct('data', tbl), as_table=true, ...
                distribute=true, subject=[1 2]);

            % subject=1: 3 cycles, subject=2: 2 cycles → 5 total
            tc.verifyEqual(height(result), 5);
            tc.verifyTrue(ismember('A', result.Properties.VariableNames));
            tc.verifyTrue(ismember('B', result.Properties.VariableNames));
        end

        function test_where_filter_with_output_table(tc)
        %   where filter works correctly, result has output column.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','value'});

            result = scifor.for_each(@(data) sum(data.value), ...
                struct('data', tbl), ...
                where=scifor.Col("speed") > 1.0, ...
                as_table=true, subject=[1]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, 50);  % 20 + 30
        end

        function test_fixed_input_with_output_table(tc)
        %   Fixed inputs work correctly with new output format.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [1.1;1.2;2.1;2.2], ...
                'VariableNames', {'subject','session','emg'});

            result = scifor.for_each(@(baseline) baseline, ...
                struct('baseline', scifor.Fixed(tbl, session="pre")), ...
                subject=[1 2], session=["post"]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [1.1; 2.1], 'AbsTol', 1e-10);
        end

        function test_merge_input_with_output_table(tc)
        %   Merge inputs work correctly with new output format.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10.0;20.0], 'VariableNames', {'subject','force'});
            tbl2 = table([1;2], [0.1;0.2], 'VariableNames', {'subject','emg'});

            result = scifor.for_each( ...
                @(combined) combined.force + combined.emg, ...
                struct('combined', scifor.Merge(tbl1, tbl2)), ...
                subject=[1 2]);

            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [10.1; 20.2], 'AbsTol', 1e-10);
        end

        function test_constant_input_with_multiple_outputs(tc)
        %   Constant inputs work correctly with multi-output.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            [r1, r2] = scifor.for_each( ...
                @(x, alpha) deal(x * alpha, x + alpha), ...
                struct('x', tbl, 'alpha', 0.5), ...
                output_names=2, subject=[1 2]);

            tc.verifyEqual(r1.output, [5.0; 10.0]);
            tc.verifyEqual(r2.output, [10.5; 20.5]);
        end

    end

    % =====================================================================
    % G. Output column naming edge cases
    % =====================================================================

    methods (Test)

        function test_default_output_column_is_output(tc)
        %   Without output_names, the data column is 'output'.
            scifor.set_schema(["subject"]);

            result = scifor.for_each(@() 42, struct(), subject=[1]);
            tc.verifyEqual(result.Properties.VariableNames, {'subject', 'output'});
        end

        function test_table_output_no_output_column(tc)
        %   Table outputs do NOT have an 'output' column — they have their own columns.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([1], 'VariableNames', {'my_data'}), ...
                struct(), subject=[1]);

            tc.verifyFalse(ismember('output', result.Properties.VariableNames));
            tc.verifyTrue(ismember('my_data', result.Properties.VariableNames));
        end

        function test_multiple_combos_consistent_column_order(tc)
        %   All combos produce consistent column order.
            scifor.set_schema(["subject"]);

            result = scifor.for_each(@() 42, struct(), subject=[1 2 3]);
            tc.verifyEqual(result.Properties.VariableNames, {'subject', 'output'});
            tc.verifyEqual(height(result), 3);
        end

    end

end
