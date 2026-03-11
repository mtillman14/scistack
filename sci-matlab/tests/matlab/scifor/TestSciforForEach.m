classdef TestSciforForEach < matlab.unittest.TestCase
%TESTSCIFORFOREACH  Tests for scifor.for_each in standalone (no-DB) mode.
%
%   Tests the pure loop orchestrator: table filtering, Fixed, Merge,
%   ColumnSelection, distribute, dry_run, where, as_table,
%   output_names, empty-list resolution, and result table structure.
%
%   NOTE: classdef test methods cannot contain nested function definitions
%   that capture workspace variables. All tests verify behavior through the
%   result table returned by for_each, or use local helper functions
%   defined after the classdef block.

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.schema_store_(string.empty(1, 0));
        end
    end

    % =====================================================================
    % Basic table input filtering
    % =====================================================================

    methods (Test)
        function test_per_combo_table_detected(tc)
        %   Table with schema key columns is filtered per combo.
            scifor.set_schema(["subject", "session"]);

            rows = table([1;1;2;2], ["pre";"post";"pre";"post"], [1.1;1.2;2.1;2.2], ...
                'VariableNames', {'subject','session','emg'});

            result = scifor.for_each(@(emg) emg, ...
                struct('emg', rows), ...
                subject=[1 2], session=["pre" "post"]);
            tc.verifyEqual(height(result), 4);
        end

        function test_constant_table_passed_unchanged(tc)
        %   Table without schema key columns is passed unchanged every iteration.
            scifor.set_schema(["subject", "session"]);

            coeffs = table([10], [100], 'VariableNames', {'freq_low','freq_high'});

            result = scifor.for_each(@(c) c.freq_low + c.freq_high, ...
                struct('c', coeffs), ...
                subject=[1 2], session=["pre"]);
            tc.verifyEqual(height(result), 2);
            tc.verifyEqual(result.output, [110; 110]);
        end

        function test_per_combo_single_value_extracted(tc)
        %   1 matching row, 1 data column -> scalar extracted.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});

            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                subject=[1 2]);
            tc.verifyEqual(result.output, [10.0; 20.0]);
        end

        function test_per_combo_multiple_rows_passed_as_vector(tc)
        %   Multiple matching rows, single data column -> numeric vector.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1.0;2.0;3.0;4.0], ...
                'VariableNames', {'subject','emg'});

            result = scifor.for_each(@(data) length(data), ...
                struct('data', tbl), ...
                subject=[1 2]);
            tc.verifyEqual(result.output, [2; 2]);

            result = scifor.for_each(@(data) isnumeric(data), ...
                struct('data', tbl), ...
                subject=[1 2]);
            tc.verifyEqual(result.output, [true; true]);
        end

        function test_per_combo_multiple_data_cols_passed_as_table(tc)
        %   Multiple data columns -> table with constant schema cols removed.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;1;2], [1.0;2.0;3.0;4.0], ...
                'VariableNames', {'subject','trial','emg'});

            % subject is constant per combo -> dropped; trial+emg remain as table
            result = scifor.for_each(@(data) istable(data), ...
                struct('data', tbl), ...
                subject=[1 2]);
            tc.verifyEqual(result.output, [true; true]);

            result = scifor.for_each(@(data) width(data), ...
                struct('data', tbl), ...
                subject=[1 2]);
            tc.verifyEqual(result.output, [2; 2]);
        end
    end

    % =====================================================================
    % as_table=
    % =====================================================================

    methods (Test)
        function test_as_table_true_keeps_schema_cols(tc)
        %   as_table=true keeps schema key columns in the passed table.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});

            % Return 1 if 'subject' column is present, 0 otherwise
            result = scifor.for_each( ...
                @(x) double(ismember('subject', x.Properties.VariableNames)), ...
                struct('x', tbl), ...
                as_table=true, subject=[1 2]);
            tc.verifyEqual(result.output, [1; 1]);
        end

        function test_as_table_false_with_one_column_is_not_table(tc)
        %   Default (as_table=false): schema key columns dropped, scalar extracted.
        %   Non-table result → table with metadata + 'output' column.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});

            % With as_table=false (default), 1 row + 1 data col -> scalar
            result = scifor.for_each(@(x) x + 2, ...
                struct('x', tbl), ...
                subject=[]);
            tc.verifyEqual(result.output, [12.0; 22.0]);
            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
        end

        function test_as_table_false_with_multiple_data_columns_is_table(tc)
        %   as_table=false: multiple data columns -> table with constant schema cols removed.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], [20.0;30.0], 'VariableNames', {'subject','value1','value2'});

            % Multiple data columns -> table (not scalar or numeric array)
            result = scifor.for_each(@(x) height(x), ...
                struct('x', tbl), ...
                subject=[]);
            tc.verifyEqual(result.output, [1; 1]);
        end

        function test_as_table_specific_inputs(tc)
        %   as_table=["data"] keeps table for 'data' but extracts scalar for 'val'.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            tbl2 = table([1;2], [5.0;6.0], 'VariableNames', {'subject','score'});

            % 'data' has as_table -> fn receives table; 'val' does not -> receives scalar
            % Return: 10*istable(data) + isnumeric(val)&&isscalar(val)
            result = scifor.for_each( ...
                @(data, val) double(istable(data)) * 10 + double(isnumeric(val) && isscalar(val)), ...
                struct('data', tbl1, 'val', tbl2), ...
                as_table=["data"], subject=[1]);
            tc.verifyEqual(result.output, 11);
        end

        function test_as_table_true_multi_row(tc)
        %   as_table=true with multi-row result keeps all rows and schema cols.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], ...
                'VariableNames', {'subject','trial'});

            % Return height of the received table
            result = scifor.for_each(@(x) height(x), ...
                struct('x', tbl), ...
                as_table=true, subject=[1]);
            tc.verifyEqual(result.output, 2);

            % Also verify schema column is present
            result2 = scifor.for_each( ...
                @(x) double(ismember('subject', x.Properties.VariableNames)), ...
                struct('x', tbl), ...
                as_table=true, subject=[1]);
            tc.verifyEqual(result2.output, 1);
        end
    end

    % =====================================================================
    % scifor.Fixed
    % =====================================================================

    methods (Test)
        function test_fixed_table_overrides_metadata(tc)
        %   Fixed(tbl, session="pre") always filters with session="pre".
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [1.1;1.2;2.1;2.2], ...
                'VariableNames', {'subject','session','emg'});

            % baseline is always session=pre, current varies
            result = scifor.for_each(@(baseline, current) baseline, ...
                struct('baseline', scifor.Fixed(tbl, session="pre"), ...
                       'current', tbl), ...
                subject=[1], session=["pre" "post"]);

            tc.verifyEqual(height(result), 2);
            % Baseline should always be session=pre value (1.1 for subject=1)
            tc.verifyEqual(result.output, [1.1; 1.1], 'AbsTol', 1e-10);
        end

        function test_fixed_constructor(tc)
        %   Fixed constructor stores data and fixed_metadata.
            tbl = table([1;2], 'VariableNames', {'x'});
            f = scifor.Fixed(tbl, session="BL", trial=1);
            tc.verifyEqual(f.data, tbl);
            tc.verifyEqual(f.fixed_metadata.session, "BL");
            tc.verifyEqual(f.fixed_metadata.trial, 1);
        end
    end

    % =====================================================================
    % scifor.Merge
    % =====================================================================

    methods (Test)
        function test_merge_two_tables(tc)
        %   Merge combines two tables column-wise per combo.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10.0;20.0], 'VariableNames', {'subject','force'});
            tbl2 = table([1;2], [0.1;0.2], 'VariableNames', {'subject','emg'});

            % Check that both 'force' and 'emg' columns are present
            result = scifor.for_each( ...
                @(combined) double(ismember('force', combined.Properties.VariableNames) ...
                    && ismember('emg', combined.Properties.VariableNames)), ...
                struct('combined', scifor.Merge(tbl1, tbl2)), ...
                subject=[1 2]);

            tc.verifyEqual(result.output, [1; 1]);
        end

        function test_merge_constructor_requires_two(tc)
        %   Merge requires at least 2 inputs.
            tbl = table([1], 'VariableNames', {'x'});
            tc.verifyError(@() scifor.Merge(tbl), 'scifor:Merge');
        end

        function test_merge_no_nesting(tc)
        %   Cannot nest Merge inside Merge.
            tbl1 = table([1], 'VariableNames', {'x'});
            tbl2 = table([1], 'VariableNames', {'y'});
            m = scifor.Merge(tbl1, tbl2);
            tc.verifyError(@() scifor.Merge(m, tbl1), 'scifor:Merge');
        end

        function test_merge_with_fixed(tc)
        %   Merge with Fixed constituent.
            scifor.set_schema(["subject", "session"]);

            tbl1 = table([1;1;2;2], ["pre";"post";"pre";"post"], [10;11;20;21], ...
                'VariableNames', {'subject','session','force'});
            tbl2 = table([1;1;2;2], ["pre";"post";"pre";"post"], [0.1;0.2;0.3;0.4], ...
                'VariableNames', {'subject','session','emg'});

            % Force always comes from session=pre (Fixed), emg varies
            result = scifor.for_each(@(combined) combined.force, ...
                struct('combined', scifor.Merge( ...
                    scifor.Fixed(tbl1, session="pre"), tbl2)), ...
                subject=[1], session=["pre" "post"]);

            tc.verifyEqual(height(result), 2);
            % Force should always come from session=pre (value=10 for subject=1)
            tc.verifyEqual(result.output, [10; 10]);
        end
    end

    % =====================================================================
    % scifor.ColumnSelection
    % =====================================================================

    methods (Test)
        function test_column_selection_single(tc)
        %   ColumnSelection with 1 column returns column values.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [1.5;2.5], [10;20], ...
                'VariableNames', {'subject','speed','force'});

            result = scifor.for_each(@(s) s, ...
                struct('s', scifor.ColumnSelection(tbl, "speed")), ...
                subject=[1 2]);

            tc.verifyEqual(result.output, [1.5; 2.5]);
        end

        function test_column_selection_multiple(tc)
        %   ColumnSelection with multiple columns returns sub-table.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [1.0;2.0], [3.0;4.0], [5.0;6.0], ...
                'VariableNames', {'subject','a','b','c'});

            % Return 1 if result is a 2-column table
            result = scifor.for_each( ...
                @(data) double(istable(data) && width(data) == 2), ...
                struct('data', scifor.ColumnSelection(tbl, ["a" "b"])), ...
                subject=[1 2]);

            tc.verifyEqual(result.output, [1; 1]);
        end

        function test_column_selection_constructor(tc)
        %   ColumnSelection stores data and columns.
            tbl = table([1;2], [3;4], 'VariableNames', {'x','y'});
            cs = scifor.ColumnSelection(tbl, ["x" "y"]);
            tc.verifyEqual(cs.data, tbl);
            tc.verifyEqual(cs.columns, ["x" "y"]);
        end
    end

    % =====================================================================
    % Empty-list resolution from table columns
    % =====================================================================

    methods (Test)
        function test_empty_list_resolved_from_table(tc)
        %   [] resolved by scanning table inputs for distinct values.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;2;3], ...
                ["pre";"pre";"pre"], ...
                [1.1;2.1;3.1], ...
                'VariableNames', {'subject','session','emg'});

            result = scifor.for_each(@(emg) emg, ...
                struct('emg', tbl), ...
                subject=[], session=["pre"]);
            tc.verifyEqual(height(result), 3);
        end

        function test_empty_list_no_table_raises(tc)
        %   [] without a table input raises an error.
            scifor.set_schema(["subject"]);
            tc.verifyError(@() scifor.for_each( ...
                @() 0, struct(), subject=[]), ...
                'scifor:for_each');
        end
    end

    % =====================================================================
    % Constant inputs
    % =====================================================================

    methods (Test)
        function test_constant_scalar_input(tc)
        %   Non-table scalars are passed unchanged.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x, alpha) x * alpha, ...
                struct('x', tbl, 'alpha', 0.5), ...
                subject=[1 2]);
            tc.verifyEqual(result.output, [5.0; 10.0]);
        end
    end

    % =====================================================================
    % Return table structure
    % =====================================================================

    methods (Test)
        function test_result_table_has_metadata_columns(tc)
        %   Result table has metadata columns.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1], ["pre";"post"], [1.0;2.0], ...
                'VariableNames', {'subject','session','emg'});

            result = scifor.for_each(@(emg) emg * 2, ...
                struct('emg', tbl), ...
                subject=[1], session=["pre" "post"]);
            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
            tc.verifyTrue(ismember('session', result.Properties.VariableNames));
        end

        function test_result_table_default_output_name(tc)
        %   Default output column is 'output'.
            scifor.set_schema(["subject"]);

            result = scifor.for_each(@() 42, ...
                struct(), ...
                subject=[1 2]);
            tc.verifyTrue(ismember('output', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [42; 42]);
        end

        function test_result_table_uses_output_names(tc)
        %   output_names= names the output columns.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x * 2, ...
                struct('x', tbl), ...
                output_names={"doubled_value"}, ...
                subject=[1 2]);
            tc.verifyTrue(ismember('doubled_value', result.Properties.VariableNames));
            tc.verifyEqual(result.doubled_value, [20.0; 40.0]);
        end

        function test_result_table_auto_output_names(tc)
        %   output_names=3 returns 3 separate tables, each with 'output' column.
            scifor.set_schema(["subject"]);

            [r1, r2, r3] = scifor.for_each(@() deal(1, 2, 3), ...
                struct(), ...
                output_names=3, ...
                subject=[1]);
            tc.verifyTrue(ismember('output', r1.Properties.VariableNames));
            tc.verifyEqual(r1.output, 1);
            tc.verifyTrue(ismember('output', r2.Properties.VariableNames));
            tc.verifyEqual(r2.output, 2);
            tc.verifyTrue(ismember('output', r3.Properties.VariableNames));
            tc.verifyEqual(r3.output, 3);
        end

        function test_all_skipped_returns_empty_table(tc)
        %   When all iterations fail, result is an empty table.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [1.0;2.0], 'VariableNames', {'subject','value'});

            result = scifor.for_each(@(x) error('always'), ...
                struct('x', tbl), ...
                subject=[1 2]);
            tc.verifyTrue(istable(result));
            tc.verifyEqual(height(result), 0);
        end

        function test_flatten_mode_table_outputs(tc)
        %   When fn returns tables, metadata is replicated per row (flatten mode).
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([10.0;20.0;30.0], 'VariableNames', {'val'}), ...
                struct(), ...
                subject=[1 2]);
            % 2 subjects * 3 rows each = 6
            tc.verifyEqual(height(result), 6);
            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
            tc.verifyTrue(ismember('val', result.Properties.VariableNames));
        end
    end

    % =====================================================================
    % output_names with multiple outputs
    % =====================================================================

    methods (Test)
        function test_multiple_outputs_with_names(tc)
        %   Multiple outputs with output_names → separate tables.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            [r1, r2] = scifor.for_each(@(x) deal(x*2, x*3), ...
                struct('x', tbl), ...
                output_names={"doubled","tripled"}, ...
                subject=[1 2]);
            tc.verifyTrue(ismember('doubled', r1.Properties.VariableNames));
            tc.verifyEqual(r1.doubled, [20.0; 40.0]);
            tc.verifyTrue(ismember('tripled', r2.Properties.VariableNames));
            tc.verifyEqual(r2.tripled, [30.0; 60.0]);
        end
    end

    % =====================================================================
    % distribute=true
    % =====================================================================

    methods (Test)
        function test_distribute_requires_schema(tc)
        %   distribute=true with no schema raises error.
            tc.verifyError(@() scifor.for_each( ...
                @() [1 2 3], struct(), ...
                distribute=true, subject=[1]), ...
                'scifor:for_each');
        end

        function test_distribute_splits_into_result_table(tc)
        %   distribute=true splits output and expands result table rows.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() [10.0; 20.0; 30.0], ...
                struct(), ...
                distribute=true, ...
                subject=[1]);
            % 3 pieces with trial=1,2,3
            tc.verifyEqual(height(result), 3);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
        end
    end

    % =====================================================================
    % dry_run
    % =====================================================================

    methods (Test)
        function test_dry_run_returns_empty(tc)
        %   dry_run=true returns [].
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [1.0;2.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                dry_run=true, subject=[1 2]);
            tc.verifyTrue(isempty(result));
        end
    end

    % =====================================================================
    % where= with Col filters
    % =====================================================================

    methods (Test)
        function test_where_col_filter(tc)
        %   where= filters table rows after combo filtering.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','value'});

            % as_table=true so fn receives table; return its height
            result = scifor.for_each(@(data) height(data), ...
                struct('data', tbl), ...
                where=scifor.Col("speed") > 1.0, ...
                as_table=true, subject=[1]);

            tc.verifyEqual(result.output, 2);  % speed > 1.0: rows with 1.5, 2.5
        end
    end

    % =====================================================================
    % Error handling
    % =====================================================================

    methods (Test)
        function test_function_error_skips(tc)
        %   Function errors skip the iteration gracefully.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});

            % error_if_ten errors when x==10, succeeds for x==20
            result = scifor.for_each(@error_if_ten, ...
                struct('x', tbl), ...
                subject=[1 2]);
            % Only subject=2 should succeed
            tc.verifyEqual(height(result), 1);
        end
    end

    % =====================================================================
    % _all_combos (pre-built combo list)
    % =====================================================================

    methods (Test)
        function test_all_combos_prebuilt(tc)
        %   _all_combos bypasses cartesian product.
            scifor.set_schema(["subject"]);

            tbl = table([1;2;3], [10;20;30], 'VariableNames', {'subject','value'});
            combos = {struct('subject', 1), struct('subject', 3)};

            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                '_all_combos', combos);
            % Only subject=1 and subject=3
            tc.verifyEqual(height(result), 2);
            tc.verifyEqual(sort(result.output), [10; 30]);
        end
    end

end

% =========================================================================
% Local helper functions (accessible from classdef methods above)
% =========================================================================

function out = error_if_ten(x)
%ERROR_IF_TEN  Helper that errors when x==10, returns x otherwise.
    if x == 10
        error('test:bad', 'bad');
    end
    out = x;
end
