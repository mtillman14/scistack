classdef TestSciforForEachNoSchema < matlab.unittest.TestCase
%TESTSCIFORFOREACHNOSCHEMA  Tests for scifor.for_each when no schema is set.
%
%   Verifies that:
%   - With no schema, iteration keys are the source of truth for filtering
%   - struct() cell-array gotcha is handled transparently
%   - ColumnSelection works correctly with no schema
%   - Schema set => schema keys are the source of truth

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.set_schema(string.empty(1, 0));
        end
    end

    % =====================================================================
    % No schema: iteration keys drive filtering
    % =====================================================================

    methods (Test)

        function test_no_schema_single_key_filters(tc)
        %   With no schema set, pass=[] still filters the table per combo.
            tbl = table([1;2], [10;30], ...
                'VariableNames', {'pass','value'});

            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                pass=[1 2]);

            tc.verifyEqual(height(result), 2);
            % Each combo should get the scalar value (1 row, 1 data col)
            tc.verifyEqual(sort(result.output), [10; 30]);
        end

        function test_no_schema_two_keys_filter(tc)
        %   With no schema, both iteration keys filter correctly.
            tbl = table([1;1;2;2], ["A";"B";"A";"B"], [10;20;30;40], ...
                'VariableNames', {'pass','Cycle','value'});

            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                pass=[1 2], Cycle=["A" "B"]);

            tc.verifyEqual(height(result), 4);
            % Each combo => 1 row => scalar extracted
            row_1A = result(result.pass == 1 & result.Cycle == "A", :);
            tc.verifyEqual(row_1A.output, 10);
            row_2B = result(result.pass == 2 & result.Cycle == "B", :);
            tc.verifyEqual(row_2B.output, 40);
        end

        function test_no_schema_empty_list_resolves_and_filters(tc)
        %   With no schema, [] resolves distinct values AND filters per combo.
            tbl = table([1;1;2;2], ["A";"B";"A";"B"], [10;20;30;40], ...
                'VariableNames', {'pass','Cycle','value'});

            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                pass=[], Cycle=[]);

            tc.verifyEqual(height(result), 4);
            row_1A = result(result.pass == 1 & result.Cycle == "A", :);
            tc.verifyEqual(row_1A.output, 10);
        end

        function test_no_schema_column_selection_single_row(tc)
        %   ColumnSelection with no schema: each combo gets 1 value, not all.
            tbl = table([1;1;2;2], ["A";"B";"A";"B"], ...
                {struct('x',1);struct('x',2);struct('x',3);struct('x',4)}, ...
                'VariableNames', {'pass','Cycle','data'});

            result = scifor.for_each(@(d) d.x, ...
                struct('d', scifor.ColumnSelection(tbl, "data")), ...
                pass=[1 2], Cycle=["A" "B"]);

            tc.verifyEqual(height(result), 4);
            row_1A = result(result.pass == 1 & result.Cycle == "A", :);
            tc.verifyEqual(row_1A.output, 1);
            row_2B = result(result.pass == 2 & result.Cycle == "B", :);
            tc.verifyEqual(row_2B.output, 4);
        end

        function test_no_schema_column_selection_with_empty_lists(tc)
        %   ColumnSelection + [] resolution, no schema.
            tbl = table([1;1;2], ["A";"B";"A"], [10;20;30], ...
                'VariableNames', {'pass','Cycle','value'});

            result = scifor.for_each(@(v) v * 2, ...
                struct('v', scifor.ColumnSelection(tbl, "value")), ...
                pass=[], Cycle=[]);

            tc.verifyEqual(height(result), 3);
            row_1A = result(result.pass == 1 & result.Cycle == "A", :);
            tc.verifyEqual(row_1A.output, 20);
        end

        function test_no_schema_constant_input_unaffected(tc)
        %   Constant (non-table) inputs still work with no schema.
            tbl = table([1;2], [10;20], 'VariableNames', {'pass','value'});

            result = scifor.for_each(@(x, alpha) x * alpha, ...
                struct('x', tbl, 'alpha', 0.5), ...
                pass=[1 2]);

            tc.verifyEqual(result.output, [5.0; 10.0]);
        end

    end

    % =====================================================================
    % struct() cell-array gotcha: collapse_struct_array
    % =====================================================================

    methods (Test)

        function test_struct_array_collapsed_to_scalar(tc)
        %   struct('a', tbl, 'b', {'x','y'}) creates a struct array;
        %   for_each should collapse it and pass 'b' as {'x','y'} cell.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});

            % dataCols is a cell array constant; struct() gotcha creates 1x2 struct array
            result = scifor.for_each(@(x, dataCols) numel(dataCols), ...
                struct('x', tbl, 'dataCols', {'col1', 'col2'}), ...
                subject=[1 2]);

            % dataCols should be the cell {'col1','col2'}, numel = 2
            tc.verifyEqual(result.output, [2; 2]);
        end

        function test_struct_array_collapsed_no_schema(tc)
        %   struct array collapse works even with no schema set.
            tbl = table([1;2], [10;20], 'VariableNames', {'pass','value'});

            result = scifor.for_each(@(x, cols) numel(cols), ...
                struct('x', tbl, 'cols', {'a', 'b', 'c'}), ...
                pass=[1 2]);

            % cols should be {'a','b','c'}, numel = 3
            tc.verifyEqual(result.output, [3; 3]);
        end

        function test_struct_array_table_field_preserved(tc)
        %   When struct array is collapsed, replicated table fields are unwrapped.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});

            % struct('x', tbl, 'names', {'a','b'}) creates 1x2 struct array
            % tbl is replicated => should be unwrapped to single table
            result = scifor.for_each(@(x, names) x, ...
                struct('x', tbl, 'names', {'a', 'b'}), ...
                subject=[1 2]);

            tc.verifyEqual(result.output, [10; 20]);
        end

    end

    % =====================================================================
    % Schema set: schema keys are source of truth for filtering
    % =====================================================================

    methods (Test)

        function test_schema_set_filters_by_schema_keys(tc)
        %   When schema is set, filtering uses schema keys (not just iteration keys).
            scifor.set_schema(["pass", "Cycle"]);

            tbl = table([1;1;2;2], ["A";"B";"A";"B"], [10;20;30;40], ...
                'VariableNames', {'pass','Cycle','value'});

            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                pass=[1], Cycle=["A"]);

            tc.verifyEqual(height(result), 1);
            tc.verifyEqual(result.output, 10);
        end

        function test_schema_set_extra_key_not_in_schema(tc)
        %   Iteration key not in schema: iteration still happens but
        %   filtering uses only schema keys.
            scifor.set_schema(["pass"]);

            % Table has pass + Cycle columns, but only pass is a schema key
            tbl = table([1;1;2;2], ["A";"B";"A";"B"], [10;20;30;40], ...
                'VariableNames', {'pass','Cycle','value'});

            % Iterate over pass and Cycle, but schema only has pass
            % So filtering uses pass only => each (pass,Cycle) combo gets
            % 2 rows (both Cycle values for that pass), not 1
            result = scifor.for_each(@(x) height(x), ...
                struct('x', tbl), ...
                as_table=true, pass=[1], Cycle=["A" "B"]);

            % pass=1 has 2 rows; Cycle is not a schema key so not filtered
            tc.verifyEqual(result.output, [2; 2]);
        end

        function test_no_schema_vs_schema_filtering_difference(tc)
        %   Demonstrates the key difference: no schema uses meta_keys,
        %   schema set uses schema_keys for filtering.
            tbl = table([1;1;1;1], ["A";"A";"B";"B"], [1;2;1;2], [10;20;30;40], ...
                'VariableNames', {'pass','Cycle','trial','value'});

            % No schema: iterate over pass, Cycle, trial => filters on all three
            result_no_schema = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                pass=[1], Cycle=["A"], trial=[1]);
            tc.verifyEqual(height(result_no_schema), 1);
            tc.verifyEqual(result_no_schema.output, 10);

            % Schema with only pass+Cycle: trial not used for filtering
            scifor.set_schema(["pass", "Cycle"]);
            result_schema = scifor.for_each(@(x) height(x), ...
                struct('x', tbl), ...
                as_table=true, pass=[1], Cycle=["A"], trial=[1]);
            % pass=1, Cycle=A has 2 rows (trial=1 and trial=2); trial not filtered
            tc.verifyEqual(result_schema.output, 2);
        end

    end

end
