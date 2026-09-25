classdef TestSciforForEachFeatures < matlab.unittest.TestCase
%TESTSCIFORFOREACHFEATURES  Comprehensive tests for scifor.for_each feature interactions.
%
%   Tests covering combinations of where=, distribute=, as_table=,
%   ColumnSelection, Merge, Fixed, and output_names.
%   Each section focuses on feature interactions not covered by the
%   basic TestSciforForEach and TestSciforForEachOutput test files.

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.set_schema(string.empty(1, 0));
        end
    end

    % =====================================================================
    % A. where= (filter interactions)
    % =====================================================================

    methods (Test)

        function test_where_equality_filter(tc)
        %   Col("x") == value with string column.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], ["A";"B";"C"], [10;20;30], ...
                'VariableNames', {'subject','group','value'});

            result = scifor.for_each(@(data) sum(data.value), ...
                struct('data', tbl), ...
                where=scifor.Col("group") == "B", ...
                as_table=true, subject=[1]);

            tc.verifyEqual(result.output, 20);
        end

        function test_where_less_than(tc)
        %   Col("x") < value numeric filter.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1;1], [1.0;2.0;3.0;4.0], [10;20;30;40], ...
                'VariableNames', {'subject','speed','value'});

            result = scifor.for_each(@(data) height(data), ...
                struct('data', tbl), ...
                where=scifor.Col("speed") < 3.0, ...
                as_table=true, subject=[1]);

            tc.verifyEqual(result.output, 2);  % speed 1.0 and 2.0
        end

        function test_where_compound_and(tc)
        %   (Col("x") > a) & (Col("y") == b) compound filter.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1;1], [1.0;2.0;3.0;4.0], ["A";"B";"A";"B"], ...
                'VariableNames', {'subject','speed','group'});

            f = (scifor.Col("speed") > 1.5) & (scifor.Col("group") == "A");
            result = scifor.for_each(@(data) height(data), ...
                struct('data', tbl), ...
                where=f, as_table=true, subject=[1]);

            tc.verifyEqual(result.output, 1);  % only speed=3.0, group=A
        end

        function test_where_compound_or(tc)
        %   (Col("x") == a) | (Col("x") == b) compound filter.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1;1], [10;20;30;40], ...
                'VariableNames', {'subject','value'});

            f = (scifor.Col("value") == 10) | (scifor.Col("value") == 40);
            result = scifor.for_each(@(data) height(data), ...
                struct('data', tbl), ...
                where=f, as_table=true, subject=[1]);

            tc.verifyEqual(result.output, 2);  % rows with 10 and 40
        end

        function test_where_not_filter(tc)
        %   ~(Col("x") == a) negation filter.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], ["A";"B";"C"], [10;20;30], ...
                'VariableNames', {'subject','group','value'});

            f = ~(scifor.Col("group") == "B");
            result = scifor.for_each(@(data) height(data), ...
                struct('data', tbl), ...
                where=f, as_table=true, subject=[1]);

            tc.verifyEqual(result.output, 2);  % A and C
        end

        function test_where_filters_all_rows_skips(tc)
        %   where removes all rows -> iteration skipped.
            scifor.set_schema(["subject"]);

            tbl = table([1;1], [10;20], ...
                'VariableNames', {'subject','value'});

            % Filter that matches nothing
            result = scifor.for_each(@(data) height(data), ...
                struct('data', tbl), ...
                where=scifor.Col("value") > 100, ...
                as_table=true, subject=[1]);

            % Function receives 0-row table -> returns 0
            tc.verifyEqual(result.output, 0);
        end

        function test_where_with_fixed(tc)
        %   where + Fixed input: where applies to Fixed-filtered rows.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;1;1], ["pre";"pre";"post";"post"], ...
                [10;20;30;40], ...
                'VariableNames', {'subject','session','value'});

            % Fixed locks session=pre, where filters value > 15
            result = scifor.for_each(@(baseline) baseline, ...
                struct('baseline', scifor.Fixed(tbl, session="pre")), ...
                where=scifor.Col("value") > 15, ...
                subject=[1], session=["post"]);

            % From session=pre rows (value=10,20), where keeps only value=20
            % 1 row, 1 data col -> scalar extracted
            tc.verifyEqual(result.output, 20);
        end

        function test_where_with_merge(tc)
        %   where + Merge input: where applies to each constituent.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','force'});
            tbl2 = table([1;1;1], [0.5;1.5;2.5], [0.1;0.2;0.3], ...
                'VariableNames', {'subject','speed','emg'});

            % Use ColumnSelection to keep speed from tbl1 only, avoiding duplicate
            result = scifor.for_each(@(combined) height(combined), ...
                struct('combined', scifor.Merge( ...
                    scifor.ColumnSelection(tbl1, ["speed" "force"]), ...
                    scifor.ColumnSelection(tbl2, "emg"))), ...
                where=scifor.Col("speed") > 1.0, ...
                subject=[1]);

            tc.verifyEqual(result.output, 2);  % speed 1.5 and 2.5
        end

    end

    % =====================================================================
    % B. distribute= (deeper coverage)
    % =====================================================================

    methods (Test)

        function test_distribute_matrix_rows(tc)
        %   Matrix output split by rows under distribute.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() [1 2; 3 4; 5 6], struct(), ...
                distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 3);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
            tc.verifyTrue(iscell(result.output));
            tc.verifyEqual(result.output{1}, [1 2]);
            tc.verifyEqual(result.output{2}, [3 4]);
            tc.verifyEqual(result.output{3}, [5 6]);
        end

        function test_distribute_logical_vector(tc)
        %   Logical vector elements distributed individually.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() [true; false; true; false], struct(), ...
                distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 4);
            tc.verifyEqual(result.trial, [1; 2; 3; 4]);
            tc.verifyEqual(result.output, [true; false; true; false]);
        end

        function test_distribute_string_array(tc)
        %   String array elements distributed individually.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() ["alpha" "beta" "gamma"], struct(), ...
                distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 3);
            tc.verifyEqual(result.trial, [1; 2; 3]);
        end

        function test_distribute_table_with_key_column(tc)
        %   Table output that already has the distribute key column uses it.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() table([10;20;30], [100;200;300], 'VariableNames', {'trial','val'}), ...
                struct(), distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 3);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
            tc.verifyTrue(ismember('val', result.Properties.VariableNames));
            % trial column comes from the output table itself
            tc.verifyEqual(result.trial, [10; 20; 30]);
            tc.verifyEqual(result.val, [100; 200; 300]);
        end

        function test_distribute_multiple_combos(tc)
        %   distribute across 2+ subjects.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() [10; 20], struct(), ...
                distribute=true, subject=[1 2]);

            % 2 subjects * 2 pieces = 4 rows
            tc.verifyEqual(height(result), 4);
            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
        end

        function test_distribute_with_where(tc)
        %   distribute + where filter.
            scifor.set_schema(["subject", "trial"]);

            tbl = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','value'});

            % where filters to speed > 1.0 (2 rows), then fn returns vector -> distribute
            result = scifor.for_each(@(data) data, ...
                struct('data', tbl), ...
                where=scifor.Col("speed") > 1.0, ...
                distribute=true, subject=[1]);

            % 2 matching rows -> 2 distributed pieces
            tc.verifyEqual(height(result), 2);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
        end

        function test_distribute_with_output_names(tc)
        %   distribute + named output columns.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() [10.0; 20.0; 30.0], struct(), ...
                distribute=true, output_names={"measurement"}, ...
                subject=[1]);

            tc.verifyEqual(height(result), 3);
            tc.verifyTrue(ismember('measurement', result.Properties.VariableNames));
            tc.verifyEqual(result.measurement, [10.0; 20.0; 30.0]);
        end

    end

    % =====================================================================
    % C. as_table= (interactions)
    % =====================================================================

    methods (Test)

        function test_as_table_with_where_filter(tc)
        %   as_table + where: schema cols kept AND where filter applied.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','value'});

            result = scifor.for_each( ...
                @(data) double(ismember('subject', data.Properties.VariableNames)) ...
                        * 100 + height(data), ...
                struct('data', tbl), ...
                as_table=true, ...
                where=scifor.Col("speed") > 1.0, ...
                subject=[1]);

            % subject present (100) + 2 filtered rows = 102
            tc.verifyEqual(result.output, 102);
        end

        function test_as_table_with_fixed_input(tc)
        %   as_table + Fixed: schema cols kept in Fixed-filtered table.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [10;20;30;40], ...
                'VariableNames', {'subject','session','value'});

            result = scifor.for_each( ...
                @(data) double(ismember('session', data.Properties.VariableNames)) ...
                        * 100 + height(data), ...
                struct('data', scifor.Fixed(tbl, session="pre")), ...
                as_table=true, subject=[1], session=["post"]);

            % as_table keeps session column; Fixed filters to session=pre -> 1 row
            % session present (100) + 1 row = 101
            tc.verifyEqual(result.output, 101);
        end

        function test_as_table_multi_row_multi_column(tc)
        %   Multi-row, multi-column data with as_table=true.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], [1;2;3], [10;20;30], [100;200;300], ...
                'VariableNames', {'subject','trial','force','emg'});

            % Return width*100 + height
            result = scifor.for_each( ...
                @(data) width(data) * 100 + height(data), ...
                struct('data', tbl), ...
                as_table=true, subject=[1]);

            % 4 columns (subject, trial, force, emg) * 100 + 3 rows = 403
            tc.verifyEqual(result.output, 403);
        end

        function test_as_table_mixed_inputs_selective(tc)
        %   as_table=["x"]: x keeps schema cols, y does not.
            scifor.set_schema(["subject"]);

            tbl_x = table([1;2], [10;20], 'VariableNames', {'subject','val'});
            tbl_y = table([1;2], [30;40], 'VariableNames', {'subject','score'});

            % x is a table with schema cols; y is extracted scalar
            result = scifor.for_each( ...
                @(x, y) double(istable(x)) * 10 + double(isnumeric(y) && isscalar(y)), ...
                struct('x', tbl_x, 'y', tbl_y), ...
                as_table=["x"], subject=[1]);

            % x is table (10) + y is scalar (1) = 11
            tc.verifyEqual(result.output, 11);
        end

        function test_as_table_false_multi_data_cols_returns_table(tc)
        %   as_table=false: multiple data columns -> table with schema cols removed.
            scifor.set_schema(["subject"]);

            tbl = table([1;1], [1;2], [10.0;20.0], [100.0;200.0], ...
                'VariableNames', {'subject','trial','force','emg'});

            % Without as_table, multi-data-col -> table (subject removed, rest kept)
            result = scifor.for_each( ...
                @(data) double(istable(data)) * 100 + width(data), ...
                struct('data', tbl), ...
                subject=[1]);

            % Data is a table (100) with 3 columns: trial, force, emg (3)
            tc.verifyEqual(result.output, 103);
        end

        function test_as_table_with_single_column_selection(tc)
        %   as_table=true + single ColumnSelection returns table with
        %   schema cols + selected data column, not a raw vector.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [10;20;30;40], [0.1;0.2;0.3;0.4], ...
                'VariableNames', {'subject','signal','noise'});

            received = [];

            function result = capture(data)
                received = data;
                result = 0;
            end

            scifor.for_each(@capture, ...
                struct('data', scifor.ColumnSelection(tbl, "signal")), ...
                as_table=true, subject=[1 2]);

            % Must be a table (as_table controls this)
            tc.verifyTrue(istable(received), ...
                'as_table=true should produce a table even with column selection');
            % Must have schema column
            tc.verifyTrue(ismember('subject', received.Properties.VariableNames), ...
                'Table should contain subject metadata column');
            % Must have selected data column
            tc.verifyTrue(ismember('signal', received.Properties.VariableNames), ...
                'Table should contain the selected data column');
            % Must NOT have unselected data column
            tc.verifyFalse(ismember('noise', received.Properties.VariableNames), ...
                'Table should NOT contain unselected data columns');
        end

        function test_as_table_with_multi_column_selection(tc)
        %   as_table=true + multi ColumnSelection returns table with
        %   schema cols + selected data columns only.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], [10;20;30;40], [100;200;300;400], ...
                'VariableNames', {'subject','a','b','c'});

            received = [];

            function result = capture(data)
                received = data;
                result = 0;
            end

            scifor.for_each(@capture, ...
                struct('data', scifor.ColumnSelection(tbl, ["a" "b"])), ...
                as_table=true, subject=[1 2]);

            % Must be a table
            tc.verifyTrue(istable(received));
            % Must have schema col + selected cols
            tc.verifyTrue(ismember('subject', received.Properties.VariableNames));
            tc.verifyTrue(ismember('a', received.Properties.VariableNames));
            tc.verifyTrue(ismember('b', received.Properties.VariableNames));
            % Must NOT have unselected col
            tc.verifyFalse(ismember('c', received.Properties.VariableNames));
        end

        function test_as_table_false_column_selection_returns_vector(tc)
        %   as_table=false (default) + single ColumnSelection returns
        %   a plain vector, NOT a table — preserving existing behavior.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [10;20;30;40], [0.1;0.2;0.3;0.4], ...
                'VariableNames', {'subject','signal','noise'});

            received = [];

            function result = capture(data)
                received = data;
                result = 0;
            end

            scifor.for_each(@capture, ...
                struct('data', scifor.ColumnSelection(tbl, "signal")), ...
                subject=[1 2]);

            % Without as_table, single column selection returns a vector
            tc.verifyFalse(istable(received), ...
                'as_table=false + column selection should return a vector, not a table');
            tc.verifyTrue(isnumeric(received), ...
                'as_table=false + column selection should return a numeric vector');
        end

        function test_iterate_default_passes_bare_vectors(tc)
        %   iterate=true without as_table feeds each column as a bare vector.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], [10;20;30;40], ...
                'VariableNames', {'subject','a','b'});

            all_vectors = true;

            function out = colmax(v)
                all_vectors = all_vectors && ~istable(v);
                out = max(v);
            end

            result = scifor.for_each(@colmax, ...
                struct('v', scifor.ColumnSelection(tbl, ["a" "b"], true)), ...
                subject=[1 2]);

            % Each per-column call gets a bare numeric vector, not a table
            tc.verifyTrue(all_vectors, ...
                'iterate without as_table should pass bare vectors');
            % Reassembled 1xN per combo: max of a/b per subject
            tc.verifyEqual(result.a, [2;4]);
            tc.verifyEqual(result.b, [20;40]);
        end

        function test_deferred_colname_resolves_to_current_column(tc)
        %   No-arg scifor.ColName() resolves per-column to the name of the
        %   column currently being iterated by for_columns.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], [10;20;30;40], ...
                'VariableNames', {'subject','a','b'});

            received = strings(0, 1);

            function out = colmax(v, col_name)
                received(end+1,1) = string(col_name); %#ok<AGROW>
                out = max(v);
            end

            result = scifor.for_each(@colmax, ...
                struct('v', scifor.ColumnSelection(tbl, ["a" "b"], true), ...
                       'col_name', scifor.ColName()), ...
                subject=[1 2]);

            % Two combos x two columns; each call sees its current column name.
            tc.verifyEqual(received, ["a";"b";"a";"b"]);
            tc.verifyEqual(result.a, [2;4]);
            tc.verifyEqual(result.b, [20;40]);
        end

        function test_deferred_colname_without_iterate_errors(tc)
        %   No-arg scifor.ColName() with no for_columns input is a hard error.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [3;4], 'VariableNames', {'subject','velocity'});

            function out = fn(t, col_name) %#ok<INUSD>
                out = 0;
            end

            tc.verifyError(@() scifor.for_each(@fn, ...
                struct('t', tbl, 'col_name', scifor.ColName()), ...
                as_table=true, subject=[1 2]), ...
                'scifor:ColName');
        end

        function test_iterate_as_table_passes_table_with_schema_cols(tc)
        %   iterate=true + as_table feeds a table with all schema cols + the
        %   one current column; non-selected data columns are dropped.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], [10;20;30;40], [0.1;0.2;0.3;0.4], ...
                'VariableNames', {'subject','a','b','noise'});

            all_tables = true;
            has_subject = true;
            one_nonschema = true;
            no_noise = true;

            function out = colmax(v)
                all_tables = all_tables && istable(v);
                vn = string(v.Properties.VariableNames);
                has_subject = has_subject && ismember("subject", vn);
                nonschema = setdiff(vn, "subject", 'stable');
                one_nonschema = one_nonschema && (numel(nonschema) == 1);
                no_noise = no_noise && ~ismember("noise", vn);
                out = max(v.(char(nonschema(1))));
            end

            result = scifor.for_each(@colmax, ...
                struct('v', scifor.ColumnSelection(tbl, ["a" "b"], true)), ...
                as_table=true, subject=[1 2]);

            tc.verifyTrue(all_tables, 'as_table iterate should pass tables');
            tc.verifyTrue(has_subject, 'schema column should be present');
            tc.verifyTrue(one_nonschema, 'exactly one non-schema (current) column');
            tc.verifyTrue(no_noise, 'non-selected data columns should be dropped');
            % Reassembled output matches the bare-vector case
            tc.verifyEqual(result.a, [2;4]);
            tc.verifyEqual(result.b, [20;40]);
        end

        function test_iterate_as_table_enables_argmax_label_lookup(tc)
        %   With a label column declared as a schema key, as_table iterate can
        %   map per-column argmax back to that label.
            % intervention is a schema key but is NOT iterated (only subject is)
            scifor.set_schema(["subject", "intervention"]);

            tbl = table([1;1;1;2;2;2], [1;2;3;1;2;3], ...
                [0.5;0.9;0.2;0.1;0.4;0.8], [10;5;8;7;9;3], ...
                'VariableNames', {'subject','intervention','StepLength','Cadence'});

            function best = best_intervention(v)
                nonschema = setdiff(string(v.Properties.VariableNames), ...
                    ["subject" "intervention"], 'stable');
                [~, idx] = max(v.(char(nonschema(1))));
                best = v.intervention(idx);
            end

            result = scifor.for_each(@best_intervention, ...
                struct('v', scifor.ColumnSelection(tbl, ["StepLength" "Cadence"], true)), ...
                as_table=true, subject=[1 2]);

            % subject 1: StepLength best at intervention 2; Cadence best at 1
            % subject 2: StepLength best at intervention 3; Cadence best at 2
            tc.verifyEqual(result.StepLength, [2;3]);
            tc.verifyEqual(result.Cadence, [1;2]);
        end

        function test_iterate_struct_return_expands_to_suffixed_columns(tc)
        %   A struct return per column expands to <col>__<field> columns.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], [10;20;30;40], ...
                'VariableNames', {'subject','a','b'});

            function s = stats(v)
                s = struct('min', min(v), 'max', max(v));
            end

            result = scifor.for_each(@stats, ...
                struct('v', scifor.ColumnSelection(tbl, ["a" "b"], true)), ...
                subject=[1 2]);

            tc.verifyEqual(string(result.Properties.VariableNames), ...
                ["subject" "a__min" "a__max" "b__min" "b__max"]);
            tc.verifyEqual(result.a__min, [1;3]);
            tc.verifyEqual(result.a__max, [2;4]);
            tc.verifyEqual(result.b__min, [10;30]);
            tc.verifyEqual(result.b__max, [20;40]);
        end

        function test_iterate_varying_output_counts_per_column(tc)
        %   Different source columns may return different numbers of outputs.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], [10;20;30;40], ...
                'VariableNames', {'subject','a','b'});

            function s = stats(v)
                if max(v) < 5
                    s = struct('max', max(v));            % 'a' -> one field
                else
                    s = struct('max', max(v), 'min', min(v));  % 'b' -> two fields
                end
            end

            result = scifor.for_each(@stats, ...
                struct('v', scifor.ColumnSelection(tbl, ["a" "b"], true)), ...
                subject=[1 2]);

            tc.verifyEqual(string(result.Properties.VariableNames), ...
                ["subject" "a__max" "b__max" "b__min"]);
            tc.verifyEqual(result.a__max, [2;4]);
            tc.verifyEqual(result.b__max, [20;40]);
            tc.verifyEqual(result.b__min, [10;30]);
        end

        function test_iterate_multi_output_with_as_table_value_and_label(tc)
        %   Motivating case: per column return both the max value and the
        %   identity of the best label (a non-iterated schema key).
            scifor.set_schema(["subject", "intervention"]);

            tbl = table([1;1;1;2;2;2], [1;2;3;1;2;3], ...
                [0.5;0.9;0.2;0.1;0.4;0.8], [10;5;8;7;9;3], ...
                'VariableNames', {'subject','intervention','StepLength','Cadence'});

            function s = best(v)
                nonschema = setdiff(string(v.Properties.VariableNames), ...
                    ["subject" "intervention"], 'stable');
                [mx, idx] = max(v.(char(nonschema(1))));
                s = struct('value', mx, 'best', v.intervention(idx));
            end

            result = scifor.for_each(@best, ...
                struct('v', scifor.ColumnSelection(tbl, ["StepLength" "Cadence"], true)), ...
                as_table=true, subject=[1 2]);

            tc.verifyEqual(result.StepLength__value, [0.9;0.8], 'AbsTol', 1e-10);
            tc.verifyEqual(result.StepLength__best, [2;3]);
            tc.verifyEqual(result.Cadence__value, [10;9], 'AbsTol', 1e-10);
            tc.verifyEqual(result.Cadence__best, [1;2]);
        end

        function test_iterate_duplicate_output_column_raises(tc)
        %   Colliding produced output names raise a clear error.
        %   Column 'a' returning field 'b' -> 'a__b'; column 'a__b' returning a
        %   scalar -> 'a__b'. The two collide.
            scifor.set_schema(["subject"]);

            tbl = table([1;1], [1;2], [3;4], ...
                'VariableNames', {'subject','a','a__b'});

            function out = stats(v)
                col = setdiff(string(v.Properties.VariableNames), "subject", 'stable');
                if col(1) == "a"
                    out = struct('b', max(v.a));      % -> 'a__b'
                else
                    out = max(v.a__b);                % scalar -> 'a__b' (collision)
                end
            end

            tc.verifyError(@() scifor.for_each(@stats, ...
                struct('v', scifor.ColumnSelection(tbl, ["a" "a__b"], true)), ...
                as_table=true, subject=[1]), ...
                'scifor:for_each:forColumnsDuplicate');
        end

        function test_iterate_multirow_table_return_raises(tc)
        %   A multi-row table return is rejected (must collapse to one row).
            scifor.set_schema(["subject"]);

            tbl = table([1;1], [1;2], 'VariableNames', {'subject','a'});

            function out = bad(~)
                out = table([1;2], 'VariableNames', {'x'});
            end

            tc.verifyError(@() scifor.for_each(@bad, ...
                struct('v', scifor.ColumnSelection(tbl, "a", true)), ...
                subject=[1]), ...
                'scifor:for_each:forColumnsBadReturn');
        end

        function test_iterate_all_columns_resolves_from_table(tc)
        %   for_columns with empty columns ([] sentinel) iterates over all
        %   non-schema columns, resolved from the table.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;2;2], [1;2;3;4], [10;20;30;40], ...
                'VariableNames', {'subject','a','b'});

            result = scifor.for_each(@(v) max(v), ...
                struct('v', scifor.ColumnSelection(tbl, [], true)), ...
                subject=[1 2]);

            % Schema key 'subject' excluded; iterates a, b
            tc.verifyEqual(string(result.Properties.VariableNames), ...
                ["subject" "a" "b"]);
            tc.verifyEqual(result.a, [2;4]);
            tc.verifyEqual(result.b, [20;40]);
        end

    end

    % =====================================================================
    % D. ColumnSelection (interactions)
    % =====================================================================

    methods (Test)

        function test_column_selection_with_fixed(tc)
        %   Fixed wrapping ColumnSelection.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [1.1;1.2;2.1;2.2], [10;20;30;40], ...
                'VariableNames', {'subject','session','emg','force'});

            % Fixed locks session=pre, ColumnSelection picks only "emg"
            result = scifor.for_each(@(val) val, ...
                struct('val', scifor.Fixed( ...
                    scifor.ColumnSelection(tbl, "emg"), ...
                    session="pre")), ...
                subject=[1 2], session=["post"]);

            % For subject=1, session fixed to pre -> emg=1.1
            % For subject=2, session fixed to pre -> emg=2.1
            tc.verifyEqual(result.output, [1.1; 2.1], 'AbsTol', 1e-10);
        end

        function test_column_selection_inside_merge(tc)
        %   ColumnSelection as Merge constituent.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10;20], [100;200], ...
                'VariableNames', {'subject','force','extra'});
            tbl2 = table([1;2], [0.1;0.2], ...
                'VariableNames', {'subject','emg'});

            % Merge: ColumnSelection picks only "force" from tbl1, + full tbl2
            result = scifor.for_each( ...
                @(combined) double(ismember('force', combined.Properties.VariableNames)) ...
                          + double(ismember('emg', combined.Properties.VariableNames)) ...
                          - double(ismember('extra', combined.Properties.VariableNames)), ...
                struct('combined', scifor.Merge( ...
                    scifor.ColumnSelection(tbl1, "force"), tbl2)), ...
                subject=[1]);

            % force present (1) + emg present (1) - extra absent (0) = 2
            tc.verifyEqual(result.output, 2);
        end

        function test_column_selection_multi_row(tc)
        %   Multi-row table, single column selection returns a vector.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], [1;2;3], [10;20;30], ...
                'VariableNames', {'subject','trial','value'});

            result = scifor.for_each(@(v) sum(v), ...
                struct('v', scifor.ColumnSelection(tbl, "value")), ...
                subject=[1]);

            tc.verifyEqual(result.output, 60);
        end

        function test_column_selection_missing_column_skips(tc)
        %   Non-existent column -> iteration skipped, empty result.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});

            result = scifor.for_each(@(v) v, ...
                struct('v', scifor.ColumnSelection(tbl, "nonexistent")), ...
                subject=[1]);

            % Missing column caught internally -> iteration skipped -> empty table
            tc.verifyTrue(istable(result));
            tc.verifyEqual(height(result), 0);
        end

        function test_column_selection_with_where(tc)
        %   ColumnSelection + where filter.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1;1], [0.5;1.5;2.5;3.5], [10;20;30;40], ...
                'VariableNames', {'subject','speed','value'});

            % ColumnSelection picks "value"; where filters speed > 2.0
            % But ColumnSelection is applied on the filtered table,
            % so we get values where speed > 2.0
            result = scifor.for_each(@(v) sum(v), ...
                struct('v', scifor.ColumnSelection(tbl, "value")), ...
                where=scifor.Col("speed") > 2.0, ...
                subject=[1]);

            tc.verifyEqual(result.output, 70);  % 30 + 40
        end

    end

    % =====================================================================
    % E. Fixed (interactions)
    % =====================================================================

    methods (Test)

        function test_fixed_multiple_overrides(tc)
        %   Fixed with 2+ override keys.
            scifor.set_schema(["subject", "session", "trial"]);

            tbl = table( ...
                [1;1;1;1], ...
                ["pre";"pre";"post";"post"], ...
                [1;2;1;2], ...
                [10;20;30;40], ...
                'VariableNames', {'subject','session','trial','value'});

            % Fixed locks both session=pre AND trial=1
            result = scifor.for_each(@(baseline) baseline, ...
                struct('baseline', scifor.Fixed(tbl, session="pre", trial=1)), ...
                subject=[1], session=["post"], trial=[2]);

            % Should always get subject=1, session=pre, trial=1 -> value=10
            tc.verifyEqual(result.output, 10);
        end

        function test_fixed_wrapping_column_selection(tc)
        %   Fixed(ColumnSelection(...)): fixed metadata + column extraction.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [10;20;30;40], [100;200;300;400], ...
                'VariableNames', {'subject','session','force','emg'});

            % Fixed locks session=pre, ColumnSelection picks "force"
            result = scifor.for_each(@(f) f, ...
                struct('f', scifor.Fixed( ...
                    scifor.ColumnSelection(tbl, "force"), ...
                    session="pre")), ...
                subject=[1 2], session=["post"]);

            tc.verifyEqual(result.output, [10; 30]);
        end

        function test_fixed_on_constant_table(tc)
        %   Fixed on table without schema cols passes unchanged.
            scifor.set_schema(["subject", "session"]);

            coeffs = table([0.5], [100], 'VariableNames', {'alpha','beta'});

            % coeffs has no schema cols, so Fixed doesn't filter — passes as-is
            result = scifor.for_each(@(c) c.alpha + c.beta, ...
                struct('c', scifor.Fixed(coeffs, session="pre")), ...
                subject=[1], session=["pre" "post"]);

            tc.verifyEqual(result.output, [100.5; 100.5], 'AbsTol', 1e-10);
        end

        function test_fixed_with_as_table(tc)
        %   Fixed + as_table=true: schema cols preserved in Fixed-filtered table.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [10;20;30;40], ...
                'VariableNames', {'subject','session','value'});

            % as_table keeps schema cols; Fixed locks session=pre
            result = scifor.for_each( ...
                @(data) data.session(1), ...
                struct('data', scifor.Fixed(tbl, session="pre")), ...
                as_table=true, subject=[1], session=["post"]);

            % Fixed filters to session=pre, as_table keeps session col
            tc.verifyEqual(result.output, "pre");
        end

    end

    % =====================================================================
    % F. Merge (interactions)
    % =====================================================================

    methods (Test)

        function test_merge_three_tables(tc)
        %   Merge with 3 constituents.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10;20], 'VariableNames', {'subject','force'});
            tbl2 = table([1;2], [0.1;0.2], 'VariableNames', {'subject','emg'});
            tbl3 = table([1;2], [1.5;2.5], 'VariableNames', {'subject','speed'});

            result = scifor.for_each( ...
                @(combined) combined.force + combined.emg + combined.speed, ...
                struct('combined', scifor.Merge(tbl1, tbl2, tbl3)), ...
                subject=[1 2]);

            tc.verifyEqual(result.output, [11.6; 22.7], 'AbsTol', 1e-10);
        end

        function test_merge_broadcast_single_to_multi_row(tc)
        %   1-row table broadcast to match multi-row table in Merge.
            scifor.set_schema(["subject"]);

            tbl_multi = table([1;1;1], [1;2;3], [10;20;30], ...
                'VariableNames', {'subject','trial','value'});
            tbl_single = table([1], [0.5], ...
                'VariableNames', {'subject','alpha'});

            % tbl_single has 1 row (after filtering) -> broadcast to 3 rows
            result = scifor.for_each( ...
                @(combined) sum(combined.value .* combined.alpha), ...
                struct('combined', scifor.Merge(tbl_multi, tbl_single)), ...
                subject=[1]);

            % (10+20+30) * 0.5 = 30
            tc.verifyEqual(result.output, 30.0, 'AbsTol', 1e-10);
        end

        function test_merge_column_conflict_skips(tc)
        %   Duplicate column names in Merge -> iteration skipped, empty result.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            tbl2 = table([1;2], [30;40], 'VariableNames', {'subject','value'});

            result = scifor.for_each( ...
                @(combined) combined.value, ...
                struct('combined', scifor.Merge(tbl1, tbl2)), ...
                subject=[1]);

            % Column conflict caught internally -> iteration skipped -> empty table
            tc.verifyTrue(istable(result));
            tc.verifyEqual(height(result), 0);
        end

        function test_merge_with_column_selection(tc)
        %   Merge(ColumnSelection(...), tbl).
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10;20], [100;200], ...
                'VariableNames', {'subject','force','extra'});
            tbl2 = table([1;2], [0.1;0.2], ...
                'VariableNames', {'subject','emg'});

            % ColumnSelection picks only "force" from tbl1
            result = scifor.for_each( ...
                @(combined) combined.force + combined.emg, ...
                struct('combined', scifor.Merge( ...
                    scifor.ColumnSelection(tbl1, "force"), tbl2)), ...
                subject=[1 2]);

            tc.verifyEqual(result.output, [10.1; 20.2], 'AbsTol', 1e-10);
        end

        function test_merge_fixed_and_column_selection(tc)
        %   Merge(Fixed(tbl), ColumnSelection(tbl2)).
            scifor.set_schema(["subject", "session"]);

            tbl1 = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [10;20;30;40], ...
                'VariableNames', {'subject','session','baseline_force'});
            tbl2 = table([1;1;2;2], ["pre";"post";"pre";"post"], ...
                [0.1;0.2;0.3;0.4], [100;200;300;400], ...
                'VariableNames', {'subject','session','emg','extra'});

            % Fixed(tbl1, session="pre") + ColumnSelection(tbl2, "emg")
            result = scifor.for_each( ...
                @(combined) combined.baseline_force + combined.emg, ...
                struct('combined', scifor.Merge( ...
                    scifor.Fixed(tbl1, session="pre"), ...
                    scifor.ColumnSelection(tbl2, "emg"))), ...
                subject=[1], session=["pre" "post"]);

            % subject=1, session=pre: baseline_force=10 (Fixed->pre), emg=0.1 (current session=pre) -> 10.1
            % subject=1, session=post: baseline_force=10 (Fixed->pre), emg=0.2 (current session=post) -> 10.2
            tc.verifyEqual(result.output, [10.1; 10.2], 'AbsTol', 1e-10);
        end

    end

    % =====================================================================
    % G. Multi-feature combos
    % =====================================================================

    methods (Test)

        function test_where_distribute_fixed(tc)
        %   where + distribute + Fixed: all three combined.
            scifor.set_schema(["subject", "session", "trial"]);

            tbl = table( ...
                [1;1;1;1], ...
                ["pre";"pre";"pre";"pre"], ...
                [10;20;30;40], ...
                'VariableNames', {'subject','session','value'});

            % Fixed locks session=pre, where filters value > 10, distribute splits results
            result = scifor.for_each(@(data) data, ...
                struct('data', scifor.Fixed(tbl, session="pre")), ...
                where=scifor.Col("value") > 10, ...
                distribute=true, ...
                subject=[1], session=["post"]);

            % Fixed->session=pre has 4 rows; where keeps value>10 -> 3 rows
            % extract_data drops schema cols, 1 data col -> vector [20;30;40]
            % distribute splits into 3 scalars
            tc.verifyEqual(height(result), 3);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [20; 30; 40]);
        end

        function test_as_table_merge_where(tc)
        %   as_table + Merge + where: ColumnSelection avoids speed conflict.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','force'});
            tbl2 = table([1;1;1], [0.5;1.5;2.5], [0.1;0.2;0.3], ...
                'VariableNames', {'subject','speed','emg'});

            % Merge with ColumnSelection to avoid duplicate "speed" column;
            % where filters speed > 1.0 on each constituent before merge
            result = scifor.for_each( ...
                @(combined) sum(combined.force) + sum(combined.emg), ...
                struct('combined', scifor.Merge( ...
                    scifor.ColumnSelection(tbl1, ["speed" "force"]), ...
                    scifor.ColumnSelection(tbl2, "emg"))), ...
                where=scifor.Col("speed") > 1.0, ...
                subject=[1]);

            % After where: speed=1.5 (force=20,emg=0.2) and speed=2.5 (force=30,emg=0.3)
            % sum(force)=50, sum(emg)=0.5 -> 50.5
            tc.verifyEqual(result.output, 50.5, 'AbsTol', 1e-10);
        end

        function test_column_selection_fixed_distribute(tc)
        %   ColumnSelection inside Fixed + distribute.
            scifor.set_schema(["subject", "session", "trial"]);

            tbl = table( ...
                [1;1;1;1], ...
                ["pre";"pre";"pre";"pre"], ...
                [1;2;3;4], ...
                [10;20;30;40], ...
                [100;200;300;400], ...
                'VariableNames', {'subject','session','trial','value','extra'});

            % Fixed locks session=pre, ColumnSelection picks "value"
            result = scifor.for_each(@(v) v, ...
                struct('v', scifor.Fixed( ...
                    scifor.ColumnSelection(tbl, "value"), ...
                    session="pre")), ...
                distribute=true, ...
                subject=[1], session=["post"]);

            % ColumnSelection returns vector [10;20;30;40], distribute splits
            tc.verifyEqual(height(result), 4);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [10; 20; 30; 40]);
        end

    end

    % =====================================================================
    % H. PathInput (interactions)
    % =====================================================================

    methods (Test)

        function test_pathinput_as_constant_input(tc)
        %   PathInput resolved per-combo alongside a table input.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            pi = scifor.PathInput("{subject}/data.mat", root_folder="/data", name="{subject}/data.mat");

            % fn receives (data_scalar, resolved_path_string)
            result = scifor.for_each( ...
                @(data, fp) data + strlength(fp), ...
                struct('data', tbl, 'fp', pi), ...
                subject=[1 2]);

            % PathInput resolved per-combo with current metadata
            path_len_1 = strlength(string(fullfile("/data", "1", "data.mat")));
            path_len_2 = strlength(string(fullfile("/data", "2", "data.mat")));
            tc.verifyEqual(result.output, [10 + path_len_1; 20 + path_len_2]);
        end

        function test_pathinput_with_where_filter(tc)
        %   where= applies to table inputs; PathInput is unaffected.
            scifor.set_schema(["subject"]);

            tbl = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','value'});
            pi = scifor.PathInput("{subject}/out.csv", root_folder="/results", name="{subject}/out.csv");

            % where filters table rows; PathInput resolved to string
            result = scifor.for_each( ...
                @(data, fp) sum(data) + double(isstring(fp)), ...
                struct('data', scifor.ColumnSelection(tbl, "value"), 'fp', pi), ...
                where=scifor.Col("speed") > 1.0, ...
                subject=[1]);

            % ColumnSelection extracts "value" column as vector after where
            % After where: speed>1.0 keeps value=[20;30]
            % sum([20;30]) = 50, isstring check = 1 -> 51
            tc.verifyEqual(result.output, 51);
        end

        function test_pathinput_with_distribute(tc)
        %   PathInput as constant + distribute splits function output.
            scifor.set_schema(["subject", "trial"]);

            pi = scifor.PathInput("{subject}/data.mat", root_folder="/data", name="{subject}/data.mat");

            % fn receives PathInput, returns a vector -> distributed
            result = scifor.for_each( ...
                @(fp) [10; 20; 30], ...
                struct('fp', pi), ...
                distribute=true, subject=[1]);

            tc.verifyEqual(height(result), 3);
            tc.verifyTrue(ismember('trial', result.Properties.VariableNames));
            tc.verifyEqual(result.output, [10; 20; 30]);
        end

        function test_pathinput_with_output_names(tc)
        %   PathInput + custom output_names.
            scifor.set_schema(["subject"]);

            pi = scifor.PathInput("{subject}/data.mat", root_folder="/data", name="{subject}/data.mat");

            result = scifor.for_each( ...
                @(fp) strlength(fp), ...
                struct('fp', pi), ...
                output_names={"path_len"}, subject=[1 2]);

            tc.verifyTrue(ismember('path_len', result.Properties.VariableNames));
            tc.verifyEqual(height(result), 2);
        end

        function test_pathinput_multiple_inputs(tc)
        %   PathInput alongside multiple table inputs and a constant.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;2], [10;20], 'VariableNames', {'subject','force'});
            tbl2 = table([1;2], [0.1;0.2], 'VariableNames', {'subject','emg'});
            pi = scifor.PathInput("{subject}/raw.mat", root_folder="/data", name="{subject}/raw.mat");

            % fn receives (force_scalar, emg_scalar, resolved_path, constant)
            result = scifor.for_each( ...
                @(f, e, fp, k) f + e + k + double(isstring(fp)), ...
                struct('f', tbl1, 'e', tbl2, 'fp', pi, 'k', 100), ...
                subject=[1 2]);

            % subject=1: 10 + 0.1 + 100 + 1 = 111.1
            % subject=2: 20 + 0.2 + 100 + 1 = 121.2
            tc.verifyEqual(result.output, [111.1; 121.2], 'AbsTol', 1e-10);
        end

        function test_pathinput_with_merge_and_where(tc)
        %   PathInput resolved + Merge data input + where filter.
            scifor.set_schema(["subject"]);

            tbl1 = table([1;1;1], [0.5;1.5;2.5], [10;20;30], ...
                'VariableNames', {'subject','speed','force'});
            tbl2 = table([1;1;1], [0.5;1.5;2.5], [0.1;0.2;0.3], ...
                'VariableNames', {'subject','speed','emg'});
            pi = scifor.PathInput("{subject}/log.txt", root_folder="/logs", name="{subject}/log.txt");

            result = scifor.for_each( ...
                @(combined, fp) sum(combined.force) + double(isstring(fp)), ...
                struct('combined', scifor.Merge( ...
                    scifor.ColumnSelection(tbl1, ["speed" "force"]), ...
                    scifor.ColumnSelection(tbl2, "emg")), ...
                    'fp', pi), ...
                where=scifor.Col("speed") > 1.0, ...
                subject=[1]);

            % After where: speed=1.5 (force=20) and speed=2.5 (force=30)
            % sum(force) = 50, isstring = 1 -> 51
            tc.verifyEqual(result.output, 51);
        end

        function test_pathinput_resolved_per_combo(tc)
        %   PathInput as sole input resolves template per metadata combo.
        %   This is the primary use case: user passes PathInput with
        %   template placeholders, and for_each resolves the path for
        %   each iteration using the current metadata values.
            scifor.set_schema(["pass"]);

            pi = scifor.PathInput("/data/EMG/{pass}.mat", 'name', "/data/EMG/{pass}.mat");

            result = scifor.for_each( ...
                @(fp) fp, ...
                struct('filePath', pi), ...
                pass=1:3);

            % Each iteration should receive a resolved path string
            tc.verifyEqual(height(result), 3);
            tc.verifyEqual(result.output(1), "/data/EMG/1.mat");
            tc.verifyEqual(result.output(2), "/data/EMG/2.mat");
            tc.verifyEqual(result.output(3), "/data/EMG/3.mat");
        end

    end

end
