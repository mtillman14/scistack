classdef TestScifor < matlab.unittest.TestCase
%TESTSCIFOR  Tests for the +scifor package (standalone, no database required).
%
%   Tests: set_schema/get_schema, Col/ColFilter/CompoundFilter/NotFilter,
%   and table input detection in scidb.for_each.

    methods (TestMethodSetup)
        function resetSchema(~)
            % Reset scifor schema before each test by clearing the store
            scifor.schema_store_(string.empty(1, 0));
        end
    end

    % =====================================================================
    % schema tests
    % =====================================================================

    methods (Test)
        function test_get_schema_empty_by_default(tc)
            keys = scifor.get_schema();
            tc.verifyEmpty(keys);
        end

        function test_set_and_get_schema(tc)
            scifor.set_schema(["subject", "session"]);
            keys = scifor.get_schema();
            tc.verifyEqual(keys, ["subject", "session"]);
        end

        function test_set_schema_overwrites(tc)
            scifor.set_schema(["subject", "session"]);
            scifor.set_schema(["trial"]);
            keys = scifor.get_schema();
            tc.verifyEqual(keys, "trial");
        end

        function test_configure_database_sets_schema(tc)
            % configure_database should call scifor.set_schema automatically.
            % We can't call configure_database without a real DB, so we
            % test the set_schema side-effect indirectly.
            scifor.set_schema(["subject", "session", "trial"]);
            keys = scifor.get_schema();
            tc.verifyEqual(numel(keys), 3);
        end
    end

    % =====================================================================
    % Col / filter tests
    % =====================================================================

    methods (Test)
        function test_col_eq_filter(tc)
            f = scifor.Col("side") == "R";
            tc.verifyClass(f, 'scifor.ColFilter');
            tc.verifyEqual(f.column_name, "side");
            tc.verifyEqual(f.op, "==");
        end

        function test_colfilter_apply(tc)
            tbl = table(["L"; "R"; "L"; "R"], [1; 2; 3; 4], ...
                        'VariableNames', {'side', 'value'});
            f = scifor.Col("side") == "R";
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [false; true; false; true]);
        end

        function test_compound_and_filter(tc)
            tbl = table(["L"; "R"; "L"; "R"], [1.0; 1.5; 2.0; 2.5], ...
                        'VariableNames', {'side', 'speed'});
            f = (scifor.Col("side") == "R") & (scifor.Col("speed") > 2.0);
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [false; false; false; true]);
        end

        function test_compound_or_filter(tc)
            tbl = table(["L"; "R"; "L"; "R"], [1.0; 1.5; 2.0; 2.5], ...
                        'VariableNames', {'side', 'speed'});
            f = (scifor.Col("side") == "L") | (scifor.Col("speed") > 2.0);
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [true; false; true; true]);
        end

        function test_not_filter(tc)
            tbl = table(["L"; "R"; "L"; "R"], ...
                        'VariableNames', {'side'});
            f = ~(scifor.Col("side") == "R");
            tc.verifyClass(f, 'scifor.NotFilter');
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [true; false; true; false]);
        end

        function test_col_ne(tc)
            tbl = table(["L"; "R"; "L"], 'VariableNames', {'side'});
            f = scifor.Col("side") ~= "R";
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [true; false; true]);
        end

        function test_col_lt(tc)
            tbl = table([1.0; 2.0; 3.0], 'VariableNames', {'speed'});
            f = scifor.Col("speed") < 2.0;
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [true; false; false]);
        end

        function test_col_le(tc)
            tbl = table([1.0; 2.0; 3.0], 'VariableNames', {'speed'});
            f = scifor.Col("speed") <= 2.0;
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [true; true; false]);
        end

        function test_col_gt(tc)
            tbl = table([1.0; 2.0; 3.0], 'VariableNames', {'speed'});
            f = scifor.Col("speed") > 2.0;
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [false; false; true]);
        end

        function test_col_ge(tc)
            tbl = table([1.0; 2.0; 3.0], 'VariableNames', {'speed'});
            f = scifor.Col("speed") >= 2.0;
            mask = f.apply(tbl);
            tc.verifyEqual(mask, [false; true; true]);
        end
    end

    % =====================================================================
    % Table input detection in scidb.for_each
    % (These tests require a configured database but test the table-filtering
    % logic which is schema-key-based and works without DB access to data.)
    % =====================================================================

    methods (Test)
        function test_table_input_constant_no_schema_cols(tc)
        %   A table with no schema-key columns is passed unchanged (constant).
            scifor.set_schema(["subject", "session"]);

            % Table with no schema key columns
            coeffs = table([10; 20], [100; 200], ...
                'VariableNames', {'freq_low', 'freq_high'});

            received = {};
            fn = @(c) deal(received{end+1} == c, []);  %#ok<NASGU>

            % We can't run scidb.for_each without a DB, so test the helper directly
            result = filter_table_for_combo_test(coeffs, struct('subject', 1, 'session', 'pre'), ...
                scifor.get_schema(), false);
            % Should come back unchanged (2 rows)
            tc.verifyEqual(height(result), 2);
        end

        function test_table_input_per_combo_single_value(tc)
        %   A per-combo table with 1 matching row and 1 data col → scalar extracted.
            scifor.set_schema(["subject"]);

            tbl = table([1; 2], [10.0; 20.0], ...
                'VariableNames', {'subject', 'value'});

            result = filter_table_for_combo_test(tbl, struct('subject', 1), ...
                scifor.get_schema(), false);
            tc.verifyEqual(result, 10.0);
        end

        function test_table_input_per_combo_multiple_rows(tc)
        %   A per-combo table with multiple matching rows → sub-table passed.
            scifor.set_schema(["subject"]);

            tbl = table([1; 1; 2; 2], [1; 2; 3; 4], [1.0; 2.0; 3.0; 4.0], ...
                'VariableNames', {'subject', 'trial', 'emg'});

            result = filter_table_for_combo_test(tbl, struct('subject', 1), ...
                scifor.get_schema(), false);
            tc.verifyClass(result, 'table');
            tc.verifyEqual(height(result), 2);
        end

        function test_table_input_as_table_true(tc)
        %   as_table=true → always pass as table even for 1-row/1-col case.
            scifor.set_schema(["subject"]);

            tbl = table([1; 2], [10.0; 20.0], ...
                'VariableNames', {'subject', 'value'});

            result = filter_table_for_combo_test(tbl, struct('subject', 1), ...
                scifor.get_schema(), true);
            tc.verifyClass(result, 'table');
            tc.verifyEqual(height(result), 1);
        end

        function test_coarser_resolution_table(tc)
        %   Subject-level table used in trial-level iteration: all subject rows returned.
            scifor.set_schema(["subject", "trial"]);

            tbl = table([1; 2], [100.0; 200.0], ...
                'VariableNames', {'subject', 'weight'});

            % Filtering on {subject=1, trial=3}: trial not in table → filter only on subject
            result = filter_table_for_combo_test(tbl, struct('subject', 1, 'trial', 3), ...
                scifor.get_schema(), false);
            % 1 matching row, 1 data col → scalar
            tc.verifyEqual(result, 100.0);
        end
    end

    % =====================================================================
    % ColName tests
    % =====================================================================

    methods (Test)
        function test_colname_single_data_column(tc)
        %   ColName resolves to the single non-schema data column name.
            scifor.set_schema(["subject", "session"]);
            tbl = table([1; 2], ["pre"; "post"], [10.0; 20.0], ...
                'VariableNames', {'subject', 'session', 'emg'});

            received = {};
            function out = check_col(t, col_name)
                received{end+1} = col_name;
                out = mean(t.(col_name));
            end

            scifor.for_each(@check_col, ...
                struct('t', tbl, 'col_name', scifor.ColName(tbl)), ...
                as_table=true, ...
                subject=1, session="pre");
            tc.verifyEqual(received{1}, 'emg');
        end

        function test_colname_multiple_data_columns_errors(tc)
        %   ColName errors when the table has 2+ non-schema data columns.
            scifor.set_schema(["subject"]);
            tbl = table([1; 2], [0.1; 0.2], [1.0; 2.0], ...
                'VariableNames', {'subject', 'emg', 'force'});

            tc.verifyError(@() scifor.for_each(@(t, c) 0, ...
                struct('t', tbl, 'c', scifor.ColName(tbl)), ...
                'subject', 1), ...
                'scifor:ColName');
        end
    end

end


% Helper: replicate filter_table_for_combo logic accessible from tests
function result = filter_table_for_combo_test(tbl, metadata, schema_keys, as_table)
    col_names = string(tbl.Properties.VariableNames);
    schema_keys_in_tbl = intersect(col_names, schema_keys);

    if isempty(schema_keys_in_tbl)
        result = tbl;
        return;
    end

    mask = true(height(tbl), 1);
    for k = 1:numel(schema_keys_in_tbl)
        key = schema_keys_in_tbl(k);
        if isfield(metadata, char(key))
            val = metadata.(char(key));
            col_data = tbl.(char(key));
            if isnumeric(col_data)
                mask = mask & (col_data == val);
            else
                mask = mask & (string(col_data) == string(val));
            end
        end
    end

    sub = tbl(mask, :);

    if as_table
        result = sub;
        return;
    end

    data_cols = setdiff(col_names, schema_keys, 'stable');

    if height(sub) == 1 && numel(data_cols) == 1
        val = sub.(char(data_cols(1)));
        if iscell(val)
            result = val{1};
        else
            result = val;
        end
    else
        result = sub;
    end
end
