classdef TestSciforForEachCategorical < matlab.unittest.TestCase
%TESTSCIFORFOREACHCATEGORICAL  Tests for the categorical option of scifor.for_each.

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.set_schema(string.empty(1, 0));
        end
    end

    % =====================================================================
    % A. Default behavior (categorical=false)
    % =====================================================================

    methods (Test)

        function test_default_numeric_metadata_is_double(tc)
        %   By default, numeric metadata columns stay as double.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[1 2]);

            tc.verifyFalse(iscategorical(result.subject));
            tc.verifyTrue(isnumeric(result.subject));
        end

        function test_default_string_metadata_is_string(tc)
        %   By default, string metadata columns stay as string.
            scifor.set_schema(["group"]);

            result = scifor.for_each(@() 1, struct(), group=["A" "B"]);

            tc.verifyFalse(iscategorical(result.group));
            tc.verifyTrue(isstring(result.group));
        end

        function test_categorical_false_same_as_default(tc)
        %   Explicit categorical=false behaves same as default.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[1 2], categorical=false);

            tc.verifyFalse(iscategorical(result.subject));
        end

    end

    % =====================================================================
    % B. categorical=true with scalar outputs
    % =====================================================================

    methods (Test)

        function test_categorical_numeric_metadata(tc)
        %   Numeric metadata becomes categorical.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x * 2, ...
                struct('x', tbl), subject=[1 2], categorical=true);

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyEqual(result.output, [20; 40]);
        end

        function test_categorical_string_metadata(tc)
        %   String metadata becomes categorical.
            scifor.set_schema(["group"]);

            result = scifor.for_each(@() 42, struct(), ...
                group=["ctrl" "exp"], categorical=true);

            tc.verifyTrue(iscategorical(result.group));
            tc.verifyEqual(categories(result.group), {'ctrl'; 'exp'});
        end

        function test_categorical_two_metadata_keys(tc)
        %   Both metadata columns become categorical.
            scifor.set_schema(["subject", "session"]);

            tbl = table([1;1;2;2], ["A";"B";"A";"B"], [10;20;30;40], ...
                'VariableNames', ["subject","session","value"]);
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), ...
                subject=[1 2], session=["A" "B"], categorical=true);

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyTrue(iscategorical(result.session));
            tc.verifyEqual(height(result), 4);
        end

        function test_categorical_does_not_affect_output_column(tc)
        %   The 'output' data column should NOT become categorical.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[1 2], categorical=true);

            tc.verifyFalse(iscategorical(result.output));
        end

    end

    % =====================================================================
    % C. categorical=true with table outputs
    % =====================================================================

    methods (Test)

        function test_categorical_table_output(tc)
        %   Table output with categorical metadata: metadata is categorical,
        %   data columns are not.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([1.0; 2.0], ["a"; "b"], 'VariableNames', {'num','str'}), ...
                struct(), subject=[1 2], categorical=true);

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyFalse(iscategorical(result.num));
            tc.verifyFalse(iscategorical(result.str));
            tc.verifyEqual(height(result), 4);
        end

        function test_categorical_table_output_metadata_replicated(tc)
        %   Replicated metadata in table output is categorical.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() table([10; 20; 30], 'VariableNames', {'val'}), ...
                struct(), subject=[1 2], categorical=true);

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyEqual(height(result), 6);
        end

    end

    % =====================================================================
    % D. categorical=true with multiple outputs
    % =====================================================================

    methods (Test)

        function test_categorical_multiple_outputs(tc)
        %   Both output tables get categorical metadata.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            [r1, r2] = scifor.for_each(@(x) deal(x*2, x+1), ...
                struct('x', tbl), subject=[1 2], categorical=true);

            tc.verifyTrue(iscategorical(r1.subject));
            tc.verifyTrue(iscategorical(r2.subject));
            tc.verifyFalse(iscategorical(r1.output));
            tc.verifyFalse(iscategorical(r2.output));
        end

    end

    % =====================================================================
    % E. Interactions with other features
    % =====================================================================

    methods (Test)

        function test_categorical_with_distribute(tc)
        %   categorical + distribute: metadata columns are categorical.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() [100; 200; 300], struct(), ...
                subject=[1 2], distribute=true, categorical=true);

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyTrue(iscategorical(result.trial));
        end

        function test_categorical_with_output_names(tc)
        %   categorical + outputs: metadata is categorical, named
        %   output column is not.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[1 2], ...
                categorical=true, outputs={"result"});

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyTrue(ismember('result', result.Properties.VariableNames));
            tc.verifyFalse(iscategorical(result.result));
        end

    end

    % =====================================================================
    % F. Schema-key column type round-trip (regression 2026-07-13)
    %
    %   Output metadata columns must come back as EXACTLY the input column's
    %   type: double stays double, string stays string, categorical stays
    %   categorical. Internally a categorical column iterates by canonical
    %   numeric values when every label is a canonical numeric spelling
    %   ("1" -> 1), giving numeric (not lexical) iteration order; labels
    %   that are not canonical spellings (zero-padded "01") iterate verbatim
    %   as strings. Regression: numeric-backed categorical keys came back as
    %   lexically-sorted string columns ("10" < "2"), changing both the
    %   schema-key identity and the column type.
    % =====================================================================

    methods (Test)

        function test_double_key_column_roundtrips_double(tc)
        %   key=[] on a double column: double out, numeric order.
            scifor.set_schema(["subject"]);

            tbl = table([1;2;10], [10;20;30], ...
                'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[]);

            tc.verifyClass(result.subject, 'double');
            tc.verifyEqual(result.subject, [1;2;10]);
            tc.verifyEqual(result.output, [10;20;30]);
        end

        function test_string_key_column_roundtrips_string(tc)
        %   key=[] on a string column: string out, values verbatim — a
        %   real string column is never converted, even when every value
        %   looks numeric.
            scifor.set_schema(["trial"]);

            tbl = table(["1";"2"], [10;20], ...
                'VariableNames', {'trial','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), trial=[]);

            tc.verifyClass(result.trial, 'string');
            tc.verifyEqual(result.trial, ["1";"2"]);
        end

        function test_categorical_numeric_key_column_roundtrips(tc)
        %   key=[] on a numeric-backed categorical column: categorical out,
        %   rows in numeric (not lexical) order, original categories kept.
            scifor.set_schema(["subject"]);

            tbl = table(categorical([1;2;10]), [10;20;30], ...
                'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[]);

            tc.verifyClass(result.subject, 'categorical');
            % lexical iteration would give "1","10","2" / [10;30;20]
            tc.verifyEqual(string(result.subject), ["1";"2";"10"]);
            tc.verifyEqual(result.output, [10;20;30]);
            tc.verifyEqual(categories(result.subject), {'1';'2';'10'});
        end

        function test_categorical_zero_padded_key_column_roundtrips(tc)
        %   Zero-padded labels are not canonical numeric spellings: they
        %   iterate verbatim, and the column comes back categorical.
            scifor.set_schema(["trial"]);

            tbl = table(categorical(["01";"02";"10"]), [1;2;3], ...
                'VariableNames', {'trial','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), trial=[]);

            tc.verifyClass(result.trial, 'categorical');
            tc.verifyEqual(string(result.trial), ["01";"02";"10"]);
            tc.verifyEqual(result.output, [1;2;3]);
        end

        function test_categorical_mixed_padding_key_column_roundtrips(tc)
        %   "1" and "01" are distinct identities; one non-canonical label
        %   keeps the whole key verbatim (no partial numeric recovery).
            scifor.set_schema(["trial"]);

            tbl = table(categorical(["1";"01"]), [1;2], ...
                'VariableNames', {'trial','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), trial=[]);

            tc.verifyClass(result.trial, 'categorical');
            tc.verifyEqual(height(result), 2);
            tc.verifyEqual(sort(string(result.trial)), ["01";"1"]);
        end

        function test_categorical_ordinal_key_column_roundtrips(tc)
        %   Ordinality and category order survive the round trip.
            scifor.set_schema(["phase"]);

            phase = categorical(["pre";"post"], ["pre" "post"], 'Ordinal', true);
            tbl = table(phase, [1;2], 'VariableNames', {'phase','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), phase=[]);

            tc.verifyClass(result.phase, 'categorical');
            tc.verifyTrue(isordinal(result.phase));
            tc.verifyEqual(categories(result.phase), {'pre'; 'post'});
        end

        function test_explicit_values_with_categorical_column(tc)
        %   Explicit numeric iterables still filter a categorical column
        %   correctly, and the output column type follows the input column.
            scifor.set_schema(["subject"]);

            tbl = table(categorical([1;2]), [10;20], ...
                'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[1 2]);

            tc.verifyClass(result.subject, 'categorical');
            tc.verifyEqual(string(result.subject), ["1";"2"]);
            tc.verifyEqual(result.output, [10;20]);
        end

        function test_explicit_values_without_table_keep_own_type(tc)
        %   A key with no input table column keeps the iterable's own type
        %   (no captured type to restore).
            scifor.set_schema(["group"]);

            result = scifor.for_each(@() 42, struct(), group=["A" "B"]);

            tc.verifyClass(result.group, 'string');
        end

        function test_conflicting_key_column_types_do_not_error(tc)
        %   Two inputs disagreeing on a key's column type: no error; the
        %   column is left at the internal canonical type (warn logged).
            scifor.set_schema(["subject"]);

            t1 = table([1;2], [10;20], 'VariableNames', {'subject','a'});
            t2 = table(categorical([1;2]), [30;40], ...
                'VariableNames', {'subject','b'});
            result = scifor.for_each(@(x, y) x + y, ...
                struct('x', t1, 'y', t2), subject=[]);

            tc.verifyEqual(height(result), 2);
            tc.verifyEqual(result.output, [40; 60]);
        end

        function test_categorical_output_roundtrips_categorical(tc)
        %   categorical=true output fed back into for_each: the key comes
        %   back categorical (matching the fed-in column), iterated in
        %   numeric order.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            r1 = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[1 2], categorical=true);
            tc.verifyClass(r1.subject, 'categorical');

            r2 = scifor.for_each(@(x) x + 1, ...
                struct('x', r1), subject=[]);

            tc.verifyClass(r2.subject, 'categorical');
            tc.verifyEqual(string(r2.subject), ["1";"2"]);
            tc.verifyEqual(r2.output, [11;21]);
        end

        function test_categorical_two_keys_nested_struct_output(tc)
        %   Mirrors the field regression: two categorical schema keys,
        %   ColumnSelection input, struct outputs (nested mode). Keys come
        %   back categorical, iterated in numeric order.
            scifor.set_schema(["FileNum", "CycleNum"]);

            tbl = table( ...
                categorical([1;1;2]), categorical([2;10;3]), ...
                {struct('a', 1); struct('a', 2); struct('a', 3)}, ...
                'VariableNames', {'FileNum', 'CycleNum', 'Seg'});
            result = scifor.for_each(@(s) s, ...
                struct('tableIn', scifor.ColumnSelection(tbl, 'Seg')), ...
                outputs={'Seg_Out'}, FileNum=[], CycleNum=[]);

            tc.verifyClass(result.FileNum, 'categorical');
            tc.verifyClass(result.CycleNum, 'categorical');
            % Combos iterate CycleNum in numeric order [2 3 10]; only the
            % three (FileNum, CycleNum) pairs with data produce rows.
            tc.verifyEqual(string(result.FileNum), ["1";"1";"2"]);
            tc.verifyEqual(string(result.CycleNum), ["2";"10";"3"]);
            tc.verifyEqual([result.Seg_Out.a]', [1;2;3]);
        end

    end
end
