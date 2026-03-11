classdef TestSciforForEachCategorical < matlab.unittest.TestCase
%TESTSCIFORFOREACHCATEGORICAL  Tests for the categorical option of scifor.for_each.

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.schema_store_(string.empty(1, 0));
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
        %   categorical + output_names: metadata is categorical, named
        %   output column is not.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x, ...
                struct('x', tbl), subject=[1 2], ...
                categorical=true, output_names={"result"});

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyTrue(ismember('result', result.Properties.VariableNames));
            tc.verifyFalse(iscategorical(result.result));
        end

    end
end
