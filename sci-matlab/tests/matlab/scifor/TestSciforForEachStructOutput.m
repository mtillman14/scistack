classdef TestSciforForEachStructOutput < matlab.unittest.TestCase
%TESTSCIFORFOREACHSTRUCTOUTPUT  Tests for struct array output normalization
%   in scifor.for_each.
%
%   When all iterations return scalar structs with identical fields,
%   the output column should be a struct array rather than a cell array.
%   When structs have mismatched fields, non-scalar structs, or mixed
%   types, the output column falls back to a cell array.

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.schema_store_(string.empty(1, 0));
        end
    end

    % =====================================================================
    % A. Scalar structs with same fields → struct array
    % =====================================================================

    methods (Test)

        function test_multiple_scalar_structs_become_struct_array(tc)
        %   Multiple iterations returning scalar structs with the same
        %   fields produce a struct array column, not a cell array.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() struct('a', 1, 'b', 2), struct(), subject=[1 2 3]);

            tc.verifyTrue(isstruct(result.output));
            tc.verifyFalse(iscell(result.output));
            tc.verifyEqual(numel(result.output), 3);
            tc.verifyEqual(result.output(1).a, 1);
            tc.verifyEqual(result.output(2).b, 2);
        end

        function test_single_scalar_struct_becomes_struct_array(tc)
        %   Even a single iteration produces a 1x1 struct array, not cell.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() struct('x', 42), struct(), subject=[1]);

            tc.verifyTrue(isstruct(result.output));
            tc.verifyFalse(iscell(result.output));
            tc.verifyEqual(result.output.x, 42);
        end

        function test_struct_array_with_varying_values(tc)
        %   Structs have the same fields but different values per combo.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each( ...
                @(v) struct('doubled', v*2, 'halved', v/2), ...
                struct('v', tbl), subject=[1 2]);

            tc.verifyTrue(isstruct(result.output));
            tc.verifyEqual(result.output(1).doubled, 20);
            tc.verifyEqual(result.output(1).halved, 5);
            tc.verifyEqual(result.output(2).doubled, 40);
            tc.verifyEqual(result.output(2).halved, 10);
        end

        function test_struct_array_indexing(tc)
        %   The struct array supports standard MATLAB struct array indexing.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() struct('val', randi(100)), struct(), subject=[1 2 3 4]);

            tc.verifyEqual(numel(result.output), 4);
            % Can index into struct array directly
            tc.verifyTrue(isnumeric(result.output(3).val));
            % Can extract field across all elements
            all_vals = [result.output.val];
            tc.verifyEqual(numel(all_vals), 4);
        end

    end

    % =====================================================================
    % B. Fallback to cell array
    % =====================================================================

    methods (Test)

        function test_mismatched_fields_stay_cell(tc)
        %   Structs with different fields → cell array (cannot form
        %   struct array).
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each( ...
                @(v) make_varying_struct(v), ...
                struct('v', tbl), subject=[1 2]);

            tc.verifyTrue(iscell(result.output));
        end

        function test_non_scalar_struct_stays_cell(tc)
        %   Non-scalar struct (e.g. 1x2 struct array) → cell array.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() [struct('a', 1), struct('a', 2)], ...
                struct(), subject=[1 2]);

            tc.verifyTrue(iscell(result.output));
        end

        function test_mixed_struct_and_numeric_stays_cell(tc)
        %   If some iterations return structs and others return numbers,
        %   the column stays as a cell array.
            scifor.set_schema(["subject"]);

            tbl = table([1;2], [10;20], 'VariableNames', {'subject','value'});
            result = scifor.for_each( ...
                @(v) pick_type(v), ...
                struct('v', tbl), subject=[1 2]);

            tc.verifyTrue(iscell(result.output));
        end

    end

    % =====================================================================
    % C. Struct output with multiple metadata keys
    % =====================================================================

    methods (Test)

        function test_struct_array_two_metadata_keys(tc)
        %   Struct array output works with multiple iterated keys.
            scifor.set_schema(["subject", "session"]);

            result = scifor.for_each( ...
                @() struct('mean', rand, 'std', rand), struct(), ...
                subject=[1 2], session=["A" "B"]);

            tc.verifyTrue(isstruct(result.output));
            tc.verifyEqual(numel(result.output), 4);
            tc.verifyEqual(height(result), 4);
            % Metadata columns present
            tc.verifyTrue(ismember('subject', result.Properties.VariableNames));
            tc.verifyTrue(ismember('session', result.Properties.VariableNames));
        end

    end

    % =====================================================================
    % D. Interactions with other features
    % =====================================================================

    methods (Test)

        function test_struct_array_with_output_names(tc)
        %   Custom output_names applies to struct array column.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() struct('a', 1), struct(), ...
                output_names={"stats"}, subject=[1 2]);

            tc.verifyTrue(ismember('stats', result.Properties.VariableNames));
            tc.verifyTrue(isstruct(result.stats));
            tc.verifyEqual(result.stats(1).a, 1);
        end

        function test_struct_array_with_categorical(tc)
        %   categorical=true converts metadata, struct array unaffected.
            scifor.set_schema(["subject"]);

            result = scifor.for_each( ...
                @() struct('a', 1, 'b', 2), struct(), ...
                subject=[1 2], categorical=true);

            tc.verifyTrue(iscategorical(result.subject));
            tc.verifyTrue(isstruct(result.output));
            tc.verifyFalse(iscell(result.output));
        end

        function test_struct_array_with_multiple_outputs(tc)
        %   Multiple outputs where both are scalar structs → both become
        %   struct arrays.
            scifor.set_schema(["subject"]);

            [r1, r2] = scifor.for_each( ...
                @() deal(struct('x', 1), struct('y', 2)), ...
                struct(), output_names=2, subject=[1 2]);

            tc.verifyTrue(isstruct(r1.output));
            tc.verifyEqual(r1.output(1).x, 1);
            tc.verifyTrue(isstruct(r2.output));
            tc.verifyEqual(r2.output(1).y, 2);
        end

        function test_struct_array_with_distribute(tc)
        %   distribute + struct output: each element distributed, and if
        %   all distributed pieces are scalar structs with same fields,
        %   the column becomes a struct array.
            scifor.set_schema(["subject", "trial"]);

            result = scifor.for_each( ...
                @() {struct('a', 10), struct('a', 20), struct('a', 30)}, ...
                struct(), distribute=true, subject=[1]);

            tc.verifyTrue(isstruct(result.output));
            tc.verifyEqual(numel(result.output), 3);
            tc.verifyEqual(result.output(1).a, 10);
            tc.verifyEqual(result.output(3).a, 30);
        end

    end

end


% =========================================================================
% Helper functions (outside classdef for test use)
% =========================================================================

function s = make_varying_struct(v)
%MAKE_VARYING_STRUCT  Return structs with different fields depending on v.
    if v > 15
        s = struct('a', v, 'b', v*2);
    else
        s = struct('a', v, 'c', v*3);
    end
end

function out = pick_type(v)
%PICK_TYPE  Return a struct for v<=15, numeric otherwise.
    if v > 15
        out = v * 2;
    else
        out = struct('val', v);
    end
end
