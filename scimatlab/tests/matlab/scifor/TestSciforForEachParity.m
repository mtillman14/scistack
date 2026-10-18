classdef TestSciforForEachParity < matlab.unittest.TestCase
%TESTSCIFORFOREACHPARITY  scifor.for_each accepts a scidb.for_each call.
%
%   Mirrors Python scifor/tests/test_outputs_parity.py:
%   - OUTPUTS as the third positional argument (as in scidb.for_each)
%   - scidb-only options (save, db, ...) accepted, logged and ignored
%   - objects in outputs= name their column by column_name / class name,
%     checked against the shared docs/claude/output-column-name-cases.json
%   - locations= (JSON text) narrows the combos via Python's owner

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.set_schema(string.empty(1, 0));
        end
    end

    methods (Test)

        % --- positional OUTPUTS ------------------------------------------

        function test_outputs_as_third_positional(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x) x * 2, struct('x', tbl), ...
                {"doubled"}, subject=[1 2]);
            tc.verifyEqual(result.doubled, [20.0; 40.0]);
        end

        function test_positional_and_named_outputs_agree(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            a = scifor.for_each(@(x) x * 2, struct('x', tbl), {"y"}, subject=[1 2]);
            b = scifor.for_each(@(x) x * 2, struct('x', tbl), outputs={"y"}, subject=[1 2]);
            tc.verifyEqual(a, b);
        end

        % --- scidb-only options --------------------------------------------

        function test_db_only_options_are_ignored(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            baseline = scifor.for_each(@(x) x * 2, struct('x', tbl), ...
                {"y"}, subject=[1 2]);
            result = scifor.for_each(@(x) x * 2, struct('x', tbl), {"y"}, ...
                save=false, db=[], introspect=true, skip_computed=true, ...
                schema_filter=struct(), finalized=true, glue=struct(), ...
                parameter_names=struct(), pipeline='', preload=true, ...
                parallel=false, subject=[1 2]);
            tc.verifyEqual(result, baseline);
            names = ["save" "db" "introspect" "skip_computed" "schema_filter" ...
                "finalized" "glue" "parameter_names" "pipeline" "preload" "parallel"];
            tc.verifyEmpty(intersect(names, string(result.Properties.VariableNames)));
        end

        function test_db_only_options_ignored_through_each_of(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1;2], [10.0;20.0], 'VariableNames', {'subject','value'});
            result = scifor.for_each(@(x, k) x * k, ...
                struct('x', tbl, 'k', scifor.EachOf(2, 3)), {"y"}, ...
                save=true, subject=[1 2]);
            tc.verifyEqual(height(result), 4);
            tc.verifyFalse(ismember('save', result.Properties.VariableNames));
        end

        % --- objects in outputs --------------------------------------------

        function test_object_output_names_column_by_class(tc)
            scifor.set_schema(["subject"]);
            result = scifor.for_each(@() 42, struct(), {OcnPlain()}, subject=[1]);
            tc.verifyEqual(result.OcnPlain, 42);
        end

        function test_column_name_property_renames_column(tc)
            scifor.set_schema(["subject"]);
            result = scifor.for_each(@() 42, struct(), {OcnRenamed()}, subject=[1]);
            tc.verifyTrue(ismember('step_len', result.Properties.VariableNames));
            tc.verifyFalse(ismember('OcnRenamed', result.Properties.VariableNames));
        end

        function test_shared_output_column_name_cases(tc)
            path = fullfile(repo_root(), 'docs', 'claude', ...
                'output-column-name-cases.json');
            spec = jsondecode(fileread(path));
            cases = spec.cases;
            if ~iscell(cases)
                cases = num2cell(cases);
            end
            for i = 1:numel(cases)
                c = cases{i};
                if isfield(c, 'string') && ~isempty(c.string)
                    output = c.string;
                    label = c.string;
                else
                    output = feval(c.class);
                    label = c.class;
                end
                tc.verifyEqual(scifor.output_column_name(output), c.expected, ...
                    sprintf('case %s', label));
            end
        end

        % --- locations= ---------------------------------------------------

        function test_locations_json_narrows_combos(tc)
            scifor.set_schema(["subject", "trial"]);
            sel = '{"include": [[["subject", "1"]], [["subject", "2"], ["trial", "2"]]]}';
            result = scifor.for_each(@() 1, struct(), {"n"}, ...
                subject=[1 2], trial=[1 2], locations=sel);
            got = sortrows([result.subject, result.trial]);
            tc.verifyEqual(got, [1 1; 1 2; 2 2]);
        end

        function test_empty_locations_is_inert(tc)
            scifor.set_schema(["subject"]);
            result = scifor.for_each(@() 1, struct(), {"n"}, ...
                subject=[1 2], locations='');
            tc.verifyEqual(height(result), 2);
        end

    end

end

function root = repo_root()
%REPO_ROOT  The monorepo root (four levels above tests/matlab/scifor).
    root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..', '..');
end
