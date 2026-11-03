classdef TestSciforForEachPlan < matlab.unittest.TestCase
%TESTSCIFORFOREACHPLAN  scifor.for_each is planned by Python (P5 stage 3, M3).
%
%   +scifor/for_each.m describes its inputs to py.scifor.matlab_loop.plan,
%   slices each table by the plan's row indices, calls the function, and
%   reports back to assemble / close. Guards:
%   - every plan is closed (no token leaks), after a run and after an error;
%   - scidb's shape: _all_combos / _row_selection as Python objects;
%   - a cell of structs as _all_combos still works (direct callers);
%   - a combo whose PathInput matches no file fails alone (Python's rule:
%     a per-combo failure, not an aborted run);
%   - text and numeric key values match MATLAB's rule (numeric column,
%     text value);
%   - dry_run returns [] and runs nothing.

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function setSchema(~)
            scifor.set_schema(["subject", "trial"]);
        end
    end

    methods (TestMethodTeardown)
        function clearSchema(~)
            scifor.set_schema(string.empty);
        end
    end

    methods (Test)
        function test_plans_are_closed(tc)
            tbl = table([1;1;2], [1;2;1], [10;20;30], 'VariableNames', {'subject', 'trial', 'value'});
            result = scifor.for_each(@(x) x * 2, struct('x', tbl), subject=[1 2], trial=[1 2]);
            tc.verifyEqual(height(result), 3);  % subject=2, trial=2 has no data
            tc.verifyEqual(sort(result.output), [20; 40; 60]);
            tc.verifyEqual(double(py.len(py.scifor.matlab_loop.open_plans())), 0);
        end

        function test_plan_closed_after_fatal_error(tc)
            tbl = table([1;2], [5;6], 'VariableNames', {'subject', 'value'});
            tc.verifyError(@() scifor.for_each(@(x) table([1;2], 'VariableNames', {'a'}), ...
                struct('x', scifor.ColumnSelection(tbl, "value", true)), subject=[1 2]), ...
                'scifor:for_each:forColumnsBadReturn');
            tc.verifyEqual(double(py.len(py.scifor.matlab_loop.open_plans())), 0);
        end

        function test_python_combos_and_row_selection(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1;1;2], ["a";"b";"c"], [10;20;30], ...
                'VariableNames', {'subject', 'x__record_id', 'value'});
            combos = py.list({py.dict(pyargs('subject', 1)), py.dict(pyargs('subject', 2))});
            sel = py.list({py.dict(pyargs('x', py.list({'b'}))), py.dict()});
            result = scifor.for_each(@(x) x, struct('x', tbl), ...
                '_all_combos', combos, '_row_selection', sel, ...
                '_record_id_column', "x__record_id");
            tc.verifyEqual(height(result), 2);
            tc.verifyEqual(result.output(result.subject == 1), 20);
            tc.verifyEqual(result.output(result.subject == 2), 30);
        end

        function test_cell_of_struct_combos(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1;2;3], [10;20;30], 'VariableNames', {'subject', 'value'});
            result = scifor.for_each(@(x) x, struct('x', tbl), ...
                '_all_combos', {struct('subject', 3)});
            tc.verifyEqual(result.output, 30);
        end

        function test_text_value_matches_numeric_column(tc)
            scifor.set_schema(["subject"]);
            tbl = table([1;2], [10;20], 'VariableNames', {'subject', 'value'});
            result = scifor.for_each(@(x) x, struct('x', tbl), subject=["2"]);
            tc.verifyEqual(result.output, 20);
        end

        function test_missing_file_fails_that_combo_only(tc)
            root = tempname;
            mkdir(fullfile(root, '1'));
            mkdir(fullfile(root, '2'));
            fid = fopen(fullfile(root, '1', 'data.txt'), 'w'); fclose(fid);
            cleanup = onCleanup(@() rmdir(root, 's')); %#ok<NASGU>
            scifor.set_schema(["subject"]);
            pi = scifor.PathInput("{subject}/data\.txt", root_folder=root, regex=true, name="Raw");
            result = scifor.for_each(@(p) strlength(p) > 0, struct('p', pi), subject=[1 2]);
            tc.verifyEqual(height(result), 1);
            tc.verifyEqual(result.subject, 1);
        end

        function test_dry_run_runs_nothing(tc)
            result = scifor.for_each(@(x) error('test:called', 'called'), ...
                struct('x', 1), dry_run=true, subject=[1 2]);
            tc.verifyEmpty(result);
        end
    end
end
