classdef TestMergeCases < matlab.unittest.TestCase
%TESTMERGECASES  scifor.merge_tables against docs/claude/merge-cases.json.
%
%   The same file is read by Python's scifor/tests/test_merge_cases.py
%   (owner: scifor.merge). MATLAB's join runs on MATLAB tables, so this
%   file is what keeps the two in step: a case added there fails here until
%   merge_tables.m adopts it.

    methods (Test)

        function test_shared_merge_cases(tc)
            spec = jsondecode(fileread(fullfile(repo_root(), 'docs', 'claude', ...
                'merge-cases.json')));
            cases = spec.cases;
            if ~iscell(cases)
                cases = num2cell(cases);
            end
            fragments = struct( ...
                'conflict', 'Column name conflict in Merge', ...
                'empty', 'has no rows', ...
                'heights', 'different row counts', ...
                'no_key', 'no shared schema column', ...
                'unmatched', 'Merge would drop rows', ...
                'many_to_many', 'both repeat key values');
            ran = 0;
            for i = 1:numel(cases)
                c = cases{i};
                if isfield(c, 'applies_to') && ~any(strcmp(cellstr(c.applies_to), 'matlab'))
                    continue;
                end
                label = sprintf('case %d %s', c.id, c.name);
                parts = cellfun(@table_of, as_cell(c.parts), 'UniformOutput', false);
                keys = string(cellstr(c.schema_keys));
                args = {};
                if isfield(c, 'drop')
                    args = [args, {'Drop', logical(c.drop(:)')}]; %#ok<AGROW>
                end
                if isfield(c, 'positional')
                    args = [args, {'Positional', logical(c.positional)}]; %#ok<AGROW>
                end
                if isfield(c, 'error')
                    try
                        scifor.merge_tables(parts, keys, args{:});
                        tc.verifyFail(sprintf('%s: expected a %s error', label, c.error));
                    catch err
                        tc.verifyEqual(err.identifier, 'scifor:Merge', label);
                        tc.verifySubstring(err.message, fragments.(c.error), label);
                    end
                else
                    got = scifor.merge_tables(parts, keys, args{:});
                    want = table_of(c.expect);
                    tc.verifyEqual(string(got.Properties.VariableNames), ...
                        string(want.Properties.VariableNames), label);
                    for name = string(want.Properties.VariableNames)
                        tc.verifyEqual(got.(char(name)), want.(char(name)), ...
                            sprintf('%s column %s', label, name));
                    end
                end
                ran = ran + 1;
            end
            tc.verifyGreaterThan(ran, 10);
        end

        function test_dropped_constants_are_reported(tc)
            a = table([1;1], [1;2], [10;20], 'VariableNames', {'subject','trial','force'});
            b = table([1;1], [2;1], [0.2;0.1], 'VariableNames', {'subject','trial','emg'});
            [got, dropped] = scifor.merge_tables({a, b}, ["subject" "trial"], ...
                'Drop', [true true]);
            tc.verifyEqual(got.emg, [0.1; 0.2]);
            tc.verifyEqual(dropped.subject, 1);
            tc.verifyFalse(isfield(dropped, 'trial'));
        end

    end
end


function t = table_of(s)
%TABLE_OF  A case's column object (struct of arrays) as a table: text
%   columns become string arrays, numbers double columns.
    names = fieldnames(s);
    t = table();
    for i = 1:numel(names)
        v = s.(names{i});
        if iscell(v)
            v = string(v);
        end
        t.(names{i}) = reshape(v, [], 1);
    end
end


function c = as_cell(v)
    if iscell(v)
        c = v;
    else
        c = num2cell(v);
    end
end


function root = repo_root()
%REPO_ROOT  The monorepo root (four levels above tests/matlab/scifor).
    root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..', '..');
end
