classdef TestKeyMatchCases < matlab.unittest.TestCase
%TESTKEYMATCHCASES  Which rows a combo value selects, against
%   docs/claude/key-match-cases.json.
%
%   Owner: scifor.keys (Python). MATLAB's loop is planned in Python, so the
%   rule runs there; this test drives it end to end, through scifor.for_each
%   over a MATLAB table, to prove MATLAB's columns reach the same rule
%   (Python's scifor/tests/test_key_match_cases.py reads the same file).

    methods (TestMethodSetup)
        function resetSchema(~)
            scifor.set_schema(string.empty(1, 0));
        end
    end

    methods (TestMethodTeardown)
        function clearSchema(~)
            scifor.set_schema(string.empty(1, 0));
        end
    end

    methods (Test)

        function test_shared_key_match_cases(tc)
            spec = jsondecode(fileread(fullfile(repo_root(), 'docs', 'claude', ...
                'key-match-cases.json')));
            cases = spec.cases;
            if ~iscell(cases)
                cases = num2cell(cases);
            end
            ran = 0;
            for i = 1:numel(cases)
                c = cases{i};
                if isfield(c, 'applies_to') && ~any(strcmp(cellstr(c.applies_to), 'matlab'))
                    continue;
                end
                label = sprintf('case %d %s', c.id, c.name);
                k = column_of(c.column);
                v = (1:numel(k))';
                tbl = table(k, v, 'VariableNames', {'k', 'v'});
                scifor.set_schema(["k"]);
                result = scifor.for_each(@(x) numel(x) * 1000 + sum(x), ...
                    struct('x', tbl), {"n"}, k=value_of(c.value));
                expect = double(c.expect(:)');
                if isempty(expect)
                    tc.verifyTrue(height(result) == 0 || all(result.n == 0), label);
                else
                    tc.verifyEqual(height(result), 1, label);
                    tc.verifyEqual(result.n, numel(expect) * 1000 + sum(expect + 1), label);
                end
                ran = ran + 1;
            end
            tc.verifyGreaterThan(ran, 10);
        end

    end
end


function k = column_of(col)
    vals = col.values;
    switch col.class
        case {'int', 'float'}
            if iscell(vals)  % a null makes jsondecode return a cell
                vals = cellfun(@null_to_nan, vals);
            end
            k = double(vals(:));
        case 'bool'
            k = logical(vals(:));
        case 'text'
            k = text_of(vals);
        case 'categorical'
            k = categorical(text_of(vals));
        otherwise
            error('TestKeyMatchCases:class', 'unknown column class %s', col.class);
    end
end


function s = text_of(vals)
    if ~iscell(vals)
        vals = num2cell(vals);
    end
    s = strings(numel(vals), 1);
    for i = 1:numel(vals)
        if isempty(vals{i})
            s(i) = missing;
        else
            s(i) = string(vals{i});
        end
    end
end


function x = null_to_nan(v)
    if isempty(v)
        x = NaN;
    else
        x = double(v);
    end
end


function v = value_of(v)
    if ischar(v)
        v = string(v);
    end
end


function root = repo_root()
%REPO_ROOT  The monorepo root (four levels above tests/matlab/scifor).
    root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..', '..');
end
