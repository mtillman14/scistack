classdef TestMissingValueCases < matlab.unittest.TestCase
%TESTMISSINGVALUECASES  scifor.has_no_data against
%   docs/claude/missing-value-cases.json (owner: Python scifor.missing;
%   scifor/tests/test_missing_value_cases.py reads the same file).

    methods (Test)

        function test_shared_missing_value_cases(tc)
            spec = jsondecode(fileread(fullfile(repo_root(), 'docs', 'claude', ...
                'missing-value-cases.json')));
            cases = spec.cases;
            if ~iscell(cases)
                cases = num2cell(cases);
            end
            for i = 1:numel(cases)
                c = cases{i};
                col = column_of(c.column);
                t = table(col, 'VariableNames', {'c'});
                tc.verifyEqual(scifor.has_no_data(t, "c"), logical(c.no_data), ...
                    sprintf('case %d %s', c.id, c.name));
            end
            tc.verifyGreaterThan(numel(cases), 10);
        end

    end
end


function col = column_of(spec)
    raw = spec.values;
    if iscell(raw)
        vals = raw;
    elseif strcmp(spec.class, 'array_cell')
        vals = num2cell(raw, 2);  % [[null,null]] decodes as one 1x2 row
    else
        vals = num2cell(raw);     % [null,null] decodes as [NaN; NaN]
    end
    vals = vals(:);
    n = numel(vals);
    switch spec.class
        case 'float'
            col = nan(n, 1);
            for i = 1:n
                if ~isempty(vals{i}), col(i) = double(vals{i}); end
            end
        case 'text'
            col = strings(n, 1);
            for i = 1:n
                if is_null(vals{i}) && ~ischar(vals{i})
                    col(i) = missing;
                else
                    col(i) = string(vals{i});
                end
            end
        case 'text_cell'
            col = vals;  % JSON null arrives as [] : MATLAB's null in a cell
            for i = 1:n
                if is_null(vals{i}) && ~ischar(vals{i}), col{i} = []; end
            end
        case 'datetime'
            col = NaT(n, 1);
            for i = 1:n
                if ~is_null(vals{i})
                    col(i) = datetime(vals{i}, 'InputFormat', 'yyyy-MM-dd''T''HH:mm:ss');
                end
            end
        case 'categorical'
            s = strings(n, 1);
            for i = 1:n
                if is_null(vals{i}), s(i) = missing; else, s(i) = string(vals{i}); end
            end
            col = categorical(s);
        case 'bool'
            col = logical(cell2mat(vals));
        case 'array_cell'
            col = cell(n, 1);
            for i = 1:n
                v = vals{i};
                if iscell(v)  % [null, null] decodes as a cell of []
                    v = cellfun(@(x) nan_if_empty(x), v);
                end
                col{i} = double(v(:)');
            end
        otherwise
            error('TestMissingValueCases:class', 'unknown class %s', spec.class);
    end
end


function tf = is_null(v)
    tf = isempty(v) || (isnumeric(v) && isscalar(v) && isnan(v));
end


function x = nan_if_empty(v)
    if isempty(v), x = NaN; else, x = double(v); end
end


function root = repo_root()
%REPO_ROOT  The monorepo root (four levels above tests/matlab/scifor).
    root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..', '..');
end
