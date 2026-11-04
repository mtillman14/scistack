classdef TestDistributeCases < matlab.unittest.TestCase
%TESTDISTRIBUTECASES  Where distribute=true files a returned table's rows,
%   against docs/claude/distribute-cases.json.
%
%   Owner: scifor.distribute (Python). This drives it end to end through
%   scifor.for_each over a MATLAB table (Python's
%   scifor/tests/test_distribute_cases.py reads the same file).

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
        function test_shared_distribute_cases(tc)
            root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..', '..');
            spec = jsondecode(fileread(fullfile(root, 'docs', 'claude', 'distribute-cases.json')));
            cases = spec.cases;
            if ~iscell(cases)
                cases = num2cell(cases);
            end
            for i = 1:numel(cases)
                c = cases{i};
                scifor.set_schema(string(c.schema));
                frame = table_of(c.frame);
                args = iterate_args(c.iterate);
                run = @() scifor.for_each(@() frame, struct(), args{:}, distribute=true);
                if isfield(c, 'error')
                    tc.verifyError(run, 'MATLAB:Python:PyException', c.name);
                    continue
                end
                result = run();
                addresses = c.addresses;
                if ~iscell(addresses)
                    addresses = num2cell(addresses);
                end
                tc.verifyEqual(height(result), numel(addresses), c.name);
                for r = 1:numel(addresses)
                    keys = fieldnames(addresses{r});
                    for k = 1:numel(keys)
                        got = string(result.(keys{k})(r));
                        tc.verifyEqual(got, string(addresses{r}.(keys{k})), ...
                            sprintf('%s: row %d, %s', c.name, r, keys{k}));
                    end
                end
            end
        end
    end
end


function t = table_of(frame)
%TABLE_OF  A table from the case's columns. A JSON null in a text column is a
%   missing string.
    t = table();
    names = fieldnames(frame);
    for n = 1:numel(names)
        values = frame.(names{n});
        if iscell(values)
            column = strings(numel(values), 1);
            for v = 1:numel(values)
                if isempty(values{v})
                    column(v) = missing;
                else
                    column(v) = string(values{v});
                end
            end
            t.(names{n}) = column;
        else
            t.(names{n}) = values(:);
        end
    end
end


function args = iterate_args(iterate)
    args = {};
    if ~isstruct(iterate)
        return
    end
    names = fieldnames(iterate);
    for n = 1:numel(names)
        args = [args, {char(names{n}), string(iterate.(names{n}))}]; %#ok<AGROW>
    end
end
