classdef TestBlameCases < matlab.unittest.TestCase
%TESTBLAMECASES  scifor.classify_blame against docs/claude/blame-cases.json
%   (owner: Python scifor.failures.classify_blame; scifor/tests/
%   test_blame_cases.py reads the same file).

    methods (Test)

        function test_shared_blame_cases(tc)
            spec = jsondecode(fileread(fullfile(repo_root(), 'docs', 'claude', ...
                'blame-cases.json')));
            cases = spec.cases;
            if ~iscell(cases)
                cases = num2cell(cases);
            end
            ids = struct('plain', 'MATLAB:test', 'setup', 'scidb:Test', ...
                'user_value', 'scistack:userValue', ...
                'user_value_probable', 'scistack:userValue:probable');
            for i = 1:numel(cases)
                c = cases{i};
                no_data = isfield(c, 'no_data') && logical(c.no_data);
                [blame, probable] = scifor.classify_blame(ids.(c.error), ...
                    frames_of(c.frames), no_data);
                label = sprintf('case %d %s', c.id, c.name);
                tc.verifyEqual(blame, c.blame, label);
                tc.verifyEqual(probable, logical(c.probable), label);
            end
            tc.verifyGreaterThan(numel(cases), 8);
        end

    end
end


function frames = frames_of(origins)
    if ischar(origins)
        origins = {origins};
    end
    frames = struct('file', {}, 'line', {}, 'function', {}, 'code', {}, 'origin', {});
    for i = 1:numel(origins)
        frames(end + 1) = struct('file', sprintf('/x/%s%d.m', origins{i}, i), ...
            'line', i, 'function', sprintf('f%d', i), 'code', '', ...
            'origin', origins{i}); %#ok<AGROW>
    end
end


function root = repo_root()
%REPO_ROOT  The monorepo root (four levels above tests/matlab/scifor).
    root = fullfile(fileparts(mfilename('fullpath')), '..', '..', '..', '..');
end
