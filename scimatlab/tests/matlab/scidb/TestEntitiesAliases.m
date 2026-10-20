classdef TestEntitiesAliases < matlab.unittest.TestCase
%TESTENTITIESALIASES  PathInput aliases declared in the TOML entities file
%   reach the MATLAB object scidb.entities() builds.
%
%   The bridge sends aliases as JSON text (a dict would cross as a struct,
%   and a canonical such as "01" is not a field name); +scidb/entities.m
%   passes it to scidb.PathInput(aliases=). Python half:
%   scimatlab/tests/test_bridge_project_root.py::TestLoadEntitiesAliases.

    properties
        root
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function makeProject(testCase)
            testCase.root = tempname;
            mkdir(fullfile(testCase.root, 'src'));
            mkdir(fullfile(testCase.root, 'Baseline', 'Xsens'));
            fclose(fopen(fullfile(testCase.root, 'Baseline', 'Xsens', 'S01_01.xlsx'), 'w'));
            fid = fopen(fullfile(testCase.root, 'scistack.toml'), 'w');
            fprintf(fid, 'entities_file = "src/scistack_entities.toml"\n');
            fclose(fid);
            fid = fopen(fullfile(testCase.root, 'src', 'scistack_entities.toml'), 'w');
            fprintf(fid, '[path_inputs]\n');
            fprintf(fid, ['XSENS = { template = "{session}/Xsens/{subject}_{session}.xlsx", ' ...
                'aliases = { session = { "01" = ["Baseline"] } } }\n']);
            fprintf(fid, 'PLAIN = "{subject}.csv"\n');
            fclose(fid);
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            py.scifor.clear_project_root();
            if isfolder(testCase.root)
                rmdir(testCase.root, 's');
            end
        end
    end

    methods (Test)
        function test_aliases_reach_the_object(testCase)
            e = scidb.entities(testCase.root);
            aliases = e.XSENS.py_obj.aliases;
            testCase.verifyEqual(double(py.len(aliases)), 1);
            spellings = cell(aliases{'session'}{'01'});
            testCase.verifyEqual(string(spellings{1}), "Baseline");
        end

        function test_aliased_template_loads_mixed_spelling(testCase)
            % Folder "Baseline", filename "01": one session, two spellings.
            e = scidb.entities(testCase.root);
            path = e.XSENS.load('session', '01', 'subject', 'S01');
            testCase.verifyEqual(path, string(fullfile( ...
                testCase.root, 'Baseline', 'Xsens', 'S01_01.xlsx')));
        end

        function test_no_aliases_declared(testCase)
            e = scidb.entities(testCase.root);
            testCase.verifyEqual(double(py.len(e.PLAIN.py_obj.aliases)), 0);
        end
    end
end
