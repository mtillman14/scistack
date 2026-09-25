classdef TestSchemaKeyTypes < matlab.unittest.TestCase
%TESTSCHEMAKEYTYPES  Schema key type declarations from the MATLAB host.
%
%   MATLAB mirror of scidb/tests/test_schema_key_types.py. The hybrid
%   contract: no declaration needed while path matches are exact; a
%   PathInput numeric-fallback resolution on an undeclared schema key
%   raises scidb:SchemaKeyTypeError; declared 'numeric' keys canonicalize
%   every spelling to one identity; declared 'string' keys are verbatim
%   and never bridge spellings.

    properties
        test_dir
        data_root
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function setupDirs(testCase)
            testCase.test_dir = tempname;
            mkdir(testCase.test_dir);
            % Zero-padded data files: data/1/6MWT-001.txt (1.5), -002.txt (2.5)
            testCase.data_root = fullfile(testCase.test_dir, 'data');
            d = fullfile(testCase.data_root, '1');
            mkdir(d);
            fid = fopen(fullfile(d, '6MWT-001.txt'), 'w');
            fprintf(fid, '1.5'); fclose(fid);
            fid = fopen(fullfile(d, '6MWT-002.txt'), 'w');
            fprintf(fid, '2.5'); fclose(fid);
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            try
                scidb.get_database().close();
            catch
            end
            if isfolder(testCase.test_dir)
                rmdir(testCase.test_dir, 's');
            end
        end
    end

    methods (Access = private)
        function configure(testCase, key_types)
            if nargin < 2 || isempty(fieldnames(key_types))
                scidb.configure_database( ...
                    fullfile(testCase.test_dir, 'test.duckdb'), ...
                    ["subject", "trial"]);
            else
                scidb.configure_database( ...
                    fullfile(testCase.test_dir, 'test.duckdb'), ...
                    ["subject", "trial"], ...
                    'schema_key_types', key_types);
            end
        end

        function pi = padded_pathinput(testCase)
            pi = scifor.PathInput("{subject}/6MWT-{trial}.txt", ...
                'root_folder', testCase.data_root, 'name', "{subject}/6MWT-{trial}.txt");
        end
    end

    methods (Test)
        function test_types_reach_python_db(testCase)
            testCase.configure(struct('trial', 'numeric'));
            py_db = py.scidb.database.get_database();
            kt = scidb.internal.pydict_to_struct(py_db.dataset_schema_key_types);
            testCase.verifyEqual(char(string(kt.trial)), 'numeric');
        end

        function test_invalid_type_value_errors(testCase)
            testCase.verifyError(@() testCase.configure( ...
                struct('trial', 'int')), 'MATLAB:Python:PyException');
        end

        function test_numeric_declared_explicit_run(testCase)
            testCase.configure(struct('trial', 'numeric'));
            scidb.for_each(@read_file_value, ...
                struct('filepath', testCase.padded_pathinput()), ...
                {ScalarVar()}, ...
                'subject', 1, 'trial', [1 2]);
            % Padded files resolved; identity stored canonically, so an
            % unpadded load finds each record with the right file's content.
            r1 = ScalarVar().load('subject', 1, 'trial', 1);
            r2 = ScalarVar().load('subject', 1, 'trial', 2);
            testCase.verifyEqual(double(r1.data), 1.5, 'AbsTol', 1e-12);
            testCase.verifyEqual(double(r2.data), 2.5, 'AbsTol', 1e-12);
        end

        function test_discovery_and_explicit_share_identity(testCase)
            testCase.configure(struct('trial', 'numeric'));
            % Discovery-driven run: disk spellings "001"/"002" canonicalize.
            scidb.for_each(@read_file_value, ...
                struct('filepath', testCase.padded_pathinput()), ...
                {ScalarVar()}, ...
                'subject', [], 'trial', []);
            n_first = numel(ScalarVar().load());
            % Explicit numeric run re-saves the same identities: no new records.
            scidb.for_each(@read_file_value, ...
                struct('filepath', testCase.padded_pathinput()), ...
                {ScalarVar()}, ...
                'subject', 1, 'trial', [1 2]);
            n_second = numel(ScalarVar().load());
            testCase.verifyEqual(n_second, n_first);
        end

        function test_undeclared_resolution_errors(testCase)
            testCase.configure(struct());
            testCase.verifyError(@() scidb.for_each(@read_file_value, ...
                struct('filepath', testCase.padded_pathinput()), ...
                {ScalarVar()}, ...
                'subject', 1, 'trial', [1 2]), ...
                'scidb:SchemaKeyTypeError');
        end

        function test_undeclared_exact_matches_need_no_declaration(testCase)
            testCase.configure(struct());
            % Discovery-driven: combos carry "001"/"002", every path
            % literal-hits, the fallback never fires (hybrid contract).
            scidb.for_each(@read_file_value, ...
                struct('filepath', testCase.padded_pathinput()), ...
                {ScalarVar()}, ...
                'subject', [], 'trial', []);
            results = ScalarVar().load();
            testCase.verifyEqual(numel(results), 2);
        end

        function test_string_declared_never_bridges(testCase)
            testCase.configure(struct('trial', 'string'));
            % trial=1 renders 6MWT-1.txt (missing); the string declaration
            % forbids the numeric bridge to 001, so the combo skips —
            % and no declare-error fires (the key IS declared).
            scidb.for_each(@read_file_value, ...
                struct('filepath', testCase.padded_pathinput()), ...
                {ScalarVar()}, ...
                'subject', 1, 'trial', 1);
            testCase.verifyError(@() ScalarVar().load(), 'scidb:NotFoundError');
        end

        function test_string_declared_exact_spelling_works(testCase)
            testCase.configure(struct('trial', 'string'));
            scidb.for_each(@read_file_value, ...
                struct('filepath', testCase.padded_pathinput()), ...
                {ScalarVar()}, ...
                'subject', 1, 'trial', ["001", "002"]);
            r = ScalarVar().load('subject', 1, 'trial', "001");
            testCase.verifyEqual(double(r.data), 1.5, 'AbsTol', 1e-12);
        end
    end
end
