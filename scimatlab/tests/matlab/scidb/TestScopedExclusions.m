classdef TestScopedExclusions < matlab.unittest.TestCase
%TESTSCOPEDEXCLUSIONS  'variable', V in scidb.exclude_schema / include_schema.
%
%   GitHub #28: an exclusion can be scoped to one variable (Python's
%   variable=, the CLI's --variable). The MATLAB wrappers read the pair in
%   scidb.internal.exclusion_args (the one owner); scidb.list_exclusions
%   reports the scope in its scoped_to column.

    properties
        test_dir
    end

    methods (TestClassSetup)
        function addPaths(~)
            this_dir = fileparts(mfilename('fullpath'));
            run(fullfile(this_dir, 'setup_paths.m'));
        end
    end

    methods (TestMethodSetup)
        function setupDatabase(testCase)
            testCase.test_dir = tempname;
            mkdir(testCase.test_dir);
            scidb.configure_database( ...
                fullfile(testCase.test_dir, 'test.duckdb'), ...
                ["subject", "session"]);
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

    methods (Test)
        function test_unscoped_exclusion_covers_everything(testCase)
            scidb.exclude_schema("withdrew", 'subject', 1);
            tbl = scidb.list_exclusions();
            testCase.verifyEqual(height(tbl), 1);
            testCase.verifyTrue(ismissing(tbl.scoped_to(1)));
        end

        function test_variable_name_scopes_the_exclusion(testCase)
            scidb.exclude_schema("bad sensor", 'subject', 1, 'variable', "RawSignal");
            tbl = scidb.list_exclusions();
            testCase.verifyEqual(height(tbl), 1);
            testCase.verifyEqual(string(tbl.scoped_to(1)), "RawSignal");
            testCase.verifyFalse(any(strcmp(tbl.Properties.VariableNames, 'variable')), ...
                "'variable' is the scope, not a schema key column");
        end

        function test_variable_instance_scopes_the_exclusion(testCase)
            scidb.exclude_schema("bad sensor", 'subject', 2, 'variable', RawSignal());
            tbl = scidb.list_exclusions();
            testCase.verifyEqual(string(tbl.scoped_to(1)), "RawSignal");
        end

        function test_scoped_include_takes_it_back(testCase)
            scidb.exclude_schema("bad sensor", 'subject', 1, 'variable', "RawSignal");
            scidb.include_schema("sensor fixed", 'subject', 1, 'variable', "RawSignal");
            testCase.verifyEqual(height(scidb.list_exclusions()), 0);
        end

        function test_scoped_and_unscoped_are_separate(testCase)
            % The same keys under two scopes are two exclusions.
            scidb.exclude_schema("withdrew", 'subject', 1);
            scidb.exclude_schema("bad sensor", 'subject', 1, 'variable', "RawSignal");
            testCase.verifyEqual(height(scidb.list_exclusions()), 2);
        end

        function test_odd_pairs_fail(testCase)
            testCase.verifyError( ...
                @() scidb.exclude_schema("r", 'subject', 1, 'variable'), ...
                'scidb:exclude_schema');
        end
    end
end
