classdef TestRegisterVariable < matlab.unittest.TestCase
%TESTREGISTERVARIABLE  Integration tests for scidb.register_variable.

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
        function test_register_with_default_schema_version(testCase)
            % register_variable should not error
            scidb.register_variable(RawSignal());
            % Should be able to save after registration
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data, [1 2 3]', 'AbsTol', 1e-10);
        end

        function test_register_with_custom_schema_version(testCase)
            scidb.register_variable(ScalarVar(), 'schema_version', 2);
            % Should be able to save/load after registration
            ScalarVar().save(42, 'subject', 1, 'session', 'A');
            result = ScalarVar().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data, 42, 'AbsTol', 1e-10);
        end

        function test_auto_registration_on_save(testCase)
            % Do NOT explicitly register — save should auto-register
            ProcessedSignal().save([4 5 6], 'subject', 1, 'session', 'A');
            result = ProcessedSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data, [4 5 6]', 'AbsTol', 1e-10);
        end

        function test_auto_registration_on_load(testCase)
            % Auto-register should work even for load (though it will
            % error with NotFound since nothing is saved yet)
            testCase.verifyError( ...
                @() FilteredSignal().load('subject', 999, 'session', 'Z'), ...
                'scidb:NotFoundError');
            % The error should be NotFound, not a registration error
        end

        function test_idempotent_registration(testCase)
            % Registering the same type multiple times should not error
            scidb.register_variable(RawSignal());
            scidb.register_variable(RawSignal());

            % Should still work normally
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            result = RawSignal().load('subject', 1, 'session', 'A');
            testCase.verifyEqual(result.data, [1 2 3]', 'AbsTol', 1e-10);
        end

        function test_register_requires_base_variable(testCase)
            % Passing something that is not a BaseVariable should error
            testCase.verifyError( ...
                @() scidb.register_variable(42), ...
                'MATLAB:validators:mustBeA');
        end

        function test_different_types_independent_tables(testCase)
            % Two variable types should have independent storage
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            ProcessedSignal().save([4 5 6], 'subject', 1, 'session', 'A');

            r1 = RawSignal().load('subject', 1, 'session', 'A');
            r2 = ProcessedSignal().load('subject', 1, 'session', 'A');

            testCase.verifyEqual(r1.data, [1 2 3]', 'AbsTol', 1e-10);
            testCase.verifyEqual(r2.data, [4 5 6]', 'AbsTol', 1e-10);
        end
    end
end
