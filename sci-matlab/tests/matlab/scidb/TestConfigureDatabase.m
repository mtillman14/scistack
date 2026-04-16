classdef TestConfigureDatabase < matlab.unittest.TestCase
%TESTCONFIGUREDATABASE  Integration tests for scidb.configure_database.

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
        function createTempDir(testCase)
            testCase.test_dir = tempname;
            mkdir(testCase.test_dir);
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            try
                db = scidb.get_database();
                db.close();
            catch
            end
            if isfolder(testCase.test_dir)
                rmdir(testCase.test_dir, 's');
            end
        end
    end

    methods (Test)
        function test_returns_database_object(testCase)
            db_path = fullfile(testCase.test_dir, 'test.duckdb');            
            db = scidb.configure_database(db_path, ["subject", "session"]);
            testCase.verifyNotEmpty(db);
        end

        function test_get_database_returns_same_instance(testCase)
            db_path = fullfile(testCase.test_dir, 'test.duckdb');            
            scidb.configure_database(db_path, ["subject", "session"]);
            db = scidb.get_database();
            testCase.verifyNotEmpty(db);
        end

        function test_single_schema_key(testCase)
            db_path = fullfile(testCase.test_dir, 'test.duckdb');            
            db = scidb.configure_database(db_path, "subject");
            testCase.verifyNotEmpty(db);
        end

        function test_schema_keys_column_vector(testCase)
            % Column vectors should be transposed internally
            db_path = fullfile(testCase.test_dir, 'test.duckdb');            
            keys = ["subject"; "session"];  % Column vector
            db = scidb.configure_database(db_path, keys);
            testCase.verifyNotEmpty(db);
            % Verify it works by saving data
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
        end

        function test_relative_paths(testCase)
            old_dir = pwd;
            cleanup_obj = onCleanup(@() cd(old_dir));
            cd(testCase.test_dir);
            db = scidb.configure_database('test.duckdb', ["subject"]);
            testCase.verifyNotEmpty(db);
            % Verify database is functional
            RawSignal().save(42, 'subject', 1);
            result = RawSignal().load('subject', 1);
            testCase.verifyEqual(result.data, 42);
        end

        function test_absolute_paths(testCase)
            db_path = fullfile(testCase.test_dir, 'test.duckdb');
            scidb.configure_database(db_path, ["subject"]);
            RawSignal().save([1 2 3], 'subject', 1);
            result = RawSignal().load('subject', 1);
            testCase.verifyEqual(result.data, [1; 2; 3]);
        end

        function test_reconfigure_database(testCase)
            % Configuring twice should work (replaces global singleton)
            db_path1 = fullfile(testCase.test_dir, 'test1.duckdb');            
            scidb.configure_database(db_path1, ["subject"]);
            RawSignal().save([1 2 3], 'subject', 1);

            % Close and reconfigure
            scidb.get_database().close();

            db_path2 = fullfile(testCase.test_dir, 'test2.duckdb');          
            scidb.configure_database(db_path2, ["subject"]);

            % Old data should not be accessible in new database
            testCase.verifyError(@() RawSignal().load('subject', 1), ...
                'scidb:NotFoundError');
        end

        function test_multiple_simultaneous_databases(testCase)
            % Configuring twice should work (replaces global singleton)
            db_path1 = fullfile(testCase.test_dir, 'test1.duckdb');            
            db1 = scidb.configure_database(db_path1, ["subject"]);
            RawSignal().save([1 2 3], 'subject', 1);

            db_path2 = fullfile(testCase.test_dir, 'test2.duckdb');            
            db2 = scidb.configure_database(db_path2, ["subject"]);
            % Old data should not be accessible in new database
            testCase.verifyError(@() RawSignal().load('subject', 1), ...
                'scidb:NotFoundError');
            % New data should be accessible in new database
            RawSignal().save([4 5 6], 'subject', 1);
            loaded = RawSignal().load('subject', 1);

            db1.close();
            db2.close();

        end

        function test_swapping_multiple_simultaneous_databases(testCase)
            % Configuring twice should work (replaces global singleton)
            db_path1 = fullfile(testCase.test_dir, 'test1.duckdb');            
            db1 = scidb.configure_database(db_path1, ["subject"]);
            RawSignal().save([1 2 3], 'subject', 1);

            db_path2 = fullfile(testCase.test_dir, 'test2.duckdb');            
            db2 = scidb.configure_database(db_path2, ["subject"]);
            RawSignal().save([4 5 6], 'subject', 1);

            db1.set_current_db();
            loaded = RawSignal().load('subject', 1);
            testCase.verifyEqual(loaded.data, [1; 2; 3]);

            db2.set_current_db();
            loaded = RawSignal().load('subject', 1);
            testCase.verifyEqual(loaded.data, [4; 5; 6]);

            db1.close();
            db2.close();

        end
    end
end
