classdef TestDatetimeRoundTrip < matlab.unittest.TestCase
%TESTDATETIMEROUNDTRIP  MATLAB datetime values through save/load.
%
%   Pins docs/claude/datetime-roundtrip.md ("MATLAB"): a datetime crosses as
%   int64 ns + NaT mask + TimeZone (+scidb/+internal/datetime_to_python.m,
%   datetime_from_python.m); it is stored as TIMESTAMP_NS (UTC) plus a
%   __tz zone column, and loads back with its TimeZone, its NaT and its
%   nanoseconds. Before 2026-10-22 a datetime crossed as ISO text without
%   its zone, and loading a Python datetime failed on fractions, zones and NaT.

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
                ["subject"]);
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
        function test_unzoned_scalar(testCase)
            d = datetime(2026, 1, 1, 10, 30, 15.25);
            ScalarVar().save(d, 'subject', 1);
            r = ScalarVar().load('subject', 1);
            testCase.verifyClass(r.data, 'datetime');
            testCase.verifyEqual(r.data.TimeZone, '');
            testCase.verifyEqual(r.data, d);
        end

        function test_zoned_scalar_keeps_its_zone(testCase)
            d = datetime(2026, 7, 1, 14, 0, 0, 'TimeZone', 'Europe/Berlin');
            ScalarVar().save(d, 'subject', 1);
            r = ScalarVar().load('subject', 1);
            testCase.verifyEqual(r.data.TimeZone, 'Europe/Berlin');
            testCase.verifyEqual(r.data, d);
            testCase.verifyEqual(r.data.Hour, 14);
        end

        function test_offset_zone(testCase)
            d = datetime(2026, 1, 1, 9, 0, 0, 'TimeZone', '+02:00');
            ScalarVar().save(d, 'subject', 1);
            r = ScalarVar().load('subject', 1);
            testCase.verifyEqual(r.data.TimeZone, '+02:00');
            testCase.verifyEqual(r.data, d);
        end

        function test_nat_scalar(testCase)
            ScalarVar().save(NaT, 'subject', 1);
            r = ScalarVar().load('subject', 1);
            testCase.verifyClass(r.data, 'datetime');
            testCase.verifyTrue(isnat(r.data));
        end

        function test_nanoseconds_survive(testCase)
            d = datetime(2026, 1, 1) + milliseconds(1e-6) * 7;  % +7 ns
            ScalarVar().save(d, 'subject', 1);
            r = ScalarVar().load('subject', 1);
            testCase.verifyEqual(r.data, d);
        end

        function test_resave_of_loaded_value_is_the_same_record(testCase)
            d = datetime(2026, 7, 1, 14, 0, 0, 'TimeZone', 'America/New_York');
            rid = ScalarVar().save(d, 'subject', 1);
            r = ScalarVar().load('subject', 1);
            rid2 = ScalarVar().save(r.data, 'subject', 1);
            testCase.verifyEqual(string(rid2), string(rid));
        end

        function test_table_column_with_zone_and_nat(testCase)
            t = table([1; 2; 3], ...
                datetime(2026, 1, [1; 2; 3], 'TimeZone', 'Asia/Tokyo'), ...
                'VariableNames', {'x', 'when'});
            t.when(2) = NaT;
            TableVar().save(t, 'subject', 1);
            r = TableVar().load('subject', 1);
            testCase.verifyClass(r.data.when, 'datetime');
            testCase.verifyEqual(r.data.when.TimeZone, 'Asia/Tokyo');
            testCase.verifyEqual(isnat(r.data.when), [false; true; false]);
            testCase.verifyEqual(r.data.when([1 3]), t.when([1 3]));
            testCase.verifyEqual(r.data.x, t.x);
        end

        function test_unzoned_table_column(testCase)
            t = table(datetime(2026, 1, [1; 2], 8, 0, 0), 'VariableNames', {'when'});
            TableVar().save(t, 'subject', 1);
            r = TableVar().load('subject', 1);
            testCase.verifyEqual(r.data.when.TimeZone, '');
            testCase.verifyEqual(r.data.when, t.when);
        end

        function test_save_from_table_datetime_data_column(testCase)
            % One record per row: the per-schema-level case.
            tbl = table([1; 2; 3], ...
                datetime(2026, 7, [1; 2; 3], 12, 0, 0, 'TimeZone', 'Europe/Berlin'), ...
                'VariableNames', {'subject', 'when'});
            tbl.when(2) = NaT;
            ScalarVar().save_from_table(tbl, 'when', {'subject'});
            r1 = ScalarVar().load('subject', 1);
            r2 = ScalarVar().load('subject', 2);
            testCase.verifyEqual(r1.data, tbl.when(1));
            testCase.verifyEqual(r1.data.TimeZone, 'Europe/Berlin');
            testCase.verifyTrue(isnat(r2.data));
        end

        function test_stored_as_instant_plus_zone(testCase)
            d = datetime(2026, 7, 1, 14, 0, 0, 'TimeZone', 'Europe/Berlin');
            ScalarVar().save(d, 'subject', 1);
            duck = py.getattr(scidb.get_database(), '_duck');
            rows = cell(duck.fetchall( ...
                "SELECT CAST(""value"" AS VARCHAR), ""value__tz"" FROM ""ScalarVar_data"""));
            row = cell(rows{1});
            testCase.verifyEqual(string(row{1}), "2026-07-01 12:00:00");
            testCase.verifyEqual(string(row{2}), "Europe/Berlin");
        end
    end
end
