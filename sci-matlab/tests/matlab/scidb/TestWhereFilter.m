classdef TestWhereFilter < matlab.unittest.TestCase
%TESTWHEREFILER  Integration tests for the where= filter parameter.
%
%   Tests that:
%     - Comparison operators (==, ~=, <, <=, >, >=) on BaseVariable
%       instances produce scidb.Filter objects.
%     - Compound filters (& and |) compose correctly.
%     - NOT filter (~) works.
%     - scidb.raw_sql() produces a valid filter.
%     - load() and load_all() accept where= and correctly restrict results.

    properties (Constant)
        SchemaKeys = ["subject"]
    end

    properties
        db_path
        pipeline_path
        db
    end

    methods (TestMethodSetup)
        function setUp(obj)
            import matlab.unittest.fixtures.TemporaryFolderFixture
            tmp = obj.applyFixture(TemporaryFolderFixture());
            obj.db_path = fullfile(tmp.Folder, 'test.duckdb');
            obj.pipeline_path = fullfile(tmp.Folder, 'test_pipeline.db');
            obj.db = scidb.configure_database( ...
                obj.db_path, obj.SchemaKeys);
        end
    end

    methods (TestMethodTeardown)
        function tearDown(obj)
            if ~isempty(obj.db) && ~isa(obj.db, 'py.NoneType')
                obj.db.close();
            end
        end
    end

    % =====================================================================
    % Filter construction tests (no database needed)
    % =====================================================================

    methods (Test)

        function testEqReturnsFilter(obj)
        %TESTEQRETURNSFILTER  == operator returns a scidb.Filter
            filt = Side() == "L";
            obj.assertClass(filt, 'scidb.Filter');
            obj.assertNotEmpty(filt.py_filter);
        end

        function testNeReturnsFilter(obj)
        %TESTNETURNFILTER  ~= operator returns a scidb.Filter
            filt = Side() ~= "L";
            obj.assertClass(filt, 'scidb.Filter');
        end

        function testLtReturnsFilter(obj)
            filt = ScalarVar() < 1.5;
            obj.assertClass(filt, 'scidb.Filter');
        end

        function testLeReturnsFilter(obj)
            filt = ScalarVar() <= 1.5;
            obj.assertClass(filt, 'scidb.Filter');
        end

        function testGtReturnsFilter(obj)
            filt = ScalarVar() > 0.5;
            obj.assertClass(filt, 'scidb.Filter');
        end

        function testGeReturnsFilter(obj)
            filt = ScalarVar() >= 0.5;
            obj.assertClass(filt, 'scidb.Filter');
        end

        function testAndCompoundFilter(obj)
        %TESTANDCOMPOUNDFILTER  & combines two filters
            f1 = Side() == "L";
            f2 = ScalarVar() > 1.0;
            combined = f1 & f2;
            obj.assertClass(combined, 'scidb.Filter');
        end

        function testOrCompoundFilter(obj)
        %TEORCOMPOUNDFILTER  | combines two filters
            f1 = Side() == "L";
            f2 = Side() == "R";
            combined = f1 | f2;
            obj.assertClass(combined, 'scidb.Filter');
        end

        function testNotFilter(obj)
        %TESTNOTFILTER  ~ negates a filter
            f = Side() == "L";
            negated = ~f;
            obj.assertClass(negated, 'scidb.Filter');
        end

        function testRawSqlFilter(obj)
        %TESTRAWSQLFILTER  scidb.raw_sql returns a scidb.Filter
            filt = scidb.raw_sql('"value" > 0.50');
            obj.assertClass(filt, 'scidb.Filter');
        end

    end

    % =====================================================================
    % End-to-end load() / load_all() with where= filter
    % =====================================================================

    methods (Test)

        function testLoadAllWithEqFilter(obj)
        %TESTLOADALLEQFILTER  load_all(where=Side()==L) returns only L records
            % Save Side and StepLength for 3 subjects
            Side().save("L", db=obj.db, subject=1);
            Side().save("R", db=obj.db, subject=2);
            Side().save("L", db=obj.db, subject=3);
            StepLength().save(0.65, db=obj.db, subject=1);
            StepLength().save(0.55, db=obj.db, subject=2);
            StepLength().save(0.60, db=obj.db, subject=3);

            results = StepLength().load_all( ...
                where=Side() == "L", db=obj.db);

            obj.assertEqual(numel(results), 2);

            % Both results should be for subjects 1 and 3
            subjects = arrayfun(@(v) v.metadata.subject, results);
            obj.assertEqual(sort(subjects(:)), [1; 3]);
        end

        function testLoadAllWithEqFilterAndStringMetadata(obj)
        %TESTLOADALLEQFILTER  load_all(where=Side()==L) returns only L records
            % Save Side and StepLength for 3 subjects
            Side().save("L", db=obj.db, subject="SS01");
            Side().save("R", db=obj.db, subject="SS02");
            Side().save("L", db=obj.db, subject="SS03");
            StepLength().save(0.65, db=obj.db, subject="SS01");
            StepLength().save(0.55, db=obj.db, subject="SS02");
            StepLength().save(0.60, db=obj.db, subject="SS03");

            results = StepLength().load_all( ...
                where=Side() == "L", db=obj.db);

            obj.assertEqual(numel(results), 2);

            % Both results should be for subjects 1 and 3
            subjects = arrayfun(@(v) v.metadata.subject, results);
            obj.assertEqual(sort(subjects(:)), ["SS01"; "SS03"]);
        end

        function testLoadAllWithNotFilter(obj)
        %TESTLOADALWITHNOTFILTER  load_all(where=~(Side()=="L")) returns R records
            Side().save("L", db=obj.db, subject=1);
            Side().save("R", db=obj.db, subject=2);
            StepLength().save(0.65, db=obj.db, subject=1);
            StepLength().save(0.55, db=obj.db, subject=2);

            results = StepLength().load_all( ...
                where=~(Side() == "L"), db=obj.db);

            obj.assertEqual(numel(results), 1);
            obj.assertEqual(results(1).metadata.subject, 2);
        end

        function testLoadAllWithCompoundAndFilter(obj)
        %TESTLOADALCOMPOUNDANDFILTER  load_all(where=(Side()=="L")&(SV()>1))
            Side().save("L", db=obj.db, subject=1);
            Side().save("L", db=obj.db, subject=2);
            Side().save("R", db=obj.db, subject=3);
            ScalarVar().save(1.5, db=obj.db, subject=1);
            ScalarVar().save(0.8, db=obj.db, subject=2);
            ScalarVar().save(1.2, db=obj.db, subject=3);
            StepLength().save(0.65, db=obj.db, subject=1);
            StepLength().save(0.55, db=obj.db, subject=2);
            StepLength().save(0.60, db=obj.db, subject=3);

            results = StepLength().load_all( ...
                where=(Side() == "L") & (ScalarVar() > 1.0), db=obj.db);

            % Only subject 1: side=L AND scalarvar > 1.0
            obj.assertEqual(numel(results), 1);
            obj.assertEqual(results(1).metadata.subject, 1);
        end

        function testLoadAllWithRawSql(obj)
        %TESTLODALWITHRAWSQL  load_all(where=raw_sql(...)) filters correctly
            StepLength().save(0.65, db=obj.db, subject=1);
            StepLength().save(0.45, db=obj.db, subject=2);
            StepLength().save(0.55, db=obj.db, subject=3);

            results = StepLength().load_all( ...
                where=scidb.raw_sql('"value" > 0.60'), db=obj.db);

            obj.assertEqual(numel(results), 1);
            % Only subject 1 has value > 0.60
            obj.assertEqual(results(1).metadata.subject, 1);
        end

        function testLoadWithWhereFilter(obj)
        %TESTLOADWITHWHERE  load(where=...) restricts to matching records
            Side().save("L", db=obj.db, subject=1);
            Side().save("R", db=obj.db, subject=2);
            StepLength().save(0.65, db=obj.db, subject=1);
            StepLength().save(0.55, db=obj.db, subject=2);

            result = StepLength().load(where=Side() == "R", db=obj.db);
            % Only subject=2 matches Side=="R"; its StepLength is 0.55
            obj.assertEqual(result, 0.55);
        end

    end

end
