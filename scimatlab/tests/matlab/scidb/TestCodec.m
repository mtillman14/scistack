classdef TestCodec < matlab.unittest.TestCase
%TESTCODEC  Custom to_db/from_db on MATLAB scidb.BaseVariable subclasses.
%
%   The owner is scidb.internal.codec; ensure_registered tells the Python
%   surrogate (scimatlab.bridge.register_matlab_variable codec=true) so the
%   to_db table is stored in the columnar "custom" form a Python codec class
%   uses. Covers save, save_from_table and load (latest, 'all', as_table).

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
        function test_codec_detected(testCase)
            c = scidb.internal.codec('CodecTraj');
            testCase.verifyTrue(c.has_codec);
            plain = scidb.internal.codec('ScalarVar');
            testCase.verifyFalse(plain.has_codec);
        end

        function test_object_round_trips_through_save_load(testCase)
            traj = CodecTrajectory([0.1; 0.2; 0.3], "heel");
            CodecTraj().save(traj, 'subject', 1, 'session', 'A');
            loaded = CodecTraj().load('subject', 1, 'session', 'A');
            testCase.verifyClass(loaded.data, 'CodecTrajectory');
            testCase.verifyEqual(loaded.data, traj);
        end

        function test_stored_as_custom_columnar_table(testCase)
            CodecTraj().save(CodecTrajectory([1; 2], "toe"), ...
                'subject', 1, 'session', 'A');
            dtype = py.scidb.database.get_database().get_dtype_meta('CodecTraj');
            testCase.verifyTrue(logical(py.operator.contains(dtype, 'custom')));
        end

        function test_load_all_versions_decodes_each(testCase)
            a = CodecTrajectory([1; 2], "a");
            b = CodecTrajectory([3; 4; 5], "b");
            CodecTraj().save(a, 'subject', 1, 'session', 'A');
            CodecTraj().save(b, 'subject', 1, 'session', 'A');
            all_v = CodecTraj().load('subject', 1, 'session', 'A', 'version', 'all');
            testCase.verifyNumElements(all_v, 2);
            labels = sort(string(arrayfun(@(v) char(v.data.label), all_v, ...
                'UniformOutput', false)));
            testCase.verifyEqual(labels(:)', ["a", "b"]);
        end

        function test_save_from_table_encodes_every_row(testCase)
            trajs = {CodecTrajectory([1; 2; 3], "a"); CodecTrajectory([4; 5], "b")};
            tbl = table([1; 2], ["A"; "A"], trajs, ...
                'VariableNames', {'subject', 'session', 'traj'});
            ids = CodecTraj().save_from_table(tbl, "traj", ["subject", "session"]);
            testCase.verifyNumElements(ids, 2);
            r1 = CodecTraj().load('subject', 1, 'session', 'A');
            r2 = CodecTraj().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(r1.data, trajs{1});
            testCase.verifyEqual(r2.data, trajs{2});
        end

        function test_save_and_save_from_table_same_record_id(testCase)
            traj = CodecTrajectory([7; 8], "same");
            id_direct = CodecTraj().save(traj, 'subject', 1, 'session', 'A');
            tbl = table(1, "A", {traj}, ...
                'VariableNames', {'subject', 'session', 'traj'});
            ids = CodecTraj().save_from_table(tbl, "traj", ["subject", "session"]);
            testCase.verifyEqual(ids(1), id_direct);
        end

        function test_as_table_holds_decoded_objects(testCase)
            CodecTraj().save(CodecTrajectory([1; 2], "x"), 'subject', 1, 'session', 'A');
            CodecTraj().save(CodecTrajectory([3; 4], "y"), 'subject', 2, 'session', 'A');
            tbl = CodecTraj().load('as_table', true);
            testCase.verifyEqual(height(tbl), 2);
            testCase.verifyClass(tbl.CodecTraj{1}, 'CodecTrajectory');
        end

        % --- scidb.for_each (stage 4) ---

        function test_for_each_object_output_round_trips(testCase)
            RawSignal().save([1 2 3], 'subject', 1, 'session', 'A');
            RawSignal().save([4 5], 'subject', 2, 'session', 'A');
            scidb.for_each(@make_codec_traj, struct('raw', RawSignal()), ...
                {CodecTraj()}, 'subject', [1 2], 'session', "A");
            r1 = CodecTraj().load('subject', 1, 'session', 'A');
            r2 = CodecTraj().load('subject', 2, 'session', 'A');
            testCase.verifyEqual(r1.data, CodecTrajectory([1; 2; 3], "heel"));
            testCase.verifyEqual(r2.data, CodecTrajectory([4; 5], "heel"));
            dtype = py.scidb.database.get_database().get_dtype_meta('CodecTraj');
            testCase.verifyTrue(logical(py.operator.contains(dtype, 'custom')));
        end

        function test_for_each_object_input_is_decoded(testCase)
            CodecTraj().save(CodecTrajectory([1; 2; 3], "a"), 'subject', 1, 'session', 'A');
            CodecTraj().save(CodecTrajectory([4; 5], "b"), 'subject', 2, 'session', 'A');
            scidb.for_each(@codec_traj_length, struct('traj', CodecTraj()), ...
                {ScalarVar()}, 'subject', [1 2], 'session', "A");
            testCase.verifyEqual(ScalarVar().load('subject', 1, 'session', 'A').data, 3);
            testCase.verifyEqual(ScalarVar().load('subject', 2, 'session', 'A').data, 2);
        end

        function test_for_each_chain_object_to_object(testCase)
            % output of one step is the decoded input of the next
            RawSignal().save([1 2 3 4], 'subject', 1, 'session', 'A');
            scidb.for_each(@make_codec_traj, struct('raw', RawSignal()), ...
                {CodecTraj()}, 'subject', 1, 'session', "A");
            scidb.for_each(@codec_traj_length, struct('traj', CodecTraj()), ...
                {ScalarVar()}, 'subject', 1, 'session', "A");
            testCase.verifyEqual(ScalarVar().load('subject', 1, 'session', 'A').data, 4);
        end

        function test_for_each_fixed_codec_input_is_decoded(testCase)
            CodecTraj().save(CodecTrajectory([1; 2], "base"), 'subject', 1, 'session', 'A');
            RawSignal().save([9 9 9], 'subject', 1, 'session', 'B');
            scidb.for_each(@codec_traj_length, ...
                struct('traj', scidb.Fixed(CodecTraj(), 'session', "A")), ...
                {ScalarVar()}, 'subject', 1, 'session', "B");
            testCase.verifyEqual(ScalarVar().load('subject', 1, 'session', 'B').data, 2);
        end

        % --- errors ---

        function test_only_to_db_errors(testCase)
            testCase.verifyError( ...
                @() CodecOnlyToDbVar().save(1, 'subject', 1, 'session', 'A'), ...
                'scidb:CodecError');
        end

        function test_static_to_db_errors(testCase)
            testCase.verifyError( ...
                @() CodecStaticToDbVar().save(1, 'subject', 1, 'session', 'A'), ...
                'scidb:CodecError');
        end

        function test_to_db_must_return_table(testCase)
            testCase.verifyError( ...
                @() CodecNonTableVar().save(1, 'subject', 1, 'session', 'A'), ...
                'scidb:CodecError');
        end
    end
end
