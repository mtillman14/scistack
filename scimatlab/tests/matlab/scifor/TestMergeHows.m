classdef TestMergeHows < matlab.unittest.TestCase
%TESTMERGEHOWS  Merge name= / How= on the MATLAB side (docs/claude/merge-node.md).

    methods (Test)

        function test_fallback_equals_python_owner(tc)
            % scifor.merge.MERGE_HOWS owns the list; the MATLAB copy is used
            % only without Python and must not drift.
            py_hows = string(cell(py.scifor.merge.MERGE_HOWS));
            tc.verifyEqual(scifor.merge_hows_fallback(), reshape(py_hows, 1, []));
        end

        function test_name_is_required(tc)
            a = table([1; 2], 'VariableNames', {'x'});
            b = table([3; 4], 'VariableNames', {'y'});
            tc.verifyError(@() scifor.Merge(a, b), 'scifor:Merge');
            m = scifor.Merge(a, b, name="ab");
            tc.verifyEqual(m.name, "ab");
            tc.verifyEqual(m.how, "exact");
        end

        function test_unknown_how_is_refused(tc)
            a = table([1; 2], 'VariableNames', {'x'});
            b = table([3; 4], 'VariableNames', {'y'});
            tc.verifyError(@() scifor.Merge(a, b, name="ab", how="cross"), 'scifor:Merge');
        end

        function test_left_join_pads(tc)
            a = table([1; 2; 3], [10; 20; 30], 'VariableNames', {'trial', 'force'});
            b = table([1; 2], [0.1; 0.2], 'VariableNames', {'trial', 'emg'});
            got = scifor.merge_tables({a, b}, "trial", 'How', "left");
            tc.verifyEqual(got.trial, [1; 2; 3]);
            tc.verifyEqual(got.emg, [0.1; 0.2; NaN]);
        end

        function test_scidb_merge_carries_name_and_how(tc)
            m = scidb.Merge(table([1], 'VariableNames', {'x'}), ...
                table([2], 'VariableNames', {'y'}), name="xy", how="outer");
            tc.verifyEqual(m.name, "xy");
            tc.verifyEqual(m.how, "outer");
        end

    end
end
