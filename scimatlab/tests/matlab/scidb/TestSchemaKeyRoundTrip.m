classdef TestSchemaKeyRoundTrip < matlab.unittest.TestCase
%TESTSCHEMAKEYROUNDTRIP  What class a schema-key column has inside a MATLAB
%   function and in the result table (F10, plan stage 5).
%
%   Python's prepare stringifies schema-key values; +scidb/for_each.m casts
%   the columns back to the classes the caller passed (coerce_meta_columns,
%   then the result restore). The review asked whether that round trip is
%   still needed now that Python's key-match rule owns comparison.
%
%   test_default_keeps_caller_classes pins today's contract.
%   test_diagnostic_without_coerce runs the same calls with
%   SCISTACK_NO_KEY_COERCE=1 and PRINTS one line per key class:
%       F10 <class>: function sees <class>/<value> (default <class>/<value>),
%           result <class> (default <class>) -> SAME | DIFFERENT
%   It never fails: its output decides whether the casts can go
%   (all SAME) or must stay (any DIFFERENT).

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
            setenv('SCISTACK_NO_KEY_COERCE', '');
        end
    end

    methods (TestMethodTeardown)
        function cleanup(testCase)
            setenv('SCISTACK_NO_KEY_COERCE', '');
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

        function test_default_keeps_caller_classes(testCase)
            for c = key_classes()
                [seen, result] = run_once(c{1}.value, c{1}.session);
                testCase.verifyEqual(seen.class, class(c{1}.value), c{1}.label);
                testCase.verifyEqual(class(result.subject), class(c{1}.value), c{1}.label);
            end
        end

        function test_diagnostic_without_coerce(testCase)
            for c = key_classes()
                [seen_on, result_on] = run_once(c{1}.value, c{1}.session);
                setenv('SCISTACK_NO_KEY_COERCE', '1');
                [seen_off, result_off] = run_once(c{1}.value, c{1}.session + "_off");
                setenv('SCISTACK_NO_KEY_COERCE', '');
                same = strcmp(seen_on.class, seen_off.class) ...
                    && isequal(string(seen_on.value), string(seen_off.value)) ...
                    && strcmp(class(result_on.subject), class(result_off.subject));
                verdict = "DIFFERENT";
                if same
                    verdict = "SAME";
                end
                fprintf(['F10 %s: function sees %s/%s (default %s/%s), result %s ' ...
                    '(default %s) -> %s\n'], c{1}.label, seen_off.class, ...
                    string(seen_off.value), seen_on.class, string(seen_on.value), ...
                    class(result_off.subject), class(result_on.subject), verdict);
            end
            testCase.verifyTrue(true);
        end

    end
end


function cs = key_classes()
    cs = { ...
        struct('label', "double", 'value', 1, 'session', "A"), ...
        struct('label', "int32", 'value', int32(2), 'session', "B"), ...
        struct('label', "string", 'value', "S1", 'session', "C"), ...
        struct('label', "padded string", 'value', "01", 'session', "D"), ...
        struct('label', "char", 'value', 'S2', 'session', "E")};
end


function [seen, result] = run_once(subject, session)
    tbl = table(["L"; "R"], [10; 20], 'VariableNames', {'side', 'force'});
    GaitData().save(tbl, 'subject', subject, 'session', session);
    setappdata(0, 'f10_seen', []);
    result = scidb.for_each(@record_subject, struct('data', GaitData()), ...
        {MergedResult()}, 'as_table', true, 'subject', subject, 'session', session);
    seen = getappdata(0, 'f10_seen');
    if isempty(seen)
        seen = struct('class', '<not called>', 'value', "");
    end
end


function n = record_subject(data)
    col = data.subject;
    if iscell(col)
        v = col{1};
    else
        v = col(1);
    end
    setappdata(0, 'f10_seen', struct('class', class(col), 'value', string(v)));
    n = height(data);
end
