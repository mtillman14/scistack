function results = run_tests()
%RUN_TESTS  Run all MATLAB integration tests for scimatlab.
%
%   results = run_tests()
%
%   Sets up paths, discovers all Test*.m files in scifor/, scidb/, and
%   scihist/ subdirectories, and runs them using MATLAB's unittest framework.
%
%   Example:
%       cd scimatlab/tests/matlab
%       results = run_tests();

    % clear functions;
    % rehash path;
    % 
    % pythonPath = '/Users/mitchelltillman/Documents/test-project/.venv/bin/python3.11';
    % pyenv('Version', pythonPath);

    this_dir = fileparts(mfilename('fullpath'));
    addpath(genpath(this_dir));

    % Set up MATLAB and Python paths
    run(fullfile(this_dir, 'setup_paths.m'));

    % Discover and run all test classes (scifor + scidb + scihist layers)
    import matlab.unittest.TestSuite
    import matlab.unittest.TestRunner
    import matlab.unittest.plugins.DiagnosticsValidationPlugin

    % addpath(genpath(...)) above put scifor/, scidb/ and scihist/ on ONE
    % MATLAB path, and MATLAB resolves a classdef by NAME, not by folder. Two
    % test classes sharing a name therefore shadow each other: whichever folder
    % wins the path order supplies the class for BOTH suites, so one folder's
    % tests run against the other folder's class. Enforce uniqueness rather
    % than trust it — the convention is already in use everywhere else
    % (TestSciforForEach vs TestForEach).
    assert_unique_test_class_names(this_dir, {'scifor', 'scidb', 'scihist'});

    suite_scifor  = TestSuite.fromFolder(fullfile(this_dir, 'scifor'));
    suite_scidb   = TestSuite.fromFolder(fullfile(this_dir, 'scidb'));
    suite_scihist = TestSuite.fromFolder(fullfile(this_dir, 'scihist'));
    suite = [suite_scifor, suite_scidb, suite_scihist];
    runner = TestRunner.withTextOutput('Verbosity', 3);

    % TestResult.Details.DiagnosticRecord is populated ONLY by
    % DiagnosticsRecordingPlugin, and TestRunner.withTextOutput does not
    % install it on every release — without it the per-test failure report
    % below has nothing to print and a failing run's only account of itself is
    % the console scrollback. Add it explicitly.
    try
        runner.addPlugin(matlab.unittest.plugins.DiagnosticsRecordingPlugin);
    catch plugin_err
        fprintf(2, 'Could not add DiagnosticsRecordingPlugin: %s\n', ...
            plugin_err.message);
    end

    % Belt and braces: mirror the whole Verbosity-3 console transcript to a
    % file as well. The structured report is the thing to read, but if the
    % plugin above is unavailable the transcript is the only place the
    % actual-vs-expected text and error stacks survive.
    console_path = fullfile(this_dir, 'test-console.log');
    try
        if isfile(console_path)
            delete(console_path);
        end
        diary(console_path);
        % onCleanup, not a plain diary('off') after the run: if runner.run
        % throws, an unclosed diary keeps capturing into the next session.
        diary_guard = onCleanup(@() diary('off')); %#ok<NASGU>
    catch
        console_path = '';
    end

    results = runner.run(suite);

    % Summary
    fprintf('\n=== Test Summary ===\n');
    fprintf('Total:  %d\n', numel(results));
    fprintf('Passed: %d\n', sum([results.Passed]));
    fprintf('Failed: %d\n', sum([results.Failed]));
    fprintf('Errors: %d\n', sum(~[results.Passed] & ~[results.Failed]));

    if all([results.Passed])
        fprintf('\nAll tests passed.\n');
    else
        fprintf('\nFailing tests:\n');
        failed = results(~[results.Passed]);
        for i = 1:numel(failed)
            fprintf('  - %s\n', failed(i).Name);
        end

        % The summary table above names WHICH tests failed but not WHY: the
        % 'Failed by verification. / Errored.' column is all it carries, and
        % the per-test diagnostics are scattered through thousands of lines of
        % Verbosity-3 console output. Collect them into one file so a failure
        % can be diagnosed (or handed over) without scrolling the console.
        report_path = fullfile(this_dir, 'test-failures.txt');
        try
            write_failure_report(failed, report_path);
            fprintf('\nFull failure diagnostics written to:\n  %s\n', report_path);
        catch report_err
            fprintf(2, '\nCould not write failure report: %s\n', report_err.message);
        end
    end
end


function assert_unique_test_class_names(this_dir, subdirs)
%ASSERT_UNIQUE_TEST_CLASS_NAMES  Refuse to run with shadowed test classes.
%   Every Test*.m across the layer folders must have a unique basename,
%   because they all share one MATLAB path and a classdef is resolved by name.
%   A collision does not error on its own — it silently runs one folder's
%   suite against the other folder's class — so it has to be caught here.
    seen = containers.Map('KeyType', 'char', 'ValueType', 'any');
    dupes = {};
    for d = 1:numel(subdirs)
        files = dir(fullfile(this_dir, subdirs{d}, 'Test*.m'));
        for f = 1:numel(files)
            [~, cls] = fileparts(files(f).name);
            if isKey(seen, cls)
                dupes{end+1} = sprintf('%s (%s/ and %s/)', ...
                    cls, seen(cls), subdirs{d}); %#ok<AGROW>
            else
                seen(cls) = subdirs{d};
            end
        end
    end
    if ~isempty(dupes)
        error('run_tests:duplicateTestClassName', ...
            ['Test class name(s) defined in more than one layer folder: %s.\n' ...
             'They share one MATLAB path, so one shadows the other and a ' ...
             'folder''s suite runs against the wrong class. Rename one — the ' ...
             'convention is to prefix the scifor-layer copy (e.g. ' ...
             'TestSciforForEach vs TestForEach).'], ...
            strjoin(dupes, ', '));
    end
end


function write_failure_report(failed, report_path)
%WRITE_FAILURE_REPORT  Dump every non-passing test's full diagnostics to a file.
%   One section per test: its name, its outcome flags, and the verbatim
%   Report text of each diagnostic record the runner attached (verification
%   failures carry the actual-vs-expected comparison; errors carry the MATLAB
%   exception identifier, message and stack).
%
%   Defensive throughout: Details/DiagnosticRecord is not part of the
%   documented TestResult API in every MATLAB release, so a missing field
%   degrades to "no diagnostic records" rather than masking the test failures
%   behind a reporting error.
    fid = fopen(report_path, 'w');
    if fid < 0
        error('run_tests:reportOpenFailed', ...
            'Could not open %s for writing', report_path);
    end
    closer = onCleanup(@() fclose(fid)); %#ok<NASGU>

    fprintf(fid, 'MATLAB test failure diagnostics\n');
    fprintf(fid, 'Generated: %s\n', ...
        char(datetime('now', 'Format', 'yyyy-MM-dd HH:mm:ss')));
    fprintf(fid, '%d failing test(s)\n', numel(failed));

    for i = 1:numel(failed)
        r = failed(i);
        fprintf(fid, '\n%s\n', repmat('=', 1, 78));
        fprintf(fid, '[%d/%d] %s\n', i, numel(failed), r.Name);
        fprintf(fid, 'Passed=%d  Failed=%d  Incomplete=%d  Duration=%.3fs\n', ...
            r.Passed, r.Failed, r.Incomplete, r.Duration);
        fprintf(fid, '%s\n', repmat('=', 1, 78));

        recs = [];
        try
            recs = r.Details.DiagnosticRecord;
        catch
            % Field absent on this release — fall through to the notice below.
        end
        if isempty(recs)
            % Say WHAT was there instead of just what was missing: a bare
            % "(none)" gives the next reader no way to tell a release without
            % DiagnosticRecord from a plugin that failed to attach.
            fprintf(fid, '(no diagnostic records attached)\n');
            try
                fprintf(fid, 'Details fields present: %s\n', ...
                    strjoin(fieldnames(r.Details)', ', '));
            catch
                fprintf(fid, 'Details is not a struct (class=%s)\n', ...
                    class(r.Details));
            end
            fprintf(fid, ['See the full console transcript in ' ...
                'test-console.log for this test''s diagnostics.\n']);
            continue;
        end
        for k = 1:numel(recs)
            rec = recs(k);
            fprintf(fid, '\n--- record %d/%d', k, numel(recs));
            try
                fprintf(fid, ': %s (%s)', string(rec.Event), string(rec.EventScope));
            catch
            end
            fprintf(fid, ' ---\n');
            try
                fprintf(fid, '%s\n', rec.Report);
            catch
                fprintf(fid, '(record has no Report text)\n');
            end
        end
    end
end
