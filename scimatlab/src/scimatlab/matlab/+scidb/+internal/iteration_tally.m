function out = iteration_tally(action, summary)
%SCIDB.INTERNAL.ITERATION_TALLY  Count failed iterations across one run.
%
%   scidb.internal.iteration_tally('reset')
%   scidb.internal.iteration_tally('add', SUMMARY)
%   T = scidb.internal.iteration_tally('get')
%
%   scifor.for_each never throws on a failed iteration: it logs the failure
%   and continues. A GUI-dispatched script therefore finished "done" even
%   when every iteration raised (scistack.log 2026-10-20: load_cbrace,
%   completed=0, failed=1, verdict=done). The Python Run fails the run when
%   any iteration fails (scistack_gui/api/run.py, combo_totals); this is the
%   MATLAB half of that rule.
%
%   +scidb/for_each.m hands scifor.for_each an '_summary_fn' that calls
%   'add' with scifor's end-of-loop summary. The generated script calls
%   'reset' before its first for_each and 'get' after its last, and raises
%   SciStack:iterationsFailed when T.failed > 0, so the run's catch writes a
%   failure marker carrying T.message.
%
%   SUMMARY fields: fn_name, completed, failed, no_data, total,
%   first_reason, and (since 2026-10-26) failures, stopped, stop_error. T adds
%   message (empty when nothing failed).
%
%   G = scidb.internal.iteration_tally('failures')
%     Every non-no-data failure group of the run so far (cell of structs in
%     the wire shape of Python's scifor.failures.FailureGroup.to_dict). The
%     generated script's catch hands it to scidb.run_marker, so the GUI shows
%     a MATLAB run's failures like a Python run's.
%   scidb.internal.iteration_tally('add_group', GROUP)
%     One run-level failure group (wire shape), e.g. a Python save error
%     that escaped for_each_save (scimatlab.bridge.run_failure_group_json):
%     the MATLAB twin of the GUI's RunFailures.add_exception.
%   E = scidb.internal.iteration_tally('take_stop')
%     The error an on_error='stop' loop ended on (or []), cleared on read.
%     scidb.for_each saves the completed combos, then rethrows it.

    persistent tally
    if isempty(tally) || strcmpi(action, 'reset')
        tally = struct('completed', 0, 'failed', 0, 'no_data', 0, ...
            'total', 0, 'parts', {{}}, 'failures', {{}}, 'stop_error', []);
    end
    out = [];
    switch lower(action)
        case 'reset'
            % done above
        case 'add'
            tally.completed = tally.completed + summary.completed;
            tally.failed = tally.failed + summary.failed;
            tally.no_data = tally.no_data + summary.no_data;
            tally.total = tally.total + summary.total;
            if summary.failed > 0
                part = sprintf('%s: %d of %d iteration(s) failed', ...
                    summary.fn_name, summary.failed, summary.total);
                if ~isempty(summary.first_reason)
                    part = sprintf('%s (first: %s)', part, summary.first_reason);
                end
                tally.parts{end+1} = part;
            end
            if isfield(summary, 'failures')
                for g = 1:numel(summary.failures)
                    % Capped: a session that never resets (direct calls,
                    % not a GUI script) must not grow this without bound.
                    if ~summary.failures{g}.no_data && numel(tally.failures) < 200
                        tally.failures{end+1} = summary.failures{g};
                    end
                end
            end
            if isfield(summary, 'stop_error') && ~isempty(summary.stop_error)
                tally.stop_error = summary.stop_error;
            end
        case 'add_group'
            if numel(tally.failures) < 200
                tally.failures{end+1} = summary;
            end
        case 'get'
            out = rmfield(tally, {'parts', 'failures', 'stop_error'});
            out.message = strjoin(tally.parts, '; ');
        case 'failures'
            out = tally.failures;
        case 'take_stop'
            out = tally.stop_error;
            tally.stop_error = [];
        otherwise
            error('scidb:iteration_tally:badAction', ...
                'Unknown action ''%s'' (reset, add, add_group, get, failures, take_stop)', action);
    end
end
