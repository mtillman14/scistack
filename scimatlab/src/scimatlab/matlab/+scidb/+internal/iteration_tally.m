function out = iteration_tally(action, summary)
%SCIDB.INTERNAL.ITERATION_TALLY  Count failed iterations across one run.
%
%   scidb.internal.iteration_tally('reset')
%   scidb.internal.iteration_tally('add', SUMMARY)
%   T = scidb.internal.iteration_tally('get')
%
%   scifor.for_each never throws on a failed iteration: it logs the failure
%   and continues. A GUI-dispatched script therefore finished "done" even
%   when every iteration raised (scidb.log 2026-10-20: load_cbrace,
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
%   first_reason. T adds message (empty when nothing failed).

    persistent tally
    if isempty(tally) || strcmpi(action, 'reset')
        tally = struct('completed', 0, 'failed', 0, 'no_data', 0, ...
            'total', 0, 'parts', {{}});
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
        case 'get'
            out = rmfield(tally, 'parts');
            out.message = strjoin(tally.parts, '; ');
        otherwise
            error('scidb:iteration_tally:badAction', ...
                'Unknown action ''%s'' (reset, add, get)', action);
    end
end
