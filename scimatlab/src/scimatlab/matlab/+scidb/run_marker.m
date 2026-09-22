function cleanup = run_marker(action, marker_dir, run_id, varargin)
%SCIDB.RUN_MARKER  Report that this run started, and how it ended.
%
%   CLEANUP = scidb.run_marker('begin', MARKER_DIR, RUN_ID)
%   scidb.run_marker('finish', MARKER_DIR, RUN_ID, OK, IDENTIFIER, MESSAGE)
%
%   A script dispatched to the MathWorks MATLAB terminal is invisible to
%   whatever dispatched it: the text is handed over and nothing comes back.
%   The GUI therefore used to report success the moment the script was
%   delivered — before MATLAB had run a line, and whether or not it then
%   failed. These markers are how the run answers for itself.
%
%   'finish' writes <RUN_ID>.done and is the DETERMINISTIC report: the
%   generated script calls it on its success path and again in its catch,
%   where it carries MATLAB's own identifier and message.
%
%   'begin' also returns an onCleanup object, which is a BEST-EFFORT net for
%   the one path a catch block cannot see: Ctrl-C, which MATLAB does not
%   make catchable. Be precise about when it fires, because the obvious
%   reading is wrong here: the GUI dispatches with run('<file>.m'), which
%   evaluates the script in the CALLER's workspace, so the cleanup variable
%   outlives the script. It therefore fires when that variable is cleared or
%   replaced — which the script does itself on its success path, and which
%   the NEXT run's 'begin' does for a run that was interrupted. So a Ctrl-C'd
%   run is reported as interrupted at the start of the following run, not at
%   the moment it was interrupted.
%
%   That leaves a real window in which a Ctrl-C'd run has no .done at all.
%   It is covered from the other side, without MATLAB's help: the watcher
%   asks who holds the DuckDB file and whether that process is still alive
%   (scistack_gui.db.probe_lock_holder), and reports a run whose holder has
%   gone as *unknown* — never as success.
%
%   MARKER_DIR is <db stem>.runs beside the database — the Python side owns
%   that convention (scidb.run_markers.marker_dir) and passes the path in.
%
%   **Never throws.** A marker is diagnostics about a run, and failing to
%   write one must not fail the run it describes. Every failure is logged
%   through scidb.Log and swallowed.
%
%   The file format is one line of JSON, defined by scidb.run_markers on the
%   Python side and pinned from both directions by
%   scistack-gui/tests/test_run_markers.py.

    arguments
        action (1,:) char
        marker_dir (1,:) char
        run_id (1,:) char
    end
    arguments (Repeating)
        varargin
    end

    cleanup = [];
    switch lower(action)
        case 'begin'
            write_started(marker_dir, run_id);
            % The cleanup fires on EVERY exit path. It checks for an
            % already-written .done first, so the normal path (where
            % 'finish' has already reported the real outcome) does not
            % overwrite a good answer with "interrupted".
            cleanup = onCleanup(@() write_interrupted(marker_dir, run_id));
        case 'finish'
            ok = true;
            identifier = '';
            message = '';
            if numel(varargin) >= 1, ok = logical(varargin{1}); end
            if numel(varargin) >= 2, identifier = char(varargin{2}); end
            if numel(varargin) >= 3, message = char(varargin{3}); end
            write_done(marker_dir, run_id, ok, identifier, message, false);
        otherwise
            scidb.Log.warn('run_marker: unknown action ''%s'' (ignored)', action);
    end
end


function write_started(marker_dir, run_id)
    try
        ensure_dir(marker_dir);
        payload = sprintf( ...
            '{"version":1,"run_id":"%s","pid":%d,"at":%.3f}', ...
            escape_json(run_id), feature('getpid'), posix_time());
        write_atomic(fullfile(marker_dir, [run_id '.started']), payload);
    catch marker_err__
        % Deliberately not rethrown: see the header. The run is more
        % important than the report about the run.
        scidb.Log.warn('run_marker: could not write .started for %s: %s', ...
            run_id, marker_err__.message);
    end
end


function write_done(marker_dir, run_id, ok, identifier, message, interrupted)
    try
        ensure_dir(marker_dir);
        if ok
            ok_text = 'true';
        else
            ok_text = 'false';
        end
        if interrupted
            interrupted_text = 'true';
        else
            interrupted_text = 'false';
        end
        payload = sprintf( ...
            ['{"version":1,"run_id":"%s","ok":%s,"interrupted":%s,' ...
             '"identifier":"%s","message":"%s","at":%.3f}'], ...
            escape_json(run_id), ok_text, interrupted_text, ...
            escape_json(identifier), escape_json(message), posix_time());
        write_atomic(fullfile(marker_dir, [run_id '.done']), payload);
    catch marker_err__
        scidb.Log.warn('run_marker: could not write .done for %s: %s', ...
            run_id, marker_err__.message);
    end
end


function write_interrupted(marker_dir, run_id)
%WRITE_INTERRUPTED  The onCleanup path: report only if nobody else has.
%
%   Reaching here with no .done on disk means the script did not get to its
%   own 'finish' call — an error that escaped, or a Ctrl-C. Either way the
%   run did not complete, and saying so is the entire point.
    try
        if exist(fullfile(marker_dir, [run_id '.done']), 'file') == 2
            return;  % 'finish' already reported the real outcome.
        end
        write_done(marker_dir, run_id, false, 'SciStack:runInterrupted', ...
            'The MATLAB script stopped before finishing (interrupted, or an error escaped).', ...
            true);
    catch
        % Nothing useful to do in a cleanup handler.
    end
end


function ensure_dir(marker_dir)
    if exist(marker_dir, 'dir') ~= 7
        [ok, msg] = mkdir(marker_dir);
        if ~ok
            error('SciStack:markerDir', ...
                'could not create marker directory %s: %s', marker_dir, msg);
        end
    end
end


function write_atomic(target, payload)
%WRITE_ATOMIC  Write via a temp file and rename.
%
%   The reader polls, so it would otherwise routinely catch a file
%   mid-write. It tolerates that (unparseable JSON means "look again"), but
%   a rename makes the file appear complete or not at all, which turns a
%   retry loop into a single read.
    temp_path = [target '.tmp'];
    fid = fopen(temp_path, 'w');
    if fid < 0
        error('SciStack:markerWrite', 'could not open %s for writing', temp_path);
    end
    closer = onCleanup(@() fclose_if_open(fid));
    fprintf(fid, '%s\n', payload);
    clear closer;  % flush and close before the rename
    [ok, msg] = movefile(temp_path, target, 'f');
    if ~ok
        error('SciStack:markerWrite', ...
            'could not move %s to %s: %s', temp_path, target, msg);
    end
end


function fclose_if_open(fid)
    try
        if ~isempty(fopen(fid))
            fclose(fid);
        end
    catch
        % Already closed.
    end
end


function t = posix_time()
%POSIX_TIME  Seconds since the epoch, UTC.
%
%   posixtime() needs a zoned datetime; 'now' is unzoned local time, and
%   handing that to posixtime() silently reads as UTC — a whole-timezone
%   error in a field the reader compares against Python's time.time().
    t = posixtime(datetime('now', 'TimeZone', 'UTC'));
end


function out = escape_json(text)
%ESCAPE_JSON  Make TEXT safe inside a JSON string literal.
%
%   MATLAB error messages routinely contain quotes, backslashes (Windows
%   paths) and newlines, every one of which would produce a marker the
%   reader cannot parse — i.e. a completed run reported as "unknown".
    out = char(text);
    out = strrep(out, '\', '\\');
    out = strrep(out, '"', '\"');
    out = strrep(out, sprintf('\r'), '\r');
    out = strrep(out, sprintf('\n'), '\n');
    out = strrep(out, sprintf('\t'), '\t');
end
