function failure = failure_from_mexception(err, fn_name, metadata_str, stage, no_data)
%SCIFOR.FAILURE_FROM_MEXCEPTION  The per-combo failure record, from an MException.
%
%   FAILURE = scifor.failure_from_mexception(ERR, FN_NAME, METADATA_STR, STAGE, NO_DATA)
%
%   The MATLAB twin of Python's scifor.failures.failure_from_exception, in
%   the SAME wire shape (ComboFailure.to_dict), so the GUI reads failures
%   from either language without knowing which ran:
%
%     fn_name, metadata (struct), combo (char), stage ('prepare_inputs' |
%     'call'), exc_type (the MException identifier, or 'MException' when it
%     has none), message, reason ("exc_type: message"), frames (struct array,
%     OUTERMOST first like Python), user_frame (struct or []), no_data.
%
%   ERR.stack is innermost-first; it is reversed. Frames classify through
%   scifor.frame_origin (the one MATLAB classifier). The SciStack frames
%   that lead INTO the user's function are trimmed, as in Python; a stack
%   with no user frame is kept whole.
%
%   Pinned against the Python shape by
%   scimatlab/tests/test_matlab_failure_record_shape.py.

    MESSAGE_MAX_CHARS = 2000;
    FRAMES_MAX = 50;

    exc_type = char(err.identifier);
    if isempty(exc_type)
        exc_type = 'MException';
    end
    message = char(err.message);
    if numel(message) > MESSAGE_MAX_CHARS
        message = [message(1:MESSAGE_MAX_CHARS - 1) char(8230)];
    end

    stack = err.stack;
    frames = struct('file', {}, 'line', {}, 'function', {}, 'code', {}, 'origin', {});
    for k = numel(stack):-1:1
        frames(end + 1) = struct( ...
            'file', char(stack(k).file), ...
            'line', double(stack(k).line), ...
            'function', char(stack(k).name), ...
            'code', '', ...
            'origin', scifor.frame_origin(stack(k).file)); %#ok<AGROW>
    end
    first_user = find(~strcmp({frames.origin}, 'framework'), 1);
    if ~isempty(first_user)
        frames = frames(first_user:end);
    end
    if numel(frames) > FRAMES_MAX
        frames = frames(end - FRAMES_MAX + 1:end);
    end
    user_idx = find(strcmp({frames.origin}, 'user'), 1, 'last');
    if isempty(user_idx)
        user_frame = [];
        if ~no_data
            scifor.Log.debug('failure_from_mexception: no user frame in %s (%d frame(s))', ...
                exc_type, numel(frames));
        end
    else
        user_frame = frames(user_idx);
    end

    failure = struct( ...
        'fn_name', char(fn_name), ...
        'metadata', struct(), ...
        'combo', char(metadata_str), ...
        'stage', char(stage), ...
        'exc_type', exc_type, ...
        'message', message, ...
        'reason', sprintf('%s: %s', exc_type, message), ...
        'frames', frames, ...
        'user_frame', user_frame, ...
        'no_data', logical(no_data));
end
