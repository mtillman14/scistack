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
    [blame, probable, rule] = classify_blame(err, frames, no_data);
    scifor.Log.debug('failure_from_mexception: %s in %s -> blame=%s via %s', ...
        exc_type, char(fn_name), blame, rule);

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
        'no_data', logical(no_data), ...
        'blame', blame, ...
        'probable', probable);
end


function [blame, probable, rule] = classify_blame(err, frames, no_data)
%CLASSIFY_BLAME  Who can fix this failure. The MATLAB twin of Python's
%   scifor.failures.classify_blame, in the same order:
%
%   1. no_data    -- scifor:NoData (a combo with no data). Not an error.
%   2. user_value -- a scistack:userValue error: SciStack failed handling a
%      value the user's function returned (scistack:userValue:probable when
%      no check named the cause).
%   3. user_code  -- the innermost non-library frame is the user's.
%   4. setup      -- SciStack's own code raised it on purpose: an error()
%      call with a scifor:/scidb:/scihist:/scistack: identifier, whose message
%      says what to change (Python's ActionableError).
%   5. internal   -- anything else SciStack's code raised.
%
%   Known limit: a built-in passed directly as the function (fn = )
%   that fails is classified internal, because MATLAB has no frame for the
%   call site the way Python's scifor.failures.call_user_function is.
    probable = false;
    id = char(err.identifier);
    if no_data
        blame = 'no_data'; rule = 'scifor:NoData';
        return;
    end
    if startsWith(id, 'scistack:userValue')
        blame = 'user_value';
        probable = strcmp(id, 'scistack:userValue:probable');
        rule = id;
        return;
    end
    innermost = find(~strcmp({frames.origin}, 'library'), 1, 'last');
    if ~isempty(innermost) && strcmp(frames(innermost).origin, 'user')
        blame = 'user_code';
        rule = sprintf('innermost non-library frame is user code (%s:%d)', ...
            frames(innermost).file, frames(innermost).line);
    elseif startsWith(id, {'scifor:', 'scidb:', 'scihist:', 'scistack:'})
        blame = 'setup';
        rule = sprintf('SciStack raised %s on purpose', id);
    else
        blame = 'internal';
        rule = 'innermost non-library frame is SciStack code';
    end
end
