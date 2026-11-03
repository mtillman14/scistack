function [blame, probable, rule] = classify_blame(identifier, frames, no_data)
%SCIFOR.CLASSIFY_BLAME  Who can fix this failure: BLAME, PROBABLE, RULE.
%
%   [BLAME, PROBABLE, RULE] = scifor.classify_blame(IDENTIFIER, FRAMES, NO_DATA)
%
%   IDENTIFIER is the MException identifier; FRAMES a struct array with an
%   'origin' field ('user' | 'framework' | 'library'), OUTERMOST first;
%   NO_DATA true for scifor:NoData.
%
%   The rule is owned by Python (scifor.failures.classify_blame). MATLAB
%   needs the MException, so this file runs the same rule natively, in the
%   same order, held to docs/claude/blame-cases.json (TestBlameCases.m):
%
%   1. no_data    -- a combo with no data. Not an error.
%   2. user_value -- a scistack:userValue error (scistack:userValue:probable
%      when no check named the cause). Python: UserValueError.
%   3. setup      -- an error() with a scifor:/scidb:/scistack: identifier:
%      SciStack refused on purpose and says what to change. Python:
%      ActionableError, checked before the frames there too.
%   4. user_code  -- the innermost non-library frame is the user's.
%   5. internal   -- anything else SciStack's code raised.
%
%   Known limit: a built-in passed directly as the function (fn = @sum)
%   that fails is classified internal, because MATLAB has no frame for the
%   call site the way Python's scifor.failures.call_user_function is.
    probable = false;
    id = char(identifier);
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
    if startsWith(id, {'scifor:', 'scidb:', 'scistack:'})
        blame = 'setup';
        rule = sprintf('SciStack raised %s on purpose', id);
        return;
    end
    if isempty(frames)
        innermost = [];
    else
        innermost = find(~strcmp({frames.origin}, 'library'), 1, 'last');
    end
    if isempty(innermost)
        blame = 'internal';
        rule = 'no non-library frame';
    elseif strcmp(frames(innermost).origin, 'user')
        blame = 'user_code';
        rule = sprintf('innermost non-library frame is user code (%s:%d)', ...
            frames(innermost).file, frames(innermost).line);
    else
        blame = 'internal';
        rule = sprintf('innermost non-library frame is SciStack code (%s:%d)', ...
            frames(innermost).file, frames(innermost).line);
    end
end
