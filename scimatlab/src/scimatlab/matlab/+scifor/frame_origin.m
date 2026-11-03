function origin = frame_origin(file)
%SCIFOR.FRAME_ORIGIN  'user' | 'framework' | 'library' for a stack frame's file.
%
%   Delegates to the ONE frame classifier, Python's
%   scifor.failures.frame_origin. MATLAB passes matlabroot as an extra
%   library root, so toolbox and built-in frames are 'library'. This file's
%   own directory (+scifor, +scidb) sits inside the scimatlab package, which
%   Python classifies as 'framework'. An empty file (an anonymous function
%   typed at the prompt) is the user's.
%
%   It runs while a failure is being REPORTED, so it never throws: if the
%   Python crossing fails, the frame is 'user' and a warning says why. A
%   classifier error once replaced the user's own error (2026-11-03:
%   "module 'os' has no attribute '__file__'" under MATLAB's embedded
%   Python 3.11).
%
%   Runs once per failure frame, so the crossing cost does not matter.

    try
        origin = char(py.scifor.failures.frame_origin(char(file), py.tuple({matlabroot})));
    catch cls_err
        scifor.Log.warn('frame_origin: could not classify "%s" (%s); treating it as user code', ...
            char(file), cls_err.message);
        origin = 'user';
    end
end
