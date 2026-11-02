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
%   Runs once per failure frame, so the crossing cost does not matter.

    origin = char(py.scifor.failures.frame_origin(char(file), py.tuple({matlabroot})));
end
