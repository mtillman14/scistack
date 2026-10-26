function origin = frame_origin(file)
%SCIFOR.FRAME_ORIGIN  'user' | 'framework' | 'library' for a stack frame's file.
%
%   The ONE MATLAB frame classifier (Python's is scifor.failures.frame_origin).
%   framework: inside scimatlab's MATLAB directory (+scifor, +scidb, +scihist
%   ...). library: inside matlabroot (toolboxes, built-ins). Everything else,
%   including an empty file (an anonymous function typed at the prompt), is
%   the user's.

    persistent framework_root matlab_root
    if isempty(framework_root)
        here = fileparts(mfilename('fullpath'));   % .../matlab/+scifor
        framework_root = normalize(fileparts(here));
        matlab_root = normalize(matlabroot);
    end
    path = normalize(file);
    if isempty(path)
        origin = 'user';
    elseif startsWith(path, [framework_root filesep])
        origin = 'framework';
    elseif startsWith(path, [matlab_root filesep])
        origin = 'library';
    else
        origin = 'user';
    end
end


function out = normalize(p)
    out = char(p);
    if ispc
        out = lower(strrep(out, '/', '\'));
    end
end
