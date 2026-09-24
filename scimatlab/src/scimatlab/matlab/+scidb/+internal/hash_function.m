function [hash, source] = hash_function(fcn)
%HASH_FUNCTION  Compute a SHA-256 hash identifying a MATLAB function.
%
%   hash = scidb.internal.hash_function(@my_function)
%   [hash, source] = scidb.internal.hash_function(@my_function)
%
%   SOURCE is the exact .m text that was hashed, so the save can store the
%   code behind the hash (cleanup-audit F35: MATLAB source was never
%   captured, only its digest).
%
%   For named functions, hashes the full source code of the .m file.
%   Anonymous functions are not supported (error).
%
%   The hashing format itself lives in Python
%   (scimatlab.bridge.compute_matlab_function_hash) so it can be tweaked
%   centrally without divergence between MATLAB-side and GUI-side
%   consumers. MATLAB only reads the source file and forwards the bytes
%   across the bridge.
%
%   Returns a 64-character hex string.

    info = functions(fcn);

    if strcmp(info.type, 'anonymous')
        error('scidb:AnonymousFunction', ...
            ['Anonymous functions cannot be hashed for scidb.for_each ' ...
             'provenance. Use a named function defined in an .m file instead.']);
    end

    % Resolve the source file path
    func_name = func2str(fcn);
    fpath = which(func_name);

    if isempty(fpath)
        error('scidb:FunctionNotFound', ...
            'Cannot locate source file for function "%s".', func_name);
    end

    % Read source and forward to Python for hashing
    source = fileread(fpath);
    hash = char(py.scimatlab.bridge.compute_matlab_function_hash( ...
        source, func_name, false));
end
