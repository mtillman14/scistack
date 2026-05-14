function hash = hash_function(fcn)
%HASH_FUNCTION  Compute a SHA-256 hash identifying a MATLAB function.
%
%   hash = scidb.internal.hash_function(@my_function)
%
%   For named functions, hashes the full source code of the .m file.
%   Anonymous functions are not supported (error).
%
%   The hashing format itself lives in Python
%   (sci_matlab.bridge.compute_matlab_function_hash) so it can be tweaked
%   centrally without divergence between MATLAB-side and GUI-side
%   consumers. MATLAB only reads the source file and forwards the bytes
%   across the bridge.
%
%   Returns a 64-character hex string.

    info = functions(fcn);

    if strcmp(info.type, 'anonymous')
        error('scidb:AnonymousFunction', ...
            ['Anonymous functions are not supported by scidb.LineageFcn. ' ...
             'Use a named function defined in an .m file instead.']);
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
    hash = char(py.sci_matlab.bridge.compute_matlab_function_hash( ...
        source, func_name, false));
end
