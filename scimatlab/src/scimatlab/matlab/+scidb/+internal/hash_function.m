function [hash, source] = hash_function(fcn)
%HASH_FUNCTION  Compute a SHA-256 hash identifying a MATLAB function.
%
%   hash = scidb.internal.hash_function(@my_function)
%   [hash, source] = scidb.internal.hash_function(@my_function)
%
%   SOURCE is the exact .m text that was hashed, so the save can store the
%   code behind the hash (cleanup-audit F35: MATLAB source was never
%   captured, only its digest). '' when the file is not UTF-8.
%
%   For named functions, hashes the full source code of the .m file.
%   Anonymous functions are not supported (error).
%
%   The hash has ONE owner: scimatlab.source_hash (SHA-256 of the file's raw
%   bytes), which the GUI uses too. MATLAB only resolves WHICH file — it
%   never reads or decodes it for hashing, because fileread's decoding
%   (Windows code pages, a BOM) is exactly what used to make the two
%   readers disagree.
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

    result = cell(py.scimatlab.source_hash.matlab_function_source(fpath, func_name));
    hash = char(result{1});
    source = char(result{2});
end
