function reject_csv_spec_args(receiver_name, args)
%REJECT_CSV_SPEC_ARGS  Error if a variable spec was passed to to_csv() as an arg.
%
%   to_csv() exports the thing it is called on; it does not absorb other
%   variables. A BaseVariable / Merge / Fixed / Variant in the name-value
%   args almost always means the caller wanted a join — point them at
%   Merge(...).to_csv(...).
%
%   ARGS is the to_csv varargin cell. RECEIVER_NAME is used in the message.

    for i = 1:numel(args)
        a = args{i};
        if isa(a, 'scidb.BaseVariable') || isa(a, 'scidb.Merge') || ...
           isa(a, 'scidb.Fixed') || isa(a, 'scidb.Variant')
            spec_name = class(a);  % e.g. 'StepLength' or 'scidb.Merge'
            error('scidb:ToCsvError', ...
                ['to_csv() exports a single variable (or one Merge), not %s plus ', ...
                 'extra variables. To export several variables together, build ', ...
                 'the Merge explicitly and call to_csv on it, e.g. ', ...
                 'scidb.Merge(%s(), %s(), name="...").to_csv(...). Everything after the ', ...
                 'filename must be name-value metadata (e.g. subject=1, where=...).'], ...
                receiver_name, receiver_name, spec_name);
        end
    end
end
