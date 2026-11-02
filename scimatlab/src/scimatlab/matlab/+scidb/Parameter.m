classdef Parameter < scifor.EachOf
%SCIDB.PARAMETER  A named pipeline configuration value: one or more values.
%
%   P = scidb.Parameter(V1)
%   P = scidb.Parameter(V1, V2, ...)
%   P = scidb.Parameter(V1, ..., description=DESC)
%
%   Replaces the former scidb.Constant (one value) and scifor.Sweep (many).
%   They were two constructs for one idea, which forced an entity to change
%   *kind* the moment a second value was added. One class, one node type in
%   the GUI, one thing to write:
%
%       % scistack_entities.m
%       sampling_rate  = scidb.Parameter(1000, description='Recording rate');
%       window_seconds = scidb.Parameter(10, 20, 30);
%
%   Values can be built programmatically -- it is plain varargs:
%
%       thresholds = scidb.Parameter(num2cell(10:10:50){:});
%       scales     = scidb.Parameter(1, 2, 4, 8, 16);
%
%   A Parameter IS a scifor.EachOf, so scidb.for_each fans it out with no
%   special handling: each value becomes an independent call with that
%   concrete value, and isa(P, 'scifor.EachOf') is true. **A one-value
%   Parameter is not a special case** -- EachOf expansion has no branch for
%   it, so scidb.Parameter(30) records byte-identical version_keys to a bare
%   30, and identical to Python's scidb.Parameter(30). That parity is
%   load-bearing: without it the same pipeline forks in history depending on
%   which language ran it.
%
%   Use .values to get the value list in ordinary MATLAB code.
%
%   A Parameter may hold NO values at all:
%
%       window_seconds = scidb.Parameter();   % declared, not yet valued
%
%   That is the state the GUI's "New parameter" form produces -- it collects
%   a name and nothing else -- and it used to be papered over by scaffolding
%   a placeholder 0 into source, indistinguishable from a real value once
%   written. It is legal at rest and an error at execution:
%   scifor.require_alternatives refuses an empty axis at for_each expansion,
%   because a zero-length axis would iterate zero times and write nothing
%   while appearing to succeed.
%
%   Mirrors Python's scidb.Parameter (scidb/src/scidb/model/parameter.py). See
%   docs/claude/entity-editability-model.md.

    properties (SetAccess = immutable)
        description char = ''
        %DESCRIPTION  Human-readable note, surfaced in the GUI sidebar.
    end

    methods
        function obj = Parameter(varargin)
        %PARAMETER  Construct a Parameter.
        %
        %   P = scidb.Parameter(V1, V2, ..., description=DESC)

            % Peel a trailing description=... (R2021b name=value) or
            % 'description', ... pair off the positional values, so every
            % remaining argument is a real alternative. Done before the
            % superclass call because scifor.EachOf treats ALL of its
            % arguments as alternatives.
            args = varargin;
            desc = '';
            if numel(args) >= 2 && (ischar(args{end-1}) || isstring(args{end-1})) ...
                    && strcmp(char(args{end-1}), 'description')
                desc = char(args{end});
                args = args(1:end-2);
            end

            % args may be EMPTY: a Parameter declared but not yet given a
            % value is a legal state (see the class help). Note the single
            % unconditional superclass call -- MATLAB does not allow one
            % inside a conditional branch, which is why scifor.EachOf has to
            % accept zero alternatives rather than Parameter special-casing
            % the empty construction here.
            obj@scifor.EachOf(args{:});
            obj.description = desc;
        end

        function v = values(obj)
        %VALUES  Every alternative, in declaration order.
            v = obj.alternatives;
        end

        function v = value(obj)
        %VALUE  The single wrapped value.
        %
        %   Errors for a multi-valued Parameter rather than silently
        %   returning the first -- picking one arbitrarily is how a fan-out
        %   quietly becomes a single run.
            if isempty(obj.alternatives)
                % Distinct from the "too many" case: nothing was declared
                % yet, so pointing at .values points at an empty cell.
                error('scidb:Parameter:NoValue', ...
                    ['value needs a value, and this Parameter has none yet ' ...
                     '-- give it at least one value.']);
            end
            if numel(obj.alternatives) ~= 1
                error('scidb:Parameter:NotSingleValued', ...
                    ['value is only defined for a single-valued Parameter; ' ...
                     'this one has %d values. Use .values for the full list.'], ...
                    numel(obj.alternatives));
            end
            v = obj.alternatives{1};
        end

        function disp(obj)
        %DISP  Show the declared values.
            items = cell(1, numel(obj.alternatives));
            for k = 1:numel(obj.alternatives)
                items{k} = scidb.Parameter.format_value(obj.alternatives{k});
            end
            body = strjoin(items, ', ');
            if isempty(obj.description)
                fprintf('  scidb.Parameter(%s)\n', body);
            else
                fprintf('  scidb.Parameter(%s)  %% %s\n', body, obj.description);
            end
        end
    end

    methods (Static)
        function s = format_value(v)
        %FORMAT_VALUE  Short display form for disp(). Never used for
        %hashing -- version key identity comes from the unwrapped values
        %themselves, not from any string rendering of them.
            if ischar(v) || isstring(v)
                s = sprintf('''%s''', char(v));
            elseif isnumeric(v) && isscalar(v)
                s = num2str(v);
            elseif islogical(v) && isscalar(v)
                s = mat2str(v);
            else
                s = sprintf('<%s>', class(v));
            end
        end
    end
end
