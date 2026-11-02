classdef Merge
%SCIDB.MERGE  Combine multiple variables into a single table input for for_each.
%
%   Merges 2+ variable inputs into a single MATLAB table that is passed
%   as one argument to the function. Each constituent is loaded individually
%   per iteration, converted to a keyed table (schema key columns + data
%   columns), and inner-joined on common schema keys.
%
%   When constituents return multiple records (e.g. iterating at a coarse
%   level), the records are joined by their shared schema keys. Unmatched
%   rows are dropped (inner join). When each constituent returns exactly
%   one record, this reduces to a simple column-wise merge.
%
%   Table variables contribute all their columns. Array and scalar variables
%   are added as a column named after the variable class name.
%
%   Constituents can be:
%   - BaseVariable instances (loaded from the database)
%   - scidb.Fixed wrappers (loaded with overridden metadata)
%   - BaseVariable instances with column selection (e.g. MyVar("col"))
%   - Combinations: scidb.Fixed(MyVar("col"), session="BL")
%
%   Properties:
%       var_specs - Cell array of variable specs to merge
%
%   Example:
%       % Merge a table with a computed column
%       scidb.for_each(@analyze, ...
%           struct('data', scidb.Merge(GaitData(), PareticSide())), ...
%           {Result()}, ...
%           subject=[1 2 3]);
%
%       % Merge with Fixed override
%       scidb.for_each(@analyze, ...
%           struct('data', scidb.Merge( ...
%               GaitData("force"), ...
%               scidb.Fixed(PareticSide(), session="BL"))), ...
%           {Result()}, ...
%           subject=[1 2 3], session=["A" "B"]);
%
%       % Multi-record merge (joined by schema keys)
%       scidb.for_each(@analyze, ...
%           struct('data', scidb.Merge(GaitData(), PareticSide())), ...
%           {Result()}, ...
%           subject=[1 2 3]);

    properties (SetAccess = private)
        var_specs  cell  % Cell array of variable specs
    end

    methods
        function obj = Merge(varargin)
        %MERGE  Construct a Merge wrapper.
        %
        %   M = scidb.Merge(VarA(), VarB(), ...)
        %
        %   Arguments:
        %       2+ variable specs: BaseVariable instances, Fixed wrappers,
        %       or BaseVariable instances with column selection.

            if nargin < 2
                error('scidb:Merge', ...
                    'Merge requires at least 2 variable inputs, got %d.', nargin);
            end

            for i = 1:nargin
                if isa(varargin{i}, 'scidb.Merge')
                    error('scidb:Merge', ...
                        'Cannot nest Merge inside another Merge.');
                end
            end

            obj.var_specs = varargin;
        end

        function to_csv(obj, filename, varargin)
        %TO_CSV  Export the merged variables to a CSV file in flat table format.
        %
        %   scidb.Merge(VarA(), VarB(), ...).to_csv(FILENAME, Name, Value, ...)
        %
        %   Each constituent is loaded independently and inner-joined on its
        %   shared schema keys, producing one row per schema_id with one value
        %   column per scalar constituent (or per table column). Every
        %   constituent must reduce to one row per schema_id; multi-row tables
        %   and bare vectors raise an error.
        %
        %   FILENAME must end with '.csv'. Name-Value args (version, where, db,
        %   metadata, branch-params) are forwarded to load() exactly as for
        %   BaseVariable.to_csv.
        %
        %   Example:
        %       % subject,trial,StepLength,Speed
        %       scidb.Merge(StepLength(), Speed()).to_csv("gait.csv", subject=1);

            scidb.internal.validate_csv_filename(filename);
            scidb.internal.reject_csv_spec_args('Merge', varargin);

            [metadata_args, version, where, db_val] = ...
                scidb.internal.split_csv_args(varargin{:});
            py_kwargs = scidb.internal.build_csv_kwargs( ...
                metadata_args, version, where, db_val);

            % Translate each MATLAB constituent to its Python spec object and
            % build the Python Merge, which owns the load/join/validate/write.
            py_specs = cell(1, numel(obj.var_specs));
            for i = 1:numel(obj.var_specs)
                py_specs{i} = constituent_to_py(obj.var_specs{i});
            end
            py_merge = py.scidb.model.merge.Merge(py_specs{:});

            scifor.Log.info('[to_csv] Merge(%d) -> %s (version=%s)', ...
                numel(obj.var_specs), string(filename), version);

            py_merge.to_csv(char(filename), pyargs(py_kwargs{:}));
        end

        function disp(obj)
        %DISP  Display the Merge wrapper.
            parts = cell(1, numel(obj.var_specs));
            for i = 1:numel(obj.var_specs)
                spec = obj.var_specs{i};
                if isa(spec, 'scidb.Fixed')
                    inner = spec.var_type;
                    inner_name = class(inner);
                    fields = fieldnames(spec.fixed_metadata);
                    fp = cell(1, numel(fields));
                    for f = 1:numel(fields)
                        val = spec.fixed_metadata.(fields{f});
                        if isnumeric(val)
                            fp{f} = sprintf('%s=%g', fields{f}, val);
                        else
                            fp{f} = sprintf('%s="%s"', fields{f}, string(val));
                        end
                    end
                    parts{i} = sprintf('Fixed(%s, %s)', inner_name, strjoin(fp, ', '));
                elseif isa(spec, 'scidb.Variant')
                    inner_name = class(spec.var_type);
                    fields = fieldnames(spec.branch_params);
                    fp = cell(1, numel(fields));
                    for f = 1:numel(fields)
                        val = spec.branch_params.(fields{f});
                        if isnumeric(val)
                            fp{f} = sprintf('%s=%g', fields{f}, val);
                        else
                            fp{f} = sprintf('%s="%s"', fields{f}, string(val));
                        end
                    end
                    parts{i} = sprintf('Variant(%s, %s)', inner_name, strjoin(fp, ', '));
                else
                    parts{i} = class(spec);
                end
            end
            fprintf('  scidb.Merge(%s)\n', strjoin(parts, ', '));
        end
    end
end


% =========================================================================
% Local helpers
% =========================================================================

function py_obj = constituent_to_py(spec)
%CONSTITUENT_TO_PY  Translate a MATLAB Merge constituent to its Python object.
%   Handles BaseVariable instances (with optional column selection),
%   scidb.Fixed, and scidb.Variant wrappers (recursively).

    if isa(spec, 'scidb.Fixed')
        inner = constituent_to_py(spec.var_type);
        kv = struct_to_pykwargs(spec.fixed_metadata);
        py_obj = py.scidb.fixed.Fixed(inner, pyargs(kv{:}));

    elseif isa(spec, 'scidb.Variant')
        inner = constituent_to_py(spec.var_type);
        kv = struct_to_pykwargs(spec.branch_params);
        py_obj = py.scidb.model.variant.Variant(inner, pyargs(kv{:}));

    elseif isa(spec, 'scidb.BaseVariable')
        py_class = scidb.internal.ensure_registered(class(spec));
        if isempty(spec.selected_columns)
            py_obj = py_class;
        else
            cols = cellstr(spec.selected_columns(:)');
            py_obj = py.scidb.model.column_selection.ColumnSelection( ...
                py_class, py.list(cols));
        end

    else
        error('scidb:Merge:UnknownConstituent', ...
            'Cannot export Merge constituent of class "%s" to CSV.', class(spec));
    end
end


function kv = struct_to_pykwargs(s)
%STRUCT_TO_PYKWARGS  Flatten a struct to a Python-ready name-value cell.
    if isempty(s) || isempty(fieldnames(s))
        kv = {};
        return;
    end
    fnames = fieldnames(s);
    nv = cell(1, 2 * numel(fnames));
    for i = 1:numel(fnames)
        nv{2*i - 1} = fnames{i};
        nv{2*i} = s.(fnames{i});
    end
    kv = scidb.internal.metadata_to_pykwargs(nv{:});
end
