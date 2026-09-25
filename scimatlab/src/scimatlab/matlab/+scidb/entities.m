function varargout = entities(project_root)
%SCIDB.ENTITIES  Load the project's declared entities from its TOML file.
%
%   E = scidb.entities() returns a struct whose fields are the declared
%   Parameters and PathInputs, rebuilt as MATLAB scidb.Parameter /
%   scidb.PathInput objects:
%
%       e = scidb.entities();
%       scidb.for_each(@process, inputs=struct('f', e.EMG_FILE), ...
%                      outputs={Filtered()}, window=e.WINDOW_SECONDS);
%
%   scidb.entities() with no output assigns every declared name into the
%   BASE workspace instead, so declared names are simply in scope:
%
%       scidb.entities();
%       scidb.for_each(@process, inputs=struct('f', EMG_FILE), ...);
%
%   This is what the GUI emits into generated commands, and it replaces the
%   old `scistack_entities;` script line. The declarations now live in one
%   language-neutral TOML file that Python and MATLAB both read, rather than
%   a .m script and a .py module that had to be kept in step -- see
%   docs/claude/entity-editability-model.md and scidb/src/scidb/entities.py.
%
%   E = scidb.entities(PROJECT_ROOT) resolves the entities file starting
%   from PROJECT_ROOT rather than the current directory.
%
%   Re-reading is cheap and always fresh: the Python side caches on the
%   file's mtime, so an edit made in the GUI is picked up on the next call
%   with nothing to clear -- the property that made a plain script
%   preferable to a classdef, preserved.
%
%   Variables are NOT returned as fields: a variable is a *type*, and
%   MATLAB requires a real classdef file per type. Any declared variable
%   that does not already resolve as a class gets a stub classdef written
%   for it here (scimatlab.stubs, via
%   py.scimatlab.bridge.ensure_variable_classdefs) and its directory added
%   to the path, so a declared variable is referenced as StepLength() the
%   same way it always was -- including one just created in the GUI, with
%   no further setup.

    arguments
        project_root string = ""
    end

    if strlength(project_root) == 0
        payload = py.scimatlab.bridge.load_entities();
    else
        payload = py.scimatlab.bridge.load_entities(char(project_root));
    end

    s = struct();

    % --- Parameters: a list of values -> scidb.Parameter(v1, v2, ...) ------
    params = payload{'parameters'};
    param_names = cell(py.builtins.list(params.keys()));
    for i = 1:numel(param_names)
        name = char(param_names{i});
        values = scidb.internal.pylist_to_cell(params{name});
        s.(name) = scidb.Parameter(values{:});
    end

    % --- PathInputs: one arm, or several wrapped in an EachOf -------------
    pis = payload{'path_inputs'};
    pi_names = cell(py.builtins.list(pis.keys()));
    for i = 1:numel(pi_names)
        name = char(pi_names{i});
        arms = scidb.internal.pylist_to_cell(pis{name});
        objs = cell(1, numel(arms));
        for j = 1:numel(arms)
            arm = arms{j};
            % root_folder is name-value on scifor.PathInput (the template is
            % its only positional), so it must be passed as one -- a second
            % positional fails the arguments block at construction time.
            % Quoted 'name', value form, not name=value: scidb.PathInput
            % forwards varargin{:} and declares no arguments block, so it
            % cannot accept the name=value syntax.
            try
                if isempty(arm.root_folder)
                    objs{j} = scidb.PathInput(arm.template, 'name', name);
                else
                    objs{j} = scidb.PathInput(arm.template, ...
                        'root_folder', string(arm.root_folder), 'name', name);
                end
            catch err
                % Without this the failure reads as a bare constructor error
                % with no hint of which declaration produced it.
                error('scidb:entities:pathInputFailed', ...
                      ['Path input ''%s'' (arm %d of %d, template ''%s'') ' ...
                       'from %s could not be constructed: %s'], ...
                      name, j, numel(arms), char(string(arm.template)), ...
                      char(payload{'path'}), err.message);
            end
        end
        if numel(objs) == 1
            s.(name) = objs{1};
        else
            % Alternate templates are an EachOf of PathInputs -- the same
            % shape Python builds, so for_each fans them out identically.
            s.(name) = scifor.EachOf(objs{:});
        end
    end

    % --- Variables need a real classdef file on the path ------------------
    % A Variable is a *type*, not a value, so it cannot cross the bridge as
    % one: `RawEMG()` only resolves once a classdef file for it exists on
    % the MATLAB path. Ask MATLAB which declared names it cannot resolve --
    % its path is the only authority, and writing a stub for a name that
    % already has a hand-written classdef elsewhere would shadow it -- then
    % have the bridge materialize exactly those (see scimatlab/stubs.py).
    % This is what makes a variable created in the GUI runnable from here
    % with no further setup, and it runs before anything in the generated
    % script references the type.
    declared_vars = scidb.internal.pylist_to_cell(payload{'variables'});
    missing = {};
    for i = 1:numel(declared_vars)
        vname = char(declared_vars{i});
        if exist(vname, 'class') ~= 8
            missing{end+1} = vname; %#ok<AGROW>
        end
    end
    if ~isempty(missing)
        if strlength(project_root) == 0
            stub_result = py.scimatlab.bridge.ensure_variable_classdefs( ...
                py.list(missing));
        else
            stub_result = py.scimatlab.bridge.ensure_variable_classdefs( ...
                py.list(missing), char(project_root));
        end
        stub_dir = char(stub_result{'dir'});
        if ~isempty(stub_dir) && isfolder(stub_dir)
            addpath(stub_dir);
            % New files in a freshly added folder are not visible to the
            % class resolver until the caches are refreshed.
            rehash;
        end

        created = scidb.internal.pylist_to_cell(stub_result{'created'});
        if ~isempty(created)
            created_names = cellfun(@char, created, 'UniformOutput', false);
            scidb.Log.info(sprintf( ...
                '[entities] Materialized %d MATLAB classdef(s) in %s: %s', ...
                numel(created_names), stub_dir, ...
                strjoin(created_names, ', ')));
        end

        stub_errors = scidb.internal.pylist_to_cell(stub_result{'errors'});
        for i = 1:numel(stub_errors)
            warning('scidb:entities:classdefWriteFailed', ...
                    'Entities file: %s', char(stub_errors{i}));
        end

        % Anything that still does not resolve is named here rather than
        % surfacing later as "Unrecognized function or variable 'X'" from
        % the middle of a for_each call.
        for i = 1:numel(missing)
            if exist(missing{i}, 'class') ~= 8
                if isempty(stub_dir)
                    expected = '(no directory could be resolved)';
                else
                    expected = fullfile(stub_dir, [missing{i} '.m']);
                end
                warning('scidb:entities:noClassdef', ...
                        ['Variable ''%s'' is declared in %s but does not ' ...
                         'resolve as a MATLAB class (expected %s). ' ...
                         '%s() will error until that file exists.'], ...
                        missing{i}, char(payload{'path'}), expected, ...
                        missing{i});
            end
        end
    end

    % --- Rejected entries are warnings here too ---------------------------
    % A declaration the parser refused is invisible in MATLAB unless it is
    % surfaced: the GUI shows these in its load-errors panel, and someone
    % running from the MATLAB prompt never sees that panel.
    errors = scidb.internal.pylist_to_cell(payload{'errors'});
    for i = 1:numel(errors)
        warning('scidb:entities:rejected', ...
                'Entities file: %s', char(errors{i}));
    end

    if nargout == 0
        names = fieldnames(s);
        for i = 1:numel(names)
            assignin('base', names{i}, s.(names{i}));
        end
        scidb.Log.info(sprintf( ...
            '[entities] Loaded %d entity declaration(s) from %s', ...
            numel(names), char(payload{'path'})));
    else
        varargout{1} = s;
    end
end
