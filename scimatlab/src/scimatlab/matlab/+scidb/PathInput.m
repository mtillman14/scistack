classdef PathInput < scifor.PathInput
%SCIDB.PATHINPUT  Resolve a path template using iteration metadata.
%
%   PI = scidb.PathInput(TEMPLATE, 'name', NAME)
%   PI = scidb.PathInput(TEMPLATE, 'root_folder', FOLDER, 'name', NAME)
%
%   Namespace alias for scifor.PathInput, so a MATLAB entities script
%   reads identically to its Python counterpart:
%
%       % scistack_entities.m
%       raw_emg = scidb.PathInput('{subject}/{trial}.mat', 'name', 'raw_emg');
%
%       # src/scistack_entities.py
%       raw_emg = scidb.PathInput('{subject}/{trial}.mat', name='raw_emg')
%
%   Every argument is forwarded unchanged, so all of scifor.PathInput's
%   options (name -- required, root_folder, regex, aliases, key_regex) work here too -- see
%   +scifor/PathInput.m for their full documentation. Inheritance keeps
%   isa(PI, 'scifor.PathInput') true, so nothing downstream changes.
%
%   See docs/claude/entity-editability-model.md.

    methods
        function obj = PathInput(varargin)
        %PATHINPUT  Construct a PathInput.
        %
        %   PI = scidb.PathInput(TEMPLATE, ...)
            obj@scifor.PathInput(varargin{:});
        end
    end
end
