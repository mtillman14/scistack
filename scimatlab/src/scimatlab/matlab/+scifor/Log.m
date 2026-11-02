classdef Log
%SCIFOR.LOG  Unified logger that delegates to Python scistacklog.Log.
%
%   scifor.Log.set_level('DEBUG')          — show all messages (both sinks)
%   scifor.Log.set_level('INFO')           — show INFO, WARN, ERROR (default)
%   scifor.Log.set_level('DEBUG', 'file')  — full detail in scistack.log only
%   scifor.Log.set_level('WARN')           — show WARN, ERROR only
%
%   scifor.Log.debug('Processing %d items', n)
%   scifor.Log.info('Loaded %s', type_name)
%   scifor.Log.warn('No data for %s', key)
%   scifor.Log.err('Failed: %s', msg)
%
%   Two sinks with independent levels (see the scistacklog package):
%   console (stderr): HH:MM:SS [layer] message
%   file (scistack.log): YYYY-MM-DD HH:MM:SS.FFF LEVEL [layer] message
%
%   All logging calls delegate to Python's scistacklog.Log class directly,
%   NOT via scidb, so standalone (no-database) scifor usage in MATLAB stays
%   free of a Python scidb dependency. scidb.Log (+scidb/Log.m) is a subclass
%   with no members of its own: one class, two package names.
%
%   Name the Python class by its DEFINING module (py.scistacklog.Log), never
%   a re-export (scidb's model.log.Log): MATLAB resolves a static call
%   py.<module>.<Class>.<method> only when the class's __module__ matches the
%   dotted path; for a re-export it falls back to constructor semantics and
%   errors. Guarded by scimatlab/tests/test_matlab_py_class_dispatch.py.
%
%   The log level is cached in MATLAB's appdata for performance, but the
%   Python logger is the source of truth.

    properties (Constant)
        DEBUG = 0
        INFO  = 1
        WARN  = 2
        ERROR = 3
    end

    methods (Static)

        function set_level(level, sink)
        %SET_LEVEL  Set the log level of one or both sinks.
        %   scifor.Log.set_level(level)          — both sinks
        %   scifor.Log.set_level(level, sink)    — 'console' | 'file' | 'both'
        %   Accepts a string ('DEBUG','INFO','WARN','ERROR') or numeric (0-3).
        %   Delegates to Python logging to unify log level settings.
            if isnumeric(level)
                % Convert numeric to string for Python
                level_names = {'DEBUG', 'INFO', 'WARN', 'ERROR'};
                if level >= 0 && level <= 3
                    level = level_names{level + 1};
                else
                    level = 'INFO';
                end
            end
            % Set Python log level (source of truth)
            if nargin < 2
                py.scistacklog.Log.set_level(char(level));
            else
                py.scistacklog.Log.set_level(char(level), char(sink));
            end
            % Cache the effective level (min of the two sinks) for fast
            % get_level() calls — a message suppressed by both sinks never
            % needs to cross the Python bridge.
            level_num = double(py.scistacklog.Log.get_level());
            setappdata(0, 'scidb_log_level', level_num);
        end

        function level = get_level()
        %GET_LEVEL  Get the current log level (default: INFO).
        %   Returns cached value for performance (avoids Python bridge crossing).
            if isappdata(0, 'scidb_log_level')
                level = getappdata(0, 'scidb_log_level');
            else
                level = scifor.Log.INFO;
            end
        end

        function set_path(log_path)
        %SET_PATH  Set the debug log file path. configure_database does not call
        %   this (it attaches both files via scidb.model.log.attach_log_file).
        %   Delegates to Python logging to unify log file location.
            py.scistacklog.Log.set_path(char(log_path));
        end

        function p = get_path()
        %GET_PATH  The debug log file (scistack-debug.log) Python writes, '' when
        %   none is attached. Python owns the path: configure_database attaches
        %   it through scidb.model.log.attach_log_file, never through set_path,
        %   so a MATLAB-side cache was never filled (returned '' since the
        %   run-journal split, 2026-10-27). The journal (scistack.log) holds
        %   only run blocks; DEBUG/INFO detail lives in this file.
            py_p = py.scistacklog.Log.get_path();
            if isa(py_p, 'py.NoneType')
                p = '';
            else
                p = char(py_p);
            end
            scifor.Log.debug('Log.get_path: %s', p);
        end

        function debug(fmt, varargin)
        %DEBUG  Log a message at DEBUG level.
        %   Delegates to Python logging for unified output; MATLAB-originated
        %   lines carry the [matlab] layer tag.
            if scifor.Log.get_level() <= scifor.Log.DEBUG
                % Only format when there are args to substitute: a
                % pre-formatted message containing a Windows path would be
                % sprintf'd twice and truncated at the first backslash
                % escape (y:\LabMembers -> y:, seen 2026-09-01).
                if nargin == 1
                    msg = fmt;
                else
                    msg = sprintf(fmt, varargin{:});
                end
                py.scistacklog.Log.debug(msg, pyargs('layer', 'matlab'));
            end
        end

        function info(fmt, varargin)
        %INFO  Log a message at INFO level.
        %   Delegates to Python logging for unified output; MATLAB-originated
        %   lines carry the [matlab] layer tag.
            if scifor.Log.get_level() <= scifor.Log.INFO
                % Only format when there are args to substitute: a
                % pre-formatted message containing a Windows path would be
                % sprintf'd twice and truncated at the first backslash
                % escape (y:\LabMembers -> y:, seen 2026-09-01).
                if nargin == 1
                    msg = fmt;
                else
                    msg = sprintf(fmt, varargin{:});
                end
                py.scistacklog.Log.info(msg, pyargs('layer', 'matlab'));
            end
        end

        function warn(fmt, varargin)
        %WARN  Log a message at WARN level.
        %   Delegates to Python logging for unified output; MATLAB-originated
        %   lines carry the [matlab] layer tag.
            if scifor.Log.get_level() <= scifor.Log.WARN
                % Only format when there are args to substitute: a
                % pre-formatted message containing a Windows path would be
                % sprintf'd twice and truncated at the first backslash
                % escape (y:\LabMembers -> y:, seen 2026-09-01).
                if nargin == 1
                    msg = fmt;
                else
                    msg = sprintf(fmt, varargin{:});
                end
                py.scistacklog.Log.warn(msg, pyargs('layer', 'matlab'));
            end
        end

        function err(fmt, varargin)
        %ERR  Log a message at ERROR level.
        %   Named 'err' to avoid conflict with MATLAB's built-in 'error'.
        %   Delegates to Python logging for unified output; MATLAB-originated
        %   lines carry the [matlab] layer tag.
            if scifor.Log.get_level() <= scifor.Log.ERROR
                % Only format when there are args to substitute: a
                % pre-formatted message containing a Windows path would be
                % sprintf'd twice and truncated at the first backslash
                % escape (y:\LabMembers -> y:, seen 2026-09-01).
                if nargin == 1
                    msg = fmt;
                else
                    msg = sprintf(fmt, varargin{:});
                end
                py.scistacklog.Log.error(msg, pyargs('layer', 'matlab'));
            end
        end

    end

end
