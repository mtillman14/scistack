classdef Log
%SCIDB.LOG  Unified logger that delegates to Python scistacklog.Log.
%
%   scidb.Log.set_level('DEBUG')          — show all messages (both sinks)
%   scidb.Log.set_level('INFO')           — show INFO, WARN, ERROR (default)
%   scidb.Log.set_level('DEBUG', 'file')  — full detail in scistack.log only
%   scidb.Log.set_level('WARN')           — show WARN, ERROR only
%
%   scidb.Log.debug('Processing %d items', n)
%   scidb.Log.info('Loaded %s', type_name)
%   scidb.Log.warn('No data for %s', key)
%   scidb.Log.err('Failed: %s', msg)
%
%   Two sinks with independent levels (see the scistacklog package):
%   console (stderr): HH:MM:SS [layer] message
%   file (scistack.log): YYYY-MM-DD HH:MM:SS.FFF LEVEL [layer] message
%
%   All logging calls delegate to the Python scistacklog.Log class to ensure
%   unified log level settings and output. Writes to scistack.log next to the
%   database file (set automatically by scidb.configure_database()).
%
%   Delegation targets py.scistacklog.Log, NOT py.scidb.log.Log, even though
%   scidb.log re-exports the very same class object. MATLAB resolves a static
%   call py.<module>.<Class>.<method> only when the class is *defined* in that
%   module (its __module__ matches the dotted path). For a re-export MATLAB
%   falls back to constructor semantics and errors with "Dot indexing into the
%   result of a function call requires parentheses after the function name /
%   The supported syntax is 'py.scidb.log.Log().info'". Always name a Python
%   class by its defining module from MATLAB. Guarded by
%   scimatlab/tests/test_matlab_py_class_dispatch.py.
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
        %   scidb.Log.set_level(level)          — both sinks
        %   scidb.Log.set_level(level, sink)    — 'console' | 'file' | 'both'
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
                level = scidb.Log.INFO;
            end
        end

        function set_path(log_path)
        %SET_PATH  Set the log file path for file output.
        %   Called automatically by scidb.configure_database().
        %   Delegates to Python logging to unify log file location.
            py.scistacklog.Log.set_path(char(log_path));
            % Cache in MATLAB for legacy compatibility
            setappdata(0, 'scidb_log_path', char(log_path));
        end

        function p = get_path()
        %GET_PATH  Get the current log file path (empty if not set).
            if isappdata(0, 'scidb_log_path')
                p = getappdata(0, 'scidb_log_path');
            else
                p = '';
            end
        end

        function debug(fmt, varargin)
        %DEBUG  Log a message at DEBUG level.
        %   Delegates to Python logging for unified output; MATLAB-originated
        %   lines carry the [matlab] layer tag.
            if scidb.Log.get_level() <= scidb.Log.DEBUG
                % Only format when there are args to substitute. Callers that
                % pre-build the message (scidb.Log.info(sprintf(...))) would
                % otherwise be sprintf'd twice, and the second pass eats
                % Windows path separators: '...in y:\LabMembers\...' warns
                % "Escaped character '\L' is not valid" and truncates to
                % '...in y:'. Observed 2026-09-01 from +scidb/entities.m.
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
            if scidb.Log.get_level() <= scidb.Log.INFO
                % Only format when there are args to substitute. Callers that
                % pre-build the message (scidb.Log.info(sprintf(...))) would
                % otherwise be sprintf'd twice, and the second pass eats
                % Windows path separators: '...in y:\LabMembers\...' warns
                % "Escaped character '\L' is not valid" and truncates to
                % '...in y:'. Observed 2026-09-01 from +scidb/entities.m.
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
            if scidb.Log.get_level() <= scidb.Log.WARN
                % Only format when there are args to substitute. Callers that
                % pre-build the message (scidb.Log.info(sprintf(...))) would
                % otherwise be sprintf'd twice, and the second pass eats
                % Windows path separators: '...in y:\LabMembers\...' warns
                % "Escaped character '\L' is not valid" and truncates to
                % '...in y:'. Observed 2026-09-01 from +scidb/entities.m.
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
            if scidb.Log.get_level() <= scidb.Log.ERROR
                % Only format when there are args to substitute. Callers that
                % pre-build the message (scidb.Log.info(sprintf(...))) would
                % otherwise be sprintf'd twice, and the second pass eats
                % Windows path separators: '...in y:\LabMembers\...' warns
                % "Escaped character '\L' is not valid" and truncates to
                % '...in y:'. Observed 2026-09-01 from +scidb/entities.m.
                if nargin == 1
                    msg = fmt;
                else
                    msg = sprintf(fmt, varargin{:});
                end
                py.scistacklog.Log.error(msg, pyargs('layer', 'matlab'));
            end
        end

    end

    methods (Static, Access = private)

        function level = parse_level(name)
        %PARSE_LEVEL  Convert a level name string to numeric value.
            switch name
                case 'DEBUG'
                    level = scidb.Log.DEBUG;
                case 'INFO'
                    level = scidb.Log.INFO;
                case 'WARN'
                    level = scidb.Log.WARN;
                case 'ERROR'
                    level = scidb.Log.ERROR;
                otherwise
                    warning('scidb:Log', 'Unknown log level ''%s'', defaulting to INFO.', name);
                    level = scidb.Log.INFO;
            end
        end

    end

end
