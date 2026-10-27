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
%   All logging calls delegate to Python's scistacklog.Log class directly —
%   NOT via scidb — so that standalone (no-database) scifor usage in MATLAB
%   stays free of a Python scidb dependency, mirroring Python scifor's own
%   "scifor stays scidb-free; it depends only on scistacklog" contract.
%   scidb.log.Log is a re-export of the same scistacklog.Log class, so
%   scifor.Log and scidb.Log share one underlying Python singleton and one
%   appdata cache — a level/path change from either class is visible to
%   both, regardless of which one a given MATLAB session touches first.
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
            % needs to cross the Python bridge. Shared appdata key with
            % scidb.Log: same underlying Python singleton, one cache.
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
            if scifor.Log.get_level() <= scifor.Log.DEBUG
                % Only format when there are args to substitute — see the
                % matching guard in +scidb/Log.m: a pre-formatted message
                % containing a Windows path is otherwise sprintf'd twice and
                % silently truncated at the first backslash escape.
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
                % Only format when there are args to substitute — see the
                % matching guard in +scidb/Log.m: a pre-formatted message
                % containing a Windows path is otherwise sprintf'd twice and
                % silently truncated at the first backslash escape.
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
                % Only format when there are args to substitute — see the
                % matching guard in +scidb/Log.m: a pre-formatted message
                % containing a Windows path is otherwise sprintf'd twice and
                % silently truncated at the first backslash escape.
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
                % Only format when there are args to substitute — see the
                % matching guard in +scidb/Log.m: a pre-formatted message
                % containing a Windows path is otherwise sprintf'd twice and
                % silently truncated at the first backslash escape.
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
