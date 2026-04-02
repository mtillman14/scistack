classdef Log
%SCIDB.LOG  Minimal structured logger with levels and timestamps.
%
%   scidb.Log.set_level('DEBUG')   — show all messages
%   scidb.Log.set_level('INFO')    — show INFO, WARN, ERROR (default)
%   scidb.Log.set_level('WARN')    — show WARN, ERROR only
%   scidb.Log.set_level('ERROR')   — show ERROR only
%
%   scidb.Log.debug('Processing %d items', n)
%   scidb.Log.info('Loaded %s', type_name)
%   scidb.Log.warn('No data for %s', key)
%   scidb.Log.err('Failed: %s', msg)
%
%   Output format: [HH:MM:SS.FFF] [LEVEL] message
%
%   Writes to scidb.log next to the database file (set automatically
%   by scidb.configure_database()). Each log call opens, appends, and
%   closes the file so every line is flushed to disk immediately.
%
%   Uses setappdata(0, ...) for global state (standard MATLAB singleton pattern).

    properties (Constant)
        DEBUG = 0
        INFO  = 1
        WARN  = 2
        ERROR = 3
    end

    methods (Static)

        function set_level(level)
        %SET_LEVEL  Set the global log level.
        %   Accepts a string ('DEBUG','INFO','WARN','ERROR') or numeric (0-3).
            if ischar(level) || isstring(level)
                level = scidb.Log.parse_level(char(upper(string(level))));
            end
            setappdata(0, 'scidb_log_level', level);
        end

        function level = get_level()
        %GET_LEVEL  Get the current log level (default: INFO).
            if isappdata(0, 'scidb_log_level')
                level = getappdata(0, 'scidb_log_level');
            else
                level = scidb.Log.INFO;
            end
        end

        function set_path(log_path)
        %SET_PATH  Set the log file path for file output.
        %   Called automatically by scidb.configure_database().
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
            if scidb.Log.get_level() <= scidb.Log.DEBUG
                scidb.Log.emit('DEBUG', fmt, varargin{:});
            end
        end

        function info(fmt, varargin)
        %INFO  Log a message at INFO level.
            if scidb.Log.get_level() <= scidb.Log.INFO
                scidb.Log.emit('INFO', fmt, varargin{:});
            end
        end

        function warn(fmt, varargin)
        %WARN  Log a message at WARN level.
            if scidb.Log.get_level() <= scidb.Log.WARN
                scidb.Log.emit('WARN', fmt, varargin{:});
            end
        end

        function err(fmt, varargin)
        %ERR  Log a message at ERROR level.
        %   Named 'err' to avoid conflict with MATLAB's built-in 'error'.
            if scidb.Log.get_level() <= scidb.Log.ERROR
                scidb.Log.emit('ERROR', fmt, varargin{:});
            end
        end

    end

    methods (Static, Access = private)

        function emit(level_str, fmt, varargin)
        %EMIT  Format and write a log message with timestamp to the log file.
        %   Opens, appends, and closes the file on every call so that
        %   each line is flushed to disk immediately.
            log_path = scidb.Log.get_path();
            if isempty(log_path)
                return;
            end

            ts = datestr(now, 'HH:MM:SS.FFF'); %#ok<TNOW1,DATST>
            msg = sprintf(fmt, varargin{:});

            fid = fopen(log_path, 'a');
            if fid ~= -1
                fprintf(fid, '[%s] [%s] %s\n', ts, level_str, msg);
                fclose(fid);
            end
        end

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
