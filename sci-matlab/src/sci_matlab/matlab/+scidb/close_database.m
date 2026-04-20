function close_database(db)
%SCIDB.CLOSE_DATABASE  Close the DuckDB connection with release/error logging.
%
%   scidb.close_database(DB)
%   Closes DB, logging "DuckDB lock RELEASED" on success and the underlying
%   error on failure. The release log is emitted only after close returns,
%   so the log never falsely claims a release that didn't happen.
%
%   A separate try/catch wraps the close itself so that:
%     * a failing close is logged as an error and rethrown, and
%     * callers can put scidb.close_database in both the try and catch
%       branches of their for_each wrapper without swallowing the original
%       error.

    arguments
        db
    end

    db_path = '';
    try
        db_path = char(db.path);
    catch
        % db.path may not be available (e.g. already-closed handle); leave blank.
    end

    try
        db.close();
    catch close_err__
        scidb.Log.error('MATLAB: db.close FAILED for %s: %s', ...
            db_path, close_err__.message);
        rethrow(close_err__);
    end

    scidb.Log.info('MATLAB: db.close — DuckDB lock RELEASED: %s', db_path);
end
