function filt = raw_sql(sql)
%SCIDB.RAW_SQL  Create a raw SQL filter for use in where= parameter.
%
%   FILT = scidb.raw_sql(SQL)
%
%   The SQL fragment is applied to the target variable's data table joined
%   with _record_save / _record (and _schema).  No WHERE keyword should be
%   included.
%
%   Example:
%       StepLength().load(where=scidb.raw_sql('"value" > 0.70'))

    py_filter = py.scidb.model.filters.raw_sql(char(sql));
    filt = scidb.Filter(py_filter);
end
