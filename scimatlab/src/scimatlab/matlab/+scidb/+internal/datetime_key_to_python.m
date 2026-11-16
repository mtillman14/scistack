function text = datetime_key_to_python(dt)
%DATETIME_KEY_TO_PYTHON  A scalar MATLAB datetime schema key as its key text.
%
%   text = datetime_key_to_python(dt)
%   returns the ONE spelling of a date/datetime key (Python owns it:
%   scicanonicalhash.datetime_key_text), e.g.
%       2026-07-09T15:12:00-05:00[America/Chicago]
%
%   Every converter of metadata name-value pairs (metadata_to_pydict for
%   load, metadata_to_pykwargs for save, delete, versions...) calls this, so
%   a key saved and a key loaded by the same datetime spell the same. A bare
%   datetime handed to Python as a keyword would be converted by MATLAB
%   itself, which turns a named zone into a fixed offset (R2024a) and the
%   value would then be saved at a different address.

    text = py.scimatlab.bridge.datetime_key_text_of( ...
        scidb.internal.datetime_to_python(dt, 'scalar'));
end
