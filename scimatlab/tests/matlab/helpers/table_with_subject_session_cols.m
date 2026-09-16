function tblOut = table_with_subject_session_cols(tblIn)
%TABLE_WITH_SUBJECT_SESSION_COLS  Label every row with its own (subject, session).
%   Returns tblIn with A and B doubled plus subject=["A";"A";"B"] and
%   session=[1;2;1]. Used to verify that a table returned from a run that
%   iterates NOTHING is still filed one record per (subject, session) — the
%   spread rule — on the MATLAB path as well as the Python path.
tblOut = tblIn;
tblOut.A = tblOut.A * 2;
tblOut.B = tblOut.B * 2;
tblOut.subject = ["A"; "A"; "B"];
tblOut.session = [1; 2; 1];
end
