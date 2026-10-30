function tblOut = table_with_duplicate_subject(tblIn)
%TABLE_WITH_DUPLICATE_SUBJECT  Label rows by subject, subject A twice.
%   Returns tblIn with A doubled and subject=["A";"A";"B"] (no session
%   column): two rows at one location, the shape on_duplicate="combine"
%   exists for (two hospital stays of one subject).
tblOut = tblIn;
tblOut.A = tblOut.A * 2;
tblOut.subject = ["A"; "A"; "B"];
end
