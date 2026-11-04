# Frame classifier under MATLAB's embedded Python

Bug (2026-11-03, C-Brace load_cbrace, MATLAB R2023b, Python 3.11.9 in-process):
a combo failed, and while recording it `scifor.frame_origin` -> Python
`scifor.failures._library_dirs()` read `os.__file__`. The embedded 3.11 loads
`os` frozen with no `__file__` and `sys._stdlib_dir` None (confirmed:
`False None`), so AttributeError replaced the user's own error.

Fix:
1. `scifor/failures.py` `_library_dirs`: sysconfig is the source; `os.__file__`
   and `sys._stdlib_dir` are optional (getattr). Debug log of the dirs found.
2. `frame_origin` never raises: classification is in `_classify_frame`; on an
   exception -> "user" + WARN `[frame_origin] could not classify ...`.
3. `+scifor/frame_origin.m`: try/catch around the Python crossing -> 'user' + warn.
4. `+scifor/for_each.m` `record_iteration_failure`: if the record cannot be
   built, log the user's error (with report) at ERROR, then
   `throw(addCause(err, rec_err))`, so the user's error is the one surfaced.

Tests: scifor/tests/test_failures.py (no os.__file__; never raises),
TestSciforForEachParity.m (broken classifier keeps the user's message).
