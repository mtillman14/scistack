# Reading your log

SciStack writes two log files next to your database:

| File | For | What's in it |
|---|---|---|
| `scistack.log` | you | One entry per run: what ran, how many combinations finished, failed or had no data, how long it took, and **who can fix each failure** |
| `scistack-debug.log` | debugging SciStack | Everything SciStack did, in full technical detail |

Open `scistack.log` first. In the GUI, the **Open log** button on a failed run's notification opens it.

## A run's entry

```
── 2026-10-22 17:55:18  clean_asia  (run=a3f2c1d9) ─────────────────────
   finished with 34 failures: ✓ 0 completed   ✗ 34 failed   – 0 no data   2m 14s

   ✗ Error in your function clean_asia — 34 combos
     AttributeError: 'StringArray' object has no attribute 'split'
     at C:\lab\clean_asia.py:42 in parse_level
         levels = col.split(",")
     combos: subject=2204674; subject=2204675; subject=2204676; subject=2204677; subject=2204678 (+29 more)

   Failed combos saved nothing; completed combos were kept.
```

Each failure is grouped by its error message. Failures with the same message are listed once, with the combinations they happened in.

## Who can fix it

Every failure starts with one of these headings:

| Heading | What it means | What to do |
|---|---|---|
| **Error in your function F** | The error was raised while your function was running: in your code, or in a library your code (or the function you chose) called. | Go to the file and line shown. |
| **Fixable in your function F** | SciStack failed while handling what your function returned, and it knows why. For example, the function returned a table for some subjects and an array for others. | The message names the combinations on each side. Make the function return the same kind of value every time. |
| **Probably fixable in your function F** | The same, but SciStack could not pin down the cause. | Check that the function returns the same kind, shape and columns for every combination. If it does, report it (see below). |
| **Problem with your setup or data** | SciStack refused on purpose. A schema key, a variable's columns, or the database don't match what was asked. | The message says what to change. |
| **SciStack internal error, not caused by your code** | A bug in SciStack. | Report it (see below). |
| **No data** | That combination has no data to work on. Not an error. | Nothing, unless you expected data there. |

The heading says where the error was *raised*, not what caused it. If it says the error was in your function but the line looks right, check what the function was given.

## Reporting a problem

Each entry names a run, such as `run=a3f2c1d9`. Every line of `scistack-debug.log` written during that run carries the same text, so searching for it finds the full technical record. Attach `scistack-debug.log` to a bug report, and include the run name.
