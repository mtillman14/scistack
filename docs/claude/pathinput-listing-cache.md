# PathInput: the directory listing cache and its timestamp limit

Owner: `scifor/src/scifor/pathinput.py` (`_DIR_CACHE`, `_RACY_WINDOW_NS`,
`PathInput._list_dir`, `clear_listing_cache`). Tests:
`scifor/tests/test_pathinput_discover_cache.py`.

## What it is

`PathInput.discover()` walks the template's folders. Every directory listing
goes through `_list_dir`, which keeps `{directory: (mtime_ns, sorted entries)}`
in one **process-wide** dict, `_DIR_CACHE`.

- **Why it exists:** on a UNC/SMB share each `os.listdir` is a network round
  trip. On 2026-09-13 a second discovery of an unchanged share blocked for
  minutes.
- **Why it is module-level, not per instance (2026-09-22):** the canvas
  rebuilds PathInput objects from stored specs on every refresh, so a
  per-instance cache started empty every time and never served anything.
- **Why sharing is safe:** an entry is served only if the directory's current
  `st_mtime_ns` equals the stored one. Adding or removing an entry changes a
  directory's mtime, so a changed directory is re-read.

A warm walk still costs one `stat` per directory, which is the mtime check.
Skipping even that would need a memo of the discovered combos above the walk,
which is the caller's business.

## The timestamp limit ("racily clean")

The mtime check is only as good as the clock that stamps mtimes, and that
clock is coarse:

| Filesystem | mtime granularity |
| --- | --- |
| Linux ext4/overlay/tmpfs | one kernel tick, about 4 ms (the coarse clock, not true ns) |
| macOS HFS+ | 1 s (APFS: ns) |
| FAT / exFAT (USB drives) | 2 s |
| SMB shares | whatever the server's filesystem has |

So this can happen within one tick:

1. A walk reads directory D (mtime T) and caches `(T, entries)`.
2. Someone creates `D/s3` in the same tick, and D's mtime is still T.
3. The next walk sees mtime T, a match, and serves the stale listing.
4. The listing stays stale until something else changes D. A new subject
   folder can stay invisible indefinitely.

Git calls this "racily clean" and handles it the same way. The fix
(2026-11-21) is **not to cache a listing whose directory mtime is within
`_RACY_WINDOW_NS` (2 s) of now.** Such a listing is read and returned but not
stored, and any older entry for that directory is dropped. Once the directory
is 2 s old, the next walk caches it normally. 2 s covers the coarsest common
filesystem, FAT.

- **Cost:** a directory touched in the last 2 s is re-read on every walk.
  That is negligible, because real data folders are old.
- **What it does not cover:** a change that leaves the mtime *identical* to a
  value cached more than 2 s earlier. That would need a filesystem that
  doesn't update mtime at all on entry changes, or a clock set backwards. In
  that case `clear_listing_cache()` is the escape hatch.
- **Clock skew on network shares:** mtimes come from the server's clock and
  "now" from the local one. If the server clock runs ahead, recent directories
  are left uncached a little longer, which only costs time. If it runs behind
  by more than 2 s, the window is effectively shorter, and the original race
  is again possible for changes within one server tick of a read.

## How it showed up

`test_a_changed_directory_is_re_read` (and `..._across_instances`) failed
intermittently on Linux. The discover log line right after a `mkdir` read:

```
pathinput_discover: 4 combo(s); 0 director(ies) read from disk, 3 served from the listing cache
```

The test's `os.utime(tree, None)` was meant to force a visible bump, but
"now" was still the same coarse tick.

## Diagnostics

- The INFO line `pathinput_discover: N combo(s); R director(ies) read from
  disk, H served from the listing cache (K too recently changed to cache)` says
  what each walk cost. `K > 0` on a long-unchanged share means mtimes are in
  the future, so suspect server clock skew.
- DEBUG `[pathinput] <dir> changed in the last 2.0 s; listing not cached`
  names each skipped directory.
- If a new folder is missing from discovery, compare the directory's
  `st_mtime_ns` (`os.stat(d).st_mtime_ns`) before and after the change. If it
  didn't move, no mtime-validated cache can see the change, so call
  `scifor.clear_listing_cache()`.

## Tests

- The `tree` fixture backdates its directories by an hour (`_age`). A
  just-built tree is inside the racy window, so it would never be cached and
  the "cache absorbs a re-walk" tests would test nothing.
- `test_a_change_in_the_same_timestamp_tick_is_seen` simulates the race
  deterministically. It discovers a fresh tree, adds a folder, puts the root's
  mtime back to its old value, and asserts the new folder is found and that
  the racy listing was never cached.
