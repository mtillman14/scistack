#!/bin/bash
# Claude Code status line: live pytest progress (tools/pytest-progress).
# Shows every run that is in progress (updated in the last 10 min), and a
# finished run's result for 5 min after it ends. Prints nothing otherwise.
cat > /dev/null  # the session JSON on stdin is not needed
dir="${SCISTACK_PYTEST_PROGRESS_DIR:-/workspace/.pytest-progress}"
[ -d "$dir" ] || exit 0
now=$(date +%s)
for f in "$dir"/*.json; do
  [ -e "$f" ] || continue
  jq -r --argjson now "$now" '
    def bad: (.failed + .errors);
    if .state == "done" and ($now - .updated) < 300 then
      "pytest \(.package) " + (if bad > 0 then "✗" else "✓" end)
      + " \(.passed) passed · \(.failed) failed · \(.errors) errors · \(.skipped) skipped"
      + (if (.failures | length) > 0 then " · first: \(.failures[0])" else "" end)
    elif .state != "done" and ($now - .updated) < 600 then
      "pytest \(.package) "
      + (if .total > 0 then "\((.done * 100 / .total) | floor)% \(.done)/\(.total)" else "collecting" end)
      + " · ✓\(.passed) ✗\(.failed) E\(.errors) s\(.skipped)"
      + (if (.failures | length) > 0 then " · ✗ \(.failures[-1])" else "" end)
      + " · \(.current | split("::") | last)"
    else empty end' "$f" 2>/dev/null
done
