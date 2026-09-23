#!/usr/bin/env bash
# Regenerate the cleanup-audit measurements (docs/claude/cleanup-audit.md §1).
# Usage: tools/audit/run.sh [out_dir]   (default: tools/audit/out, gitignored)
# Needs only git, node, and coreutils — no Python.
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(git -C "$HERE" rev-parse --show-toplevel)"
OUT="$(mkdir -p "${1:-$HERE/out}" && cd "${1:-$HERE/out}" && pwd)"
cd "$ROOT"

PKGS="scifor scimatlab sciduckdb scidb scilineage scihist scistack scistacklog scistackplot scistackplotdb scicanonicalhash scidb-net scistack-gui"
TEST_RE='(^|/)tests?/|\.test\.'
EXCLUDE_RE='(\.d\.ts$|/dist/|/out/|/media/|^examples/|^docs/|^build/|^site/|^spec/|^path-gen/|^csv-data|^tmp\.py$|^\.claude/|^tools/)'

git ls-files > "$OUT/files.txt"
grep -E '\.(py|m|tsx?)$' "$OUT/files.txt" | grep -vE "$TEST_RE" | grep -vE "$EXCLUDE_RE" > "$OUT/src.txt"

lines() { tr '\n' '\0' | xargs -0 cat 2>/dev/null | wc -l | tr -d ' '; }

# sizes.txt — per package, by language, source vs tests
{
  printf "%-18s %8s %8s %8s %8s %8s\n" package py py_test m m_test ts
  for d in $PKGS; do
    mine() { grep "^$d/" "$OUT/files.txt" | grep -vE "$EXCLUDE_RE" | grep -E "$1"; }
    printf "%-18s %8s %8s %8s %8s %8s\n" "$d" \
      "$(mine '\.py$' | grep -vE "$TEST_RE" | lines)" "$(mine '\.py$' | grep -E "$TEST_RE" | lines)" \
      "$(mine '\.m$' | grep -vE "$TEST_RE" | lines)" "$(mine '\.m$' | grep -E "$TEST_RE" | lines)" \
      "$(mine '\.tsx?$' | grep -vE "$TEST_RE" | lines)"
  done
  echo; echo "== largest source files"
  tr '\n' '\0' < "$OUT/src.txt" | xargs -0 wc -l | sort -rn | sed -n '2,41p'
} > "$OUT/sizes.txt"

# hotspots.txt — commits, fix-like commits, current lines per source file
FIX_RE='fix|bug|crash|broke|hang|regress|trap|wrong|repair'
git log --format='@@%s' --name-only --no-merges > "$OUT/log.txt"
grep -v '^@@' "$OUT/log.txt" | grep -v '^$' | sort | uniq -c > "$OUT/churn_all.txt"
awk -v re="$FIX_RE" '/^@@/{fix = (tolower($0) ~ re); next} NF && fix' "$OUT/log.txt" | sort | uniq -c > "$OUT/churn_fix.txt"
{
  echo "commits: $(grep -c '^@@' "$OUT/log.txt"), fix-like: $(grep '^@@' "$OUT/log.txt" | grep -ciE "$FIX_RE")"
  echo "commits fix_commits lines file"
  awk 'FILENAME==ARGV[1]{a[$2]=$1; next} FILENAME==ARGV[2]{x[$2]=$1; next}
       {cmd="wc -l < \"" $0 "\""; cmd | getline l; close(cmd); print (a[$0]+0), (x[$0]+0), l+0, $0}' \
    "$OUT/churn_all.txt" "$OUT/churn_fix.txt" "$OUT/src.txt" | sort -k2,2nr -k1,1nr | head -50
} > "$OUT/hotspots.txt"

# markers.txt — comments/docstrings flagging patched-around problems
{
  echo "trap/silently/workaround/hack per file:"
  tr '\n' '\0' < "$OUT/src.txt" | xargs -0 grep -ciE '\b(trap|silently|workaround|hack|kludge)\b' | awk -F: '$2>0' | sort -t: -k2 -rn
  echo; echo "date-stamped lines: $(tr '\n' '\0' < "$OUT/src.txt" | xargs -0 cat | grep -cE '20[0-9]{2}-[01][0-9]-[0-3][0-9]' || true)"
  echo "TODO/FIXME/XXX: $(tr '\n' '\0' < "$OUT/src.txt" | xargs -0 cat | grep -cE '\b(TODO|FIXME|XXX)\b' || true)"
  echo "get_database() calls: $(tr '\n' '\0' < "$OUT/src.txt" | xargs -0 cat | grep -cE '\bget_database\(\)' || true)"
} > "$OUT/markers.txt"

node "$HERE/pyscan.js" "$OUT/src.txt" "$OUT/scan.json"
node "$HERE/report.js" "$OUT"
rm -f "$OUT/log.txt" "$OUT/churn_all.txt" "$OUT/churn_fix.txt"
ls "$OUT"
