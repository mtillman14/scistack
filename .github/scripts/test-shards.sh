#!/usr/bin/env bash
# Owns how CI splits the pytest suite into parallel jobs. ci.yml builds its
# test matrix from `matrix` and each job asks `files` for its share, so the
# package list and shard counts live here and nowhere else.
#
#   test-shards.sh matrix                    JSON matrix for the test job
#   test-shards.sh check                     fail if a tests/ dir is unaccounted for
#   test-shards.sh files <pkg> <shard> <of>  test files for one shard of a package
#   test-shards.sh run <pkgs> <shard> <of>   run pytest for one matrix entry
#
# Packages always run in separate pytest processes, one at a time (their
# conftest.py modules collide when two tests/ dirs share a session). A package
# with N > 1 shards is split by FILE, so each shard is still one pytest process
# over a subset of that package's tests/ — same rootdir, same conftest.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# name|packages|shards — tune shards from the --durations output of past runs.
# A group of several packages must have 1 shard.
SHARD_GROUPS=(
    "small|scistacklog scicanonicalhash path-gen sciduckdb scilineage scistack|1"
    "scifor|scifor|1"
    "scihist|scihist|1"
    "scistackplotdb|scistackplotdb|1"
    "scistackstats|scistackstats scistackstatsdb|1"
    "scistackplot|scistackplot|3"
    "scidb|scidb|4"
    "scistack-gui|scistack-gui|4"
)

# tests/ dirs deliberately not run in CI, with the reason.
EXCLUDED=(
    "scimatlab|needs a MATLAB licence"
    "scidb-net|outdated, slated for removal"
    "tests|repo-level integration suite, run by hand"
)

die() { echo "::error::$*" >&2; exit 1; }

# One line per test file: "<weight> <path>", weight = number of test functions
# (min 1). Parametrized cases count once; good enough to balance by.
weighted_files() {
    local pkg="$1"
    find "$ROOT/$pkg/tests" -name 'test_*.py' -not -path '*/__pycache__/*' | sort |
    while read -r f; do
        local n
        n=$(grep -cE '^\s*(async\s+)?def test_' "$f")
        (( n < 1 )) && n=1
        echo "$n ${f#"$ROOT"/}"
    done
}

# Greedy: heaviest file first, into the currently lightest shard (lowest index
# on ties). Deterministic for a given tree. Prints "<shard> <weight> <path>".
assign() {
    local pkg="$1" of="$2"
    weighted_files "$pkg" | sort -k1,1nr -k2,2 |
    awk -v of="$of" '{
        best = 1
        for (s = 2; s <= of; s++) if (load[s] < load[best]) best = s
        load[best] += $1
        print best, $1, $2
    }'
}

cmd_files() {
    local pkg="$1" shard="$2" of="$3"
    [[ -d "$ROOT/$pkg/tests" ]] || die "no tests/ dir for $pkg"
    (( shard >= 1 && shard <= of )) || die "shard $shard out of range 1..$of"
    local plan
    plan=$(assign "$pkg" "$of")
    # Log the whole split so an unbalanced shard is visible from any one job.
    echo "shard plan for $pkg ($of shards):" >&2
    echo "$plan" | awk '{w[$1] += $2; n[$1]++} END {for (s in w) printf "  shard %s: %d files, weight %d\n", s, n[s], w[s]}' | sort >&2
    echo "$plan" | awk -v s="$shard" '$1 == s {print $3}'
}

cmd_matrix() {
    local first=1 entry name pkgs of s
    printf '{"include":['
    for entry in "${SHARD_GROUPS[@]}"; do
        IFS='|' read -r name pkgs of <<<"$entry"
        if [[ "$pkgs" == *" "* ]] && (( of != 1 )); then
            die "group $name has several packages, so it must have 1 shard"
        fi
        for (( s = 1; s <= of; s++ )); do
            (( first )) || printf ','
            first=0
            if (( of == 1 )); then label="$name"; else label="$name $s/$of"; fi
            printf '{"name":"%s","pkgs":"%s","shard":%d,"of":%d}' "$label" "$pkgs" "$s" "$of"
        done
    done
    printf ']}\n'
}

# Every top-level dir with a tests/ holding test_*.py must be in exactly one
# group or in EXCLUDED, so a new package cannot silently go untested.
cmd_check() {
    local -A seen=()
    local entry name pkgs of reason pkg bad=0
    for entry in "${SHARD_GROUPS[@]}"; do
        IFS='|' read -r name pkgs of <<<"$entry"
        for pkg in $pkgs; do
            [[ -n "${seen[$pkg]:-}" ]] && { echo "::error::$pkg is listed twice"; bad=1; }
            [[ -d "$ROOT/$pkg/tests" ]] || { echo "::error::$pkg has no tests/ dir"; bad=1; }
            seen[$pkg]="group $name"
        done
    done
    for entry in "${EXCLUDED[@]}"; do
        IFS='|' read -r pkg reason <<<"$entry"
        [[ -n "${seen[$pkg]:-}" ]] && { echo "::error::$pkg is both grouped and excluded"; bad=1; }
        seen[$pkg]="excluded ($reason)"
    done
    while read -r dir; do
        pkg="${dir#"$ROOT"/}"; pkg="${pkg%/tests}"
        if [[ -z "${seen[$pkg]:-}" ]]; then
            echo "::error::$pkg/tests is not in SHARD_GROUPS or EXCLUDED of .github/scripts/test-shards.sh"
            bad=1
        else
            echo "  $pkg: ${seen[$pkg]}"
        fi
    done < <(find "$ROOT" -mindepth 1 -maxdepth 2 -type d -name tests -not -path '*/node_modules/*' |
             while read -r d; do [[ -n "$(find "$d" -name 'test_*.py' -print -quit)" ]] && echo "$d"; done | sort)
    return "$bad"
}

cmd_run() {
    local pkgs="$1" shard="$2" of="$3" pkg fail=() pass=() files
    for pkg in $pkgs; do
        echo "::group::$pkg (shard $shard/$of)"
        if (( of == 1 )); then
            files="$ROOT/$pkg/tests"
        else
            files=$(cmd_files "$pkg" "$shard" "$of") || exit 1
            [[ -n "$files" ]] || { echo "no files in this shard"; echo "::endgroup::"; continue; }
            echo "$files"
        fi
        # shellcheck disable=SC2086  # word-split the file list on purpose
        if (cd "$ROOT" && pytest --durations=15 $files); then pass+=("$pkg"); else fail+=("$pkg"); fi
        echo "::endgroup::"
    done
    echo "PASSED (${#pass[@]}): ${pass[*]:-—}"
    echo "FAILED (${#fail[@]}): ${fail[*]:-—}"
    [[ ${#fail[@]} -eq 0 ]]
}

case "${1:-}" in
    matrix) cmd_matrix ;;
    check)  cmd_check ;;
    files)  (( $# == 4 )) || die "usage: $0 files <pkg> <shard> <of>"; cmd_files "$2" "$3" "$4" ;;
    run)    (( $# == 4 )) || die "usage: $0 run <pkgs> <shard> <of>"; cmd_run "$2" "$3" "$4" ;;
    *)      die "usage: $0 {matrix|check|files|run}" ;;
esac
