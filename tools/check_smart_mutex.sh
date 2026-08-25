#!/usr/bin/env bash
#
# Reject bare mutex declarations that bypass the fork lock census.
# Use a SmartMutex wrapper or a SMARTMUTEX-EXEMPT rationale for safe exceptions.

set -u
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE_DIR="$REPO_ROOT/src"
failed=0

while IFS= read -r -d '' source_file; do
    case "$source_file" in
        */extensions/fork_census.h)
            continue
            ;;
    esac
    while IFS=: read -r line content; do
        if [ -z "${line:-}" ]; then
            continue
        fi
        if printf '%s' "$content" | grep -q 'SMARTMUTEX-EXEMPT'; then
            continue
        fi

        source_code="${content%%//*}"
        if printf '%s' "$source_code" | grep -Eq '(lock_guard|unique_lock|shared_lock|scoped_lock)<'; then
            continue
        fi
        if printf '%s' "$source_code" | grep -Eq 'std::(mutex|shared_mutex|recursive_mutex)[[:space:]]*[&*>(]'; then
            continue
        fi
        if printf '%s' "$source_code" | grep -Eq 'std::(mutex|shared_mutex|recursive_mutex)'; then
            printf '  %s:%s: %s\n' \
                "$source_file" \
                "$line" \
                "$(printf '%s' "$content" | sed 's/^[[:space:]]*//')"
            failed=1
        fi
    done < <(grep -nE 'std::(mutex|shared_mutex|recursive_mutex)' "$source_file" 2>/dev/null)
done < <(find "$SOURCE_DIR" -type f \( -name '*.h' -o -name '*.hpp' -o -name '*.cpp' \) -print0)

if [ "$failed" -ne 0 ]; then
    echo
    echo "check_smart_mutex: FAILED — bare std::mutex declaration(s) above are not census-covered."
    echo "Use SmartMutex/SmartSharedMutex (extensions/fork_census.h), or add a"
    echo "'// SMARTMUTEX-EXEMPT: <reason>' marker if a non-AP thread can never hold it over node state."
    exit 1
fi
echo "check_smart_mutex: OK (no unsanctioned std::mutex declarations)"
exit 0
