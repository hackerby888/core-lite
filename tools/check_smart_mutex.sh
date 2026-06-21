#!/usr/bin/env bash
#
# Fork-eligibility census enforcement (tick fork-rollback). A bare std::mutex/std::shared_mutex
# DECLARATION in src/ escapes the fork lock census, so it is forbidden unless declared as SmartMutex/
# SmartSharedMutex or annotated `// SMARTMUTEX-EXEMPT: <reason>`. Flags declarations only (lock_guard/
# shared_lock usages and reference/template/ctor forms are ignored). Failure output prints the fix.

set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$ROOT/src"
fail=0

while IFS= read -r -d '' f; do
    case "$f" in
        */extensions/fork_census.h) continue ;;   # the wrapper legitimately wraps std::mutex internally
    esac
    while IFS=: read -r line content; do
        [ -z "${line:-}" ] && continue
        # explicit opt-out (checked on the raw line, before comment strip)
        printf '%s' "$content" | grep -q 'SMARTMUTEX-EXEMPT' && continue
        code="${content%%//*}"                                              # drop // comment
        printf '%s' "$code" | grep -Eq '(lock_guard|unique_lock|shared_lock|scoped_lock)<' && continue
        printf '%s' "$code" | grep -Eq 'std::(mutex|shared_mutex|recursive_mutex)[[:space:]]*[&*>(]' && continue
        if printf '%s' "$code" | grep -Eq 'std::(mutex|shared_mutex|recursive_mutex)'; then
            printf '  %s:%s: %s\n' "$f" "$line" "$(printf '%s' "$content" | sed 's/^[[:space:]]*//')"
            fail=1
        fi
    done < <(grep -nE 'std::(mutex|shared_mutex|recursive_mutex)' "$f" 2>/dev/null)
done < <(find "$SRC" -type f \( -name '*.h' -o -name '*.hpp' -o -name '*.cpp' \) -print0)

if [ "$fail" -ne 0 ]; then
    echo ""
    echo "check_smart_mutex: FAILED — bare std::mutex declaration(s) above are not census-covered."
    echo "Use SmartMutex/SmartSharedMutex (extensions/fork_census.h), or add a"
    echo "'// SMARTMUTEX-EXEMPT: <reason>' marker if a non-AP thread can never hold it over node state."
    exit 1
fi
echo "check_smart_mutex: OK (no unsanctioned std::mutex declarations)"
exit 0
