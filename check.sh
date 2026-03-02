#!/bin/bash
# Lux quality gate. Usage: ./check.sh

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'
BOLD='\033[1m'

PASS=0
FAIL=0
WARN=0

check() {
    local desc="$1"
    shift
    printf "  %-45s" "$desc"
    if "$@" > /dev/null 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS+1))
    else
        echo -e "${RED}FAIL${NC}"
        FAIL=$((FAIL+1))
    fi
}

warn_check() {
    local desc="$1"
    shift
    printf "  %-45s" "$desc"
    if "$@" > /dev/null 2>&1; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS+1))
    else
        echo -e "${YELLOW}WARN${NC}"
        WARN=$((WARN+1))
    fi
}

check "Tests pass (zig build test)" zig build test
check "Release build (ReleaseSafe)" zig build -Doptimize=ReleaseSafe

check "No @panic in production code" bash -c '! grep -rn "@panic" src/*.zig 2>/dev/null | grep -v "^.*:.*test " | grep -v "//.*@panic" | grep -q "@panic"'
check "No anyerror return types" bash -c '! grep -rn "anyerror" src/*.zig 2>/dev/null | grep -v "^.*:.*test " | grep -v "//.*anyerror" | grep -q "anyerror"'
check "No discarded errors (_ = fn())" bash -c '! grep -rEn "_ = [a-zA-Z]+\(" src/*.zig 2>/dev/null | grep -v "^.*:.*test " | grep -v "//.*_" | grep -v "addJournalEntry\|orderedRemove\|orelse" | grep -q "_ ="'
warn_check "No TODO/FIXME/HACK comments" bash -c '! grep -rn "TODO\|FIXME\|HACK\|XXX" src/ --include="*.zig" 2>/dev/null | grep -v "^Binary" | grep -q .'
check "No debug print in production" bash -c '! grep -rn "std.debug.print" src/*.zig 2>/dev/null | grep -v "^.*:.*test " | grep -q "std.debug.print"'

check "No var in app.js" bash -c '! grep -nP "^\s*var\s|\svar\s" src/static/app.js | grep -v "// var ok" | grep -q .'
check "No console.log in app.js" bash -c '! grep -n "console\.log" src/static/app.js | grep -q .'
check "No unsafe eval in app.js" bash -c '! grep -n "[^a-z]eval(" src/static/app.js | grep -q .'

TOTAL_TESTS=$(grep -rc '^test "' src/*.zig 2>/dev/null | awk -F: '{s+=$2} END {print s}')
printf "  %-45s%s tests\n" "Total test count" "$TOTAL_TESTS"

check "Read-only mode implemented" bash -c 'grep -rq "enforceReadOnly" src/*.zig'
check "SQL value escaping exists" bash -c 'grep -rq "escapeStringValue" src/*.zig'
warn_check "No unescaped user input in SQL" bash -c '! grep -rn "++ pk_value\|++ table_name" src/*.zig 2>/dev/null | grep -v "escaped" | grep -q "++"'

if [ "$FAIL" -gt 0 ]; then
    echo -e "  ${RED}${BOLD}FAILED${NC} (PASS:$PASS WARN:$WARN FAIL:$FAIL)"
    exit 1
else
    echo -e "  ${GREEN}${BOLD}PASSED${NC} (PASS:$PASS WARN:$WARN FAIL:$FAIL)"
    exit 0
fi
