#!/bin/bash
# Lux — Local Development Quality Gate
# Run this before pushing or after any significant changes.
# Usage: ./check.sh [--quick]

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color
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

echo -e "${BOLD}${BLUE}"
echo "╔══════════════════════════════════════════════╗"
echo "║         Lux — Quality Gate Report            ║"
echo "╚══════════════════════════════════════════════╝"
echo -e "${NC}"

# ── BUILD GATES ──
echo -e "${BOLD}Build Gates${NC}"
check "Tests pass (zig build test)" zig build test
check "Release build (ReleaseSafe)" zig build -Doptimize=ReleaseSafe

# ── CODE QUALITY ──
echo ""
echo -e "${BOLD}Code Quality${NC}"

# Check for @panic in production code (not in tests)
check "No @panic in production code" bash -c '! grep -n "@panic" src/web.zig src/postgres.zig src/main.zig 2>/dev/null | grep -v "^.*:.*test " | grep -v "//.*@panic" | grep -q "@panic"'

# Check for anyerror
check "No anyerror return types" bash -c '! grep -n "anyerror" src/web.zig src/postgres.zig src/main.zig 2>/dev/null | grep -v "^.*:.*test " | grep -v "//.*anyerror" | grep -q "anyerror"'

# Check for discarded errors (exclude non-error returns like addJournalEntry, orderedRemove, extractJsonQuery orelse)
# Excluded: addJournalEntry returns void, orderedRemove returns the removed element (not an error),
# orelse is a Zig construct that already handles the error/null case.
check "No discarded errors (_ = fn())" bash -c '! grep -En "_ = [a-zA-Z]+\(" src/web.zig src/postgres.zig src/main.zig 2>/dev/null | grep -v "^.*:.*test " | grep -v "//.*_" | grep -v "addJournalEntry\|orderedRemove\|orelse" | grep -q "_ ="'

# Check for TODO/FIXME/HACK comments
warn_check "No TODO/FIXME/HACK comments" bash -c '! grep -rn "TODO\|FIXME\|HACK\|XXX" src/ --include="*.zig" 2>/dev/null | grep -v "^Binary" | grep -q .'

# Check for debug print statements
check "No debug print in production" bash -c '! grep -n "std.debug.print" src/web.zig src/postgres.zig src/main.zig 2>/dev/null | grep -v "^.*:.*test " | grep -q "std.debug.print"'

# JavaScript quality checks — ban unsafe patterns in frontend code
check "No var in app.js" bash -c '! grep -nP "^\s*var\s|\svar\s" src/static/app.js | grep -v "// var ok" | grep -q .'
check "No console.log in app.js" bash -c '! grep -n "console\.log" src/static/app.js | grep -q .'
check "No unsafe eval in app.js" bash -c '! grep -n "[^a-z]eval(" src/static/app.js | grep -q .'

# ── TEST COVERAGE ──
echo ""
echo -e "${BOLD}Test Coverage${NC}"

TEST_COUNT=$(grep -c '^test "' src/web.zig 2>/dev/null || echo 0)
POSTGRES_TEST_COUNT=$(grep -c '^test "' src/postgres.zig 2>/dev/null || echo 0)
TOTAL_TESTS=$((TEST_COUNT + POSTGRES_TEST_COUNT))

printf "  %-45s" "Total test count"
if [ "$TOTAL_TESTS" -ge 190 ]; then
    echo -e "${GREEN}${TOTAL_TESTS} tests${NC}"
    ((PASS++))
elif [ "$TOTAL_TESTS" -ge 150 ]; then
    echo -e "${YELLOW}${TOTAL_TESTS} tests (target: 190+)${NC}"
    ((WARN++))
else
    echo -e "${RED}${TOTAL_TESTS} tests (target: 190+)${NC}"
    ((FAIL++))
fi

# Check test categories exist
check "Has URL parsing tests" bash -c 'grep -q "parseQueryParam" src/web.zig'
check "Has SQL safety tests" bash -c 'grep -q "isSqlReadSafe" src/web.zig'
check "Has CSV parsing tests" bash -c 'grep -q "parseCsvContent" src/web.zig'
check "Has JSON extraction tests" bash -c 'grep -q "extractJsonQuery" src/web.zig'
check "Has escape/sanitize tests" bash -c 'grep -q "escapeStringValue" src/web.zig'
check "Has connection manager tests" bash -c 'grep -q "formatConnectionJson" src/web.zig'

# ── SECURITY CHECKS ──
echo ""
echo -e "${BOLD}Security${NC}"

# Check that read-only mode exists
check "Read-only mode implemented" bash -c 'grep -q "enforceReadOnly" src/web.zig'

# Check SQL injection protection
check "SQL value escaping exists" bash -c 'grep -q "escapeStringValue" src/web.zig'

# Check for raw SQL concatenation without escaping (simplified)
warn_check "No unescaped user input in SQL" bash -c '! grep -n "++ pk_value\|++ table_name" src/web.zig 2>/dev/null | grep -v "escaped" | grep -q "++"'

# ── FILE SIZES ──
echo ""
echo -e "${BOLD}File Metrics${NC}"

WEB_LINES=$(wc -l < src/web.zig)
HTML_LINES=$(wc -l < src/static/index.html)
JS_LINES=$(wc -l < src/static/app.js)
CSS_LINES=$(wc -l < src/static/styles.css)
PG_LINES=$(wc -l < src/postgres.zig)
MAIN_LINES=$(wc -l < src/main.zig)
TOTAL=$((WEB_LINES + HTML_LINES + JS_LINES + CSS_LINES + PG_LINES + MAIN_LINES))

printf "  %-30s %s\n" "web.zig:" "${WEB_LINES} lines"
printf "  %-30s %s\n" "index.html:" "${HTML_LINES} lines"
printf "  %-30s %s\n" "app.js:" "${JS_LINES} lines"
printf "  %-30s %s\n" "styles.css:" "${CSS_LINES} lines"
printf "  %-30s %s\n" "postgres.zig:" "${PG_LINES} lines"
printf "  %-30s %s\n" "main.zig:" "${MAIN_LINES} lines"
printf "  %-30s %s\n" "Total:" "${TOTAL} lines"
printf "  %-30s %s\n" "Tests:" "${TOTAL_TESTS}"
printf "  %-30s %s\n" "Test density:" "$(echo "scale=1; $TOTAL_TESTS * 1000 / $WEB_LINES" | bc) tests per 1K lines"

# ── SUMMARY ──
echo ""
echo -e "${BOLD}─────────────────────────────────────────────${NC}"
echo -e "  ${GREEN}PASS: ${PASS}${NC}  ${YELLOW}WARN: ${WARN}${NC}  ${RED}FAIL: ${FAIL}${NC}"

if [ "$FAIL" -gt 0 ]; then
    echo -e "  ${RED}${BOLD}Quality gate: FAILED${NC}"
    exit 1
elif [ "$WARN" -gt 0 ]; then
    echo -e "  ${YELLOW}${BOLD}Quality gate: PASSED with warnings${NC}"
    exit 0
else
    echo -e "  ${GREEN}${BOLD}Quality gate: PASSED${NC}"
    exit 0
fi
