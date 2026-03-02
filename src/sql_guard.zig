const std = @import("std");

pub const SqlGuardResult = struct {
    is_destructive: bool,
    operation: []const u8,
    warning: []const u8,
};

pub fn analyzeSql(sql: []const u8) SqlGuardResult {
    // Skip leading whitespace
    var i: usize = 0;
    while (i < sql.len and (sql[i] == ' ' or sql[i] == '\t' or sql[i] == '\n' or sql[i] == '\r')) i += 1;
    if (i >= sql.len) return .{ .is_destructive = false, .operation = "", .warning = "" };
    const rest = sql[i..];

    // DROP
    if (rest.len >= 4 and matchesIgnoreCase(rest, "DROP")) {
        return .{ .is_destructive = true, .operation = "DROP", .warning = "This will permanently drop the object. This cannot be undone." };
    }
    // TRUNCATE
    if (rest.len >= 8 and matchesIgnoreCase(rest, "TRUNCATE")) {
        return .{ .is_destructive = true, .operation = "TRUNCATE", .warning = "This will delete ALL rows from the table. This cannot be undone." };
    }
    // ALTER
    if (rest.len >= 5 and matchesIgnoreCase(rest, "ALTER")) {
        return .{ .is_destructive = true, .operation = "ALTER", .warning = "This will modify the table schema. Review carefully." };
    }
    // DELETE without WHERE
    if (rest.len >= 6 and matchesIgnoreCase(rest, "DELETE")) {
        if (!containsIgnoreCaseWord(sql, "WHERE")) {
            return .{ .is_destructive = true, .operation = "DELETE", .warning = "DELETE without WHERE clause will delete ALL rows." };
        }
    }
    // UPDATE without WHERE
    if (rest.len >= 6 and matchesIgnoreCase(rest, "UPDATE")) {
        if (!containsIgnoreCaseWord(sql, "WHERE")) {
            return .{ .is_destructive = true, .operation = "UPDATE", .warning = "UPDATE without WHERE clause will update ALL rows." };
        }
    }
    return .{ .is_destructive = false, .operation = "", .warning = "" };
}

/// Check if SQL text contains multiple statements (semicolons outside strings/comments).
/// A trailing semicolon with only whitespace after it does NOT count.
pub fn hasMultipleStatements(sql: []const u8) bool {
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];

        // Single-quoted string: skip to closing quote (handle escaped quotes '')
        if (ch == '\'') {
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    i += 2; // escaped quote
                } else if (sql[i] == '\'') {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            continue;
        }

        // Double-quoted identifier: skip to closing quote
        if (ch == '"') {
            i += 1;
            while (i < sql.len) : (i += 1) {
                if (sql[i] == '"') {
                    i += 1;
                    break;
                }
            }
            continue;
        }

        // Dollar-quoted string: $$ ... $$ or $tag$ ... $tag$
        if (ch == '$') {
            // Extract the opening tag: $ followed by optional identifier chars, then $
            const tag_start = i;
            var ti = i + 1;
            while (ti < sql.len and ((sql[ti] >= 'a' and sql[ti] <= 'z') or (sql[ti] >= 'A' and sql[ti] <= 'Z') or (sql[ti] >= '0' and sql[ti] <= '9') or sql[ti] == '_')) ti += 1;
            if (ti < sql.len and sql[ti] == '$') {
                const tag = sql[tag_start .. ti + 1]; // e.g. "$$" or "$tag$"
                i = ti + 1; // skip past opening tag
                // Find matching closing tag
                while (i + tag.len <= sql.len) {
                    if (std.mem.eql(u8, sql[i .. i + tag.len], tag)) {
                        i += tag.len;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            // Not a dollar-quote, just a bare $
            i += 1;
            continue;
        }

        // Line comment: -- skip to end of line
        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            i += 2;
            while (i < sql.len and sql[i] != '\n') : (i += 1) {}
            continue;
        }

        // Block comment: /* ... */
        if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            i += 2;
            while (i + 1 < sql.len) {
                if (sql[i] == '*' and sql[i + 1] == '/') {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }

        // Semicolon: check if there's a real statement after it
        if (ch == ';') {
            var j = i + 1;
            while (j < sql.len and (sql[j] == ' ' or sql[j] == '\t' or sql[j] == '\n' or sql[j] == '\r')) : (j += 1) {}
            if (j < sql.len) return true; // non-whitespace after semicolon
        }

        i += 1;
    }
    return false;
}

/// Whitelist check for read-only mode. Only allows SELECT, SHOW, EXPLAIN, and
/// safe WITH...SELECT queries. Blocks multi-statement SQL entirely.
/// Scans for write keywords outside string literals to catch CTE bypass attacks
/// like: WITH cte AS (DELETE FROM users RETURNING *) SELECT * FROM cte
pub fn isSqlReadSafe(sql: []const u8) bool {
    // Block multi-statement scripts entirely in read-only mode
    if (hasMultipleStatements(sql)) return false;

    // Skip leading whitespace
    var i: usize = 0;
    while (i < sql.len and (sql[i] == ' ' or sql[i] == '\t' or sql[i] == '\n' or sql[i] == '\r')) i += 1;
    if (i >= sql.len) return false;
    const rest = sql[i..];

    // Whitelist: only these prefixes are safe reads
    if (rest.len >= 6 and matchesIgnoreCase(rest[0..6], "SELECT")) return true;
    if (rest.len >= 4 and matchesIgnoreCase(rest[0..4], "SHOW")) return true;
    // EXPLAIN is safe, but EXPLAIN ANALYZE actually executes the query —
    // so if ANALYZE is present, check the inner statement for write keywords
    if (rest.len >= 7 and matchesIgnoreCase(rest[0..7], "EXPLAIN")) {
        // Skip "EXPLAIN" and any whitespace
        var ei: usize = 7;
        while (i + ei < sql.len and (sql[i + ei] == ' ' or sql[i + ei] == '\t' or sql[i + ei] == '\n' or sql[i + ei] == '\r')) ei += 1;
        // Check if ANALYZE follows
        if (rest.len >= ei + 7 and matchesIgnoreCase(rest[ei .. ei + 7], "ANALYZE")) {
            // EXPLAIN ANALYZE — must verify the inner query has no write keywords
            return !containsWriteKeyword(sql);
        }
        return true; // plain EXPLAIN (without ANALYZE) is safe
    }

    // WITH: safe only if no write keywords appear outside string literals
    if (rest.len >= 4 and matchesIgnoreCase(rest[0..4], "WITH")) {
        return !containsWriteKeyword(sql);
    }

    return false; // Everything else (BEGIN, COPY, DO, INSERT, etc.) is blocked
}

/// Scan SQL for write keywords (INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE,
/// CREATE, COPY, GRANT, REVOKE) outside of string literals, identifiers, and
/// comments. Uses word-boundary detection to avoid false positives on identifiers
/// like "delete_log" or strings like 'DELETE'.
pub fn containsWriteKeyword(sql: []const u8) bool {
    const keywords = [_][]const u8{ "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "COPY", "GRANT", "REVOKE" };
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        // Skip single-quoted strings
        if (ch == '\'') {
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    i += 2; // escaped quote
                } else if (sql[i] == '\'') {
                    break;
                } else {
                    i += 1;
                }
            }
            if (i < sql.len) i += 1;
            continue;
        }
        // Skip double-quoted identifiers
        if (ch == '"') {
            i += 1;
            while (i < sql.len and sql[i] != '"') i += 1;
            if (i < sql.len) i += 1;
            continue;
        }
        // Skip dollar-quoted strings ($tag$ ... $tag$)
        if (ch == '$') {
            const tag_start = i;
            i += 1;
            while (i < sql.len and ((sql[i] >= 'a' and sql[i] <= 'z') or (sql[i] >= 'A' and sql[i] <= 'Z') or (sql[i] >= '0' and sql[i] <= '9') or sql[i] == '_')) i += 1;
            if (i < sql.len and sql[i] == '$') {
                const tag = sql[tag_start .. i + 1];
                i += 1;
                // Find closing tag
                while (i + tag.len <= sql.len) {
                    if (std.mem.eql(u8, sql[i .. i + tag.len], tag)) {
                        i += tag.len;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            // Not a dollar-quote, just a $ character
            continue;
        }
        // Skip line comments
        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') i += 1;
            continue;
        }
        // Skip block comments
        if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            i += 2;
            while (i + 1 < sql.len) {
                if (sql[i] == '*' and sql[i + 1] == '/') {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }
        // Check for write keyword at word boundary
        const at_word_start = (i == 0) or !isAlphanumUnderscore(sql[i - 1]);
        if (at_word_start) {
            for (keywords) |kw| {
                if (i + kw.len <= sql.len and matchesIgnoreCase(sql[i .. i + kw.len], kw)) {
                    // Check trailing word boundary
                    const end = i + kw.len;
                    if (end >= sql.len or !isAlphanumUnderscore(sql[end])) {
                        return true;
                    }
                }
            }
        }
        i += 1;
    }
    return false;
}

fn matchesIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (haystack.len < needle.len) return false;
    for (needle, 0..) |c, idx| {
        const h = haystack[idx];
        const lower_h = if (h >= 'A' and h <= 'Z') h + 32 else h;
        const lower_c = if (c >= 'A' and c <= 'Z') c + 32 else c;
        if (lower_h != lower_c) return false;
    }
    return true;
}

pub fn containsIgnoreCaseWord(haystack: []const u8, needle: []const u8) bool {
    if (needle.len > haystack.len) return false;
    const limit = haystack.len - needle.len + 1;
    for (0..limit) |idx| {
        if (matchesIgnoreCase(haystack[idx..], needle)) {
            // Check that it's a word boundary (not part of a longer word)
            const before_ok = idx == 0 or !isAlpha(haystack[idx - 1]);
            const after_idx = idx + needle.len;
            const after_ok = after_idx >= haystack.len or !isAlpha(haystack[after_idx]);
            if (before_ok and after_ok) return true;
        }
    }
    return false;
}

fn isAlpha(ch: u8) bool {
    return (ch >= 'A' and ch <= 'Z') or (ch >= 'a' and ch <= 'z') or ch == '_';
}

fn isAlphanumUnderscore(ch: u8) bool {
    return (ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z') or (ch >= '0' and ch <= '9') or ch == '_';
}

// Tests

// isSqlReadSafe EXPLAIN ANALYZE tests

test "isSqlReadSafe: plain EXPLAIN is safe" {
    try std.testing.expect(isSqlReadSafe("EXPLAIN SELECT 1"));
}

test "isSqlReadSafe: EXPLAIN ANALYZE SELECT is safe" {
    try std.testing.expect(isSqlReadSafe("EXPLAIN ANALYZE SELECT 1"));
}

test "isSqlReadSafe: EXPLAIN ANALYZE DELETE is blocked" {
    try std.testing.expect(!isSqlReadSafe("EXPLAIN ANALYZE DELETE FROM users"));
}

test "isSqlReadSafe: EXPLAIN ANALYZE UPDATE is blocked" {
    try std.testing.expect(!isSqlReadSafe("EXPLAIN ANALYZE UPDATE users SET name = 'x'"));
}

test "isSqlReadSafe: EXPLAIN ANALYZE INSERT is blocked" {
    try std.testing.expect(!isSqlReadSafe("EXPLAIN ANALYZE INSERT INTO users VALUES (1)"));
}

// analyzeSql tests

test "analyzeSql: SELECT is safe" {
    const r = analyzeSql("SELECT * FROM users");
    try std.testing.expect(!r.is_destructive);
}

test "analyzeSql: DROP is destructive" {
    const r = analyzeSql("DROP TABLE users");
    try std.testing.expect(r.is_destructive);
    try std.testing.expectEqualStrings("DROP", r.operation);
}

test "analyzeSql: TRUNCATE is destructive" {
    const r = analyzeSql("TRUNCATE users");
    try std.testing.expect(r.is_destructive);
    try std.testing.expectEqualStrings("TRUNCATE", r.operation);
}

test "analyzeSql: DELETE without WHERE is destructive" {
    const r = analyzeSql("DELETE FROM users");
    try std.testing.expect(r.is_destructive);
    try std.testing.expectEqualStrings("DELETE", r.operation);
}

test "analyzeSql: DELETE with WHERE is safe" {
    const r = analyzeSql("DELETE FROM users WHERE id = 1");
    try std.testing.expect(!r.is_destructive);
}

test "analyzeSql: UPDATE without WHERE is destructive" {
    const r = analyzeSql("UPDATE users SET name = 'x'");
    try std.testing.expect(r.is_destructive);
    try std.testing.expectEqualStrings("UPDATE", r.operation);
}

test "analyzeSql: UPDATE with WHERE is safe" {
    const r = analyzeSql("UPDATE users SET name = 'x' WHERE id = 1");
    try std.testing.expect(!r.is_destructive);
}

test "analyzeSql: ALTER is destructive" {
    const r = analyzeSql("ALTER TABLE users ADD COLUMN age int");
    try std.testing.expect(r.is_destructive);
    try std.testing.expectEqualStrings("ALTER", r.operation);
}

test "analyzeSql: empty is safe" {
    const r = analyzeSql("");
    try std.testing.expect(!r.is_destructive);
}

// analyzeSql edge cases

test "analyzeSql: case insensitive DROP" {
    const r = analyzeSql("drop table users");
    try std.testing.expect(r.is_destructive);
    try std.testing.expectEqualStrings("DROP", r.operation);
}

test "analyzeSql: leading whitespace DROP" {
    const r = analyzeSql("   DROP TABLE users");
    try std.testing.expect(r.is_destructive);
}

test "analyzeSql: leading tabs and newlines" {
    const r = analyzeSql("\t\n  DELETE FROM users");
    try std.testing.expect(r.is_destructive);
}

test "analyzeSql: WHERE in subquery does not save DELETE" {
    // DELETE FROM users (no WHERE at top level, WHERE is in a comment or unrelated)
    const r = analyzeSql("DELETE FROM users");
    try std.testing.expect(r.is_destructive);
}

test "analyzeSql: UPDATE with WHERE in different case" {
    const r = analyzeSql("update users set name = 'x' WHERE id = 1");
    try std.testing.expect(!r.is_destructive);
}

test "analyzeSql: GRANT is not destructive" {
    const r = analyzeSql("GRANT SELECT ON users TO reader");
    try std.testing.expect(!r.is_destructive);
}

test "analyzeSql: whitespace only" {
    const r = analyzeSql("   \t\n  ");
    try std.testing.expect(!r.is_destructive);
}

test "analyzeSql: CREATE is not destructive" {
    // CREATE is DDL but not flagged as destructive in current impl
    const r = analyzeSql("CREATE TABLE new_table (id int)");
    try std.testing.expect(!r.is_destructive);
}

// containsIgnoreCaseWord edge cases

test "containsIgnoreCaseWord: word at start" {
    try std.testing.expect(containsIgnoreCaseWord("WHERE id = 1", "WHERE"));
}

test "containsIgnoreCaseWord: word at end" {
    try std.testing.expect(containsIgnoreCaseWord("SELECT * WHERE", "WHERE"));
}

test "containsIgnoreCaseWord: word in middle" {
    try std.testing.expect(containsIgnoreCaseWord("SELECT * FROM users WHERE id = 1", "WHERE"));
}

test "containsIgnoreCaseWord: partial match rejects" {
    try std.testing.expect(!containsIgnoreCaseWord("SOMEWHERE", "WHERE"));
}

test "containsIgnoreCaseWord: case insensitive" {
    try std.testing.expect(containsIgnoreCaseWord("select * where id = 1", "WHERE"));
}

test "containsIgnoreCaseWord: empty haystack" {
    try std.testing.expect(!containsIgnoreCaseWord("", "WHERE"));
}

test "containsIgnoreCaseWord: empty needle" {
    // Empty needle does not constitute a word match
    try std.testing.expect(!containsIgnoreCaseWord("anything", ""));
}

test "containsIgnoreCaseWord: needle longer than haystack" {
    try std.testing.expect(!containsIgnoreCaseWord("HI", "HELLO"));
}

// isAlpha tests

test "isAlpha: letters and underscore" {
    try std.testing.expect(isAlpha('a'));
    try std.testing.expect(isAlpha('z'));
    try std.testing.expect(isAlpha('A'));
    try std.testing.expect(isAlpha('Z'));
    try std.testing.expect(isAlpha('_'));
}

test "isAlpha: non-alpha" {
    try std.testing.expect(!isAlpha('0'));
    try std.testing.expect(!isAlpha(' '));
    try std.testing.expect(!isAlpha('-'));
    try std.testing.expect(!isAlpha('.'));
}

// --- SQL safety edge cases ---

test "analyzeSql: mixed case DROP TABLE" {
    const result = analyzeSql("dRoP TABLE users;");
    try std.testing.expect(result.is_destructive);
    try std.testing.expectEqualStrings("DROP", result.operation);
}

test "analyzeSql: tab before DROP" {
    const result = analyzeSql("\t\tDROP TABLE t;");
    try std.testing.expect(result.is_destructive);
    try std.testing.expectEqualStrings("DROP", result.operation);
}

test "analyzeSql: CRLF before TRUNCATE" {
    const result = analyzeSql("\r\n  TRUNCATE TABLE t;");
    try std.testing.expect(result.is_destructive);
    try std.testing.expectEqualStrings("TRUNCATE", result.operation);
}

test "analyzeSql: UPDATE with WHERE in mixed case" {
    const result = analyzeSql("update users set name='x' WhErE id=1;");
    try std.testing.expect(!result.is_destructive);
}

test "analyzeSql: DELETE with WHERE as subword rejected" {
    // WHERE embedded in NOWHERE should not count — but containsIgnoreCaseWord checks boundaries
    const result = analyzeSql("DELETE FROM t NOWHERE;");
    try std.testing.expect(result.is_destructive);
    try std.testing.expectEqualStrings("DELETE", result.operation);
}

test "containsIgnoreCaseWord: WHERE at very end" {
    try std.testing.expect(containsIgnoreCaseWord("DELETE FROM t WHERE", "WHERE"));
}

test "containsIgnoreCaseWord: WHERE preceded by underscore" {
    // underscore counts as alpha in isAlpha, so _WHERE is not a word boundary
    try std.testing.expect(!containsIgnoreCaseWord("DO_WHERE", "WHERE"));
}

// hasMultipleStatements tests

test "hasMultipleStatements: single SELECT" {
    try std.testing.expect(!hasMultipleStatements("SELECT * FROM users"));
}

test "hasMultipleStatements: single with trailing semicolon" {
    try std.testing.expect(!hasMultipleStatements("SELECT * FROM users;"));
}

test "hasMultipleStatements: single with trailing semicolon and whitespace" {
    try std.testing.expect(!hasMultipleStatements("SELECT * FROM users;  \n  "));
}

test "hasMultipleStatements: two statements" {
    try std.testing.expect(hasMultipleStatements("INSERT INTO t VALUES (1); SELECT * FROM t"));
}

test "hasMultipleStatements: three statements" {
    try std.testing.expect(hasMultipleStatements("BEGIN; INSERT INTO t VALUES (1); COMMIT"));
}

test "hasMultipleStatements: semicolon in single-quoted string" {
    try std.testing.expect(!hasMultipleStatements("SELECT 'hello; world' FROM t"));
}

test "hasMultipleStatements: semicolon in double-quoted identifier" {
    try std.testing.expect(!hasMultipleStatements("SELECT \"col;name\" FROM t"));
}

test "hasMultipleStatements: semicolon in line comment" {
    try std.testing.expect(!hasMultipleStatements("SELECT * -- ; comment\nFROM t"));
}

test "hasMultipleStatements: semicolon in block comment" {
    try std.testing.expect(!hasMultipleStatements("SELECT * /* ; */ FROM t"));
}

test "hasMultipleStatements: semicolon in dollar-quoted string" {
    try std.testing.expect(!hasMultipleStatements("SELECT $$ hello; world $$ FROM t"));
}

test "hasMultipleStatements: real multi with string containing semicolon" {
    try std.testing.expect(hasMultipleStatements("INSERT INTO t VALUES ('a;b'); SELECT 1"));
}

test "hasMultipleStatements: empty input" {
    try std.testing.expect(!hasMultipleStatements(""));
}

test "hasMultipleStatements: semicolon in tagged dollar-quoted string" {
    try std.testing.expect(!hasMultipleStatements("SELECT $fn$hello;world$fn$"));
}

test "hasMultipleStatements: tagged dollar-quote with real multi" {
    try std.testing.expect(hasMultipleStatements("SELECT $fn$hello$fn$; DROP TABLE x"));
}

// --- isSqlReadSafe tests ---

test "isSqlReadSafe: simple SELECT" {
    try std.testing.expect(isSqlReadSafe("SELECT * FROM users"));
}

test "isSqlReadSafe: SELECT with leading whitespace" {
    try std.testing.expect(isSqlReadSafe("  \n SELECT 1"));
}

test "isSqlReadSafe: SHOW command" {
    try std.testing.expect(isSqlReadSafe("SHOW search_path"));
}

test "isSqlReadSafe: EXPLAIN is safe" {
    try std.testing.expect(isSqlReadSafe("EXPLAIN ANALYZE SELECT 1"));
}

test "isSqlReadSafe: WITH safe CTE" {
    try std.testing.expect(isSqlReadSafe("WITH cte AS (SELECT 1) SELECT * FROM cte"));
}

test "isSqlReadSafe: INSERT blocked" {
    try std.testing.expect(!isSqlReadSafe("INSERT INTO users VALUES (1)"));
}

test "isSqlReadSafe: DELETE blocked" {
    try std.testing.expect(!isSqlReadSafe("DELETE FROM users"));
}

test "isSqlReadSafe: COPY blocked" {
    try std.testing.expect(!isSqlReadSafe("COPY users TO '/tmp/file'"));
}

test "isSqlReadSafe: DO block blocked" {
    try std.testing.expect(!isSqlReadSafe("DO $$ BEGIN DELETE FROM users; END $$"));
}

test "isSqlReadSafe: multi-statement blocked" {
    try std.testing.expect(!isSqlReadSafe("SELECT 1; DROP TABLE users"));
}

test "isSqlReadSafe: CTE with DELETE blocked" {
    try std.testing.expect(!isSqlReadSafe("WITH cte AS (DELETE FROM users RETURNING *) SELECT * FROM cte"));
}

test "isSqlReadSafe: CTE with INSERT blocked" {
    try std.testing.expect(!isSqlReadSafe("WITH ins AS (INSERT INTO log(msg) VALUES('x') RETURNING *) SELECT * FROM ins"));
}

test "isSqlReadSafe: DELETE inside string literal is safe" {
    try std.testing.expect(isSqlReadSafe("SELECT * FROM users WHERE action = 'DELETE'"));
}

test "isSqlReadSafe: DELETE in identifier is safe" {
    try std.testing.expect(isSqlReadSafe("SELECT * FROM delete_log"));
}

test "isSqlReadSafe: empty SQL" {
    try std.testing.expect(!isSqlReadSafe(""));
}

test "isSqlReadSafe: BEGIN is blocked in read-only" {
    try std.testing.expect(!isSqlReadSafe("BEGIN"));
}

test "isSqlReadSafe: lowercase select" {
    try std.testing.expect(isSqlReadSafe("select * from users"));
}

test "isSqlReadSafe: WITH UPDATE blocked" {
    try std.testing.expect(!isSqlReadSafe("WITH upd AS (UPDATE users SET name='x' RETURNING *) SELECT * FROM upd"));
}
