// sql_guard: Application-level SQL safety analysis for read-only mode.
//
// This is a convenience guard, not a security boundary. It prevents accidental
// writes (fat-finger DELETE without WHERE, etc.) when read-only mode is toggled.
// It is NOT a substitute for PostgreSQL roles — a determined user can always
// connect directly. For real enforcement, use a read-only DB role.
//
// The guard uses whitelist prefix matching + write-keyword scanning outside
// string literals, comments, and dollar-quoted blocks. CTE write attacks like
// `WITH x AS (DELETE ...) SELECT * FROM x` are caught by containsWriteKeyword.

const std = @import("std");
const utils = @import("utils");

pub const SqlGuardResult = struct {
    is_destructive: bool,
    operation: []const u8,
    warning: []const u8,
};

pub fn analyzeSql(sql: []const u8) SqlGuardResult {
    var i: usize = 0;
    while (i < sql.len and (sql[i] == ' ' or sql[i] == '\t' or sql[i] == '\n' or sql[i] == '\r')) i += 1;
    if (i >= sql.len) return .{ .is_destructive = false, .operation = "", .warning = "" };
    const rest = sql[i..];

    if (rest.len >= 4 and utils.matchesIgnoreCase(rest, "DROP")) {
        return .{ .is_destructive = true, .operation = "DROP", .warning = "This will permanently drop the object. This cannot be undone." };
    }
    if (rest.len >= 8 and utils.matchesIgnoreCase(rest, "TRUNCATE")) {
        return .{ .is_destructive = true, .operation = "TRUNCATE", .warning = "This will delete ALL rows from the table. This cannot be undone." };
    }
    if (rest.len >= 5 and utils.matchesIgnoreCase(rest, "ALTER")) {
        return .{ .is_destructive = true, .operation = "ALTER", .warning = "This will modify the table schema. Review carefully." };
    }
    if (rest.len >= 6 and utils.matchesIgnoreCase(rest, "DELETE")) {
        if (!containsIgnoreCaseWord(sql, "WHERE")) {
            return .{ .is_destructive = true, .operation = "DELETE", .warning = "DELETE without WHERE clause will delete ALL rows." };
        }
    }
    if (rest.len >= 6 and utils.matchesIgnoreCase(rest, "UPDATE")) {
        if (!containsIgnoreCaseWord(sql, "WHERE")) {
            return .{ .is_destructive = true, .operation = "UPDATE", .warning = "UPDATE without WHERE clause will update ALL rows." };
        }
    }
    return .{ .is_destructive = false, .operation = "", .warning = "" };
}

pub fn hasMultipleStatements(sql: []const u8) bool {
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];

        if (ch == '\'') {
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    i += 2;
                } else if (sql[i] == '\'') {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            continue;
        }

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

        if (ch == '$') {
            const tag_start = i;
            var ti = i + 1;
            while (ti < sql.len and ((sql[ti] >= 'a' and sql[ti] <= 'z') or (sql[ti] >= 'A' and sql[ti] <= 'Z') or (sql[ti] >= '0' and sql[ti] <= '9') or sql[ti] == '_')) ti += 1;
            if (ti < sql.len and sql[ti] == '$') {
                const tag = sql[tag_start .. ti + 1];
                i = ti + 1;
                while (i + tag.len <= sql.len) {
                    if (std.mem.eql(u8, sql[i .. i + tag.len], tag)) {
                        i += tag.len;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            i += 1;
            continue;
        }

        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            i += 2;
            while (i < sql.len and sql[i] != '\n') : (i += 1) {}
            continue;
        }

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

        if (ch == ';') {
            var j = i + 1;
            while (j < sql.len and (sql[j] == ' ' or sql[j] == '\t' or sql[j] == '\n' or sql[j] == '\r')) : (j += 1) {}
            if (j < sql.len) return true;
        }

        i += 1;
    }
    return false;
}

/// Whitelist approach: only SELECT, SHOW, EXPLAIN, and safe WITH...SELECT pass.
/// Catches CTE write attacks like `WITH x AS (DELETE ...) SELECT * FROM x`.
pub fn isSqlReadSafe(sql: []const u8) bool {
    if (hasMultipleStatements(sql)) return false;

    var i: usize = 0;
    while (i < sql.len and (sql[i] == ' ' or sql[i] == '\t' or sql[i] == '\n' or sql[i] == '\r')) i += 1;
    if (i >= sql.len) return false;
    const rest = sql[i..];

    if (rest.len >= 6 and utils.matchesIgnoreCase(rest[0..6], "SELECT")) return true;
    if (rest.len >= 4 and utils.matchesIgnoreCase(rest[0..4], "SHOW")) return true;
    // EXPLAIN ANALYZE actually executes the inner query, so check it for writes
    if (rest.len >= 7 and utils.matchesIgnoreCase(rest[0..7], "EXPLAIN")) {
        var ei: usize = 7;
        while (i + ei < sql.len and (sql[i + ei] == ' ' or sql[i + ei] == '\t' or sql[i + ei] == '\n' or sql[i + ei] == '\r')) ei += 1;
        if (rest.len >= ei + 7 and utils.matchesIgnoreCase(rest[ei .. ei + 7], "ANALYZE")) {
            return !containsWriteKeyword(sql);
        }
        return true;
    }

    if (rest.len >= 4 and utils.matchesIgnoreCase(rest[0..4], "WITH")) {
        return !containsWriteKeyword(sql);
    }

    return false;
}

/// Word-boundary aware: "delete_log" and 'DELETE' inside strings don't trigger.
pub fn containsWriteKeyword(sql: []const u8) bool {
    const keywords = [_][]const u8{ "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "CREATE", "COPY", "GRANT", "REVOKE" };
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'') {
            i += 1;
            while (i < sql.len) {
                if (sql[i] == '\'' and i + 1 < sql.len and sql[i + 1] == '\'') {
                    i += 2;
                } else if (sql[i] == '\'') {
                    break;
                } else {
                    i += 1;
                }
            }
            if (i < sql.len) i += 1;
            continue;
        }
        if (ch == '"') {
            i += 1;
            while (i < sql.len and sql[i] != '"') i += 1;
            if (i < sql.len) i += 1;
            continue;
        }
        if (ch == '$') {
            const tag_start = i;
            i += 1;
            while (i < sql.len and ((sql[i] >= 'a' and sql[i] <= 'z') or (sql[i] >= 'A' and sql[i] <= 'Z') or (sql[i] >= '0' and sql[i] <= '9') or sql[i] == '_')) i += 1;
            if (i < sql.len and sql[i] == '$') {
                const tag = sql[tag_start .. i + 1];
                i += 1;
                while (i + tag.len <= sql.len) {
                    if (std.mem.eql(u8, sql[i .. i + tag.len], tag)) {
                        i += tag.len;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            continue;
        }
        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') i += 1;
            continue;
        }
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
        const at_word_start = (i == 0) or !isAlphanumUnderscore(sql[i - 1]);
        if (at_word_start) {
            for (keywords) |kw| {
                if (i + kw.len <= sql.len and utils.matchesIgnoreCase(sql[i .. i + kw.len], kw)) {
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

pub fn containsIgnoreCaseWord(haystack: []const u8, needle: []const u8) bool {
    if (needle.len > haystack.len) return false;
    const limit = haystack.len - needle.len + 1;
    for (0..limit) |idx| {
        if (utils.matchesIgnoreCase(haystack[idx..], needle)) {
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

test "analyzeSql: detects destructive operations" {
    // DROP, TRUNCATE, ALTER are always destructive
    try std.testing.expect(analyzeSql("DROP TABLE users").is_destructive);
    try std.testing.expect(analyzeSql("TRUNCATE users").is_destructive);
    try std.testing.expect(analyzeSql("ALTER TABLE users ADD COLUMN age int").is_destructive);
    try std.testing.expectEqualStrings("DROP", analyzeSql("dRoP TABLE users").operation);
    // DELETE/UPDATE without WHERE are destructive, with WHERE are safe
    try std.testing.expect(analyzeSql("DELETE FROM users").is_destructive);
    try std.testing.expect(!analyzeSql("DELETE FROM users WHERE id = 1").is_destructive);
    try std.testing.expect(analyzeSql("UPDATE users SET name = 'x'").is_destructive);
    try std.testing.expect(!analyzeSql("update users set name='x' WhErE id=1").is_destructive);
    // "NOWHERE" doesn't count as WHERE (word boundary)
    try std.testing.expect(analyzeSql("DELETE FROM t NOWHERE").is_destructive);
    // SELECT, CREATE, GRANT, empty, whitespace are not destructive
    try std.testing.expect(!analyzeSql("SELECT * FROM users").is_destructive);
    try std.testing.expect(!analyzeSql("CREATE TABLE t (id int)").is_destructive);
    try std.testing.expect(!analyzeSql("").is_destructive);
    try std.testing.expect(!analyzeSql("  \t\n  ").is_destructive);
    // Leading whitespace doesn't hide destructive ops
    try std.testing.expect(analyzeSql("\t\n  DELETE FROM users").is_destructive);
}

test "containsIgnoreCaseWord: word boundary matching" {
    try std.testing.expect(containsIgnoreCaseWord("WHERE id = 1", "WHERE"));
    try std.testing.expect(containsIgnoreCaseWord("SELECT * WHERE", "WHERE"));
    try std.testing.expect(containsIgnoreCaseWord("select * where id = 1", "WHERE"));
    // Partial match within a word must reject
    try std.testing.expect(!containsIgnoreCaseWord("SOMEWHERE", "WHERE"));
    try std.testing.expect(!containsIgnoreCaseWord("DO_WHERE", "WHERE"));
}

test "hasMultipleStatements: splits on semicolons outside strings and comments" {
    // Single statements (including trailing semicolon)
    try std.testing.expect(!hasMultipleStatements("SELECT * FROM users"));
    try std.testing.expect(!hasMultipleStatements("SELECT * FROM users;"));
    try std.testing.expect(!hasMultipleStatements("SELECT * FROM users;  \n  "));
    // Multiple statements
    try std.testing.expect(hasMultipleStatements("INSERT INTO t VALUES (1); SELECT * FROM t"));
    // Semicolons inside strings/comments/dollar-quotes must not split
    try std.testing.expect(!hasMultipleStatements("SELECT 'hello; world' FROM t"));
    try std.testing.expect(!hasMultipleStatements("SELECT \"col;name\" FROM t"));
    try std.testing.expect(!hasMultipleStatements("SELECT * -- ; comment\nFROM t"));
    try std.testing.expect(!hasMultipleStatements("SELECT * /* ; */ FROM t"));
    try std.testing.expect(!hasMultipleStatements("SELECT $$ hello; world $$ FROM t"));
    try std.testing.expect(!hasMultipleStatements("SELECT $fn$hello;world$fn$"));
    // Real multi after dollar-quoted string
    try std.testing.expect(hasMultipleStatements("SELECT $fn$hello$fn$; DROP TABLE x"));
    // Real multi with semicolon inside string value
    try std.testing.expect(hasMultipleStatements("INSERT INTO t VALUES ('a;b'); SELECT 1"));
}

test "isSqlReadSafe: whitelist-based read safety" {
    // Safe: SELECT, SHOW, EXPLAIN, WITH...SELECT
    try std.testing.expect(isSqlReadSafe("SELECT * FROM users"));
    try std.testing.expect(isSqlReadSafe("select * from users"));
    try std.testing.expect(isSqlReadSafe("SHOW search_path"));
    try std.testing.expect(isSqlReadSafe("EXPLAIN SELECT 1"));
    try std.testing.expect(isSqlReadSafe("EXPLAIN ANALYZE SELECT 1"));
    try std.testing.expect(isSqlReadSafe("WITH cte AS (SELECT 1) SELECT * FROM cte"));
    // Blocked: write operations, BEGIN, COPY, DO
    try std.testing.expect(!isSqlReadSafe("INSERT INTO users VALUES (1)"));
    try std.testing.expect(!isSqlReadSafe("DELETE FROM users"));
    try std.testing.expect(!isSqlReadSafe("COPY users TO '/tmp/file'"));
    try std.testing.expect(!isSqlReadSafe("DO $$ BEGIN DELETE FROM users; END $$"));
    try std.testing.expect(!isSqlReadSafe("BEGIN"));
    try std.testing.expect(!isSqlReadSafe(""));
    // EXPLAIN ANALYZE with write is blocked
    try std.testing.expect(!isSqlReadSafe("EXPLAIN ANALYZE DELETE FROM users"));
    // Multi-statement injection blocked
    try std.testing.expect(!isSqlReadSafe("SELECT 1; DROP TABLE users"));
    // CTE write attacks blocked
    try std.testing.expect(!isSqlReadSafe("WITH cte AS (DELETE FROM users RETURNING *) SELECT * FROM cte"));
    // Write keywords inside strings/identifiers don't trigger
    try std.testing.expect(isSqlReadSafe("SELECT * FROM users WHERE action = 'DELETE'"));
    try std.testing.expect(isSqlReadSafe("SELECT * FROM delete_log"));
}

fn fuzzHasMultipleStatements(_: void, input: []const u8) anyerror!void {
    _ = hasMultipleStatements(input);
}

test "hasMultipleStatements: fuzz" {
    try std.testing.fuzz({}, fuzzHasMultipleStatements, .{});
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
