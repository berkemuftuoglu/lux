const std = @import("std");

pub const IdentifierError = error{
    InvalidIdentifier,
    OutOfMemory,
};

pub const StringEscapeError = error{
    InvalidCharacter,
    OutOfMemory,
};

pub fn escapeIdentifier(allocator: std.mem.Allocator, name: []const u8) IdentifierError![]u8 {
    // Null bytes in identifiers/values are never valid in PostgreSQL
    for (name) |ch| {
        if (ch == 0) return error.InvalidIdentifier;
    }
    var extra: usize = 0;
    for (name) |ch| {
        if (ch == '"') extra += 1;
    }
    if (extra == 0) {
        return allocator.dupe(u8, name);
    }
    const buf = try allocator.alloc(u8, name.len + extra);
    var pos: usize = 0;
    for (name) |ch| {
        if (ch == '"') {
            buf[pos] = '"';
            pos += 1;
            buf[pos] = '"';
            pos += 1;
        } else {
            buf[pos] = ch;
            pos += 1;
        }
    }
    return buf;
}

pub fn escapeStringValue(allocator: std.mem.Allocator, value: []const u8) StringEscapeError![]u8 {
    // Null bytes in identifiers/values are never valid in PostgreSQL
    for (value) |ch| {
        if (ch == 0) return error.InvalidCharacter;
    }
    var extra: usize = 0;
    for (value) |ch| {
        if (ch == '\'' or ch == '\\') extra += 1;
    }
    const out_len = value.len + extra;
    const buf = try allocator.alloc(u8, out_len);
    var pos: usize = 0;
    for (value) |ch| {
        if (ch == '\'') {
            buf[pos] = '\'';
            pos += 1;
            buf[pos] = '\'';
            pos += 1;
        } else if (ch == '\\') {
            buf[pos] = '\\';
            pos += 1;
            buf[pos] = '\\';
            pos += 1;
        } else {
            buf[pos] = ch;
            pos += 1;
        }
    }
    return buf;
}

/// Write a SQL value to a JSON streaming writer. The "NULL" sentinel becomes JSON null.
pub fn writeSqlValue(jw: *std.json.Stringify, val: []const u8) std.json.Stringify.Error!void {
    if (std.mem.eql(u8, val, "NULL")) {
        try jw.write(null);
    } else {
        try jw.write(val);
    }
}

/// Write "columns" and "rows" fields from a query result into an already-open JSON object.
pub fn writeColumnsAndRows(jw: *std.json.Stringify, col_names: []const []const u8, rows: []const []const []const u8) std.json.Stringify.Error!void {
    try jw.objectField("columns");
    try jw.write(col_names);
    try jw.objectField("rows");
    try jw.beginArray();
    for (rows) |row| {
        try jw.beginArray();
        for (row) |val| {
            try writeSqlValue(jw, val);
        }
        try jw.endArray();
    }
    try jw.endArray();
}

/// Allocating JSON writer backed by std.io.Writer.Allocating.
/// Call init() then access jw/aw via pointer to avoid dangling
/// self-referential pointers from value-copy.
pub const JsonWriter = struct {
    aw: std.io.Writer.Allocating,
    jw: std.json.Stringify,

    pub fn init(allocator: std.mem.Allocator) JsonWriter {
        return .{
            .aw = .init(allocator),
            // SAFETY: jw is initialized via writer() before first use, which wires it to aw
            .jw = undefined,
        };
    }

    /// Wire the JSON serializer to the allocating writer.
    /// Must be called after the struct is at its final stack address
    /// (i.e. after `var jw = JsonWriter.init(...);`).
    pub fn writer(self: *JsonWriter) *std.json.Stringify {
        self.jw = .{ .writer = &self.aw.writer };
        return &self.jw;
    }

    pub fn toOwnedSlice(self: *JsonWriter) error{OutOfMemory}![]u8 {
        return self.aw.toOwnedSlice();
    }
};

pub fn getJsonString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const val = obj.get(key) orelse return null;
    return switch (val) {
        .string => |s| s,
        else => null,
    };
}

pub fn matchesIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (haystack.len < needle.len) return false;
    for (needle, 0..) |c, idx| {
        if (std.ascii.toLower(haystack[idx]) != std.ascii.toLower(c)) return false;
    }
    return true;
}

pub fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len > haystack.len) return null;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        var match = true;
        for (0..needle.len) |j| {
            if (std.ascii.toLower(haystack[i + j]) != std.ascii.toLower(needle[j])) {
                match = false;
                break;
            }
        }
        if (match) return i;
    }
    return null;
}

test "matchesIgnoreCase: prefix semantics, case-insensitive" {
    try std.testing.expect(matchesIgnoreCase("Select", "SELECT"));
    try std.testing.expect(matchesIgnoreCase("SELECT * FROM", "SELECT"));
    try std.testing.expect(!matchesIgnoreCase("SEL", "SELECT"));
    try std.testing.expect(!matchesIgnoreCase("Content-Type:", "content-length:"));
}

test "indexOfIgnoreCase: finds at any position" {
    try std.testing.expectEqual(@as(usize, 0), indexOfIgnoreCase("DROP TABLE t", "drop").?);
    try std.testing.expectEqual(@as(usize, 16), indexOfIgnoreCase("ALTER TABLE foo ADD COLUMN bar", "ADD").?);
    try std.testing.expect(indexOfIgnoreCase("SELECT * FROM t", "DROP") == null);
}

test "escapeStringValue: doubles quotes and backslashes, rejects null" {
    const a = std.testing.allocator;
    const r1 = try escapeStringValue(a, "it's a\\test");
    defer a.free(r1);
    try std.testing.expectEqualStrings("it''s a\\\\test", r1);
    try std.testing.expectError(error.InvalidCharacter, escapeStringValue(a, "x\x00y"));
}

test "escapeIdentifier: doubles quotes, rejects null" {
    const a = std.testing.allocator;
    const r1 = try escapeIdentifier(a, "my\"table");
    defer a.free(r1);
    try std.testing.expectEqualStrings("my\"\"table", r1);
    try std.testing.expectError(error.InvalidIdentifier, escapeIdentifier(a, "t\x00;DROP"));
}

fn testEscapeIdentifierAlloc(allocator: std.mem.Allocator) !void {
    const result = try escapeIdentifier(allocator, "test_table");
    defer allocator.free(result);
}

test "escapeIdentifier: allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testEscapeIdentifierAlloc, .{});
}

fn testEscapeStringValueAlloc(allocator: std.mem.Allocator) !void {
    const result = try escapeStringValue(allocator, "test's value");
    defer allocator.free(result);
}

test "escapeStringValue: allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testEscapeStringValueAlloc, .{});
}

fn fuzzEscapeIdentifier(_: void, input: []const u8) anyerror!void {
    const result = escapeIdentifier(std.testing.allocator, input) catch return;
    defer std.testing.allocator.free(result);
    try std.testing.expect(result.len >= input.len);
}

test "escapeIdentifier: fuzz" {
    try std.testing.fuzz({}, fuzzEscapeIdentifier, .{});
}

fn fuzzEscapeStringValue(_: void, input: []const u8) anyerror!void {
    const result = escapeStringValue(std.testing.allocator, input) catch return;
    defer std.testing.allocator.free(result);
    try std.testing.expect(result.len >= input.len);
}

test "escapeStringValue: fuzz" {
    try std.testing.fuzz({}, fuzzEscapeStringValue, .{});
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
