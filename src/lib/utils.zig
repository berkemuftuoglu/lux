const std = @import("std");

pub const IdentifierError = error{
    InvalidIdentifier,
    OutOfMemory,
};

pub const StringEscapeError = error{
    InvalidCharacter,
    OutOfMemory,
};

pub fn sendResponse(stream: std.net.Stream, status: []const u8, content_type: []const u8, body: []const u8) !void {
    var header_buf: [640]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n\r\n", .{
        status,
        content_type,
        body.len,
    }) catch return;

    stream.writeAll(header) catch return;
    stream.writeAll(body) catch return;
}

pub fn sendHtmlResponseWithCsp(stream: std.net.Stream, body: []const u8) !void {
    const csp = "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'";
    var header_buf: [896]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {d}\r\nConnection: close\r\nCache-Control: no-store\r\nContent-Security-Policy: {s}\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n\r\n", .{
        body.len,
        csp,
    }) catch return;

    stream.writeAll(header) catch return;
    stream.writeAll(body) catch return;
}

pub fn sendResponseWithDownload(
    stream: std.net.Stream,
    status: []const u8,
    content_type: []const u8,
    filename: []const u8,
    body: []const u8,
) !void {
    var header_buf: [1152]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nContent-Disposition: attachment; filename=\"{s}\"\r\nConnection: close\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nReferrer-Policy: no-referrer\r\n\r\n", .{
        status,
        content_type,
        body.len,
        filename,
    }) catch return;

    stream.writeAll(header) catch return;
    stream.writeAll(body) catch return;
}

pub fn findContentLength(request: []const u8) ?usize {
    var i: usize = 0;
    while (i + 16 < request.len) : (i += 1) {
        if (matchesIgnoreCase(request[i..], "content-length:")) {
            var pos = i + 15; // skip "content-length:"
            // Skip whitespace
            while (pos < request.len and request[pos] == ' ') pos += 1;
            // Parse number
            var end = pos;
            while (end < request.len and request[end] >= '0' and request[end] <= '9') end += 1;
            if (end > pos) {
                return std.fmt.parseInt(usize, request[pos..end], 10) catch null;
            }
        }
    }
    return null;
}

pub fn findHeader(request: []const u8, header_name: []const u8) ?[]const u8 {
    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse return null;
    const headers = request[0..header_end];
    var line_iter = std.mem.splitSequence(u8, headers, "\r\n");
    _ = line_iter.next(); // skip request line
    while (line_iter.next()) |line| {
        const colon = std.mem.indexOf(u8, line, ":") orelse continue;
        const name = line[0..colon];
        if (name.len != header_name.len) continue;
        if (eqlLower(name, header_name)) {
            const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
            return value;
        }
    }
    return null;
}

pub fn eqlLower(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        const la = if (ca >= 'A' and ca <= 'Z') ca + 32 else ca;
        const lb = if (cb >= 'A' and cb <= 'Z') cb + 32 else cb;
        if (la != lb) return false;
    }
    return true;
}

/// Absent Origin is allowed because same-origin requests may omit it.
pub fn checkOrigin(request: []const u8, port: u16) bool {
    const origin = findHeader(request, "Origin") orelse return true;
    var buf_127: [64]u8 = undefined;
    var buf_local: [64]u8 = undefined;
    const expected_127 = std.fmt.bufPrint(&buf_127, "http://127.0.0.1:{d}", .{port}) catch return false;
    const expected_local = std.fmt.bufPrint(&buf_local, "http://localhost:{d}", .{port}) catch return false;
    if (std.mem.eql(u8, origin, expected_127)) return true;
    if (std.mem.eql(u8, origin, expected_local)) return true;
    return false;
}

pub fn escapeIdentifier(allocator: std.mem.Allocator, name: []const u8) IdentifierError![]u8 {
    // Null bytes would cause C string truncation in libpq
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
    // Null bytes would cause C string truncation in libpq
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

pub fn writeJsonEscaped(writer: anytype, s: []const u8) !void {
    const hex_digits = "0123456789abcdef";
    for (s) |ch| {
        switch (ch) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            0x00...0x08, 0x0b, 0x0c, 0x0e...0x1f => {
                try writer.writeAll("\\u00");
                try writer.writeByte(hex_digits[ch >> 4]);
                try writer.writeByte(hex_digits[ch & 0x0f]);
            },
            else => try writer.writeByte(ch),
        }
    }
}

pub fn matchesIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (haystack.len < needle.len) return false;
    for (needle, 0..) |c, idx| {
        const h = haystack[idx];
        const lower_h = if (h >= 'A' and h <= 'Z') h + 32 else h;
        const lower_c = if (c >= 'A' and c <= 'Z') c + 32 else c;
        if (lower_h != lower_c) return false;
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

pub fn parseQueryParam(query_string: []const u8, name: []const u8, out: *usize) void {
    var iter = std.mem.splitScalar(u8, query_string, '&');
    while (iter.next()) |param| {
        if (std.mem.startsWith(u8, param, name) and param.len > name.len and param[name.len] == '=') {
            const val_str = param[name.len + 1 ..];
            out.* = std.fmt.parseInt(usize, val_str, 10) catch return;
            return;
        }
    }
}

pub fn parseStringQueryParam(query_string: []const u8, name: []const u8, buf: []u8) ?[]const u8 {
    var iter = std.mem.splitScalar(u8, query_string, '&');
    while (iter.next()) |param| {
        if (std.mem.startsWith(u8, param, name) and param.len > name.len and param[name.len] == '=') {
            const val_str = param[name.len + 1 ..];
            if (val_str.len == 0 or val_str.len > buf.len) return null;
            return urlDecode(buf, val_str);
        }
    }
    return null;
}

pub fn urlDecode(buf: []u8, input: []const u8) ?[]const u8 {
    var out: usize = 0;
    var i: usize = 0;
    while (i < input.len) {
        if (input[i] == '%' and i + 2 < input.len) {
            const hi = hexVal(input[i + 1]) orelse {
                if (out >= buf.len) return null;
                buf[out] = input[i];
                out += 1;
                i += 1;
                continue;
            };
            const lo = hexVal(input[i + 2]) orelse {
                if (out >= buf.len) return null;
                buf[out] = input[i];
                out += 1;
                i += 1;
                continue;
            };
            if (out >= buf.len) return null;
            buf[out] = (@as(u8, hi) << 4) | @as(u8, lo);
            out += 1;
            i += 3;
        } else if (input[i] == '+') {
            if (out >= buf.len) return null;
            buf[out] = ' ';
            out += 1;
            i += 1;
        } else {
            if (out >= buf.len) return null;
            buf[out] = input[i];
            out += 1;
            i += 1;
        }
    }
    return buf[0..out];
}

fn hexVal(ch: u8) ?u4 {
    if (ch >= '0' and ch <= '9') return @intCast(ch - '0');
    if (ch >= 'a' and ch <= 'f') return @intCast(ch - 'a' + 10);
    if (ch >= 'A' and ch <= 'F') return @intCast(ch - 'A' + 10);
    return null;
}

// Minimal JSON string field extractor — avoids pulling in a full JSON parser.
pub fn extractJsonField(body: []const u8, field_name: []const u8) ?[]const u8 {
    var search_pos: usize = 0;
    while (search_pos < body.len) {
        const quote_pos = std.mem.indexOfScalarPos(u8, body, search_pos, '"') orelse return null;
        if (quote_pos + 1 + field_name.len + 1 > body.len) return null;
        const after_quote = body[quote_pos + 1 ..];
        if (after_quote.len >= field_name.len + 1 and
            std.mem.eql(u8, after_quote[0..field_name.len], field_name) and
            after_quote[field_name.len] == '"')
        {
            var pos = quote_pos + 1 + field_name.len + 1;
            while (pos < body.len and (body[pos] == ' ' or body[pos] == ':')) pos += 1;
            if (pos >= body.len or body[pos] != '"') return null;
            pos += 1;
            const start = pos;
            while (pos < body.len) {
                if (body[pos] == '\\' and pos + 1 < body.len) {
                    pos += 2;
                    continue;
                }
                if (body[pos] == '"') {
                    return body[start..pos];
                }
                pos += 1;
            }
            return null;
        }
        search_pos = quote_pos + 1;
    }
    return null;
}

pub fn extractJsonQuery(body: []const u8) ?[]const u8 {
    return extractJsonField(body, "query");
}

pub fn readRequestBody(
    stream: std.net.Stream,
    request: []const u8,
    extra_buf: []u8,
    max_content_length: usize,
) ?[]const u8 {
    const content_length = findContentLength(request) orelse return null;
    if (content_length > max_content_length) return null;

    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse return null;
    const body_start = header_end + 4;
    var body = request[body_start..];

    if (body.len < content_length) {
        if (content_length > extra_buf.len) return null;
        const already_have = body.len;
        @memcpy(extra_buf[0..already_have], body);
        var read_so_far = already_have;
        while (read_so_far < content_length) {
            const n = stream.read(extra_buf[read_so_far..content_length]) catch return null;
            if (n == 0) break;
            read_so_far += n;
        }
        return extra_buf[0..read_so_far];
    } else {
        return body[0..content_length];
    }
}

test "extractJsonQuery: simple query" {
    const body = "{\"query\": \"Sum all values\"}";
    const result = extractJsonQuery(body);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("Sum all values", result.?);
}

test "extractJsonQuery: no whitespace" {
    const body = "{\"query\":\"test\"}";
    const result = extractJsonQuery(body);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("test", result.?);
}

test "extractJsonQuery: missing field" {
    const body = "{\"other\": \"value\"}";
    try std.testing.expect(extractJsonQuery(body) == null);
}

test "extractJsonQuery: empty body" {
    try std.testing.expect(extractJsonQuery("") == null);
}

test "findContentLength: standard header" {
    const req = "POST /api HTTP/1.1\r\nContent-Length: 42\r\n\r\n";
    try std.testing.expectEqual(@as(?usize, 42), findContentLength(req));
}

test "findContentLength: case insensitive" {
    const req = "POST /api HTTP/1.1\r\ncontent-length: 100\r\n\r\n";
    try std.testing.expectEqual(@as(?usize, 100), findContentLength(req));
}

test "findContentLength: missing header" {
    const req = "GET / HTTP/1.1\r\n\r\n";
    try std.testing.expect(findContentLength(req) == null);
}

test "matchesIgnoreCase: matches case-insensitively with prefix semantics" {
    // Exact matches at different cases
    try std.testing.expect(matchesIgnoreCase("SELECT", "SELECT"));
    try std.testing.expect(matchesIgnoreCase("Select", "SELECT"));
    try std.testing.expect(matchesIgnoreCase("sElEcT", "SELECT"));
    // Prefix match (haystack longer than needle)
    try std.testing.expect(matchesIgnoreCase("SELECT * FROM", "SELECT"));
    // Non-alpha chars match exactly
    try std.testing.expect(matchesIgnoreCase("content-length:", "Content-Length:"));
    try std.testing.expect(matchesIgnoreCase("abc123", "ABC123"));
}

test "matchesIgnoreCase: rejects non-matches" {
    try std.testing.expect(!matchesIgnoreCase("Content-Type:", "content-length:"));
    // Haystack shorter than needle
    try std.testing.expect(!matchesIgnoreCase("ab", "abcdef"));
    try std.testing.expect(!matchesIgnoreCase("SEL", "SELECT"));
}

test "matchesIgnoreCase: handles empty needle" {
    try std.testing.expect(matchesIgnoreCase("anything", ""));
}

test "writeJsonEscaped: plain text passes through" {
    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "hello world 123");
    try std.testing.expectEqualStrings("hello world 123", fbs.getWritten());
}

test "writeJsonEscaped: empty string" {
    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "");
    try std.testing.expectEqualStrings("", fbs.getWritten());
}

test "writeJsonEscaped: escapes double quotes" {
    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "say \"hello\"");
    try std.testing.expectEqualStrings("say \\\"hello\\\"", fbs.getWritten());
}

test "writeJsonEscaped: escapes backslashes" {
    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "path\\to\\file");
    try std.testing.expectEqualStrings("path\\\\to\\\\file", fbs.getWritten());
}

test "writeJsonEscaped: escapes newlines and tabs" {
    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "line1\nline2\ttab");
    try std.testing.expectEqualStrings("line1\\nline2\\ttab", fbs.getWritten());
}

test "writeJsonEscaped: escapes carriage return" {
    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "a\rb");
    try std.testing.expectEqualStrings("a\\rb", fbs.getWritten());
}

test "writeJsonEscaped: mixed special chars" {
    var buf: [128]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "He said \"hi\\there\"\n");
    try std.testing.expectEqualStrings("He said \\\"hi\\\\there\\\"\\n", fbs.getWritten());
}

test "extractJsonQuery: query with escaped quotes inside" {
    const body = "{\"query\": \"say \\\"hello\\\"\"}";
    const result = extractJsonQuery(body);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("say \\\"hello\\\"", result.?);
}

test "extractJsonQuery: multiple keys finds query" {
    const body = "{\"other\": 42, \"query\": \"test query\", \"extra\": true}";
    const result = extractJsonQuery(body);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("test query", result.?);
}

test "extractJsonQuery: whitespace variations" {
    const body = "{  \"query\"  :  \"spaced out\"  }";
    const result = extractJsonQuery(body);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("spaced out", result.?);
}

test "extractJsonQuery: malformed no closing quote" {
    const body = "{\"query\": \"unterminated}";
    try std.testing.expect(extractJsonQuery(body) == null);
}

test "extractJsonField: extracts non-query fields" {
    const body = "{\"conninfo\": \"postgresql://localhost/db\", \"table\": \"orders\"}";
    const conninfo = extractJsonField(body, "conninfo");
    try std.testing.expect(conninfo != null);
    try std.testing.expectEqualStrings("postgresql://localhost/db", conninfo.?);
    const table = extractJsonField(body, "table");
    try std.testing.expect(table != null);
    try std.testing.expectEqualStrings("orders", table.?);
}

test "extractJsonField: returns null for missing field" {
    const body = "{\"query\": \"test\"}";
    try std.testing.expect(extractJsonField(body, "missing") == null);
}

test "extractJsonField: empty body" {
    try std.testing.expect(extractJsonField("", "query") == null);
}

test "eqlLower: case insensitive match" {
    try std.testing.expect(eqlLower("Hello", "hello"));
    try std.testing.expect(eqlLower("HELLO", "hello"));
    try std.testing.expect(eqlLower("hello", "hello"));
}

test "eqlLower: mismatch" {
    try std.testing.expect(!eqlLower("hello", "world"));
    try std.testing.expect(!eqlLower("hi", "hello"));
}

test "eqlLower: empty strings match" {
    try std.testing.expect(eqlLower("", ""));
}

test "eqlLower: different lengths" {
    try std.testing.expect(!eqlLower("", "hello"));
    try std.testing.expect(!eqlLower("hello", ""));
}

test "findContentLength: with extra headers" {
    const req = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 25\r\n\r\n";
    try std.testing.expectEqual(@as(?usize, 25), findContentLength(req));
}

test "findContentLength: zero length" {
    const req = "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n";
    try std.testing.expectEqual(@as(?usize, 0), findContentLength(req));
}

test "parseQueryParam: parses limit" {
    var limit: usize = 50;
    parseQueryParam("limit=100&offset=0", "limit", &limit);
    try std.testing.expectEqual(@as(usize, 100), limit);
}

test "parseQueryParam: parses offset" {
    var offset: usize = 0;
    parseQueryParam("limit=50&offset=25", "offset", &offset);
    try std.testing.expectEqual(@as(usize, 25), offset);
}

test "parseQueryParam: missing param keeps default" {
    var limit: usize = 50;
    parseQueryParam("offset=10", "limit", &limit);
    try std.testing.expectEqual(@as(usize, 50), limit);
}

test "parseQueryParam: empty query string" {
    var limit: usize = 50;
    parseQueryParam("", "limit", &limit);
    try std.testing.expectEqual(@as(usize, 50), limit);
}

test "parseQueryParam: single param" {
    var limit: usize = 50;
    parseQueryParam("limit=200", "limit", &limit);
    try std.testing.expectEqual(@as(usize, 200), limit);
}

test "parseStringQueryParam: parses sort column" {
    var buf: [128]u8 = undefined;
    const result = parseStringQueryParam("sort=name&dir=asc", "sort", &buf);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("name", result.?);
}

test "parseStringQueryParam: parses dir param" {
    var buf: [8]u8 = undefined;
    const result = parseStringQueryParam("sort=name&dir=desc", "dir", &buf);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("desc", result.?);
}

test "parseStringQueryParam: missing param returns null" {
    var buf: [128]u8 = undefined;
    const result = parseStringQueryParam("limit=50&offset=0", "sort", &buf);
    try std.testing.expect(result == null);
}

test "parseStringQueryParam: empty query string returns null" {
    var buf: [128]u8 = undefined;
    const result = parseStringQueryParam("", "sort", &buf);
    try std.testing.expect(result == null);
}

test "parseStringQueryParam: empty value returns null" {
    var buf: [128]u8 = undefined;
    const result = parseStringQueryParam("sort=", "sort", &buf);
    try std.testing.expect(result == null);
}

test "parseStringQueryParam: single param" {
    var buf: [128]u8 = undefined;
    const result = parseStringQueryParam("sort=email", "sort", &buf);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("email", result.?);
}

test "escapeStringValue: no quotes passes through" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "hello world");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("hello world", result);
}

test "escapeStringValue: empty string" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("", result);
}

test "escapeStringValue: single quotes are doubled" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "it's a test");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("it''s a test", result);
}

test "escapeStringValue: multiple single quotes" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "O'Brien's 'data'");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("O''Brien''s ''data''", result);
}

test "escapeStringValue: backslashes are doubled" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "path\\to\\file");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("path\\\\to\\\\file", result);
}

test "escapeStringValue: only single quotes" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "'''");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("''''''", result);
}

test "escapeIdentifier: no quotes passes through" {
    const allocator = std.testing.allocator;
    const result = try escapeIdentifier(allocator, "my_table");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("my_table", result);
}

test "escapeIdentifier: empty string" {
    const allocator = std.testing.allocator;
    const result = try escapeIdentifier(allocator, "");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("", result);
}

test "escapeIdentifier: double quotes are doubled" {
    const allocator = std.testing.allocator;
    const result = try escapeIdentifier(allocator, "my\"table");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("my\"\"table", result);
}

test "escapeIdentifier: multiple double quotes" {
    const allocator = std.testing.allocator;
    const result = try escapeIdentifier(allocator, "a\"b\"c");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("a\"\"b\"\"c", result);
}

test "escapeIdentifier: only double quotes" {
    const allocator = std.testing.allocator;
    const result = try escapeIdentifier(allocator, "\"\"");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("\"\"\"\"", result);
}

test "eqlLower: mixed case both sides" {
    try std.testing.expect(eqlLower("HeLLo", "hello"));
    try std.testing.expect(eqlLower("hello", "HELLO"));
}

test "eqlLower: numbers and special chars" {
    try std.testing.expect(eqlLower("abc123", "abc123"));
    try std.testing.expect(eqlLower("ABC123", "abc123"));
}

test "eqlLower: single char" {
    try std.testing.expect(eqlLower("A", "a"));
    try std.testing.expect(!eqlLower("A", "b"));
}

test "parseQueryParam: param with special chars in value" {
    var result: usize = 0;
    parseQueryParam("limit=abc&offset=0", "limit", &result);
    try std.testing.expectEqual(@as(usize, 0), result);
}

test "parseQueryParam: multiple same params uses first" {
    var result: usize = 0;
    parseQueryParam("limit=10&limit=20", "limit", &result);
    try std.testing.expectEqual(@as(usize, 10), result);
}

test "extractJsonField: connection string with special chars" {
    const body = "{\"conninfo\":\"postgresql://user:p@ss@localhost:5432/mydb?sslmode=require\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("postgresql://user:p@ss@localhost:5432/mydb?sslmode=require", result.?);
}

test "extractJsonField: connection string with escaped quotes" {
    const body = "{\"conninfo\":\"host=localhost dbname=\\\"my db\\\"\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("host=localhost dbname=\\\"my db\\\"", result.?);
}

test "extractJsonField: multiple fields extracts correct one" {
    const body = "{\"env\":\"dev\",\"conninfo\":\"postgresql://localhost/db\",\"ssl\":\"require\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("postgresql://localhost/db", result.?);
}

test "extractJsonField: field with unicode" {
    const body = "{\"conninfo\":\"postgresql://user@localhost/caf\\u00e9\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
}

test "extractJsonField: empty value" {
    const body = "{\"conninfo\":\"\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("", result.?);
}

test "extractJsonField: value with colons and slashes" {
    const body = "{\"conninfo\":\"postgresql://admin:s3cr3t@db.example.com:5432/production\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("postgresql://admin:s3cr3t@db.example.com:5432/production", result.?);
}

test "extractJsonField: value with newlines escaped" {
    const body = "{\"sql\":\"SELECT\\n* FROM\\nusers\"}";
    const result = extractJsonField(body, "sql");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("SELECT\\n* FROM\\nusers", result.?);
}

test "extractJsonField: whitespace around colon" {
    const body = "{\"conninfo\" : \"localhost\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("localhost", result.?);
}

test "extractJsonField: null body" {
    const result = extractJsonField("", "conninfo");
    try std.testing.expect(result == null);
}

test "extractJsonField: body with only braces" {
    const result = extractJsonField("{}", "conninfo");
    try std.testing.expect(result == null);
}

test "extractJsonField: partial field name match should not match" {
    const body = "{\"conninfo_extra\":\"wrong\",\"conninfo\":\"right\"}";
    const result = extractJsonField(body, "conninfo");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("right", result.?);
}

test "extractJsonField: field with number value returns null" {
    const body = "{\"count\":42}";
    const result = extractJsonField(body, "count");
    try std.testing.expect(result == null);
}

test "escapeStringValue: long string" {
    const allocator = std.testing.allocator;
    const input = "It's a 'test' with 'many' quotes";
    const result = try escapeStringValue(allocator, input);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("It''s a ''test'' with ''many'' quotes", result);
}

test "escapeStringValue: no special chars" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "simple text 123");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("simple text 123", result);
}

test "writeJsonEscaped: control characters" {
    var buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "line1\nline2\ttab");
    try std.testing.expectEqualStrings("line1\\nline2\\ttab", fbs.getWritten());
}

test "writeJsonEscaped: all special chars together" {
    var buf: [128]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try writeJsonEscaped(fbs.writer(), "quote:\" backslash:\\ newline:\n tab:\t cr:\r");
    const expected = "quote:\\\" backslash:\\\\ newline:\\n tab:\\t cr:\\r";
    try std.testing.expectEqualStrings(expected, fbs.getWritten());
}

test "parseQueryParam: negative number does not parse" {
    var out: usize = 42;
    parseQueryParam("limit=-5", "limit", &out);
    try std.testing.expectEqual(@as(usize, 42), out);
}

test "parseQueryParam: overflow value does not parse" {
    var out: usize = 10;
    parseQueryParam("limit=99999999999999999999999999", "limit", &out);
    try std.testing.expectEqual(@as(usize, 10), out);
}

test "parseQueryParam: zero value parses correctly" {
    var out: usize = 99;
    parseQueryParam("offset=0", "offset", &out);
    try std.testing.expectEqual(@as(usize, 0), out);
}

test "parseQueryParam: param name is prefix of another param" {
    var out: usize = 0;
    parseQueryParam("limited=100&limit=25", "limit", &out);
    try std.testing.expectEqual(@as(usize, 25), out);
}

test "parseStringQueryParam: value exceeds buffer returns null" {
    var buf: [3]u8 = undefined;
    const result = parseStringQueryParam("sort=longname", "sort", &buf);
    try std.testing.expect(result == null);
}

test "parseStringQueryParam: exact buffer size works" {
    var buf: [3]u8 = undefined;
    const result = parseStringQueryParam("dir=asc", "dir", &buf);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("asc", result.?);
}

test "parseStringQueryParam: multiple params finds correct one" {
    var buf: [10]u8 = undefined;
    const result = parseStringQueryParam("limit=50&sort=name&dir=desc", "sort", &buf);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("name", result.?);
}

test "parseStringQueryParam: param with equals in value" {
    var buf: [20]u8 = undefined;
    const result = parseStringQueryParam("filter=a=b", "filter", &buf);
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("a=b", result.?);
}

test "urlDecode: plain string unchanged" {
    var buf: [64]u8 = undefined;
    const result = urlDecode(&buf, "hello") orelse unreachable;
    try std.testing.expectEqualStrings("hello", result);
}

test "urlDecode: plus to space" {
    var buf: [64]u8 = undefined;
    const result = urlDecode(&buf, "hello+world") orelse unreachable;
    try std.testing.expectEqualStrings("hello world", result);
}

test "urlDecode: percent encoding" {
    var buf: [64]u8 = undefined;
    const result = urlDecode(&buf, "hello%20world") orelse unreachable;
    try std.testing.expectEqualStrings("hello world", result);
}

test "urlDecode: special chars" {
    var buf: [64]u8 = undefined;
    const result = urlDecode(&buf, "%2Fpath%3Fquery%3Dval") orelse unreachable;
    try std.testing.expectEqualStrings("/path?query=val", result);
}

test "urlDecode: empty string" {
    var buf: [64]u8 = undefined;
    const result = urlDecode(&buf, "") orelse unreachable;
    try std.testing.expectEqualStrings("", result);
}

test "escapeStringValue: consecutive single quotes" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "''");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("''''", result);
}

test "escapeStringValue: single quote at start and end" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "'hello'");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("''hello''", result);
}

test "escapeStringValue: unicode characters preserved" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "caf\xc3\xa9");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("caf\xc3\xa9", result);
}

test "escapeStringValue: mixed quotes and other chars" {
    const allocator = std.testing.allocator;
    const result = try escapeStringValue(allocator, "it's a \"test\"");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("it''s a \"test\"", result);
}

test "escapeStringValue: null bytes rejected at any position" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidCharacter, escapeStringValue(allocator, "hello\x00world"));
    try std.testing.expectError(error.InvalidCharacter, escapeStringValue(allocator, "\x00start"));
    try std.testing.expectError(error.InvalidCharacter, escapeStringValue(allocator, "end\x00"));
    try std.testing.expectError(error.InvalidCharacter, escapeStringValue(allocator, "\x00"));
}

test "writeJsonEscaped: null byte is escaped per RFC 8259" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try writeJsonEscaped(buf.writer(std.testing.allocator), "ab\x00cd");
    try std.testing.expectEqualStrings("ab\\u0000cd", buf.items);
}

test "writeJsonEscaped: backslash followed by quote" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try writeJsonEscaped(buf.writer(std.testing.allocator), "\\\"");
    try std.testing.expectEqualStrings("\\\\\\\"", buf.items);
}

test "writeJsonEscaped: very long string" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    const long = "a" ** 2000;
    try writeJsonEscaped(buf.writer(std.testing.allocator), long);
    try std.testing.expectEqual(@as(usize, 2000), buf.items.len);
}

test "writeJsonEscaped: all whitespace escapes" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try writeJsonEscaped(buf.writer(std.testing.allocator), "\n\r\t");
    try std.testing.expectEqualStrings("\\n\\r\\t", buf.items);
}

test "indexOfIgnoreCase: finds word at any position" {
    const at_start = indexOfIgnoreCase("DROP TABLE t", "drop");
    try std.testing.expect(at_start != null);
    try std.testing.expectEqual(@as(usize, 0), at_start.?);
    const in_middle = indexOfIgnoreCase("ALTER TABLE foo ADD COLUMN bar", "ADD");
    try std.testing.expect(in_middle != null);
    try std.testing.expectEqual(@as(usize, 16), in_middle.?);
}

test "indexOfIgnoreCase: returns null when not found" {
    try std.testing.expect(indexOfIgnoreCase("SELECT * FROM t", "DROP") == null);
    try std.testing.expect(indexOfIgnoreCase("ab", "abcdef") == null);
}

test "indexOfIgnoreCase: empty needle returns zero" {
    const result = indexOfIgnoreCase("anything", "");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?);
}

test "eqlLower: unicode bytes pass through" {
    try std.testing.expect(eqlLower("\xc3\xa9", "\xc3\xa9"));
}

test "eqlLower: one char difference" {
    try std.testing.expect(!eqlLower("abc", "abd"));
}

test "findContentLength: various formats" {
    try std.testing.expectEqual(
        @as(?usize, 42),
        findContentLength("POST / HTTP/1.1\r\nContent-Length: 42\r\n\r\n"),
    );
}

test "findContentLength: content-length at end of headers" {
    try std.testing.expectEqual(
        @as(?usize, 10),
        findContentLength("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 10\r\n\r\n"),
    );
}

test "findContentLength: zero content length value" {
    try std.testing.expectEqual(
        @as(?usize, 0),
        findContentLength("POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n"),
    );
}

test "findContentLength: large value" {
    try std.testing.expectEqual(
        @as(?usize, 999999),
        findContentLength("POST / HTTP/1.1\r\nContent-Length: 999999\r\n\r\n"),
    );
}

test "findContentLength: not present" {
    try std.testing.expect(findContentLength("POST / HTTP/1.1\r\nHost: x\r\n\r\n") == null);
}

test "findContentLength: only header name no value" {
    const result = findContentLength("Content-Length: \r\n\r\n");
    try std.testing.expect(result == null);
}

test "findContentLength: content-length in lowercase" {
    const result = findContentLength("content-length: 42\r\n\r\n");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 42), result.?);
}

test "findContentLength: content-length with no space after colon" {
    const result = findContentLength("Content-Length:99\r\n\r\n");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 99), result.?);
}

test "findContentLength: content-length with multiple spaces" {
    const result = findContentLength("Content-Length:   77\r\n\r\n");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 77), result.?);
}

test "findContentLength: very large content-length" {
    const result = findContentLength("Content-Length: 1048576\r\n\r\n");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 1048576), result.?);
}

test "findContentLength: content-length among many headers" {
    const request = "Host: localhost\r\nAccept: */*\r\nContent-Length: 256\r\nConnection: close\r\n\r\n";
    const result = findContentLength(request);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 256), result.?);
}

test "checkOrigin: no origin header allows request" {
    const request = "POST /api/connect HTTP/1.1\r\nHost: localhost\r\n\r\n";
    try std.testing.expect(checkOrigin(request, 8080));
}

test "checkOrigin: valid 127.0.0.1 origin allows request" {
    const request = "POST /api/connect HTTP/1.1\r\nOrigin: http://127.0.0.1:8080\r\nHost: localhost\r\n\r\n";
    try std.testing.expect(checkOrigin(request, 8080));
}

test "checkOrigin: valid localhost origin allows request" {
    const request = "POST /api/connect HTTP/1.1\r\nOrigin: http://localhost:8080\r\nHost: localhost\r\n\r\n";
    try std.testing.expect(checkOrigin(request, 8080));
}

test "checkOrigin: evil origin rejects request" {
    const request = "POST /api/connect HTTP/1.1\r\nOrigin: http://evil.com\r\nHost: localhost\r\n\r\n";
    try std.testing.expect(!checkOrigin(request, 8080));
}

test "checkOrigin: wrong port rejects request" {
    const request = "POST /api/connect HTTP/1.1\r\nOrigin: http://127.0.0.1:9999\r\nHost: localhost\r\n\r\n";
    try std.testing.expect(!checkOrigin(request, 8080));
}

test "checkOrigin: custom port works" {
    const request = "POST /api/sql HTTP/1.1\r\nOrigin: http://127.0.0.1:3000\r\n\r\n";
    try std.testing.expect(checkOrigin(request, 3000));
}

test "findHeader: finds Origin header" {
    const request = "POST /api HTTP/1.1\r\nOrigin: http://localhost:8080\r\nHost: localhost\r\n\r\n";
    const origin = findHeader(request, "Origin");
    try std.testing.expect(origin != null);
    try std.testing.expectEqualStrings("http://localhost:8080", origin.?);
}

test "findHeader: returns null for missing header" {
    const request = "POST /api HTTP/1.1\r\nHost: localhost\r\n\r\n";
    const origin = findHeader(request, "Origin");
    try std.testing.expect(origin == null);
}

test "findHeader: case insensitive" {
    const request = "POST /api HTTP/1.1\r\norigin: http://localhost:8080\r\n\r\n";
    const origin = findHeader(request, "Origin");
    try std.testing.expect(origin != null);
    try std.testing.expectEqualStrings("http://localhost:8080", origin.?);
}

test "findHeader: trims whitespace from value" {
    const request = "POST /api HTTP/1.1\r\nOrigin:  http://localhost:8080  \r\n\r\n";
    const origin = findHeader(request, "Origin");
    try std.testing.expect(origin != null);
    try std.testing.expectEqualStrings("http://localhost:8080", origin.?);
}

test "escapeIdentifier: null bytes rejected at any position" {
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.InvalidIdentifier, escapeIdentifier(allocator, "table\x00; DROP TABLE users"));
    // Start position
    try std.testing.expectError(error.InvalidIdentifier, escapeIdentifier(allocator, "\x00table"));
    // End position
    try std.testing.expectError(error.InvalidIdentifier, escapeIdentifier(allocator, "table\x00"));
}

test "escapeIdentifier: normal string still works" {
    const allocator = std.testing.allocator;
    const result = try escapeIdentifier(allocator, "normal_table");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("normal_table", result);
}

test "extractJsonField: nested escaped quotes in value" {
    const body = "{\"key\": \"value with \\\"nested\\\" quotes\"}";
    const result = extractJsonField(body, "key");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("value with \\\"nested\\\" quotes", result.?);
}

test "extractJsonField: very long value" {
    const long_val = "a" ** 500;
    const body = "{\"data\": \"" ++ long_val ++ "\"}";
    const result = extractJsonField(body, "data");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 500), result.?.len);
}

test "extractJsonField: value with backslash sequences" {
    const body = "{\"path\": \"C:\\\\Users\\\\test\\\\file.txt\"}";
    const result = extractJsonField(body, "path");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("C:\\\\Users\\\\test\\\\file.txt", result.?);
}

test "extractJsonField: field name that is substring of another" {
    const body = "{\"name\": \"Alice\", \"username\": \"bob\"}";
    const result = extractJsonField(body, "name");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("Alice", result.?);
}

test "extractJsonField: value is a single character" {
    const body = "{\"x\": \"y\"}";
    const result = extractJsonField(body, "x");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("y", result.?);
}

test "extractJsonField: unicode value" {
    const body = "{\"greeting\": \"\\u3053\\u3093\\u306b\\u3061\\u306f\"}";
    const result = extractJsonField(body, "greeting");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("\\u3053\\u3093\\u306b\\u3061\\u306f", result.?);
}

test "extractJsonField: multiple colons in value" {
    const body = "{\"url\": \"http://host:8080/path:sub\"}";
    const result = extractJsonField(body, "url");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("http://host:8080/path:sub", result.?);
}

test "extractJsonField: field not found in deeply nested body" {
    const body = "{\"outer\": {\"inner\": \"val\"}, \"other\": \"test\"}";
    const result = extractJsonField(body, "missing");
    try std.testing.expect(result == null);
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

fn fuzzParseStringQueryParam(_: void, input: []const u8) anyerror!void {
    var buf: [256]u8 = undefined;
    _ = parseStringQueryParam(input, "q", &buf);
}

test "parseStringQueryParam: fuzz" {
    try std.testing.fuzz({}, fuzzParseStringQueryParam, .{});
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
