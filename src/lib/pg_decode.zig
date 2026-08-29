const std = @import("std");
const log = std.log.scoped(.pg_decode);

pub const DecodeError = error{
    OutOfMemory,
    InvalidWireFormat,
};

// PostgreSQL built-in type OIDs (pg_type.dat — stable since PG 8.0)
pub const oid = struct {
    pub const bool_oid = 16;
    pub const bytea = 17;
    pub const char_oid = 18;
    pub const int8 = 20;
    pub const int2 = 21;
    pub const int4 = 23;
    pub const text = 25;
    pub const json = 114;
    pub const float4 = 700;
    pub const float8 = 701;
    pub const cidr = 650;
    pub const inet = 869;
    pub const bpchar = 1042;
    pub const varchar = 1043;
    pub const date = 1082;
    pub const time = 1083;
    pub const timestamp = 1114;
    pub const timestamptz = 1184;
    pub const numeric = 1700;
    pub const uuid = 2950;
    pub const jsonb = 3802;
};

/// Decode a single column value from its PostgreSQL binary wire form to a
/// text representation matching libpq's text-protocol output.
/// Returned slice is allocated from `arena` (caller arena lifetime).
pub fn decodeColumnByOid(arena: std.mem.Allocator, col_oid: u32, raw: []const u8) DecodeError![]const u8 {
    return switch (col_oid) {
        oid.bool_oid => decodeBool(arena, raw),
        oid.bytea => decodeBytea(arena, raw),
        oid.int2 => decodeInt2(arena, raw),
        oid.int4 => decodeInt4(arena, raw),
        oid.int8 => decodeInt8(arena, raw),
        oid.text, oid.varchar, oid.bpchar, oid.char_oid, oid.json => arena.dupe(u8, raw) catch return error.OutOfMemory,
        oid.float4 => decodeFloat4(arena, raw),
        oid.float8 => decodeFloat8(arena, raw),
        oid.date => decodeDate(arena, raw),
        oid.time => decodeTime(arena, raw),
        oid.timestamp => decodeTimestamp(arena, raw),
        oid.timestamptz => decodeTimestamptz(arena, raw),
        oid.numeric => decodeNumeric(arena, raw),
        oid.uuid => decodeUuid(arena, raw),
        oid.jsonb => decodeJsonb(arena, raw),
        oid.inet, oid.cidr => decodeUtf8OrHex(arena, raw),
        else => {
            log.warn("unknown OID {d} ({d} bytes) — returning placeholder", .{ col_oid, raw.len });
            return std.fmt.allocPrint(arena, "?[OID={d}]", .{col_oid}) catch return error.OutOfMemory;
        },
    };
}

fn decodeBool(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 1) return arena.dupe(u8, "?[truncated]") catch return error.OutOfMemory;
    return arena.dupe(u8, if (raw[0] != 0) "t" else "f") catch return error.OutOfMemory;
}

fn decodeInt2(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 2) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const val = std.mem.readInt(i16, raw[0..2], .big);
    return std.fmt.allocPrint(arena, "{d}", .{val}) catch return error.OutOfMemory;
}

fn decodeInt4(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 4) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const val = std.mem.readInt(i32, raw[0..4], .big);
    return std.fmt.allocPrint(arena, "{d}", .{val}) catch return error.OutOfMemory;
}

fn decodeInt8(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 8) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const val = std.mem.readInt(i64, raw[0..8], .big);
    return std.fmt.allocPrint(arena, "{d}", .{val}) catch return error.OutOfMemory;
}

fn decodeFloat4(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 4) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const bits = std.mem.readInt(u32, raw[0..4], .big);
    const val: f32 = @bitCast(bits);
    return std.fmt.allocPrint(arena, "{d}", .{val}) catch return error.OutOfMemory;
}

fn decodeFloat8(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 8) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const bits = std.mem.readInt(u64, raw[0..8], .big);
    const val: f64 = @bitCast(bits);
    return std.fmt.allocPrint(arena, "{d}", .{val}) catch return error.OutOfMemory;
}

fn decodeUuid(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 16) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    return std.fmt.allocPrint(
        arena,
        "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}",
        .{
            raw[0],  raw[1],  raw[2],  raw[3],
            raw[4],  raw[5],  raw[6],  raw[7],
            raw[8],  raw[9],  raw[10], raw[11],
            raw[12], raw[13], raw[14], raw[15],
        },
    ) catch return error.OutOfMemory;
}

const hex_chars = "0123456789abcdef";

fn decodeBytea(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    // "\\x" prefix + 2 hex chars per byte
    const buf = arena.alloc(u8, 2 + raw.len * 2) catch return error.OutOfMemory;
    buf[0] = '\\';
    buf[1] = 'x';
    for (raw, 0..) |b, i| {
        buf[2 + i * 2] = hex_chars[b >> 4];
        buf[2 + i * 2 + 1] = hex_chars[b & 0x0f];
    }
    return buf;
}

// Civil date algorithm: convert days-since-epoch to {year, month, day}.
// Epoch here is 2000-01-01 (PostgreSQL date epoch).
// Algorithm from http://howardhinnant.github.io/date_algorithms.html (civil_from_days).
// PG date epoch is 2000-01-01 = Unix day 10957 = day 730425 since 0000-03-01 (Hinnant's epoch).

fn daysToYmd(days_since_pg_epoch: i32) struct { year: i32, month: u8, day: u8 } {
    // Convert to days since 0000-03-01 (Hinnant's epoch) for the civil algorithm.
    // Unix epoch = 1970-01-01 = day 719468 since 0000-03-01.
    // PG epoch = 2000-01-01 = Unix day 10957 = day 719468+10957 = 730425 since 0000-03-01.
    const z: i64 = @as(i64, days_since_pg_epoch) + 730425;
    const era: i64 = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe: u64 = @intCast(z - era * 146097);
    const yoe: u64 = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const y: i64 = @as(i64, @intCast(yoe)) + era * 400;
    const doy: u64 = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp: u64 = (5 * doy + 2) / 153;
    const d: u8 = @intCast(doy - (153 * mp + 2) / 5 + 1);
    const m: u8 = if (mp < 10) @intCast(mp + 3) else @intCast(mp - 9);
    const year: i32 = @intCast(if (m <= 2) y + 1 else y);
    return .{ .year = year, .month = m, .day = d };
}

fn decodeDate(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 4) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const days = std.mem.readInt(i32, raw[0..4], .big);
    const ymd = daysToYmd(days);
    // Use unsigned cast for zero-padded fields; year is signed (BCE support).
    const yr: u32 = @intCast(@abs(ymd.year));
    if (ymd.year < 0) {
        return std.fmt.allocPrint(arena, "-{d:0>4}-{d:0>2}-{d:0>2}", .{ yr, ymd.month, ymd.day }) catch return error.OutOfMemory;
    }
    return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2}", .{ yr, ymd.month, ymd.day }) catch return error.OutOfMemory;
}

fn decodeTime(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 8) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const us = std.mem.readInt(i64, raw[0..8], .big);
    const total_us: u64 = @intCast(@max(0, us));
    const s_total: u64 = total_us / 1_000_000;
    const frac_us: u64 = total_us % 1_000_000;
    const hh: u64 = s_total / 3600;
    const mm: u64 = (s_total % 3600) / 60;
    const ss: u64 = s_total % 60;
    return std.fmt.allocPrint(arena, "{d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{ hh, mm, ss, frac_us }) catch return error.OutOfMemory;
}

fn decodeTimestampInner(arena: std.mem.Allocator, raw: []const u8, with_tz: bool) DecodeError![]const u8 {
    if (raw.len < 8) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;
    const us = std.mem.readInt(i64, raw[0..8], .big);

    // PG timestamp epoch: 2000-01-01 00:00:00 UTC.
    // us can be negative (dates before 2000).
    const us_per_day: i64 = 86_400 * 1_000_000;
    // days_pg is floor-divided (negative us → day before epoch)
    const days_pg: i32 = @intCast(@divFloor(us, us_per_day));
    const us_in_day: u64 = @intCast(us - @as(i64, days_pg) * us_per_day);

    const s_in_day: u64 = us_in_day / 1_000_000;
    const frac_us: u64 = us_in_day % 1_000_000;
    const hh: u64 = s_in_day / 3600;
    const mm: u64 = (s_in_day % 3600) / 60;
    const ss: u64 = s_in_day % 60;
    const ymd = daysToYmd(days_pg);
    const yr: u32 = @intCast(@abs(ymd.year));

    if (frac_us == 0) {
        if (with_tz) {
            return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}+00", .{
                yr, ymd.month, ymd.day, hh, mm, ss,
            }) catch return error.OutOfMemory;
        } else {
            return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
                yr, ymd.month, ymd.day, hh, mm, ss,
            }) catch return error.OutOfMemory;
        }
    } else {
        if (with_tz) {
            return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}+00", .{
                yr, ymd.month, ymd.day, hh, mm, ss, frac_us,
            }) catch return error.OutOfMemory;
        } else {
            return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>6}", .{
                yr, ymd.month, ymd.day, hh, mm, ss, frac_us,
            }) catch return error.OutOfMemory;
        }
    }
}

fn decodeTimestamp(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    return decodeTimestampInner(arena, raw, false);
}

fn decodeTimestamptz(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    return decodeTimestampInner(arena, raw, true);
}

// PostgreSQL NumericVar binary wire format:
//   i16 ndigits   — number of base-10000 digit groups
//   i16 weight    — weight of first digit group (groups before decimal point = weight+1)
//   i16 sign      — 0x0000=positive, 0x4000=negative, 0xC000=NaN
//   i16 dscale    — number of digits after decimal point
//   ndigits × i16 — base-10000 digit groups (most-significant first)
//
// Reconstruction: groups[0] corresponds to 10000^weight, each group is 0..9999.
fn decodeNumeric(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 8) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;

    const ndigits: u16 = @bitCast(std.mem.readInt(i16, raw[0..2], .big));
    const weight = std.mem.readInt(i16, raw[2..4], .big);
    const sign: u16 = @bitCast(std.mem.readInt(i16, raw[4..6], .big));
    const dscale: u16 = @bitCast(std.mem.readInt(i16, raw[6..8], .big));

    if (sign == 0xC000) return arena.dupe(u8, "NaN") catch return error.OutOfMemory;

    const header_size: usize = 8;
    const needed = header_size + @as(usize, ndigits) * 2;
    if (raw.len < needed) return std.fmt.allocPrint(arena, "?[truncated]", .{}) catch return error.OutOfMemory;

    // Read digit groups
    var digits: [1024]i16 = undefined;
    const nd: usize = @min(ndigits, digits.len);
    for (0..nd) |i| {
        digits[i] = std.mem.readInt(i16, raw[header_size + i * 2 ..][0..2], .big);
    }

    // Build decimal string.
    // weight is the index of the most significant group; groups before and
    // including weight are integer digits; groups after weight are fractional.
    var out = std.ArrayList(u8){};
    errdefer out.deinit(arena);

    if (sign == 0x4000) out.append(arena, '-') catch return error.OutOfMemory;

    // Integer part: groups[0..weight+1] if weight >= 0
    const int_groups: i32 = @as(i32, weight) + 1;

    if (int_groups <= 0) {
        // All groups are fractional; emit leading "0"
        out.append(arena, '0') catch return error.OutOfMemory;
    } else {
        // Emit integer groups. First group has no leading zeros.
        var gi: i32 = 0;
        while (gi < int_groups) : (gi += 1) {
            const group_val: u16 = if (gi < @as(i32, @intCast(nd))) @intCast(digits[@intCast(gi)]) else 0;
            if (gi == 0) {
                const s = std.fmt.allocPrint(arena, "{d}", .{group_val}) catch return error.OutOfMemory;
                out.appendSlice(arena, s) catch return error.OutOfMemory;
            } else {
                const s = std.fmt.allocPrint(arena, "{d:0>4}", .{group_val}) catch return error.OutOfMemory;
                out.appendSlice(arena, s) catch return error.OutOfMemory;
            }
        }
    }

    if (dscale > 0) {
        out.append(arena, '.') catch return error.OutOfMemory;

        // Fractional part: groups starting at index max(0, int_groups)
        var frac_digits_written: u16 = 0;
        var gi: i32 = if (int_groups > 0) int_groups else 0;
        while (frac_digits_written < dscale) {
            const group_val: u16 = if (gi < @as(i32, @intCast(nd))) @intCast(digits[@intCast(gi)]) else 0;
            // Emit exactly 4 digits, then trim to dscale
            const group_str = std.fmt.allocPrint(arena, "{d:0>4}", .{group_val}) catch return error.OutOfMemory;
            var ci: usize = 0;
            while (ci < 4 and frac_digits_written < dscale) : (ci += 1) {
                out.append(arena, group_str[ci]) catch return error.OutOfMemory;
                frac_digits_written += 1;
            }
            gi += 1;
        }
    }

    return out.toOwnedSlice(arena) catch return error.OutOfMemory;
}

fn decodeJsonb(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (raw.len < 1) return arena.dupe(u8, "?[truncated]") catch return error.OutOfMemory;
    // Binary jsonb carries a 1-byte version prefix (0x01 for every PG release to
    // date); the text protocol does not. Stripping unconditionally ate the opening
    // '{' of every text-protocol value, which then failed to round-trip on UPDATE.
    // 0x01 is not valid as the first byte of JSON text, so the check is unambiguous.
    const body = if (raw[0] == 0x01) raw[1..] else raw;
    return arena.dupe(u8, body) catch return error.OutOfMemory;
}

fn decodeUtf8OrHex(arena: std.mem.Allocator, raw: []const u8) DecodeError![]const u8 {
    if (std.unicode.utf8ValidateSlice(raw)) {
        return arena.dupe(u8, raw) catch return error.OutOfMemory;
    }
    return decodeBytea(arena, raw);
}

// --- Tests ---

test "decodeBool: true byte decodes to t" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.bool_oid, &[_]u8{0x01});
    try std.testing.expectEqualStrings("t", r);
}

test "decodeBool: false byte decodes to f" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.bool_oid, &[_]u8{0x00});
    try std.testing.expectEqualStrings("f", r);
}

test "decodeInt4: positive 42" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.int4, &[_]u8{ 0, 0, 0, 42 });
    try std.testing.expectEqualStrings("42", r);
}

test "decodeInt4: negative -42" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.int4, &[_]u8{ 255, 255, 255, 214 });
    try std.testing.expectEqualStrings("-42", r);
}

test "decodeInt8: max int64" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // 9223372036854775807 = 0x7FFFFFFFFFFFFFFF
    const r = try decodeColumnByOid(arena.allocator(), oid.int8, &[_]u8{ 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });
    try std.testing.expectEqualStrings("9223372036854775807", r);
}

test "decodeInt2: min int16 -32768" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.int2, &[_]u8{ 0x80, 0x00 });
    try std.testing.expectEqualStrings("-32768", r);
}

test "decodeFloat4: 1.5" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // f32 bit pattern for 1.5: 0x3FC00000
    const r = try decodeColumnByOid(arena.allocator(), oid.float4, &[_]u8{ 0x3F, 0xC0, 0x00, 0x00 });
    try std.testing.expectEqualStrings("1.5", r);
}

test "decodeFloat8: 3.14159" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // f64 bit pattern for 3.14159
    const val: f64 = 3.14159;
    const bits: u64 = @bitCast(val);
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, bits, .big);
    const r = try decodeColumnByOid(arena.allocator(), oid.float8, &raw);
    // Round-trip check: parse result back
    const parsed = try std.fmt.parseFloat(f64, r);
    try std.testing.expect(@abs(parsed - 3.14159) < 1e-10);
}

test "decodeText: passes through UTF-8 unchanged" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.text, "hello world");
    try std.testing.expectEqualStrings("hello world", r);
}

test "decodeUuid: canonical 36-char form" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const raw = [_]u8{ 0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00 };
    const r = try decodeColumnByOid(arena.allocator(), oid.uuid, &raw);
    try std.testing.expectEqualStrings("550e8400-e29b-41d4-a716-446655440000", r);
}

test "decodeBytea: hex-dump with lowercase prefix" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.bytea, &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF });
    try std.testing.expectEqualStrings("\\xdeadbeef", r);
}

test "decodeTime: zero microseconds midnight" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.time, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 });
    try std.testing.expectEqualStrings("00:00:00.000000", r);
}

test "decodeTimestamp: PG epoch is 2000-01-01 00:00:00" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.timestamp, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 });
    try std.testing.expectEqualStrings("2000-01-01 00:00:00", r);
}

test "decodeTimestamp: plus one day" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // 1 day = 86400 * 1_000_000 = 86_400_000_000 microseconds
    const one_day_us: i64 = 86_400 * 1_000_000;
    var raw: [8]u8 = undefined;
    std.mem.writeInt(i64, &raw, one_day_us, .big);
    const r = try decodeColumnByOid(arena.allocator(), oid.timestamp, &raw);
    try std.testing.expectEqualStrings("2000-01-02 00:00:00", r);
}

test "decodeTimestamptz: appends +00" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.timestamptz, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 });
    try std.testing.expectEqualStrings("2000-01-01 00:00:00+00", r);
}

test "decodeDate: PG epoch 0 days = 2000-01-01" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.date, &[_]u8{ 0, 0, 0, 0 });
    try std.testing.expectEqualStrings("2000-01-01", r);
}

test "decodeDate: 366 days = 2001-01-01 (leap year 2000)" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var raw: [4]u8 = undefined;
    std.mem.writeInt(i32, &raw, 366, .big);
    const r = try decodeColumnByOid(arena.allocator(), oid.date, &raw);
    try std.testing.expectEqualStrings("2001-01-01", r);
}

test "decodeNumeric: 12345.67" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    // 12345.67 in NumericVar wire format:
    // ndigits=2, weight=0 (groups: 1, 2345 before decimal; but weight=0 means 1 group before decimal)
    // Actually: 12345.67 = [1, 2345] with weight=1, dscale=2
    // weight=1: first group (1) = 10000^1 * 1 = 10000... wait.
    // Let's compute properly:
    // 12345.67 = 1*10000^1 + 2345*10000^0 + 6700*10000^-1
    // ndigits=3, weight=1, dscale=2
    // groups: 1, 2345, 6700
    var buf: [14]u8 = undefined;
    std.mem.writeInt(i16, buf[0..2], 3, .big); // ndigits
    std.mem.writeInt(i16, buf[2..4], 1, .big); // weight
    std.mem.writeInt(i16, buf[4..6], 0, .big); // sign = positive
    std.mem.writeInt(i16, buf[6..8], 2, .big); // dscale
    std.mem.writeInt(i16, buf[8..10], 1, .big); // group 0: 1
    std.mem.writeInt(i16, buf[10..12], 2345, .big); // group 1: 2345
    std.mem.writeInt(i16, buf[12..14], 6700, .big); // group 2: 6700
    const r = try decodeColumnByOid(arena.allocator(), oid.numeric, &buf);
    try std.testing.expectEqualStrings("12345.67", r);
}

test "decodeJsonb: strips leading version byte in binary form" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const raw = [_]u8{ 0x01, '{', '"', 'a', '"', ':', '1', '}' };
    const r = try decodeColumnByOid(arena.allocator(), oid.jsonb, &raw);
    try std.testing.expectEqualStrings("{\"a\":1}", r);
}

test "decodeJsonb: text form keeps its opening brace" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.jsonb, "{\"ok\": true, \"row\": 2}");
    try std.testing.expectEqualStrings("{\"ok\": true, \"row\": 2}", r);
}

test "decodeJsonb: text form for non-object values" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    try std.testing.expectEqualStrings("[1,2,3]", try decodeColumnByOid(alloc, oid.jsonb, "[1,2,3]"));
    try std.testing.expectEqualStrings("\"str\"", try decodeColumnByOid(alloc, oid.jsonb, "\"str\""));
    try std.testing.expectEqualStrings("42", try decodeColumnByOid(alloc, oid.jsonb, "42"));
    try std.testing.expectEqualStrings("null", try decodeColumnByOid(alloc, oid.jsonb, "null"));
}

test "decodeUtf8OrHex: valid UTF-8 passes through" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.inet, "hello");
    try std.testing.expectEqualStrings("hello", r);
}

test "decodeColumnByOid: unknown OID returns placeholder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), 999, &[_]u8{0xFF});
    try std.testing.expect(std.mem.startsWith(u8, r, "?[OID="));
    try std.testing.expectEqualStrings("?[OID=999]", r);
}

test "decodeColumnByOid: truncated int4 returns placeholder" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const r = try decodeColumnByOid(arena.allocator(), oid.int4, &[_]u8{ 0, 0 }); // only 2 bytes
    try std.testing.expectEqualStrings("?[truncated]", r);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
