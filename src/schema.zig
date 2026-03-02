const std = @import("std");
const builtin = @import("builtin");
const postgres = @import("postgres.zig");
const utils = @import("utils.zig");
const web = @import("web.zig");

const ServerState = web.ServerState;

// ── Connection file helpers ───────────────────────────────────────────────

const CONNECTIONS_FILENAME = "connections.json";
const MAX_CONNECTIONS = 100;

const ConnectionEntry = struct {
    id: u64,
    name: []const u8,
    conninfo: []const u8,
    color: []const u8,
};

const ConnectionFileError = error{
    OutOfMemory,
    ReadFailed,
    WriteFailed,
    ParseFailed,
};

/// Build a JSON string representing a single connection entry.
/// Caller owns the returned memory.
fn formatConnectionJson(allocator: std.mem.Allocator, id: u64, name: []const u8, conninfo: []const u8, color: []const u8) ConnectionFileError![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    const w = buf.writer();
    w.writeAll("{\"id\":") catch return error.OutOfMemory;
    w.print("{d}", .{id}) catch return error.OutOfMemory;
    w.writeAll(",\"name\":\"") catch return error.OutOfMemory;
    utils.writeJsonEscaped(w, name) catch return error.OutOfMemory;
    w.writeAll("\",\"conninfo\":\"") catch return error.OutOfMemory;
    utils.writeJsonEscaped(w, conninfo) catch return error.OutOfMemory;
    w.writeAll("\",\"color\":\"") catch return error.OutOfMemory;
    utils.writeJsonEscaped(w, color) catch return error.OutOfMemory;
    w.writeAll("\"}") catch return error.OutOfMemory;
    return buf.toOwnedSlice() catch return error.OutOfMemory;
}

/// Build the path to the platform-specific config directory for Lux.
/// - Linux: $XDG_CONFIG_HOME/lux/ or ~/.config/lux/
/// - macOS: $XDG_CONFIG_HOME/lux/ (if set) or ~/Library/Application Support/lux/
/// - Windows: %APPDATA%\lux\ or %USERPROFILE%\AppData\Roaming\lux\
/// Creates the directory if it does not exist. Caller owns the returned memory.
fn getConfigDir(allocator: std.mem.Allocator) ConnectionFileError![]u8 {
    const sep = comptime if (builtin.os.tag == .windows) '\\' else '/';
    const sep_str = comptime if (builtin.os.tag == .windows) "\\" else "/";

    // All platforms: XDG_CONFIG_HOME takes priority if set
    if (getEnvVar("XDG_CONFIG_HOME")) |xdg| {
        const path = std.fmt.allocPrint(allocator, "{s}" ++ sep_str ++ "lux" ++ sep_str, .{xdg}) catch return error.OutOfMemory;
        std.fs.cwd().makePath(path) catch {};
        return path;
    }

    switch (builtin.os.tag) {
        .windows => {
            // %APPDATA% (typically C:\Users\<user>\AppData\Roaming)
            if (getEnvVar("APPDATA")) |appdata| {
                const path = std.fmt.allocPrint(allocator, "{s}" ++ sep_str ++ "lux" ++ sep_str, .{appdata}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch {};
                return path;
            }
            // Fallback: %USERPROFILE%\AppData\Roaming\lux\
            if (getEnvVar("USERPROFILE")) |profile| {
                const path = std.fmt.allocPrint(allocator, "{s}" ++ sep_str ++ "AppData" ++ sep_str ++ "Roaming" ++ sep_str ++ "lux" ++ sep_str, .{profile}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch {};
                return path;
            }
            return error.ReadFailed;
        },
        .macos => {
            // ~/Library/Application Support/lux/
            if (getEnvVar("HOME")) |home| {
                const path = std.fmt.allocPrint(allocator, "{s}/Library/Application Support/lux/", .{home}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch {};
                return path;
            }
            return error.ReadFailed;
        },
        else => {
            // Linux: ~/.config/lux/
            if (getEnvVar("HOME")) |home| {
                const path = std.fmt.allocPrint(allocator, "{s}/.config/lux/", .{home}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch {};
                return path;
            }
            return error.ReadFailed;
        },
    }
    _ = sep; // suppress unused variable — sep used in format strings above
}

/// Cross-platform env var lookup. Uses std.posix.getenv on POSIX,
/// std.process.getEnvVarOwned is not needed since we only read the pointer.
fn getEnvVar(key: [*:0]const u8) ?[]const u8 {
    if (builtin.os.tag == .windows) {
        // On Windows, std.posix.getenv is not available.
        // Use the C runtime getenv which Zig links via linkLibC.
        const c = @cImport(@cInclude("stdlib.h"));
        const val = c.getenv(key);
        if (val) |ptr| {
            return std.mem.span(@as([*:0]const u8, @ptrCast(ptr)));
        }
        return null;
    }
    return std.posix.getenv(std.mem.span(key));
}

/// Build the full path to the connections file. Caller owns the returned memory.
fn getConfigFilePath(allocator: std.mem.Allocator) ConnectionFileError![]u8 {
    const dir = try getConfigDir(allocator);
    defer allocator.free(dir);
    const path = std.fmt.allocPrint(allocator, "{s}{s}", .{ dir, CONNECTIONS_FILENAME }) catch return error.OutOfMemory;
    return path;
}

/// Read the connections file and return its raw contents.
/// Uses XDG config directory. Falls back to CWD for backward compatibility.
/// Caller owns the returned memory.
fn readConnectionsFile(allocator: std.mem.Allocator) ConnectionFileError![]u8 {
    // Try XDG config dir first
    if (getConfigFilePath(allocator)) |config_path| {
        defer allocator.free(config_path);
        if (std.fs.cwd().openFile(config_path, .{})) |file| {
            defer file.close();
            const data = file.readToEndAlloc(allocator, 1024 * 1024) catch return error.ReadFailed;
            return data;
        } else |_| {}
    } else |_| {}

    // Backward compatibility: try CWD
    if (std.fs.cwd().openFile(CONNECTIONS_FILENAME, .{})) |file| {
        defer file.close();
        const data = file.readToEndAlloc(allocator, 1024 * 1024) catch return error.ReadFailed;
        return data;
    } else |_| {}

    // No file found — return empty array
    return allocator.dupe(u8, "{\"connections\":[]}") catch return error.OutOfMemory;
}

/// Write raw JSON content to the connections file (mode 0600 — owner-only).
/// Writes to XDG config directory.
fn writeConnectionsFile(allocator: std.mem.Allocator, data: []const u8) ConnectionFileError!void {
    const config_path = getConfigFilePath(allocator) catch {
        // Fallback to CWD if XDG resolution fails
        const file = std.fs.cwd().createFile(CONNECTIONS_FILENAME, .{ .mode = 0o600 }) catch return error.WriteFailed;
        defer file.close();
        file.writeAll(data) catch return error.WriteFailed;
        return;
    };
    defer allocator.free(config_path);
    const file = std.fs.cwd().createFile(config_path, .{ .mode = 0o600 }) catch return error.WriteFailed;
    defer file.close();
    file.writeAll(data) catch return error.WriteFailed;
}

/// Find the highest connection ID in the file content, for auto-increment.
fn findMaxConnectionId(file_content: []const u8) u64 {
    var max_id: u64 = 0;
    var pos: usize = 0;
    while (pos < file_content.len) {
        // Search for "id":
        const id_key = "\"id\":";
        const idx = std.mem.indexOfPos(u8, file_content, pos, id_key) orelse break;
        var vp = idx + id_key.len;
        // Skip whitespace
        while (vp < file_content.len and file_content[vp] == ' ') vp += 1;
        // Parse number
        var end = vp;
        while (end < file_content.len and file_content[end] >= '0' and file_content[end] <= '9') end += 1;
        if (end > vp) {
            const id_val = std.fmt.parseInt(u64, file_content[vp..end], 10) catch 0;
            if (id_val > max_id) max_id = id_val;
        }
        pos = end;
    }
    return max_id;
}

// ── Handler functions ─────────────────────────────────────────────────────

/// Handle POST /api/connect — connect to Postgres, fetch schema, store for per-query use.
pub fn handleConnect(
    stream: std.net.Stream,
    request: []const u8,
    state: *ServerState,
) !void {
    const allocator = state.allocator;

    // Find Content-Length header
    const content_length = utils.findContentLength(request) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };

    if (content_length > 4096) {
        try utils.sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"Request too large\"}");
        return;
    }

    // Find body
    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
        return;
    };
    const body_start = header_end + 4;
    var body = request[body_start..];

    // Read remaining body if needed
    var extra_buf: [4096]u8 = undefined;
    if (body.len < content_length) {
        const already_have = body.len;
        @memcpy(extra_buf[0..already_have], body);
        var read_so_far = already_have;
        while (read_so_far < content_length) {
            const n = stream.read(extra_buf[read_so_far..content_length]) catch {
                try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Read failed\"}");
                return;
            };
            if (n == 0) break;
            read_so_far += n;
        }
        body = extra_buf[0..read_so_far];
    } else {
        body = body[0..content_length];
    }

    // Parse conninfo
    const conninfo_str = utils.extractJsonField(body, "conninfo") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing conninfo field\"}");
        return;
    };

    // Build null-terminated connection string
    const conninfo_z = allocator.allocSentinel(u8, conninfo_str.len, 0) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    @memcpy(conninfo_z[0..conninfo_str.len], conninfo_str);

    // Test connection and fetch schema
    var pg_conn = postgres.PgConnection.connectVerbose(conninfo_z) catch {
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to connect to PostgreSQL. libpq returned null.\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!pg_conn.isOk()) {
        // Capture the actual error message from libpq
        var err_buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"Connection failed: ") catch {};
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch {};
        ew.writeAll("\"}") catch {};
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    }

    var schema = pg_conn.fetchSchema(allocator) catch {
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to fetch database schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to format schema\"}");
        return;
    };

    // Fetch enhanced schema (PK, FK, ENUM, nullability)
    var enhanced = pg_conn.fetchEnhancedSchema(allocator) catch null;

    // Store connection info and schema in state (free old if any)
    if (state.conninfo_z) |old| allocator.free(old);
    if (state.schema_text) |old| allocator.free(old);
    if (state.schema_tables) |old| {
        for (old) |table| {
            for (table.columns) |col| {
                if (col.name.len > 0) allocator.free(col.name);
                if (col.data_type.len > 0) allocator.free(col.data_type);
            }
            allocator.free(table.columns);
            if (table.name.len > 0) allocator.free(table.name);
        }
        allocator.free(old);
    }
    if (state.enhanced_schema) |old| {
        for (old) |*t| @constCast(t).deinit(allocator);
        allocator.free(old);
    }
    state.conninfo_z = conninfo_z;
    // Store a copy of the connection string for reconnect
    if (state.last_conninfo) |old_ci| allocator.free(@constCast(old_ci));
    state.last_conninfo = allocator.dupe(u8, conninfo_str) catch null;
    state.schema_text = schema_text;
    state.schema_tables = schema.tables;
    if (enhanced) |*es| {
        state.enhanced_schema = es.tables;
        // Prevent deinit from freeing tables we now own
        es.tables = &.{};
        es.deinit();
    }
    // Prevent schema.deinit from freeing tables we now own
    schema.tables = &.{};
    schema.deinit();

    // Build response with schema info
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();
    w.writeAll("{\"status\":\"connected\",\"schema\":\"") catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"status\":\"connected\"}");
        return;
    };
    utils.writeJsonEscaped(w, schema_text) catch return;
    // Count tables
    var n_tables: usize = 0;
    if (state.schema_tables) |tables| n_tables = tables.len;
    w.print("\",\"tables\":{d}}}", .{n_tables}) catch return;
    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle GET /api/schema — return database schema as JSON.
/// Re-fetches schema from PostgreSQL to ensure fresh data.
/// Falls back to cached data if the refresh fails.
pub fn handleSchema(stream: std.net.Stream, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Re-fetch schema from PostgreSQL to return fresh data.
    // If anything fails, fall back to cached data.
    refresh: {
        const conninfo_z = state.conninfo_z orelse break :refresh;
        var pg_conn = postgres.PgConnection.connect(conninfo_z) catch break :refresh;
        defer pg_conn.deinit();
        var schema = pg_conn.fetchSchema(allocator) catch break :refresh;
        const new_text = schema.format(allocator) catch {
            schema.deinit();
            break :refresh;
        };
        var enhanced = pg_conn.fetchEnhancedSchema(allocator) catch null;

        // Free old state
        if (state.schema_text) |old| allocator.free(old);
        if (state.schema_tables) |old| {
            for (old) |table| {
                for (table.columns) |col| {
                    if (col.name.len > 0) allocator.free(col.name);
                    if (col.data_type.len > 0) allocator.free(col.data_type);
                }
                allocator.free(table.columns);
                if (table.name.len > 0) allocator.free(table.name);
            }
            allocator.free(old);
        }
        if (state.enhanced_schema) |old| {
            for (old) |*t| @constCast(t).deinit(allocator);
            allocator.free(old);
        }

        // Store new state
        state.schema_text = new_text;
        state.schema_tables = schema.tables;
        if (enhanced) |*es| {
            state.enhanced_schema = es.tables;
            es.tables = &.{};
            es.deinit();
        }
        schema.tables = &.{};
        schema.deinit();
    }

    const tables = state.schema_tables orelse {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"tables\":[]}");
        return;
    };

    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();

    try w.writeAll("{\"tables\":[");

    // Use enhanced schema if available, otherwise fall back to basic
    if (state.enhanced_schema) |etables| {
        for (etables, 0..) |etable, ti| {
            if (ti > 0) try w.writeByte(',');
            try w.writeAll("{\"name\":\"");
            try utils.writeJsonEscaped(w, etable.name);
            try w.print("\",\"has_primary_key\":{s},\"primary_key_columns\":[", .{if (etable.has_primary_key) "true" else "false"});
            for (etable.primary_key_columns, 0..) |pk, pi| {
                if (pi > 0) try w.writeByte(',');
                try w.writeByte('"');
                try utils.writeJsonEscaped(w, pk);
                try w.writeByte('"');
            }
            try w.writeAll("],\"columns\":[");
            for (etable.columns, 0..) |col, ci| {
                if (ci > 0) try w.writeByte(',');
                try w.writeAll("{\"name\":\"");
                try utils.writeJsonEscaped(w, col.name);
                try w.writeAll("\",\"type\":\"");
                try utils.writeJsonEscaped(w, col.data_type);
                try w.print("\",\"is_primary_key\":{s},\"is_nullable\":{s}", .{
                    if (col.is_primary_key) "true" else "false",
                    if (col.is_nullable) "true" else "false",
                });
                if (col.column_default) |def| {
                    try w.writeAll(",\"column_default\":\"");
                    try utils.writeJsonEscaped(w, def);
                    try w.writeByte('"');
                }
                if (col.fk_target_table) |fkt| {
                    try w.writeAll(",\"fk_target_table\":\"");
                    try utils.writeJsonEscaped(w, fkt);
                    try w.writeByte('"');
                }
                if (col.fk_target_column) |fkc| {
                    try w.writeAll(",\"fk_target_column\":\"");
                    try utils.writeJsonEscaped(w, fkc);
                    try w.writeByte('"');
                }
                if (col.enum_values) |vals| {
                    try w.writeAll(",\"enum_values\":[");
                    for (vals, 0..) |v, vi| {
                        if (vi > 0) try w.writeByte(',');
                        try w.writeByte('"');
                        try utils.writeJsonEscaped(w, v);
                        try w.writeByte('"');
                    }
                    try w.writeByte(']');
                }
                try w.writeByte('}');
            }
            try w.writeAll("]}");
        }
    } else {
        for (tables, 0..) |table, ti| {
            if (ti > 0) try w.writeByte(',');
            try w.writeAll("{\"name\":\"");
            try utils.writeJsonEscaped(w, table.name);
            try w.writeAll("\",\"columns\":[");
            for (table.columns, 0..) |col, ci| {
                if (ci > 0) try w.writeByte(',');
                try w.writeAll("{\"name\":\"");
                try utils.writeJsonEscaped(w, col.name);
                try w.writeAll("\",\"type\":\"");
                try utils.writeJsonEscaped(w, col.data_type);
                try w.writeAll("\"}");
            }
            try w.writeAll("]}");
        }
    }
    try w.writeAll("]}");

    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle POST /api/reconnect — reconnect using the last connection string.
pub fn handleReconnect(stream: std.net.Stream, state: *ServerState) !void {
    const allocator = state.allocator;

    const last_ci = state.last_conninfo orelse {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No previous connection to reconnect to\"}");
        return;
    };

    // Build null-terminated connection string
    const conninfo_z = allocator.allocSentinel(u8, last_ci.len, 0) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    @memcpy(conninfo_z[0..last_ci.len], last_ci);

    // Test connection
    var pg_conn = postgres.PgConnection.connectVerbose(conninfo_z) catch {
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to reconnect. libpq returned null.\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!pg_conn.isOk()) {
        var err_buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"Reconnect failed: ") catch {};
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch {};
        ew.writeAll("\"}") catch {};
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    }

    // Fetch schema
    var schema = pg_conn.fetchSchema(allocator) catch {
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Reconnected but failed to fetch schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        allocator.free(conninfo_z);
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Reconnected but failed to format schema\"}");
        return;
    };

    // Fetch enhanced schema
    var enhanced = pg_conn.fetchEnhancedSchema(allocator) catch null;

    // Store in state (free old)
    if (state.conninfo_z) |old| allocator.free(old);
    if (state.schema_text) |old| allocator.free(old);
    if (state.schema_tables) |old| {
        for (old) |table| {
            for (table.columns) |col| {
                if (col.name.len > 0) allocator.free(col.name);
                if (col.data_type.len > 0) allocator.free(col.data_type);
            }
            allocator.free(table.columns);
            if (table.name.len > 0) allocator.free(table.name);
        }
        allocator.free(old);
    }
    if (state.enhanced_schema) |old| {
        for (old) |*t| @constCast(t).deinit(allocator);
        allocator.free(old);
    }
    state.conninfo_z = conninfo_z;
    state.schema_text = schema_text;
    state.schema_tables = schema.tables;
    if (enhanced) |*es| {
        state.enhanced_schema = es.tables;
        es.tables = &.{};
        es.deinit();
    }
    schema.tables = &.{};
    schema.deinit();

    // Build response
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();
    w.writeAll("{\"ok\":true,\"schema\":\"") catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"ok\":true}");
        return;
    };
    utils.writeJsonEscaped(w, schema_text) catch return;
    var n_tables: usize = 0;
    if (state.schema_tables) |tables| n_tables = tables.len;
    w.print("\",\"tables\":{d}}}", .{n_tables}) catch return;
    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle GET /api/health — check database connection health.
pub fn handleHealthCheck(stream: std.net.Stream, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"connected\":false}");
        return;
    }

    const allocator = state.allocator;
    const conninfo_z = state.conninfo_z.?;

    const start_time = std.time.milliTimestamp();
    var pg_conn = postgres.PgConnection.connect(conninfo_z) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"connected\":false}");
        return;
    };
    defer pg_conn.deinit();

    // Run SELECT 1 to verify connection
    const sql: [*:0]const u8 = "SELECT 1";
    var pg_result = pg_conn.runQuery(allocator, sql) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"connected\":false}");
        return;
    };
    defer pg_result.deinit();

    const end_time = std.time.milliTimestamp();
    const latency = @as(u64, @intCast(@max(0, end_time - start_time)));

    var resp_buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"connected\":true,\"latency_ms\":{d}}}", .{latency}) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"connected\":true}");
        return;
    };
    try utils.sendResponse(stream, "200 OK", "application/json", resp);
}

/// Handle POST /api/settings/read-only — toggle read-only mode.
pub fn handleReadOnlyToggle(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    var extra_buf: [1024]u8 = undefined;
    const body = readRequestBodySimple(stream, request, &extra_buf, 1024) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };
    const enabled_str = utils.extractJsonField(body, "enabled") orelse {
        // Toggle if no explicit value
        state.read_only = !state.read_only;
        var resp_buf: [64]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.read_only) "true" else "false"}) catch return;
        try utils.sendResponse(stream, "200 OK", "application/json", resp);
        return;
    };
    state.read_only = std.mem.eql(u8, enabled_str, "true");
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.read_only) "true" else "false"}) catch return;
    try utils.sendResponse(stream, "200 OK", "application/json", resp);
}

/// Handle GET /api/settings/read-only — get current read-only state.
pub fn handleReadOnlyGet(stream: std.net.Stream, state: *const ServerState) !void {
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.read_only) "true" else "false"}) catch return;
    try utils.sendResponse(stream, "200 OK", "application/json", resp);
}

/// Handle GET /api/connections — return saved connections.
pub fn handleGetConnections(stream: std.net.Stream, state: *ServerState) !void {
    const allocator = state.allocator;
    const data = readConnectionsFile(allocator) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"connections\":[]}");
        return;
    };
    defer allocator.free(data);
    try utils.sendResponse(stream, "200 OK", "application/json", data);
}

/// Handle POST /api/connections — save a new connection.
pub fn handlePostConnection(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    const allocator = state.allocator;

    // Parse body
    const content_length = utils.findContentLength(request) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };
    if (content_length > 4096) {
        try utils.sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"Request too large\"}");
        return;
    }
    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
        return;
    };
    const body_start = header_end + 4;
    var body = request[body_start..];
    var extra_buf: [4096]u8 = undefined;
    if (body.len < content_length) {
        const already_have = body.len;
        @memcpy(extra_buf[0..already_have], body);
        var read_so_far = already_have;
        while (read_so_far < content_length) {
            const n = stream.read(extra_buf[read_so_far..content_length]) catch {
                try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Read failed\"}");
                return;
            };
            if (n == 0) break;
            read_so_far += n;
        }
        body = extra_buf[0..read_so_far];
    } else {
        body = body[0..content_length];
    }

    // Extract fields
    const name = utils.extractJsonField(body, "name") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing name field\"}");
        return;
    };
    const conninfo = utils.extractJsonField(body, "conninfo") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing conninfo field\"}");
        return;
    };
    const color = utils.extractJsonField(body, "color") orelse "gray";

    // Read existing file
    const existing = readConnectionsFile(allocator) catch {
        // Start fresh if read fails
        const entry = formatConnectionJson(allocator, 1, name, conninfo, color) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(entry);

        var new_file = std.ArrayList(u8).init(allocator);
        defer new_file.deinit();
        const nw = new_file.writer();
        nw.writeAll("{\"connections\":[") catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        nw.writeAll(entry) catch return;
        nw.writeAll("]}") catch return;
        writeConnectionsFile(allocator, new_file.items) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
            return;
        };
        state.next_connection_id = 2;
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"id\":1}");
        return;
    };
    defer allocator.free(existing);

    // Find max existing ID
    const max_id = findMaxConnectionId(existing);
    const new_id = @max(max_id + 1, state.next_connection_id);
    state.next_connection_id = new_id + 1;

    // Build the new entry JSON
    const entry = formatConnectionJson(allocator, new_id, name, conninfo, color) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(entry);

    // Insert entry into existing JSON — find the closing "]}" and insert before it
    var new_file = std.ArrayList(u8).init(allocator);
    defer new_file.deinit();
    const nw = new_file.writer();

    // Find last ']' in existing content
    const close_bracket = std.mem.lastIndexOfScalar(u8, existing, ']') orelse {
        // Malformed file — rewrite
        nw.writeAll("{\"connections\":[") catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        nw.writeAll(entry) catch return;
        nw.writeAll("]}") catch return;
        writeConnectionsFile(allocator, new_file.items) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
            return;
        };
        var id_buf: [64]u8 = undefined;
        const id_resp = std.fmt.bufPrint(&id_buf, "{{\"id\":{d}}}", .{new_id}) catch {
            try utils.sendResponse(stream, "200 OK", "application/json", "{\"id\":0}");
            return;
        };
        try utils.sendResponse(stream, "200 OK", "application/json", id_resp);
        return;
    };

    // Check if there are existing entries (non-empty array)
    const before_bracket = std.mem.trimRight(u8, existing[0..close_bracket], " \t\n\r");
    const needs_comma = before_bracket.len > 0 and before_bracket[before_bracket.len - 1] != '[';

    nw.writeAll(existing[0..close_bracket]) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    if (needs_comma) nw.writeByte(',') catch return;
    nw.writeAll(entry) catch return;
    nw.writeAll("]}") catch return;

    writeConnectionsFile(allocator, new_file.items) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
        return;
    };

    var id_buf: [64]u8 = undefined;
    const id_resp = std.fmt.bufPrint(&id_buf, "{{\"id\":{d}}}", .{new_id}) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"id\":0}");
        return;
    };
    try utils.sendResponse(stream, "200 OK", "application/json", id_resp);
}

/// Handle DELETE /api/connections/:id — delete a saved connection.
pub fn handleDeleteConnection(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    const allocator = state.allocator;

    // Parse ID from path: /api/connections/<id>
    const prefix = "/api/connections/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }
    const id_str = path[prefix.len..];
    const target_id = std.fmt.parseInt(u64, id_str, 10) catch {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid connection ID\"}");
        return;
    };

    // Read existing file
    const existing = readConnectionsFile(allocator) catch {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"No connections file\"}");
        return;
    };
    defer allocator.free(existing);

    // Rebuild the connections array, skipping the one with target_id.
    var new_file = std.ArrayList(u8).init(allocator);
    defer new_file.deinit();
    const nw = new_file.writer();
    nw.writeAll("{\"connections\":[") catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };

    var found = false;
    var first = true;
    var pos: usize = 0;

    // Find the start of the array
    const arr_start = std.mem.indexOf(u8, existing, "[") orelse {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Malformed connections file\"}");
        return;
    };
    pos = arr_start + 1;

    // Parse each object in the array
    while (pos < existing.len) {
        // Skip whitespace and commas
        while (pos < existing.len and (existing[pos] == ' ' or existing[pos] == ',' or existing[pos] == '\n' or existing[pos] == '\r' or existing[pos] == '\t')) pos += 1;
        if (pos >= existing.len or existing[pos] == ']') break;
        if (existing[pos] != '{') break;

        // Find matching closing brace (simple: count depth)
        var depth: usize = 0;
        var in_string = false;
        var obj_end = pos;
        while (obj_end < existing.len) {
            const ch = existing[obj_end];
            if (in_string) {
                if (ch == '\\' and obj_end + 1 < existing.len) {
                    obj_end += 2;
                    continue;
                }
                if (ch == '"') in_string = false;
            } else {
                if (ch == '"') in_string = true;
                if (ch == '{') depth += 1;
                if (ch == '}') {
                    depth -= 1;
                    if (depth == 0) {
                        obj_end += 1;
                        break;
                    }
                }
            }
            obj_end += 1;
        }

        const obj_str = existing[pos..obj_end];

        // Check if this object has the target ID
        const obj_id = findMaxConnectionId(obj_str);
        if (obj_id == target_id) {
            found = true;
        } else {
            if (!first) nw.writeByte(',') catch return;
            nw.writeAll(obj_str) catch return;
            first = false;
        }

        pos = obj_end;
    }

    nw.writeAll("]}") catch return;

    if (!found) {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Connection not found\"}");
        return;
    }

    writeConnectionsFile(allocator, new_file.items) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
        return;
    };

    try utils.sendResponse(stream, "200 OK", "application/json", "{\"ok\":true}");
}

// ── Internal helpers ──────────────────────────────────────────────────────

/// Simple body reader (used by handleReadOnlyToggle).
fn readRequestBodySimple(
    stream: std.net.Stream,
    request: []const u8,
    extra_buf: []u8,
    max_content_length: usize,
) ?[]const u8 {
    const content_length = utils.findContentLength(request) orelse return null;
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

// ── Tests ─────────────────────────────────────────────────────────────────

test "formatConnectionJson: basic entry" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 1, "My DB", "postgresql://localhost/test", "green");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"name\":\"My DB\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"conninfo\":\"postgresql://localhost/test\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"color\":\"green\"") != null);
}

test "formatConnectionJson: escapes special characters in name" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 2, "Test \"DB\"", "pg://localhost", "blue");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "Test \\\"DB\\\"") != null);
}

test "formatConnectionJson: large ID" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 999999, "Prod", "pg://prod", "red");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":999999") != null);
}

test "formatConnectionJson: empty name and conninfo" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 0, "", "", "");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":0") != null);
}

test "formatConnectionJson: name with quotes and backslashes" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 1, "my\\\"db", "pg://x", "red");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":1") != null);
}

test "formatConnectionJson: id zero" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 0, "test", "pg://test", "gray");
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"id\":0") != null);
}

test "findMaxConnectionId: no ids" {
    const result = findMaxConnectionId("{\"connections\":[]}");
    try std.testing.expectEqual(@as(u64, 0), result);
}

test "findMaxConnectionId: single id" {
    const result = findMaxConnectionId("{\"connections\":[{\"id\":5,\"name\":\"test\"}]}");
    try std.testing.expectEqual(@as(u64, 5), result);
}

test "findMaxConnectionId: multiple ids" {
    const result = findMaxConnectionId("{\"connections\":[{\"id\":3},{\"id\":7},{\"id\":2}]}");
    try std.testing.expectEqual(@as(u64, 7), result);
}

test "findMaxConnectionId: empty string" {
    const result = findMaxConnectionId("");
    try std.testing.expectEqual(@as(u64, 0), result);
}

test "findMaxConnectionId: id at boundaries" {
    const result = findMaxConnectionId("{\"id\":100}");
    try std.testing.expectEqual(@as(u64, 100), result);
}

test "findMaxConnectionId: multiple ids with whitespace" {
    const result = findMaxConnectionId("{\"connections\":[{\"id\": 3},{\"id\": 7}]}");
    try std.testing.expectEqual(@as(u64, 7), result);
}

test "findMaxConnectionId: id with leading zeros" {
    // Parses as decimal — "007" = 7
    const result = findMaxConnectionId("{\"id\":007}");
    try std.testing.expectEqual(@as(u64, 7), result);
}

test "findMaxConnectionId: id key without number" {
    const result = findMaxConnectionId("{\"id\":\"string\"}");
    try std.testing.expectEqual(@as(u64, 0), result);
}

test "findMaxConnectionId: very large id" {
    const result = findMaxConnectionId("{\"id\":18446744073709551614}");
    try std.testing.expect(result > 0);
}

test "getConfigFilePath: returns path ending with connections.json" {
    const allocator = std.testing.allocator;
    const path = getConfigFilePath(allocator) catch return;
    defer allocator.free(path);
    try std.testing.expect(std.mem.endsWith(u8, path, "connections.json"));
}

test "getConfigDir: returns a path ending with lux dir separator" {
    const allocator = std.testing.allocator;
    const path = getConfigDir(allocator) catch return;
    defer allocator.free(path);
    // Should end with "lux/" or "lux\"
    const ends_slash = std.mem.endsWith(u8, path, "lux/");
    const ends_backslash = std.mem.endsWith(u8, path, "lux\\");
    try std.testing.expect(ends_slash or ends_backslash);
}
