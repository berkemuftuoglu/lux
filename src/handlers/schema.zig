const std = @import("std");
const builtin = @import("builtin");
const httpz = @import("httpz");
const postgres = @import("postgres");
const utils = @import("utils");
const web = @import("web");

const log = std.log.scoped(.schema);

const connections_filename = "connections.json";

const ConnectionFileError = error{
    OutOfMemory,
    ReadFailed,
    WriteFailed,
    ParseFailed,
};

fn sendJsonResponse(res: *httpz.Response, body: []const u8) void {
    res.content_type = httpz.ContentType.JSON;
    res.body = body;
}

fn sendJsonError(res: *httpz.Response, status: u16, body: []const u8) void {
    res.status = status;
    res.content_type = httpz.ContentType.JSON;
    res.body = body;
}

fn formatConnectionJson(allocator: std.mem.Allocator, id: u64, name: []const u8, conninfo: []const u8, color: []const u8) ConnectionFileError![]u8 {
    var buf = std.ArrayList(u8){};
    errdefer buf.deinit(allocator);
    const w = buf.writer(allocator);
    w.writeAll("{\"id\":") catch return error.OutOfMemory;
    w.print("{d}", .{id}) catch return error.OutOfMemory;
    w.writeAll(",\"name\":\"") catch return error.OutOfMemory;
    utils.writeJsonEscaped(w, name) catch return error.OutOfMemory;
    w.writeAll("\",\"conninfo\":\"") catch return error.OutOfMemory;
    utils.writeJsonEscaped(w, conninfo) catch return error.OutOfMemory;
    w.writeAll("\",\"color\":\"") catch return error.OutOfMemory;
    utils.writeJsonEscaped(w, color) catch return error.OutOfMemory;
    w.writeAll("\"}") catch return error.OutOfMemory;
    return buf.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

fn getConfigDir(allocator: std.mem.Allocator) ConnectionFileError![]u8 {
    const sep_str = comptime if (builtin.os.tag == .windows) "\\" else "/";

    if (getEnvVar("XDG_CONFIG_HOME")) |xdg| {
        const path = std.fmt.allocPrint(allocator, "{s}" ++ sep_str ++ "lux" ++ sep_str, .{xdg}) catch return error.OutOfMemory;
        std.fs.cwd().makePath(path) catch |err| {
            log.warn("could not create config dir: {s}", .{@errorName(err)});
        };
        return path;
    }

    switch (builtin.os.tag) {
        .windows => {
            if (getEnvVar("APPDATA")) |appdata| {
                const path = std.fmt.allocPrint(allocator, "{s}" ++ sep_str ++ "lux" ++ sep_str, .{appdata}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch |err| {
                    log.warn("could not create config dir: {s}", .{@errorName(err)});
                };
                return path;
            }
            if (getEnvVar("USERPROFILE")) |profile| {
                const path = std.fmt.allocPrint(allocator, "{s}" ++ sep_str ++ "AppData" ++ sep_str ++ "Roaming" ++ sep_str ++ "lux" ++ sep_str, .{profile}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch |err| {
                    log.warn("could not create config dir: {s}", .{@errorName(err)});
                };
                return path;
            }
            return error.ReadFailed;
        },
        .macos => {
            if (getEnvVar("HOME")) |home| {
                const path = std.fmt.allocPrint(allocator, "{s}/Library/Application Support/lux/", .{home}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch |err| {
                    log.warn("could not create config dir: {s}", .{@errorName(err)});
                };
                return path;
            }
            return error.ReadFailed;
        },
        else => {
            if (getEnvVar("HOME")) |home| {
                const path = std.fmt.allocPrint(allocator, "{s}/.config/lux/", .{home}) catch return error.OutOfMemory;
                std.fs.cwd().makePath(path) catch |err| {
                    log.warn("could not create config dir: {s}", .{@errorName(err)});
                };
                return path;
            }
            return error.ReadFailed;
        },
    }
}

fn getEnvVar(key: []const u8) ?[]const u8 {
    return std.posix.getenv(key);
}

fn freeSchemaState(state: *web.ServerState) void {
    const allocator = state.allocator;
    if (state.conninfo_z) |old| allocator.free(old);
    state.conninfo_z = null;
    if (state.schema_text) |old| allocator.free(old);
    state.schema_text = null;
    state.schema_tables = null;
    if (state.schema_arena) |*a| a.deinit();
    state.schema_arena = null;
    state.enhanced_schema = null;
    if (state.enhanced_arena) |*a| a.deinit();
    state.enhanced_arena = null;
}

fn getConfigFilePath(allocator: std.mem.Allocator) ConnectionFileError![]u8 {
    const dir = try getConfigDir(allocator);
    defer allocator.free(dir);
    const path = std.fmt.allocPrint(allocator, "{s}{s}", .{ dir, connections_filename }) catch return error.OutOfMemory;
    return path;
}

fn readConnectionsFile(allocator: std.mem.Allocator) ConnectionFileError![]u8 {
    if (getConfigFilePath(allocator)) |config_path| {
        defer allocator.free(config_path);
        if (std.fs.cwd().openFile(config_path, .{})) |file| {
            defer file.close();
            const data = file.readToEndAlloc(allocator, 1024 * 1024) catch return error.ReadFailed;
            return data;
        } else |_| {}
    } else |_| {}

    // Backward compat: fall back to CWD for pre-XDG installs
    if (std.fs.cwd().openFile(connections_filename, .{})) |file| {
        defer file.close();
        const data = file.readToEndAlloc(allocator, 1024 * 1024) catch return error.ReadFailed;
        return data;
    } else |_| {}

    return allocator.dupe(u8, "{\"connections\":[]}") catch return error.OutOfMemory;
}

fn writeConnectionsFile(allocator: std.mem.Allocator, data: []const u8) ConnectionFileError!void {
    const config_path = getConfigFilePath(allocator) catch {
        const file = std.fs.cwd().createFile(connections_filename, .{ .mode = 0o600 }) catch return error.WriteFailed;
        defer file.close();
        file.writeAll(data) catch return error.WriteFailed;
        return;
    };
    defer allocator.free(config_path);
    const file = std.fs.cwd().createFile(config_path, .{ .mode = 0o600 }) catch return error.WriteFailed;
    defer file.close();
    file.writeAll(data) catch return error.WriteFailed;
}

fn findMaxConnectionId(file_content: []const u8) u64 {
    var max_id: u64 = 0;
    var pos: usize = 0;
    while (pos < file_content.len) {
        const id_key = "\"id\":";
        const idx = std.mem.indexOfPos(u8, file_content, pos, id_key) orelse break;
        var vp = idx + id_key.len;
        while (vp < file_content.len and file_content[vp] == ' ') vp += 1;
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

pub fn handleConnect(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    const allocator = state.allocator;
    const arena = res.arena;

    const body = req.body() orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };

    const conninfo_str = utils.extractJsonField(arena, body, "conninfo") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing conninfo field\"}");
        return;
    };

    const conninfo_z = allocator.dupeZ(u8, conninfo_str) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var pg_conn = postgres.PgConnection.connectVerbose(conninfo_z) catch {
        allocator.free(conninfo_z);
        sendJsonResponse(res, "{\"error\":\"Failed to connect to PostgreSQL. libpq returned null.\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!pg_conn.isOk()) {
        var err_buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"Connection failed: ") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        allocator.free(conninfo_z);
        sendJsonResponse(res, fbs.getWritten());
        return;
    }

    var schema = pg_conn.fetchSchema(allocator) catch {
        allocator.free(conninfo_z);
        sendJsonResponse(res, "{\"error\":\"Failed to fetch database schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        allocator.free(conninfo_z);
        sendJsonResponse(res, "{\"error\":\"Failed to format schema\"}");
        return;
    };

    var enhanced = pg_conn.fetchEnhancedSchema(allocator) catch null;

    freeSchemaState(state);
    state.conninfo_z = conninfo_z;
    if (state.last_conninfo) |old_ci| allocator.free(@constCast(old_ci));
    state.last_conninfo = allocator.dupe(u8, conninfo_str) catch null;
    state.schema_text = schema_text;
    state.schema_tables = schema.tables;
    state.schema_arena = schema.arena;
    if (enhanced) |*es| {
        state.enhanced_schema = es.tables;
        state.enhanced_arena = es.arena;
    }

    var json_buf = std.ArrayList(u8){};
    const w = json_buf.writer(arena);
    w.writeAll("{\"status\":\"connected\",\"schema\":\"") catch {
        sendJsonResponse(res, "{\"status\":\"connected\"}");
        return;
    };
    utils.writeJsonEscaped(w, schema_text) catch return;
    var n_tables: usize = 0;
    if (state.schema_tables) |tables| n_tables = tables.len;
    w.print("\",\"tables\":{d}}}", .{n_tables}) catch return;
    sendJsonResponse(res, json_buf.items);
}

pub fn handleSchema(handler: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;
    const arena = res.arena;

    // Fall back to cached data if re-fetch fails
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

        // Preserve conninfo_z -- we're reusing the existing connection
        const saved_conninfo = state.conninfo_z;
        state.conninfo_z = null;
        freeSchemaState(state);
        state.conninfo_z = saved_conninfo;

        state.schema_text = new_text;
        state.schema_tables = schema.tables;
        state.schema_arena = schema.arena;
        if (enhanced) |*es| {
            state.enhanced_schema = es.tables;
            state.enhanced_arena = es.arena;
        }
    }

    const tables = state.schema_tables orelse {
        sendJsonResponse(res, "{\"tables\":[]}");
        return;
    };

    var json_buf = std.ArrayList(u8){};
    const w = json_buf.writer(arena);

    try w.writeAll("{\"tables\":[");

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

    sendJsonResponse(res, json_buf.items);
}

pub fn handleReconnect(handler: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    const allocator = state.allocator;
    const arena = res.arena;

    const last_ci = state.last_conninfo orelse {
        sendJsonResponse(res, "{\"error\":\"No previous connection to reconnect to\"}");
        return;
    };

    const conninfo_z = allocator.dupeZ(u8, last_ci) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var pg_conn = postgres.PgConnection.connectVerbose(conninfo_z) catch {
        allocator.free(conninfo_z);
        sendJsonResponse(res, "{\"error\":\"Failed to reconnect. libpq returned null.\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!pg_conn.isOk()) {
        var err_buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"Reconnect failed: ") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        allocator.free(conninfo_z);
        sendJsonResponse(res, fbs.getWritten());
        return;
    }

    var schema = pg_conn.fetchSchema(allocator) catch {
        allocator.free(conninfo_z);
        sendJsonResponse(res, "{\"error\":\"Reconnected but failed to fetch schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        allocator.free(conninfo_z);
        sendJsonResponse(res, "{\"error\":\"Reconnected but failed to format schema\"}");
        return;
    };

    var enhanced = pg_conn.fetchEnhancedSchema(allocator) catch null;

    freeSchemaState(state);
    state.conninfo_z = conninfo_z;
    state.schema_text = schema_text;
    state.schema_tables = schema.tables;
    state.schema_arena = schema.arena;
    if (enhanced) |*es| {
        state.enhanced_schema = es.tables;
        state.enhanced_arena = es.arena;
    }

    var json_buf = std.ArrayList(u8){};
    const w = json_buf.writer(arena);
    w.writeAll("{\"ok\":true,\"schema\":\"") catch {
        sendJsonResponse(res, "{\"ok\":true}");
        return;
    };
    utils.writeJsonEscaped(w, schema_text) catch return;
    var n_tables: usize = 0;
    if (state.schema_tables) |tables| n_tables = tables.len;
    w.print("\",\"tables\":{d}}}", .{n_tables}) catch return;
    sendJsonResponse(res, json_buf.items);
}

pub fn handleHealthCheck(handler: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"connected\":false}");
        return;
    }

    const allocator = res.arena;
    const conninfo_z = state.conninfo_z.?;

    const start_time = std.time.milliTimestamp();
    var pg_conn = postgres.PgConnection.connect(conninfo_z) catch {
        sendJsonResponse(res, "{\"connected\":false}");
        return;
    };
    defer pg_conn.deinit();

    const sql: [*:0]const u8 = "SELECT 1";
    var pg_result = pg_conn.runQuery(allocator, sql) catch {
        sendJsonResponse(res, "{\"connected\":false}");
        return;
    };
    defer pg_result.deinit();

    const end_time = std.time.milliTimestamp();
    const latency: u64 = @intCast(@max(0, end_time - start_time));

    var resp_buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"connected\":true,\"latency_ms\":{d}}}", .{latency}) catch {
        sendJsonResponse(res, "{\"connected\":true}");
        return;
    };
    sendJsonResponse(res, resp);
}

pub fn handleReadOnlyToggle(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    const arena = res.arena;

    const body = req.body() orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };
    const enabled_str = utils.extractJsonField(arena, body, "enabled") orelse {
        state.flags.read_only = !state.flags.read_only;
        var resp_buf: [64]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.flags.read_only) "true" else "false"}) catch return;
        sendJsonResponse(res, resp);
        return;
    };
    state.flags.read_only = std.mem.eql(u8, enabled_str, "true");
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.flags.read_only) "true" else "false"}) catch return;
    sendJsonResponse(res, resp);
}

pub fn handleReadOnlyGet(handler: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.flags.read_only) "true" else "false"}) catch return;
    sendJsonResponse(res, resp);
}

pub fn handleGetConnections(_: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const arena = res.arena;
    const data = readConnectionsFile(arena) catch {
        sendJsonResponse(res, "{\"connections\":[]}");
        return;
    };
    sendJsonResponse(res, data);
}

pub fn handlePostConnection(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    const allocator = res.arena;

    const body = req.body() orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };

    const name = utils.extractJsonField(allocator, body, "name") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing name field\"}");
        return;
    };
    const conninfo = utils.extractJsonField(allocator, body, "conninfo") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing conninfo field\"}");
        return;
    };
    const color = utils.extractJsonField(allocator, body, "color") orelse "gray";

    const existing = readConnectionsFile(allocator) catch {
        const entry = formatConnectionJson(allocator, 1, name, conninfo, color) catch {
            sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
            return;
        };

        var new_file = std.ArrayList(u8){};
        const nw = new_file.writer(allocator);
        nw.writeAll("{\"connections\":[") catch {
            sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
            return;
        };
        nw.writeAll(entry) catch return;
        nw.writeAll("]}") catch return;
        writeConnectionsFile(allocator, new_file.items) catch {
            sendJsonError(res, 500, "{\"error\":\"Failed to write connections file\"}");
            return;
        };
        state.next_connection_id = 2;
        sendJsonResponse(res, "{\"id\":1}");
        return;
    };

    const max_id = findMaxConnectionId(existing);
    const new_id = @max(max_id + 1, state.next_connection_id);
    state.next_connection_id = new_id + 1;

    const entry = formatConnectionJson(allocator, new_id, name, conninfo, color) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var new_file = std.ArrayList(u8){};
    const nw = new_file.writer(allocator);

    const close_bracket = std.mem.lastIndexOfScalar(u8, existing, ']') orelse {
        // Malformed file -- rewrite from scratch
        nw.writeAll("{\"connections\":[") catch {
            sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
            return;
        };
        nw.writeAll(entry) catch return;
        nw.writeAll("]}") catch return;
        writeConnectionsFile(allocator, new_file.items) catch {
            sendJsonError(res, 500, "{\"error\":\"Failed to write connections file\"}");
            return;
        };
        var id_buf: [64]u8 = undefined;
        const id_resp = std.fmt.bufPrint(&id_buf, "{{\"id\":{d}}}", .{new_id}) catch {
            sendJsonResponse(res, "{\"id\":0}");
            return;
        };
        sendJsonResponse(res, id_resp);
        return;
    };

    const before_bracket = std.mem.trimRight(u8, existing[0..close_bracket], " \t\n\r");
    const needs_comma = before_bracket.len > 0 and before_bracket[before_bracket.len - 1] != '[';

    nw.writeAll(existing[0..close_bracket]) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };
    if (needs_comma) nw.writeByte(',') catch return;
    nw.writeAll(entry) catch return;
    nw.writeAll("]}") catch return;

    writeConnectionsFile(allocator, new_file.items) catch {
        sendJsonError(res, 500, "{\"error\":\"Failed to write connections file\"}");
        return;
    };

    var id_buf: [64]u8 = undefined;
    const id_resp = std.fmt.bufPrint(&id_buf, "{{\"id\":{d}}}", .{new_id}) catch {
        sendJsonResponse(res, "{\"id\":0}");
        return;
    };
    sendJsonResponse(res, id_resp);
}

pub fn handleDeleteConnection(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    _ = handler;
    const allocator = res.arena;

    const id_str = req.param("id") orelse {
        sendJsonError(res, 404, "{\"error\":\"Invalid path\"}");
        return;
    };
    const target_id = std.fmt.parseInt(u64, id_str, 10) catch {
        sendJsonError(res, 400, "{\"error\":\"Invalid connection ID\"}");
        return;
    };

    const existing = readConnectionsFile(allocator) catch {
        sendJsonError(res, 404, "{\"error\":\"No connections file\"}");
        return;
    };

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, existing, .{}) catch {
        sendJsonError(res, 500, "{\"error\":\"Malformed connections file\"}");
        return;
    };

    const conns_val = parsed.value.object.get("connections") orelse {
        sendJsonError(res, 500, "{\"error\":\"Malformed connections file\"}");
        return;
    };
    const conns_arr = switch (conns_val) {
        .array => |a| a,
        else => {
            sendJsonError(res, 500, "{\"error\":\"Malformed connections file\"}");
            return;
        },
    };

    var new_file = std.ArrayList(u8){};
    const nw = new_file.writer(allocator);
    nw.writeAll("{\"connections\":[") catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    var found = false;
    var first = true;
    for (conns_arr.items) |item| {
        const obj = switch (item) {
            .object => |o| o,
            else => continue,
        };
        const id_val = obj.get("id") orelse continue;
        const obj_id: u64 = switch (id_val) {
            .integer => |i| @intCast(@as(u64, @bitCast(@as(i64, i)))),
            else => continue,
        };
        if (obj_id == target_id) {
            found = true;
        } else {
            if (!first) nw.writeByte(',') catch return;
            const item_json = std.json.Stringify.valueAlloc(allocator, item, .{}) catch return;
            nw.writeAll(item_json) catch return;
            first = false;
        }
    }

    nw.writeAll("]}") catch return;

    if (!found) {
        sendJsonError(res, 404, "{\"error\":\"Connection not found\"}");
        return;
    }

    writeConnectionsFile(allocator, new_file.items) catch {
        sendJsonError(res, 500, "{\"error\":\"Failed to write connections file\"}");
        return;
    };

    sendJsonResponse(res, "{\"ok\":true}");
}

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
    const ends_slash = std.mem.endsWith(u8, path, "lux/");
    const ends_backslash = std.mem.endsWith(u8, path, "lux\\");
    try std.testing.expect(ends_slash or ends_backslash);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
