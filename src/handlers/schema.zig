const std = @import("std");
const builtin = @import("builtin");
const httpz = @import("httpz");
const pg = @import("pg");
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

const sendJsonResponse = web.sendJsonResponse;
const sendJsonError = web.sendJsonError;

fn formatConnectionJson(allocator: std.mem.Allocator, id: u64, name: []const u8, conninfo: []const u8, color: []const u8) ConnectionFileError![]u8 {
    const entry = .{ .id = id, .name = name, .conninfo = conninfo, .color = color };
    return std.json.Stringify.valueAlloc(allocator, entry, .{}) catch return error.OutOfMemory;
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
    if (state.pool) |p| p.deinit();
    state.pool = null;
    if (state.conninfo_uri) |old| allocator.free(@constCast(old));
    state.conninfo_uri = null;
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

const ConnectionEntry = struct { id: u64 = 0, name: []const u8 = "", conninfo: []const u8 = "", color: []const u8 = "" };
const ConnectionsFile = struct { connections: []const ConnectionEntry = &.{} };

fn findMaxConnectionId(allocator: std.mem.Allocator, file_content: []const u8) u64 {
    const parsed = std.json.parseFromSlice(ConnectionsFile, allocator, file_content, .{ .ignore_unknown_fields = true }) catch return 0;
    defer parsed.deinit();
    var max_id: u64 = 0;
    for (parsed.value.connections) |conn| {
        if (conn.id > max_id) max_id = conn.id;
    }
    return max_id;
}

pub fn handleConnect(handler: *web.Handler, req: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    const allocator = state.allocator;
    const arena = res.arena;

    const obj = (try req.jsonObject()) orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };

    const conninfo_str = utils.getJsonString(obj, "conninfo") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing conninfo field\"}");
        return;
    };

    var pool = postgres.initPool(allocator, conninfo_str) catch {
        sendJsonResponse(res, "{\"error\":\"Failed to connect to PostgreSQL.\"}");
        return;
    };

    // Verify the pool works by running a test query
    var test_result = postgres.runQuery(pool, arena, "SELECT 1") catch {
        pool.deinit();
        sendJsonResponse(res, "{\"error\":\"Connection test failed. Check your connection string.\"}");
        return;
    };
    test_result.deinit();

    var schema = postgres.fetchSchema(pool, allocator) catch {
        pool.deinit();
        sendJsonResponse(res, "{\"error\":\"Failed to fetch database schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        pool.deinit();
        sendJsonResponse(res, "{\"error\":\"Failed to format schema\"}");
        return;
    };

    var enhanced = postgres.fetchEnhancedSchema(pool, allocator) catch null;

    freeSchemaState(state);
    state.pool = pool;
    state.conninfo_uri = allocator.dupe(u8, conninfo_str) catch null;
    if (state.last_conninfo) |old_ci| allocator.free(@constCast(old_ci));
    state.last_conninfo = allocator.dupe(u8, conninfo_str) catch null;
    state.schema_text = schema_text;
    state.schema_tables = schema.tables;
    state.schema_arena = schema.arena;
    if (enhanced) |*es| {
        state.enhanced_schema = es.tables;
        state.enhanced_arena = es.arena;
    }

    var jw = utils.JsonWriter.init(arena);
    const s = jw.writer();
    s.beginObject() catch {
        sendJsonResponse(res, "{\"status\":\"connected\"}");
        return;
    };
    s.objectField("status") catch return;
    s.write("connected") catch return;
    s.objectField("schema") catch return;
    s.write(schema_text) catch return;
    var n_tables: usize = 0;
    if (state.schema_tables) |tables| n_tables = tables.len;
    s.objectField("tables") catch return;
    s.write(n_tables) catch return;
    s.endObject() catch return;
    sendJsonResponse(res, jw.toOwnedSlice() catch return);
}

pub fn handleSchema(handler: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;
    const arena = res.arena;
    const pool = state.pool.?;

    // Fall back to cached data if re-fetch fails
    refresh: {
        var schema = postgres.fetchSchema(pool, allocator) catch break :refresh;
        const new_text = schema.format(allocator) catch {
            schema.deinit();
            break :refresh;
        };
        var enhanced = postgres.fetchEnhancedSchema(pool, allocator) catch null;

        // Preserve pool -- we're reusing the existing connection
        const saved_pool = state.pool;
        const saved_uri = state.conninfo_uri;
        state.pool = null;
        state.conninfo_uri = null;
        freeSchemaState(state);
        state.pool = saved_pool;
        state.conninfo_uri = saved_uri;

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

    var jw = utils.JsonWriter.init(arena);
    const s = jw.writer();

    try s.beginObject();
    try s.objectField("tables");
    try s.beginArray();

    if (state.enhanced_schema) |etables| {
        for (etables) |etable| {
            try s.beginObject();
            try s.objectField("name");
            try s.write(etable.name);
            try s.objectField("has_primary_key");
            try s.write(etable.has_primary_key);
            try s.objectField("primary_key_columns");
            try s.write(etable.primary_key_columns);
            try s.objectField("columns");
            try s.beginArray();
            for (etable.columns) |col| {
                try s.beginObject();
                try s.objectField("name");
                try s.write(col.name);
                try s.objectField("type");
                try s.write(col.data_type);
                try s.objectField("is_primary_key");
                try s.write(col.is_primary_key);
                try s.objectField("is_nullable");
                try s.write(col.is_nullable);
                if (col.column_default) |def| {
                    try s.objectField("column_default");
                    try s.write(def);
                }
                if (col.fk_target_table) |fkt| {
                    try s.objectField("fk_target_table");
                    try s.write(fkt);
                }
                if (col.fk_target_column) |fkc| {
                    try s.objectField("fk_target_column");
                    try s.write(fkc);
                }
                if (col.enum_values) |vals| {
                    try s.objectField("enum_values");
                    try s.write(vals);
                }
                try s.endObject();
            }
            try s.endArray();
            try s.endObject();
        }
    } else {
        for (tables) |table| {
            try s.beginObject();
            try s.objectField("name");
            try s.write(table.name);
            try s.objectField("columns");
            try s.beginArray();
            for (table.columns) |col| {
                try s.beginObject();
                try s.objectField("name");
                try s.write(col.name);
                try s.objectField("type");
                try s.write(col.data_type);
                try s.endObject();
            }
            try s.endArray();
            try s.endObject();
        }
    }
    try s.endArray();
    try s.endObject();

    sendJsonResponse(res, try jw.toOwnedSlice());
}

pub fn handleReconnect(handler: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    const allocator = state.allocator;
    const arena = res.arena;

    const last_ci = state.last_conninfo orelse {
        sendJsonResponse(res, "{\"error\":\"No previous connection to reconnect to\"}");
        return;
    };

    var pool = postgres.initPool(allocator, last_ci) catch {
        sendJsonResponse(res, "{\"error\":\"Failed to reconnect.\"}");
        return;
    };

    var schema = postgres.fetchSchema(pool, allocator) catch {
        pool.deinit();
        sendJsonResponse(res, "{\"error\":\"Reconnected but failed to fetch schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        pool.deinit();
        sendJsonResponse(res, "{\"error\":\"Reconnected but failed to format schema\"}");
        return;
    };

    var enhanced = postgres.fetchEnhancedSchema(pool, allocator) catch null;

    freeSchemaState(state);
    state.pool = pool;
    state.conninfo_uri = allocator.dupe(u8, last_ci) catch null;
    state.schema_text = schema_text;
    state.schema_tables = schema.tables;
    state.schema_arena = schema.arena;
    if (enhanced) |*es| {
        state.enhanced_schema = es.tables;
        state.enhanced_arena = es.arena;
    }

    var jw = utils.JsonWriter.init(arena);
    const s = jw.writer();
    s.beginObject() catch {
        sendJsonResponse(res, "{\"ok\":true}");
        return;
    };
    s.objectField("ok") catch return;
    s.write(true) catch return;
    s.objectField("schema") catch return;
    s.write(schema_text) catch return;
    var n_tables: usize = 0;
    if (state.schema_tables) |tbls| n_tables = tbls.len;
    s.objectField("tables") catch return;
    s.write(n_tables) catch return;
    s.endObject() catch return;
    sendJsonResponse(res, jw.toOwnedSlice() catch return);
}

pub fn handleHealthCheck(handler: *web.Handler, _: *httpz.Request, res: *httpz.Response) !void {
    const state = handler.state;
    if (!state.hasDbConnection()) {
        sendJsonResponse(res, "{\"connected\":false}");
        return;
    }

    const allocator = res.arena;
    const pool = state.pool.?;

    const start_time = std.time.milliTimestamp();
    var pg_result = postgres.runQuery(pool, allocator, "SELECT 1") catch {
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

    const obj = (try req.jsonObject()) orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };
    const enabled_str = utils.getJsonString(obj, "enabled") orelse {
        state.flags.read_only = !state.flags.read_only;
        setReadOnlyOnPool(state);
        var resp_buf: [64]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.flags.read_only) "true" else "false"}) catch return;
        sendJsonResponse(res, resp);
        return;
    };
    state.flags.read_only = std.mem.eql(u8, enabled_str, "true");
    setReadOnlyOnPool(state);
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.flags.read_only) "true" else "false"}) catch return;
    sendJsonResponse(res, resp);
}

/// SET default_transaction_read_only on ALL pooled connections.
/// Acquires every connection in the pool, applies the setting, then releases them all.
fn setReadOnlyOnPool(state: *web.ServerState) void {
    const pool = state.pool orelse return;
    const set_sql = if (state.flags.read_only)
        "SET default_transaction_read_only = on"
    else
        "SET default_transaction_read_only = off";

    var conns: [postgres.pool_size]*pg.Conn = undefined;
    var count: usize = 0;

    while (count < postgres.pool_size) {
        const conn = pool.acquire() catch break;
        _ = conn.queryOpts(set_sql, .{}, .{}) catch |err| {
            log.warn("SET read_only on conn {d} failed: {s}", .{ count, @errorName(err) });
        };
        conns[count] = conn;
        count += 1;
    }

    var i = count;
    while (i > 0) {
        i -= 1;
        conns[i].release();
    }
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

    const obj = (try req.jsonObject()) orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing request body\"}");
        return;
    };

    const name = utils.getJsonString(obj, "name") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing name field\"}");
        return;
    };
    const conninfo = utils.getJsonString(obj, "conninfo") orelse {
        sendJsonError(res, 400, "{\"error\":\"Missing conninfo field\"}");
        return;
    };
    const color = utils.getJsonString(obj, "color") orelse "gray";

    const existing = readConnectionsFile(allocator) catch "{}";

    const parsed = std.json.parseFromSlice(std.json.Value, allocator, existing, .{}) catch {
        sendJsonError(res, 500, "{\"error\":\"Malformed connections file\"}");
        return;
    };

    const conns_val = switch (parsed.value) {
        .object => |o| o.get("connections"),
        else => null,
    };
    var conns_arr = if (conns_val) |cv| switch (cv) {
        .array => |a| a,
        else => std.json.Array.init(allocator),
    } else std.json.Array.init(allocator);

    var max_id: u64 = 0;
    for (conns_arr.items) |item| {
        const item_obj = switch (item) {
            .object => |o| o,
            else => continue,
        };
        const id_val = item_obj.get("id") orelse continue;
        const obj_id: u64 = switch (id_val) {
            .integer => |i| @intCast(@as(u64, @bitCast(@as(i64, i)))),
            else => continue,
        };
        if (obj_id > max_id) max_id = obj_id;
    }

    const new_id = @max(max_id + 1, state.next_connection_id);
    state.next_connection_id = new_id + 1;

    // Build new entry as a std.json.Value object
    var new_entry = std.json.ObjectMap.init(allocator);
    new_entry.put("id", .{ .integer = @intCast(new_id) }) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };
    new_entry.put("name", .{ .string = name }) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };
    new_entry.put("conninfo", .{ .string = conninfo }) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };
    new_entry.put("color", .{ .string = color }) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    conns_arr.append(.{ .object = new_entry }) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    // Re-serialize the whole file
    var out_obj = std.json.ObjectMap.init(allocator);
    out_obj.put("connections", .{ .array = conns_arr }) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };
    const serialized = std.json.Stringify.valueAlloc(allocator, std.json.Value{ .object = out_obj }, .{}) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    writeConnectionsFile(allocator, serialized) catch {
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

    const conns_val = switch (parsed.value) {
        .object => |o| o.get("connections"),
        else => null,
    };
    var conns_arr = if (conns_val) |cv| switch (cv) {
        .array => |a| a,
        else => {
            sendJsonError(res, 500, "{\"error\":\"Malformed connections file\"}");
            return;
        },
    } else {
        sendJsonError(res, 500, "{\"error\":\"Malformed connections file\"}");
        return;
    };

    var found = false;
    var i: usize = 0;
    while (i < conns_arr.items.len) {
        const item = conns_arr.items[i];
        const obj = switch (item) {
            .object => |o| o,
            else => {
                i += 1;
                continue;
            },
        };
        const id_val = obj.get("id") orelse {
            i += 1;
            continue;
        };
        const obj_id: u64 = switch (id_val) {
            .integer => |iv| @intCast(@as(u64, @bitCast(@as(i64, iv)))),
            else => {
                i += 1;
                continue;
            },
        };
        if (obj_id == target_id) {
            _ = conns_arr.orderedRemove(i);
            found = true;
        } else {
            i += 1;
        }
    }

    if (!found) {
        sendJsonError(res, 404, "{\"error\":\"Connection not found\"}");
        return;
    }

    var out_obj = std.json.ObjectMap.init(allocator);
    out_obj.put("connections", .{ .array = conns_arr }) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };
    const serialized = std.json.Stringify.valueAlloc(allocator, std.json.Value{ .object = out_obj }, .{}) catch {
        sendJsonError(res, 500, "{\"error\":\"Out of memory\"}");
        return;
    };

    writeConnectionsFile(allocator, serialized) catch {
        sendJsonError(res, 500, "{\"error\":\"Failed to write connections file\"}");
        return;
    };

    sendJsonResponse(res, "{\"ok\":true}");
}

test "formatConnectionJson: roundtrips through std.json" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 1, "My DB", "pg://localhost", "green");
    defer allocator.free(json);
    const parsed = try std.json.parseFromSlice(ConnectionEntry, allocator, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed.value.id);
    try std.testing.expectEqualStrings("My DB", parsed.value.name);
}

test "findMaxConnectionId: finds max across multiple entries" {
    const result = findMaxConnectionId(std.testing.allocator, "{\"connections\":[{\"id\":3},{\"id\":7},{\"id\":2}]}");
    try std.testing.expectEqual(@as(u64, 7), result);
}

test "findMaxConnectionId: empty connections returns zero" {
    const result = findMaxConnectionId(std.testing.allocator, "{\"connections\":[]}");
    try std.testing.expectEqual(@as(u64, 0), result);
}

test "findMaxConnectionId: invalid json returns zero" {
    const result = findMaxConnectionId(std.testing.allocator, "garbage");
    try std.testing.expectEqual(@as(u64, 0), result);
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
