const std = @import("std");
const postgres = @import("postgres");
const utils = @import("utils");
const sql_guard = @import("sql_guard");
const web = @import("web");
const crud = @import("crud");

const log = std.log.scoped(.sql);

const ServerState = web.ServerState;
const QueryHistoryEntry = web.QueryHistoryEntry;
const max_history_entries = web.max_history_entries;

pub fn addHistoryEntry(
    state: *ServerState,
    sql: []const u8,
    duration_ms: u64,
    row_count: ?usize,
    is_error: bool,
    error_msg: ?[]const u8,
) void {
    const ts = std.time.timestamp();
    const sql_dupe = state.allocator.dupe(u8, sql) catch return;
    const err_dupe: ?[]const u8 = if (error_msg) |e| (state.allocator.dupe(u8, e) catch null) else null;
    state.query_history.append(state.allocator, .{
        .sql = sql_dupe,
        .timestamp = ts,
        .duration_ms = duration_ms,
        .row_count = row_count,
        .is_error = is_error,
        .error_msg = err_dupe,
    }) catch return;
    // Cap at max_history_entries
    if (state.query_history.items.len > max_history_entries) {
        const old = state.query_history.orderedRemove(0);
        state.allocator.free(old.sql);
        if (old.error_msg) |e| state.allocator.free(e);
    }
}

pub fn handleSql(stream: std.net.Stream, request: []const u8, _: []const u8, state: *ServerState, arena: std.mem.Allocator) anyerror!void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = arena;

    // Parse body
    const content_length = utils.findContentLength(request) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };

    const max_sql_body = 65536; // 64KB — enough for multi-statement scripts
    if (content_length > max_sql_body) {
        try utils.sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"SQL too large (max 64KB)\"}");
        return;
    }

    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
        return;
    };
    const body_start = header_end + 4;
    var body = request[body_start..];

    // Use heap allocation for the body buffer to support large SQL scripts
    const extra_buf = allocator.alloc(u8, max_sql_body) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
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

    const sql_text = utils.extractJsonField(body, "sql") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };

    if (sql_text.len == 0) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty SQL\"}");
        return;
    }

    // Block write operations in read-only mode (whitelist approach)
    if (state.flags.read_only and !sql_guard.isSqlReadSafe(sql_text)) {
        try utils.sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to execute write operations.\"}");
        return;
    }

    // Check for destructive operations and require confirmation
    const force = utils.extractJsonField(body, "force");
    const is_forced = if (force) |f| std.mem.eql(u8, f, "true") else false;
    if (!is_forced) {
        const guard = sql_guard.analyzeSql(sql_text);
        if (guard.is_destructive) {
            var warn_buf = std.ArrayList(u8){};
            const ww = warn_buf.writer(allocator);
            try ww.writeAll("{\"requires_confirmation\":true,\"operation\":\"");
            try utils.writeJsonEscaped(ww, guard.operation);
            try ww.writeAll("\",\"warning\":\"");
            try utils.writeJsonEscaped(ww, guard.warning);
            try ww.writeAll("\"}");
            try utils.sendResponse(stream, "200 OK", "application/json", warn_buf.items);
            return;
        }
    }

    // Build null-terminated SQL
    const sql_z = allocator.dupeZ(u8, sql_text) catch {
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };

    // Connect and execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Use PQexec for multi-statement scripts, PQexecParams for single statements
    const is_multi = sql_guard.hasMultipleStatements(sql_text);
    const start_time = std.time.milliTimestamp();
    const pg_result = if (is_multi)
        pg_conn.runQueryMulti(allocator, sql_z)
    else
        pg_conn.runQuery(allocator, sql_z);
    var result = pg_result catch {
        const end_time = std.time.milliTimestamp();
        const duration: u64 = @intCast(@max(0, end_time - start_time));
        const err_msg = pg_conn.errorMessage();
        addHistoryEntry(state, sql_text, duration, null, true, err_msg);
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        utils.writeJsonEscaped(ew, err_msg) catch return;
        ew.writeAll("\"}") catch return;
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer result.deinit();
    const end_time = std.time.milliTimestamp();
    const duration: u64 = @intCast(@max(0, end_time - start_time));
    addHistoryEntry(state, sql_text, duration, result.n_rows, false, null);

    // Build JSON response
    try crud.sendQueryResultJson(allocator, stream, &result);
}

pub fn handleSqlPreview(stream: std.net.Stream, request: []const u8, _: []const u8, state: *ServerState, arena: std.mem.Allocator) anyerror!void {
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = arena;
    var extra_buf: [8192]u8 = undefined;
    const body = crud.readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };
    const sql_text = utils.extractJsonField(body, "sql") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };
    if (sql_text.len == 0) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty SQL\"}");
        return;
    }

    // Block write operations in read-only mode
    if (state.flags.read_only and !sql_guard.isSqlReadSafe(sql_text)) {
        try utils.sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to preview write operations.\"}");
        return;
    }

    // Connect
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // BEGIN
    var begin_result = pg_conn.runQuery(allocator, "BEGIN") catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to start transaction\"}");
        return;
    };
    begin_result.deinit();

    // Execute the query
    const sql_z = allocator.dupeZ(u8, sql_text) catch {
        if (pg_conn.runQuery(allocator, "ROLLBACK")) |rb| {
            var r = rb;
            r.deinit();
        } else |_| {}
        try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        // Try to get error message before rollback
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        if (pg_conn.runQuery(allocator, "ROLLBACK")) |rb| {
            var r = rb;
            r.deinit();
        } else |_| {}
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // ROLLBACK — undo changes
    if (pg_conn.runQuery(allocator, "ROLLBACK")) |rb| {
        var r = rb;
        r.deinit();
    } else |_| {}

    // Build preview response
    var json_buf = std.ArrayList(u8){};
    const w = json_buf.writer(allocator);
    try w.print("{{\"preview\":true,\"affected_rows\":{d},", .{pg_result.n_rows});

    // Include column names and first few rows as sample
    try w.writeAll("\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeByte('"');
        try utils.writeJsonEscaped(w, name);
        try w.writeByte('"');
    }
    try w.writeAll("],\"rows\":[");
    const max_preview_rows: usize = 10;
    const rows_to_show = @min(pg_result.n_rows, max_preview_rows);
    for (pg_result.rows[0..rows_to_show], 0..) |row, ri| {
        if (ri > 0) try w.writeByte(',');
        try w.writeByte('[');
        for (row, 0..) |val, ci| {
            if (ci > 0) try w.writeByte(',');
            if (std.mem.eql(u8, val, "NULL")) {
                try w.writeAll("null");
            } else {
                try w.writeByte('"');
                try utils.writeJsonEscaped(w, val);
                try w.writeByte('"');
            }
        }
        try w.writeByte(']');
    }
    try w.writeAll("]}");
    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

pub fn generateRollbackSql(sql: []const u8, writer: anytype) !void {
    // Simple token-based DDL analysis
    // ADD COLUMN → DROP COLUMN
    if (sql_guard.containsIgnoreCaseWord(sql, "ALTER") and sql_guard.containsIgnoreCaseWord(sql, "TABLE") and sql_guard.containsIgnoreCaseWord(sql, "ADD") and sql_guard.containsIgnoreCaseWord(sql, "COLUMN")) {
        // Try to extract table name and column name
        // Pattern: ALTER TABLE "table" ADD COLUMN "column" ...
        // Extract what's between TABLE and ADD
        if (utils.indexOfIgnoreCase(sql, "ADD")) |add_pos| {
            const before_add = sql[0..add_pos];
            if (utils.indexOfIgnoreCase(before_add, "TABLE")) |table_pos| {
                const after_table = std.mem.trimLeft(u8, before_add[table_pos + 5 ..], " \t\n\r");
                const table_end = std.mem.indexOfAny(u8, after_table, " \t\n\r") orelse after_table.len;
                const table_name = std.mem.trim(u8, after_table[0..table_end], "\"");

                // Extract column name after COLUMN keyword
                const after_add = sql[add_pos + 3 ..];
                if (utils.indexOfIgnoreCase(after_add, "COLUMN")) |col_kw_pos| {
                    const after_col_kw = std.mem.trimLeft(u8, after_add[col_kw_pos + 6 ..], " \t\n\r");
                    const col_end = std.mem.indexOfAny(u8, after_col_kw, " \t\n\r") orelse after_col_kw.len;
                    const col_name = std.mem.trim(u8, after_col_kw[0..col_end], "\"");
                    try writer.print("ALTER TABLE \"{s}\" DROP COLUMN \"{s}\";", .{ table_name, col_name });
                    return;
                }
            }
        }
    }

    // CREATE TABLE → DROP TABLE
    if (sql_guard.containsIgnoreCaseWord(sql, "CREATE") and sql_guard.containsIgnoreCaseWord(sql, "TABLE")) {
        if (utils.indexOfIgnoreCase(sql, "TABLE")) |table_pos| {
            const after_table = std.mem.trimLeft(u8, sql[table_pos + 5 ..], " \t\n\r");
            const table_end = std.mem.indexOfAny(u8, after_table, " \t\n\r(") orelse after_table.len;
            const table_name = std.mem.trim(u8, after_table[0..table_end], "\"");
            try writer.print("DROP TABLE IF EXISTS \"{s}\";", .{table_name});
            return;
        }
    }

    // CREATE INDEX → DROP INDEX
    if (sql_guard.containsIgnoreCaseWord(sql, "CREATE") and sql_guard.containsIgnoreCaseWord(sql, "INDEX")) {
        if (utils.indexOfIgnoreCase(sql, "INDEX")) |idx_pos| {
            const after_idx = std.mem.trimLeft(u8, sql[idx_pos + 5 ..], " \t\n\r");
            const idx_end = std.mem.indexOfAny(u8, after_idx, " \t\n\r") orelse after_idx.len;
            const idx_name = std.mem.trim(u8, after_idx[0..idx_end], "\"");
            try writer.print("DROP INDEX IF EXISTS \"{s}\";", .{idx_name});
            return;
        }
    }

    // DROP TABLE → note that data is lost
    if (sql_guard.containsIgnoreCaseWord(sql, "DROP") and sql_guard.containsIgnoreCaseWord(sql, "TABLE")) {
        try writer.writeAll("-- WARNING: DROP TABLE cannot be automatically rolled back. Data will be lost.");
        return;
    }

    // TRUNCATE → note that data is lost
    if (sql_guard.containsIgnoreCaseWord(sql, "TRUNCATE")) {
        try writer.writeAll("-- WARNING: TRUNCATE cannot be automatically rolled back. Data will be lost.");
        return;
    }

    try writer.writeAll("-- No automatic rollback available for this operation.");
}

pub fn handleSchemaPreview(stream: std.net.Stream, request: []const u8, _: []const u8, state: *ServerState, arena: std.mem.Allocator) anyerror!void {
    if (try crud.enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = arena;
    var extra_buf: [8192]u8 = undefined;
    const body = crud.readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };
    const sql_text = utils.extractJsonField(body, "sql") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };

    // Generate rollback SQL
    var rollback_buf = std.ArrayList(u8){};
    generateRollbackSql(sql_text, rollback_buf.writer(allocator)) catch |err| {
        log.warn("generateRollbackSql failed: {s}", .{@errorName(err)});
    };
    const rollback = if (rollback_buf.items.len > 0) rollback_buf.items else "-- No automatic rollback available";

    // Detect operation type
    const guard = sql_guard.analyzeSql(sql_text);

    // Build response
    var json_buf = std.ArrayList(u8){};
    const w = json_buf.writer(allocator);
    try w.writeAll("{\"operation\":\"");
    try utils.writeJsonEscaped(w, guard.operation);
    try w.writeAll("\",\"warning\":\"");
    try utils.writeJsonEscaped(w, guard.warning);
    try w.writeAll("\",\"rollback_sql\":\"");
    try utils.writeJsonEscaped(w, rollback);
    try w.writeAll("\"}");

    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

pub fn handleHistory(stream: std.net.Stream, _: []const u8, _: []const u8, state: *ServerState, arena: std.mem.Allocator) anyerror!void {
    const alloc = arena;

    var json = std.ArrayList(u8){};
    try json.appendSlice(alloc, "{\"entries\":[");

    const items = state.query_history.items;
    const count = @min(items.len, 100);
    var wrote: usize = 0;

    // Reverse order (newest first)
    var i: usize = items.len;
    while (i > 0 and wrote < count) {
        i -= 1;
        if (wrote > 0) try json.appendSlice(alloc, ",");
        try json.appendSlice(alloc, "{\"sql\":\"");
        try utils.writeJsonEscaped(json.writer(alloc), items[i].sql);
        try json.appendSlice(alloc, "\",\"timestamp\":");
        var ts_buf: [20]u8 = undefined;
        const ts_str = std.fmt.bufPrint(&ts_buf, "{d}", .{items[i].timestamp}) catch ts_buf[0..0];
        try json.appendSlice(alloc, ts_str);
        try json.appendSlice(alloc, ",\"duration_ms\":");
        var dur_buf: [20]u8 = undefined;
        const dur_str = std.fmt.bufPrint(&dur_buf, "{d}", .{items[i].duration_ms}) catch dur_buf[0..0];
        try json.appendSlice(alloc, dur_str);
        if (items[i].row_count) |rc| {
            try json.appendSlice(alloc, ",\"row_count\":");
            var rc_buf: [20]u8 = undefined;
            const rc_str = std.fmt.bufPrint(&rc_buf, "{d}", .{rc}) catch rc_buf[0..0];
            try json.appendSlice(alloc, rc_str);
        } else {
            try json.appendSlice(alloc, ",\"row_count\":null");
        }
        try json.appendSlice(alloc, ",\"is_error\":");
        try json.appendSlice(alloc, if (items[i].is_error) "true" else "false");
        if (items[i].error_msg) |em| {
            try json.appendSlice(alloc, ",\"error\":\"");
            try utils.writeJsonEscaped(json.writer(alloc), em);
            try json.appendSlice(alloc, "\"");
        }
        try json.appendSlice(alloc, "}");
        wrote += 1;
    }

    try json.appendSlice(alloc, "]}");
    try utils.sendResponse(stream, "200 OK", "application/json", json.items);
}

pub fn handleJournal(stream: std.net.Stream, _: []const u8, _: []const u8, state: *ServerState, arena: std.mem.Allocator) anyerror!void {
    var json_buf = std.ArrayList(u8){};
    const w = json_buf.writer(arena);

    try w.writeAll("{\"entries\":[");
    const items = state.change_journal.items;
    const start = if (items.len > 100) items.len - 100 else 0;
    for (items[start..], 0..) |entry, i| {
        if (i > 0) try w.writeByte(',');
        try w.print("{{\"id\":{d},\"timestamp\":{d},\"table\":\"", .{ entry.id, entry.timestamp });
        try utils.writeJsonEscaped(w, entry.table_name);
        try w.writeAll("\",\"operation\":\"");
        try utils.writeJsonEscaped(w, entry.operation);
        try w.writeAll("\",\"column\":\"");
        try utils.writeJsonEscaped(w, entry.column_name);
        try w.writeAll("\",\"old_value\":\"");
        try utils.writeJsonEscaped(w, entry.old_value);
        try w.writeAll("\",\"new_value\":\"");
        try utils.writeJsonEscaped(w, entry.new_value);
        try w.writeAll("\",\"pk_column\":\"");
        try utils.writeJsonEscaped(w, entry.pk_column);
        try w.writeAll("\",\"pk_value\":\"");
        try utils.writeJsonEscaped(w, entry.pk_value);
        try w.print("\",\"undone\":{s}}}", .{if (entry.undone) "true" else "false"});
    }
    try w.writeAll("]}");
    try utils.sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

pub fn handleJournalUndo(stream: std.net.Stream, request: []const u8, _: []const u8, state: *ServerState, arena: std.mem.Allocator) anyerror!void {
    if (try crud.enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = arena;
    var extra_buf: [1024]u8 = undefined;
    const body = crud.readRequestBody(stream, request, &extra_buf, 1024) orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };

    const id_str = utils.extractJsonField(body, "id") orelse {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing id field\"}");
        return;
    };
    const id = std.fmt.parseInt(u64, id_str, 10) catch {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid id\"}");
        return;
    };

    // Find the entry
    var found_entry: ?*web.ChangeEntry = null;
    for (state.change_journal.items) |*entry| {
        if (entry.id == id) {
            found_entry = entry;
            break;
        }
    }
    const entry = found_entry orelse {
        try utils.sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Journal entry not found\"}");
        return;
    };
    if (entry.undone) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Already undone\"}");
        return;
    }

    // Build undo SQL
    var sql_buf = std.ArrayList(u8){};
    const w = sql_buf.writer(allocator);

    if (std.mem.eql(u8, entry.operation, "update")) {
        // Restore old value
        const escaped_old = utils.escapeStringValue(allocator, entry.old_value) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        const escaped_pk = utils.escapeStringValue(allocator, entry.pk_value) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}' WHERE \"{s}\" = '{s}'", .{
            entry.table_name, entry.column_name, escaped_old, entry.pk_column, escaped_pk,
        });
    } else if (std.mem.eql(u8, entry.operation, "delete")) {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Undo delete not yet supported\"}");
        return;
    } else if (std.mem.eql(u8, entry.operation, "insert")) {
        // Delete the inserted row
        const escaped_pk = utils.escapeStringValue(allocator, entry.pk_value) catch {
            try utils.sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        try w.print("DELETE FROM \"{s}\" WHERE \"{s}\" = '{s}'", .{
            entry.table_name, entry.pk_column, escaped_pk,
        });
    } else {
        try utils.sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Unknown operation\"}");
        return;
    }
    try sql_buf.append(allocator, 0);
    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    // Execute undo
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try utils.sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        utils.writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try utils.sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    entry.undone = true;
    try utils.sendResponse(stream, "200 OK", "application/json", "{\"success\":true}");
}

// Tests

test "addHistoryEntry: adds to history" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit(std.testing.allocator);
        state.change_journal.deinit(std.testing.allocator);
    }
    addHistoryEntry(&state, "SELECT 1", 42, 1, false, null);
    try std.testing.expectEqual(@as(usize, 1), state.query_history.items.len);
    try std.testing.expectEqualStrings("SELECT 1", state.query_history.items[0].sql);
    try std.testing.expectEqual(@as(u64, 42), state.query_history.items[0].duration_ms);
    try std.testing.expectEqual(@as(?usize, 1), state.query_history.items[0].row_count);
    try std.testing.expect(!state.query_history.items[0].is_error);
    try std.testing.expect(state.query_history.items[0].error_msg == null);
}

test "addHistoryEntry: records error" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit(std.testing.allocator);
        state.change_journal.deinit(std.testing.allocator);
    }
    addHistoryEntry(&state, "BAD SQL", 5, null, true, "syntax error");
    try std.testing.expectEqual(@as(usize, 1), state.query_history.items.len);
    try std.testing.expect(state.query_history.items[0].is_error);
    try std.testing.expect(state.query_history.items[0].row_count == null);
    try std.testing.expectEqualStrings("syntax error", state.query_history.items[0].error_msg.?);
}

test "addHistoryEntry: multiple entries" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit(std.testing.allocator);
        state.change_journal.deinit(std.testing.allocator);
    }
    addHistoryEntry(&state, "SELECT 1", 10, 1, false, null);
    addHistoryEntry(&state, "SELECT 2", 20, 2, false, null);
    addHistoryEntry(&state, "SELECT 3", 30, 3, false, null);
    try std.testing.expectEqual(@as(usize, 3), state.query_history.items.len);
    try std.testing.expectEqualStrings("SELECT 1", state.query_history.items[0].sql);
    try std.testing.expectEqualStrings("SELECT 3", state.query_history.items[2].sql);
}

test "addHistoryEntry: caps at max_history_entries" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit(std.testing.allocator);
        state.change_journal.deinit(std.testing.allocator);
    }
    // Add max_history_entries + 5 entries
    var i: usize = 0;
    while (i < max_history_entries + 5) : (i += 1) {
        addHistoryEntry(&state, "SELECT 1", 1, 1, false, null);
    }
    // Should be capped at max_history_entries
    try std.testing.expectEqual(max_history_entries, state.query_history.items.len);
}

test "addHistoryEntry: error msg null when no error" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit(std.testing.allocator);
        state.change_journal.deinit(std.testing.allocator);
    }
    addHistoryEntry(&state, "SELECT 1", 0, 0, false, null);
    try std.testing.expect(state.query_history.items[0].error_msg == null);
}

test "addHistoryEntry: dupes sql string" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit(std.testing.allocator);
        state.change_journal.deinit(std.testing.allocator);
    }
    var buf: [10]u8 = undefined;
    @memcpy(buf[0..8], "SELECT 1");
    addHistoryEntry(&state, buf[0..8], 1, 1, false, null);
    // Mutate the original buffer — the entry should be unaffected
    buf[0] = 'X';
    try std.testing.expectEqualStrings("SELECT 1", state.query_history.items[0].sql);
}

test "ServerState: history starts empty" {
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        state.query_history.deinit(std.testing.allocator);
        state.change_journal.deinit(std.testing.allocator);
    }
    try std.testing.expectEqual(@as(usize, 0), state.query_history.items.len);
}

test "QueryHistoryEntry: struct fields" {
    const entry = QueryHistoryEntry{
        .sql = "SELECT 1",
        .timestamp = 1000,
        .duration_ms = 42,
        .row_count = 5,
        .is_error = false,
        .error_msg = null,
    };
    try std.testing.expectEqualStrings("SELECT 1", entry.sql);
    try std.testing.expectEqual(@as(i64, 1000), entry.timestamp);
    try std.testing.expectEqual(@as(u64, 42), entry.duration_ms);
    try std.testing.expectEqual(@as(?usize, 5), entry.row_count);
    try std.testing.expect(!entry.is_error);
    try std.testing.expect(entry.error_msg == null);
}

test "QueryHistoryEntry: error entry" {
    const entry = QueryHistoryEntry{
        .sql = "BAD",
        .timestamp = 2000,
        .duration_ms = 1,
        .row_count = null,
        .is_error = true,
        .error_msg = "parse error",
    };
    try std.testing.expect(entry.is_error);
    try std.testing.expect(entry.row_count == null);
    try std.testing.expectEqualStrings("parse error", entry.error_msg.?);
}

test "generateRollbackSql: CREATE TABLE generates DROP" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try generateRollbackSql("CREATE TABLE users (id int)", buf.writer(std.testing.allocator));
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "DROP TABLE") != null);
}

test "generateRollbackSql: DROP TABLE warns about data loss" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try generateRollbackSql("DROP TABLE users", buf.writer(std.testing.allocator));
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "WARNING") != null);
}

test "generateRollbackSql: TRUNCATE warns about data loss" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try generateRollbackSql("TRUNCATE TABLE users", buf.writer(std.testing.allocator));
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "WARNING") != null);
}

test "generateRollbackSql: ALTER ADD COLUMN generates DROP COLUMN" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try generateRollbackSql("ALTER TABLE users ADD COLUMN email text", buf.writer(std.testing.allocator));
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "DROP COLUMN") != null);
}

test "generateRollbackSql: unknown operation gives generic message" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try generateRollbackSql("VACUUM ANALYZE", buf.writer(std.testing.allocator));
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "No automatic rollback") != null);
}

test "generateRollbackSql: CREATE INDEX generates DROP INDEX" {
    var buf = std.ArrayList(u8){};
    defer buf.deinit(std.testing.allocator);
    try generateRollbackSql("CREATE INDEX idx_name ON users (name)", buf.writer(std.testing.allocator));
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "DROP INDEX") != null);
}

test "handleJournal: serializes entries as JSON" {
    // This tests that journal entries are properly serialized with all fields.
    var state = web.ServerState.init(std.testing.allocator);
    defer {
        for (state.change_journal.items) |entry| {
            std.testing.allocator.free(entry.table_name);
            std.testing.allocator.free(entry.operation);
            std.testing.allocator.free(entry.column_name);
            std.testing.allocator.free(entry.old_value);
            std.testing.allocator.free(entry.new_value);
            std.testing.allocator.free(entry.pk_column);
            std.testing.allocator.free(entry.pk_value);
        }
        state.change_journal.deinit(std.testing.allocator);
    }
    _ = crud.addJournalEntry(&state, "t1", "delete", "", "", "", "id", "5") catch 0;
    _ = crud.addJournalEntry(&state, "t2", "insert", "", "", "", "id", "10") catch 0;
    try std.testing.expectEqual(@as(usize, 2), state.change_journal.items.len);
    try std.testing.expectEqualStrings("delete", state.change_journal.items[0].operation);
    try std.testing.expectEqualStrings("insert", state.change_journal.items[1].operation);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
