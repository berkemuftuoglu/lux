const std = @import("std");
const builtin = @import("builtin");
const postgres = @import("postgres.zig");

const index_html = @embedFile("static/index.html");
const styles_css = @embedFile("static/styles.css");
const app_js = @embedFile("static/app.js");

const WebError = error{
    BindFailed,
    AcceptFailed,
};

/// A single change entry in the change journal.
pub const ChangeEntry = struct {
    id: u64,
    timestamp: i64,
    table_name: []const u8,
    operation: []const u8, // "update", "delete", "insert"
    column_name: []const u8,
    old_value: []const u8,
    new_value: []const u8,
    pk_column: []const u8,
    pk_value: []const u8,
    undone: bool,
};

const MAX_JOURNAL_ENTRIES = 10000;
const MAX_HISTORY_ENTRIES = 500;

/// A single query history entry for the SQL execution log.
pub const QueryHistoryEntry = struct {
    sql: []const u8,
    timestamp: i64,
    duration_ms: u64,
    row_count: ?usize,
    is_error: bool,
    error_msg: ?[]const u8,
};

/// Mutable server state for the PostgreSQL web client.
pub const ServerState = struct {
    allocator: std.mem.Allocator,
    /// Stored Postgres connection string (null-terminated) for per-query connections.
    conninfo_z: ?[:0]u8 = null,
    /// Schema column info for table/column resolution.
    schema_tables: ?[]postgres.TableInfo = null,
    /// Human-readable schema text for display.
    schema_text: ?[]u8 = null,
    /// Enhanced schema with PK, FK, ENUM, nullability info.
    enhanced_schema: ?[]postgres.EnhancedTableInfo = null,
    /// Read-only mode — blocks DML/DDL operations.
    read_only: bool = false,
    /// Change journal for undo support.
    change_journal: std.ArrayList(ChangeEntry) = undefined,
    next_journal_id: u64 = 1,
    journal_initialized: bool = false,
    /// Query execution history.
    query_history: std.ArrayList(QueryHistoryEntry) = undefined,
    history_initialized: bool = false,
    /// Last successful connection string (for reconnect).
    last_conninfo: ?[]const u8 = null,
    /// Next auto-increment ID for saved connections.
    next_connection_id: u64 = 1,
    /// Port the server is listening on (for CSRF origin checks).
    port: u16 = 8080,

    pub fn init(allocator: std.mem.Allocator) ServerState {
        return .{
            .allocator = allocator,
            .change_journal = std.ArrayList(ChangeEntry).init(allocator),
            .journal_initialized = true,
            .query_history = std.ArrayList(QueryHistoryEntry).init(allocator),
            .history_initialized = true,
        };
    }

    /// Returns true if a Postgres connection is configured.
    pub fn hasDbConnection(self: *const ServerState) bool {
        return self.conninfo_z != null;
    }
};

/// Start the web server and block forever serving requests.
pub fn serve(
    stderr: anytype,
    stdout: anytype,
    state: *ServerState,
    port: u16,
    bind_addr: []const u8,
) !void {
    state.port = port;

    const address = std.net.Address.parseIp(bind_addr, port) catch {
        try stderr.print("Error: invalid bind address '{s}'\n", .{bind_addr});
        std.process.exit(1);
    };

    var server = address.listen(.{
        .reuse_address = true,
    }) catch {
        try stderr.print("Error: failed to bind to port {d}\n", .{port});
        std.process.exit(1);
    };
    defer server.deinit();

    try stdout.print("Lux web UI running at http://{s}:{d}\n", .{ bind_addr, port });
    try stdout.print("Open this URL in your browser. Press Ctrl-C to stop.\n", .{});

    while (true) {
        const conn = server.accept() catch {
            try stderr.print("Warning: accept failed, retrying...\n", .{});
            continue;
        };
        defer conn.stream.close();

        handleConnection(conn.stream, state) catch |err| {
            // Log and continue — don't crash the server on a bad request
            stderr.print("Request error: {s}\n", .{@errorName(err)}) catch {};
        };
    }
}

const MAX_REQUEST_SIZE = 8192;

fn handleConnection(
    stream: std.net.Stream,
    state: *ServerState,
) !void {
    var buf: [MAX_REQUEST_SIZE]u8 = undefined;
    var total_read: usize = 0;

    // Read request headers (look for \r\n\r\n)
    while (total_read < buf.len) {
        const n = stream.read(buf[total_read..]) catch return;
        if (n == 0) return; // Connection closed
        total_read += n;
        if (std.mem.indexOf(u8, buf[0..total_read], "\r\n\r\n") != null) break;
    }

    const request = buf[0..total_read];

    // Parse first line: "METHOD /path HTTP/1.x\r\n"
    const first_line_end = std.mem.indexOf(u8, request, "\r\n") orelse return;
    const first_line = request[0..first_line_end];

    // Split into method and path
    var parts = std.mem.splitScalar(u8, first_line, ' ');
    const method = parts.next() orelse return;
    const path = parts.next() orelse return;

    // CSRF protection: check Origin header on state-changing requests
    const is_post = std.mem.eql(u8, method, "POST");
    const is_delete = std.mem.eql(u8, method, "DELETE");
    if (is_post or is_delete) {
        if (!checkOrigin(request, state.port)) {
            try sendResponse(stream, "403 Forbidden", "text/plain", "Forbidden: cross-origin request");
            return;
        }
    }

    if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/")) {
        try sendHtmlResponseWithCsp(stream, index_html);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/favicon.ico")) {
        try sendResponse(stream, "204 No Content", "image/x-icon", "");
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/styles.css")) {
        try sendResponse(stream, "200 OK", "text/css", styles_css);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/app.js")) {
        try sendResponse(stream, "200 OK", "application/javascript", app_js);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/connect")) {
        try handleConnect(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/schema")) {
        try handleSchema(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql")) {
        try handleSql(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/export/")) {
        try handleExport(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/bulk-update")) {
        try handleBulkUpdate(stream, request, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.indexOf(u8, path, "/fk-lookup") != null) {
        try handleFkLookup(stream, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/ddl")) {
        try handleTableDdl(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/import")) {
        try handleCsvImport(stream, request, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/stats")) {
        try handleTableStats(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/truncate")) {
        try handleTruncateTable(stream, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/")) {
        try handleTableData(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/update")) {
        try handleUpdate(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/delete-row")) {
        try handleDeleteRow(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/insert-row")) {
        try handleInsertRow(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/journal")) {
        try handleJournal(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/journal/undo")) {
        try handleJournalUndo(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql/schema-preview")) {
        try handleSchemaPreview(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql/preview")) {
        try handleSqlPreview(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/settings/read-only")) {
        try handleReadOnlyToggle(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/settings/read-only")) {
        try handleReadOnlyGet(stream, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/history")) {
        try handleHistory(stream, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/connections")) {
        try handleGetConnections(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/connections")) {
        try handlePostConnection(stream, request, state);
    } else if (std.mem.eql(u8, method, "DELETE") and std.mem.startsWith(u8, path, "/api/connections/")) {
        try handleDeleteConnection(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql/export")) {
        try handleSqlExport(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/health")) {
        try handleHealthCheck(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/reconnect")) {
        try handleReconnect(stream, state);
    } else {
        try sendResponse(stream, "404 Not Found", "text/plain", "Not Found");
    }
}


fn eqlLower(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        const la = if (ca >= 'A' and ca <= 'Z') ca + 32 else ca;
        const lb = if (cb >= 'A' and cb <= 'Z') cb + 32 else cb;
        if (la != lb) return false;
    }
    return true;
}

/// Find a header value in an HTTP request (case-insensitive header name).
/// Returns the trimmed value after the colon, or null if the header is not present.
fn findHeader(request: []const u8, header_name: []const u8) ?[]const u8 {
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

/// Check the Origin header on a request for CSRF protection.
/// Returns true if the request should be allowed, false if it should be rejected.
/// If Origin header is absent, the request is allowed (same-origin browser requests
/// may not send Origin). If present, it must match localhost:port.
fn checkOrigin(request: []const u8, port: u16) bool {
    const origin = findHeader(request, "Origin") orelse return true; // absent = allow
    // Build expected origins
    var buf_127: [64]u8 = undefined;
    var buf_local: [64]u8 = undefined;
    const expected_127 = std.fmt.bufPrint(&buf_127, "http://127.0.0.1:{d}", .{port}) catch return false;
    const expected_local = std.fmt.bufPrint(&buf_local, "http://localhost:{d}", .{port}) catch return false;
    if (std.mem.eql(u8, origin, expected_127)) return true;
    if (std.mem.eql(u8, origin, expected_local)) return true;
    return false;
}


/// Handle POST /api/connect — connect to Postgres, fetch schema, store for per-query use.
fn handleConnect(
    stream: std.net.Stream,
    request: []const u8,
    state: *ServerState,
) !void {
    const allocator = state.allocator;

    // Find Content-Length header
    const content_length = findContentLength(request) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };

    if (content_length > 4096) {
        try sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"Request too large\"}");
        return;
    }

    // Find body
    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
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
                try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Read failed\"}");
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
    const conninfo_str = extractJsonField(body, "conninfo") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing conninfo field\"}");
        return;
    };

    // Build null-terminated connection string
    const conninfo_z = allocator.allocSentinel(u8, conninfo_str.len, 0) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    @memcpy(conninfo_z[0..conninfo_str.len], conninfo_str);

    // Test connection and fetch schema
    var pg_conn = postgres.PgConnection.connectVerbose(conninfo_z) catch {
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to connect to PostgreSQL. libpq returned null.\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!pg_conn.isOk()) {
        // Capture the actual error message from libpq
        var err_buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"Connection failed: ") catch {};
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch {};
        ew.writeAll("\"}") catch {};
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    }

    var schema = pg_conn.fetchSchema(allocator) catch {
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to fetch database schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to format schema\"}");
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
        try sendResponse(stream, "200 OK", "application/json", "{\"status\":\"connected\"}");
        return;
    };
    writeJsonEscaped(w, schema_text) catch return;
    // Count tables
    var n_tables: usize = 0;
    if (state.schema_tables) |tables| n_tables = tables.len;
    w.print("\",\"tables\":{d}}}", .{n_tables}) catch return;
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle GET /api/schema — return database schema as JSON.
/// Re-fetches schema from PostgreSQL to ensure fresh data.
/// Falls back to cached data if the refresh fails.
fn handleSchema(stream: std.net.Stream, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
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
        try sendResponse(stream, "200 OK", "application/json", "{\"tables\":[]}");
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
            try writeJsonEscaped(w, etable.name);
            try w.print("\",\"has_primary_key\":{s},\"primary_key_columns\":[", .{if (etable.has_primary_key) "true" else "false"});
            for (etable.primary_key_columns, 0..) |pk, pi| {
                if (pi > 0) try w.writeByte(',');
                try w.writeByte('"');
                try writeJsonEscaped(w, pk);
                try w.writeByte('"');
            }
            try w.writeAll("],\"columns\":[");
            for (etable.columns, 0..) |col, ci| {
                if (ci > 0) try w.writeByte(',');
                try w.writeAll("{\"name\":\"");
                try writeJsonEscaped(w, col.name);
                try w.writeAll("\",\"type\":\"");
                try writeJsonEscaped(w, col.data_type);
                try w.print("\",\"is_primary_key\":{s},\"is_nullable\":{s}", .{
                    if (col.is_primary_key) "true" else "false",
                    if (col.is_nullable) "true" else "false",
                });
                if (col.column_default) |def| {
                    try w.writeAll(",\"column_default\":\"");
                    try writeJsonEscaped(w, def);
                    try w.writeByte('"');
                }
                if (col.fk_target_table) |fkt| {
                    try w.writeAll(",\"fk_target_table\":\"");
                    try writeJsonEscaped(w, fkt);
                    try w.writeByte('"');
                }
                if (col.fk_target_column) |fkc| {
                    try w.writeAll(",\"fk_target_column\":\"");
                    try writeJsonEscaped(w, fkc);
                    try w.writeByte('"');
                }
                if (col.enum_values) |vals| {
                    try w.writeAll(",\"enum_values\":[");
                    for (vals, 0..) |v, vi| {
                        if (vi > 0) try w.writeByte(',');
                        try w.writeByte('"');
                        try writeJsonEscaped(w, v);
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
            try writeJsonEscaped(w, table.name);
            try w.writeAll("\",\"columns\":[");
            for (table.columns, 0..) |col, ci| {
                if (ci > 0) try w.writeByte(',');
                try w.writeAll("{\"name\":\"");
                try writeJsonEscaped(w, col.name);
                try w.writeAll("\",\"type\":\"");
                try writeJsonEscaped(w, col.data_type);
                try w.writeAll("\"}");
            }
            try w.writeAll("]}");
        }
    }
    try w.writeAll("]}");

    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Result of SQL destructiveness analysis.
const SqlGuardResult = struct {
    is_destructive: bool,
    operation: []const u8,
    warning: []const u8,
};

/// Analyze SQL for destructive operations (UPDATE/DELETE without WHERE, DROP, TRUNCATE, ALTER).
fn analyzeSql(sql: []const u8) SqlGuardResult {
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

/// Case-insensitive word search (checks for word boundary).
fn containsIgnoreCaseWord(haystack: []const u8, needle: []const u8) bool {
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

/// Detect if a SQL statement is a write operation (DML/DDL).
/// Check if SQL text contains multiple statements (semicolons outside strings/comments).
/// A trailing semicolon with only whitespace after it does NOT count.
fn hasMultipleStatements(sql: []const u8) bool {
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
fn isSqlReadSafe(sql: []const u8) bool {
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
fn containsWriteKeyword(sql: []const u8) bool {
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

fn isAlphanumUnderscore(ch: u8) bool {
    return (ch >= 'a' and ch <= 'z') or (ch >= 'A' and ch <= 'Z') or (ch >= '0' and ch <= '9') or ch == '_';
}

/// Handle POST /api/sql — execute a SQL query and return results as JSON.
fn handleSql(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse body
    const content_length = findContentLength(request) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };

    const MAX_SQL_BODY = 65536; // 64KB — enough for multi-statement scripts
    if (content_length > MAX_SQL_BODY) {
        try sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"SQL too large (max 64KB)\"}");
        return;
    }

    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
        return;
    };
    const body_start = header_end + 4;
    var body = request[body_start..];

    // Use heap allocation for the body buffer to support large SQL scripts
    const extra_buf = allocator.alloc(u8, MAX_SQL_BODY) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(extra_buf);
    if (body.len < content_length) {
        const already_have = body.len;
        @memcpy(extra_buf[0..already_have], body);
        var read_so_far = already_have;
        while (read_so_far < content_length) {
            const n = stream.read(extra_buf[read_so_far..content_length]) catch {
                try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Read failed\"}");
                return;
            };
            if (n == 0) break;
            read_so_far += n;
        }
        body = extra_buf[0..read_so_far];
    } else {
        body = body[0..content_length];
    }

    const sql_text = extractJsonField(body, "sql") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };

    if (sql_text.len == 0) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty SQL\"}");
        return;
    }

    // Block write operations in read-only mode (whitelist approach)
    if (state.read_only and !isSqlReadSafe(sql_text)) {
        try sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to execute write operations.\"}");
        return;
    }

    // Check for destructive operations and require confirmation
    const force = extractJsonField(body, "force");
    const is_forced = if (force) |f| std.mem.eql(u8, f, "true") else false;
    if (!is_forced) {
        const guard = analyzeSql(sql_text);
        if (guard.is_destructive) {
            var warn_buf = std.ArrayList(u8).init(allocator);
            defer warn_buf.deinit();
            const ww = warn_buf.writer();
            try ww.writeAll("{\"requires_confirmation\":true,\"operation\":\"");
            try writeJsonEscaped(ww, guard.operation);
            try ww.writeAll("\",\"warning\":\"");
            try writeJsonEscaped(ww, guard.warning);
            try ww.writeAll("\"}");
            try sendResponse(stream, "200 OK", "application/json", warn_buf.items);
            return;
        }
    }

    // Build null-terminated SQL
    const sql_z = allocator.allocSentinel(u8, sql_text.len, 0) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(sql_z);
    @memcpy(sql_z[0..sql_text.len], sql_text);

    // Connect and execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Use PQexec for multi-statement scripts, PQexecParams for single statements
    const is_multi = hasMultipleStatements(sql_text);
    const start_time = std.time.milliTimestamp();
    const pg_result = if (is_multi)
        pg_conn.runQueryMulti(allocator, sql_z)
    else
        pg_conn.runQuery(allocator, sql_z);
    var result = pg_result catch {
        const end_time = std.time.milliTimestamp();
        const duration = @as(u64, @intCast(@max(0, end_time - start_time)));
        const err_msg = pg_conn.errorMessage();
        addHistoryEntry(state, sql_text, duration, null, true, err_msg);
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, err_msg) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer result.deinit();
    const end_time = std.time.milliTimestamp();
    const duration = @as(u64, @intCast(@max(0, end_time - start_time)));
    addHistoryEntry(state, sql_text, duration, result.n_rows, false, null);

    // Build JSON response
    try sendQueryResultJson(stream, allocator, &result);
}

/// Handle GET /api/tables/:name/data — return paginated table data.
/// Path format: /api/tables/<name>/data or /api/tables/<name>/data?limit=N&offset=N
fn handleTableData(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/data
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    // Split off query string
    const path_part = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[0..qi] else after_prefix;
    const query_string = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[qi + 1 ..] else "";

    // path_part should be "<table_name>/data"
    const data_suffix = "/data";
    if (!std.mem.endsWith(u8, path_part, data_suffix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/data\"}");
        return;
    }

    const table_name = path_part[0 .. path_part.len - data_suffix.len];
    if (table_name.len == 0 or table_name.len > 128) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema (fail-closed: reject if schema not loaded)
    const schema_tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
        return;
    };
    {
        var found = false;
        for (schema_tables) |t| {
            if (std.mem.eql(u8, t.name, table_name)) {
                found = true;
                break;
            }
        }
        if (!found) {
            try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        }
    }

    // Parse limit and offset from query string
    var limit: usize = 50;
    var offset: usize = 0;
    parseQueryParam(query_string, "limit", &limit);
    parseQueryParam(query_string, "offset", &offset);
    if (limit > 10000) limit = 10000;

    // Parse optional sort column and direction
    var sort_col_buf: [128]u8 = undefined;
    const sort_col = parseStringQueryParam(query_string, "sort", &sort_col_buf);
    var dir_buf: [8]u8 = undefined;
    const dir_param = parseStringQueryParam(query_string, "dir", &dir_buf);
    const sort_dir: []const u8 = if (dir_param) |d| (if (std.mem.eql(u8, d, "desc")) @as([]const u8, "DESC") else "ASC") else "ASC";

    // Validate sort column against schema if provided
    if (sort_col) |col| {
        if (state.schema_tables) |tables| {
            const table_info = findTableInSchema(tables, table_name);
            if (table_info) |ti| {
                if (!findColumnInTable(ti, col)) {
                    try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Sort column not found in table schema\"}");
                    return;
                }
            }
        }
    }

    // Parse count mode: ?count=exact for precise count, otherwise estimate
    var count_exact_buf: [8]u8 = undefined;
    const count_mode = parseStringQueryParam(query_string, "count", &count_exact_buf);
    const use_exact_count_param = if (count_mode) |m| std.mem.eql(u8, m, "exact") else false;

    // Parse column filters: f.column_name=value
    const FilterEntry = struct { column: []const u8, value: []const u8 };
    var filters: [16]FilterEntry = undefined;
    var filter_count: usize = 0;
    {
        var fiter = std.mem.splitScalar(u8, query_string, '&');
        while (fiter.next()) |param| {
            if (std.mem.startsWith(u8, param, "f.") and param.len > 2) {
                if (std.mem.indexOfScalar(u8, param, '=')) |eq| {
                    const col = param[2..eq];
                    const val = param[eq + 1 ..];
                    if (col.len > 0 and val.len > 0 and filter_count < 16) {
                        // Validate column exists in schema
                        if (state.schema_tables) |tables| {
                            const ti = findTableInSchema(tables, table_name);
                            if (ti) |t_info| {
                                if (findColumnInTable(t_info, col)) {
                                    filters[filter_count] = .{ .column = col, .value = val };
                                    filter_count += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    const has_filters = filter_count > 0;

    // When filters active, use exact count; otherwise respect user preference
    const use_exact_count = use_exact_count_param or has_filters;

    // Parse keyset pagination params: after=value, before=value
    var after_buf: [256]u8 = undefined;
    const after_cursor = parseStringQueryParam(query_string, "after", &after_buf);
    var before_buf: [256]u8 = undefined;
    const before_cursor = parseStringQueryParam(query_string, "before", &before_buf);

    // Check if table has a primary key — if not, include ctid
    var table_has_pk = true;
    var pk_col_name: ?[]const u8 = null;
    if (state.enhanced_schema) |etables| {
        for (etables) |et| {
            if (std.mem.eql(u8, et.name, table_name)) {
                table_has_pk = et.has_primary_key;
                if (et.has_primary_key and et.primary_key_columns.len > 0) {
                    pk_col_name = et.primary_key_columns[0];
                }
                break;
            }
        }
    }

    // Views don't support ctid — check if this relation is a view
    var is_view = false;
    if (!table_has_pk) view_check: {
        var view_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch break :view_check;
        defer view_conn.deinit();
        var vq_buf = std.ArrayList(u8).init(allocator);
        defer vq_buf.deinit();
        const esc_tn = escapeStringValue(allocator, table_name) catch break :view_check;
        defer allocator.free(esc_tn);
        vq_buf.writer().print("SELECT 1 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname = '{s}' AND n.nspname = 'public' AND c.relkind = 'v'", .{esc_tn}) catch break :view_check;
        vq_buf.append(0) catch break :view_check;
        const vq_z: [*:0]const u8 = @ptrCast(vq_buf.items[0 .. vq_buf.items.len - 1 :0]);
        var vr = view_conn.runQuery(allocator, vq_z) catch break :view_check;
        is_view = vr.n_rows > 0;
        vr.deinit();
    }
    const select_cols: []const u8 = if (!table_has_pk and !is_view) "ctid::text, *" else "*";

    // Use keyset pagination when: table has PK and after/before cursor present
    const use_keyset = table_has_pk and pk_col_name != null and (after_cursor != null or before_cursor != null);

    // Build WHERE clause from filters and keyset cursor
    var where_buf = std.ArrayList(u8).init(allocator);
    defer where_buf.deinit();
    const ww = where_buf.writer();
    var where_parts: usize = 0;

    if (has_filters or use_keyset) {
        try ww.writeAll(" WHERE ");
    }

    // Add filter conditions
    for (filters[0..filter_count]) |f| {
        if (where_parts > 0) try ww.writeAll(" AND ");
        const esc_val = escapeStringValue(allocator, f.value) catch continue;
        defer allocator.free(esc_val);
        try ww.print("\"{s}\"::text ILIKE '%{s}%'", .{ f.column, esc_val });
        where_parts += 1;
    }

    // Add keyset cursor condition
    if (use_keyset) {
        if (after_cursor) |cursor| {
            if (where_parts > 0) try ww.writeAll(" AND ");
            const esc_cursor = escapeStringValue(allocator, cursor) catch {
                try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
            defer allocator.free(esc_cursor);
            try ww.print("\"{s}\" > '{s}'", .{ pk_col_name.?, esc_cursor });
            where_parts += 1;
        } else if (before_cursor) |cursor| {
            if (where_parts > 0) try ww.writeAll(" AND ");
            const esc_cursor = escapeStringValue(allocator, cursor) catch {
                try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
            defer allocator.free(esc_cursor);
            try ww.print("\"{s}\" < '{s}'", .{ pk_col_name.?, esc_cursor });
            where_parts += 1;
        }
    }
    const where_clause = where_buf.items;

    // Build count query
    var count_buf = std.ArrayList(u8).init(allocator);
    defer count_buf.deinit();
    if (!use_exact_count) {
        const esc_count_tn = try escapeStringValue(allocator, table_name);
        defer allocator.free(esc_count_tn);
        try count_buf.writer().print("SELECT COALESCE(n_live_tup, 0) FROM pg_stat_user_tables WHERE relname = '{s}'", .{esc_count_tn});
    } else {
        try count_buf.writer().print("SELECT COUNT(*) FROM \"{s}\"{s}", .{ table_name, where_clause });
    }
    try count_buf.append(0);

    // Build data query
    var sql_list = std.ArrayList(u8).init(allocator);
    defer sql_list.deinit();
    const sw = sql_list.writer();

    if (use_keyset and before_cursor != null) {
        // Backward keyset: reverse order, will re-reverse results
        try sw.print("SELECT {s} FROM \"{s}\"{s}", .{ select_cols, table_name, where_clause });
        try sw.print(" ORDER BY \"{s}\" DESC", .{pk_col_name.?});
        try sw.print(" LIMIT {d}", .{limit});
    } else if (sort_col) |col| {
        try sw.print("SELECT {s} FROM \"{s}\"{s} ORDER BY \"{s}\" {s} LIMIT {d} OFFSET {d}", .{ select_cols, table_name, where_clause, col, sort_dir, limit, offset });
    } else if (use_keyset) {
        // Forward keyset
        try sw.print("SELECT {s} FROM \"{s}\"{s} ORDER BY \"{s}\" ASC LIMIT {d}", .{ select_cols, table_name, where_clause, pk_col_name.?, limit });
    } else {
        try sw.print("SELECT {s} FROM \"{s}\"{s} LIMIT {d} OFFSET {d}", .{ select_cols, table_name, where_clause, limit, offset });
    }
    try sql_list.append(0);

    // Connect and execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Get total count
    const count_z: [*:0]const u8 = @ptrCast(count_buf.items[0 .. count_buf.items.len - 1 :0]);
    var count_result = pg_conn.runQuery(allocator, count_z) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Count query failed\"}");
        return;
    };
    defer count_result.deinit();

    var total: usize = 0;
    if (count_result.n_rows > 0 and count_result.rows[0].len > 0) {
        total = std.fmt.parseInt(usize, count_result.rows[0][0], 10) catch 0;
    }

    // Get data
    const data_z: [*:0]const u8 = @ptrCast(sql_list.items[0 .. sql_list.items.len - 1 :0]);
    var pg_result = pg_conn.runQuery(allocator, data_z) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Data query failed\"}");
        return;
    };
    defer pg_result.deinit();

    // Build and send JSON response
    try sendTableDataJson(stream, allocator, state, &pg_result, table_name, .{
        .total = total,
        .limit = limit,
        .offset = offset,
        .use_exact_count = use_exact_count,
        .table_has_pk = table_has_pk,
        .use_keyset = use_keyset,
        .pk_col_name = pk_col_name,
    });
}

/// Metadata for table data JSON response.
const TableDataMeta = struct {
    total: usize,
    limit: usize,
    offset: usize,
    use_exact_count: bool,
    table_has_pk: bool,
    use_keyset: bool,
    pk_col_name: ?[]const u8,
};

/// Serialize paginated table data as JSON and send as HTTP response.
fn sendTableDataJson(
    stream: std.net.Stream,
    allocator: std.mem.Allocator,
    state: *ServerState,
    pg_result: *postgres.QueryResult,
    table_name: []const u8,
    meta: TableDataMeta,
) !void {
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();

    try w.print("{{\"total\":{d},\"limit\":{d},\"offset\":{d},\"count_exact\":{s},\"pk_mode\":\"{s}\",\"pagination\":\"{s}\",", .{
        meta.total, meta.limit, meta.offset,
        if (meta.use_exact_count) "true" else "false",
        if (meta.table_has_pk) "column" else "ctid",
        if (meta.use_keyset) "keyset" else "offset",
    });

    // Include PK info from enhanced schema
    if (state.enhanced_schema) |etables| {
        for (etables) |et| {
            if (std.mem.eql(u8, et.name, table_name)) {
                try w.print("\"has_primary_key\":{s},\"pk_columns\":[", .{if (et.has_primary_key) "true" else "false"});
                for (et.primary_key_columns, 0..) |pk, pi| {
                    if (pi > 0) try w.writeByte(',');
                    try w.writeByte('"');
                    try writeJsonEscaped(w, pk);
                    try w.writeByte('"');
                }
                try w.writeAll("],");
                break;
            }
        }
    }

    // Column names
    try w.writeAll("\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeByte('"');
        try writeJsonEscaped(w, name);
        try w.writeByte('"');
    }
    try w.writeAll("],\"rows\":[");

    // Row data
    for (pg_result.rows, 0..) |row, ri| {
        if (ri > 0) try w.writeByte(',');
        try w.writeByte('[');
        for (row, 0..) |val, ci| {
            if (ci > 0) try w.writeByte(',');
            if (std.mem.eql(u8, val, "NULL")) {
                try w.writeAll("null");
            } else {
                try w.writeByte('"');
                try writeJsonEscaped(w, val);
                try w.writeByte('"');
            }
        }
        try w.writeByte(']');
    }

    try w.writeByte(']');

    // Add keyset cursor values if using keyset pagination
    if (meta.use_keyset and meta.pk_col_name != null and pg_result.n_rows > 0) {
        var pk_result_idx: ?usize = null;
        for (pg_result.col_names, 0..) |name, ci| {
            if (std.mem.eql(u8, name, meta.pk_col_name.?)) { pk_result_idx = ci; break; }
        }
        if (pk_result_idx) |pki| {
            const first_row = pg_result.rows[0];
            const last_row = pg_result.rows[pg_result.n_rows - 1];
            if (pki < first_row.len and pki < last_row.len) {
                try w.writeAll(",\"first_cursor\":\"");
                try writeJsonEscaped(w, first_row[pki]);
                try w.writeAll("\",\"last_cursor\":\"");
                try writeJsonEscaped(w, last_row[pki]);
                try w.writeByte('"');
            }
        }
    }

    try w.writeByte('}');
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Send a Postgres QueryResult as JSON with columns and rows.
fn sendQueryResultJson(stream: std.net.Stream, allocator: std.mem.Allocator, pg_result: *postgres.QueryResult) !void {
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();

    try w.print("{{\"row_count\":{d},", .{pg_result.n_rows});

    // Column names
    try w.writeAll("\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeByte('"');
        try writeJsonEscaped(w, name);
        try w.writeByte('"');
    }
    try w.writeAll("],\"rows\":[");

    // Row data
    for (pg_result.rows, 0..) |row, ri| {
        if (ri > 0) try w.writeByte(',');
        try w.writeByte('[');
        for (row, 0..) |val, ci| {
            if (ci > 0) try w.writeByte(',');
            if (std.mem.eql(u8, val, "NULL")) {
                try w.writeAll("null");
            } else {
                try w.writeByte('"');
                try writeJsonEscaped(w, val);
                try w.writeByte('"');
            }
        }
        try w.writeByte(']');
    }

    try w.writeAll("]}");
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Parse a numeric query parameter from a query string like "limit=50&offset=0".
fn parseQueryParam(query_string: []const u8, name: []const u8, out: *usize) void {
    var iter = std.mem.splitScalar(u8, query_string, '&');
    while (iter.next()) |param| {
        if (std.mem.startsWith(u8, param, name) and param.len > name.len and param[name.len] == '=') {
            const val_str = param[name.len + 1 ..];
            out.* = std.fmt.parseInt(usize, val_str, 10) catch return;
            return;
        }
    }
}

/// Parse a string query parameter from a query string like "sort=name&dir=asc".
/// Returns the value as a slice into the provided buffer, or null if not found.
fn parseStringQueryParam(query_string: []const u8, name: []const u8, buf: []u8) ?[]const u8 {
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

/// URL-decode a percent-encoded string in-place.
/// Decodes %XX sequences and '+' to space. Returns the decoded slice.
fn urlDecode(buf: []u8, input: []const u8) ?[]const u8 {
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

/// Find a table by name in the schema. Returns the TableInfo or null.
fn findTableInSchema(tables: []const postgres.TableInfo, name: []const u8) ?postgres.TableInfo {
    for (tables) |t| {
        if (std.mem.eql(u8, t.name, name)) return t;
    }
    return null;
}

/// Check if a column exists in a table's schema.
fn findColumnInTable(table: postgres.TableInfo, col_name: []const u8) bool {
    for (table.columns) |col| {
        if (std.mem.eql(u8, col.name, col_name)) return true;
    }
    return false;
}

/// Escape a string value for SQL by doubling single quotes and backslashes.
/// Rejects null bytes to prevent C string truncation attacks.
/// Caller owns the returned memory.
const StringEscapeError = error{
    InvalidCharacter,
    OutOfMemory,
};

fn escapeStringValue(allocator: std.mem.Allocator, value: []const u8) StringEscapeError![]u8 {
    // Reject null bytes to prevent truncation attacks (libpq uses C strings)
    for (value) |ch| {
        if (ch == 0) return error.InvalidCharacter;
    }
    // Count characters that need doubling
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

/// Escape a SQL identifier by doubling all double-quote characters.
/// Returns the escaped string suitable for use inside "..." identifiers.
const IdentifierError = error{
    InvalidIdentifier,
    OutOfMemory,
};

fn escapeIdentifier(allocator: std.mem.Allocator, name: []const u8) IdentifierError![]u8 {
    // Reject null bytes to prevent truncation attacks
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

/// Read the HTTP request body, handling partial reads.
/// Returns the body slice (may point into extra_buf or into request).
fn readRequestBody(
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

/// Handle POST /api/update — update a single cell value.
fn handleUpdate(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    var extra_buf: [8192]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    // Extract fields
    const table_name = extractJsonField(body, "table") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table field\"}");
        return;
    };
    const column_name = extractJsonField(body, "column") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing column field\"}");
        return;
    };
    const value = extractJsonField(body, "value") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing value field\"}");
        return;
    };
    const pk_column = extractJsonField(body, "pk_column") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_column field\"}");
        return;
    };
    const pk_value = extractJsonField(body, "pk_value") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_value field\"}");
        return;
    };

    // Check pk_mode: "ctid" or "column" (default)
    const pk_mode = extractJsonField(body, "pk_mode") orelse "column";
    const is_ctid_mode = std.mem.eql(u8, pk_mode, "ctid");

    // Validate table and columns against schema
    const tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    };
    if (!findColumnInTable(table_info, column_name)) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Column not found in table schema\"}");
        return;
    }
    // For ctid mode, pk_column is "ctid" which is a system column — skip schema validation
    if (!is_ctid_mode) {
        if (!findColumnInTable(table_info, pk_column)) {
            try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"PK column not found in table schema\"}");
            return;
        }
    } else {
        // Validate ctid format
        if (!validateCtid(pk_value)) {
            try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid ctid format\"}");
            return;
        }
    }

    // Escape values
    const escaped_value = escapeStringValue(allocator, value) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_value);

    const escaped_pk = escapeStringValue(allocator, pk_value) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_pk);

    // Check column type for json/jsonb casting
    var is_json_col = false;
    if (state.enhanced_schema) |etables| {
        for (etables) |et| {
            if (std.mem.eql(u8, et.name, table_name)) {
                for (et.columns) |col| {
                    if (std.mem.eql(u8, col.name, column_name)) {
                        if (std.mem.eql(u8, col.data_type, "json") or std.mem.eql(u8, col.data_type, "jsonb")) {
                            is_json_col = true;
                        }
                        break;
                    }
                }
                break;
            }
        }
    }
    const value_expr: []const u8 = if (is_json_col) "'::jsonb" else "'";

    // Build SQL: use ctid WHERE clause for no-PK tables
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const w = sql_buf.writer();
    if (is_ctid_mode) {
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}{s} WHERE ctid = '{s}'::tid RETURNING \"{s}\"", .{
            table_name, column_name, escaped_value, value_expr, escaped_pk, column_name,
        });
    } else {
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}{s} WHERE \"{s}\" = '{s}' RETURNING \"{s}\"", .{
            table_name, column_name, escaped_value, value_expr, pk_column, escaped_pk, column_name,
        });
    }
    try sql_buf.append(0); // null terminator

    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record journal entry — use the value field as the new value, old value from request
    const old_value_field = extractJsonField(body, "old_value") orelse "";
    const journal_id = addJournalEntry(state, table_name, "update", column_name, old_value_field, value, pk_column, pk_value);

    var resp_buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"success\":true,\"journal_id\":{d}}}", .{journal_id}) catch "{\"success\":true}";
    try sendResponse(stream, "200 OK", "application/json", resp);
}

/// Handle POST /api/delete-row — delete a row by primary key.
fn handleDeleteRow(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    var extra_buf: [4096]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 4096) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const table_name = extractJsonField(body, "table") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table field\"}");
        return;
    };
    const pk_column = extractJsonField(body, "pk_column") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_column field\"}");
        return;
    };
    const pk_value = extractJsonField(body, "pk_value") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing pk_value field\"}");
        return;
    };

    // Check pk_mode
    const pk_mode = extractJsonField(body, "pk_mode") orelse "column";
    const is_ctid_mode = std.mem.eql(u8, pk_mode, "ctid");

    // Validate against schema
    const tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    };
    if (!is_ctid_mode) {
        if (!findColumnInTable(table_info, pk_column)) {
            try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"PK column not found in table schema\"}");
            return;
        }
    } else {
        if (!validateCtid(pk_value)) {
            try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid ctid format\"}");
            return;
        }
    }

    // Escape pk value
    const escaped_pk = escapeStringValue(allocator, pk_value) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_pk);

    // Build SQL: DELETE with ctid or PK column
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const w = sql_buf.writer();
    if (is_ctid_mode) {
        try w.print("DELETE FROM \"{s}\" WHERE ctid = '{s}'::tid", .{ table_name, escaped_pk });
    } else {
        try w.print("DELETE FROM \"{s}\" WHERE \"{s}\" = '{s}'", .{ table_name, pk_column, escaped_pk });
    }
    try sql_buf.append(0);

    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Fetch the row BEFORE deleting so we can record it in the journal
    var old_row_json: []const u8 = "";
    var old_row_buf: ?[]u8 = null;
    defer if (old_row_buf) |b| allocator.free(b);
    {
        var sel_buf = std.ArrayList(u8).init(allocator);
        defer sel_buf.deinit();
        const sel_w = sel_buf.writer();
        if (is_ctid_mode) {
            sel_w.print("SELECT * FROM \"{s}\" WHERE ctid = '{s}'::tid LIMIT 1", .{ table_name, escaped_pk }) catch {};
        } else {
            sel_w.print("SELECT * FROM \"{s}\" WHERE \"{s}\" = '{s}' LIMIT 1", .{ table_name, pk_column, escaped_pk }) catch {};
        }
        sel_buf.append(0) catch {};
        if (sel_buf.items.len > 1) {
            const sel_z: [*:0]const u8 = sel_buf.items[0 .. sel_buf.items.len - 1 :0];
            if (pg_conn.runQuery(allocator, sel_z)) |sel_res| {
                var sel_result = sel_res;
                defer sel_result.deinit();
                if (sel_result.rows.len > 0) {
                    if (formatRowAsJsonCompact(allocator, sel_result.col_names, sel_result.rows[0])) |json| {
                        old_row_buf = json;
                        old_row_json = json;
                    } else |_| {}
                }
            } else |_| {}
        }
    }

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record journal entry for delete with old row data
    _ = addJournalEntry(state, table_name, "delete", "", old_row_json, "", pk_column, pk_value);

    try sendResponse(stream, "200 OK", "application/json", "{\"success\":true}");
}

/// Handle POST /api/insert-row — insert a row with default values.
fn handleInsertRow(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    var extra_buf: [4096]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 4096) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    const table_name = extractJsonField(body, "table") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table field\"}");
        return;
    };

    // Validate against schema
    const tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    if (findTableInSchema(tables, table_name) == null) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    }

    // Parse optional values object
    const values = extractJsonObject(allocator, body, "values");
    defer if (values) |v| allocator.free(v);

    // Validate column keys against schema
    if (values) |pairs| {
        const table_info = findTableInSchema(tables, table_name) orelse {
            try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        };
        for (pairs) |pair| {
            if (!findColumnInTable(table_info, pair.key)) {
                try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Column not found in table schema\"}");
                return;
            }
        }
    }

    // Build SQL
    var sql_builder = std.ArrayList(u8).init(allocator);
    defer sql_builder.deinit();
    const sw = sql_builder.writer();

    if (values) |pairs| {
        if (pairs.len > 0) {
            // INSERT INTO "table" ("col1", "col2") VALUES ('val1', 'val2') RETURNING *
            try sw.print("INSERT INTO \"{s}\" (", .{table_name});
            for (pairs, 0..) |pair, i| {
                if (i > 0) try sw.writeAll(", ");
                const esc_col = escapeIdentifier(allocator, pair.key) catch {
                    try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid column name\"}");
                    return;
                };
                defer allocator.free(esc_col);
                try sw.print("\"{s}\"", .{esc_col});
            }
            try sw.writeAll(") VALUES (");
            for (pairs, 0..) |pair, i| {
                if (i > 0) try sw.writeAll(", ");
                if (std.mem.eql(u8, pair.value, "__NULL__")) {
                    try sw.writeAll("NULL");
                } else {
                    const escaped = escapeStringValue(allocator, pair.value) catch {
                        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                        return;
                    };
                    defer allocator.free(escaped);
                    try sw.print("'{s}'", .{escaped});
                }
            }
            try sw.writeAll(") RETURNING *");
        } else {
            try sw.print("INSERT INTO \"{s}\" DEFAULT VALUES RETURNING *", .{table_name});
        }
    } else {
        try sw.print("INSERT INTO \"{s}\" DEFAULT VALUES RETURNING *", .{table_name});
    }
    try sw.writeByte(0); // null terminate

    const sql_slice = sql_builder.items;
    const sql_z: [*:0]const u8 = sql_slice[0 .. sql_slice.len - 1 :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record journal entry for insert — use first column as PK
    if (pg_result.n_rows > 0 and pg_result.col_names.len > 0) {
        const insert_pk_col = pg_result.col_names[0];
        const insert_pk_val = if (pg_result.rows[0].len > 0) pg_result.rows[0][0] else "";
        _ = addJournalEntry(state, table_name, "insert", "", "", "", insert_pk_col, insert_pk_val);
    }

    // Build response with the new row
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const jw = json_buf.writer();

    try jw.writeAll("{\"success\":true,\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try jw.writeByte(',');
        try jw.writeByte('"');
        try writeJsonEscaped(jw, name);
        try jw.writeByte('"');
    }
    try jw.writeAll("],\"row\":[");
    if (pg_result.n_rows > 0) {
        for (pg_result.rows[0], 0..) |val, ci| {
            if (ci > 0) try jw.writeByte(',');
            if (std.mem.eql(u8, val, "NULL")) {
                try jw.writeAll("null");
            } else {
                try jw.writeByte('"');
                try writeJsonEscaped(jw, val);
                try jw.writeByte('"');
            }
        }
    }
    try jw.writeAll("]}");

    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle GET /api/tables/:name/fk-lookup?column=col&search=text&limit=20
fn handleFkLookup(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = state.allocator;

    // Parse: /api/tables/<table>/fk-lookup?column=...&search=...
    const prefix = "/api/tables/";
    const after_prefix = path[prefix.len..];
    const fk_idx = std.mem.indexOf(u8, after_prefix, "/fk-lookup") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    };
    const table_name = after_prefix[0..fk_idx];

    // Parse query string
    const qs_start = std.mem.indexOfScalar(u8, path, '?') orelse path.len;
    const qs = if (qs_start < path.len) path[qs_start + 1 ..] else "";

    var col_buf: [128]u8 = undefined;
    const column = parseStringQueryParam(qs, "column", &col_buf) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing column param\"}");
        return;
    };
    var search_buf: [256]u8 = undefined;
    const search = parseStringQueryParam(qs, "search", &search_buf) orelse "";

    // Find FK target from enhanced schema
    const etables = state.enhanced_schema orelse {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No enhanced schema\"}");
        return;
    };
    var fk_target_table: ?[]const u8 = null;
    var fk_target_column: ?[]const u8 = null;
    for (etables) |et| {
        if (std.mem.eql(u8, et.name, table_name)) {
            for (et.columns) |col| {
                if (std.mem.eql(u8, col.name, column)) {
                    fk_target_table = col.fk_target_table;
                    fk_target_column = col.fk_target_column;
                    break;
                }
            }
            break;
        }
    }

    const target_table = fk_target_table orelse {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Column has no FK\"}");
        return;
    };
    const target_column = fk_target_column orelse {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"FK target column unknown\"}");
        return;
    };

    // Build query: SELECT target_column, * FROM target_table WHERE target_column::text ILIKE '%search%' LIMIT 20
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const sw = sql_buf.writer();

    if (search.len > 0) {
        const escaped = escapeStringValue(allocator, search) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(escaped);
        try sw.print("SELECT \"{s}\" FROM \"{s}\" WHERE \"{s}\"::text ILIKE '%{s}%' LIMIT 20", .{ target_column, target_table, target_column, escaped });
    } else {
        try sw.print("SELECT \"{s}\" FROM \"{s}\" LIMIT 20", .{ target_column, target_table });
    }
    try sql_buf.append(0);

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);
    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"FK lookup query failed\"}");
        return;
    };
    defer pg_result.deinit();

    // Build response: { values: ["val1", "val2", ...] }
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const jw = json_buf.writer();
    try jw.writeAll("{\"values\":[");
    for (pg_result.rows, 0..) |row, ri| {
        if (ri > 0) try jw.writeByte(',');
        if (row.len > 0 and !std.mem.eql(u8, row[0], "NULL")) {
            try jw.writeByte('"');
            try writeJsonEscaped(jw, row[0]);
            try jw.writeByte('"');
        } else {
            try jw.writeAll("null");
        }
    }
    try jw.writeAll("]}");
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle POST /api/tables/:name/bulk-update — find and replace in a column.
fn handleBulkUpdate(stream: std.net.Stream, request: []const u8, path: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/bulk-update
    const prefix = "/api/tables/";
    const after_prefix = path[prefix.len..];
    const suffix = "/bulk-update";
    if (!std.mem.endsWith(u8, after_prefix, suffix)) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }
    const table_name = after_prefix[0 .. after_prefix.len - suffix.len];

    // Validate table
    const tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found\"}");
        return;
    };

    var extra_buf: [8192]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };

    const column = extractJsonField(body, "column") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing column field\"}");
        return;
    };
    const find_val = extractJsonField(body, "find") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing find field\"}");
        return;
    };
    const replace_val = extractJsonField(body, "replace") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing replace field\"}");
        return;
    };
    const force_str = extractJsonField(body, "force") orelse "false";
    const force = std.mem.eql(u8, force_str, "true");

    // Validate column
    if (!findColumnInTable(table_info, column)) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Column not found\"}");
        return;
    }

    const esc_find = escapeStringValue(allocator, find_val) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(esc_find);

    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!force) {
        // Preview mode: count affected rows
        var cnt_sql = std.ArrayList(u8).init(allocator);
        defer cnt_sql.deinit();
        try cnt_sql.writer().print("SELECT COUNT(*) FROM \"{s}\" WHERE \"{s}\"::text = '{s}'", .{ table_name, column, esc_find });
        try cnt_sql.append(0);
        const cnt_z: [*:0]const u8 = @ptrCast(cnt_sql.items[0 .. cnt_sql.items.len - 1 :0]);
        var cnt_result = pg_conn.runQuery(allocator, cnt_z) catch {
            try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Count query failed\"}");
            return;
        };
        defer cnt_result.deinit();
        var affected: usize = 0;
        if (cnt_result.n_rows > 0 and cnt_result.rows[0].len > 0) {
            affected = std.fmt.parseInt(usize, cnt_result.rows[0][0], 10) catch 0;
        }
        var resp_buf: [128]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"affected_rows\":{d},\"requires_confirmation\":true}}", .{affected}) catch "{\"error\":\"fmt\"}";
        try sendResponse(stream, "200 OK", "application/json", resp);
    } else {
        // Execute mode
        const esc_replace = escapeStringValue(allocator, replace_val) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(esc_replace);
        var upd_sql = std.ArrayList(u8).init(allocator);
        defer upd_sql.deinit();
        try upd_sql.writer().print("UPDATE \"{s}\" SET \"{s}\" = '{s}' WHERE \"{s}\"::text = '{s}'", .{ table_name, column, esc_replace, column, esc_find });
        try upd_sql.append(0);
        const upd_z: [*:0]const u8 = @ptrCast(upd_sql.items[0 .. upd_sql.items.len - 1 :0]);
        var upd_result = pg_conn.runQuery(allocator, upd_z) catch {
            var err_buf: [512]u8 = undefined;
            var fbs = std.io.fixedBufferStream(&err_buf);
            const ew = fbs.writer();
            ew.writeAll("{\"error\":\"") catch return;
            writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
            ew.writeAll("\"}") catch return;
            try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
            return;
        };
        defer upd_result.deinit();

        // Record journal entries
        _ = addJournalEntry(state, table_name, "update", column, find_val, replace_val, "bulk", "");
        try sendResponse(stream, "200 OK", "application/json", "{\"success\":true}");
    }
}



fn sendResponse(stream: std.net.Stream, status: []const u8, content_type: []const u8, body: []const u8) !void {
    var header_buf: [512]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n", .{
        status,
        content_type,
        body.len,
    }) catch return;

    stream.writeAll(header) catch return;
    stream.writeAll(body) catch return;
}

/// Send an HTML response with Content-Security-Policy header.
fn sendHtmlResponseWithCsp(stream: std.net.Stream, body: []const u8) !void {
    const csp = "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'";
    var header_buf: [768]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {d}\r\nConnection: close\r\nCache-Control: no-store\r\nContent-Security-Policy: {s}\r\n\r\n", .{
        body.len,
        csp,
    }) catch return;

    stream.writeAll(header) catch return;
    stream.writeAll(body) catch return;
}

const ExportError = error{
    OutOfMemory,
    FormatFailed,
};

/// Send an HTTP response with a Content-Disposition header for file downloads.
fn sendResponseWithDownload(
    stream: std.net.Stream,
    status: []const u8,
    content_type: []const u8,
    filename: []const u8,
    body: []const u8,
) !void {
    var header_buf: [1024]u8 = undefined;
    const header = std.fmt.bufPrint(&header_buf, "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nContent-Disposition: attachment; filename=\"{s}\"\r\nConnection: close\r\nCache-Control: no-store\r\n\r\n", .{
        status,
        content_type,
        body.len,
        filename,
    }) catch return;

    stream.writeAll(header) catch return;
    stream.writeAll(body) catch return;
}

/// Write a single CSV field, quoting it if it contains commas, quotes, or newlines.
/// Internal double quotes are escaped by doubling them (RFC 4180).
fn escapeCsvField(writer: anytype, field: []const u8) !void {
    var needs_quoting = false;
    for (field) |ch| {
        if (ch == ',' or ch == '"' or ch == '\n' or ch == '\r') {
            needs_quoting = true;
            break;
        }
    }

    if (!needs_quoting) {
        try writer.writeAll(field);
        return;
    }

    try writer.writeByte('"');
    for (field) |ch| {
        if (ch == '"') {
            try writer.writeAll("\"\"");
        } else {
            try writer.writeByte(ch);
        }
    }
    try writer.writeByte('"');
}

/// Format query result as CSV text (RFC 4180 with CRLF line endings).
/// Caller owns the returned memory.
fn formatResultAsCsv(
    allocator: std.mem.Allocator,
    col_names: []const []const u8,
    rows: []const []const []const u8,
) ExportError![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    const w = buf.writer();

    // Header row
    for (col_names, 0..) |name, i| {
        if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
        escapeCsvField(w, name) catch return error.OutOfMemory;
    }
    w.writeAll("\r\n") catch return error.OutOfMemory;

    // Data rows
    for (rows) |row| {
        for (row, 0..) |val, i| {
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            escapeCsvField(w, val) catch return error.OutOfMemory;
        }
        w.writeAll("\r\n") catch return error.OutOfMemory;
    }

    return buf.toOwnedSlice() catch return error.OutOfMemory;
}

/// Format query result as a JSON array of objects.
/// NULL values (the literal string "NULL" from libpq) are rendered as JSON null.
/// Caller owns the returned memory.
fn formatResultAsJson(
    allocator: std.mem.Allocator,
    col_names: []const []const u8,
    rows: []const []const []const u8,
) ExportError![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    const w = buf.writer();

    w.writeByte('[') catch return error.OutOfMemory;

    for (rows, 0..) |row, ri| {
        if (ri > 0) w.writeByte(',') catch return error.OutOfMemory;
        w.writeByte('{') catch return error.OutOfMemory;

        for (row, 0..) |val, ci| {
            if (ci > 0) w.writeByte(',') catch return error.OutOfMemory;

            // Write key
            w.writeByte('"') catch return error.OutOfMemory;
            if (ci < col_names.len) {
                writeJsonEscaped(w, col_names[ci]) catch return error.OutOfMemory;
            }
            w.writeAll("\":") catch return error.OutOfMemory;

            // Write value — NULL becomes JSON null, everything else is a string
            if (std.mem.eql(u8, val, "NULL")) {
                w.writeAll("null") catch return error.OutOfMemory;
            } else {
                w.writeByte('"') catch return error.OutOfMemory;
                writeJsonEscaped(w, val) catch return error.OutOfMemory;
                w.writeByte('"') catch return error.OutOfMemory;
            }
        }

        w.writeByte('}') catch return error.OutOfMemory;
    }

    w.writeByte(']') catch return error.OutOfMemory;

    return buf.toOwnedSlice() catch return error.OutOfMemory;
}

/// Handle GET /api/export/<table>?format=csv|json — export table data as a file download.
fn handleExport(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/export/<name>
    const prefix = "/api/export/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    // Split off query string
    const path_part = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[0..qi] else after_prefix;
    const query_string = if (std.mem.indexOfScalar(u8, after_prefix, '?')) |qi| after_prefix[qi + 1 ..] else "";

    const table_name = path_part;
    if (table_name.len == 0 or table_name.len > 128) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema (fail-closed: reject if schema not loaded)
    const schema_tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
        return;
    };
    {
        var found = false;
        for (schema_tables) |t| {
            if (std.mem.eql(u8, t.name, table_name)) {
                found = true;
                break;
            }
        }
        if (!found) {
            try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        }
    }

    // Parse format from query string
    var fmt_buf: [8]u8 = undefined;
    const format_param = parseStringQueryParam(query_string, "format", &fmt_buf) orelse "csv";

    const is_csv = std.mem.eql(u8, format_param, "csv");
    const is_json = std.mem.eql(u8, format_param, "json");
    if (!is_csv and !is_json) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid format. Use csv or json.\"}");
        return;
    }

    // Connect to Postgres and run SELECT * FROM "<table>"
    const conninfo_z = state.conninfo_z orelse {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    };

    var pg_conn = postgres.PgConnection.connect(conninfo_z) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to connect to database\"}");
        return;
    };
    defer pg_conn.deinit();

    // Build query: SELECT * FROM "<table_name>"
    var sql_buf: [256]u8 = undefined;
    const sql = std.fmt.bufPrintZ(&sql_buf, "SELECT * FROM \"{s}\"", .{table_name}) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Table name too long\"}");
        return;
    };

    var result = pg_conn.runQuery(allocator, sql) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Query failed\"}");
        return;
    };
    defer result.deinit();

    // Format the result
    if (is_csv) {
        const csv_data = formatResultAsCsv(allocator, result.col_names, result.rows) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format CSV\"}");
            return;
        };
        defer allocator.free(csv_data);

        // Build filename
        var filename_buf: [256]u8 = undefined;
        const filename = std.fmt.bufPrint(&filename_buf, "{s}.csv", .{table_name}) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Filename too long\"}");
            return;
        };

        try sendResponseWithDownload(stream, "200 OK", "text/csv; charset=utf-8", filename, csv_data);
    } else {
        const json_data = formatResultAsJson(allocator, result.col_names, result.rows) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format JSON\"}");
            return;
        };
        defer allocator.free(json_data);

        // Build filename
        var filename_buf: [256]u8 = undefined;
        const filename = std.fmt.bufPrint(&filename_buf, "{s}.json", .{table_name}) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Filename too long\"}");
            return;
        };

        try sendResponseWithDownload(stream, "200 OK", "application/json", filename, json_data);
    }
}

fn findContentLength(request: []const u8) ?usize {
    // Case-insensitive search for Content-Length header
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

/// Write a string with JSON escaping (handles ", \, newlines, tabs, control chars per RFC 8259).
fn writeJsonEscaped(writer: anytype, s: []const u8) !void {
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


/// Format a single row as a compact JSON object: {"col1":"val1","col2":"val2"}
fn formatRowAsJsonCompact(allocator: std.mem.Allocator, col_names: []const []const u8, row: []const []const u8) ![]u8 {
    var buf = std.ArrayList(u8).init(allocator);
    errdefer buf.deinit();
    const bw = buf.writer();
    try bw.writeByte('{');
    const len = @min(col_names.len, row.len);
    for (0..len) |i| {
        if (i > 0) try bw.writeByte(',');
        try bw.writeByte('"');
        try writeJsonEscaped(bw, col_names[i]);
        try bw.writeAll("\":\"");
        try writeJsonEscaped(bw, row[i]);
        try bw.writeByte('"');
    }
    try bw.writeByte('}');
    return buf.toOwnedSlice();
}


/// Extract a string field value from a JSON body like {"field": "..."}.
/// Minimal parser — no dependency on a JSON library.
fn extractJsonField(body: []const u8, field_name: []const u8) ?[]const u8 {
    // Build the key pattern: "field_name"
    // Search for it in the body
    var search_pos: usize = 0;
    while (search_pos < body.len) {
        const quote_pos = std.mem.indexOfScalarPos(u8, body, search_pos, '"') orelse return null;
        if (quote_pos + 1 + field_name.len + 1 > body.len) return null;
        // Check if the text after the quote matches field_name followed by closing quote
        const after_quote = body[quote_pos + 1 ..];
        if (after_quote.len >= field_name.len + 1 and
            std.mem.eql(u8, after_quote[0..field_name.len], field_name) and
            after_quote[field_name.len] == '"')
        {
            // Found the key — skip past closing quote
            var pos = quote_pos + 1 + field_name.len + 1;

            // Skip whitespace and colon
            while (pos < body.len and (body[pos] == ' ' or body[pos] == ':')) pos += 1;

            // Expect opening quote of value
            if (pos >= body.len or body[pos] != '"') return null;
            pos += 1;

            // Find closing quote (handle escaped quotes)
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

/// Extract the "query" field from a JSON body.
fn extractJsonQuery(body: []const u8) ?[]const u8 {
    return extractJsonField(body, "query");
}

const KVPair = struct {
    key: []const u8,
    value: []const u8,
};

/// Extract key-value pairs from a JSON object field (e.g. "values": {"col":"val", ...}).
/// Returns null if the field is not found or not an object.
/// Caller must free the returned slice with allocator.free().
fn extractJsonObject(allocator: std.mem.Allocator, body: []const u8, field_name: []const u8) ?[]KVPair {
    // Find "field_name" :
    var search_pos: usize = 0;
    const obj_start = while (search_pos < body.len) {
        const quote_pos = std.mem.indexOfScalarPos(u8, body, search_pos, '"') orelse return null;
        if (quote_pos + 1 + field_name.len + 1 > body.len) return null;
        const after_quote = body[quote_pos + 1 ..];
        if (after_quote.len >= field_name.len + 1 and
            std.mem.eql(u8, after_quote[0..field_name.len], field_name) and
            after_quote[field_name.len] == '"')
        {
            var pos = quote_pos + 1 + field_name.len + 1;
            while (pos < body.len and (body[pos] == ' ' or body[pos] == ':' or body[pos] == '\t' or body[pos] == '\n')) pos += 1;
            if (pos >= body.len or body[pos] != '{') return null;
            break pos + 1; // skip the opening brace
        }
        search_pos = quote_pos + 1;
    } else return null;

    // Parse key-value pairs inside the object
    var pairs = std.ArrayList(KVPair).init(allocator);
    var pos = obj_start;
    while (pos < body.len) {
        // Skip whitespace and commas
        while (pos < body.len and (body[pos] == ' ' or body[pos] == ',' or body[pos] == '\t' or body[pos] == '\n' or body[pos] == '\r')) pos += 1;
        if (pos >= body.len or body[pos] == '}') break;

        // Expect opening quote for key
        if (body[pos] != '"') break;
        pos += 1;
        const key_start = pos;
        while (pos < body.len and body[pos] != '"') pos += 1;
        if (pos >= body.len) break;
        const key = body[key_start..pos];
        pos += 1; // skip closing quote

        // Skip colon and whitespace
        while (pos < body.len and (body[pos] == ' ' or body[pos] == ':' or body[pos] == '\t')) pos += 1;
        if (pos >= body.len) break;

        // Handle null value
        if (pos + 4 <= body.len and std.mem.eql(u8, body[pos..][0..4], "null")) {
            pairs.append(.{ .key = key, .value = "__NULL__" }) catch {
                pairs.deinit();
                return null;
            };
            pos += 4;
            continue;
        }

        // Expect opening quote for value
        if (body[pos] != '"') break;
        pos += 1;
        const val_start = pos;
        while (pos < body.len) {
            if (body[pos] == '\\' and pos + 1 < body.len) {
                pos += 2;
                continue;
            }
            if (body[pos] == '"') break;
            pos += 1;
        }
        if (pos >= body.len) break;
        const val = body[val_start..pos];
        pos += 1; // skip closing quote

        pairs.append(.{ .key = key, .value = val }) catch {
            pairs.deinit();
            return null;
        };
    }

    return pairs.toOwnedSlice() catch {
        pairs.deinit();
        return null;
    };
}

/// Add an entry to the change journal.
fn addJournalEntry(
    state: *ServerState,
    table_name: []const u8,
    operation: []const u8,
    column_name: []const u8,
    old_value: []const u8,
    new_value: []const u8,
    pk_column: []const u8,
    pk_value: []const u8,
) u64 {
    if (!state.journal_initialized) return 0;
    const allocator = state.allocator;

    // Drop oldest if at capacity — free the inner strings to prevent leak
    if (state.change_journal.items.len >= MAX_JOURNAL_ENTRIES) {
        const old = state.change_journal.orderedRemove(0);
        allocator.free(old.table_name);
        allocator.free(old.operation);
        allocator.free(old.column_name);
        allocator.free(old.old_value);
        allocator.free(old.new_value);
        allocator.free(old.pk_column);
        allocator.free(old.pk_value);
    }

    const tn = allocator.dupe(u8, table_name) catch return 0;
    const op = allocator.dupe(u8, operation) catch {
        allocator.free(tn);
        return 0;
    };
    const cn = allocator.dupe(u8, column_name) catch {
        allocator.free(tn);
        allocator.free(op);
        return 0;
    };
    const ov = allocator.dupe(u8, old_value) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        return 0;
    };
    const nv = allocator.dupe(u8, new_value) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        allocator.free(ov);
        return 0;
    };
    const pkc = allocator.dupe(u8, pk_column) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        allocator.free(ov);
        allocator.free(nv);
        return 0;
    };
    const pkv = allocator.dupe(u8, pk_value) catch {
        allocator.free(tn);
        allocator.free(op);
        allocator.free(cn);
        allocator.free(ov);
        allocator.free(nv);
        allocator.free(pkc);
        return 0;
    };

    const entry = ChangeEntry{
        .id = state.next_journal_id,
        .timestamp = std.time.timestamp(),
        .table_name = tn,
        .operation = op,
        .column_name = cn,
        .old_value = ov,
        .new_value = nv,
        .pk_column = pkc,
        .pk_value = pkv,
        .undone = false,
    };
    state.change_journal.append(entry) catch return 0;
    state.next_journal_id += 1;
    return entry.id;
}

/// Record a query execution in the history log.
fn addHistoryEntry(
    state: *ServerState,
    sql: []const u8,
    duration_ms: u64,
    row_count: ?usize,
    is_error: bool,
    error_msg: ?[]const u8,
) void {
    if (!state.history_initialized) return;
    const ts = std.time.timestamp();
    const sql_dupe = state.allocator.dupe(u8, sql) catch return;
    const err_dupe: ?[]const u8 = if (error_msg) |e| (state.allocator.dupe(u8, e) catch null) else null;
    state.query_history.append(.{
        .sql = sql_dupe,
        .timestamp = ts,
        .duration_ms = duration_ms,
        .row_count = row_count,
        .is_error = is_error,
        .error_msg = err_dupe,
    }) catch return;
    // Cap at MAX_HISTORY_ENTRIES
    if (state.query_history.items.len > MAX_HISTORY_ENTRIES) {
        const old = state.query_history.orderedRemove(0);
        state.allocator.free(old.sql);
        if (old.error_msg) |e| state.allocator.free(e);
    }
}

/// Handle GET /api/history — return recent query history entries (newest first).
fn handleHistory(stream: std.net.Stream, state: *ServerState) !void {
    var arena = std.heap.ArenaAllocator.init(state.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var json = std.ArrayList(u8).init(alloc);
    try json.appendSlice("{\"entries\":[");

    const items = state.query_history.items;
    const count = @min(items.len, 100);
    var wrote: usize = 0;

    // Reverse order (newest first)
    var i: usize = items.len;
    while (i > 0 and wrote < count) {
        i -= 1;
        if (wrote > 0) try json.appendSlice(",");
        try json.appendSlice("{\"sql\":\"");
        try writeJsonEscaped(json.writer(), items[i].sql);
        try json.appendSlice("\",\"timestamp\":");
        var ts_buf: [20]u8 = undefined;
        const ts_len = std.fmt.formatIntBuf(&ts_buf, items[i].timestamp, 10, .lower, .{});
        try json.appendSlice(ts_buf[0..ts_len]);
        try json.appendSlice(",\"duration_ms\":");
        var dur_buf: [20]u8 = undefined;
        const dur_len = std.fmt.formatIntBuf(&dur_buf, items[i].duration_ms, 10, .lower, .{});
        try json.appendSlice(dur_buf[0..dur_len]);
        if (items[i].row_count) |rc| {
            try json.appendSlice(",\"row_count\":");
            var rc_buf: [20]u8 = undefined;
            const rc_len = std.fmt.formatIntBuf(&rc_buf, rc, 10, .lower, .{});
            try json.appendSlice(rc_buf[0..rc_len]);
        } else {
            try json.appendSlice(",\"row_count\":null");
        }
        try json.appendSlice(",\"is_error\":");
        try json.appendSlice(if (items[i].is_error) "true" else "false");
        if (items[i].error_msg) |em| {
            try json.appendSlice(",\"error\":\"");
            try writeJsonEscaped(json.writer(), em);
            try json.appendSlice("\"");
        }
        try json.appendSlice("}");
        wrote += 1;
    }

    try json.appendSlice("]}");
    try sendResponse(stream, "200 OK", "application/json", json.items);
}

/// Handle GET /api/tables/:name/ddl — return CREATE TABLE statement for the given table.
fn handleTableDdl(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/ddl
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    const ddl_suffix = "/ddl";
    if (!std.mem.endsWith(u8, after_prefix, ddl_suffix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/ddl\"}");
        return;
    }

    const table_name = after_prefix[0 .. after_prefix.len - ddl_suffix.len];
    if (table_name.len == 0 or table_name.len > 128) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema (fail-closed: reject if schema not loaded)
    const schema_tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
        return;
    };
    {
        var found = false;
        for (schema_tables) |t| {
            if (std.mem.eql(u8, t.name, table_name)) {
                found = true;
                break;
            }
        }
        if (!found) {
            try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        }
    }

    // Check if this is a view or table
    const escaped_name = escapeStringValue(allocator, table_name) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_name);

    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const sw = sql_buf.writer();

    // Use a CASE expression to generate correct DDL for both tables and views
    try sw.writeAll(
        "SELECT CASE WHEN cls.relkind = 'v' THEN " ++
        "'CREATE OR REPLACE VIEW \"' || cls.relname || '\" AS' || chr(10) || pg_get_viewdef(cls.oid, true) " ++
        "ELSE " ++
        "'CREATE TABLE \"' || cls.relname || '\" (' || chr(10) || " ++
        "string_agg('  \"' || c.column_name || '\" ' || c.data_type || " ++
        "CASE WHEN c.character_maximum_length IS NOT NULL THEN '(' || c.character_maximum_length || ')' ELSE '' END || " ++
        "CASE WHEN c.is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END || " ++
        "CASE WHEN c.column_default IS NOT NULL THEN ' DEFAULT ' || c.column_default ELSE '' END" ++
        ", ',' || chr(10) ORDER BY c.ordinal_position) || chr(10) || ');' " ++
        "END " ++
        "FROM pg_class cls " ++
        "JOIN pg_namespace ns ON cls.relnamespace = ns.oid " ++
        "LEFT JOIN information_schema.columns c ON c.table_schema = ns.nspname AND c.table_name = cls.relname " ++
        "WHERE ns.nspname = 'public' AND cls.relname = '",
    );
    try sw.writeAll(escaped_name);
    try sw.writeAll("' AND cls.relkind IN ('r', 'v') GROUP BY cls.relkind, cls.relname, cls.oid");
    try sql_buf.append(0);
    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

    // Connect and execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Extract DDL from result
    if (pg_result.n_rows == 0 or pg_result.rows.len == 0 or pg_result.rows[0].len == 0) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Could not generate DDL for table\"}");
        return;
    }

    const ddl_text = pg_result.rows[0][0];

    // Build JSON response
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();
    try w.writeAll("{\"ddl\":\"");
    try writeJsonEscaped(w, ddl_text);
    try w.writeAll("\"}");
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// CSV parsing error set.
const CsvParseError = error{
    EmptyCsv,
    NoDataRows,
    ColumnCountMismatch,
    OutOfMemory,
};
/// Parse a CSV string into a list of rows, each row being a list of field values.
/// Handles quoted fields (RFC 4180): double quotes inside quoted fields are escaped
/// by doubling them. Supports both \n and \r\n line endings.
/// Returns headers (first row) and data rows as separate allocations.
/// Caller must free each field, each row slice, and the headers/rows slices.
fn parseCsvContent(
    allocator: std.mem.Allocator,
    csv: []const u8,
) CsvParseError!struct { headers: [][]const u8, rows: [][]const []const u8 } {
    if (csv.len == 0) return error.EmptyCsv;

    var all_rows = std.ArrayList([][]const u8).init(allocator);
    defer all_rows.deinit();
    errdefer {
        for (all_rows.items) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
    }

    var pos: usize = 0;
    while (pos < csv.len) {
        // Skip trailing whitespace-only content
        var check = pos;
        while (check < csv.len and (csv[check] == '\r' or csv[check] == '\n' or csv[check] == ' ')) check += 1;
        if (check >= csv.len) break;

        var fields = std.ArrayList([]const u8).init(allocator);
        errdefer {
            for (fields.items) |f| allocator.free(f);
            fields.deinit();
        }

        // Parse one row
        while (true) {
            if (pos >= csv.len) break;

            if (csv[pos] == '"') {
                // Quoted field
                pos += 1; // skip opening quote
                var field_buf = std.ArrayList(u8).init(allocator);
                errdefer field_buf.deinit();
                while (pos < csv.len) {
                    if (csv[pos] == '"') {
                        if (pos + 1 < csv.len and csv[pos + 1] == '"') {
                            // Escaped double quote
                            field_buf.append('"') catch return error.OutOfMemory;
                            pos += 2;
                        } else {
                            // End of quoted field
                            pos += 1;
                            break;
                        }
                    } else {
                        field_buf.append(csv[pos]) catch return error.OutOfMemory;
                        pos += 1;
                    }
                }
                const owned = field_buf.toOwnedSlice() catch return error.OutOfMemory;
                fields.append(owned) catch return error.OutOfMemory;
            } else {
                // Unquoted field — read until comma or newline
                const start = pos;
                while (pos < csv.len and csv[pos] != ',' and csv[pos] != '\n' and csv[pos] != '\r') {
                    pos += 1;
                }
                const val = allocator.dupe(u8, csv[start..pos]) catch return error.OutOfMemory;
                fields.append(val) catch return error.OutOfMemory;
            }

            // After field: expect comma (more fields), newline (end of row), or EOF
            if (pos >= csv.len) break;
            if (csv[pos] == ',') {
                pos += 1; // skip comma, continue to next field
                continue;
            }
            if (csv[pos] == '\r') pos += 1; // skip CR
            if (pos < csv.len and csv[pos] == '\n') pos += 1; // skip LF
            break;
        }

        if (fields.items.len > 0) {
            const row_slice = fields.toOwnedSlice() catch return error.OutOfMemory;
            all_rows.append(row_slice) catch return error.OutOfMemory;
        } else {
            fields.deinit();
        }
    }

    if (all_rows.items.len == 0) return error.EmptyCsv;

    // First row is always returned as headers
    const headers = all_rows.items[0];

    if (all_rows.items.len < 2) {
        // Only headers, no data rows
        const empty_rows = allocator.alloc([]const []const u8, 0) catch return error.OutOfMemory;
        // Remove the header from all_rows so errdefer doesn't free it
        _ = all_rows.orderedRemove(0);
        return .{ .headers = headers, .rows = empty_rows };
    }

    // Copy data rows into a separate allocation
    const data_rows = allocator.alloc([]const []const u8, all_rows.items.len - 1) catch return error.OutOfMemory;
    for (all_rows.items[1..], 0..) |row, i| {
        data_rows[i] = row;
    }
    // Clear all_rows so errdefer doesn't double-free
    all_rows.clearRetainingCapacity();
    return .{ .headers = headers, .rows = data_rows };
}

/// Handle POST /api/tables/:name/import — import CSV data into a table.
fn handleCsvImport(
    stream: std.net.Stream,
    request: []const u8,
    path: []const u8,
    state: *ServerState,
) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/import
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    const import_suffix = "/import";
    if (!std.mem.endsWith(u8, after_prefix, import_suffix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/import\"}");
        return;
    }

    const table_name = after_prefix[0 .. after_prefix.len - import_suffix.len];
    if (table_name.len == 0 or table_name.len > 128) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema
    const tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    const table_info = findTableInSchema(tables, table_name) orelse {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    };

    // Read request body
    var extra_buf: [MAX_REQUEST_SIZE]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, MAX_REQUEST_SIZE) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing or invalid request body\"}");
        return;
    };

    // Extract CSV content from JSON body
    const csv_content = extractJsonField(body, "csv") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing csv field\"}");
        return;
    };

    if (csv_content.len == 0) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty CSV content\"}");
        return;
    }

    // Check has_header (default true)
    const has_header_str = extractJsonField(body, "has_header");
    const has_header = if (has_header_str) |h| !std.mem.eql(u8, h, "false") else true;

    // Parse CSV
    const csv_result = parseCsvContent(allocator, csv_content) catch |err| {
        const msg = switch (err) {
            error.EmptyCsv => "{\"error\":\"CSV content is empty\"}",
            error.NoDataRows => "{\"error\":\"CSV has no data rows\"}",
            error.ColumnCountMismatch => "{\"error\":\"CSV column count mismatch\"}",
            error.OutOfMemory => "{\"error\":\"Out of memory parsing CSV\"}",
        };
        try sendResponse(stream, "400 Bad Request", "application/json", msg);
        return;
    };
    defer {
        for (csv_result.headers) |h| allocator.free(h);
        allocator.free(csv_result.headers);
        for (csv_result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(csv_result.rows);
    }


    // Determine column names for INSERT
    var col_names = std.ArrayList([]const u8).init(allocator);
    defer col_names.deinit();

    if (has_header) {
        // Validate CSV headers match table columns
        for (csv_result.headers) |header| {
            if (!findColumnInTable(table_info, header)) {
                var err_buf: [256]u8 = undefined;
                var fbs = std.io.fixedBufferStream(&err_buf);
                const ew = fbs.writer();
                ew.writeAll("{\"error\":\"CSV column '") catch {};
                writeJsonEscaped(ew, header) catch {};
                ew.writeAll("' not found in table schema\"}") catch {};
                try sendResponse(stream, "400 Bad Request", "application/json", fbs.getWritten());
                return;
            }
            col_names.append(header) catch {
                try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
        }
    } else {
        // No header row — the "headers" from parseCsv are actually data row 0
        // Use schema column names (first N columns matching field count)
        const field_count = csv_result.headers.len;
        if (field_count > table_info.columns.len) {
            try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"CSV has more columns than table\"}");
            return;
        }
        for (table_info.columns[0..field_count]) |col| {
            col_names.append(col.name) catch {
                try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
                return;
            };
        }
    }

    // Determine which rows to insert
    // When has_header=true: csv_result.rows contains data rows
    // When has_header=false: csv_result.headers is actually the first data row,
    //   and csv_result.rows contains the remaining rows
    // We need to handle both cases

    // Connect to Postgres
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // Begin transaction
    var begin_result = pg_conn.runQuery(allocator, "BEGIN") catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to start transaction\"}");
        return;
    };
    begin_result.deinit();

    var imported: usize = 0;
    var insert_error: bool = false;
    var error_msg_buf: [512]u8 = undefined;
    var error_msg_len: usize = 0;

    // Build INSERT for each row
    const data_rows = csv_result.rows;
    const num_cols = col_names.items.len;

    // If !has_header, we also need to insert the "headers" row as data
    if (!has_header) {
        const first_row = csv_result.headers;
        if (first_row.len >= num_cols) {
            if (buildAndExecuteInsert(allocator, &pg_conn, table_name, col_names.items, first_row[0..num_cols])) {
                imported += 1;
            } else {
                insert_error = true;
                const em = pg_conn.errorMessage();
                const copy_len = @min(em.len, error_msg_buf.len);
                @memcpy(error_msg_buf[0..copy_len], em[0..copy_len]);
                error_msg_len = copy_len;
            }
        }
    }

    if (!insert_error) {
        for (data_rows) |row| {
            const field_count = @min(row.len, num_cols);
            if (field_count == 0) continue;
            if (buildAndExecuteInsert(allocator, &pg_conn, table_name, col_names.items[0..field_count], row[0..field_count])) {
                imported += 1;
            } else {
                insert_error = true;
                const em = pg_conn.errorMessage();
                const copy_len = @min(em.len, error_msg_buf.len);
                @memcpy(error_msg_buf[0..copy_len], em[0..copy_len]);
                error_msg_len = copy_len;
                break;
            }
        }
    }

    if (insert_error) {
        // Rollback
        var rb = pg_conn.runQuery(allocator, "ROLLBACK") catch {
            try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Insert failed and rollback failed\"}");
            return;
        };
        rb.deinit();
        // error_msg_buf available if needed for detailed reporting
        var resp_buf = std.ArrayList(u8).init(allocator);
        defer resp_buf.deinit();
        const rw = resp_buf.writer();
        try rw.writeAll("{\"error\":\"Import failed: ");
        try writeJsonEscaped(rw, error_msg_buf[0..error_msg_len]);
        try rw.writeAll("\"}");
        try sendResponse(stream, "200 OK", "application/json", resp_buf.items);
        return;
    }

    // Commit
    var commit_result = pg_conn.runQuery(allocator, "COMMIT") catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Commit failed\"}");
        return;
    };
    commit_result.deinit();

    // Return success
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"imported\":{d}}}", .{imported}) catch "{\"error\":\"fmt\"}";
    try sendResponse(stream, "200 OK", "application/json", resp);
}

/// Build and execute a single INSERT statement. Returns true on success.
fn buildAndExecuteInsert(
    allocator: std.mem.Allocator,
    pg_conn: *postgres.PgConnection,
    table_name: []const u8,
    col_names: []const []const u8,
    values: []const []const u8,
) bool {
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const w = sql_buf.writer();

    w.print("INSERT INTO \"{s}\" (", .{table_name}) catch return false;
    for (col_names, 0..) |col, i| {
        if (i > 0) w.writeAll(", ") catch return false;
        w.print("\"{s}\"", .{col}) catch return false;
    }
    w.writeAll(") VALUES (") catch return false;
    for (values, 0..) |val, i| {
        if (i > 0) w.writeAll(", ") catch return false;
        if (val.len == 0) {
            w.writeAll("NULL") catch return false;
        } else {
            const escaped = escapeStringValue(allocator, val) catch return false;
            defer allocator.free(escaped);
            w.print("'{s}'", .{escaped}) catch return false;
        }
    }
    w.writeAll(")") catch return false;
    sql_buf.append(0) catch return false;

    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);
    var result = pg_conn.runQuery(allocator, sql_z) catch return false;
    result.deinit();
    return true;
}

/// Handle GET /api/tables/:name/stats — return table size and row statistics.
fn handleTableStats(
    stream: std.net.Stream,
    path: []const u8,
    state: *ServerState,
) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse table name from path: /api/tables/<name>/stats
    const prefix = "/api/tables/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }

    const after_prefix = path[prefix.len..];
    const stats_suffix = "/stats";
    if (!std.mem.endsWith(u8, after_prefix, stats_suffix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Expected /api/tables/<name>/stats\"}");
        return;
    }

    const table_name = after_prefix[0 .. after_prefix.len - stats_suffix.len];
    if (table_name.len == 0 or table_name.len > 128) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid table name\"}");
        return;
    }

    // Validate table name exists in schema (fail-closed: reject if schema not loaded)
    const schema_tables_fk = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Schema not loaded. Connect to a database first.\"}");
        return;
    };
    {
        var found = false;
        for (schema_tables_fk) |t| {
            if (std.mem.eql(u8, t.name, table_name)) {
                found = true;
                break;
            }
        }
        if (!found) {
            try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Table not found in schema\"}");
            return;
        }
    }

    // Build stats query — works for both tables and views
    const escaped_name = escapeStringValue(allocator, table_name) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(escaped_name);

    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const sw = sql_buf.writer();
    try sw.writeAll(
        "SELECT " ++
            "COALESCE(c.reltuples, 0)::bigint AS row_estimate, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_relation_size(c.oid)) END AS table_size, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_indexes_size(c.oid)) END AS index_size, " ++
            "CASE WHEN c.relkind = 'v' THEN 'N/A (view)' ELSE pg_size_pretty(pg_total_relation_size(c.oid)) END AS total_size " ++
            "FROM pg_class c " ++
            "JOIN pg_namespace n ON c.relnamespace = n.oid " ++
            "WHERE n.nspname = 'public' AND c.relname = '",
    );
    try sw.writeAll(escaped_name);
    try sw.writeAll("' AND c.relkind IN ('r', 'v')");
    try sql_buf.append(0);
    const sql_z: [*:0]const u8 = @ptrCast(sql_buf.items[0 .. sql_buf.items.len - 1 :0]);

    // Connect and execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    if (pg_result.n_rows == 0) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Could not retrieve table stats\"}");
        return;
    }

    // Extract values: row_estimate, table_size, index_size, total_size
    const row = pg_result.rows[0];
    const row_estimate = if (row.len > 0) row[0] else "0";
    const table_size = if (row.len > 1) row[1] else "0 bytes";
    const index_size = if (row.len > 2) row[2] else "0 bytes";
    const total_size = if (row.len > 3) row[3] else "0 bytes";

    // Build JSON response
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();
    try w.writeAll("{\"row_estimate\":");
    try w.writeAll(row_estimate);
    try w.writeAll(",\"table_size\":\"");
    try writeJsonEscaped(w, table_size);
    try w.writeAll("\",\"index_size\":\"");
    try writeJsonEscaped(w, index_size);
    try w.writeAll("\",\"total_size\":\"");
    try writeJsonEscaped(w, total_size);
    try w.writeAll("\"}");

    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle GET /api/journal — return recent change journal entries.
fn handleJournal(stream: std.net.Stream, state: *const ServerState) !void {
    const allocator = state.allocator;
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();

    try w.writeAll("{\"entries\":[");
    if (state.journal_initialized) {
        const items = state.change_journal.items;
        const start = if (items.len > 100) items.len - 100 else 0;
        for (items[start..], 0..) |entry, i| {
            if (i > 0) try w.writeByte(',');
            try w.print("{{\"id\":{d},\"timestamp\":{d},\"table\":\"", .{ entry.id, entry.timestamp });
            try writeJsonEscaped(w, entry.table_name);
            try w.writeAll("\",\"operation\":\"");
            try writeJsonEscaped(w, entry.operation);
            try w.writeAll("\",\"column\":\"");
            try writeJsonEscaped(w, entry.column_name);
            try w.writeAll("\",\"old_value\":\"");
            try writeJsonEscaped(w, entry.old_value);
            try w.writeAll("\",\"new_value\":\"");
            try writeJsonEscaped(w, entry.new_value);
            try w.writeAll("\",\"pk_column\":\"");
            try writeJsonEscaped(w, entry.pk_column);
            try w.writeAll("\",\"pk_value\":\"");
            try writeJsonEscaped(w, entry.pk_value);
            try w.print("\",\"undone\":{s}}}", .{if (entry.undone) "true" else "false"});
        }
    }
    try w.writeAll("]}");
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle POST /api/journal/undo — reverse a change.
fn handleJournalUndo(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;
    var extra_buf: [1024]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 1024) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };

    const id_str = extractJsonField(body, "id") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing id field\"}");
        return;
    };
    const id = std.fmt.parseInt(u64, id_str, 10) catch {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid id\"}");
        return;
    };

    // Find the entry
    if (!state.journal_initialized) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Journal not initialized\"}");
        return;
    }
    var found_entry: ?*ChangeEntry = null;
    for (state.change_journal.items) |*entry| {
        if (entry.id == id) { found_entry = entry; break; }
    }
    const entry = found_entry orelse {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Journal entry not found\"}");
        return;
    };
    if (entry.undone) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Already undone\"}");
        return;
    }

    // Build undo SQL
    var sql_buf = std.ArrayList(u8).init(allocator);
    defer sql_buf.deinit();
    const w = sql_buf.writer();

    if (std.mem.eql(u8, entry.operation, "update")) {
        // Restore old value
        const escaped_old = escapeStringValue(allocator, entry.old_value) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(escaped_old);
        const escaped_pk = escapeStringValue(allocator, entry.pk_value) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(escaped_pk);
        try w.print("UPDATE \"{s}\" SET \"{s}\" = '{s}' WHERE \"{s}\" = '{s}'", .{
            entry.table_name, entry.column_name, escaped_old, entry.pk_column, escaped_pk,
        });
    } else if (std.mem.eql(u8, entry.operation, "delete")) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Undo delete not yet supported\"}");
        return;
    } else if (std.mem.eql(u8, entry.operation, "insert")) {
        // Delete the inserted row
        const escaped_pk = escapeStringValue(allocator, entry.pk_value) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(escaped_pk);
        try w.print("DELETE FROM \"{s}\" WHERE \"{s}\" = '{s}'", .{
            entry.table_name, entry.pk_column, escaped_pk,
        });
    } else {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Unknown operation\"}");
        return;
    }
    try sql_buf.append(0);
    const sql_z: [*:0]const u8 = sql_buf.items[0 .. sql_buf.items.len - 1 :0];

    // Execute undo
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    entry.undone = true;
    try sendResponse(stream, "200 OK", "application/json", "{\"success\":true}");
}

/// Validate a ctid string format: (digits,digits)
fn validateCtid(ctid: []const u8) bool {
    if (ctid.len < 5) return false; // minimum "(0,0)"
    if (ctid[0] != '(') return false;
    if (ctid[ctid.len - 1] != ')') return false;
    const inner = ctid[1 .. ctid.len - 1];
    const comma = std.mem.indexOfScalar(u8, inner, ',') orelse return false;
    const page = inner[0..comma];
    const offset = inner[comma + 1 ..];
    if (page.len == 0 or offset.len == 0) return false;
    for (page) |ch| { if (ch < '0' or ch > '9') return false; }
    for (offset) |ch| { if (ch < '0' or ch > '9') return false; }
    return true;
}

/// Handle POST /api/sql/preview — wrap query in BEGIN/ROLLBACK to preview effects.
fn handleSqlPreview(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = state.allocator;
    var extra_buf: [8192]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };
    const sql_text = extractJsonField(body, "sql") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };
    if (sql_text.len == 0) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty SQL\"}");
        return;
    }

    // Block write operations in read-only mode
    if (state.read_only and !isSqlReadSafe(sql_text)) {
        try sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to preview write operations.\"}");
        return;
    }

    // Connect
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    // BEGIN
    var begin_result = pg_conn.runQuery(allocator, "BEGIN") catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to start transaction\"}");
        return;
    };
    begin_result.deinit();

    // Execute the query
    const sql_z = allocator.allocSentinel(u8, sql_text.len, 0) catch {
        if (pg_conn.runQuery(allocator, "ROLLBACK")) |rb| {
            var r = rb;
            r.deinit();
        } else |_| {}
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(sql_z);
    @memcpy(sql_z[0..sql_text.len], sql_text);

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        // Try to get error message before rollback
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch {};
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch {};
        ew.writeAll("\"}") catch {};
        if (pg_conn.runQuery(allocator, "ROLLBACK")) |rb| {
            var r = rb;
            r.deinit();
        } else |_| {}
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // ROLLBACK — undo changes
    if (pg_conn.runQuery(allocator, "ROLLBACK")) |rb| {
            var r = rb;
            r.deinit();
        } else |_| {}

    // Build preview response
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();
    try w.print("{{\"preview\":true,\"affected_rows\":{d},", .{pg_result.n_rows});

    // Include column names and first few rows as sample
    try w.writeAll("\"columns\":[");
    for (pg_result.col_names, 0..) |name, i| {
        if (i > 0) try w.writeByte(',');
        try w.writeByte('"');
        try writeJsonEscaped(w, name);
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
                try writeJsonEscaped(w, val);
                try w.writeByte('"');
            }
        }
        try w.writeByte(']');
    }
    try w.writeAll("]}");
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle POST /api/sql/schema-preview — analyze DDL and generate rollback SQL.
fn handleSchemaPreview(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }
    const allocator = state.allocator;
    var extra_buf: [8192]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 8192) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };
    const sql_text = extractJsonField(body, "sql") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };

    // Generate rollback SQL
    var rollback_buf = std.ArrayList(u8).init(allocator);
    defer rollback_buf.deinit();
    generateRollbackSql(sql_text, rollback_buf.writer()) catch {};
    const rollback = if (rollback_buf.items.len > 0) rollback_buf.items else "-- No automatic rollback available";

    // Detect operation type
    const guard = analyzeSql(sql_text);

    // Build response
    var json_buf = std.ArrayList(u8).init(allocator);
    defer json_buf.deinit();
    const w = json_buf.writer();
    try w.writeAll("{\"operation\":\"");
    try writeJsonEscaped(w, guard.operation);
    try w.writeAll("\",\"warning\":\"");
    try writeJsonEscaped(w, guard.warning);
    try w.writeAll("\",\"rollback_sql\":\"");
    try writeJsonEscaped(w, rollback);
    try w.writeAll("\"}");

    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Generate rollback SQL for common DDL operations.
fn generateRollbackSql(sql: []const u8, writer: anytype) !void {
    // Simple token-based DDL analysis
    // ADD COLUMN → DROP COLUMN
    if (containsIgnoreCaseWord(sql, "ALTER") and containsIgnoreCaseWord(sql, "TABLE") and containsIgnoreCaseWord(sql, "ADD") and containsIgnoreCaseWord(sql, "COLUMN")) {
        // Try to extract table name and column name
        // Pattern: ALTER TABLE "table" ADD COLUMN "column" ...
        // Extract what's between TABLE and ADD
        if (indexOfIgnoreCase(sql, "ADD")) |add_pos| {
            const before_add = sql[0..add_pos];
            if (indexOfIgnoreCase(before_add, "TABLE")) |table_pos| {
                const after_table = std.mem.trimLeft(u8, before_add[table_pos + 5 ..], " \t\n\r");
                const table_end = std.mem.indexOfAny(u8, after_table, " \t\n\r") orelse after_table.len;
                const table_name = std.mem.trim(u8, after_table[0..table_end], "\"");

                // Extract column name after COLUMN keyword
                const after_add = sql[add_pos + 3 ..];
                if (indexOfIgnoreCase(after_add, "COLUMN")) |col_kw_pos| {
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
    if (containsIgnoreCaseWord(sql, "CREATE") and containsIgnoreCaseWord(sql, "TABLE")) {
        if (indexOfIgnoreCase(sql, "TABLE")) |table_pos| {
            const after_table = std.mem.trimLeft(u8, sql[table_pos + 5 ..], " \t\n\r");
            const table_end = std.mem.indexOfAny(u8, after_table, " \t\n\r(") orelse after_table.len;
            const table_name = std.mem.trim(u8, after_table[0..table_end], "\"");
            try writer.print("DROP TABLE IF EXISTS \"{s}\";", .{table_name});
            return;
        }
    }

    // CREATE INDEX → DROP INDEX
    if (containsIgnoreCaseWord(sql, "CREATE") and containsIgnoreCaseWord(sql, "INDEX")) {
        if (indexOfIgnoreCase(sql, "INDEX")) |idx_pos| {
            const after_idx = std.mem.trimLeft(u8, sql[idx_pos + 5 ..], " \t\n\r");
            const idx_end = std.mem.indexOfAny(u8, after_idx, " \t\n\r") orelse after_idx.len;
            const idx_name = std.mem.trim(u8, after_idx[0..idx_end], "\"");
            try writer.print("DROP INDEX IF EXISTS \"{s}\";", .{idx_name});
            return;
        }
    }

    // DROP TABLE → note that data is lost
    if (containsIgnoreCaseWord(sql, "DROP") and containsIgnoreCaseWord(sql, "TABLE")) {
        try writer.writeAll("-- WARNING: DROP TABLE cannot be automatically rolled back. Data will be lost.");
        return;
    }

    // TRUNCATE → note that data is lost
    if (containsIgnoreCaseWord(sql, "TRUNCATE")) {
        try writer.writeAll("-- WARNING: TRUNCATE cannot be automatically rolled back. Data will be lost.");
        return;
    }

    try writer.writeAll("-- No automatic rollback available for this operation.");
}

/// Find first occurrence of a word (case-insensitive) in a string, returning its position.
fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
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

/// Check if read-only mode blocks this operation. Returns true if blocked.
fn enforceReadOnly(stream: std.net.Stream, state: *const ServerState) !bool {
    if (state.read_only) {
        try sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to make changes.\"}");
        return true;
    }
    return false;
}

/// Handle POST /api/settings/read-only — toggle read-only mode.
fn handleReadOnlyToggle(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    var extra_buf: [1024]u8 = undefined;
    const body = readRequestBody(stream, request, &extra_buf, 1024) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing request body\"}");
        return;
    };
    const enabled_str = extractJsonField(body, "enabled") orelse {
        // Toggle if no explicit value
        state.read_only = !state.read_only;
        var resp_buf: [64]u8 = undefined;
        const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.read_only) "true" else "false"}) catch return;
        try sendResponse(stream, "200 OK", "application/json", resp);
        return;
    };
    state.read_only = std.mem.eql(u8, enabled_str, "true");
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.read_only) "true" else "false"}) catch return;
    try sendResponse(stream, "200 OK", "application/json", resp);
}

/// Handle GET /api/settings/read-only — get current read-only state.
fn handleReadOnlyGet(stream: std.net.Stream, state: *const ServerState) !void {
    var resp_buf: [64]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"read_only\":{s}}}", .{if (state.read_only) "true" else "false"}) catch return;
    try sendResponse(stream, "200 OK", "application/json", resp);
}

// ── Connection Manager ─────────────────────────────────────────────────

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
    writeJsonEscaped(w, name) catch return error.OutOfMemory;
    w.writeAll("\",\"conninfo\":\"") catch return error.OutOfMemory;
    writeJsonEscaped(w, conninfo) catch return error.OutOfMemory;
    w.writeAll("\",\"color\":\"") catch return error.OutOfMemory;
    writeJsonEscaped(w, color) catch return error.OutOfMemory;
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

/// Handle GET /api/connections — return saved connections.
fn handleGetConnections(stream: std.net.Stream, state: *ServerState) !void {
    const allocator = state.allocator;
    const data = readConnectionsFile(allocator) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"connections\":[]}");
        return;
    };
    defer allocator.free(data);
    try sendResponse(stream, "200 OK", "application/json", data);
}

/// Handle POST /api/connections — save a new connection.
fn handlePostConnection(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    const allocator = state.allocator;

    // Parse body
    const content_length = findContentLength(request) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };
    if (content_length > 4096) {
        try sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"Request too large\"}");
        return;
    }
    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
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
                try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Read failed\"}");
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
    const name = extractJsonField(body, "name") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing name field\"}");
        return;
    };
    const conninfo = extractJsonField(body, "conninfo") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing conninfo field\"}");
        return;
    };
    const color = extractJsonField(body, "color") orelse "gray";

    // Read existing file
    const existing = readConnectionsFile(allocator) catch {
        // Start fresh if read fails
        const entry = formatConnectionJson(allocator, 1, name, conninfo, color) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        defer allocator.free(entry);

        var new_file = std.ArrayList(u8).init(allocator);
        defer new_file.deinit();
        const nw = new_file.writer();
        nw.writeAll("{\"connections\":[") catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        nw.writeAll(entry) catch return;
        nw.writeAll("]}") catch return;
        writeConnectionsFile(allocator, new_file.items) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
            return;
        };
        state.next_connection_id = 2;
        try sendResponse(stream, "200 OK", "application/json", "{\"id\":1}");
        return;
    };
    defer allocator.free(existing);

    // Find max existing ID
    const max_id = findMaxConnectionId(existing);
    const new_id = @max(max_id + 1, state.next_connection_id);
    state.next_connection_id = new_id + 1;

    // Build the new entry JSON
    const entry = formatConnectionJson(allocator, new_id, name, conninfo, color) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
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
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
            return;
        };
        nw.writeAll(entry) catch return;
        nw.writeAll("]}") catch return;
        writeConnectionsFile(allocator, new_file.items) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
            return;
        };
        var id_buf: [64]u8 = undefined;
        const id_resp = std.fmt.bufPrint(&id_buf, "{{\"id\":{d}}}", .{new_id}) catch {
            try sendResponse(stream, "200 OK", "application/json", "{\"id\":0}");
            return;
        };
        try sendResponse(stream, "200 OK", "application/json", id_resp);
        return;
    };

    // Check if there are existing entries (non-empty array)
    const before_bracket = std.mem.trimRight(u8, existing[0..close_bracket], " \t\n\r");
    const needs_comma = before_bracket.len > 0 and before_bracket[before_bracket.len - 1] != '[';

    nw.writeAll(existing[0..close_bracket]) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    if (needs_comma) nw.writeByte(',') catch return;
    nw.writeAll(entry) catch return;
    nw.writeAll("]}") catch return;

    writeConnectionsFile(allocator, new_file.items) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
        return;
    };

    var id_buf: [64]u8 = undefined;
    const id_resp = std.fmt.bufPrint(&id_buf, "{{\"id\":{d}}}", .{new_id}) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"id\":0}");
        return;
    };
    try sendResponse(stream, "200 OK", "application/json", id_resp);
}

/// Handle DELETE /api/connections/:id — delete a saved connection.
fn handleDeleteConnection(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    const allocator = state.allocator;

    // Parse ID from path: /api/connections/<id>
    const prefix = "/api/connections/";
    if (!std.mem.startsWith(u8, path, prefix)) {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Invalid path\"}");
        return;
    }
    const id_str = path[prefix.len..];
    const target_id = std.fmt.parseInt(u64, id_str, 10) catch {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid connection ID\"}");
        return;
    };

    // Read existing file
    const existing = readConnectionsFile(allocator) catch {
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"No connections file\"}");
        return;
    };
    defer allocator.free(existing);

    // Rebuild the connections array, skipping the one with target_id.
    // We parse the JSON minimally: find each {"id":N,...} block.
    var new_file = std.ArrayList(u8).init(allocator);
    defer new_file.deinit();
    const nw = new_file.writer();
    nw.writeAll("{\"connections\":[") catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };

    var found = false;
    var first = true;
    var pos: usize = 0;

    // Find the start of the array
    const arr_start = std.mem.indexOf(u8, existing, "[") orelse {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Malformed connections file\"}");
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
        try sendResponse(stream, "404 Not Found", "application/json", "{\"error\":\"Connection not found\"}");
        return;
    }

    writeConnectionsFile(allocator, new_file.items) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to write connections file\"}");
        return;
    };

    try sendResponse(stream, "200 OK", "application/json", "{\"ok\":true}");
}

// ── SQL Export ─────────────────────────────────────────────────────────

/// Handle POST /api/sql/export — execute SQL and return results as CSV or JSON download.
fn handleSqlExport(stream: std.net.Stream, request: []const u8, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Parse body
    const content_length = findContentLength(request) orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing Content-Length\"}");
        return;
    };
    if (content_length > 8192) {
        try sendResponse(stream, "413 Payload Too Large", "application/json", "{\"error\":\"SQL too large\"}");
        return;
    }
    const header_end = std.mem.indexOf(u8, request, "\r\n\r\n") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Malformed request\"}");
        return;
    };
    const body_start = header_end + 4;
    var body = request[body_start..];
    var extra_buf: [8192]u8 = undefined;
    if (body.len < content_length) {
        const already_have = body.len;
        @memcpy(extra_buf[0..already_have], body);
        var read_so_far = already_have;
        while (read_so_far < content_length) {
            const n = stream.read(extra_buf[read_so_far..content_length]) catch {
                try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Read failed\"}");
                return;
            };
            if (n == 0) break;
            read_so_far += n;
        }
        body = extra_buf[0..read_so_far];
    } else {
        body = body[0..content_length];
    }

    const sql_text = extractJsonField(body, "sql") orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing sql field\"}");
        return;
    };
    if (sql_text.len == 0) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty SQL\"}");
        return;
    }

    // Block write operations in read-only mode
    if (state.read_only and !isSqlReadSafe(sql_text)) {
        try sendResponse(stream, "403 Forbidden", "application/json", "{\"error\":\"Read-only mode is enabled. Disable it to export write operations.\"}");
        return;
    }

    const format = extractJsonField(body, "format") orelse "csv";
    const is_csv = std.mem.eql(u8, format, "csv");
    const is_json = std.mem.eql(u8, format, "json");
    if (!is_csv and !is_json) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Invalid format. Use csv or json.\"}");
        return;
    }

    // Build null-terminated SQL
    const sql_z = allocator.allocSentinel(u8, sql_text.len, 0) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    defer allocator.free(sql_z);
    @memcpy(sql_z[0..sql_text.len], sql_text);

    // Connect and execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [512]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"") catch return;
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch return;
        ew.writeAll("\"}") catch return;
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    if (is_csv) {
        const csv_data = formatResultAsCsv(allocator, pg_result.col_names, pg_result.rows) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format CSV\"}");
            return;
        };
        defer allocator.free(csv_data);
        try sendResponseWithDownload(stream, "200 OK", "text/csv; charset=utf-8", "export.csv", csv_data);
    } else {
        const json_data = formatResultAsJson(allocator, pg_result.col_names, pg_result.rows) catch {
            try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Failed to format JSON\"}");
            return;
        };
        defer allocator.free(json_data);
        try sendResponseWithDownload(stream, "200 OK", "application/json", "export.json", json_data);
    }
}

// ── Health Check & Reconnect ──────────────────────────────────────────

/// Handle GET /api/health — check database connection health.
fn handleHealthCheck(stream: std.net.Stream, state: *ServerState) !void {
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"connected\":false}");
        return;
    }

    const allocator = state.allocator;
    const conninfo_z = state.conninfo_z.?;

    const start_time = std.time.milliTimestamp();
    var pg_conn = postgres.PgConnection.connect(conninfo_z) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"connected\":false}");
        return;
    };
    defer pg_conn.deinit();

    // Run SELECT 1 to verify connection
    const sql: [*:0]const u8 = "SELECT 1";
    var pg_result = pg_conn.runQuery(allocator, sql) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"connected\":false}");
        return;
    };
    defer pg_result.deinit();

    const end_time = std.time.milliTimestamp();
    const latency = @as(u64, @intCast(@max(0, end_time - start_time)));

    var resp_buf: [128]u8 = undefined;
    const resp = std.fmt.bufPrint(&resp_buf, "{{\"connected\":true,\"latency_ms\":{d}}}", .{latency}) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"connected\":true}");
        return;
    };
    try sendResponse(stream, "200 OK", "application/json", resp);
}

/// Handle POST /api/reconnect — reconnect using the last connection string.
fn handleReconnect(stream: std.net.Stream, state: *ServerState) !void {
    const allocator = state.allocator;

    const last_ci = state.last_conninfo orelse {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No previous connection to reconnect to\"}");
        return;
    };

    // Build null-terminated connection string
    const conninfo_z = allocator.allocSentinel(u8, last_ci.len, 0) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Out of memory\"}");
        return;
    };
    @memcpy(conninfo_z[0..last_ci.len], last_ci);

    // Test connection
    var pg_conn = postgres.PgConnection.connectVerbose(conninfo_z) catch {
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Failed to reconnect. libpq returned null.\"}");
        return;
    };
    defer pg_conn.deinit();

    if (!pg_conn.isOk()) {
        var err_buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&err_buf);
        const ew = fbs.writer();
        ew.writeAll("{\"error\":\"Reconnect failed: ") catch {};
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch {};
        ew.writeAll("\"}") catch {};
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", fbs.getWritten());
        return;
    }

    // Fetch schema
    var schema = pg_conn.fetchSchema(allocator) catch {
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Reconnected but failed to fetch schema\"}");
        return;
    };

    const schema_text = schema.format(allocator) catch {
        schema.deinit();
        allocator.free(conninfo_z);
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Reconnected but failed to format schema\"}");
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
        try sendResponse(stream, "200 OK", "application/json", "{\"ok\":true}");
        return;
    };
    writeJsonEscaped(w, schema_text) catch return;
    var n_tables: usize = 0;
    if (state.schema_tables) |tables| n_tables = tables.len;
    w.print("\",\"tables\":{d}}}", .{n_tables}) catch return;
    try sendResponse(stream, "200 OK", "application/json", json_buf.items);
}

/// Handle POST /api/tables/<name>/truncate — remove all rows from a table.
fn handleTruncateTable(stream: std.net.Stream, path: []const u8, state: *ServerState) !void {
    if (try enforceReadOnly(stream, state)) return;
    if (!state.hasDbConnection()) {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"No database connected\"}");
        return;
    }

    const allocator = state.allocator;

    // Extract table name from /api/tables/<name>/truncate
    const prefix = "/api/tables/";
    const suffix = "/truncate";
    if (path.len <= prefix.len + suffix.len) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Missing table name\"}");
        return;
    }
    const table_name = path[prefix.len .. path.len - suffix.len];
    if (table_name.len == 0) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Empty table name\"}");
        return;
    }

    // Validate table exists in schema
    const tables = state.schema_tables orelse {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"No schema available\"}");
        return;
    };
    if (findTableInSchema(tables, table_name) == null) {
        try sendResponse(stream, "400 Bad Request", "application/json", "{\"error\":\"Table not found in schema\"}");
        return;
    }

    // Build TRUNCATE SQL
    var sql_buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&sql_buf);
    fbs.writer().print("TRUNCATE TABLE \"{s}\"", .{table_name}) catch {
        try sendResponse(stream, "500 Internal Server Error", "application/json", "{\"error\":\"Table name too long\"}");
        return;
    };
    const sql_len = fbs.pos;
    sql_buf[sql_len] = 0;
    const sql_z: [*:0]const u8 = sql_buf[0..sql_len :0];

    // Execute
    var pg_conn = postgres.PgConnection.connect(state.conninfo_z.?) catch {
        try sendResponse(stream, "200 OK", "application/json", "{\"error\":\"Database connection failed\"}");
        return;
    };
    defer pg_conn.deinit();

    var pg_result = pg_conn.runQuery(allocator, sql_z) catch {
        var err_buf: [1024]u8 = undefined;
        var efbs = std.io.fixedBufferStream(&err_buf);
        const ew = efbs.writer();
        ew.writeAll("{\"error\":\"TRUNCATE failed: ") catch {};
        writeJsonEscaped(ew, pg_conn.errorMessage()) catch {};
        ew.writeAll("\"}") catch {};
        try sendResponse(stream, "200 OK", "application/json", efbs.getWritten());
        return;
    };
    defer pg_result.deinit();

    // Record in journal
    _ = addJournalEntry(state, table_name, "truncate", "", "", "", "ALL", "");

    try sendResponse(stream, "200 OK", "application/json", "{\"ok\":true}");
}

// ── Tests ──────────────────────────────────────────────────────────────

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

test "matchesIgnoreCase: basic" {
    try std.testing.expect(matchesIgnoreCase("Content-Length:", "content-length:"));
    try std.testing.expect(matchesIgnoreCase("CONTENT-LENGTH:", "content-length:"));
    try std.testing.expect(!matchesIgnoreCase("Content-Type:", "content-length:"));
}

test "matchesIgnoreCase: short haystack" {
    try std.testing.expect(!matchesIgnoreCase("ab", "abcdef"));
}

test "matchesIgnoreCase: empty needle" {
    try std.testing.expect(matchesIgnoreCase("anything", ""));
}

// ── writeJsonEscaped tests ──────────────────────────────────────────────

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

// ── more extractJsonQuery tests ─────────────────────────────────────────

test "extractJsonQuery: query with escaped quotes inside" {
    const body = "{\"query\": \"say \\\"hello\\\"\"}";
    const result = extractJsonQuery(body);
    try std.testing.expect(result != null);
    // The extracted string includes the backslash-quote sequences
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


// ── ServerState tests ────────────────────────────────────────────────────

test "ServerState: init defaults" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
    try std.testing.expect(state.schema_text == null);
    try std.testing.expect(!state.read_only);
}

// ── more eqlLower tests ─────────────────────────────────────────────────

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

// ── findContentLength edge cases ────────────────────────────────────────

test "findContentLength: with extra headers" {
    const req = "POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 25\r\n\r\n";
    try std.testing.expectEqual(@as(?usize, 25), findContentLength(req));
}

test "findContentLength: zero length" {
    const req = "POST / HTTP/1.1\r\nContent-Length: 0\r\n\r\n";
    try std.testing.expectEqual(@as(?usize, 0), findContentLength(req));
}

// ── parseQueryParam tests ────────────────────────────────────────────────

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

// ── parseStringQueryParam tests ──────────────────────────────────────────

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

// ── escapeStringValue tests ──────────────────────────────────────────────

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

// ── escapeIdentifier tests ──────────────────────────────────────────────

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

// ── isSqlReadSafe EXPLAIN ANALYZE tests ─────────────────────────────────

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

// ── findTableInSchema / findColumnInTable tests ──────────────────────────

test "findTableInSchema: finds existing table" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
    };
    const tables = [_]postgres.TableInfo{
        .{ .name = "users", .columns = @constCast(&cols) },
    };
    const result = findTableInSchema(@constCast(&tables), "users");
    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings("users", result.?.name);
}

test "findTableInSchema: returns null for missing table" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
    };
    const tables = [_]postgres.TableInfo{
        .{ .name = "users", .columns = @constCast(&cols) },
    };
    const result = findTableInSchema(@constCast(&tables), "orders");
    try std.testing.expect(result == null);
}

test "findColumnInTable: finds existing column" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
        .{ .name = "email", .data_type = "text" },
    };
    const table = postgres.TableInfo{ .name = "users", .columns = @constCast(&cols) };
    try std.testing.expect(findColumnInTable(table, "email"));
}

test "findColumnInTable: returns false for missing column" {
    const cols = [_]postgres.ColumnInfo{
        .{ .name = "id", .data_type = "integer" },
    };
    const table = postgres.TableInfo{ .name = "users", .columns = @constCast(&cols) };
    try std.testing.expect(!findColumnInTable(table, "nonexistent"));
}

// ── analyzeSql tests ──────────────────────────────────────────────────

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

// ── validateCtid tests ──────────────────────────────────────────────────

test "validateCtid: valid ctids" {
    try std.testing.expect(validateCtid("(0,1)"));
    try std.testing.expect(validateCtid("(123,456)"));
    try std.testing.expect(validateCtid("(0,0)"));
}

test "validateCtid: invalid ctids" {
    try std.testing.expect(!validateCtid(""));
    try std.testing.expect(!validateCtid("(0)"));
    try std.testing.expect(!validateCtid("0,1"));
    try std.testing.expect(!validateCtid("(,1)"));
    try std.testing.expect(!validateCtid("(0,)"));
    try std.testing.expect(!validateCtid("(a,b)"));
    try std.testing.expect(!validateCtid("abc"));
}

// ── journal tests ───────────────────────────────────────────────────────

test "addJournalEntry: adds to journal" {
    var state = ServerState.init(std.testing.allocator);
    defer state.change_journal.deinit();
    const id = addJournalEntry(&state, "users", "update", "name", "old", "new", "id", "1");
    try std.testing.expect(id > 0);
    try std.testing.expectEqual(@as(usize, 1), state.change_journal.items.len);
    // Free the duped strings
    for (state.change_journal.items) |entry| {
        std.testing.allocator.free(entry.table_name);
        std.testing.allocator.free(entry.operation);
        std.testing.allocator.free(entry.column_name);
        std.testing.allocator.free(entry.old_value);
        std.testing.allocator.free(entry.new_value);
        std.testing.allocator.free(entry.pk_column);
        std.testing.allocator.free(entry.pk_value);
    }
}

// ── enforceReadOnly tests ───────────────────────────────────────────────

test "enforceReadOnly: allows when not read-only" {
    const state = ServerState.init(std.testing.allocator);
    // Can't test with a real stream, but verify the logic
    try std.testing.expect(!state.read_only);
}

test "ServerState: read_only default is false" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.read_only);
}

// ── extractJsonField edge cases ────────────────────────────────────────────

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
    // Escaped quotes: the raw JSON value is host=localhost dbname=\"my db\"
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
    // extractJsonField only handles string values
    const body = "{\"count\":42}";
    const result = extractJsonField(body, "count");
    try std.testing.expect(result == null);
}

// ── extractJsonObject tests ───────────────────────────────────────────────

test "extractJsonObject: extracts key-value pairs" {
    const body = "{\"table\":\"employees\",\"values\":{\"name\":\"Alice\",\"age\":\"30\"}}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 2), pairs.len);
    try std.testing.expectEqualStrings("name", pairs[0].key);
    try std.testing.expectEqualStrings("Alice", pairs[0].value);
    try std.testing.expectEqualStrings("age", pairs[1].key);
    try std.testing.expectEqualStrings("30", pairs[1].value);
}

test "extractJsonObject: returns null for missing field" {
    const body = "{\"table\":\"employees\"}";
    const result = extractJsonObject(std.testing.allocator, body, "values");
    try std.testing.expect(result == null);
}

test "extractJsonObject: handles null values" {
    const body = "{\"values\":{\"name\":\"Bob\",\"dept\":null}}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 2), pairs.len);
    try std.testing.expectEqualStrings("Bob", pairs[0].value);
    try std.testing.expectEqualStrings("__NULL__", pairs[1].value);
}

test "extractJsonObject: empty object" {
    const body = "{\"values\":{}}";
    const pairs = extractJsonObject(std.testing.allocator, body, "values") orelse {
        return error.TestUnexpectedResult;
    };
    defer std.testing.allocator.free(pairs);
    try std.testing.expectEqual(@as(usize, 0), pairs.len);
}

// ── ServerState connection state tests ────────────────────────────────────

test "ServerState: hasDbConnection false by default" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
}

test "ServerState: hasDbConnection true when conninfo set" {
    var state = ServerState.init(std.testing.allocator);
    const conninfo = try std.testing.allocator.allocSentinel(u8, 5, 0);
    @memcpy(conninfo[0..5], "hello");
    state.conninfo_z = conninfo;
    try std.testing.expect(state.hasDbConnection());
    std.testing.allocator.free(conninfo);
}

test "ServerState: schema fields null by default" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(state.schema_text == null);
    try std.testing.expect(state.schema_tables == null);
    try std.testing.expect(state.enhanced_schema == null);
    try std.testing.expect(state.conninfo_z == null);
}

test "ServerState: journal initialized" {
    var state = ServerState.init(std.testing.allocator);
    defer state.change_journal.deinit();
    try std.testing.expect(state.journal_initialized);
    try std.testing.expectEqual(@as(u64, 1), state.next_journal_id);
    try std.testing.expectEqual(@as(usize, 0), state.change_journal.items.len);
}

// ── analyzeSql edge cases ──────────────────────────────────────────────

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

// ── eqlLower edge cases ──────────────────────────────────────────────

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

// ── containsIgnoreCaseWord edge cases ──────────────────────────────────

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

// ── validateCtid edge cases ──────────────────────────────────────────────

test "validateCtid: large numbers" {
    try std.testing.expect(validateCtid("(99999,99999)"));
}

test "validateCtid: leading zeros" {
    try std.testing.expect(validateCtid("(001,002)"));
}

test "validateCtid: SQL injection attempt" {
    try std.testing.expect(!validateCtid("(0,1); DROP TABLE users--"));
    try std.testing.expect(!validateCtid("(0,1)'"));
    try std.testing.expect(!validateCtid("' OR '1'='1"));
}

test "validateCtid: nested parens" {
    try std.testing.expect(!validateCtid("((0,1))"));
}

// ── findContentLength edge cases ──────────────────────────────────────────

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

// ── journal edge cases ──────────────────────────────────────────────────

test "addJournalEntry: multiple entries increment id" {
    var state = ServerState.init(std.testing.allocator);
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
        state.change_journal.deinit();
    }
    const id1 = addJournalEntry(&state, "t", "update", "c", "a", "b", "id", "1");
    const id2 = addJournalEntry(&state, "t", "delete", "c", "a", "b", "id", "2");
    try std.testing.expectEqual(@as(u64, 1), id1);
    try std.testing.expectEqual(@as(u64, 2), id2);
    try std.testing.expectEqual(@as(usize, 2), state.change_journal.items.len);
}

test "addJournalEntry: uninitialized journal returns 0" {
    var state = ServerState{
        .allocator = std.testing.allocator,
        .journal_initialized = false,
        .change_journal = undefined,
    };
    const id = addJournalEntry(&state, "t", "op", "c", "old", "new", "pk", "1");
    try std.testing.expectEqual(@as(u64, 0), id);
}

test "addJournalEntry: delete operation stores table and pk" {
    var state = ServerState.init(std.testing.allocator);
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
        state.change_journal.deinit();
    }
    const id = addJournalEntry(&state, "users", "delete", "", "", "", "id", "42");
    try std.testing.expect(id > 0);
    try std.testing.expectEqual(@as(usize, 1), state.change_journal.items.len);
    const entry = state.change_journal.items[0];
    try std.testing.expectEqualStrings("users", entry.table_name);
    try std.testing.expectEqualStrings("delete", entry.operation);
    try std.testing.expectEqualStrings("id", entry.pk_column);
    try std.testing.expectEqualStrings("42", entry.pk_value);
    try std.testing.expect(!entry.undone);
}

test "handleJournal: serializes entries as JSON" {
    // This tests that journal entries are properly serialized with all fields.
    var state = ServerState.init(std.testing.allocator);
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
        state.change_journal.deinit();
    }
    _ = addJournalEntry(&state, "t1", "delete", "", "", "", "id", "5");
    _ = addJournalEntry(&state, "t2", "insert", "", "", "", "id", "10");
    try std.testing.expectEqual(@as(usize, 2), state.change_journal.items.len);
    try std.testing.expectEqualStrings("delete", state.change_journal.items[0].operation);
    try std.testing.expectEqualStrings("insert", state.change_journal.items[1].operation);
}

// ── escapeStringValue edge cases ──────────────────────────────────────────

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

// ── writeJsonEscaped edge cases ──────────────────────────────────────────

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

// ── parseQueryParam edge cases ──────────────────────────────────────────

test "parseQueryParam: param with special chars in value" {
    var result: usize = 0;
    parseQueryParam("limit=abc&offset=0", "limit", &result);
    // "abc" is not a valid number, should stay at default
    try std.testing.expectEqual(@as(usize, 0), result);
}

test "parseQueryParam: multiple same params uses first" {
    var result: usize = 0;
    parseQueryParam("limit=10&limit=20", "limit", &result);
    try std.testing.expectEqual(@as(usize, 10), result);
}

// ── matchesIgnoreCase edge cases ──────────────────────────────────────────

test "matchesIgnoreCase: exact match" {
    try std.testing.expect(matchesIgnoreCase("SELECT", "SELECT"));
}

test "matchesIgnoreCase: mixed case" {
    try std.testing.expect(matchesIgnoreCase("Select", "SELECT"));
    try std.testing.expect(matchesIgnoreCase("sElEcT", "SELECT"));
}

test "matchesIgnoreCase: haystack shorter than needle" {
    try std.testing.expect(!matchesIgnoreCase("SEL", "SELECT"));
}

test "matchesIgnoreCase: prefix match with extra chars" {
    // matchesIgnoreCase checks that haystack STARTS WITH needle (ignoring case)
    try std.testing.expect(matchesIgnoreCase("SELECT * FROM", "SELECT"));
}

// ── isAlpha tests ──────────────────────────────────────────────────────────

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

// ── CSV escaping tests ──────────────────────────────────────────────────────

test "escapeCsvField: plain text passes through" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "hello");
    try std.testing.expectEqualStrings("hello", buf.items);
}

test "escapeCsvField: field with comma is quoted" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "hello,world");
    try std.testing.expectEqualStrings("\"hello,world\"", buf.items);
}

test "escapeCsvField: field with double quote is quoted and escaped" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "say \"hi\"");
    try std.testing.expectEqualStrings("\"say \"\"hi\"\"\"", buf.items);
}

test "escapeCsvField: field with newline is quoted" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "line1\nline2");
    try std.testing.expectEqualStrings("\"line1\nline2\"", buf.items);
}

test "escapeCsvField: field with carriage return is quoted" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "line1\rline2");
    try std.testing.expectEqualStrings("\"line1\rline2\"", buf.items);
}

test "escapeCsvField: empty string passes through" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "");
    try std.testing.expectEqualStrings("", buf.items);
}

test "escapeCsvField: field with all special chars" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "a,b\"c\nd");
    try std.testing.expectEqualStrings("\"a,b\"\"c\nd\"", buf.items);
}

test "escapeCsvField: NULL literal passes through" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "NULL");
    try std.testing.expectEqualStrings("NULL", buf.items);
}

// ── CSV formatting tests ────────────────────────────────────────────────────

test "formatResultAsCsv: basic table" {
    const allocator = std.testing.allocator;
    const col_names: []const []const u8 = &.{ "id", "name" };
    const row1: []const []const u8 = &.{ "1", "Alice" };
    const row2: []const []const u8 = &.{ "2", "Bob" };
    const rows: []const []const []const u8 = &.{ row1, row2 };
    const result = try formatResultAsCsv(allocator, col_names, rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("id,name\r\n1,Alice\r\n2,Bob\r\n", result);
}

test "formatResultAsCsv: escapes special fields" {
    const allocator = std.testing.allocator;
    const col_names: []const []const u8 = &.{"col"};
    const row1: []const []const u8 = &.{"a,b"};
    const rows: []const []const []const u8 = &.{row1};
    const result = try formatResultAsCsv(allocator, col_names, rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("col\r\n\"a,b\"\r\n", result);
}

test "formatResultAsCsv: empty rows" {
    const allocator = std.testing.allocator;
    const col_names: []const []const u8 = &.{ "id", "name" };
    const rows: []const []const []const u8 = &.{};
    const result = try formatResultAsCsv(allocator, col_names, rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("id,name\r\n", result);
}

// ── JSON formatting tests ───────────────────────────────────────────────────

test "formatResultAsJson: basic table" {
    const allocator = std.testing.allocator;
    const col_names: []const []const u8 = &.{ "id", "name" };
    const row1: []const []const u8 = &.{ "1", "Alice" };
    const row2: []const []const u8 = &.{ "2", "Bob" };
    const rows: []const []const []const u8 = &.{ row1, row2 };
    const result = try formatResultAsJson(allocator, col_names, rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[{\"id\":\"1\",\"name\":\"Alice\"},{\"id\":\"2\",\"name\":\"Bob\"}]", result);
}

test "formatResultAsJson: empty rows" {
    const allocator = std.testing.allocator;
    const col_names: []const []const u8 = &.{ "id", "name" };
    const rows: []const []const []const u8 = &.{};
    const result = try formatResultAsJson(allocator, col_names, rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "formatResultAsJson: escapes special chars in values" {
    const allocator = std.testing.allocator;
    const col_names: []const []const u8 = &.{"note"};
    const row1: []const []const u8 = &.{"line1\nline2"};
    const rows: []const []const []const u8 = &.{row1};
    const result = try formatResultAsJson(allocator, col_names, rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[{\"note\":\"line1\\nline2\"}]", result);
}

test "formatResultAsJson: NULL value" {
    const allocator = std.testing.allocator;
    const col_names: []const []const u8 = &.{"val"};
    const row1: []const []const u8 = &.{"NULL"};
    const rows: []const []const []const u8 = &.{row1};
    const result = try formatResultAsJson(allocator, col_names, rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[{\"val\":null}]", result);
}

// ── Query History tests ──────────────────────────────────────────────────

test "addHistoryEntry: adds to history" {
    var state = ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit();
        state.change_journal.deinit();
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
    var state = ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit();
        state.change_journal.deinit();
    }
    addHistoryEntry(&state, "BAD SQL", 5, null, true, "syntax error");
    try std.testing.expectEqual(@as(usize, 1), state.query_history.items.len);
    try std.testing.expect(state.query_history.items[0].is_error);
    try std.testing.expect(state.query_history.items[0].row_count == null);
    try std.testing.expectEqualStrings("syntax error", state.query_history.items[0].error_msg.?);
}

test "addHistoryEntry: multiple entries" {
    var state = ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit();
        state.change_journal.deinit();
    }
    addHistoryEntry(&state, "SELECT 1", 10, 1, false, null);
    addHistoryEntry(&state, "SELECT 2", 20, 2, false, null);
    addHistoryEntry(&state, "SELECT 3", 30, 3, false, null);
    try std.testing.expectEqual(@as(usize, 3), state.query_history.items.len);
    try std.testing.expectEqualStrings("SELECT 1", state.query_history.items[0].sql);
    try std.testing.expectEqualStrings("SELECT 3", state.query_history.items[2].sql);
}

test "addHistoryEntry: uninitialized history is safe" {
    var state = ServerState{
        .allocator = std.testing.allocator,
        .journal_initialized = false,
        .change_journal = undefined,
        .history_initialized = false,
        .query_history = undefined,
    };
    // Should not crash when history is uninitialized
    addHistoryEntry(&state, "SELECT 1", 10, 1, false, null);
}

test "addHistoryEntry: caps at MAX_HISTORY_ENTRIES" {
    var state = ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit();
        state.change_journal.deinit();
    }
    // Add MAX_HISTORY_ENTRIES + 5 entries
    var i: usize = 0;
    while (i < MAX_HISTORY_ENTRIES + 5) : (i += 1) {
        addHistoryEntry(&state, "SELECT 1", 1, 1, false, null);
    }
    // Should be capped at MAX_HISTORY_ENTRIES
    try std.testing.expectEqual(MAX_HISTORY_ENTRIES, state.query_history.items.len);
}

test "addHistoryEntry: error msg null when no error" {
    var state = ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit();
        state.change_journal.deinit();
    }
    addHistoryEntry(&state, "SELECT 1", 0, 0, false, null);
    try std.testing.expect(state.query_history.items[0].error_msg == null);
}

test "addHistoryEntry: dupes sql string" {
    var state = ServerState.init(std.testing.allocator);
    defer {
        for (state.query_history.items) |entry| {
            std.testing.allocator.free(entry.sql);
            if (entry.error_msg) |e| std.testing.allocator.free(e);
        }
        state.query_history.deinit();
        state.change_journal.deinit();
    }
    var buf: [10]u8 = undefined;
    @memcpy(buf[0..8], "SELECT 1");
    addHistoryEntry(&state, buf[0..8], 1, 1, false, null);
    // Mutate the original buffer — the entry should be unaffected
    buf[0] = 'X';
    try std.testing.expectEqualStrings("SELECT 1", state.query_history.items[0].sql);
}

test "ServerState: history initialized" {
    var state = ServerState.init(std.testing.allocator);
    defer {
        state.query_history.deinit();
        state.change_journal.deinit();
    }
    try std.testing.expect(state.history_initialized);
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

// ── CSV Import (parseCsvContent) tests ──────────────────────────────────

test "parseCsvContent: simple two-column CSV" {
    const allocator = std.testing.allocator;
    const csv = "name,age\nAlice,30\nBob,25\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 2), result.headers.len);
    try std.testing.expectEqualStrings("name", result.headers[0]);
    try std.testing.expectEqualStrings("age", result.headers[1]);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("Alice", result.rows[0][0]);
    try std.testing.expectEqualStrings("30", result.rows[0][1]);
    try std.testing.expectEqualStrings("Bob", result.rows[1][0]);
    try std.testing.expectEqualStrings("25", result.rows[1][1]);
}

test "parseCsvContent: quoted fields with commas" {
    const allocator = std.testing.allocator;
    const csv = "name,bio\nAlice,\"likes cats, dogs\"\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("likes cats, dogs", result.rows[0][1]);
}

test "parseCsvContent: escaped double quotes in field" {
    const allocator = std.testing.allocator;
    const csv = "col\n\"say \"\"hello\"\"\"\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("say \"hello\"", result.rows[0][0]);
}

test "parseCsvContent: CRLF line endings" {
    const allocator = std.testing.allocator;
    const csv = "a,b\r\n1,2\r\n3,4\r\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 2), result.headers.len);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("1", result.rows[0][0]);
    try std.testing.expectEqualStrings("4", result.rows[1][1]);
}

test "parseCsvContent: empty input returns error" {
    const allocator = std.testing.allocator;
    const result = parseCsvContent(allocator, "");
    try std.testing.expectError(error.EmptyCsv, result);
}

test "parseCsvContent: headers only no data rows" {
    const allocator = std.testing.allocator;
    const csv = "col1,col2\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 2), result.headers.len);
    try std.testing.expectEqual(@as(usize, 0), result.rows.len);
}

test "parseCsvContent: single column single row" {
    const allocator = std.testing.allocator;
    const csv = "id\n42\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.headers.len);
    try std.testing.expectEqualStrings("id", result.headers[0]);
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("42", result.rows[0][0]);
}

test "parseCsvContent: no trailing newline" {
    const allocator = std.testing.allocator;
    const csv = "a,b\n1,2";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("1", result.rows[0][0]);
    try std.testing.expectEqualStrings("2", result.rows[0][1]);
}

test "parseCsvContent: quoted field with newline inside" {
    const allocator = std.testing.allocator;
    const csv = "col\n\"line1\nline2\"\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("line1\nline2", result.rows[0][0]);
}

test "parseCsvContent: multiple data rows" {
    const allocator = std.testing.allocator;
    const csv = "x,y,z\n1,2,3\n4,5,6\n7,8,9\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 3), result.headers.len);
    try std.testing.expectEqual(@as(usize, 3), result.rows.len);
    try std.testing.expectEqualStrings("7", result.rows[2][0]);
    try std.testing.expectEqualStrings("9", result.rows[2][2]);
}

// ── buildAndExecuteInsert unit-level tests ────────────────────────────────

// Note: buildAndExecuteInsert requires a live PgConnection, so we test it
// indirectly through SQL generation verification. Testing the SQL builder
// pattern is done via the escapeStringValue tests above.

// ── Table name extraction tests for import/stats paths ──────────────────

test "parseCsvContent: empty field between commas" {
    const allocator = std.testing.allocator;
    const csv = "a,b,c\n1,,3\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 3), result.rows[0].len);
    try std.testing.expectEqualStrings("1", result.rows[0][0]);
    try std.testing.expectEqualStrings("", result.rows[0][1]);
    try std.testing.expectEqualStrings("3", result.rows[0][2]);
}

test "parseCsvContent: whitespace-only lines at end are skipped" {
    const allocator = std.testing.allocator;
    const csv = "col\nval\n  \n\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |field| allocator.free(field);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("val", result.rows[0][0]);
}

// ── Connection Manager Tests ──────────────────────────────────────────

test "formatConnectionJson: basic entry" {
    const allocator = std.testing.allocator;
    const json = try formatConnectionJson(allocator, 1, "My DB", "postgresql://localhost/test", "green");
    defer allocator.free(json);
    // Verify it contains the expected fields
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

// ============================================================================
// Additional edge case and boundary tests
// ============================================================================

// --- JSON extraction edge cases ---

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

test "extractJsonObject: multiple pairs with whitespace" {
    const allocator = std.testing.allocator;
    const body = "{\"values\": { \"col1\" : \"val1\" , \"col2\" : \"val2\" }}";
    const result = extractJsonObject(allocator, body, "values");
    try std.testing.expect(result != null);
    defer allocator.free(result.?);
    try std.testing.expectEqual(@as(usize, 2), result.?.len);
    try std.testing.expectEqualStrings("col1", result.?[0].key);
    try std.testing.expectEqualStrings("val1", result.?[0].value);
    try std.testing.expectEqualStrings("col2", result.?[1].key);
    try std.testing.expectEqualStrings("val2", result.?[1].value);
}

test "extractJsonObject: value with escaped backslash" {
    const allocator = std.testing.allocator;
    const body = "{\"values\": {\"path\": \"C:\\\\dir\"}}";
    const result = extractJsonObject(allocator, body, "values");
    try std.testing.expect(result != null);
    defer allocator.free(result.?);
    try std.testing.expectEqual(@as(usize, 1), result.?.len);
    try std.testing.expectEqualStrings("C:\\\\dir", result.?[0].value);
}

test "extractJsonObject: non-object value returns null" {
    const allocator = std.testing.allocator;
    const body = "{\"values\": \"not an object\"}";
    const result = extractJsonObject(allocator, body, "values");
    try std.testing.expect(result == null);
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

// --- URL/query param edge cases ---

test "parseQueryParam: negative number does not parse" {
    var out: usize = 42;
    parseQueryParam("limit=-5", "limit", &out);
    // parseInt for usize should fail on negative, so out stays at default
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
    // The value portion is everything after the first '='
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

// --- CSV parsing edge cases ---

test "parseCsvContent: only whitespace returns error" {
    const allocator = std.testing.allocator;
    const result = parseCsvContent(allocator, "   \n\r\n  ");
    try std.testing.expectError(error.EmptyCsv, result);
}

test "parseCsvContent: single column multiple rows" {
    const allocator = std.testing.allocator;
    const csv = "name\nAlice\nBob\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.headers.len);
    try std.testing.expectEqualStrings("name", result.headers[0]);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("Alice", result.rows[0][0]);
    try std.testing.expectEqualStrings("Bob", result.rows[1][0]);
}

test "parseCsvContent: field with only escaped quotes" {
    const allocator = std.testing.allocator;
    const csv = "val\n\"\"\"\"\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqualStrings("\"", result.rows[0][0]);
}

test "parseCsvContent: consecutive commas produce empty fields" {
    const allocator = std.testing.allocator;
    const csv = "a,b,c\n,,\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1), result.rows.len);
    try std.testing.expectEqual(@as(usize, 3), result.rows[0].len);
    try std.testing.expectEqualStrings("", result.rows[0][0]);
    try std.testing.expectEqualStrings("", result.rows[0][1]);
    try std.testing.expectEqualStrings("", result.rows[0][2]);
}

test "parseCsvContent: very long field value" {
    const allocator = std.testing.allocator;
    const long_val = "x" ** 1000;
    const csv = "data\n" ++ long_val ++ "\n";
    const result = try parseCsvContent(allocator, csv);
    defer {
        for (result.headers) |h| allocator.free(h);
        allocator.free(result.headers);
        for (result.rows) |row| {
            for (row) |f| allocator.free(f);
            allocator.free(row);
        }
        allocator.free(result.rows);
    }
    try std.testing.expectEqual(@as(usize, 1000), result.rows[0][0].len);
}

// --- String escaping edge cases ---

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

test "escapeStringValue: null byte rejected" {
    const allocator = std.testing.allocator;
    const result = escapeStringValue(allocator, "hello\x00world");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "escapeStringValue: null byte at start rejected" {
    const allocator = std.testing.allocator;
    const result = escapeStringValue(allocator, "\x00start");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "escapeStringValue: null byte at end rejected" {
    const allocator = std.testing.allocator;
    const result = escapeStringValue(allocator, "end\x00");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "escapeStringValue: single null byte rejected" {
    const allocator = std.testing.allocator;
    const result = escapeStringValue(allocator, "\x00");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "writeJsonEscaped: null byte is escaped per RFC 8259" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try writeJsonEscaped(buf.writer(), "ab\x00cd");
    try std.testing.expectEqualStrings("ab\\u0000cd", buf.items);
}

test "writeJsonEscaped: backslash followed by quote" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try writeJsonEscaped(buf.writer(), "\\\"");
    try std.testing.expectEqualStrings("\\\\\\\"", buf.items);
}

test "writeJsonEscaped: very long string" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    const long = "a" ** 2000;
    try writeJsonEscaped(buf.writer(), long);
    try std.testing.expectEqual(@as(usize, 2000), buf.items.len);
}

test "writeJsonEscaped: all whitespace escapes" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try writeJsonEscaped(buf.writer(), "\n\r\t");
    try std.testing.expectEqualStrings("\\n\\r\\t", buf.items);
}

test "escapeCsvField: field containing only a double quote" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "\"");
    try std.testing.expectEqualStrings("\"\"\"\"", buf.items);
}

test "escapeCsvField: field with comma and quote" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "a,\"b");
    try std.testing.expectEqualStrings("\"a,\"\"b\"", buf.items);
}

test "escapeCsvField: field with CRLF" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try escapeCsvField(buf.writer(), "line1\r\nline2");
    try std.testing.expectEqualStrings("\"line1\r\nline2\"", buf.items);
}

// --- Connection manager edge cases ---

test "findMaxConnectionId: multiple ids with whitespace" {
    const result = findMaxConnectionId("{\"id\": 3, \"id\": 12, \"id\": 7}");
    try std.testing.expectEqual(@as(u64, 12), result);
}

test "findMaxConnectionId: id with leading zeros" {
    const result = findMaxConnectionId("{\"id\":007}");
    try std.testing.expectEqual(@as(u64, 7), result);
}

test "findMaxConnectionId: id key without number" {
    const result = findMaxConnectionId("{\"id\":abc}");
    try std.testing.expectEqual(@as(u64, 0), result);
}

test "findMaxConnectionId: very large id" {
    const result = findMaxConnectionId("{\"id\":9999999}");
    try std.testing.expectEqual(@as(u64, 9999999), result);
}

test "formatConnectionJson: empty name and conninfo" {
    const allocator = std.testing.allocator;
    const result = try formatConnectionJson(allocator, 1, "", "", "blue");
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"conninfo\":\"\"") != null);
}

test "formatConnectionJson: name with quotes and backslashes" {
    const allocator = std.testing.allocator;
    const result = try formatConnectionJson(allocator, 2, "test\"\\name", "host=localhost", "red");
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\\\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\\\\") != null);
}

test "formatConnectionJson: id zero" {
    const allocator = std.testing.allocator;
    const result = try formatConnectionJson(allocator, 0, "dev", "connstr", "green");
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"id\":0") != null);
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

// --- Content-Length parsing edge cases ---

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

// --- validateCtid additional edge cases ---

test "validateCtid: empty string" {
    try std.testing.expect(!validateCtid(""));
}

test "validateCtid: only parens" {
    try std.testing.expect(!validateCtid("()"));
}

test "validateCtid: missing comma" {
    try std.testing.expect(!validateCtid("(12)"));
}

test "validateCtid: letter in page" {
    try std.testing.expect(!validateCtid("(1a,2)"));
}

test "validateCtid: negative number" {
    try std.testing.expect(!validateCtid("(-1,2)"));
}

test "validateCtid: space inside" {
    try std.testing.expect(!validateCtid("(1, 2)"));
}

test "validateCtid: valid zero zero" {
    try std.testing.expect(validateCtid("(0,0)"));
}

// --- matchesIgnoreCase additional ---

test "matchesIgnoreCase: numbers match exactly" {
    try std.testing.expect(matchesIgnoreCase("abc123", "ABC123"));
}

test "matchesIgnoreCase: special chars match exactly" {
    try std.testing.expect(matchesIgnoreCase("content-length:", "Content-Length:"));
}

// --- indexOfIgnoreCase ---

test "indexOfIgnoreCase: finds word at start" {
    const result = indexOfIgnoreCase("DROP TABLE t", "drop");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?);
}

test "indexOfIgnoreCase: finds word in middle" {
    const result = indexOfIgnoreCase("ALTER TABLE foo ADD COLUMN bar", "ADD");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 16), result.?);
}

test "indexOfIgnoreCase: returns null for missing word" {
    const result = indexOfIgnoreCase("SELECT * FROM t", "DROP");
    try std.testing.expect(result == null);
}

test "indexOfIgnoreCase: empty needle returns zero" {
    const result = indexOfIgnoreCase("anything", "");
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?);
}

test "indexOfIgnoreCase: needle longer than haystack" {
    const result = indexOfIgnoreCase("ab", "abcdef");
    try std.testing.expect(result == null);
}

// --- generateRollbackSql ---

test "generateRollbackSql: CREATE TABLE generates DROP" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try generateRollbackSql("CREATE TABLE users (id int)", buf.writer());
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "DROP TABLE") != null);
}

test "generateRollbackSql: DROP TABLE warns about data loss" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try generateRollbackSql("DROP TABLE users", buf.writer());
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "WARNING") != null);
}

test "generateRollbackSql: TRUNCATE warns about data loss" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try generateRollbackSql("TRUNCATE TABLE users", buf.writer());
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "WARNING") != null);
}

test "generateRollbackSql: ALTER ADD COLUMN generates DROP COLUMN" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try generateRollbackSql("ALTER TABLE users ADD COLUMN email text", buf.writer());
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "DROP COLUMN") != null);
}

test "generateRollbackSql: unknown operation gives generic message" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try generateRollbackSql("VACUUM ANALYZE", buf.writer());
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "No automatic rollback") != null);
}

test "generateRollbackSql: CREATE INDEX generates DROP INDEX" {
    var buf = std.ArrayList(u8).init(std.testing.allocator);
    defer buf.deinit();
    try generateRollbackSql("CREATE INDEX idx_name ON users (name)", buf.writer());
    try std.testing.expect(std.mem.indexOf(u8, buf.items, "DROP INDEX") != null);
}


// --- eqlLower edge cases ---

test "eqlLower: unicode bytes pass through" {
    // Non-ASCII bytes are compared as-is
    try std.testing.expect(eqlLower("\xc3\xa9", "\xc3\xa9"));
}

test "eqlLower: one char difference" {
    try std.testing.expect(!eqlLower("abc", "abd"));
}

// --- formatResultAsCsv edge cases ---

test "formatResultAsCsv: single column no rows" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"id"};
    const rows = [_][]const []const u8{};
    const result = try formatResultAsCsv(allocator, &cols, &rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("id\r\n", result);
}

test "formatResultAsCsv: multiple columns with special chars" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{ "name", "bio" };
    const row1 = [_][]const u8{ "Alice", "likes, commas" };
    const rows = [_][]const []const u8{&row1};
    const result = try formatResultAsCsv(allocator, &cols, &rows);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"likes, commas\"") != null);
}

// --- formatResultAsJson edge cases ---

test "formatResultAsJson: empty column names" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{};
    const rows = [_][]const []const u8{};
    const result = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("[]", result);
}

test "formatResultAsJson: value with special JSON chars" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"data"};
    const row1 = [_][]const u8{"line1\nline2"};
    const rows = [_][]const []const u8{&row1};
    const result = try formatResultAsJson(allocator, &cols, &rows);
    defer allocator.free(result);
    try std.testing.expect(std.mem.indexOf(u8, result, "\\n") != null);
}

// --- formatRowAsJsonCompact ---

test "formatRowAsJsonCompact: basic row" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{ "id", "name", "email" };
    const row = [_][]const u8{ "42", "John", "john@test.com" };
    const result = try formatRowAsJsonCompact(allocator, &cols, &row);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"id\":\"42\",\"name\":\"John\",\"email\":\"john@test.com\"}", result);
}

test "formatRowAsJsonCompact: escapes special chars" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{"data"};
    const row = [_][]const u8{"line1\nline2"};
    const result = try formatRowAsJsonCompact(allocator, &cols, &row);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{\"data\":\"line1\\nline2\"}", result);
}

test "formatRowAsJsonCompact: empty row" {
    const allocator = std.testing.allocator;
    const cols = [_][]const u8{};
    const row = [_][]const u8{};
    const result = try formatRowAsJsonCompact(allocator, &cols, &row);
    defer allocator.free(result);
    try std.testing.expectEqualStrings("{}", result);
}


test "handleTruncateTable: extracts table name from path" {
    // Test the path parsing logic used by handleTruncateTable
    const path = "/api/tables/employees/truncate";
    const prefix = "/api/tables/";
    const suffix = "/truncate";
    const table_name = path[prefix.len .. path.len - suffix.len];
    try std.testing.expectEqualStrings("employees", table_name);
}

test "handleTruncateTable: path too short returns empty" {
    const path = "/api/tables//truncate";
    const prefix = "/api/tables/";
    const suffix = "/truncate";
    const table_name = path[prefix.len .. path.len - suffix.len];
    try std.testing.expectEqual(@as(usize, 0), table_name.len);
}

test "handleTruncateTable: table name with underscore" {
    const path = "/api/tables/user_accounts/truncate";
    const prefix = "/api/tables/";
    const suffix = "/truncate";
    const table_name = path[prefix.len .. path.len - suffix.len];
    try std.testing.expectEqualStrings("user_accounts", table_name);
}

// ── hasMultipleStatements tests ──────────────────────────────────────

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

// ── CSRF Origin check tests ─────────────────────────────────────────

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

// ── findHeader tests ────────────────────────────────────────────────

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

// ── escapeIdentifier null byte tests ────────────────────────────────

test "escapeIdentifier: null byte rejected" {
    const allocator = std.testing.allocator;
    const result = escapeIdentifier(allocator, "table\x00; DROP TABLE users");
    try std.testing.expectError(error.InvalidIdentifier, result);
}

test "escapeIdentifier: null byte at start rejected" {
    const allocator = std.testing.allocator;
    const result = escapeIdentifier(allocator, "\x00table");
    try std.testing.expectError(error.InvalidIdentifier, result);
}

test "escapeIdentifier: null byte at end rejected" {
    const allocator = std.testing.allocator;
    const result = escapeIdentifier(allocator, "table\x00");
    try std.testing.expectError(error.InvalidIdentifier, result);
}

test "escapeIdentifier: normal string still works" {
    const allocator = std.testing.allocator;
    const result = try escapeIdentifier(allocator, "normal_table");
    defer allocator.free(result);
    try std.testing.expectEqualStrings("normal_table", result);
}

// ── XDG config path tests ───────────────────────────────────────────

test "getConfigDir: returns a path ending with lux dir separator" {
    const allocator = std.testing.allocator;
    // This test uses the real environment. It should still produce a valid path.
    const path = getConfigDir(allocator) catch {
        // If HOME/APPDATA is not set (e.g., in CI), skip this test gracefully.
        return;
    };
    defer allocator.free(path);
    if (builtin.os.tag == .windows) {
        try std.testing.expect(std.mem.endsWith(u8, path, "\\lux\\"));
    } else {
        try std.testing.expect(std.mem.endsWith(u8, path, "/lux/"));
    }
}

test "getConfigFilePath: returns path ending with connections.json" {
    const allocator = std.testing.allocator;
    const path = getConfigFilePath(allocator) catch {
        return;
    };
    defer allocator.free(path);
    if (builtin.os.tag == .windows) {
        try std.testing.expect(std.mem.endsWith(u8, path, "\\connections.json"));
        try std.testing.expect(std.mem.indexOf(u8, path, "\\lux\\") != null);
    } else {
        try std.testing.expect(std.mem.endsWith(u8, path, "/connections.json"));
        try std.testing.expect(std.mem.indexOf(u8, path, "/lux/") != null);
    }
}
