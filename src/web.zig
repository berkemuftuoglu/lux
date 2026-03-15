const std = @import("std");
const builtin = @import("builtin");
const postgres = @import("postgres");
const utils = @import("utils");
const crud = @import("crud");
const schema_mod = @import("schema");
const export_mod = @import("export");
const sql_mod = @import("sql");

const log = std.log.scoped(.web);

var shutdown_requested = std.atomic.Value(bool).init(false);

fn handleSignal(_: c_int) callconv(.c) void {
    shutdown_requested.store(true, .release);
}

const index_html = @embedFile("static/index.html");
const styles_css = @embedFile("static/css/styles.css");
const state_js = @embedFile("static/js/state.js");
const utils_js = @embedFile("static/js/utils.js");
const connection_js = @embedFile("static/js/connection.js");
const sql_tabs_js = @embedFile("static/js/sql-tabs.js");
const sql_js_file = @embedFile("static/js/sql.js");
const saved_queries_js = @embedFile("static/js/saved-queries.js");
const cmd_palette_js = @embedFile("static/js/cmd-palette.js");
const table_create_js = @embedFile("static/js/table-create.js");
const table_ops_js = @embedFile("static/js/table-ops.js");
const app_js = @embedFile("static/js/app.js");
const grid_js = @embedFile("static/js/grid.js");
const sidebar_js = @embedFile("static/js/sidebar.js");
const crud_js = @embedFile("static/js/crud.js");

const StaticResult = struct { data: []const u8, is_heap: bool };

/// In Debug builds, reads the file fresh from disk on each request (dev hot-reload).
/// In release builds, returns the compile-time embedded bytes.
/// Falls back to embedded if the disk file is missing (wrong CWD, deleted file).
fn readStaticFile(allocator: std.mem.Allocator, disk_path: []const u8, embedded: []const u8) StaticResult {
    if (comptime builtin.mode != .Debug) return .{ .data = embedded, .is_heap = false };
    // Debug: read from disk for hot-reload
    const data = std.fs.cwd().readFileAlloc(allocator, disk_path, 4 * 1024 * 1024) catch |err| {
        log.warn("dev: could not read '{s}' ({s}), using embedded", .{ disk_path, @errorName(err) });
        return .{ .data = embedded, .is_heap = false };
    };
    return .{ .data = data, .is_heap = true };
}

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

pub const max_journal_entries = 10000;
pub const max_history_entries = 500;

pub const QueryHistoryEntry = struct {
    sql: []const u8,
    timestamp: i64,
    duration_ms: u64,
    row_count: ?usize,
    is_error: bool,
    error_msg: ?[]const u8,
};

pub const ServerState = struct {
    allocator: std.mem.Allocator,
    /// Stored Postgres connection string (null-terminated) for per-query connections.
    conninfo_z: ?[:0]u8 = null,
    /// Schema column info for table/column resolution.
    schema_tables: ?[]postgres.TableInfo = null,
    schema_text: ?[]u8 = null,
    /// Enhanced schema with PK, FK, ENUM, nullability info.
    enhanced_schema: ?[]postgres.EnhancedTableInfo = null,
    /// Read-only mode — blocks DML/DDL operations.
    read_only: bool = false,
    change_journal: std.ArrayList(ChangeEntry) = .{},
    next_journal_id: u64 = 1,
    query_history: std.ArrayList(QueryHistoryEntry) = .{},
    /// Last successful connection string (for reconnect).
    last_conninfo: ?[]const u8 = null,
    /// Next auto-increment ID for saved connections.
    next_connection_id: u64 = 1,
    /// Port the server is listening on (for CSRF origin checks).
    port: u16 = 8080,

    pub fn init(allocator: std.mem.Allocator) ServerState {
        return .{
            .allocator = allocator,
            .change_journal = .{},
            .query_history = .{},
        };
    }

    pub fn hasDbConnection(self: *const ServerState) bool {
        return self.conninfo_z != null;
    }
};

pub fn serve(
    state: *ServerState,
    port: u16,
    bind_addr: []const u8,
) !void {
    state.port = port;

    const address = std.net.Address.parseIp(bind_addr, port) catch {
        log.err("invalid bind address '{s}'", .{bind_addr});
        std.process.exit(1);
    };

    var server = address.listen(.{
        .reuse_address = true,
    }) catch {
        log.err("failed to bind to port {d}", .{port});
        std.process.exit(1);
    };
    defer server.deinit();

    log.info("Lux web UI running at http://{s}:{d}", .{ bind_addr, port });
    log.info("open this URL in your browser, press Ctrl-C to stop", .{});
    if (comptime builtin.mode == .Debug) {
        log.warn("DEV MODE: static assets served from disk (src/static/), not embedded", .{});
        log.warn("edit CSS/JS/HTML and refresh browser — no rebuild needed", .{});
    }

    // Install signal handlers for graceful shutdown
    var sa = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    while (!shutdown_requested.load(.acquire)) {
        const conn = server.accept() catch |err| {
            if (shutdown_requested.load(.acquire)) break;
            log.warn("accept failed: {s}", .{@errorName(err)});
            continue;
        };
        defer conn.stream.close();

        // Set 5-second receive timeout so idle clients don't hold connections forever
        const timeout = std.posix.timeval{ .sec = 5, .usec = 0 };
        std.posix.setsockopt(conn.stream.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&timeout)) catch |err| {
            log.warn("failed to set socket timeout: {s}", .{@errorName(err)});
        };

        handleConnection(conn.stream, state) catch |err| {
            log.debug("request error: {s}", .{@errorName(err)});
        };
    }

    log.info("shutting down", .{});
}

const max_request_size = 8192;

fn handleConnection(
    stream: std.net.Stream,
    state: *ServerState,
) !void {
    var buf: [max_request_size]u8 = undefined;
    var total_read: usize = 0;

    // Read request headers (look for \r\n\r\n)
    while (total_read < buf.len) {
        const n = stream.read(buf[total_read..]) catch return;
        if (n == 0) return; // Connection closed
        total_read += n;
        if (std.mem.indexOf(u8, buf[0..total_read], "\r\n\r\n") != null) break;
    }

    // If the buffer is full and we never found \r\n\r\n, the request is oversized
    if (total_read == buf.len and std.mem.indexOf(u8, buf[0..total_read], "\r\n\r\n") == null) {
        utils.sendResponse(stream, "400 Bad Request", "text/plain", "Request too large") catch return;
        return;
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
        if (!utils.checkOrigin(request, state.port)) {
            try utils.sendResponse(stream, "403 Forbidden", "text/plain", "Forbidden: cross-origin request");
            return;
        }
    }

    if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/")) {
        const r = readStaticFile(state.allocator, "src/static/index.html", index_html);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendHtmlResponseWithCsp(stream, r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/favicon.ico")) {
        try utils.sendResponse(stream, "204 No Content", "image/x-icon", "");
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/css/styles.css")) {
        const r = readStaticFile(state.allocator, "src/static/css/styles.css", styles_css);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "text/css", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/state.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/state.js", state_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/utils.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/utils.js", utils_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/connection.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/connection.js", connection_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/sql-tabs.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/sql-tabs.js", sql_tabs_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/sql.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/sql.js", sql_js_file);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/saved-queries.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/saved-queries.js", saved_queries_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/cmd-palette.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/cmd-palette.js", cmd_palette_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/table-create.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/table-create.js", table_create_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/table-ops.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/table-ops.js", table_ops_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/app.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/app.js", app_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/grid.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/grid.js", grid_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/sidebar.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/sidebar.js", sidebar_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/js/crud.js")) {
        const r = readStaticFile(state.allocator, "src/static/js/crud.js", crud_js);
        defer if (r.is_heap) state.allocator.free(r.data);
        try utils.sendResponse(stream, "200 OK", "application/javascript", r.data);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/connect")) {
        try schema_mod.handleConnect(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/schema")) {
        try schema_mod.handleSchema(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql")) {
        try sql_mod.handleSql(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/export/")) {
        try export_mod.handleExport(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/bulk-update")) {
        try crud.handleBulkUpdate(stream, request, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.indexOf(u8, path, "/fk-lookup") != null) {
        try crud.handleFkLookup(stream, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/ddl")) {
        try export_mod.handleTableDdl(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/import")) {
        try export_mod.handleCsvImport(stream, request, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/stats")) {
        try export_mod.handleTableStats(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.startsWith(u8, path, "/api/tables/") and std.mem.endsWith(u8, path, "/truncate")) {
        try crud.handleTruncateTable(stream, path, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.startsWith(u8, path, "/api/tables/")) {
        try crud.handleTableData(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/update")) {
        try crud.handleUpdate(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/delete-row")) {
        try crud.handleDeleteRow(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/insert-row")) {
        try crud.handleInsertRow(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/journal")) {
        try sql_mod.handleJournal(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/journal/undo")) {
        try sql_mod.handleJournalUndo(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql/schema-preview")) {
        try sql_mod.handleSchemaPreview(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql/preview")) {
        try sql_mod.handleSqlPreview(stream, request, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/settings/read-only")) {
        try schema_mod.handleReadOnlyToggle(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/settings/read-only")) {
        try schema_mod.handleReadOnlyGet(stream, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/history")) {
        try sql_mod.handleHistory(stream, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/connections")) {
        try schema_mod.handleGetConnections(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/connections")) {
        try schema_mod.handlePostConnection(stream, request, state);
    } else if (std.mem.eql(u8, method, "DELETE") and std.mem.startsWith(u8, path, "/api/connections/")) {
        try schema_mod.handleDeleteConnection(stream, path, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/sql/export")) {
        try export_mod.handleSqlExport(stream, request, state);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/api/health")) {
        try schema_mod.handleHealthCheck(stream, state);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/api/reconnect")) {
        try schema_mod.handleReconnect(stream, state);
    } else {
        try utils.sendResponse(stream, "404 Not Found", "text/plain", "Not Found");
    }
}

// ServerState tests

test "ServerState: init defaults" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
    try std.testing.expect(state.schema_text == null);
    try std.testing.expect(!state.read_only);
}

// readStaticFile tests

test "readStaticFile: disk read succeeds returns is_heap=true" {
    // Only meaningful in debug builds (which is how `zig build test` runs)
    if (comptime builtin.mode != .Debug) return;

    const allocator = std.testing.allocator;
    const content = "hello from disk";

    // Write a temp file
    const tmp_path = "/tmp/lux_test_static_file.txt";
    {
        const f = try std.fs.createFileAbsolute(tmp_path, .{});
        defer f.close();
        try f.writeAll(content);
    }
    defer std.fs.deleteFileAbsolute(tmp_path) catch {};

    const embedded: []const u8 = "embedded fallback";
    const result = readStaticFile(allocator, tmp_path, embedded);
    defer if (result.is_heap) allocator.free(result.data);

    try std.testing.expect(result.is_heap);
    try std.testing.expectEqualStrings(content, result.data);
}

test "readStaticFile: missing disk file falls back to embedded" {
    if (comptime builtin.mode != .Debug) return;

    const allocator = std.testing.allocator;
    const embedded: []const u8 = "embedded fallback";

    const result = readStaticFile(allocator, "/tmp/lux_nonexistent_file_xyz.txt", embedded);
    defer if (result.is_heap) allocator.free(result.data);

    try std.testing.expect(!result.is_heap);
    try std.testing.expectEqualStrings(embedded, result.data);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
