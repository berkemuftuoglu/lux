const std = @import("std");
const postgres = @import("postgres.zig");
const utils = @import("utils.zig");
const crud = @import("crud.zig");
const schema_mod = @import("schema.zig");
const export_mod = @import("export.zig");
const sql_mod = @import("sql.zig");

const log = std.log.scoped(.web);

var shutdown_requested = std.atomic.Value(bool).init(false);

fn handleSignal(_: c_int) callconv(.c) void {
    shutdown_requested.store(true, .release);
}

const index_html = @embedFile("static/index.html");
const styles_css = @embedFile("static/styles.css");
const app_js = @embedFile("static/app.js");
const grid_js = @embedFile("static/grid.js");
const sidebar_js = @embedFile("static/sidebar.js");
const crud_js = @embedFile("static/crud.js");

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
        try utils.sendHtmlResponseWithCsp(stream, index_html);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/favicon.ico")) {
        try utils.sendResponse(stream, "204 No Content", "image/x-icon", "");
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/styles.css")) {
        try utils.sendResponse(stream, "200 OK", "text/css", styles_css);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/app.js")) {
        try utils.sendResponse(stream, "200 OK", "application/javascript", app_js);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/grid.js")) {
        try utils.sendResponse(stream, "200 OK", "application/javascript", grid_js);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/sidebar.js")) {
        try utils.sendResponse(stream, "200 OK", "application/javascript", sidebar_js);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/crud.js")) {
        try utils.sendResponse(stream, "200 OK", "application/javascript", crud_js);
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

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
