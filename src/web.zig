const std = @import("std");
const builtin = @import("builtin");
const postgres = @import("postgres.zig");
const utils = @import("utils.zig");
const crud = @import("crud.zig");
const schema_mod = @import("schema.zig");
const export_mod = @import("export.zig");
const sql_mod = @import("sql.zig");

const index_html = @embedFile("static/index.html");
const styles_css = @embedFile("static/styles.css");
const app_js = @embedFile("static/app.js");
const grid_js = @embedFile("static/grid.js");
const sidebar_js = @embedFile("static/sidebar.js");
const crud_js = @embedFile("static/crud.js");

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

pub const MAX_JOURNAL_ENTRIES = 10000;
pub const MAX_HISTORY_ENTRIES = 500;

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

// ── ServerState tests ────────────────────────────────────────────────────

test "ServerState: init defaults" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
    try std.testing.expect(state.schema_text == null);
    try std.testing.expect(!state.read_only);
}
