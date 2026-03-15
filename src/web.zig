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

const StaticResult = struct { data: []const u8, is_heap: bool };

fn readStaticFile(allocator: std.mem.Allocator, disk_path: []const u8, embedded: []const u8) StaticResult {
    if (comptime builtin.mode != .Debug) return .{ .data = embedded, .is_heap = false };
    const data = std.fs.cwd().readFileAlloc(allocator, disk_path, 4 * 1024 * 1024) catch |err| {
        log.warn("dev: could not read '{s}' ({s}), using embedded", .{ disk_path, @errorName(err) });
        return .{ .data = embedded, .is_heap = false };
    };
    return .{ .data = data, .is_heap = true };
}

const static_files = .{
    .{ "/css/styles.css", "text/css", "src/static/css/styles.css", @embedFile("static/css/styles.css") },
    .{ "/js/state.js", "application/javascript", "src/static/js/state.js", @embedFile("static/js/state.js") },
    .{ "/js/utils.js", "application/javascript", "src/static/js/utils.js", @embedFile("static/js/utils.js") },
    .{ "/js/connection.js", "application/javascript", "src/static/js/connection.js", @embedFile("static/js/connection.js") },
    .{ "/js/sql-tabs.js", "application/javascript", "src/static/js/sql-tabs.js", @embedFile("static/js/sql-tabs.js") },
    .{ "/js/sql.js", "application/javascript", "src/static/js/sql.js", @embedFile("static/js/sql.js") },
    .{ "/js/saved-queries.js", "application/javascript", "src/static/js/saved-queries.js", @embedFile("static/js/saved-queries.js") },
    .{ "/js/cmd-palette.js", "application/javascript", "src/static/js/cmd-palette.js", @embedFile("static/js/cmd-palette.js") },
    .{ "/js/table-create.js", "application/javascript", "src/static/js/table-create.js", @embedFile("static/js/table-create.js") },
    .{ "/js/table-ops.js", "application/javascript", "src/static/js/table-ops.js", @embedFile("static/js/table-ops.js") },
    .{ "/js/app.js", "application/javascript", "src/static/js/app.js", @embedFile("static/js/app.js") },
    .{ "/js/grid.js", "application/javascript", "src/static/js/grid.js", @embedFile("static/js/grid.js") },
    .{ "/js/sidebar.js", "application/javascript", "src/static/js/sidebar.js", @embedFile("static/js/sidebar.js") },
    .{ "/js/crud.js", "application/javascript", "src/static/js/crud.js", @embedFile("static/js/crud.js") },
};

pub fn isValidPath(path: []const u8) bool {
    if (path.len == 0) return false;
    for (path) |byte| {
        if (byte < 0x20 or byte > 0x7E) return false;
    }
    return true;
}

const Method = enum { GET, POST, DELETE };
const MatchType = enum { exact, prefix };

pub const HandlerError = error{
    OutOfMemory,
    ReadOnlyMode,
    NoConnection,
    MissingContentLength,
    PayloadTooLarge,
    MalformedRequest,
    MissingField,
    InvalidData,
    BrokenPipe,
    ConnectionResetByPeer,
} || postgres.PgError;

const HandlerFn = *const fn (std.net.Stream, []const u8, []const u8, *ServerState, std.mem.Allocator) anyerror!void;

fn handleRequestError(stream: std.net.Stream, err: anyerror) void {
    const status_and_body = switch (err) {
        error.OutOfMemory => .{ "500 Internal Server Error", "{\"error\":\"Out of memory\"}" },
        error.ConnectionFailed => .{ "200 OK", "{\"error\":\"Database connection failed\"}" },
        error.QueryFailed => .{ "200 OK", "{\"error\":\"Query execution failed\"}" },
        error.NoResults => .{ "200 OK", "{\"error\":\"No results\"}" },
        error.InvalidData => .{ "400 Bad Request", "{\"error\":\"Invalid data\"}" },
        error.ReadOnlyMode => .{ "403 Forbidden", "{\"error\":\"Read-only mode is enabled\"}" },
        error.NoConnection => .{ "200 OK", "{\"error\":\"No database connected\"}" },
        error.MissingContentLength => .{ "400 Bad Request", "{\"error\":\"Missing Content-Length\"}" },
        error.PayloadTooLarge => .{ "413 Payload Too Large", "{\"error\":\"Request too large\"}" },
        error.MalformedRequest => .{ "400 Bad Request", "{\"error\":\"Malformed request\"}" },
        error.MissingField => .{ "400 Bad Request", "{\"error\":\"Missing required field\"}" },
        // Client disconnected — nothing to send
        error.BrokenPipe, error.ConnectionResetByPeer => return,
        else => .{ "500 Internal Server Error", "{\"error\":\"Internal server error\"}" },
    };
    utils.sendResponse(stream, status_and_body[0], "application/json", status_and_body[1]) catch return;
}

const Route = struct {
    method: Method,
    path: []const u8,
    match: MatchType,
    handler: HandlerFn,
};

// Order matters: more-specific suffixes must come before the catch-all GET.
fn dispatchTableRoute(stream: std.net.Stream, method: []const u8, request: []const u8, path: []const u8, state: *ServerState, arena: std.mem.Allocator) anyerror!void {
    if (std.mem.eql(u8, method, "POST") and std.mem.endsWith(u8, path, "/bulk-update")) {
        return crud.handleBulkUpdate(stream, request, path, state, arena);
    }
    if (std.mem.eql(u8, method, "POST") and std.mem.endsWith(u8, path, "/import")) {
        return export_mod.handleCsvImport(stream, request, path, state, arena);
    }
    if (std.mem.eql(u8, method, "POST") and std.mem.endsWith(u8, path, "/truncate")) {
        return crud.handleTruncateTable(stream, request, path, state, arena);
    }
    if (std.mem.eql(u8, method, "GET") and std.mem.indexOf(u8, path, "/fk-lookup") != null) {
        return crud.handleFkLookup(stream, request, path, state, arena);
    }
    if (std.mem.eql(u8, method, "GET") and std.mem.endsWith(u8, path, "/ddl")) {
        return export_mod.handleTableDdl(stream, request, path, state, arena);
    }
    if (std.mem.eql(u8, method, "GET") and std.mem.endsWith(u8, path, "/stats")) {
        return export_mod.handleTableStats(stream, request, path, state, arena);
    }
    if (std.mem.eql(u8, method, "GET")) {
        return crud.handleTableData(stream, request, path, state, arena);
    }
    try utils.sendResponse(stream, "404 Not Found", "text/plain", "Not Found");
}

const routes = [_]Route{
    .{ .method = .GET, .path = "/api/schema", .match = .exact, .handler = schema_mod.handleSchema },
    .{ .method = .GET, .path = "/api/settings/read-only", .match = .exact, .handler = schema_mod.handleReadOnlyGet },
    .{ .method = .GET, .path = "/api/connections", .match = .exact, .handler = schema_mod.handleGetConnections },
    .{ .method = .GET, .path = "/api/history", .match = .exact, .handler = sql_mod.handleHistory },
    .{ .method = .GET, .path = "/api/journal", .match = .exact, .handler = sql_mod.handleJournal },
    .{ .method = .GET, .path = "/api/health", .match = .exact, .handler = schema_mod.handleHealthCheck },
    .{ .method = .POST, .path = "/api/connect", .match = .exact, .handler = schema_mod.handleConnect },
    .{ .method = .POST, .path = "/api/sql", .match = .exact, .handler = sql_mod.handleSql },
    .{ .method = .POST, .path = "/api/settings/read-only", .match = .exact, .handler = schema_mod.handleReadOnlyToggle },
    .{ .method = .POST, .path = "/api/connections", .match = .exact, .handler = schema_mod.handlePostConnection },
    .{ .method = .POST, .path = "/api/journal/undo", .match = .exact, .handler = sql_mod.handleJournalUndo },
    .{ .method = .POST, .path = "/api/sql/schema-preview", .match = .exact, .handler = sql_mod.handleSchemaPreview },
    .{ .method = .POST, .path = "/api/sql/preview", .match = .exact, .handler = sql_mod.handleSqlPreview },
    .{ .method = .POST, .path = "/api/sql/export", .match = .exact, .handler = export_mod.handleSqlExport },
    .{ .method = .POST, .path = "/api/update", .match = .exact, .handler = crud.handleUpdate },
    .{ .method = .POST, .path = "/api/delete-row", .match = .exact, .handler = crud.handleDeleteRow },
    .{ .method = .POST, .path = "/api/insert-row", .match = .exact, .handler = crud.handleInsertRow },
    .{ .method = .POST, .path = "/api/reconnect", .match = .exact, .handler = schema_mod.handleReconnect },
    .{ .method = .GET, .path = "/api/export/", .match = .prefix, .handler = export_mod.handleExport },
    .{ .method = .DELETE, .path = "/api/connections/", .match = .prefix, .handler = schema_mod.handleDeleteConnection },
};

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

comptime {
    std.debug.assert(max_journal_entries >= max_history_entries);
    std.debug.assert(max_request_size >= 4096);
    std.debug.assert(max_request_size <= 65536);
    std.debug.assert(max_journal_entries <= 100000);
}

pub const QueryHistoryEntry = struct {
    sql: []const u8,
    timestamp: i64,
    duration_ms: u64,
    row_count: ?usize,
    is_error: bool,
    error_msg: ?[]const u8,
};

pub const ServerState = struct {
    pub const Flags = packed struct {
        read_only: bool = false,
        _padding: u7 = 0,
    };

    allocator: std.mem.Allocator,
    conninfo_z: ?[:0]u8 = null,
    schema_tables: ?[]postgres.TableInfo = null,
    schema_text: ?[]u8 = null,
    schema_arena: ?std.heap.ArenaAllocator = null,
    enhanced_schema: ?[]postgres.EnhancedTableInfo = null,
    enhanced_arena: ?std.heap.ArenaAllocator = null,
    flags: Flags = .{},
    change_journal: std.ArrayList(ChangeEntry) = .{},
    next_journal_id: u64 = 1,
    query_history: std.ArrayList(QueryHistoryEntry) = .{},
    last_conninfo: ?[]const u8 = null,
    next_connection_id: u64 = 1,
    /// Stored for CSRF origin checks, not for binding.
    port: u16 = 8080,

    pub fn init(allocator: std.mem.Allocator) ServerState {
        return .{
            .allocator = allocator,
            .change_journal = .{},
            .query_history = .{},
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*ServerState {
        const self = try allocator.create(ServerState);
        self.* = init(allocator);
        return self;
    }

    pub fn hasDbConnection(self: *const ServerState) bool {
        return self.conninfo_z != null;
    }

    pub fn deinit(self: *ServerState) void {
        for (self.change_journal.items) |entry| {
            self.allocator.free(entry.table_name);
            self.allocator.free(entry.operation);
            self.allocator.free(entry.column_name);
            self.allocator.free(entry.old_value);
            self.allocator.free(entry.new_value);
            self.allocator.free(entry.pk_column);
            self.allocator.free(entry.pk_value);
        }
        self.change_journal.deinit(self.allocator);
        for (self.query_history.items) |entry| {
            self.allocator.free(entry.sql);
            if (entry.error_msg) |e| self.allocator.free(e);
        }
        self.query_history.deinit(self.allocator);
        if (self.conninfo_z) |old| self.allocator.free(old);
        if (self.schema_text) |old| self.allocator.free(old);
        if (self.schema_arena) |*a| a.deinit();
        if (self.enhanced_arena) |*a| a.deinit();
        if (self.last_conninfo) |old| self.allocator.free(@constCast(old));
        self.* = undefined;
    }

    pub fn destroy(self: *ServerState, allocator: std.mem.Allocator) void {
        self.deinit();
        allocator.destroy(self);
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

        // Prevent idle clients from holding the single-threaded server indefinitely
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
    var arena = std.heap.ArenaAllocator.init(state.allocator);
    defer arena.deinit();
    const request_alloc = arena.allocator();

    var buf: [max_request_size]u8 = undefined;
    var total_read: usize = 0;

    // Headers may arrive split across TCP segments; loop until \r\n\r\n.
    while (total_read < buf.len) {
        const n = stream.read(buf[total_read..]) catch return;
        if (n == 0) return; // Connection closed
        total_read += n;
        if (std.mem.indexOf(u8, buf[0..total_read], "\r\n\r\n") != null) break;
    }

    if (total_read == buf.len and std.mem.indexOf(u8, buf[0..total_read], "\r\n\r\n") == null) {
        utils.sendResponse(stream, "400 Bad Request", "text/plain", "Request too large") catch return;
        return;
    }

    const request = buf[0..total_read];

    const first_line_end = std.mem.indexOf(u8, request, "\r\n") orelse return;
    const first_line = request[0..first_line_end];

    var parts = std.mem.splitScalar(u8, first_line, ' ');
    const method = parts.next() orelse return;
    const path_raw = parts.next() orelse return;

    const path = if (std.mem.indexOf(u8, path_raw, "?")) |q| path_raw[0..q] else path_raw;

    const is_post = std.mem.eql(u8, method, "POST");
    const is_delete = std.mem.eql(u8, method, "DELETE");
    if (is_post or is_delete) {
        if (!utils.checkOrigin(request, state.port)) {
            try utils.sendResponse(stream, "403 Forbidden", "text/plain", "Forbidden: cross-origin request");
            return;
        }
    }

    if (!isValidPath(path)) {
        try utils.sendResponse(stream, "400 Bad Request", "text/plain", "Bad Request: invalid path");
        return;
    }

    // Index needs CSP header (sendHtmlResponseWithCsp), unlike other static files
    if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/")) {
        const r = readStaticFile(request_alloc, "src/static/index.html", index_html);
        try utils.sendHtmlResponseWithCsp(stream, r.data);
        return;
    }

    if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/favicon.ico")) {
        try utils.sendResponse(stream, "204 No Content", "image/x-icon", "");
        return;
    }

    inline for (static_files) |entry| {
        if (std.mem.eql(u8, path, entry[0])) {
            const r = readStaticFile(request_alloc, entry[2], entry[3]);
            try utils.sendResponse(stream, "200 OK", entry[1], r.data);
            return;
        }
    }

    if (std.mem.startsWith(u8, path, "/api/tables/")) {
        dispatchTableRoute(stream, method, request, path, state, request_alloc) catch |err| {
            handleRequestError(stream, err);
        };
        return;
    }

    const method_enum: ?Method = if (std.mem.eql(u8, method, "GET"))
        .GET
    else if (std.mem.eql(u8, method, "POST"))
        .POST
    else if (std.mem.eql(u8, method, "DELETE"))
        .DELETE
    else
        null;

    if (method_enum) |m| {
        inline for (routes) |route| {
            if (route.method == m) {
                const matched = switch (route.match) {
                    .exact => std.mem.eql(u8, path, route.path),
                    .prefix => std.mem.startsWith(u8, path, route.path),
                };
                if (matched) {
                    route.handler(stream, request, path, state, request_alloc) catch |err| {
                        handleRequestError(stream, err);
                    };
                    return;
                }
            }
        }
    }

    try utils.sendResponse(stream, "404 Not Found", "text/plain", "Not Found");
}

test "ServerState: init defaults" {
    const state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
    try std.testing.expect(state.schema_text == null);
    try std.testing.expect(!state.flags.read_only);
}

test "ServerState.Flags: packed struct has read_only field" {
    var flags = ServerState.Flags{};
    try std.testing.expect(!flags.read_only);
    flags.read_only = true;
    try std.testing.expect(flags.read_only);
}

test "ServerState.Flags: default is all false" {
    const flags = ServerState.Flags{};
    try std.testing.expect(!flags.read_only);
}

test "ServerState: flags.read_only works like old read_only" {
    var state = ServerState.init(std.testing.allocator);
    try std.testing.expect(!state.flags.read_only);
    state.flags.read_only = true;
    try std.testing.expect(state.flags.read_only);
}

test "readStaticFile: disk read succeeds returns is_heap=true" {
    if (comptime builtin.mode != .Debug) return;

    const allocator = std.testing.allocator;
    const content = "hello from disk";

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

test "ServerState: create returns heap-allocated state" {
    const state = try ServerState.create(std.testing.allocator);
    defer state.destroy(std.testing.allocator);
    try std.testing.expect(!state.hasDbConnection());
    try std.testing.expect(!state.flags.read_only);
}

test "ServerState: deinit cleans up empty state without crash" {
    var state = ServerState.init(std.testing.allocator);
    state.deinit();
}

test "isValidPath: valid API path" {
    try std.testing.expect(isValidPath("/api/tables/users"));
}

test "isValidPath: valid JS path" {
    try std.testing.expect(isValidPath("/js/app.js"));
}

test "isValidPath: empty path is invalid" {
    try std.testing.expect(!isValidPath(""));
}

test "isValidPath: null byte is invalid" {
    try std.testing.expect(!isValidPath("/api/tables/users\x00"));
}

test "isValidPath: control char 0x01 is invalid" {
    try std.testing.expect(!isValidPath("/api/tables/users\x01"));
}

test "isValidPath: DEL 0x7F is invalid" {
    try std.testing.expect(!isValidPath("/api/tables/users\x7F"));
}

test "isValidPath: root path is valid" {
    try std.testing.expect(isValidPath("/"));
}

test "isValidPath: space 0x20 is valid" {
    try std.testing.expect(isValidPath("/api/search?q=hello world"));
}

test "static_files tuple: has 14 JS/CSS entries" {
    try std.testing.expectEqual(@as(usize, 14), static_files.len);
}

comptime {
    std.testing.refAllDeclsRecursive(@This());
}
